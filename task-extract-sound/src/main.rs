use anyhow::{Context, Result};
use futures::StreamExt;
use lapin::options::BasicAckOptions;
use shared::{ExtractSoundPayload, RabbitMQClient, RabbitMQConfig, S3Client, WorkerMessage, WebhookClient};
use std::sync::Arc;
use tokio::sync::Semaphore;
use tracing::{error, info};
use tokio::process::Command;

const EVENT_TYPE: &str = "extract_sound";
const DEFAULT_MAX_CONCURRENT_JOBS: usize = 5;
const AUDIO_SEGMENT_DURATION_SECONDS: u32 = 300;
const AUDIO_SAMPLE_RATE: u32 = 16000;
const AUDIO_CHANNELS: u32 = 1;


#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG").unwrap_or("info".to_string()),
        )
        .init();

    info!("Starting extract-sound worker service");

    dotenvy::dotenv().ok();

    let config = RabbitMQConfig::from_env();
    let rabbitmq_client = RabbitMQClient::new(config)
        .await
        .context("Failed to create RabbitMQ client")?;

    let webhook_client = Arc::new(WebhookClient::new());
    let s3_client = Arc::new(
        S3Client::from_env()
            .await
            .context("Failed to create S3 client")?
    );

    let queue_name = std::env::var("QUEUE_EXTRACT_SOUND").unwrap_or("core.extract_sound".to_string());
    
    let max_concurrent = DEFAULT_MAX_CONCURRENT_JOBS;

    info!("Listening on queue: {} (max concurrent: {})", queue_name, max_concurrent);

    let mut consumer = rabbitmq_client
        .consume(&queue_name)
        .await
        .context("Failed to start consuming messages")?;

    let mut shutdown = Box::pin(setup_shutdown_handler());
    let semaphore = Arc::new(Semaphore::new(max_concurrent));

    info!("Service ready, waiting for messages...");

    loop {
        tokio::select! {
            Some(delivery) = consumer.next() => {
                match delivery {
                    Ok(delivery) => {
                        match serde_json::from_slice::<WorkerMessage<ExtractSoundPayload>>(&delivery.data) {
                            Ok(message) => {
                                let webhook_client = Arc::clone(&webhook_client);
                                let s3_client = Arc::clone(&s3_client);
                                let semaphore = Arc::clone(&semaphore);
                                
                                tokio::spawn(async move {
                                    let _permit = semaphore.acquire().await.unwrap();
                                    
                                    info!("Processing clip: {}", message.payload.clip_id);
                                    info!("Video ID: {}", message.payload.video_id);

                                    match process_extract_sound(&message.payload, &s3_client).await {
                                        Ok(result) => {
                                            info!("Clip {} completed successfully", message.payload.clip_id);
                                            info!("Webhook success payload: {}", serde_json::to_string_pretty(&result).unwrap_or_default());

                                            if let Err(e) = webhook_client
                                                .send_success(
                                                    &message.webhook_url_success,
                                                    EVENT_TYPE,
                                                    result,
                                                )
                                                .await
                                            {
                                                error!("Failed to send success webhook: {}", e);
                                            }
                                        }
                                        Err(e) => {
                                            error!("Clip {} failed: {}", message.payload.clip_id, e);

                                            let failure_payload = serde_json::json!({
                                                "clip_id": message.payload.clip_id,
                                                "video_id": message.payload.video_id,
                                            });

                                            info!("Webhook failure payload: {}", serde_json::to_string_pretty(&failure_payload).unwrap_or_default());

                                            if let Err(webhook_err) = webhook_client
                                                .send_failure(
                                                    &message.webhook_url_failure,
                                                    EVENT_TYPE,
                                                    failure_payload,
                                                )
                                                .await
                                            {
                                                error!("Failed to send failure webhook: {}", webhook_err);
                                            }
                                        }
                                    }

                                    if let Err(e) = delivery.ack(BasicAckOptions::default()).await {
                                        error!("Failed to acknowledge message: {}", e);
                                    }
                                });
                            }
                            Err(e) => {
                                error!("Failed to parse message: {}", e);
                                if let Err(ack_err) = delivery.ack(BasicAckOptions::default()).await {
                                    error!("Failed to acknowledge invalid message: {}", ack_err);
                                }
                            }
                        }
                    }
                    Err(e) => {
                        error!("Error receiving message: {}", e);
                    }
                }
            }
            _ = &mut shutdown => {
                info!("Shutdown signal received, stopping service...");
                break;
            }
        }
    }

    info!("Service stopped gracefully");
    Ok(())
}

async fn get_video_duration(video_path: &str) -> Result<u64> {
    let output = Command::new("ffprobe")
        .arg("-v")
        .arg("error")
        .arg("-show_entries")
        .arg("format=duration")
        .arg("-of")
        .arg("default=noprint_wrappers=1:nokey=1")
        .arg(video_path)
        .output()
        .await
        .context("Failed to execute ffprobe")?;

    if !output.status.success() {
        let error = String::from_utf8_lossy(&output.stderr);
        return Err(anyhow::anyhow!("ffprobe failed: {}", error));
    }

    let duration_str = String::from_utf8_lossy(&output.stdout);
    let duration_float = duration_str
        .trim()
        .parse::<f64>()
        .context("Failed to parse video duration")?;

    // Round to nearest second
    let duration = duration_float.round() as u64;

    Ok(duration)
}

async fn process_extract_sound(
    payload: &ExtractSoundPayload, 
    s3_client: &S3Client,
) -> Result<serde_json::Value> {
    let temp_dir = std::env::temp_dir().join(payload.clip_id.to_string());
    tokio::fs::create_dir_all(&temp_dir).await?;
    
    let result = process_extract_sound_inner(payload, s3_client, &temp_dir).await;
    
    if let Err(e) = tokio::fs::remove_dir_all(&temp_dir).await {
        error!("Failed to clean up temporary directory for clip {}: {}", payload.clip_id, e);
    }
    
    result
}

async fn process_extract_sound_inner(
    payload: &ExtractSoundPayload, 
    s3_client: &S3Client,
    temp_dir: &std::path::PathBuf,
) -> Result<serde_json::Value> {

    let video_file_name = format!("{}", payload.name);
    let video_s3_key = format!("{}/{}", payload.clip_id, video_file_name);
    let temp_video_path = temp_dir.join(&video_file_name);

    s3_client
        .download_file(&video_s3_key, &temp_video_path.to_string_lossy())
        .await
        .context("Failed to download video from S3")?;

    // Get video duration
    let duration = get_video_duration(&temp_video_path.to_string_lossy())
        .await
        .context("Failed to get video duration")?;
    
    info!("Video duration: {} seconds", duration);

    // Generate a UUID v7 for the audio pattern
    let audio_pattern_id = uuid::Uuid::now_v7();
    let audio_output_pattern = format!("{}/{}_%03d.wav", temp_dir.to_string_lossy(), audio_pattern_id);

    let output = Command::new("ffmpeg")
        .arg("-i")
        .arg(&temp_video_path.to_string_lossy().as_ref())
        .arg("-f")
        .arg("segment")
        .arg("-segment_time")
        .arg(&AUDIO_SEGMENT_DURATION_SECONDS.to_string())
        .arg("-segment_start_number")
        .arg("1")
        .arg("-c:a")
        .arg("pcm_s16le")
        .arg("-ar")
        .arg(&AUDIO_SAMPLE_RATE.to_string())
        .arg("-ac")
        .arg(&AUDIO_CHANNELS.to_string())
        .arg(&audio_output_pattern)
        .output()
        .await
        .context("Failed to execute ffmpeg")?;

    if !output.status.success() {
        let error = String::from_utf8_lossy(&output.stderr);
        return Err(anyhow::anyhow!("ffmpeg failed: {}", error));
    }

    let mut audio_files = Vec::new();
    let mut segment_index = 1;

    loop {
        let temp_segment_file = format!("{}_{:03}.wav", audio_pattern_id, segment_index);
        let temp_segment_path = temp_dir.join(&temp_segment_file);

        if !tokio::fs::try_exists(&temp_segment_path).await.unwrap_or(false) {
            break;
        }

        // Generate a unique UUID v7 for each audio segment
        let segment_file_name = format!("{}.wav", uuid::Uuid::now_v7());
        let s3_key = format!("{}/audios/{}", payload.clip_id, segment_file_name);

        let upload_result = s3_client
            .upload_file(&temp_segment_path.to_string_lossy(), &s3_key)
            .await;

        if let Err(e) = upload_result {
            return Err(e.context("Failed to upload audio segment to S3"));
        }

        audio_files.push(segment_file_name);
        segment_index += 1;
    }

    Ok(serde_json::json!({
        "clip_id": payload.clip_id,
        "video_id": payload.video_id,
        "audio_files": audio_files,
        "duration": duration,
    }))
}

async fn setup_shutdown_handler() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("Failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {
            info!("Received Ctrl+C signal");
        },
        _ = terminate => {
            info!("Received SIGTERM signal");
        },
    }
}
