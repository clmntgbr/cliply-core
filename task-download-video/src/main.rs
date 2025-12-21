use anyhow::{Context, Result};
use futures::StreamExt;
use lapin::options::BasicAckOptions;
use shared::{DownloadVideoPayload, RabbitMQClient, RabbitMQConfig, S3Client, WorkerMessage, WebhookClient};
use std::sync::Arc;
use tokio::sync::Semaphore;
use tracing::{error, info};
use tokio::process::Command;
use tokio::io::{AsyncBufReadExt, BufReader};

const EVENT_TYPE: &str = "download_video";
const DEFAULT_MAX_CONCURRENT_JOBS: usize = 10;

struct CleanupGuard<F: FnOnce()> {
    cleanup_fn: Option<F>,
}

impl<F: FnOnce()> CleanupGuard<F> {
    fn new(cleanup_fn: F) -> Self {
        Self {
            cleanup_fn: Some(cleanup_fn),
        }
    }
}

impl<F: FnOnce()> Drop for CleanupGuard<F> {
    fn drop(&mut self) {
        if let Some(cleanup_fn) = self.cleanup_fn.take() {
            cleanup_fn();
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG").unwrap_or("info".to_string()),
        )
        .init();

    info!("Starting download-video worker service");

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

    let queue_name = std::env::var("QUEUE_DOWNLOAD_VIDEO").unwrap_or("core.download_video".to_string());
    
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
                        match serde_json::from_slice::<WorkerMessage<DownloadVideoPayload>>(&delivery.data) {
                            Ok(message) => {
                                let webhook_client = Arc::clone(&webhook_client);
                                let s3_client = Arc::clone(&s3_client);
                                let semaphore = Arc::clone(&semaphore);
                                
                                tokio::spawn(async move {
                                    let _permit = semaphore.acquire().await.unwrap();
                                    
                                    info!("Processing video: {}", message.payload.video_id);
                                    info!("Clip ID: {}", message.payload.clip_id);

                                    match process_download_video(&message.payload, &s3_client).await {
                                        Ok(result) => {
                                            info!("Video {} completed successfully", message.payload.video_id);
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
                                            error!("Video {} failed: {}", message.payload.video_id, e);

                                            let failure_payload = serde_json::json!({
                                                "video_id": message.payload.video_id,
                                                "clip_id": message.payload.clip_id,
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

async fn process_download_video(
    payload: &DownloadVideoPayload,
    s3_client: &S3Client,
) -> Result<serde_json::Value> {
    // Create a unique directory for this specific download to avoid conflicts
    let unique_dir = format!("/tmp/videos/{}", payload.video_id);
    tokio::fs::create_dir_all(&unique_dir).await
        .context("Failed to create temporary directory")?;
    
    // Setup cleanup that runs at the end
    let cleanup_dir = unique_dir.clone();
    let _cleanup_guard = CleanupGuard::new(move || {
        tokio::spawn(async move {
            if tokio::fs::try_exists(&cleanup_dir).await.unwrap_or(false) {
                if let Err(e) = tokio::fs::remove_dir_all(&cleanup_dir).await {
                    error!("Failed to remove temporary directory during cleanup: {}", e);
                }
            }
        });
    });
    
    let output_template = format!("{}/video.%(ext)s", unique_dir);

    // Get metadata first
    let metadata_output = Command::new("yt-dlp")
        .arg("--dump-json")
        .arg("--no-playlist")
        .arg(&payload.url)
        .output()
        .await
        .context("Failed to execute yt-dlp for metadata")?;

    if !metadata_output.status.success() {
        let error = String::from_utf8_lossy(&metadata_output.stderr);
        return Err(anyhow::anyhow!("Failed to get video metadata: {}", error));
    }

    let metadata: serde_json::Value = serde_json::from_slice(&metadata_output.stdout)
        .context("Failed to parse yt-dlp metadata")?;

    // Download the video with improved error handling
    let mut child = Command::new("yt-dlp")
        .arg("-f")
        .arg("bestvideo[height<=720][ext=mp4]+bestaudio[ext=m4a]/best[height<=720][ext=mp4]/best[height<=720]")
        .arg("--no-playlist")
        .arg("--merge-output-format")
        .arg("mp4")
        .arg("--newline")
        // Add these flags to improve reliability
        .arg("--no-part")  // Don't use .part files
        .arg("--no-overwrites")
        .arg("--extractor-args")
        .arg("youtube:player_client=default")  // Suppress JS runtime warning
        .arg("-o")
        .arg(&output_template)
        .arg(&payload.url)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .context("Failed to spawn yt-dlp for download")?;

    let stdout = child.stdout.take().context("Failed to capture stdout")?;
    let stderr = child.stderr.take().context("Failed to capture stderr")?;

    let stdout_reader = BufReader::new(stdout);
    let stderr_reader = BufReader::new(stderr);

    let mut stdout_lines = stdout_reader.lines();
    let mut stderr_lines = stderr_reader.lines();

    // Collect error messages for better debugging
    let mut error_messages = Vec::new();

    loop {
        tokio::select! {
            line = stdout_lines.next_line() => {
                match line {
                    Ok(Some(_line)) => {},
                    Ok(None) => break,
                    Err(e) => {
                        error!("Error reading stdout: {}", e);
                        break;
                    }
                }
            }
            line = stderr_lines.next_line() => {
                match line {
                    Ok(Some(line)) => {
                        if !line.trim().is_empty() {
                            // Only log actual errors, not warnings
                            if line.contains("ERROR:") {
                                error!("yt-dlp error: {}", line);
                                error_messages.push(line.clone());
                            } else if line.contains("WARNING:") {
                                // Still log warnings but at debug level would be better
                                info!("yt-dlp warning: {}", line);
                            }
                        }
                    }
                    Ok(None) => {},
                    Err(e) => {
                        error!("Error reading stderr: {}", e);
                    }
                }
            }
        }
    }

    let status = child.wait().await.context("Failed to wait for yt-dlp process")?;

    if !status.success() {
        let error_detail = if error_messages.is_empty() {
            "Unknown error".to_string()
        } else {
            error_messages.join("; ")
        };
        return Err(anyhow::anyhow!(
            "yt-dlp process failed with status: {} - Details: {}", 
            status, 
            error_detail
        ));
    }

    // Find the downloaded file (it should be video.mp4)
    let temp_file_path = format!("{}/video.mp4", unique_dir);
    
    // Wait a moment for filesystem to sync (sometimes helps with timing issues)
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Verify the file exists before proceeding
    if !tokio::fs::try_exists(&temp_file_path).await.unwrap_or(false) {
        // List directory contents for debugging
        let mut entries = tokio::fs::read_dir(&unique_dir).await
            .context("Failed to read directory contents")?;
        let mut files = Vec::new();
        while let Some(entry) = entries.next_entry().await? {
            files.push(entry.file_name().to_string_lossy().to_string());
        }
        
        return Err(anyhow::anyhow!(
            "Downloaded file not found at {}. Directory contents: {:?}",
            temp_file_path,
            files
        ));
    }

    let file_metadata = tokio::fs::metadata(&temp_file_path)
        .await
        .context("Failed to read downloaded file metadata")?;
    
    let file_size = file_metadata.len();

    // Ensure we have a valid file
    if file_size == 0 {
        return Err(anyhow::anyhow!("Downloaded file is empty"));
    }

    let file_name = format!("{}.mp4", uuid::Uuid::now_v7());
    let s3_key = format!("{}/{}", payload.clip_id, file_name);
    
    info!("Uploading to S3 with key: {}", s3_key);
    
    // Upload to S3
    let upload_result = s3_client
        .upload_file(&temp_file_path, &s3_key)
        .await;

    // Cleanup happens automatically via CleanupGuard
    // But we can also explicitly remove the file before directory removal
    let _ = tokio::fs::remove_file(&temp_file_path).await;

    if let Err(e) = upload_result {
        error!("S3 upload failed for key {}: {:?}", s3_key, e);
        return Err(anyhow::anyhow!("Failed to upload video to S3: {}", e));
    }

    Ok(serde_json::json!({
        "name": file_name,
        "original_file_name": format!(
            "{}.mp4",
            metadata.get("title").and_then(|t| t.as_str()).unwrap_or("video")
        ),
        "format": "mp4",
        "size": file_size,
        "video_id": payload.video_id,
        "clip_id": payload.clip_id,
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
