use anyhow::{Context, Result};
use futures::StreamExt;
use lapin::options::BasicAckOptions;
use shared::{TranscriptAudioPayload, RabbitMQClient, RabbitMQConfig, WorkerMessage, WebhookClient, S3Client};
use std::sync::Arc;
use tokio::sync::Semaphore;
use tracing::{error, info};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

const EVENT_TYPE: &str = "transcript_audio";
const DEFAULT_MAX_CONCURRENT_TASKS: usize = 10;
const MAX_PARALLEL_FILES: usize = 4;
const ASSEMBLYAI_UPLOAD_URL: &str = "https://api.assemblyai.com/v2/upload";
const ASSEMBLYAI_TRANSCRIPT_URL: &str = "https://api.assemblyai.com/v2/transcript";
const ASSEMBLYAI_SRT_CHARS_PER_CAPTION: u32 = 32;
const POLL_INTERVAL_SECONDS: u64 = 3;
const CHUNK_DURATION_MS: i64 = 300_000;
const MIN_SRT_BLOCK_LINES: usize = 3;
const TIMESTAMP_PARTS_COUNT: usize = 2;
const TIME_PARTS_COUNT: usize = 3;
const MS_PER_HOUR: i64 = 3600000;
const MS_PER_MINUTE: i64 = 60000;
const MS_PER_SECOND: i64 = 1000;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG").unwrap_or("info".to_string()),
        )
        .init();

    info!("Starting task-transcript-audio service");

    dotenvy::dotenv().ok();

    let config = RabbitMQConfig::from_env();
    let rabbitmq_client = RabbitMQClient::new(config)
        .await
        .context("Failed to create RabbitMQ client")?;

    let webhook_client = Arc::new(WebhookClient::new());

    let queue_name = std::env::var("QUEUE_TRANSCRIPT_AUDIO").unwrap_or("core.transcript_audio".to_string());
    
    let max_concurrent = DEFAULT_MAX_CONCURRENT_TASKS;

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
                        match serde_json::from_slice::<WorkerMessage<TranscriptAudioPayload>>(&delivery.data) {
                            Ok(message) => {
                                let webhook_client = Arc::clone(&webhook_client);
                                let semaphore = Arc::clone(&semaphore);
                                
                                tokio::spawn(async move {
                                    let _permit = semaphore.acquire().await.unwrap();
                                    
                                    info!("Processing clip: {}", message.payload.clip_id);
                                    info!("Video ID: {}", message.payload.video_id);

                                    match process_transcript_audio(&message.payload).await {
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

async fn process_transcript_audio(
    payload: &TranscriptAudioPayload,
) -> Result<serde_json::Value> {

    let assemblyai_api_key = std::env::var("ASSEMBLYAI_API_KEY")
        .context("ASSEMBLYAI_API_KEY environment variable not set")?;
    
    let s3_client = Arc::new(S3Client::from_env().await
        .context("Failed to create S3 client")?);

    let temp_dir = std::env::temp_dir().join(payload.clip_id.to_string());
    tokio::fs::create_dir_all(&temp_dir).await
        .context("Failed to create temporary directory")?;
    
    let result = process_transcript_audio_inner(payload, &s3_client, &temp_dir, assemblyai_api_key).await;
    
    if let Err(e) = tokio::fs::remove_dir_all(&temp_dir).await {
        error!("Failed to clean up temporary directory for clip {}: {}", payload.clip_id, e);
    }
    
    result
}

async fn process_transcript_audio_inner(
    payload: &TranscriptAudioPayload,
    s3_client: &Arc<S3Client>,
    temp_dir: &std::path::PathBuf,
    assemblyai_api_key: String,
) -> Result<serde_json::Value> {

    if payload.activate_transcribing_audio {
        return transcript_fake_audio(payload, s3_client, temp_dir).await;
    }

    let semaphore = Arc::new(Semaphore::new(MAX_PARALLEL_FILES));
    let mut tasks = Vec::new();

    for (index, audio_file) in payload.audio_files.iter().enumerate() {
        let s3_client_clone = Arc::clone(&s3_client);
        let api_key = assemblyai_api_key.clone();
        let clip_id = payload.clip_id.to_string();
        let audio_file = audio_file.clone();
        let temp_dir = temp_dir.clone();
        let semaphore = Arc::clone(&semaphore);

        let task = tokio::spawn(async move {
            let _permit = semaphore.acquire().await.unwrap();
            
            let s3_path = format!("{}/audios/{}", clip_id, audio_file);
            
            process_single_audio_file(
                &s3_client_clone,
                &api_key,
                &s3_path,
                &temp_dir,
                &audio_file,
                index,
            ).await
        });

        tasks.push(task);
    }

    let mut subtitle_parts = Vec::new();
    for (index, task) in tasks.into_iter().enumerate() {
        match task.await {
            Ok(Ok(subtitle)) => {
                subtitle_parts.push((index, subtitle));
            }
            Ok(Err(e)) => {
                error!("Failed to process file {}: {}", index, e);
                return Err(e);
            }
            Err(e) => {
                error!("Task {} panicked: {}", index, e);
                return Err(anyhow::anyhow!("Task {} panicked: {}", index, e));
            }
        }
    }

    subtitle_parts.sort_by_key(|(index, _)| *index);
    
    let merged_srt = merge_subtitles(subtitle_parts.into_iter().map(|(_, s)| s).collect())?;

    let srt_name = format!("{}.srt", uuid::Uuid::now_v7());
    let srt_path = temp_dir.join(&srt_name);
    
    tokio::fs::write(&srt_path, &merged_srt).await
        .context("Failed to write merged SRT file")?;

    let s3_srt_path = format!("{}/subtitles/{}", payload.clip_id, srt_name);
    
    s3_client.as_ref().upload_file(
        srt_path.to_str().unwrap(),
        &s3_srt_path,
    ).await
        .context("Failed to upload SRT to S3")?;

    Ok(serde_json::json!({
        "clip_id": payload.clip_id,
        "video_id": payload.video_id,
        "subtitle_srt_name": srt_name,
    }))
}

async fn transcript_fake_audio(
    payload: &TranscriptAudioPayload,
    s3_client: &Arc<S3Client>,
    temp_dir: &std::path::PathBuf,
) -> Result<serde_json::Value> {
    // Charger le fichier fake.srt (supposé être à la racine du projet ou dans un dossier connu)
    let fake_srt_path = PathBuf::from("/app/task-transcript-audio/src/fake.srt");
    
    let srt_content = tokio::fs::read_to_string(&fake_srt_path).await
        .context("Failed to read fake.srt file")?;

    // Créer le nom du fichier de sortie
    let srt_name = format!("{}.srt", uuid::Uuid::now_v7());
    let srt_path = temp_dir.join(&srt_name);
    
    // Écrire le contenu dans le répertoire temporaire
    tokio::fs::write(&srt_path, &srt_content).await
        .context("Failed to write SRT file")?;

    // Construire le chemin S3
    let s3_srt_path = format!("{}/subtitles/{}", payload.clip_id, srt_name);
    
    // Upload vers S3
    s3_client.as_ref().upload_file(
        srt_path.to_str().unwrap(),
        &s3_srt_path,
    ).await
        .context("Failed to upload SRT to S3")?;

    // Retourner le même format JSON que process_transcript_audio_inner
    Ok(serde_json::json!({
        "clip_id": payload.clip_id,
        "video_id": payload.video_id,
        "subtitle_srt_name": srt_name,
    }))
}

#[derive(Debug, Serialize)]
struct AssemblyAITranscriptRequest {
    audio_url: String,
    language_detection: bool,
}

#[derive(Debug, Deserialize)]
struct AssemblyAITranscriptResponse {
    id: String,
    #[allow(dead_code)]
    status: String,
    #[allow(dead_code)]
    #[serde(default)]
    error: Option<String>,
}

#[derive(Debug, Deserialize)]
struct AssemblyAITranscriptStatusResponse {
    #[allow(dead_code)]
    id: String,
    status: String,
    #[serde(default)]
    error: Option<String>,
}

#[derive(Debug, Clone)]
struct SubtitleEntry {
    #[allow(dead_code)]
    index: usize,
    start_time: i64,
    end_time: i64,
    text: String,
}

async fn process_single_audio_file(
    s3_client: &Arc<S3Client>,
    api_key: &str,
    audio_file: &str,
    temp_dir: &PathBuf,
    original_filename: &str,
    _index: usize,
) -> Result<Vec<SubtitleEntry>> {
    let local_file_path = temp_dir.join(original_filename);
    s3_client.download_file(audio_file, local_file_path.to_str().unwrap()).await
        .context("Failed to download audio file from S3")?;

    let upload_url = upload_to_assemblyai(api_key, &local_file_path).await
        .context("Failed to upload file to AssemblyAI")?;

    let transcript_id = start_transcription(api_key, &upload_url).await
        .context("Failed to start transcription")?;

    let srt_content = poll_transcription(api_key, &transcript_id).await
        .context("Failed to get transcription result")?;

    let subtitle_entries = parse_srt(&srt_content)?;

    let _ = tokio::fs::remove_file(&local_file_path).await;

    Ok(subtitle_entries)
}

async fn upload_to_assemblyai(api_key: &str, file_path: &PathBuf) -> Result<String> {
    let client = reqwest::Client::new();
    
    let file_content = tokio::fs::read(file_path).await
        .context("Failed to read audio file")?;

    let response = client
        .post(ASSEMBLYAI_UPLOAD_URL)
        .header("authorization", api_key)
        .body(file_content)
        .send()
        .await
        .context("Failed to upload file to AssemblyAI")?;

    if !response.status().is_success() {
        let error_text = response.text().await.unwrap_or_default();
        return Err(anyhow::anyhow!("AssemblyAI upload failed: {}", error_text));
    }

    let upload_response: serde_json::Value = response.json().await
        .context("Failed to parse upload response")?;

    let upload_url = upload_response["upload_url"]
        .as_str()
        .context("No upload_url in response")?
        .to_string();

    Ok(upload_url)
}

async fn start_transcription(api_key: &str, audio_url: &str) -> Result<String> {
    let client = reqwest::Client::new();

    let request_body = AssemblyAITranscriptRequest {
        audio_url: audio_url.to_string(),
        language_detection: true,
    };

    let response = client
        .post(ASSEMBLYAI_TRANSCRIPT_URL)
        .header("authorization", api_key)
        .json(&request_body)
        .send()
        .await
        .context("Failed to start transcription")?;

    if !response.status().is_success() {
        let error_text = response.text().await.unwrap_or_default();
        return Err(anyhow::anyhow!("AssemblyAI transcription start failed: {}", error_text));
    }

    let transcript_response: AssemblyAITranscriptResponse = response.json().await
        .context("Failed to parse transcription response")?;

    Ok(transcript_response.id)
}

async fn poll_transcription(api_key: &str, transcript_id: &str) -> Result<String> {
    let client = reqwest::Client::new();
    let status_url = format!("{}/{}", ASSEMBLYAI_TRANSCRIPT_URL, transcript_id);

    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(POLL_INTERVAL_SECONDS)).await;

        let response = client
            .get(&status_url)
            .header("authorization", api_key)
            .send()
            .await
            .context("Failed to poll transcription status")?;

        if !response.status().is_success() {
            let status = response.status();
            let error_text = response.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!("AssemblyAI poll failed (status {}): {}", status, error_text));
        }

        let response_text = response.text().await
            .context("Failed to read response text")?;
        
        let status_response: AssemblyAITranscriptStatusResponse = serde_json::from_str(&response_text)
            .context("Failed to parse status response")?;

        match status_response.status.as_str() {
            "completed" => {
                let srt_url = format!(
                    "{}/{}/srt?chars_per_caption={}",
                    ASSEMBLYAI_TRANSCRIPT_URL,
                    transcript_id,
                    ASSEMBLYAI_SRT_CHARS_PER_CAPTION
                );
                
                let srt_response = client
                    .get(&srt_url)
                    .header("authorization", api_key)
                    .send()
                    .await
                    .context("Failed to get SRT")?;

                if !srt_response.status().is_success() {
                    let status = srt_response.status();
                    let error_text = srt_response.text().await.unwrap_or_default();
                    
                    if status.as_u16() == 400 && error_text.contains("Transcript text is empty") {
                        return Ok(String::new());
                    }
                    
                    return Err(anyhow::anyhow!("Failed to get SRT (status {}): {}", status, error_text));
                }

                let srt_content = srt_response.text().await
                    .context("Failed to read SRT content")?;

                return Ok(srt_content);
            }
            "error" => {
                let error_msg = status_response.error.unwrap_or("Unknown error".to_string());
                return Err(anyhow::anyhow!("Transcription failed: {}", error_msg));
            }
            "queued" | "processing" => {
                continue;
            }
            _ => {
                continue;
            }
        }
    }
}

fn parse_srt(srt_content: &str) -> Result<Vec<SubtitleEntry>> {
    let mut entries = Vec::new();
    let blocks: Vec<&str> = srt_content.trim().split("\n\n").collect();

    for block in blocks {
        let lines: Vec<&str> = block.lines().collect();
        if lines.len() < MIN_SRT_BLOCK_LINES {
            continue;
        }

        let index_str = lines[0].trim();
        let index: usize = index_str.parse()
            .context(format!("Failed to parse index: {}", index_str))?;

        let timestamp_line = lines[1].trim();
        let (start_ms, end_ms) = parse_timestamp_line(timestamp_line)?;

        let text = lines[2..].join("\n");

        entries.push(SubtitleEntry {
            index,
            start_time: start_ms,
            end_time: end_ms,
            text,
        });
    }

    Ok(entries)
}

fn parse_timestamp_line(line: &str) -> Result<(i64, i64)> {
    let parts: Vec<&str> = line.split(" --> ").collect();
    if parts.len() != TIMESTAMP_PARTS_COUNT {
        return Err(anyhow::anyhow!("Invalid timestamp line: {}", line));
    }

    let start_ms = parse_timestamp(parts[0])?;
    let end_ms = parse_timestamp(parts[1])?;

    Ok((start_ms, end_ms))
}

fn parse_timestamp(ts: &str) -> Result<i64> {
    let parts: Vec<&str> = ts.split(',').collect();
    if parts.len() != TIMESTAMP_PARTS_COUNT {
        return Err(anyhow::anyhow!("Invalid timestamp: {}", ts));
    }

    let time_parts: Vec<&str> = parts[0].split(':').collect();
    if time_parts.len() != TIME_PARTS_COUNT {
        return Err(anyhow::anyhow!("Invalid time format: {}", parts[0]));
    }

    let hours: i64 = time_parts[0].parse()
        .context("Failed to parse hours")?;
    let minutes: i64 = time_parts[1].parse()
        .context("Failed to parse minutes")?;
    let seconds: i64 = time_parts[2].parse()
        .context("Failed to parse seconds")?;
    let milliseconds: i64 = parts[1].parse()
        .context("Failed to parse milliseconds")?;

    let total_ms = (hours * 3600 + minutes * 60 + seconds) * MS_PER_SECOND + milliseconds;
    Ok(total_ms)
}

fn format_timestamp(ms: i64) -> String {
    let hours = ms / MS_PER_HOUR;
    let minutes = (ms % MS_PER_HOUR) / MS_PER_MINUTE;
    let seconds = (ms % MS_PER_MINUTE) / MS_PER_SECOND;
    let milliseconds = ms % MS_PER_SECOND;

    format!("{:02}:{:02}:{:02},{:03}", hours, minutes, seconds, milliseconds)
}

fn merge_subtitles(parts: Vec<Vec<SubtitleEntry>>) -> Result<String> {
    let mut all_entries = Vec::new();

    for (part_index, part) in parts.into_iter().enumerate() {
        let time_offset = (part_index as i64) * CHUNK_DURATION_MS;
        
        for mut entry in part {
            entry.start_time += time_offset;
            entry.end_time += time_offset;
            all_entries.push(entry);
        }
    }

    let mut srt_content = String::new();
    for (new_index, entry) in all_entries.iter().enumerate() {
        srt_content.push_str(&format!("{}\n", new_index + 1));
        srt_content.push_str(&format!(
            "{} --> {}\n",
            format_timestamp(entry.start_time),
            format_timestamp(entry.end_time)
        ));
        srt_content.push_str(&format!("{}\n\n", entry.text));
    }

    Ok(srt_content)
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