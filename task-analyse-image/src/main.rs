use anyhow::{Context, Result};
use futures::StreamExt;
use lapin::options::BasicAckOptions;
use shared::{AnalyseImagePayload, RabbitMQClient, RabbitMQConfig, WorkerMessage, WebhookClient, S3Client};
use std::sync::Arc;
use tokio::sync::Semaphore;
use tracing::{error, info};
use serde::Deserialize;

const EVENT_TYPE: &str = "analyse_image";
const DEFAULT_MAX_CONCURRENT_TASKS: usize = 10;
const HUGGINGFACE_API_URL: &str = "https://api-inference.huggingface.co/models/Organika/sdxl-detector";

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG").unwrap_or("info".to_string()),
        )
        .init();

    info!("Starting task-analyse-image service");

    dotenvy::dotenv().ok();

    let config = RabbitMQConfig::from_env();
    let rabbitmq_client = RabbitMQClient::new(config)
        .await
        .context("Failed to create RabbitMQ client")?;

    let webhook_client = Arc::new(WebhookClient::new());

    let queue_name = std::env::var("QUEUE_ANALYSE_IMAGE").unwrap_or("core.analyse_image".to_string());
    
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
                        match serde_json::from_slice::<WorkerMessage<AnalyseImagePayload>>(&delivery.data) {
                            Ok(message) => {
                                let webhook_client = Arc::clone(&webhook_client);
                                let semaphore = Arc::clone(&semaphore);
                                
                                tokio::spawn(async move {
                                    let _permit = semaphore.acquire().await.unwrap();
                                    
                                    info!("Processing image: {}", message.payload.image_id);
                                    info!("Task ID: {}", message.payload.task_id);

                                    match process_analyse_image(&message.payload).await {
                                        Ok(result) => {
                                            info!("Image {} analysis completed successfully", message.payload.image_id);
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
                                            error!("Image {} analysis failed: {}", message.payload.image_id, e);

                                            let failure_payload = serde_json::json!({
                                                "task_id": message.payload.task_id,
                                                "image_id": message.payload.image_id,
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

async fn process_analyse_image(
    payload: &AnalyseImagePayload,
) -> Result<serde_json::Value> {
    let huggingface_api_key = std::env::var("HUGGINGFACE_API_KEY")
        .context("HUGGINGFACE_API_KEY environment variable not set")?;
    
    let s3_client = Arc::new(S3Client::from_env().await
        .context("Failed to create S3 client")?);

    // Create temporary directory for this image
    let temp_dir = std::env::temp_dir().join(payload.image_id.to_string());
    tokio::fs::create_dir_all(&temp_dir).await
        .context("Failed to create temporary directory")?;
    
    let result = process_analyse_image_inner(payload, &s3_client, &temp_dir, huggingface_api_key).await;
    
    // Cleanup temporary directory
    if let Err(e) = tokio::fs::remove_dir_all(&temp_dir).await {
        error!("Failed to clean up temporary directory for image {}: {}", payload.image_id, e);
    }
    
    result
}

async fn process_analyse_image_inner(
    payload: &AnalyseImagePayload,
    s3_client: &Arc<S3Client>,
    temp_dir: &std::path::PathBuf,
    huggingface_api_key: String,
) -> Result<serde_json::Value> {
    // Download image from S3
    let image_filename = format!("image_{}.tmp", payload.image_id);
    let local_image_path = temp_dir.join(&image_filename);
    
    info!("Downloading image from S3: {}/{}", payload.s3_bucket, payload.s3_key);
    
    s3_client.download_file(&payload.s3_key, local_image_path.to_str().unwrap()).await
        .context("Failed to download image from S3")?;

    // Verify file exists and has content
    let file_metadata = tokio::fs::metadata(&local_image_path).await
        .context("Failed to read downloaded image metadata")?;
    
    if file_metadata.len() == 0 {
        return Err(anyhow::anyhow!("Downloaded image file is empty"));
    }

    info!("Image downloaded successfully, size: {} bytes", file_metadata.len());

    // Read image file
    let image_data = tokio::fs::read(&local_image_path).await
        .context("Failed to read image file")?;

    // Analyze image with Hugging Face API
    info!("Analyzing image with Hugging Face model: Organika/sdxl-detector");
    
    let analysis_result = analyze_image_with_huggingface(&huggingface_api_key, &image_data).await
        .context("Failed to analyze image with Hugging Face")?;

    // Parse the result to extract AI detection information
    let (is_ai_generated, confidence) = parse_analysis_result(&analysis_result)?;

    info!(
        "Analysis complete - AI Generated: {}, Confidence: {:.2}%",
        is_ai_generated,
        confidence * 100.0
    );

    Ok(serde_json::json!({
        "task_id": payload.task_id,
        "image_id": payload.image_id,
        "is_ai_generated": is_ai_generated,
        "confidence": confidence,
    }))
}

#[derive(Debug, Deserialize)]
struct HuggingFaceClassificationResult {
    label: String,
    score: f64,
}

async fn analyze_image_with_huggingface(
    api_key: &str,
    image_data: &[u8],
) -> Result<Vec<HuggingFaceClassificationResult>> {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(60))
        .build()
        .context("Failed to create HTTP client")?;

    let response = client
        .post(HUGGINGFACE_API_URL)
        .header("Authorization", format!("Bearer {}", api_key))
        .header("Content-Type", "application/octet-stream")
        .body(image_data.to_vec())
        .send()
        .await
        .context("Failed to send request to Hugging Face API")?;

    let status = response.status();
    
    if !status.is_success() {
        let error_text = response.text().await.unwrap_or_default();
        
        // Handle model loading case
        if status.as_u16() == 503 && error_text.contains("loading") {
            info!("Model is loading, waiting 20 seconds and retrying...");
            tokio::time::sleep(tokio::time::Duration::from_secs(20)).await;
            
            // Retry the request
            let retry_response = client
                .post(HUGGINGFACE_API_URL)
                .header("Authorization", format!("Bearer {}", api_key))
                .header("Content-Type", "application/octet-stream")
                .body(image_data.to_vec())
                .send()
                .await
                .context("Failed to send retry request to Hugging Face API")?;
            
            let retry_status = retry_response.status();
            
            if !retry_status.is_success() {
                let retry_error = retry_response.text().await.unwrap_or_default();
                return Err(anyhow::anyhow!(
                    "Hugging Face API request failed after retry (status {}): {}",
                    retry_status,
                    retry_error
                ));
            }
            
            let results: Vec<HuggingFaceClassificationResult> = retry_response.json().await
                .context("Failed to parse Hugging Face API response after retry")?;
            
            return Ok(results);
        }
        
        return Err(anyhow::anyhow!(
            "Hugging Face API request failed (status {}): {}",
            status,
            error_text
        ));
    }

    let results: Vec<HuggingFaceClassificationResult> = response.json().await
        .context("Failed to parse Hugging Face API response")?;

    Ok(results)
}

fn parse_analysis_result(results: &[HuggingFaceClassificationResult]) -> Result<(bool, f64)> {
    if results.is_empty() {
        return Err(anyhow::anyhow!("No classification results returned from API"));
    }

    // The model returns classifications with labels like "artificial" and "natural"
    // We need to find the label indicating AI generation
    let mut artificial_score = 0.0;
    let mut natural_score = 0.0;

    for result in results {
        let label_lower = result.label.to_lowercase();
        if label_lower.contains("artificial") || label_lower.contains("ai") || label_lower.contains("synthetic") {
            artificial_score = result.score;
        } else if label_lower.contains("natural") || label_lower.contains("real") || label_lower.contains("human") {
            natural_score = result.score;
        }
    }

    // If we have both scores, use artificial score
    // If we only have one, determine based on that
    let (is_ai_generated, confidence) = if artificial_score > 0.0 || natural_score > 0.0 {
        if artificial_score > natural_score {
            (true, artificial_score)
        } else {
            (false, natural_score)
        }
    } else {
        // Fallback: use the first result with highest score
        let best_result = results.iter()
            .max_by(|a, b| a.score.partial_cmp(&b.score).unwrap())
            .context("Failed to find best classification result")?;
        
        let label_lower = best_result.label.to_lowercase();
        let is_ai = label_lower.contains("artificial") 
            || label_lower.contains("ai") 
            || label_lower.contains("synthetic");
        
        (is_ai, best_result.score)
    };

    Ok((is_ai_generated, confidence))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_analysis_result_artificial() {
        let results = vec![
            HuggingFaceClassificationResult {
                label: "artificial".to_string(),
                score: 0.95,
            },
            HuggingFaceClassificationResult {
                label: "natural".to_string(),
                score: 0.05,
            },
        ];

        let (is_ai_generated, confidence) = parse_analysis_result(&results).unwrap();
        assert!(is_ai_generated);
        assert_eq!(confidence, 0.95);
    }

    #[test]
    fn test_parse_analysis_result_natural() {
        let results = vec![
            HuggingFaceClassificationResult {
                label: "natural".to_string(),
                score: 0.92,
            },
            HuggingFaceClassificationResult {
                label: "artificial".to_string(),
                score: 0.08,
            },
        ];

        let (is_ai_generated, confidence) = parse_analysis_result(&results).unwrap();
        assert!(!is_ai_generated);
        assert_eq!(confidence, 0.92);
    }

    #[test]
    fn test_parse_analysis_result_empty() {
        let results = vec![];
        let result = parse_analysis_result(&results);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_analysis_result_single() {
        let results = vec![HuggingFaceClassificationResult {
            label: "ai-generated".to_string(),
            score: 0.88,
        }];

        let (is_ai_generated, confidence) = parse_analysis_result(&results).unwrap();
        assert!(is_ai_generated);
        assert_eq!(confidence, 0.88);
    }
}

