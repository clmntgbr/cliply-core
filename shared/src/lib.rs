use anyhow::{Context, Result};
use lapin::{
    options::*, types::FieldTable, Channel, Connection, ConnectionProperties, Consumer,
};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tracing::{error, info, warn};
use uuid::Uuid;
use aws_sdk_s3::Client as AwsS3Client;
use aws_sdk_s3::config::{Credentials, Region, BehaviorVersion};
use aws_sdk_s3::primitives::ByteStream;
use std::path::Path;

/// Common message structure for all workers (generic over payload type)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerMessage<T> {
    pub payload: T,
    pub webhook_url_success: String,
    pub webhook_url_failure: String,
}

/// Payload for download video
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadVideoPayload {
    pub video_id: Uuid,
    pub url: String,
    pub clip_id: Uuid,
}

/// Payload for extract sound
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtractSoundPayload {
    pub clip_id: Uuid,
    pub name: String,
    pub video_id: Uuid,
}

#[derive(Debug, Clone)]
pub struct RabbitMQConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub vhost: String,
    pub max_retries: u32,
    pub retry_delay: Duration,
}

impl Default for RabbitMQConfig {
    fn default() -> Self {
        Self {
            host: "localhost".to_string(),
            port: 5672,
            user: "guest".to_string(),
            password: "guest".to_string(),
            vhost: "/".to_string(),
            max_retries: 3,
            retry_delay: Duration::from_secs(5),
        }
    }
}

impl RabbitMQConfig {
    pub fn from_env() -> Self {
        Self {
            host: std::env::var("RABBITMQ_HOST").unwrap_or("localhost".to_string()),
            port: std::env::var("RABBITMQ_PORT")
                .unwrap_or("5672".to_string())
                .parse()
                .unwrap_or(5672),
            user: std::env::var("RABBITMQ_USER").unwrap_or("guest".to_string()),
            password: std::env::var("RABBITMQ_PASSWORD").unwrap_or("guest".to_string()),
            vhost: std::env::var("RABBITMQ_VHOST").unwrap_or("/".to_string()),
            max_retries: std::env::var("MAX_RETRIES")
                .unwrap_or("3".to_string())
                .parse()
                .unwrap_or(3),
            retry_delay: Duration::from_secs(
                std::env::var("RETRY_DELAY_SECONDS")
                    .unwrap_or("5".to_string())
                    .parse()
                    .unwrap_or(5),
            ),
        }
    }

    pub fn connection_url(&self) -> String {
        format!(
            "amqp://{}:{}@{}:{}/{}",
            self.user, self.password, self.host, self.port, self.vhost
        )
    }
}

/// RabbitMQ client for queue operations
pub struct RabbitMQClient {
    _connection: Connection,
    channel: Channel,
    _config: RabbitMQConfig,
}

impl RabbitMQClient {
    /// Create a new RabbitMQ client with retry logic
    pub async fn new(config: RabbitMQConfig) -> Result<Self> {
        let mut last_error = None;

        for attempt in 1..=config.max_retries {
            info!(
                "Attempting to connect to RabbitMQ (attempt {}/{})",
                attempt, config.max_retries
            );

            match Self::connect(&config).await {
                Ok((connection, channel)) => {
                    info!("Successfully connected to RabbitMQ");
                    return Ok(Self {
                        _connection: connection,
                        channel,
                        _config: config,
                    });
                }
                Err(e) => {
                    warn!(
                        "Failed to connect to RabbitMQ (attempt {}/{}): {}",
                        attempt, config.max_retries, e
                    );
                    last_error = Some(e);
                    if attempt < config.max_retries {
                        tokio::time::sleep(config.retry_delay).await;
                    }
                }
            }
        }

        Err(last_error.unwrap_or_else(|| anyhow::anyhow!("Failed to connect to RabbitMQ")))
    }

    async fn connect(config: &RabbitMQConfig) -> Result<(Connection, Channel)> {
        let connection = Connection::connect(
            &config.connection_url(),
            ConnectionProperties::default(),
        )
        .await
        .context("Failed to create RabbitMQ connection")?;

        let channel = connection
            .create_channel()
            .await
            .context("Failed to create channel")?;

        Ok((connection, channel))
    }

    /// Declare a queue and start consuming messages
    pub async fn consume(&self, queue_name: &str) -> Result<Consumer> {
        self.channel
            .queue_declare(
                queue_name,
                QueueDeclareOptions {
                    durable: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await
            .context("Failed to declare queue")?;

        info!("Queue '{}' declared successfully", queue_name);

        let consumer = self
            .channel
            .basic_consume(
                queue_name,
                "consumer",
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await
            .context("Failed to start consuming")?;

        info!("Started consuming from queue '{}'", queue_name);

        Ok(consumer)
    }

    /// Get the channel reference
    pub fn channel(&self) -> &Channel {
        &self.channel
    }
}

/// HTTP client for sending webhook notifications
pub struct WebhookClient {
    #[allow(dead_code)]
    client: reqwest::Client,
    auth_token: Option<String>,
}

impl WebhookClient {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
                .unwrap_or_default(),
            auth_token: std::env::var("CORE_TOKEN").ok(),
        }
    }

    /// Send a webhook notification with JSON payload
    pub async fn send(&self, url: &str, payload: &serde_json::Value) -> Result<()> {
        info!("=== WEBHOOK DEBUG MODE ===");
        info!("Would send webhook to: {}", url);
        info!("Payload (pretty): \n{}", serde_json::to_string_pretty(payload).unwrap_or_default());
        
        if let Some(token) = &self.auth_token {
            info!("Auth token present: {}...", &token.chars().take(10).collect::<String>());
        } else {
            info!("No auth token configured");
        }
        
        info!("=== END WEBHOOK DEBUG ===\n");

        // Comment out actual HTTP sending for testing
        /*
        let mut request = self.client.post(url)
            .header("Content-Type", "application/json")
            .json(payload);
        
        if let Some(token) = &self.auth_token {
            request = request.header("X-Authentication-Token", token);
        }

        match request.send().await {
            Ok(resp) => {
                if resp.status().is_success() {
                    info!("Webhook sent successfully to {}", url);
                    Ok(())
                } else {
                    let status = resp.status();
                    let body = resp.text().await.unwrap_or_default();
                    error!("Webhook failed with status {}: {}", status, body);
                    Err(anyhow::anyhow!("Webhook request failed with status {}", status))
                }
            }
            Err(e) => {
                error!("Failed to send webhook to {}: {}", url, e);
                Err(anyhow::anyhow!("Failed to send webhook: {}", e))
            }
        }
        */
        
        Ok(())
    }

    /// Send a success webhook
    pub async fn send_success(
        &self,
        url: &str,
        _event_type: &str,
        result: serde_json::Value,
    ) -> Result<()> {
        // Send success payload as-is, without adding 'name' field
        self.send(url, &result).await
    }

    /// Send a failure webhook
    pub async fn send_failure(
        &self,
        url: &str,
        _event_type: &str,
        result: serde_json::Value,
    ) -> Result<()> {
        // Send failure payload as-is, without adding 'name' field
        self.send(url, &result).await
    }
}

impl Default for WebhookClient {
    fn default() -> Self {
        Self::new()
    }
}

/// S3 client for file operations
pub struct S3Client {
    client: AwsS3Client,
    bucket_name: String,
}

impl S3Client {
    /// Create a new S3 client from environment variables
    pub async fn from_env() -> Result<Self> {
        let access_key = std::env::var("S3_ACCESS_KEY")
            .context("S3_ACCESS_KEY environment variable not set")?;
        let secret_key = std::env::var("S3_SECRET_KEY")
            .context("S3_SECRET_KEY environment variable not set")?;
        let endpoint = std::env::var("S3_ENDPOINT")
            .context("S3_ENDPOINT environment variable not set")?;
        let region = std::env::var("S3_REGION").unwrap_or("us-east-1".to_string());
        let bucket_name = std::env::var("S3_BUCKET_NAME")
            .context("S3_BUCKET_NAME environment variable not set")?;

        Self::new(&access_key, &secret_key, &endpoint, &region, &bucket_name).await
    }

    /// Create a new S3 client with explicit configuration
    pub async fn new(
        access_key: &str,
        secret_key: &str,
        endpoint: &str,
        region: &str,
        bucket_name: &str,
    ) -> Result<Self> {
        let credentials = Credentials::new(
            access_key,
            secret_key,
            None,
            None,
            "custom",
        );

        let config = aws_sdk_s3::Config::builder()
            .credentials_provider(credentials)
            .endpoint_url(endpoint)
            .region(Region::new(region.to_string()))
            .force_path_style(true) // Required for MinIO and similar S3-compatible services
            .behavior_version(BehaviorVersion::latest())
            .build();

        let client = AwsS3Client::from_conf(config);

        Ok(Self {
            client,
            bucket_name: bucket_name.to_string(),
        })
    }

    /// Upload a file to S3
    pub async fn upload_file(&self, file_path: &str, object_name: &str) -> Result<()> {
        use tokio::io::AsyncReadExt;
    
        let file_size = tokio::fs::metadata(file_path)
            .await
            .context("Failed to get file metadata")?
            .len();
    
        info!("Uploading S3 file: {} ({} bytes)", file_path, file_size);
    
        const PART_SIZE: usize = 10 * 1024 * 1024; // 10 MB
        
        if file_size <= PART_SIZE as u64 {
            let body = ByteStream::from_path(Path::new(file_path))
                .await
                .context("Failed to read file for upload")?;
    
            self.client
                .put_object()
                .bucket(&self.bucket_name)
                .key(object_name)
                .body(body)
                .send()
                .await
                .context("Failed to upload file")?;
    
            info!("Successfully uploaded file {} to S3", file_path);
            return Ok(());
        }
    
        // Multipart upload pour les gros fichiers
        info!(
            "Using multipart upload (file size: {:.2} MB, part size: {} MB)",
            file_size as f64 / 1024.0 / 1024.0,
            PART_SIZE / 1024 / 1024
        );
    
        let multipart_upload = self.client
            .create_multipart_upload()
            .bucket(&self.bucket_name)
            .key(object_name)
            .send()
            .await
            .context("Failed to create multipart upload")?;
    
        let upload_id = multipart_upload
            .upload_id()
            .context("No upload ID")?
            .to_string();
    
        let mut file = tokio::fs::File::open(file_path)
            .await
            .context("Failed to open file")?;
    
        let mut part_number = 1i32;
        let mut completed_parts = Vec::new();
        let mut total_uploaded = 0u64;
        let mut last_progress_report = 0;
    
        loop {
            let mut buffer = Vec::with_capacity(PART_SIZE);
            buffer.resize(PART_SIZE, 0u8);
            
            let mut total_read = 0;
            
            while total_read < PART_SIZE {
                let bytes_read = file.read(&mut buffer[total_read..])
                    .await
                    .context("Failed to read file chunk")?;
    
                if bytes_read == 0 {
                    break;
                }
    
                total_read += bytes_read;
            }
    
            if total_read == 0 {
                break;
            }
    
            buffer.truncate(total_read);
    
            total_uploaded += total_read as u64;
            let progress = ((total_uploaded as f64 / file_size as f64) * 100.0) as u32;
            
            if progress >= last_progress_report + 20 || total_uploaded == file_size {
                info!(
                    "Upload progress: {}% ({:.2} MB / {:.2} MB)",
                    progress,
                    total_uploaded as f64 / 1024.0 / 1024.0,
                    file_size as f64 / 1024.0 / 1024.0
                );
                last_progress_report = progress;
            }
            
            let body = ByteStream::from(buffer);
    
            match self.client
                .upload_part()
                .bucket(&self.bucket_name)
                .key(object_name)
                .upload_id(&upload_id)
                .part_number(part_number)
                .body(body)
                .send()
                .await
            {
                Ok(output) => {
                    let e_tag = output.e_tag()
                        .context(format!("No ETag for part {}", part_number))?
                        .to_string();
    
                    completed_parts.push(
                        aws_sdk_s3::types::CompletedPart::builder()
                            .part_number(part_number)
                            .e_tag(e_tag)
                            .build()
                    );
                    
                    part_number += 1;
                }
                Err(e) => {
                    error!("Failed to upload part {}: {:?}", part_number, e);
                    
                    // Annuler l'upload multipart en cas d'erreur
                    if let Err(abort_err) = self.client
                        .abort_multipart_upload()
                        .bucket(&self.bucket_name)
                        .key(object_name)
                        .upload_id(&upload_id)
                        .send()
                        .await
                    {
                        error!("Failed to abort multipart upload: {:?}", abort_err);
                    }
                    
                    return Err(anyhow::anyhow!("Failed to upload part {}: {:?}", part_number, e));
                }
            }
        }
    
        if completed_parts.is_empty() {
            return Err(anyhow::anyhow!("No parts were uploaded"));
        }
    
        let completed = aws_sdk_s3::types::CompletedMultipartUpload::builder()
            .set_parts(Some(completed_parts))
            .build();
    
        self.client
            .complete_multipart_upload()
            .bucket(&self.bucket_name)
            .key(object_name)
            .upload_id(&upload_id)
            .multipart_upload(completed)
            .send()
            .await
            .context("Failed to complete multipart upload")?;
    
        info!(
            "Successfully uploaded file {} to S3 (multipart, {} parts, {:.2} MB total)",
            file_path,
            part_number - 1,
            total_uploaded as f64 / 1024.0 / 1024.0
        );
        
        Ok(())
    }

    /// Download a file from S3
    pub async fn download_file(&self, object_name: &str, file_path: &str) -> Result<()> {
        info!("Downloading S3 file: {}", file_path);

        match self.client
            .get_object()
            .bucket(&self.bucket_name)
            .key(object_name)
            .send()
            .await
        {
            Ok(output) => {
                let data = output.body.collect().await
                    .context("Failed to collect file data")?;
                
                tokio::fs::write(file_path, data.into_bytes())
                    .await
                    .context("Failed to write file")?;

                info!("Successfully downloaded file {} from S3", file_path);
                Ok(())
            }
            Err(e) => {
                error!("Error downloading S3 file {} ({:?})", file_path, e);
                Err(anyhow::anyhow!("Failed to download file: {:?}", e))
            }
        }
    }

    /// Delete a file from S3
    pub async fn delete_file(&self, object_name: &str) -> Result<()> {
        info!("Deleting S3 file: {}", object_name);

        match self.client
            .delete_object()
            .bucket(&self.bucket_name)
            .key(object_name)
            .send()
            .await
        {
            Ok(_) => {
                info!("Successfully deleted file {} from S3", object_name);
                Ok(())
            }
            Err(e) => {
                error!("Error deleting S3 file {} ({:?})", object_name, e);
                Err(anyhow::anyhow!("Failed to delete file: {:?}", e))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rabbitmq_config_connection_url() {
        let config = RabbitMQConfig {
            host: "localhost".to_string(),
            port: 5672,
            user: "admin".to_string(),
            password: "secret".to_string(),
            vhost: "/test".to_string(),
            max_retries: 3,
            retry_delay: Duration::from_secs(5),
        };

        assert_eq!(
            config.connection_url(),
            "amqp://admin:secret@localhost:5672//test"
        );
    }

    #[test]
    fn test_worker_message_serialization() {
        let msg = WorkerMessage {
            payload: serde_json::json!({"url": "https://example.com/video.mp4"}),
            webhook_url_success: "https://example.com/webhook".to_string(),
            webhook_url_failure: "https://example.com/webhook".to_string(),
        };

        let json = serde_json::to_string(&msg).unwrap();
        let deserialized: WorkerMessage<serde_json::Value> = serde_json::from_str(&json).unwrap();

        assert_eq!(msg.webhook_url_success, deserialized.webhook_url_success);
        assert_eq!(msg.webhook_url_failure, deserialized.webhook_url_failure);
    }
}

