use anyhow::{Context, Result};
use futures::StreamExt;
use lapin::options::BasicAckOptions;
use shared::{GetVideoPayload, RabbitMQClient, RabbitMQConfig, S3Client, TaskMessage, WebhookClient};
use std::sync::Arc;
use tokio::sync::Semaphore;
use tracing::{error, info};
use tokio::io::AsyncWriteExt;

const TASK_TYPE: &str = "get_video";

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG").unwrap_or("info".to_string()),
        )
        .init();

    info!("Starting task-get-video service");

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

    let queue_name = std::env::var("QUEUE_GET_VIDEO").unwrap_or("core.get_video".to_string());
    
    let max_concurrent = std::env::var("MAX_CONCURRENT_TASKS")
        .unwrap_or("10".to_string())
        .parse()
        .unwrap_or(10);

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
                        let data = String::from_utf8_lossy(&delivery.data);
                        info!("Received message: {}", data);

                        match serde_json::from_slice::<TaskMessage<GetVideoPayload>>(&delivery.data) {
                            Ok(message) => {
                                let webhook_client = Arc::clone(&webhook_client);
                                let s3_client = Arc::clone(&s3_client);
                                let semaphore = Arc::clone(&semaphore);
                                
                                tokio::spawn(async move {
                                    let _permit = semaphore.acquire().await.unwrap();
                                    
                                    info!("Processing task: {}", message.task_id);

                                    match process_get_video(&message.payload, &message.task_id, &s3_client).await {
                                        Ok(result) => {
                                            info!("Task {} completed successfully", message.task_id);

                                            if let Err(e) = webhook_client
                                                .send_success(
                                                    &message.webhook_url_success,
                                                    message.task_id,
                                                    TASK_TYPE,
                                                    result,
                                                )
                                                .await
                                            {
                                                error!("Failed to send success webhook: {}", e);
                                            }
                                        }
                                        Err(e) => {
                                            error!("Task {} failed: {}", message.task_id, e);

                                            if let Err(webhook_err) = webhook_client
                                                .send_error_with_stream(
                                                    &message.webhook_url_failure,
                                                    message.task_id,
                                                    TASK_TYPE,
                                                    &message.payload.stream_id,
                                                )
                                                .await
                                            {
                                                error!("Failed to send error webhook: {}", webhook_err);
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

async fn process_get_video(
    payload: &GetVideoPayload,
    task_id: &uuid::Uuid,
    s3_client: &S3Client,
) -> Result<serde_json::Value> {
    info!("Downloading video from: {}", payload.url);

    // Extract original filename from URL
    let original_file_name = payload.url
        .split('/')
        .last()
        .and_then(|s| s.split('?').next()) // Remove query parameters
        .unwrap_or("video.mp4")
        .to_string();
    
    // Generate unique filename
    let file_extension = original_file_name
        .split('.')
        .last()
        .unwrap_or("mp4");
    let file_name = format!("{}.{}", task_id, file_extension);
    
    // Create temporary directory if it doesn't exist
    let temp_dir = "/tmp/videos";
    tokio::fs::create_dir_all(temp_dir).await?;
    let temp_file_path = format!("{}/{}", temp_dir, file_name);

    // Download video
    info!("Downloading video to: {}", temp_file_path);
    let response = reqwest::get(&payload.url)
        .await
        .context("Failed to download video")?;

    if !response.status().is_success() {
        return Err(anyhow::anyhow!(
            "Failed to download video: HTTP {}",
            response.status()
        ));
    }

    // Get content length and mime type
    let content_length = response.content_length().unwrap_or(0);
    let mime_type = response
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("video/mp4")
        .to_string();

    // Save to temporary file
    let bytes = response.bytes().await.context("Failed to read video bytes")?;
    let mut file = tokio::fs::File::create(&temp_file_path)
        .await
        .context("Failed to create temporary file")?;
    file.write_all(&bytes).await.context("Failed to write video file")?;
    file.flush().await?;

    let actual_size = bytes.len() as u64;
    info!("Video downloaded: {} bytes", actual_size);

    // Upload to S3: stream_id/file_name
    let s3_key = format!("{}/{}", payload.stream_id, file_name);
    info!("Uploading to S3: {}", s3_key);
    
    s3_client
        .upload_file(&temp_file_path, &s3_key)
        .await
        .context("Failed to upload video to S3")?;

    // Clean up temporary file
    if let Err(e) = tokio::fs::remove_file(&temp_file_path).await {
        error!("Failed to remove temporary file {}: {}", temp_file_path, e);
    }

    info!("Video processed successfully: {}", file_name);

    Ok(serde_json::json!({
        "file_name": file_name,
        "original_file_name": original_file_name,
        "mime_type": mime_type,
        "size": if content_length > 0 { content_length } else { actual_size },
        "stream_id": payload.stream_id,
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
