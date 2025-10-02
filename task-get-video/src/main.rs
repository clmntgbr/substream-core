use anyhow::{Context, Result};
use futures::StreamExt;
use lapin::options::BasicAckOptions;
use shared::{GetVideoPayload, RabbitMQClient, RabbitMQConfig, S3Client, TaskMessage, WebhookClient};
use std::sync::Arc;
use tokio::sync::Semaphore;
use tracing::{error, info};
use tokio::process::Command;

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

    let temp_dir = "/tmp/videos";
    tokio::fs::create_dir_all(temp_dir).await?;
    
    let output_template = format!("{}/{}.%(ext)s", temp_dir, task_id);

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

    info!("Video metadata retrieved: {}", metadata.get("title").and_then(|t| t.as_str()).unwrap_or("Unknown"));

    let download_output = Command::new("yt-dlp")
        .arg("-f")
        .arg("bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best")
        .arg("--no-playlist")
        .arg("--merge-output-format")
        .arg("mp4")
        .arg("-o")
        .arg(&output_template)
        .arg(&payload.url)
        .output()
        .await
        .context("Failed to execute yt-dlp for download")?;

    if !download_output.status.success() {
        let error = String::from_utf8_lossy(&download_output.stderr);
        return Err(anyhow::anyhow!("Failed to download video: {}", error));
    }

    let file_name = format!("{}.mp4", task_id);
    let temp_file_path = format!("{}/{}", temp_dir, file_name);

    let file_metadata = tokio::fs::metadata(&temp_file_path)
        .await
        .context("Failed to read downloaded file metadata")?;
    
    let file_size = file_metadata.len();
    info!("Video downloaded: {} bytes", file_size);

    let s3_key = format!("{}/{}", payload.stream_id, file_name);
    info!("Uploading to S3: {}", s3_key);
    
    s3_client
        .upload_file(&temp_file_path, &s3_key)
        .await
        .context("Failed to upload video to S3")?;

    if let Err(e) = tokio::fs::remove_file(&temp_file_path).await {
        error!("Failed to remove temporary file {}: {}", temp_file_path, e);
    }

    info!("Video processed successfully: {}", file_name);

    Ok(serde_json::json!({
        "file_name": file_name,
        "original_file_name": metadata.get("title").and_then(|t| t.as_str()).unwrap_or("video"),
        "title": metadata.get("title"),
        "description": metadata.get("description"),
        "duration": metadata.get("duration"),
        "uploader": metadata.get("uploader"),
        "upload_date": metadata.get("upload_date"),
        "view_count": metadata.get("view_count"),
        "like_count": metadata.get("like_count"),
        "thumbnail": metadata.get("thumbnail"),
        "mime_type": "video/mp4",
        "size": file_size,
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
