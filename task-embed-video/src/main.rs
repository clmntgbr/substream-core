use anyhow::{Context, Result};
use futures::StreamExt;
use lapin::options::BasicAckOptions;
use shared::{EmbedSubtitlePayload, RabbitMQClient, RabbitMQConfig, S3Client, TaskMessage, WebhookClient};
use std::sync::Arc;
use tokio::sync::Semaphore;
use tracing::{error, info};

const TASK_TYPE: &str = "embed_video";

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG").unwrap_or("info".to_string()),
        )
        .init();

    info!("Starting task-embed-video service");

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

    let queue_name = std::env::var("QUEUE_EMBED_VIDEO").unwrap_or("core.embed_video".to_string());
    
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
                        match serde_json::from_slice::<TaskMessage<EmbedSubtitlePayload>>(&delivery.data) {
                            Ok(message) => {
                                let webhook_client = Arc::clone(&webhook_client);
                                let s3_client = Arc::clone(&s3_client);
                                let semaphore = Arc::clone(&semaphore);
                                
                                tokio::spawn(async move {
                                    let _permit = semaphore.acquire().await.unwrap();
                                    
                                    info!("Processing stream: {}", message.payload.stream_id);
                                    info!("Task ID: {}", message.task_id);

                                    match process_embed_subtitle(&message.payload, &message.task_id, &s3_client).await {
                                        Ok(result) => {
                                            info!("Stream {} completed successfully", message.payload.stream_id);

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
                                            error!("Stream {} failed: {}", message.payload.stream_id, e);

                                            if let Err(webhook_err) = webhook_client
                                                .send_error(
                                                    &message.webhook_url_failure,
                                                    message.task_id,
                                                    TASK_TYPE,
                                                    &message.payload.stream_id,
                                                )
                                                .await
                                            {
                                                error!("Failed to send failure webhook: {}", webhook_err);
                                            }
                                        }
                                    }
                                });
                            }
                            Err(e) => {
                                error!("Failed to deserialize message: {}", e);
                            }
                        }

                        if let Err(e) = delivery.ack(BasicAckOptions::default()).await {
                            error!("Failed to ack message: {}", e);
                        }
                    }
                    Err(e) => {
                        error!("Failed to receive message: {}", e);
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

async fn process_embed_subtitle(
    payload: &EmbedSubtitlePayload, 
    _task_id: &uuid::Uuid,
    s3_client: &S3Client,
) -> Result<serde_json::Value> {
    info!("Starting video embed subtitle process for stream: {}", payload.stream_id);
    
    let temp_dir = std::env::temp_dir().join(format!("embed_{}", payload.stream_id));
    tokio::fs::create_dir_all(&temp_dir)
        .await
        .context("Failed to create temporary directory")?;
    
    let resized_file_path = temp_dir.join(&payload.resize_file_name);
    let subtitle_file_path = temp_dir.join(&payload.subtitle_ass_file_name);
    let embed_file_name = format!("{}_embedded.mp4", payload.stream_id);
    let embed_file_path = temp_dir.join(&embed_file_name);
    
    let s3_resized_path = format!("{}/{}", payload.stream_id, payload.resize_file_name);
    info!("Downloading resized video: {}", s3_resized_path);
    s3_client
        .download_file(&s3_resized_path, &resized_file_path.to_string_lossy())
        .await
        .context("Failed to download resized video from S3")?;
    
    let s3_subtitle_path = format!("{}/subtitles/{}", payload.stream_id, payload.subtitle_ass_file_name);
    info!("Downloading subtitle file: {}", s3_subtitle_path);
    s3_client
        .download_file(&s3_subtitle_path, &subtitle_file_path.to_string_lossy())
        .await
        .context("Failed to download subtitle file from S3")?;
    
    info!("Processing video embed subtitle for stream: {}", payload.stream_id);
    tokio::fs::copy(&resized_file_path, &embed_file_path)
        .await
        .context("Failed to create embedded video file")?;
    
    info!("Video embed subtitle processing completed for stream: {}", payload.stream_id);
    
    let s3_embed_path = format!("{}/{}", payload.stream_id, embed_file_name);
    info!("Uploading embedded video to S3: {}", s3_embed_path);
    s3_client
        .upload_file(&embed_file_path.to_string_lossy(), &s3_embed_path)
        .await
        .context("Failed to upload embedded video to S3")?;
    
    info!("Cleaning up temporary files for stream: {}", payload.stream_id);
    if let Err(e) = tokio::fs::remove_dir_all(&temp_dir).await {
        error!("Failed to clean up temporary directory: {}", e);
    }
    
    let result = serde_json::json!({
        "stream_id": payload.stream_id,
        "embed_file_name": embed_file_name,
    });

    info!("Video embed subtitle process completed successfully for stream: {}", payload.stream_id);
    Ok(result)
}

async fn setup_shutdown_handler() {
    use tokio::signal;
    
    #[cfg(unix)]
    {
        let mut sigterm = signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("Failed to create SIGTERM handler");
        let mut sigint = signal::unix::signal(signal::unix::SignalKind::interrupt())
            .expect("Failed to create SIGINT handler");
        
        tokio::select! {
            _ = sigterm.recv() => {
                info!("Received SIGTERM");
            }
            _ = sigint.recv() => {
                info!("Received SIGINT");
            }
        }
    }
    
    #[cfg(windows)]
    {
        let mut ctrl_c = signal::windows::ctrl_c()
            .expect("Failed to create Ctrl+C handler");
        
        ctrl_c.recv().await;
        info!("Received Ctrl+C");
    }
}
