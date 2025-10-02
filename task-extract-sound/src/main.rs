use anyhow::{Context, Result};
use futures::StreamExt;
use lapin::options::BasicAckOptions;
use shared::{ExtractSoundPayload, RabbitMQClient, RabbitMQConfig, TaskMessage, WebhookClient};
use std::sync::Arc;
use tokio::sync::Semaphore;
use tracing::{error, info};

const TASK_TYPE: &str = "extract_sound";

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG").unwrap_or("info".to_string()),
        )
        .init();

    info!("Starting task-extract-sound service");

    dotenvy::dotenv().ok();

    let config = RabbitMQConfig::from_env();
    let rabbitmq_client = RabbitMQClient::new(config)
        .await
        .context("Failed to create RabbitMQ client")?;

    let webhook_client = Arc::new(WebhookClient::new());

    let queue_name = std::env::var("QUEUE_EXTRACT_SOUND").unwrap_or("core.extract_sound".to_string());
    
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

                        match serde_json::from_slice::<TaskMessage<ExtractSoundPayload>>(&delivery.data) {
                            Ok(message) => {
                                let webhook_client = Arc::clone(&webhook_client);
                                let semaphore = Arc::clone(&semaphore);
                                
                                tokio::spawn(async move {
                                    let _permit = semaphore.acquire().await.unwrap();
                                    
                                    info!("Processing task: {}", message.task_id);

                                    match process_extract_sound(&message.payload, &message.task_id).await {
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

async fn process_extract_sound(payload: &ExtractSoundPayload, task_id: &uuid::Uuid) -> Result<serde_json::Value> {
    info!("Extracting sound from video: {}", payload.file_name);

    // TODO: Real extraction implementation
    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

    let audio_file_name = format!("{}.mp3", task_id);

    info!("Sound extracted successfully to: {}", audio_file_name);

    Ok(serde_json::json!({
        "stream_id": payload.stream_id,
        "audio_files": [audio_file_name],
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
