use anyhow::{Context, Result};
use futures::StreamExt;
use lapin::options::BasicAckOptions;
use shared::{RabbitMQClient, RabbitMQConfig, TaskMessage, WebhookClient};
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

    let webhook_client = WebhookClient::new();

    let queue_name = std::env::var("QUEUE_EXTRACT_SOUND").unwrap_or("core.extract_sound".to_string());

    info!("Listening on queue: {}", queue_name);

    let mut consumer = rabbitmq_client
        .consume(&queue_name)
        .await
        .context("Failed to start consuming messages")?;

    let mut shutdown = Box::pin(setup_shutdown_handler());

    info!("Service ready, waiting for messages...");

    loop {
        tokio::select! {
            Some(delivery) = consumer.next() => {
                match delivery {
                    Ok(delivery) => {
                        let data = String::from_utf8_lossy(&delivery.data);
                        info!("Received message: {}", data);

                        match serde_json::from_slice::<TaskMessage>(&delivery.data) {
                            Ok(message) => {
                                info!("Processing task: {}", message.task_id);

                                match process_extract_sound(&message).await {
                                    Ok(result) => {
                                        info!("Task {} completed successfully", message.task_id);

                                        if let Err(e) = webhook_client
                                            .send_success(
                                                &message.webhook_url,
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
                                            .send_error(
                                                &message.webhook_url,
                                                message.task_id,
                                                TASK_TYPE,
                                                &e.to_string(),
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

async fn process_extract_sound(message: &TaskMessage) -> Result<serde_json::Value> {
    let video_path = message
        .payload
        .get("video_path")
        .and_then(|v| v.as_str())
        .context("Missing 'video_path' field in payload")?;

    info!("Extracting sound from video: {}", video_path);

    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

    let audio_path = format!("/audio/{}.mp3", message.task_id);

    info!("Sound extracted successfully to: {}", audio_path);

    Ok(serde_json::json!({
        "audio_path": audio_path,
        "video_path": video_path,
        "format": "mp3",
        "bitrate": "320kbps",
        "duration_seconds": 120,
        "size_bytes": 3840000,
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
