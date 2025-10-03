use anyhow::{Context, Result};
use futures::StreamExt;
use lapin::options::BasicAckOptions;
use shared::{ExtractSoundPayload, RabbitMQClient, RabbitMQConfig, S3Client, TaskMessage, WebhookClient};
use std::sync::Arc;
use tokio::sync::Semaphore;
use tracing::{error, info};
use tokio::process::Command;

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
    let s3_client = Arc::new(
        S3Client::from_env()
            .await
            .context("Failed to create S3 client")?
    );

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
                                let s3_client = Arc::clone(&s3_client);
                                let semaphore = Arc::clone(&semaphore);
                                
                                tokio::spawn(async move {
                                    let _permit = semaphore.acquire().await.unwrap();
                                    
                                    info!("Processing stream: {}", message.payload.stream_id);

                                    match process_extract_sound(&message.payload, &message.task_id, &s3_client).await {
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

async fn process_extract_sound(
    payload: &ExtractSoundPayload, 
    task_id: &uuid::Uuid,
    s3_client: &S3Client,
) -> Result<serde_json::Value> {
    info!("Extracting sound from video: {}", payload.file_name);

    let temp_dir = "/tmp/audio";
    tokio::fs::create_dir_all(temp_dir).await?;

    let video_s3_key = format!("{}/{}", payload.stream_id, payload.file_name);
    let temp_video_path = format!("{}/{}", temp_dir, payload.file_name);

    info!("Downloading video from S3: {}", video_s3_key);
    s3_client
        .download_file(&video_s3_key, &temp_video_path)
        .await
        .context("Failed to download video from S3")?;

    let audio_output_pattern = format!("{}/{}_%03d.wav", temp_dir, payload.stream_id);

    info!("Extracting audio and splitting into 5-minute segments");
    let output = Command::new("ffmpeg")
        .arg("-i")
        .arg(&temp_video_path)
        .arg("-f")
        .arg("segment")
        .arg("-segment_time")
        .arg("300")
        .arg("-segment_start_number")
        .arg("1")
        .arg("-c:a")
        .arg("pcm_s16le")
        .arg("-ar")
        .arg("16000")
        .arg("-ac")
        .arg("1")
        .arg(&audio_output_pattern)
        .output()
        .await
        .context("Failed to execute ffmpeg")?;

    if !output.status.success() {
        let error = String::from_utf8_lossy(&output.stderr);
        if let Err(e) = tokio::fs::remove_file(&temp_video_path).await {
            error!("Failed to remove temp video: {}", e);
        }
        return Err(anyhow::anyhow!("ffmpeg failed: {}", error));
    }

    if let Err(e) = tokio::fs::remove_file(&temp_video_path).await {
        error!("Failed to remove temp video: {}", e);
    }

    let mut audio_files = Vec::new();
    let mut segment_index = 1;

    loop {
        let segment_file_name = format!("{}_{:03}.wav", payload.stream_id, segment_index);
        let segment_path = format!("{}/{}", temp_dir, segment_file_name);

        if !tokio::fs::try_exists(&segment_path).await.unwrap_or(false) {
            break;
        }

        let s3_key = format!("{}/{}", payload.stream_id, segment_file_name);
        info!("Uploading audio segment: {}", segment_file_name);

        let upload_result = s3_client
            .upload_file(&segment_path, &s3_key)
            .await;

        if let Err(e) = tokio::fs::remove_file(&segment_path).await {
            error!("Failed to remove temp audio segment: {}", e);
        }

        upload_result.context("Failed to upload audio segment to S3")?;

        audio_files.push(segment_file_name);
        segment_index += 1;
    }

    info!("Extracted and uploaded {} audio segments", audio_files.len());

    Ok(serde_json::json!({
        "stream_id": payload.stream_id,
        "audio_files": audio_files,
        "segment_count": audio_files.len(),
        "segment_duration_seconds": 300,
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
