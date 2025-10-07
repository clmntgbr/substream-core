use anyhow::{Context, Result};
use futures::StreamExt;
use lapin::options::BasicAckOptions;
use shared::{ExtractSoundPayload, RabbitMQClient, RabbitMQConfig, S3Client, TaskMessage, WebhookClient};
use std::sync::Arc;
use tokio::sync::Semaphore;
use tracing::{error, info};
use tokio::process::Command;

const TASK_TYPE: &str = "extract_sound";
const DEFAULT_MAX_CONCURRENT_TASKS: usize = 10;
const AUDIO_SEGMENT_DURATION_SECONDS: u32 = 300;
const AUDIO_SAMPLE_RATE: u32 = 16000;
const AUDIO_CHANNELS: u32 = 1;


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
        .unwrap_or(DEFAULT_MAX_CONCURRENT_TASKS.to_string())
        .parse()
        .unwrap_or(DEFAULT_MAX_CONCURRENT_TASKS);

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
                        match serde_json::from_slice::<TaskMessage<ExtractSoundPayload>>(&delivery.data) {
                            Ok(message) => {
                                let webhook_client = Arc::clone(&webhook_client);
                                let s3_client = Arc::clone(&s3_client);
                                let semaphore = Arc::clone(&semaphore);
                                
                                tokio::spawn(async move {
                                    let _permit = semaphore.acquire().await.unwrap();
                                    
                                    info!("Processing stream: {}", message.payload.stream_id);
                                    info!("Task ID: {}", message.payload.task_id);

                                    match process_extract_sound(&message.payload, &message.payload.task_id, &s3_client).await {
                                        Ok(result) => {
                                            info!("Stream {} completed successfully", message.payload.stream_id);

                                            if let Err(e) = webhook_client
                                                .send_success(
                                                    &message.webhook_url_success,
                                                    message.payload.task_id,
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
                                                    message.payload.task_id,
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
    _task_id: &uuid::Uuid,
    s3_client: &S3Client,
) -> Result<serde_json::Value> {
    let start_time = std::time::Instant::now();
    
    let temp_dir = std::env::temp_dir().join(payload.stream_id.to_string());
    tokio::fs::create_dir_all(&temp_dir).await?;
    
    let result = process_extract_sound_inner(payload, s3_client, &temp_dir, &start_time).await;
    
    if let Err(e) = tokio::fs::remove_dir_all(&temp_dir).await {
        error!("Failed to clean up temporary directory for stream {}: {}", payload.stream_id, e);
    }
    
    result
}

async fn process_extract_sound_inner(
    payload: &ExtractSoundPayload, 
    s3_client: &S3Client,
    temp_dir: &std::path::PathBuf,
    start_time: &std::time::Instant,
) -> Result<serde_json::Value> {

    let video_s3_key = format!("{}/{}", payload.stream_id, payload.file_name);
    let temp_video_path = temp_dir.join(&payload.file_name);

    s3_client
        .download_file(&video_s3_key, &temp_video_path.to_string_lossy())
        .await
        .context("Failed to download video from S3")?;

    let audio_output_pattern = format!("{}/{}_%03d.wav", temp_dir.to_string_lossy(), payload.stream_id);

    let output = Command::new("ffmpeg")
        .arg("-i")
        .arg(&temp_video_path.to_string_lossy().as_ref())
        .arg("-f")
        .arg("segment")
        .arg("-segment_time")
        .arg(&AUDIO_SEGMENT_DURATION_SECONDS.to_string())
        .arg("-segment_start_number")
        .arg("1")
        .arg("-c:a")
        .arg("pcm_s16le")
        .arg("-ar")
        .arg(&AUDIO_SAMPLE_RATE.to_string())
        .arg("-ac")
        .arg(&AUDIO_CHANNELS.to_string())
        .arg(&audio_output_pattern)
        .output()
        .await
        .context("Failed to execute ffmpeg")?;

    if !output.status.success() {
        let error = String::from_utf8_lossy(&output.stderr);
        return Err(anyhow::anyhow!("ffmpeg failed: {}", error));
    }

    let mut audio_files = Vec::new();
    let mut segment_index = 1;

    loop {
        let segment_file_name = format!("{}_{:03}.wav", payload.stream_id, segment_index);
        let segment_path = temp_dir.join(&segment_file_name);

        if !tokio::fs::try_exists(&segment_path).await.unwrap_or(false) {
            break;
        }

        let s3_key = format!("{}/audios/{}", payload.stream_id, segment_file_name);

        let upload_result = s3_client
            .upload_file(&segment_path.to_string_lossy(), &s3_key)
            .await;

        if let Err(e) = upload_result {
            return Err(e.context("Failed to upload audio segment to S3"));
        }

        audio_files.push(segment_file_name);
        segment_index += 1;
    }

    Ok(serde_json::json!({
        "stream_id": payload.stream_id,
        "audio_files": audio_files,
        "processing_time": start_time.elapsed().as_millis(),
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
