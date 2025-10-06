use anyhow::{Context, Result};
use futures::StreamExt;
use lapin::options::BasicAckOptions;
use shared::{ChunkVideoPayload, RabbitMQClient, RabbitMQConfig, S3Client, TaskMessage, WebhookClient};
use std::sync::Arc;
use tokio::sync::Semaphore;
use tracing::{error, info};

const TASK_TYPE: &str = "chunk_video";
const DEFAULT_MAX_CONCURRENT_TASKS: usize = 10;
const VIDEO_OVERLAP_SECONDS: f64 = 8.0;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG").unwrap_or("info".to_string()),
        )
        .init();

    info!("Starting task-chunk-video service");

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

    let queue_name = std::env::var("QUEUE_CHUNK_VIDEO").unwrap_or("core.chunk_video".to_string());
    
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
                        match serde_json::from_slice::<TaskMessage<ChunkVideoPayload>>(&delivery.data) {
                            Ok(message) => {
                                let webhook_client = Arc::clone(&webhook_client);
                                let s3_client = Arc::clone(&s3_client);
                                let semaphore = Arc::clone(&semaphore);
                                
                                tokio::spawn(async move {
                                    let _permit = semaphore.acquire().await.unwrap();
                                    
                                    info!("Processing stream: {}", message.payload.stream_id);
                                    info!("Task ID: {}", message.task_id);

                                    match process_chunk_video(&message.payload, &message.task_id, &s3_client).await {
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

async fn process_chunk_video(
    payload: &ChunkVideoPayload, 
    _task_id: &uuid::Uuid,
    s3_client: &S3Client,
) -> Result<serde_json::Value> {
    
    let temp_dir = std::env::temp_dir().join(&payload.stream_id);
    tokio::fs::create_dir_all(&temp_dir)
        .await
        .context("Failed to create temporary directory")?;
    
    let result = process_chunk_video_inner(payload, s3_client, &temp_dir).await;
    
    if let Err(e) = tokio::fs::remove_dir_all(&temp_dir).await {
        error!("Failed to clean up temporary directory for stream {}: {}", payload.stream_id, e);
    }
    
    result
}

async fn process_chunk_video_inner(
    payload: &ChunkVideoPayload, 
    s3_client: &S3Client,
    temp_dir: &std::path::PathBuf,
) -> Result<serde_json::Value> {
    
    let embed_file_path = temp_dir.join(&payload.embed_file_name);
    
    let s3_embed_path = format!("{}/{}", payload.stream_id, payload.embed_file_name);
    s3_client
        .download_file(&s3_embed_path, &embed_file_path.to_string_lossy())
        .await
        .context("Failed to download embedded video from S3")?;
    
    
    let video_duration = get_video_duration(&embed_file_path.to_string_lossy().as_ref())
        .await
        .context("Failed to get video duration")?;
    
    
    let chunk_duration = video_duration / payload.chunk_number as f64;
    let overlap_seconds = VIDEO_OVERLAP_SECONDS;
    
    
    let mut chunk_file_names = Vec::new();
    
    for i in 1..=payload.chunk_number {
        let chunk_file_name = format!("{}_{:03}.mp4", payload.stream_id, i);
        let chunk_file_path = temp_dir.join(&chunk_file_name);
        
        let start_time = if i == 1 { 
            0.0 
        } else { 
            (i - 1) as f64 * chunk_duration - overlap_seconds 
        };
        
        let end_time = if i == payload.chunk_number { 
            video_duration 
        } else { 
            i as f64 * chunk_duration 
        };
        
        
        extract_video_segment(&embed_file_path.to_string_lossy().as_ref(), 
                             &chunk_file_path.to_string_lossy().as_ref(), 
                             start_time, 
                             end_time)
            .await
            .context(format!("Failed to extract chunk {}", chunk_file_name))?;
        
        let s3_chunk_path = format!("{}/{}", payload.stream_id, chunk_file_name);
        s3_client
            .upload_file(&chunk_file_path.to_string_lossy(), &s3_chunk_path)
            .await
            .context(format!("Failed to upload chunk {} to S3", chunk_file_name))?;
        
        chunk_file_names.push(chunk_file_name);
    }
    
    
    let result = serde_json::json!({
        "stream_id": payload.stream_id,
        "chunk_file_names": chunk_file_names,
    });

    Ok(result)
}

async fn get_video_duration(file_path: &str) -> Result<f64> {
    let output = tokio::process::Command::new("ffprobe")
        .args([
            "-v", "quiet",
            "-show_entries", "format=duration",
            "-of", "csv=p=0",
            file_path,
        ])
        .output()
        .await
        .context("Failed to execute ffprobe")?;

    if !output.status.success() {
        return Err(anyhow::anyhow!("ffprobe failed: {}", String::from_utf8_lossy(&output.stderr)));
    }

    let duration_str = String::from_utf8_lossy(&output.stdout).trim().to_string();
    duration_str.parse::<f64>()
        .context(format!("Failed to parse video duration: {}", duration_str))
}

async fn extract_video_segment(input_path: &str, output_path: &str, start_time: f64, end_time: f64) -> Result<()> {
    let duration = end_time - start_time;
    
    let output = tokio::process::Command::new("ffmpeg")
        .args([
            "-i", input_path,
            "-ss", &start_time.to_string(),
            "-t", &duration.to_string(),
            "-c", "copy",
            "-avoid_negative_ts", "make_zero",
            "-y",
            output_path,
        ])
        .output()
        .await
        .context("Failed to execute ffmpeg")?;

    if !output.status.success() {
        return Err(anyhow::anyhow!(
            "ffmpeg failed: {}", 
            String::from_utf8_lossy(&output.stderr)
        ));
    }

    Ok(())
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
