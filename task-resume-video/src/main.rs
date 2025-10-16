use anyhow::{Context, Result};
use futures::StreamExt;
use lapin::options::BasicAckOptions;
use shared::{ResumeVideoPayload, RabbitMQClient, RabbitMQConfig, TaskMessage, WebhookClient, S3Client};
use std::sync::Arc;
use tokio::sync::Semaphore;
use tracing::{error, info};
use rand::Rng;

const TASK_TYPE: &str = "resume_video";
const DEFAULT_MAX_CONCURRENT_TASKS: usize = 10;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG").unwrap_or("info".to_string()),
        )
        .init();

    info!("Starting task-resume-video service");

    dotenvy::dotenv().ok();

    let config = RabbitMQConfig::from_env();
    let rabbitmq_client = RabbitMQClient::new(config)
        .await
        .context("Failed to create RabbitMQ client")?;

    let webhook_client = Arc::new(WebhookClient::new());
    let s3_client = Arc::new(S3Client::from_env().await.context("Failed to create S3 client")?);

    let queue_name = std::env::var("QUEUE_RESUME_VIDEO").unwrap_or("core.resume_video".to_string());
    
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
                        match serde_json::from_slice::<TaskMessage<ResumeVideoPayload>>(&delivery.data) {
                            Ok(message) => {
                                let webhook_client = Arc::clone(&webhook_client);
                                let s3_client = Arc::clone(&s3_client);
                                let semaphore = Arc::clone(&semaphore);
                                
                                tokio::spawn(async move {
                                    let _permit = semaphore.acquire().await.unwrap();
                                    
                                    info!("Processing stream: {}", message.payload.stream_id);
                                    info!("Task ID: {}", message.payload.task_id);

                                    match process_resume_video(&message.payload, &message.payload.task_id, &s3_client).await {
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
                                                    &message.payload.stream_id.to_string(),
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

fn generate_random_text() -> String {
    let mut rng = rand::thread_rng();
    let sentences = vec![
        "Lorem ipsum dolor sit amet, consectetur adipiscing elit.",
        "Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.",
        "Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris.",
        "Duis aute irure dolor in reprehenderit in voluptate velit esse cillum.",
        "Excepteur sint occaecat cupidatat non proident, sunt in culpa.",
        "Nisi ut aliquip ex ea commodo consequat in magna aliqua.",
        "Quis autem vel eum iure reprehenderit qui in ea voluptate velit.",
        "At vero eos et accusamus et iusto odio dignissimos ducimus.",
    ];
    
    let num_sentences = rng.gen_range(3..=8);
    let mut text = String::new();
    
    for _ in 0..num_sentences {
        let idx = rng.gen_range(0..sentences.len());
        text.push_str(sentences[idx]);
        text.push(' ');
    }
    
    text.trim().to_string()
}

async fn process_resume_video(
    payload: &ResumeVideoPayload, 
    _task_id: &uuid::Uuid,
    s3_client: &S3Client,
) -> Result<serde_json::Value> {
    
    let start_time = std::time::Instant::now();
    
    let temp_dir = std::env::temp_dir().join(payload.stream_id.to_string());
    tokio::fs::create_dir_all(&temp_dir)
        .await
        .context("Failed to create temporary directory")?;
    
    let result = process_resume_video_inner(payload, s3_client, &temp_dir, &start_time).await;
    
    if let Err(e) = tokio::fs::remove_dir_all(&temp_dir).await {
        error!("Failed to clean up temporary directory for stream {}: {}", payload.stream_id, e);
    }
    
    result
}

async fn process_resume_video_inner(
    payload: &ResumeVideoPayload, 
    s3_client: &S3Client,
    temp_dir: &std::path::PathBuf,
    start_time: &std::time::Instant,
) -> Result<serde_json::Value> {
    
    let random_text = generate_random_text();
    
    info!("Generated random text for resume (length: {} chars)", random_text.len());
    
    let resume_file_name = format!("{}.txt", payload.stream_id);
    let temp_file_path = temp_dir.join(&resume_file_name);
    
    tokio::fs::write(&temp_file_path, &random_text)
        .await
        .context("Failed to write resume file")?;
    
    info!("Created resume file: {}", resume_file_name);
    
    let s3_key = format!("{}/{}", payload.stream_id, resume_file_name);
    s3_client
        .upload_file(&temp_file_path.to_string_lossy(), &s3_key)
        .await
        .context("Failed to upload resume file to S3")?;
    
    info!("Successfully uploaded resume file to S3");
    
    Ok(serde_json::json!({
        "stream_id": payload.stream_id,
        "resume_file_name": resume_file_name,
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

