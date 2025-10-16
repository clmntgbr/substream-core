use anyhow::{Context, Result};
use futures::StreamExt;
use lapin::options::BasicAckOptions;
use shared::{ResumeVideoPayload, RabbitMQClient, RabbitMQConfig, TaskMessage, WebhookClient, S3Client};
use std::sync::Arc;
use tokio::sync::Semaphore;
use tracing::{error, info};
use serde::{Deserialize, Serialize};

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

#[derive(Debug, Serialize)]
struct ChatGPTRequest {
    model: String,
    messages: Vec<ChatMessage>,
    max_tokens: u32,
    temperature: f32,
}

#[derive(Debug, Serialize, Deserialize)]
struct ChatMessage {
    role: String,
    content: String,
}

#[derive(Debug, Deserialize)]
struct ChatGPTResponse {
    choices: Vec<ChatChoice>,
}

#[derive(Debug, Deserialize)]
struct ChatChoice {
    message: ChatMessage,
}

fn clean_srt_content(srt_content: &str) -> String {
    let mut cleaned_text = Vec::new();
    
    for block in srt_content.split("\n\n") {
        let block = block.trim();
        if block.is_empty() {
            continue;
        }
        
        let lines: Vec<&str> = block.lines().collect();
        
        if lines.len() < 3 {
            continue;
        }
        
        for line in &lines[2..] {
            let text = line.trim();
            if !text.is_empty() {
                cleaned_text.push(text.to_string());
            }
        }
    }
    
    cleaned_text.join(" ")
}

async fn generate_summary_with_chatgpt(srt_content: &str, api_key: &str) -> Result<String> {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(60))
        .build()
        .context("Failed to build HTTP client")?;
    
    let cleaned_content = clean_srt_content(srt_content);

    info!("SRT content: {}", cleaned_content);
    info!("Cleaned SRT content: original {} chars -> cleaned {} chars", srt_content.len(), cleaned_content.len());
    
    let prompt = format!(
        "Based on the following transcript from a video, please provide a concise summary in a few lines. IMPORTANT: Write the summary in the same language as the transcript.\n\n{}",
        cleaned_content
    );
    
    info!("Preparing ChatGPT request (cleaned content length: {} chars, prompt length: {} chars)", cleaned_content.len(), prompt.len());
    
    let request_body = ChatGPTRequest {
        model: "gpt-4o".to_string(),
        messages: vec![
            ChatMessage {
                role: "system".to_string(),
                content: "You are a helpful assistant that creates concise summaries from subtitle files. Always write the summary in the same language as the input text.".to_string(),
            },
            ChatMessage {
                role: "user".to_string(),
                content: prompt,
            },
        ],
        max_tokens: 500,
        temperature: 0.7,
    };
    
    info!("Calling ChatGPT API to generate summary");
    
    let response = match client
        .post("https://api.openai.com/v1/chat/completions")
        .header("Authorization", format!("Bearer {}", api_key))
        .header("Content-Type", "application/json")
        .json(&request_body)
        .send()
        .await
    {
        Ok(resp) => resp,
        Err(e) => {
            error!("Failed to send request to ChatGPT API: {:?}", e);
            return Err(anyhow::anyhow!("Failed to send request to ChatGPT API: {}", e));
        }
    };
    
    let status = response.status();
    info!("ChatGPT API response status: {}", status);
    
    if !status.is_success() {
        let error_text = response.text().await.unwrap_or_default();
        error!("ChatGPT API error response: {}", error_text);
        return Err(anyhow::anyhow!(
            "ChatGPT API request failed with status {}: {}",
            status,
            error_text
        ));
    }
    
    let response_text = match response.text().await {
        Ok(text) => {
            info!("Received response from ChatGPT (length: {} chars)", text.len());
            text
        },
        Err(e) => {
            error!("Failed to read response body: {:?}", e);
            return Err(anyhow::anyhow!("Failed to read response body: {}", e));
        }
    };
    
    let chat_response: ChatGPTResponse = match serde_json::from_str(&response_text) {
        Ok(resp) => resp,
        Err(e) => {
            error!("Failed to parse ChatGPT response: {:?}", e);
            error!("Response was: {}", response_text);
            return Err(anyhow::anyhow!("Failed to parse ChatGPT response: {}", e));
        }
    };
    
    let summary = chat_response
        .choices
        .first()
        .context("No response from ChatGPT")?
        .message
        .content
        .clone();
    
    info!("Successfully generated summary (length: {} chars)", summary.len());
    
    Ok(summary)
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
    
    let srt_file_path = temp_dir.join(&payload.subtitle_srt_file_name);
    let s3_srt_key = format!("{}/subtitles/{}", payload.stream_id, payload.subtitle_srt_file_name);
    
    info!("Downloading SRT file from S3: {}", s3_srt_key);
    s3_client
        .download_file(&s3_srt_key, &srt_file_path.to_string_lossy())
        .await
        .context("Failed to download SRT file from S3")?;
    
    let srt_content = tokio::fs::read_to_string(&srt_file_path)
        .await
        .context("Failed to read SRT file")?;
    
    info!("Read SRT file (length: {} chars)", srt_content.len());
    
    let openai_api_key = std::env::var("OPENAI_API_KEY")
        .context("OPENAI_API_KEY environment variable not set")?;
    
    let summary = generate_summary_with_chatgpt(&srt_content, &openai_api_key)
        .await
        .context("Failed to generate summary with ChatGPT")?;
    
    if let Err(e) = tokio::fs::remove_file(&srt_file_path).await {
        error!("Failed to remove temporary SRT file: {}", e);
    }
    
    let resume_file_name = format!("{}.txt", payload.stream_id);
    let temp_file_path = temp_dir.join(&resume_file_name);
    
    tokio::fs::write(&temp_file_path, &summary)
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

