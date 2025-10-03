use anyhow::{Context, Result};
use futures::StreamExt;
use lapin::options::BasicAckOptions;
use shared::{GenerateSubtitlePayload, RabbitMQClient, RabbitMQConfig, TaskMessage, WebhookClient, S3Client};
use std::sync::Arc;
use tokio::sync::Semaphore;
use tracing::{error, info};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

const TASK_TYPE: &str = "generate_subtitle";
const MAX_PARALLEL_FILES: usize = 4;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG").unwrap_or("info".to_string()),
        )
        .init();

    info!("Starting task-generate-subtitle service");

    dotenvy::dotenv().ok();

    let config = RabbitMQConfig::from_env();
    let rabbitmq_client = RabbitMQClient::new(config)
        .await
        .context("Failed to create RabbitMQ client")?;

    let webhook_client = Arc::new(WebhookClient::new());

    let queue_name = std::env::var("QUEUE_GENERATE_SUBTITLE").unwrap_or("core.generate_subtitle".to_string());
    
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
                        match serde_json::from_slice::<TaskMessage<GenerateSubtitlePayload>>(&delivery.data) {
                            Ok(message) => {
                                let webhook_client = Arc::clone(&webhook_client);
                                let semaphore = Arc::clone(&semaphore);
                                
                                tokio::spawn(async move {
                                    let _permit = semaphore.acquire().await.unwrap();
                                    
                                    info!("Processing stream: {}", message.payload.stream_id);
                                    info!("Task ID: {}", message.task_id);

                                    match process_generate_subtitle(&message.payload, &message.task_id).await {
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

async fn process_generate_subtitle(
    payload: &GenerateSubtitlePayload, 
    task_id: &uuid::Uuid,
) -> Result<serde_json::Value> {
    info!("Task {}: Processing stream {} with {} audio files", 
        task_id, payload.stream_id, payload.audio_files.len());

    let assemblyai_api_key = std::env::var("ASSEMBLYAI_API_KEY")
        .context("ASSEMBLYAI_API_KEY environment variable not set")?;
    
    let s3_client = Arc::new(S3Client::from_env().await
        .context("Failed to create S3 client")?);

    // Create temporary directory for processing
    let temp_dir = std::env::temp_dir().join(format!("subtitle_{}", task_id));
    tokio::fs::create_dir_all(&temp_dir).await
        .context("Failed to create temporary directory")?;

    // Process files in parallel with max concurrent limit
    let semaphore = Arc::new(Semaphore::new(MAX_PARALLEL_FILES));
    let mut tasks = Vec::new();

    for (index, audio_file) in payload.audio_files.iter().enumerate() {
        let s3_client_clone = Arc::clone(&s3_client);
        let api_key = assemblyai_api_key.clone();
        let stream_id = payload.stream_id.clone();
        let audio_file = audio_file.clone();
        let temp_dir = temp_dir.clone();
        let semaphore = Arc::clone(&semaphore);

        let task = tokio::spawn(async move {
            let _permit = semaphore.acquire().await.unwrap();
            
            let s3_path = format!("{}/{}", stream_id, audio_file);
            
            process_single_audio_file(
                &s3_client_clone,
                &api_key,
                &s3_path,
                &temp_dir,
                index,
            ).await
        });

        tasks.push(task);
    }

    // Wait for all tasks to complete
    let mut subtitle_parts = Vec::new();
    for (index, task) in tasks.into_iter().enumerate() {
        match task.await {
            Ok(Ok(subtitle)) => {
                subtitle_parts.push((index, subtitle));
            }
            Ok(Err(e)) => {
                error!("Failed to process file {}: {}", index, e);
                // Clean up temp directory
                let _ = tokio::fs::remove_dir_all(&temp_dir).await;
                return Err(e);
            }
            Err(e) => {
                error!("Task {} panicked: {}", index, e);
                // Clean up temp directory
                let _ = tokio::fs::remove_dir_all(&temp_dir).await;
                return Err(anyhow::anyhow!("Task {} panicked: {}", index, e));
            }
        }
    }

    // Sort by index to maintain order
    subtitle_parts.sort_by_key(|(index, _)| *index);
    
    // Merge subtitles
    let merged_srt = merge_subtitles(subtitle_parts.into_iter().map(|(_, s)| s).collect())?;

    // Upload merged SRT to S3
    let srt_filename = format!("{}.srt", payload.stream_id);
    let srt_path = temp_dir.join(&srt_filename);
    
    tokio::fs::write(&srt_path, &merged_srt).await
        .context("Failed to write merged SRT file")?;

    // Construct S3 path: stream_id/stream_id.srt
    let s3_srt_path = format!("{}/{}", payload.stream_id, srt_filename);
    
    s3_client.as_ref().upload_file(
        srt_path.to_str().unwrap(),
        &s3_srt_path,
    ).await
        .context("Failed to upload SRT to S3")?;

    // Clean up temp directory
    tokio::fs::remove_dir_all(&temp_dir).await
        .context("Failed to clean up temporary directory")?;

    Ok(serde_json::json!({
        "stream_id": payload.stream_id,
        "subtitle_srt_file": s3_srt_path,
    }))
}

// AssemblyAI API structures
#[derive(Debug, Serialize)]
struct AssemblyAITranscriptRequest {
    audio_url: String,
    language_detection: bool,
}

#[derive(Debug, Deserialize)]
struct AssemblyAITranscriptResponse {
    id: String,
    #[allow(dead_code)]
    status: String,
    #[allow(dead_code)]
    #[serde(default)]
    error: Option<String>,
}

#[derive(Debug, Deserialize)]
struct AssemblyAITranscriptStatusResponse {
    #[allow(dead_code)]
    id: String,
    status: String,
    #[serde(default)]
    error: Option<String>,
}

#[derive(Debug, Clone)]
struct SubtitleEntry {
    #[allow(dead_code)]
    index: usize,
    start_time: i64,  // milliseconds
    end_time: i64,    // milliseconds
    text: String,
}

async fn process_single_audio_file(
    s3_client: &Arc<S3Client>,
    api_key: &str,
    audio_file: &str,
    temp_dir: &PathBuf,
    index: usize,
) -> Result<Vec<SubtitleEntry>> {
    // Download file from S3
    let local_file_path = temp_dir.join(format!("audio_{}.wav", index));
    s3_client.download_file(audio_file, local_file_path.to_str().unwrap()).await
        .context("Failed to download audio file from S3")?;

    // Upload to AssemblyAI
    let upload_url = upload_to_assemblyai(api_key, &local_file_path).await
        .context("Failed to upload file to AssemblyAI")?;

    // Start transcription
    let transcript_id = start_transcription(api_key, &upload_url).await
        .context("Failed to start transcription")?;

    // Poll for completion
    let srt_content = poll_transcription(api_key, &transcript_id).await
        .context("Failed to get transcription result")?;

    // Parse SRT content
    let subtitle_entries = parse_srt(&srt_content)?;

    // Clean up local file
    let _ = tokio::fs::remove_file(&local_file_path).await;

    Ok(subtitle_entries)
}

async fn upload_to_assemblyai(api_key: &str, file_path: &PathBuf) -> Result<String> {
    let client = reqwest::Client::new();
    
    let file_content = tokio::fs::read(file_path).await
        .context("Failed to read audio file")?;

    let response = client
        .post("https://api.assemblyai.com/v2/upload")
        .header("authorization", api_key)
        .body(file_content)
        .send()
        .await
        .context("Failed to upload file to AssemblyAI")?;

    if !response.status().is_success() {
        let error_text = response.text().await.unwrap_or_default();
        return Err(anyhow::anyhow!("AssemblyAI upload failed: {}", error_text));
    }

    let upload_response: serde_json::Value = response.json().await
        .context("Failed to parse upload response")?;

    let upload_url = upload_response["upload_url"]
        .as_str()
        .context("No upload_url in response")?
        .to_string();

    Ok(upload_url)
}

async fn start_transcription(api_key: &str, audio_url: &str) -> Result<String> {
    let client = reqwest::Client::new();

    let request_body = AssemblyAITranscriptRequest {
        audio_url: audio_url.to_string(),
        language_detection: true,
    };

    let response = client
        .post("https://api.assemblyai.com/v2/transcript")
        .header("authorization", api_key)
        .json(&request_body)
        .send()
        .await
        .context("Failed to start transcription")?;

    if !response.status().is_success() {
        let error_text = response.text().await.unwrap_or_default();
        return Err(anyhow::anyhow!("AssemblyAI transcription start failed: {}", error_text));
    }

    let transcript_response: AssemblyAITranscriptResponse = response.json().await
        .context("Failed to parse transcription response")?;

    Ok(transcript_response.id)
}

async fn poll_transcription(api_key: &str, transcript_id: &str) -> Result<String> {
    let client = reqwest::Client::new();
    let status_url = format!("https://api.assemblyai.com/v2/transcript/{}", transcript_id);

    // Poll every 3 seconds
    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

        let response = client
            .get(&status_url)
            .header("authorization", api_key)
            .send()
            .await
            .context("Failed to poll transcription status")?;

        if !response.status().is_success() {
            let status = response.status();
            let error_text = response.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!("AssemblyAI poll failed (status {}): {}", status, error_text));
        }

        let response_text = response.text().await
            .context("Failed to read response text")?;
        
        let status_response: AssemblyAITranscriptStatusResponse = serde_json::from_str(&response_text)
            .context("Failed to parse status response")?;

        match status_response.status.as_str() {
            "completed" => {
                // Get SRT with 32 chars per caption
                let srt_url = format!(
                    "https://api.assemblyai.com/v2/transcript/{}/srt?chars_per_caption=32",
                    transcript_id
                );
                
                let srt_response = client
                    .get(&srt_url)
                    .header("authorization", api_key)
                    .send()
                    .await
                    .context("Failed to get SRT")?;

                if !srt_response.status().is_success() {
                    let status = srt_response.status();
                    let error_text = srt_response.text().await.unwrap_or_default();
                    
                    // Handle empty transcript case (no speech detected)
                    if status.as_u16() == 400 && error_text.contains("Transcript text is empty") {
                        return Ok(String::new());
                    }
                    
                    return Err(anyhow::anyhow!("Failed to get SRT (status {}): {}", status, error_text));
                }

                let srt_content = srt_response.text().await
                    .context("Failed to read SRT content")?;

                return Ok(srt_content);
            }
            "error" => {
                let error_msg = status_response.error.unwrap_or("Unknown error".to_string());
                return Err(anyhow::anyhow!("Transcription failed: {}", error_msg));
            }
            "queued" | "processing" => {
                continue;
            }
            _ => {
                continue;
            }
        }
    }
}

fn parse_srt(srt_content: &str) -> Result<Vec<SubtitleEntry>> {
    let mut entries = Vec::new();
    let blocks: Vec<&str> = srt_content.trim().split("\n\n").collect();

    for block in blocks {
        let lines: Vec<&str> = block.lines().collect();
        if lines.len() < 3 {
            continue;
        }

        let index_str = lines[0].trim();
        let index: usize = index_str.parse()
            .context(format!("Failed to parse index: {}", index_str))?;

        let timestamp_line = lines[1].trim();
        let (start_ms, end_ms) = parse_timestamp_line(timestamp_line)?;

        let text = lines[2..].join("\n");

        entries.push(SubtitleEntry {
            index,
            start_time: start_ms,
            end_time: end_ms,
            text,
        });
    }

    Ok(entries)
}

fn parse_timestamp_line(line: &str) -> Result<(i64, i64)> {
    // Format: 00:00:00,000 --> 00:00:02,500
    let parts: Vec<&str> = line.split(" --> ").collect();
    if parts.len() != 2 {
        return Err(anyhow::anyhow!("Invalid timestamp line: {}", line));
    }

    let start_ms = parse_timestamp(parts[0])?;
    let end_ms = parse_timestamp(parts[1])?;

    Ok((start_ms, end_ms))
}

fn parse_timestamp(ts: &str) -> Result<i64> {
    // Format: 00:00:00,000
    let parts: Vec<&str> = ts.split(',').collect();
    if parts.len() != 2 {
        return Err(anyhow::anyhow!("Invalid timestamp: {}", ts));
    }

    let time_parts: Vec<&str> = parts[0].split(':').collect();
    if time_parts.len() != 3 {
        return Err(anyhow::anyhow!("Invalid time format: {}", parts[0]));
    }

    let hours: i64 = time_parts[0].parse()
        .context("Failed to parse hours")?;
    let minutes: i64 = time_parts[1].parse()
        .context("Failed to parse minutes")?;
    let seconds: i64 = time_parts[2].parse()
        .context("Failed to parse seconds")?;
    let milliseconds: i64 = parts[1].parse()
        .context("Failed to parse milliseconds")?;

    let total_ms = (hours * 3600 + minutes * 60 + seconds) * 1000 + milliseconds;
    Ok(total_ms)
}

fn format_timestamp(ms: i64) -> String {
    let hours = ms / 3600000;
    let minutes = (ms % 3600000) / 60000;
    let seconds = (ms % 60000) / 1000;
    let milliseconds = ms % 1000;

    format!("{:02}:{:02}:{:02},{:03}", hours, minutes, seconds, milliseconds)
}

fn merge_subtitles(parts: Vec<Vec<SubtitleEntry>>) -> Result<String> {
    let mut all_entries = Vec::new();
    
    // Each audio file is 5 minutes (300 seconds = 300000 milliseconds)
    const CHUNK_DURATION_MS: i64 = 300_000;

    for (part_index, part) in parts.into_iter().enumerate() {
        let time_offset = (part_index as i64) * CHUNK_DURATION_MS;
        
        for mut entry in part {
            // Adjust timestamps based on which chunk this is
            entry.start_time += time_offset;
            entry.end_time += time_offset;
            all_entries.push(entry);
        }
    }

    // Generate SRT content
    let mut srt_content = String::new();
    for (new_index, entry) in all_entries.iter().enumerate() {
        srt_content.push_str(&format!("{}\n", new_index + 1));
        srt_content.push_str(&format!(
            "{} --> {}\n",
            format_timestamp(entry.start_time),
            format_timestamp(entry.end_time)
        ));
        srt_content.push_str(&format!("{}\n\n", entry.text));
    }

    Ok(srt_content)
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

