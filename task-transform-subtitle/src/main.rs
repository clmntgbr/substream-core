use anyhow::{Context, Result};
use futures::StreamExt;
use lapin::options::BasicAckOptions;
use shared::{TransformSubtitlePayload, RabbitMQClient, RabbitMQConfig, TaskMessage, WebhookClient, S3Client};
use std::sync::Arc;
use tokio::sync::Semaphore;
use tracing::{error, info};

const TASK_TYPE: &str = "transform_subtitle";

struct CleanupGuard<F: FnOnce()> {
    cleanup_fn: Option<F>,
}

impl<F: FnOnce()> CleanupGuard<F> {
    fn new(cleanup_fn: F) -> Self {
        Self {
            cleanup_fn: Some(cleanup_fn),
        }
    }
}

impl<F: FnOnce()> Drop for CleanupGuard<F> {
    fn drop(&mut self) {
        if let Some(cleanup_fn) = self.cleanup_fn.take() {
            cleanup_fn();
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG").unwrap_or("info".to_string()),
        )
        .init();

    info!("Starting task-transform-subtitle service");

    dotenvy::dotenv().ok();

    let config = RabbitMQConfig::from_env();
    let rabbitmq_client = RabbitMQClient::new(config)
        .await
        .context("Failed to create RabbitMQ client")?;

    let webhook_client = Arc::new(WebhookClient::new());
    let s3_client = Arc::new(S3Client::from_env().await.context("Failed to create S3 client")?);

    let queue_name = std::env::var("QUEUE_TRANSFORM_SUBTITLE").unwrap_or("core.transform_subtitle".to_string());
    
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
                        match serde_json::from_slice::<TaskMessage<TransformSubtitlePayload>>(&delivery.data) {
                            Ok(message) => {
                                let webhook_client = Arc::clone(&webhook_client);
                                let s3_client = Arc::clone(&s3_client);
                                let semaphore = Arc::clone(&semaphore);
                                
                                tokio::spawn(async move {
                                    let _permit = semaphore.acquire().await.unwrap();
                                    
                                    info!("Processing stream: {}", message.payload.stream_id);
                                    info!("Task ID: {}", message.task_id);

                                    match process_transform_subtitle(&message.payload, &message.task_id, &s3_client).await {
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

#[derive(Debug)]
struct SubtitleEntry {
    start_time: String,
    end_time: String,
    text: String,
}

fn parse_srt(srt_content: &str) -> Result<Vec<SubtitleEntry>> {
    let mut entries = Vec::new();
    let blocks: Vec<&str> = srt_content.split("\n\n").collect();
    
    for block in blocks {
        let block = block.trim();
        if block.is_empty() {
            continue;
        }
        
        let lines: Vec<&str> = block.lines().collect();
        if lines.len() < 3 {
            continue;
        }
        
        let time_line = lines[1];
        let parts: Vec<&str> = time_line.split(" --> ").collect();
        if parts.len() != 2 {
            continue;
        }
        
        let start_time = convert_srt_time_to_ass(parts[0].trim());
        let end_time = convert_srt_time_to_ass(parts[1].trim());
        
        let text = lines[2..].join("\\N");
        
        entries.push(SubtitleEntry {
            start_time,
            end_time,
            text,
        });
    }
    
    Ok(entries)
}

fn convert_srt_time_to_ass(srt_time: &str) -> String {
    let time = srt_time.replace(",", ".");
    
    let parts: Vec<&str> = time.split('.').collect();
    if parts.len() == 2 {
        let time_part = parts[0];
        let ms_part = parts[1];
        
        let cs = if ms_part.len() >= 2 {
            &ms_part[0..2]
        } else {
            ms_part
        };
        
        let formatted_time = if time_part.starts_with("00:") {
            format!("0:{}", &time_part[3..])
        } else if time_part.starts_with("0") && !time_part.starts_with("0:") {
            time_part[1..].to_string()
        } else {
            time_part.to_string()
        };
        
        format!("{}.{}", formatted_time, cs)
    } else {
        time
    }
}

fn convert_color(hex_color: &str) -> String {
    let hex = hex_color.trim_start_matches('#');
    if hex.len() == 6 {
        let r = &hex[0..2];
        let g = &hex[2..4];
        let b = &hex[4..6];
        format!("&H00{}{}{}", b.to_uppercase(), g.to_uppercase(), r.to_uppercase())
    } else {
        "&H00FFFFFF".to_string()
    }
}

fn create_ass_content(entries: Vec<SubtitleEntry>) -> String {
    let subtitle_font = "Arial";
    let subtitle_size = "20";
    let subtitle_color = "#FFFFFF";
    let subtitle_outline_color = "#000000";
    let subtitle_bold = "0";
    let subtitle_italic = "0";
    let subtitle_underline = "0";
    let subtitle_outline_thickness = "2";
    let subtitle_shadow = "SIMPLE";
    let y_axis_alignment = "10";
    
    let mut ass_content = format!(
r#"[Script Info]
ScriptType: v4.00+
PlayResX: 384
PlayResY: 288
ScaledBorderAndShadow: yes

[V4+ Styles]
Format: Name,Fontname, Fontsize,PrimaryColour, SecondaryColour,OutlineColour, BackColour, Bold, Italic, Underline,StrikeOut, ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, Alignment, MarginL, MarginR, MarginV, Encoding
Style: Default, {},{}, {}, {}, {},&H00000000, {}, {},{}, 0, 100, 100, 0, 0,1, {}, {},2,10,10,{},0

[Events]
Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text
"#,
        subtitle_font,
        subtitle_size,
        convert_color(subtitle_color),
        convert_color(subtitle_color),
        convert_color(subtitle_outline_color),
        subtitle_bold,
        subtitle_italic,
        subtitle_underline,
        subtitle_outline_thickness,
        if subtitle_shadow != "NONE" { "1" } else { "0" },
        y_axis_alignment
    );
    
    for entry in entries {
        ass_content.push_str(&format!(
            "Dialogue: 0,{},{},Default,,0,0,0,,{}\n",
            entry.start_time,
            entry.end_time,
            entry.text
        ));
    }
    
    ass_content
}

async fn process_transform_subtitle(
    payload: &TransformSubtitlePayload, 
    _task_id: &uuid::Uuid,
    s3_client: &S3Client,
) -> Result<serde_json::Value> {
    info!("Converting SRT to ASS for stream: {}", payload.stream_id);
    
    let temp_dir = std::env::var("TEMP_DIR").unwrap_or("/tmp".to_string());
    let srt_file_path = format!("{}/{}", temp_dir, payload.subtitle_srt_file_name);
    
    let s3_srt_key = format!("{}/subtitles/{}", payload.stream_id, payload.subtitle_srt_file_name);
    s3_client
        .download_file(&s3_srt_key, &srt_file_path)
        .await
        .context("Failed to download SRT file from S3")?;
    
    let srt_content = tokio::fs::read_to_string(&srt_file_path)
        .await
        .context("Failed to read SRT file")?;
    
    let entries = parse_srt(&srt_content)
        .context("Failed to parse SRT content")?;
    
    info!("Parsed {} subtitle entries", entries.len());
    
    let ass_content = create_ass_content(entries);
    
    if let Err(e) = tokio::fs::remove_file(&srt_file_path).await {
        error!("Failed to remove temporary SRT file: {}", e);
    }
    
    let ass_file_name = format!("{}.ass", payload.stream_id);
    let temp_file_path = format!("{}/{}", temp_dir, ass_file_name);
    
    let temp_file_path_cleanup = temp_file_path.clone();
    let _cleanup_guard = CleanupGuard::new(move || {
        let temp_file_path = temp_file_path_cleanup.clone();
        tokio::spawn(async move {
            if tokio::fs::try_exists(&temp_file_path).await.unwrap_or(false) {
                if let Err(e) = tokio::fs::remove_file(&temp_file_path).await {
                    error!("Failed to remove temporary ASS file during cleanup: {}", e);
                }
            }
        });
    });
    
    tokio::fs::write(&temp_file_path, &ass_content)
        .await
        .context("Failed to write ASS file")?;
    
    info!("Created temporary ASS file: {}", temp_file_path);
    
    let s3_key = format!("{}/subtitles/{}", payload.stream_id, ass_file_name);
    s3_client
        .upload_file(&temp_file_path, &s3_key)
        .await
        .context("Failed to upload ASS file to S3")?;
    
    if let Err(e) = tokio::fs::remove_file(&temp_file_path).await {
        error!("Failed to remove temporary ASS file: {}", e);
    }
    
    info!("Successfully uploaded ASS file to S3: {}", s3_key);
    
    Ok(serde_json::json!({
        "stream_id": payload.stream_id,
        "subtitle_ass_file_name": ass_file_name,
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

