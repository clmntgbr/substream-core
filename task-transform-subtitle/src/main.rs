use anyhow::{Context, Result};
use futures::StreamExt;
use lapin::options::BasicAckOptions;
use shared::{TransformSubtitlePayload, SubtitleOption, RabbitMQClient, RabbitMQConfig, TaskMessage, WebhookClient, S3Client};
use std::sync::Arc;
use tokio::sync::Semaphore;
use tracing::{error, info};

const TASK_TYPE: &str = "transform_subtitle";
const DEFAULT_MAX_CONCURRENT_TASKS: usize = 10;
const MIN_SRT_BLOCK_LINES: usize = 3;
const TIMESTAMP_PARTS_COUNT: usize = 2;
const HEX_COLOR_LENGTH: usize = 6;
const SCRIPT_TYPE: &str = "v4.00+";
const PLAY_RES_X: u32 = 384;
const PLAY_RES_Y: u32 = 288;
const MARGIN_L: u32 = 10;
const MARGIN_R: u32 = 10;


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
                        match serde_json::from_slice::<TaskMessage<TransformSubtitlePayload>>(&delivery.data) {
                            Ok(message) => {
                                let webhook_client = Arc::clone(&webhook_client);
                                let s3_client = Arc::clone(&s3_client);
                                let semaphore = Arc::clone(&semaphore);
                                
                                tokio::spawn(async move {
                                    let _permit = semaphore.acquire().await.unwrap();
                                    
                                    info!("Processing stream: {}", message.payload.stream_id);
                                    info!("Task ID: {}", message.payload.task_id);

                                    match process_transform_subtitle(&message.payload, &message.payload.task_id, &s3_client).await {
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
        if lines.len() < MIN_SRT_BLOCK_LINES {
            continue;
        }
        
        let time_line = lines[1];
        let parts: Vec<&str> = time_line.split(" --> ").collect();
        if parts.len() != TIMESTAMP_PARTS_COUNT {
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
    if parts.len() == TIMESTAMP_PARTS_COUNT {
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
    if hex.len() == HEX_COLOR_LENGTH {
        let r = &hex[0..2];
        let g = &hex[2..4];
        let b = &hex[4..6];
        format!("&H00{}{}{}", b.to_uppercase(), g.to_uppercase(), r.to_uppercase())
    } else {
        "&H00FFFFFF".to_string()
    }
}

fn create_ass_content(entries: Vec<SubtitleEntry>, options: &SubtitleOption) -> String {
    let subtitle_font = &options.subtitle_font;
    let subtitle_size = options.subtitle_size;
    let subtitle_color = &options.subtitle_color;
    let subtitle_outline_color = &options.subtitle_outline_color;
    let subtitle_bold = if options.subtitle_bold { "1" } else { "0" };
    let subtitle_italic = if options.subtitle_italic { "1" } else { "0" };
    let subtitle_underline = if options.subtitle_underline { "1" } else { "0" };
    let subtitle_outline_thickness = options.subtitle_outline_thickness;
    let subtitle_shadow = if options.subtitle_shadow > 0 { "1" } else { "0" };
    let subtitle_shadow_color = &options.subtitle_shadow_color;
    let y_axis_alignment = (options.y_axis_alignment * 10.0) as u32;
    
    let mut ass_content = format!(
r#"[Script Info]
ScriptType: {}
PlayResX: {}
PlayResY: {}
ScaledBorderAndShadow: yes

[V4+ Styles]
Format: Name,Fontname, Fontsize,PrimaryColour, SecondaryColour,OutlineColour, BackColour, Bold, Italic, Underline,StrikeOut, ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, Alignment, MarginL, MarginR, MarginV, Encoding
Style: Default, {},{}, {}, {}, {}, {}, {}, {},{}, 0, 100, 100, 0, 0,1, {}, {},2,{},{},{},0

[Events]
Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text
"#,
        SCRIPT_TYPE,
        PLAY_RES_X,
        PLAY_RES_Y,
        subtitle_font,
        subtitle_size,
        convert_color(subtitle_color),
        convert_color(subtitle_color),
        convert_color(subtitle_outline_color),
        convert_color(subtitle_shadow_color),
        subtitle_bold,
        subtitle_italic,
        subtitle_underline,
        subtitle_outline_thickness,
        subtitle_shadow,
        y_axis_alignment,
        MARGIN_L,
        MARGIN_R
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
    
    let start_time = std::time::Instant::now();
    
    let temp_dir = std::env::temp_dir().join(payload.stream_id.to_string());
    tokio::fs::create_dir_all(&temp_dir)
        .await
        .context("Failed to create temporary directory")?;
    
    let result = process_transform_subtitle_inner(payload, s3_client, &temp_dir, &start_time).await;
    
    if let Err(e) = tokio::fs::remove_dir_all(&temp_dir).await {
        error!("Failed to clean up temporary directory for stream {}: {}", payload.stream_id, e);
    }
    
    result
}

async fn process_transform_subtitle_inner(
    payload: &TransformSubtitlePayload, 
    s3_client: &S3Client,
    temp_dir: &std::path::PathBuf,
    start_time: &std::time::Instant,
) -> Result<serde_json::Value> {
    
    let srt_file_path = temp_dir.join(&payload.subtitle_srt_file_name);
    
    let s3_srt_key = format!("{}/subtitles/{}", payload.stream_id, payload.subtitle_srt_file_name);
    s3_client
        .download_file(&s3_srt_key, &srt_file_path.to_string_lossy())
        .await
        .context("Failed to download SRT file from S3")?;
    
    let srt_content = tokio::fs::read_to_string(&srt_file_path)
        .await
        .context("Failed to read SRT file")?;
    
    let entries = parse_srt(&srt_content)
        .context("Failed to parse SRT content")?;
    
    
    let ass_content = create_ass_content(entries, &payload.option);
    
    if let Err(e) = tokio::fs::remove_file(&srt_file_path).await {
        error!("Failed to remove temporary SRT file: {}", e);
    }
    
    let ass_file_name = format!("{}.ass", payload.stream_id);
    let temp_file_path = temp_dir.join(&ass_file_name);
    
    tokio::fs::write(&temp_file_path, &ass_content)
        .await
        .context("Failed to write ASS file")?;
    
    
    let s3_key = format!("{}/subtitles/{}", payload.stream_id, ass_file_name);
    s3_client
        .upload_file(&temp_file_path.to_string_lossy(), &s3_key)
        .await
        .context("Failed to upload ASS file to S3")?;
    
    
    Ok(serde_json::json!({
        "stream_id": payload.stream_id,
        "subtitle_ass_file_name": ass_file_name,
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

