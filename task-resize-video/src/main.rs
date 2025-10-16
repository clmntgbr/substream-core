use anyhow::{Context, Result};
use futures::StreamExt;
use lapin::options::BasicAckOptions;
use shared::{ResizeVideoPayload, RabbitMQClient, RabbitMQConfig, S3Client, TaskMessage, WebhookClient};
use std::sync::Arc;
use tokio::sync::Semaphore;
use tracing::{error, info};

const TASK_TYPE: &str = "resize_video";
const DEFAULT_MAX_CONCURRENT_TASKS: usize = 2;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG").unwrap_or("info".to_string()),
        )
        .init();

    info!("Starting task-resize-video service");

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

    let queue_name = std::env::var("QUEUE_RESIZE_VIDEO").unwrap_or("core.resize_video".to_string());
    
    let max_concurrent = DEFAULT_MAX_CONCURRENT_TASKS;

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
                        match serde_json::from_slice::<TaskMessage<ResizeVideoPayload>>(&delivery.data) {
                            Ok(message) => {
                                let webhook_client = Arc::clone(&webhook_client);
                                let s3_client = Arc::clone(&s3_client);
                                let semaphore = Arc::clone(&semaphore);
                                
                                tokio::spawn(async move {
                                    let _permit = semaphore.acquire().await.unwrap();
                                    
                                    info!("Processing stream: {}", message.payload.stream_id);
                                    info!("Task ID: {}", message.payload.task_id);

                                    match process_resize_video(&message.payload, &message.payload.task_id, &s3_client).await {
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
                                                .send_error(
                                                    &message.webhook_url_failure,
                                                    message.payload.task_id,
                                                    TASK_TYPE,
                                                    &message.payload.stream_id.to_string(),
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

async fn process_resize_video(
    payload: &ResizeVideoPayload, 
    _task_id: &uuid::Uuid,
    s3_client: &S3Client,
) -> Result<serde_json::Value> {
    
    let start_time = std::time::Instant::now();
    
    let temp_dir = std::env::temp_dir().join(payload.stream_id.to_string());
    tokio::fs::create_dir_all(&temp_dir)
        .await
        .context("Failed to create temporary directory")?;
    
    let result = process_resize_video_inner(payload, s3_client, &temp_dir, &start_time).await;
    
    if let Err(e) = tokio::fs::remove_dir_all(&temp_dir).await {
        error!("Failed to clean up temporary directory for stream {}: {}", payload.stream_id, e);
    }
    
    result
}

async fn process_resize_video_inner(
    payload: &ResizeVideoPayload, 
    s3_client: &S3Client,
    temp_dir: &std::path::PathBuf,
    start_time: &std::time::Instant,
) -> Result<serde_json::Value> {
    
    let original_file_path = temp_dir.join(&payload.file_name);
    let resize_file_name = format!("{}_resize.mp4", payload.stream_id);
    let resized_file_path = temp_dir.join(&resize_file_name);
    
    let s3_original_path = format!("{}/{}", payload.stream_id, payload.file_name);
    s3_client
        .download_file(&s3_original_path, &original_file_path.to_string_lossy())
        .await
        .context("Failed to download original video from S3")?;
    
    // Handle different video formats
    match payload.format.as_str() {
        "original" => {
            // Just copy the original file
            tokio::fs::copy(&original_file_path, &resized_file_path)
                .await
                .context("Failed to copy original video file")?;
            
            let s3_resized_path = format!("{}/{}", payload.stream_id, resize_file_name);
            s3_client
                .upload_file(&resized_file_path.to_string_lossy(), &s3_resized_path)
                .await
                .context("Failed to upload resized video to S3")?;
        },
        "zoomed_916" => {
            // Transform to TikTok format (zoomed 9:16)
            transform_video_to_zoomed_916(&original_file_path, &resized_file_path)
                .await
                .context("Failed to transform video to zoomed 9:16 format")?;
            
            let s3_resized_path = format!("{}/{}", payload.stream_id, resize_file_name);
            s3_client
                .upload_file(&resized_file_path.to_string_lossy(), &s3_resized_path)
                .await
                .context("Failed to upload transformed video to S3")?;
        },
        "normal_916_with_borders" => {
            // Transform to 9:16 with borders
            transform_video_to_916_with_borders(&original_file_path, &resized_file_path)
                .await
                .context("Failed to transform video to 9:16 with borders")?;
            
            let s3_resized_path = format!("{}/{}", payload.stream_id, resize_file_name);
            s3_client
                .upload_file(&resized_file_path.to_string_lossy(), &s3_resized_path)
                .await
                .context("Failed to upload transformed video to S3")?;
        },
        _ => {
            return Err(anyhow::anyhow!("Unknown video format: {}", payload.format));
        }
    }
    
    // Delete the original file
    tokio::fs::remove_file(&original_file_path)
        .await
        .context("Failed to delete original video file")?;
    
    let result = serde_json::json!({
        "stream_id": payload.stream_id,
        "resize_file_name": resize_file_name,
        "processing_time": start_time.elapsed().as_millis(),
    });

    Ok(result)
}


async fn transform_video_to_zoomed_916(
    input_video_path: &std::path::PathBuf,
    output_video_path: &std::path::PathBuf,
) -> Result<()> {
    use tokio::process::Command;
    
    info!(
        "Transforming video to zoomed 9:16 format: {} -> {}",
        input_video_path.display(),
        output_video_path.display()
    );
    
    // Probe to get video dimensions
    let probe = Command::new("ffprobe")
        .arg("-v").arg("error")
        .arg("-select_streams").arg("v:0")
        .arg("-show_entries").arg("stream=width,height")
        .arg("-of").arg("csv=p=0")
        .arg(&input_video_path)
        .output()
        .await?;

    let dimensions = String::from_utf8_lossy(&probe.stdout);
    let parts: Vec<&str> = dimensions.trim().split(',').collect();
    if parts.len() != 2 {
        return Err(anyhow::anyhow!("Failed to get video dimensions"));
    }
    
    let original_width: i32 = parts[0].parse()?;
    let original_height: i32 = parts[1].parse()?;
    
    let max_width = 1080;
    let max_height = 1920;
    
    let target_aspect_ratio = max_width as f64 / max_height as f64;
    let original_aspect_ratio = original_width as f64 / original_height as f64;
    
    info!(
        "Original: {}x{}, Target: {}x{}, Original AR: {:.3}, Target AR: {:.3}",
        original_width, original_height, max_width, max_height,
        original_aspect_ratio, target_aspect_ratio
    );
    
    // Check if already in 9:16 format
    if (original_aspect_ratio - target_aspect_ratio).abs() < 0.01 {
        if original_width <= max_width && original_height <= max_height {
            info!("Video is already in correct 9:16 format, copying without re-encoding");
            let output = Command::new("ffmpeg")
                .arg("-i").arg(&input_video_path)
                .arg("-c:v").arg("copy")
                .arg("-c:a").arg("copy")
                .arg("-y")
                .arg(&output_video_path)
                .output()
                .await?;
            
            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr);
                return Err(anyhow::anyhow!("FFmpeg command failed: {}", stderr));
            }
            return Ok(());
        } else {
            info!("Video is 9:16 but too large, scaling down to max 1080x1920");
            let output = Command::new("ffmpeg")
                .arg("-i").arg(&input_video_path)
                .arg("-vf").arg(format!("scale={}:{}", max_width, max_height))
                .arg("-c:v").arg("libx264")
                .arg("-c:a").arg("aac")
                .arg("-preset").arg("ultrafast")
                .arg("-crf").arg("28")
                .arg("-maxrate").arg("2M")
                .arg("-bufsize").arg("4M")
                .arg("-pix_fmt").arg("yuv420p")
                .arg("-movflags").arg("+faststart")
                .arg("-threads").arg("0")
                .arg("-y")
                .arg(&output_video_path)
                .output()
                .await?;
            
            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr);
                return Err(anyhow::anyhow!("FFmpeg command failed: {}", stderr));
            }
            return Ok(());
        }
    }
    
    // Calculate crop dimensions
    let (crop_width, crop_height, crop_x, crop_y) = if original_aspect_ratio > target_aspect_ratio {
        // Video is wider, crop sides
        let new_width = (original_height as f64 * target_aspect_ratio) as i32;
        let x_offset = (original_width - new_width) / 2;
        (new_width, original_height, x_offset, 0)
    } else {
        // Video is taller, crop top/bottom
        let new_height = (original_width as f64 / target_aspect_ratio) as i32;
        let y_offset = (original_height - new_height) / 2;
        (original_width, new_height, 0, y_offset)
    };
    
    info!(
        "Crop dimensions: {}x{} at offset ({}, {})",
        crop_width, crop_height, crop_x, crop_y
    );
    
    // Crop and potentially scale
    let vf_filter = if crop_width <= max_width && crop_height <= max_height {
        info!("Cropping without scaling");
        format!("crop={}:{}:{}:{}", crop_width, crop_height, crop_x, crop_y)
    } else {
        info!("Cropping and scaling down to max 1080x1920");
        format!(
            "crop={}:{}:{}:{},scale={}:{}",
            crop_width, crop_height, crop_x, crop_y, max_width, max_height
        )
    };
    
    let output = Command::new("ffmpeg")
        .arg("-i").arg(&input_video_path)
        .arg("-vf").arg(&vf_filter)
        .arg("-c:v").arg("libx264")
        .arg("-c:a").arg("aac")
        .arg("-preset").arg("ultrafast")
        .arg("-crf").arg("28")
        .arg("-maxrate").arg("2M")
        .arg("-bufsize").arg("4M")
        .arg("-pix_fmt").arg("yuv420p")
        .arg("-movflags").arg("+faststart")
        .arg("-threads").arg("0")
        .arg("-y")
        .arg(&output_video_path)
        .output()
        .await
        .context("Failed to execute ffmpeg command")?;
    
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        error!("FFmpeg failed with stderr: {}", stderr);
        return Err(anyhow::anyhow!("FFmpeg command failed: {}", stderr));
    }
    
    info!("Successfully transformed video to zoomed 9:16 format");
    Ok(())
}

async fn transform_video_to_916_with_borders(
    input_video_path: &std::path::PathBuf,
    output_video_path: &std::path::PathBuf,
) -> Result<()> {
    use tokio::process::Command;
    
    info!(
        "Transforming video to 9:16 with borders: {} -> {}",
        input_video_path.display(),
        output_video_path.display()
    );
    
    // Probe to get video dimensions
    let probe = Command::new("ffprobe")
        .arg("-v").arg("error")
        .arg("-select_streams").arg("v:0")
        .arg("-show_entries").arg("stream=width,height")
        .arg("-of").arg("csv=p=0")
        .arg(&input_video_path)
        .output()
        .await?;

    let dimensions = String::from_utf8_lossy(&probe.stdout);
    let parts: Vec<&str> = dimensions.trim().split(',').collect();
    if parts.len() != 2 {
        return Err(anyhow::anyhow!("Failed to get video dimensions"));
    }
    
    let original_width: i32 = parts[0].parse()?;
    let original_height: i32 = parts[1].parse()?;
    
    let target_width = 1080;
    let target_height = 1920;
    
    let target_aspect_ratio = target_width as f64 / target_height as f64;
    let original_aspect_ratio = original_width as f64 / original_height as f64;
    
    info!(
        "Original: {}x{}, Target: {}x{}, Original AR: {:.3}, Target AR: {:.3}",
        original_width, original_height, target_width, target_height,
        original_aspect_ratio, target_aspect_ratio
    );
    
    // Check if already in 9:16 format
    if (original_aspect_ratio - target_aspect_ratio).abs() < 0.01 {
        if original_width <= target_width && original_height <= target_height {
            info!("Video is already in correct 9:16 format, copying without re-encoding");
            let output = Command::new("ffmpeg")
                .arg("-i").arg(&input_video_path)
                .arg("-c:v").arg("copy")
                .arg("-c:a").arg("copy")
                .arg("-y")
                .arg(&output_video_path)
                .output()
                .await?;
            
            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr);
                return Err(anyhow::anyhow!("FFmpeg command failed: {}", stderr));
            }
            return Ok(());
        } else {
            info!("Video is 9:16 but too large, scaling down");
            let output = Command::new("ffmpeg")
                .arg("-i").arg(&input_video_path)
                .arg("-vf").arg(format!("scale={}:{}", target_width, target_height))
                .arg("-c:v").arg("libx264")
                .arg("-c:a").arg("aac")
                .arg("-preset").arg("ultrafast")
                .arg("-crf").arg("28")
                .arg("-maxrate").arg("2M")
                .arg("-bufsize").arg("4M")
                .arg("-pix_fmt").arg("yuv420p")
                .arg("-movflags").arg("+faststart")
                .arg("-threads").arg("0")
                .arg("-y")
                .arg(&output_video_path)
                .output()
                .await?;
            
            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr);
                return Err(anyhow::anyhow!("FFmpeg command failed: {}", stderr));
            }
            return Ok(());
        }
    }
    
    // Calculate scaled dimensions to fit within target while maintaining aspect ratio
    let (scale_width, scale_height) = if original_aspect_ratio > target_aspect_ratio {
        // Video is wider, fit to width
        let scale_width = target_width;
        let scale_height = (target_width as f64 / original_aspect_ratio) as i32;
        (scale_width, scale_height)
    } else {
        // Video is taller, fit to height
        let scale_height = target_height;
        let scale_width = (target_height as f64 * original_aspect_ratio) as i32;
        (scale_width, scale_height)
    };
    
    info!(
        "Scaled: {}x{}, Target: {}x{}, will add borders",
        scale_width, scale_height, target_width, target_height
    );
    
    // Scale and pad with black borders
    let vf_filter = format!(
        "scale={}:{},pad={}:{}:(ow-iw)/2:(oh-ih)/2:black",
        scale_width, scale_height, target_width, target_height
    );
    
    let output = Command::new("ffmpeg")
        .arg("-i").arg(&input_video_path)
        .arg("-vf").arg(&vf_filter)
        .arg("-c:v").arg("libx264")
        .arg("-c:a").arg("aac")
        .arg("-preset").arg("ultrafast")
        .arg("-crf").arg("28")
        .arg("-maxrate").arg("2M")
        .arg("-bufsize").arg("4M")
        .arg("-pix_fmt").arg("yuv420p")
        .arg("-movflags").arg("+faststart")
        .arg("-threads").arg("0")
        .arg("-y")
        .arg(&output_video_path)
        .output()
        .await
        .context("Failed to execute ffmpeg command")?;
    
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        error!("FFmpeg failed with stderr: {}", stderr);
        return Err(anyhow::anyhow!("FFmpeg command failed: {}", stderr));
    }
    
    info!("Successfully transformed video to 9:16 with borders");
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
