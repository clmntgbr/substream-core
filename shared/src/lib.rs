use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use lapin::{
    options::*, types::FieldTable, Channel, Connection, ConnectionProperties, Consumer,
};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tracing::{error, info, warn};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskMessage {
    pub task_id: Uuid,
    pub payload: serde_json::Value,
    pub webhook_url_success: String,
    pub webhook_url_failure: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebhookResponse {
    pub task_id: Uuid,
    pub task_type: String,
    pub status: TaskStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    pub completed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum TaskStatus {
    Success,
    Error,
}

#[derive(Debug, Clone)]
pub struct RabbitMQConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub vhost: String,
    pub max_retries: u32,
    pub retry_delay: Duration,
}

impl Default for RabbitMQConfig {
    fn default() -> Self {
        Self {
            host: "localhost".to_string(),
            port: 5672,
            user: "guest".to_string(),
            password: "guest".to_string(),
            vhost: "/".to_string(),
            max_retries: 3,
            retry_delay: Duration::from_secs(5),
        }
    }
}

impl RabbitMQConfig {
    pub fn from_env() -> Self {
        Self {
            host: std::env::var("RABBITMQ_HOST").unwrap_or("localhost".to_string()),
            port: std::env::var("RABBITMQ_PORT")
                .unwrap_or("5672".to_string())
                .parse()
                .unwrap_or(5672),
            user: std::env::var("RABBITMQ_USER").unwrap_or("guest".to_string()),
            password: std::env::var("RABBITMQ_PASSWORD").unwrap_or("guest".to_string()),
            vhost: std::env::var("RABBITMQ_VHOST").unwrap_or("/".to_string()),
            max_retries: std::env::var("MAX_RETRIES")
                .unwrap_or("3".to_string())
                .parse()
                .unwrap_or(3),
            retry_delay: Duration::from_secs(
                std::env::var("RETRY_DELAY_SECONDS")
                    .unwrap_or("5".to_string())
                    .parse()
                    .unwrap_or(5),
            ),
        }
    }

    pub fn connection_url(&self) -> String {
        format!(
            "amqp://{}:{}@{}:{}/{}",
            self.user, self.password, self.host, self.port, self.vhost
        )
    }
}

/// RabbitMQ client for queue operations
pub struct RabbitMQClient {
    _connection: Connection,
    channel: Channel,
    _config: RabbitMQConfig,
}

impl RabbitMQClient {
    /// Create a new RabbitMQ client with retry logic
    pub async fn new(config: RabbitMQConfig) -> Result<Self> {
        let mut last_error = None;

        for attempt in 1..=config.max_retries {
            info!(
                "Attempting to connect to RabbitMQ (attempt {}/{})",
                attempt, config.max_retries
            );

            match Self::connect(&config).await {
                Ok((connection, channel)) => {
                    info!("Successfully connected to RabbitMQ");
                    return Ok(Self {
                        _connection: connection,
                        channel,
                        _config: config,
                    });
                }
                Err(e) => {
                    warn!(
                        "Failed to connect to RabbitMQ (attempt {}/{}): {}",
                        attempt, config.max_retries, e
                    );
                    last_error = Some(e);
                    if attempt < config.max_retries {
                        tokio::time::sleep(config.retry_delay).await;
                    }
                }
            }
        }

        Err(last_error.unwrap_or_else(|| anyhow::anyhow!("Failed to connect to RabbitMQ")))
    }

    async fn connect(config: &RabbitMQConfig) -> Result<(Connection, Channel)> {
        let connection = Connection::connect(
            &config.connection_url(),
            ConnectionProperties::default(),
        )
        .await
        .context("Failed to create RabbitMQ connection")?;

        let channel = connection
            .create_channel()
            .await
            .context("Failed to create channel")?;

        Ok((connection, channel))
    }

    /// Declare a queue and start consuming messages
    pub async fn consume(&self, queue_name: &str) -> Result<Consumer> {
        // Declare the queue (create if doesn't exist)
        self.channel
            .queue_declare(
                queue_name,
                QueueDeclareOptions {
                    durable: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await
            .context("Failed to declare queue")?;

        info!("Queue '{}' declared successfully", queue_name);

        // Start consuming
        let consumer = self
            .channel
            .basic_consume(
                queue_name,
                "consumer",
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await
            .context("Failed to start consuming")?;

        info!("Started consuming from queue '{}'", queue_name);

        Ok(consumer)
    }

    /// Get the channel reference
    pub fn channel(&self) -> &Channel {
        &self.channel
    }
}

/// HTTP client for sending webhook notifications
pub struct WebhookClient {
    client: reqwest::Client,
}

impl WebhookClient {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
                .unwrap_or_default(),
        }
    }

    /// Send a webhook notification
    pub async fn send(&self, url: &str, response: &WebhookResponse) -> Result<()> {
        info!(
            "Sending webhook to {} for task {} with status {:?}",
            url, response.task_id, response.status
        );

        match self.client.post(url).json(response).send().await {
            Ok(resp) => {
                if resp.status().is_success() {
                    info!("Webhook sent successfully to {}", url);
                    Ok(())
                } else {
                    let status = resp.status();
                    let body = resp.text().await.unwrap_or_default();
                    error!(
                        "Webhook failed with status {}: {}",
                        status, body
                    );
                    Err(anyhow::anyhow!(
                        "Webhook request failed with status {}",
                        status
                    ))
                }
            }
            Err(e) => {
                error!("Failed to send webhook to {}: {}", url, e);
                Err(anyhow::anyhow!("Failed to send webhook: {}", e))
            }
        }
    }

    /// Send a success webhook
    pub async fn send_success(
        &self,
        url: &str,
        task_id: Uuid,
        task_type: &str,
        result: serde_json::Value,
    ) -> Result<()> {
        let response = WebhookResponse {
            task_id,
            task_type: task_type.to_string(),
            status: TaskStatus::Success,
            result: Some(result),
            error: None,
            completed_at: Utc::now(),
        };

        self.send(url, &response).await
    }

    /// Send an error webhook
    pub async fn send_error(
        &self,
        url: &str,
        task_id: Uuid,
        task_type: &str,
        error: &str,
    ) -> Result<()> {
        let response = WebhookResponse {
            task_id,
            task_type: task_type.to_string(),
            status: TaskStatus::Error,
            result: None,
            error: Some(error.to_string()),
            completed_at: Utc::now(),
        };

        self.send(url, &response).await
    }
}

impl Default for WebhookClient {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rabbitmq_config_connection_url() {
        let config = RabbitMQConfig {
            host: "localhost".to_string(),
            port: 5672,
            user: "admin".to_string(),
            password: "secret".to_string(),
            vhost: "/test".to_string(),
            max_retries: 3,
            retry_delay: Duration::from_secs(5),
        };

        assert_eq!(
            config.connection_url(),
            "amqp://admin:secret@localhost:5672//test"
        );
    }

    #[test]
    fn test_task_message_serialization() {
        let msg = TaskMessage {
            task_id: Uuid::new_v4(),
            payload: serde_json::json!({"url": "https://example.com/video.mp4"}),
            webhook_url: "https://example.com/webhook".to_string(),
        };

        let json = serde_json::to_string(&msg).unwrap();
        let deserialized: TaskMessage = serde_json::from_str(&json).unwrap();

        assert_eq!(msg.task_id, deserialized.task_id);
        assert_eq!(msg.webhook_url, deserialized.webhook_url);
    }

    #[test]
    fn test_webhook_response_success() {
        let response = WebhookResponse {
            task_id: Uuid::new_v4(),
            task_type: "get_video".to_string(),
            status: TaskStatus::Success,
            result: Some(serde_json::json!({"path": "/videos/test.mp4"})),
            error: None,
            completed_at: Utc::now(),
        };

        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("success"));
        assert!(!json.contains("error"));
    }

    #[test]
    fn test_webhook_response_error() {
        let response = WebhookResponse {
            task_id: Uuid::new_v4(),
            task_type: "extract_sound".to_string(),
            status: TaskStatus::Error,
            result: None,
            error: Some("File not found".to_string()),
            completed_at: Utc::now(),
        };

        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("error"));
        assert!(json.contains("File not found"));
    }
}

