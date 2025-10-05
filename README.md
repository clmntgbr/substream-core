# Substream Core

A distributed video processing system built in Rust that handles video downloads, audio extraction, subtitle generation, and subtitle transformation using RabbitMQ for task orchestration.

## 🚀 Features

- **Distributed Task Processing**: Microservices architecture with RabbitMQ message queuing
- **Video Processing Pipeline**: Complete workflow from video download to subtitle generation
- **AssemblyAI Integration**: AI-powered subtitle generation from audio files
- **Docker Support**: Containerized services with health checks and auto-restart
- **Webhook Notifications**: Success/failure callbacks for task completion
- **S3 Storage**: Cloud storage integration for file management
- **Robust Error Handling**: Retry mechanisms and comprehensive logging

## 📋 Prerequisites

- Docker and Docker Compose
- Rust 1.70+ (for local development)
- RabbitMQ server
- AWS S3 credentials (for file storage)
- AssemblyAI API key (for subtitle generation)

## 🏃‍♂️ Quick Start

### Using Docker (Recommended)

```bash
# Clone and setup
git clone <repository-url>
cd substream-core

# Copy environment template
cp .env.example .env
# Edit .env with your configuration

# Start all services
make up
```

### Local Development

```bash
# Setup environment
cp .env.example .env
# Edit .env with your configuration

# Build the project
cargo build

# Run individual services (in separate terminals)
make dev-video    # task-get-video
make dev-sound    # task-extract-sound
# Or run all services with hot reload
make dev-docker
```

## 🏗️ Architecture

The system consists of four microservices that process tasks in a pipeline:

```
Video URL → Get Video → Extract Sound → Generate Subtitle → Transform Subtitle
```

### Services Overview

| Service | Queue | Description | Processing Time |
|---------|-------|-------------|-----------------|
| `task-get-video` | `core.get_video` | Downloads videos from URLs | ~5s (simulated) |
| `task-extract-sound` | `core.extract_sound` | Extracts audio from video files | ~3s (simulated) |
| `task-generate-subtitle` | `core.generate_subtitle` | Generates subtitles using AssemblyAI | Variable |
| `task-transform-subtitle` | `core.transform_subtitle` | Transforms subtitle formats | ~1s (simulated) |
| `task-resize-video` | `core.resize_video` | Resizes videos to specified format | Variable |

## 📨 Message Format

All tasks use a consistent message structure:

```json
{
  "task_id": "550e8400-e29b-41d4-a716-446655440000",
  "payload": {
    // Task-specific payload (see below)
  },
  "webhook_url_success": "https://your-app.com/webhook/success",
  "webhook_url_failure": "https://your-app.com/webhook/failure"
}
```

### Task-Specific Payloads

#### Get Video Task
```json
{
  "url": "https://example.com/video.mp4",
  "stream_id": "unique-stream-identifier"
}
```

#### Extract Sound Task
```json
{
  "file_name": "video.mp4",
  "stream_id": "unique-stream-identifier"
}
```

#### Generate Subtitle Task
```json
{
  "stream_id": "unique-stream-identifier",
  "audio_files": ["audio1.wav", "audio2.wav"]
}
```

#### Transform Subtitle Task
```json
{
  "stream_id": "unique-stream-identifier",
  "subtitle_srt_file_name": "subtitles.srt"
}
```

#### Resize Video Task
```json
{
  "stream_id": "unique-stream-identifier",
  "file_name": "video.mp4",
  "format": "720p"
}
```

## 🔧 Configuration

### Environment Variables

Create a `.env` file with the following variables:

```bash
# RabbitMQ Configuration
RABBITMQ_HOST=localhost
RABBITMQ_PORT=5672
RABBITMQ_USER=guest
RABBITMQ_PASSWORD=guest
RABBITMQ_VHOST=/

# AWS S3 Configuration
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_REGION=us-east-1
S3_BUCKET_NAME=your-bucket-name

# AssemblyAI Configuration
ASSEMBLYAI_API_KEY=your_assemblyai_api_key

# Logging
RUST_LOG=info
```

## 🛠️ Development

### Available Commands

```bash
# Development
make dev              # Run services locally (no Docker)
make dev-docker       # Run with Docker + hot reload
make dev-video        # Run only video task service
make dev-sound        # Run only sound extraction service
make dev-resize       # Run only video resize service

# Production
make prod             # Build and run with Docker
make build            # Build Docker images
make up               # Start Docker services
make down             # Stop Docker services

# Utilities
make logs             # View all service logs
make logs-video       # View video service logs
make logs-sound       # View sound service logs
make logs-resize      # View resize service logs
make clean            # Clean build artifacts
make test             # Run tests
make check            # Check code without building
make fmt              # Format code
make clippy           # Run linter
```

### Project Structure

```
substream-core/
├── shared/                    # Common library with message types and utilities
│   ├── src/lib.rs            # Shared structures and RabbitMQ client
│   └── Cargo.toml
├── task-get-video/           # Video download service
├── task-extract-sound/       # Audio extraction service
├── task-generate-subtitle/   # Subtitle generation service
├── task-transform-subtitle/  # Subtitle transformation service
├── task-resize-video/        # Video resize service
├── docker-compose.yml        # Production Docker setup
├── docker-compose.dev.yml    # Development Docker setup
└── Makefile                  # Development commands
```

## 🔄 Workflow Example

1. **Submit a video processing task**:
   ```bash
   # Send message to RabbitMQ queue 'core.get_video'
   {
     "task_id": "uuid",
     "payload": {
       "url": "https://example.com/video.mp4",
       "stream_id": "stream-123"
     },
     "webhook_url_success": "https://your-app.com/success",
     "webhook_url_failure": "https://your-app.com/failure"
   }
   ```

2. **Processing pipeline**:
   - `task-get-video` downloads the video and stores it in S3
   - `task-extract-sound` extracts audio from the video
   - `task-generate-subtitle` generates subtitles using AssemblyAI
   - `task-transform-subtitle` processes the subtitle format

3. **Webhook notifications**:
   - Success: Contains the final subtitle file information
   - Failure: Contains error details and retry information

## 🐳 Docker

### Production Deployment

```bash
# Build and start all services
make prod

# View logs
make logs

# Stop services
make down
```

### Development with Hot Reload

```bash
# Start development environment
make dev-docker

# View logs
make dev-docker-logs

# Stop development environment
make dev-docker-down
```

## 🧪 Testing

```bash
# Run all tests
make test

# Check code quality
make check
make clippy
make fmt
```

## 📊 Monitoring

Each service includes:
- Health checks every 30 seconds
- Automatic restart on failure
- Comprehensive logging with structured output
- Webhook notifications for task completion

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run tests and linting: `make test && make clippy`
5. Submit a pull request

## 📄 License

[Add your license information here]

## 🆘 Support

For issues and questions:
- Check the logs: `make logs`
- Verify environment configuration
- Ensure RabbitMQ is running and accessible
- Check AWS S3 credentials and permissions
- Verify AssemblyAI API key is valid
