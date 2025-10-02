# Substream Core

Rust task processing system with RabbitMQ.

## Quick Start

```bash
# 1. Configure network connection to your PHP project's RabbitMQ
./setup-network.sh

# 2. Start services
cp .env.example .env
docker-compose up --build
```

## Services

- `task-get-video`: Queue `core.get_video` - Downloads videos (5s simulation)
- `task-extract-sound`: Queue `core.extract_sound` - Extracts audio (3s simulation)

## Message Format

```json
{
  "task_id": "uuid",
  "payload": {
    "url": "https://example.com/video.mp4"  // or "video_path" for extract_sound
  },
  "webhook_url": "https://your-callback-url"
}
```

## Environment Variables

See `.env.example` for configuration options.
