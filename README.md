# Substream Core

Rust task processing system with RabbitMQ.

## Quick Start

```bash
cp .env.dist .env
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
    "url": "https://example.com/video.mp4"
  },
  "webhook_url_success": "https://your-callback-url",
  "webhook_url_failure": "https://your-callback-url",
}
```

## Environment Variables

See `.env.example` for configuration options.
