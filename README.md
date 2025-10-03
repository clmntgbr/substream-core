# Substream Core

Rust task processing system with RabbitMQ.

## Quick Start

```bash
cp .env.dist .env
make docker-dev
```

## Services

- `task-get-video`: Queue `core.get_video` - Downloads videos (5s simulation)
- `task-extract-sound`: Queue `core.extract_sound` - Extracts audio (3s simulation)
- `task-generate-subtitle`: Queue `core.generate_subtitle` - Generates subtitles from audio files using AssemblyAI
- `task-transform-subtitle`: Queue `core.transform_subtitle` - Transforms or post-processes subtitle files (e.g., format conversion)


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
