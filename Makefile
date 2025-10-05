.PHONY: help dev dev-docker prod build up down logs clean test

help:
	@echo "Available commands:"
	@echo "  make dev          - Run services locally (no Docker)"
	@echo "  make dev-docker   - Run with Docker + hot reload"
	@echo "  make prod         - Build and run with Docker"
	@echo "  make build        - Build Docker images"
	@echo "  make up           - Start Docker services"
	@echo "  make down         - Stop Docker services"
	@echo "  make logs         - View Docker logs"
	@echo "  make clean        - Clean build artifacts"
	@echo "  make test         - Run tests"

# Development (local, no Docker)
dev:
	@echo "Starting development mode..."
	@echo "Make sure your RabbitMQ is running!"
	@cp -n .env.example .env 2>/dev/null || true
	@cargo build
	@echo "Run these commands in separate terminals:"
	@echo "  cargo run --package task-get-video"
	@echo "  cargo run --package task-extract-sound"

dev-video:
	cargo run --package task-get-video

dev-sound:
	cargo run --package task-extract-sound

dev-resize:
	cargo run --package task-resize-video

# Development with Docker (hot reload)
dev-docker:
	@cp -n .env.example .env 2>/dev/null || true
	docker-compose -f docker-compose.dev.yml up --build

dev-docker-down:
	docker-compose -f docker-compose.dev.yml down

dev-docker-logs:
	docker-compose -f docker-compose.dev.yml logs -f

# Production (Docker)
prod: build up

build:
	docker-compose build

up:
	@cp -n .env.example .env 2>/dev/null || true
	docker-compose up -d

down:
	docker-compose down

restart:
	docker-compose restart

logs:
	docker-compose logs -f

logs-video:
	docker-compose logs -f task-get-video

logs-sound:
	docker-compose logs -f task-extract-sound

logs-resize:
	docker-compose logs -f task-resize-video

# Utilities
clean:
	cargo clean
	docker-compose down -v

test:
	cargo test

check:
	cargo check --all

fmt:
	cargo fmt --all

clippy:
	cargo clippy --all -- -D warnings

