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
	@echo "  cargo run --package task-download-video"
	@echo "  cargo run --package task-extract-sound"
	@echo "  cargo run --package task-transcript-audio"
	@echo "  cargo run --package task-analyse-image"

dev-video:
	cargo run --package task-download-video

dev-sound:
	cargo run --package task-extract-sound

dev-transcript-audio:
	cargo run --package task-transcript-audio

dev-analyse-image:
	cargo run --package task-analyse-image

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

logs-download-video:
	docker-compose logs -f task-download-video

logs-extract-sound:
	docker-compose logs -f task-extract-sound

logs-transcript-audio:
	docker-compose logs -f task-transcript-audio

logs-analyse-image:
	docker-compose logs -f task-analyse-image

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

