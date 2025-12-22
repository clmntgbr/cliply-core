# Build stage
FROM rust:latest as builder

WORKDIR /app

# Install build dependencies
RUN apt-get update && \
    apt-get install -y pkg-config libssl-dev && \
    rm -rf /var/lib/apt/lists/*

# Copy workspace files
COPY Cargo.toml ./
COPY shared ./shared
COPY task-download-video ./task-download-video
COPY task-extract-sound ./task-extract-sound


# Build all packages in workspace
RUN cargo build --release

# Runtime stage
FROM debian:bookworm-slim

WORKDIR /app

# Install runtime dependencies including ffmpeg for video/audio processing
RUN apt-get update && \
    apt-get install -y \
    ca-certificates \
    libssl3 \
    ffmpeg \
    curl \
    python3 \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

# Install yt-dlp
RUN curl -L https://github.com/yt-dlp/yt-dlp/releases/latest/download/yt-dlp -o /usr/local/bin/yt-dlp && \
    chmod a+rx /usr/local/bin/yt-dlp

# Create non-root user
RUN useradd -m -u 1000 appuser && \
    chown -R appuser:appuser /app

# Copy all binaries from builder
COPY --from=builder /app/target/release/task-download-video /app/task-download-video
COPY --from=builder /app/target/release/task-extract-sound /app/task-extract-sound
COPY --from=builder /app/target/release/task-transcript-audio /app/task-transcript-audio

# Switch to non-root user
USER appuser
