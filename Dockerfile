FROM rust:1.94-slim-trixie AS builder

WORKDIR /app/mesastream

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    pkg-config \
    libssl-dev \
    libopus-dev \
    && rm -rf /var/lib/apt/lists/*

# COPY ./lib /app/lib

COPY ./Cargo.toml ./Cargo.lock ./
COPY ./src/main.rs ./src/main.rs

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    cargo fetch

COPY ./src ./src

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/cargo-target \
    CARGO_TARGET_DIR=/cargo-target cargo build --release \
    && cp /cargo-target/release/mesastream /usr/local/bin/mesastream

FROM debian:trixie-slim AS runtime

WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    ca-certificates \
    libopus0 \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Download latest yt-dlp from GitHub releases
RUN curl -L https://github.com/yt-dlp/yt-dlp/releases/latest/download/yt-dlp -o /usr/local/bin/yt-dlp \
    && chmod a+rx /usr/local/bin/yt-dlp

COPY --from=builder /usr/local/bin/mesastream /usr/local/bin/mesastream

EXPOSE 8080
CMD ["/usr/local/bin/mesastream"]
