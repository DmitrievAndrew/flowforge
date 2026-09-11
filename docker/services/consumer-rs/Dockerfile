# ---------- Stage 1: build ----------
FROM rust:1.98-slim-bookworm AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    librdkafka-dev \
    cmake \
    build-essential \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

COPY consumer-rs/Cargo.toml consumer-rs/Cargo.lock* ./
COPY consumer-rs/src ./src

RUN cargo build --release

# ---------- Stage 2: runtime ----------
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    librdkafka1 \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/flowforge-consumer /usr/local/bin/flowforge-consumer

CMD ["flowforge-consumer"]
