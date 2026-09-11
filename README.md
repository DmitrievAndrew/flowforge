# FlowForge

**Real-time market intelligence & ML platform for algorithmic trading.**

FlowForge ingests live market data from MOEX ISS, processes it in real-time with a Rust-based stream consumer, stores features in a unified Feature Store, and serves ML predictions for trading signals.

---

## Important: Model Availability

This public repository contains the complete infrastructure and pipeline:

- Kafka-based event streaming
- Rust stream consumer (Kafka -> MongoDB)
- Python data producer (MOEX ISS -> Kafka)
- ClickHouse analytics layer
- Redis online feature store
- Feast feature store definitions
- Grafana dashboards

**The trained ML model and training code are NOT included** - they are proprietary.

- For **personal, non-commercial use**: contact [dmitriev.andrew13@yandex.ru] to request access.
- For **commercial use**: see [COMMERCIAL_LICENSE.md](COMMERCIAL_LICENSE.md).

Without the model, the pipeline still works end-to-end (data flows, features are computed, dashboards update). Only the final ML prediction step requires the model.

---

## Quick Start

**Requirements:** Docker and Docker Compose. Nothing else.

    # 1. Clone
    git clone https://github.com/YOUR_USERNAME/flowforge.git
    cd flowforge

    # 2. Configure (defaults work out of the box)
    cp docker/.env.example docker/.env

    # 3. Start everything
    make up

    # 4. Initialize Kafka topics and MongoDB indexes
    make init-all

That's it. The whole pipeline runs in Docker:

- Python producer reads MOEX ISS and publishes to Kafka
- Rust consumer reads from Kafka and writes aggregates to MongoDB
- Grafana and Kafka UI are available for monitoring

### What you get

| Service | URL | Credentials |
|---------|-----|-------------|
| Kafka UI | http://localhost:8080 | - |
| Grafana | http://localhost:3000 | admin / admin |
| ClickHouse HTTP | http://localhost:8123 | default / clickhouse |
| MongoDB | localhost:27017 | admin / password |
| Redis | localhost:6379 | password: redispass |

### Useful commands

    make logs              # all logs
    make logs-producer     # producer logs only
    make logs-consumer     # Rust consumer logs only
    make status            # container status
    make build             # build producer and consumer images
    make rebuild           # rebuild without cache + restart
    make down              # stop everything
    make clean             # stop and wipe all data
    ---
    
    ## Architecture
    
        MOEX ISS API
             |
             v
        [ Python Producer ]  -- raw-trades -->  [ Kafka ]
                                                     |
                                                     v
                                             [ Rust Consumer ]
                                                     |
                             +-----------------------+-----------------------+
                             |                       |                       |
                             v                       v                       v
                        [ MongoDB ]           [ ClickHouse ]           [ Redis ]
                        (raw events,          (aggregates,             (online
                         audit,                offline store            feature
                         predictions)          for Feast)               store)
                                                     |
                                                     v
                                           [ Feast Feature Store ]
                                                     |
                                                     v
                                           [ ML Model (XGBoost/ONNX) ]
    
    Detailed architecture: [docs/architecture.md](docs/architecture.md)
    
    ---
    
    ## Tech Stack
    
    | Layer | Technology | Purpose |
    |-------|------------|---------|
    | Data source | MOEX ISS REST API | Russian equities market data |
    | Event bus | Apache Kafka | Reliable event streaming |
    | Stream consumer | Rust (rdkafka, tokio, mongodb) | Low-latency processing |
    | Data producer | Python (requests, kafka-python) | Data ingestion |
    | Raw storage | MongoDB | Audit log, raw events |
    | Analytics DB | ClickHouse | Aggregates, feature offline store |
    | Online store | Redis | Low-latency feature serving |
    | Feature Store | Feast | Unified features for train/serve |
    | Monitoring | Grafana | Dashboards |
    | Orchestration | Docker Compose | One-command startup |
    
    **Why Rust for the consumer?** For a load of 15 tickers x 1 candle/min (~500 events/min), Apache Spark is overkill: 2+ GB RAM, 100+ ms latency, JVM overhead. Rust gives < 10 MB memory, < 1 ms latency, instant startup, and a single static binary.
    ---
    
    ## Project Structure
    
        flowforge/
        |-- consumer-rs/              # Rust stream consumer
        |   |-- Cargo.toml
        |   +-- src/
        |       |-- main.rs
        |       |-- config.rs
        |       |-- models.rs
        |       |-- indicators.rs
        |       |-- kafka_consumer.rs
        |       +-- mongo_writer.rs
        |-- src/                      # Python code
        |   |-- producers/            # MOEX data producer
        |   |-- feature_store/        # Feast definitions (planned)
        |   |-- ml/                   # Training & inference (planned)
        |   +-- common/               # Shared utilities
        |-- docker/
        |   |-- docker-compose.yml
        |   +-- services/
        |       |-- producer/         # Python producer Dockerfile
        |       +-- consumer-rs/      # Rust consumer Dockerfile
        |-- requirements/
        |   |-- producer.txt          # Producer dependencies only
        |   |-- ml.txt                # ML / Feast / inference
        |   |-- dev.txt               # Local development
        |   +-- all.txt               # CI (meta-file)
        |-- config/
        |   +-- tickers.txt           # List of tickers to track
        |-- docs/
        |   |-- architecture.md
        |   |-- deployment_guide.md
        |   |-- monitoring_guide.md
        |   +-- disaster_recovery.md
        |-- scripts/                  # Init scripts
        |-- Makefile
        +-- README.md
    
    ---
    
    ## License
    
    MIT - see [LICENSE](LICENSE).
    
    **Note:** The MIT license applies only to the source code. The trained model, training scripts, and related IP are **not** covered and are subject to separate terms.
    
    ---
    
    ## Contributing
    
    Issues and pull requests are welcome for infrastructure and pipeline components.
    
    ---
    
    ## Status
    
    | Stage | Component | Status |
    |-------|-----------|--------|
    | 0 | Infrastructure (Docker Compose) | Done |
    | 1 | Producer (MOEX -> Kafka) | Done |
    | 2 | Consumer (Kafka -> MongoDB) | Done (Rust) |
    | 3 | ClickHouse analytics layer | In progress |
    | 4 | Feature Store (Feast) | Planned |
    | 5 | ML training | Planned |
    | 6 | Real-time inference | Planned |
    | 7 | Risk management | Planned |
    | 8 | Monitoring & alerting | Planned |
    | 9 | Orchestration (Airflow) | Planned |
    | 10 | VPS deployment | Planned |
