# FlowForge

**Real-time market intelligence & ML platform**

> ⚠️ **Important:** This repository contains the complete infrastructure and pipeline components.
> The trained ML model and training code are **not included** (proprietary).

## Quick Start

1. Copy `.env.example` to `.env` and adjust variables.
2. Run `make up` to start all services.
3. Run `make init-all` to create Kafka topics, ClickHouse tables, and MongoDB indexes.
4. Access:
   - Kafka UI: http://localhost:8080
   - Grafana: http://localhost:3000 (admin/admin)

See [docs/](docs/) for detailed documentation.

## License

MIT License – see [LICENSE](LICENSE) file.
