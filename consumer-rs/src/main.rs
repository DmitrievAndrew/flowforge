mod config;
mod indicators;
mod kafka_consumer;
mod models;
mod mongo_writer;

use anyhow::Result;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    // Логирование
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    // Читаем .env (если есть) и переменные окружения
    let _ = dotenvy::dotenv();

    let config = config::Config::from_env()?;
    info!("Starting FlowForge consumer");
    info!("  kafka: {}", config.kafka_brokers);
    info!("  topic: {}", config.kafka_topic);
    info!("  mongodb: {}/{}", config.mongodb_database, config.mongodb_collection);

    kafka_consumer::run(&config).await
}
