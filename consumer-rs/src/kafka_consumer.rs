use std::collections::HashMap;

use anyhow::Result;
use chrono::{NaiveDateTime, Utc};
use rdkafka::Message;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use tracing::{error, info, warn};

use crate::config::Config;
use crate::indicators::RollingState;
use crate::models::{Aggregate, Candle};
use crate::mongo_writer::MongoWriter;

/// Парсит строку `begin` свечи в Unix-время (секунды, с миллисекундной точностью).
/// Ожидаемый формат: "YYYY-MM-DD HH:MM:SS".
fn parse_candle_time(begin: &str) -> Option<f64> {
    NaiveDateTime::parse_from_str(begin, "%Y-%m-%d %H:%M:%S")
        .ok()
        .map(|dt| dt.and_utc().timestamp_millis() as f64 / 1000.0)
}

pub async fn run(config: &Config) -> Result<()> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &config.kafka_brokers)
        .set("group.id", &config.kafka_group_id)
        .set("auto.offset.reset", "latest")
        .set("enable.auto.commit", "false")
        .create()?;

    let topics = [config.kafka_topic.as_str()];
    consumer.subscribe(&topics)?;
    info!("Subscribed to Kafka topic: {}", config.kafka_topic);

    let mongo = MongoWriter::new(
        &config.mongodb_uri,
        &config.mongodb_database,
        &config.mongodb_collection,
    )
    .await?;
    info!(
        "Connected to MongoDB: {}/{}",
        config.mongodb_database, config.mongodb_collection
    );

    let mut states: HashMap<String, RollingState> = HashMap::new();
    let window_size = 20usize;

    loop {
        let msg = match consumer.recv().await {
            Ok(m) => m,
            Err(e) => {
                error!("Kafka recv error: {}", e);
                continue;
            }
        };

        let payload = match msg.payload() {
            Some(p) => p,
            None => continue,
        };

        let json_str = match std::str::from_utf8(payload) {
            Ok(s) => s,
            Err(e) => {
                warn!("Invalid UTF-8 in Kafka message: {}", e);
                continue;
            }
        };

        let candle: Candle = match serde_json::from_str(json_str) {
            Ok(c) => c,
            Err(e) => {
                warn!("Failed to parse candle JSON: {}", e);
                continue;
            }
        };

        // Обновляем скользящее состояние
        let state = states
            .entry(candle.ticker.clone())
            .or_insert_with(|| RollingState::new(window_size));
        state.push(candle.close, candle.volume, candle.value);

        // --- Признаки по скользящему окну ---
        let sma_5 = state.sma(5);
        let sma_20 = state.sma(20);
        let volatility = state.volatility(5).unwrap_or(0.0);
        let avg_volume = state.avg_volume(5).unwrap_or(candle.volume as f64);
        let avg_value = state.avg_value(5).unwrap_or(candle.value);
        let vwap = state.vwap(5);

        // --- Производные от одной свечи ---
        let body = candle.close - candle.open;
        let body_pct = if candle.open != 0.0 {
            body / candle.open * 100.0
        } else {
            0.0
        };
        let range = candle.high - candle.low;
        let range_pct = if candle.close != 0.0 {
            range / candle.close * 100.0
        } else {
            0.0
        };
        let upper_shadow = candle.high - candle.open.max(candle.close);
        let lower_shadow = candle.open.min(candle.close) - candle.low;

        // --- Метрики пайплайна ---
        let latency_ms = parse_candle_time(&candle.begin).map(|t| {
            if candle.fetched_at > 0.0 {
                (candle.fetched_at - t) * 1000.0
            } else {
                0.0
            }
        });

        let agg = Aggregate {
            ticker: candle.ticker.clone(),
            window_start: candle.begin.clone(),
            window_end: candle.end.clone(),

            open: candle.open,
            close: candle.close,
            high: candle.high,
            low: candle.low,
            volume: candle.volume,
            value: candle.value,

            body,
            body_pct,
            range,
            range_pct,
            upper_shadow,
            lower_shadow,

            sma_5,
            sma_20,
            volatility,
            avg_volume,
            avg_value,
            vwap,

            latency_ms,

            processed_at: Utc::now().to_rfc3339(),
        };

        if let Err(e) = mongo.insert(&agg).await {
            error!("MongoDB insert failed for {}: {}", candle.ticker, e);
        } else {
            info!(
                "Processed {} {} close={:.2} body={:+.2} rng={:.2} vwap={:?} lat={:?}ms",
                candle.ticker, candle.begin, candle.close, body, range, vwap, latency_ms
            );
        }

        if let Err(e) = consumer.commit_message(&msg, CommitMode::Async) {
            warn!("Kafka commit failed: {}", e);
        }
    }
}
