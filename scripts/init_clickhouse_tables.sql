CREATE DATABASE IF NOT EXISTS flowforge;

CREATE TABLE IF NOT EXISTS flowforge.minute_aggregates
(
    ticker String,
    timestamp DateTime,
    open Float64,
    high Float64,
    low Float64,
    close Float64,
    volume UInt64,
    rsi Float32,
    macd Float32,
    sma_20 Float32,
    volatility Float32
) ENGINE = ReplacingMergeTree()
ORDER BY (ticker, timestamp)
PARTITION BY toYYYYMM(timestamp);

CREATE TABLE IF NOT EXISTS flowforge.predictions_log
(
    ticker String,
    timestamp DateTime,
    model_version String,
    predicted_class Int8,
    probability Float32,
    actual_class Nullable(Int8),
    trade_executed UInt8,
    pnl Float64
) ENGINE = MergeTree()
ORDER BY (ticker, timestamp)
PARTITION BY toYYYYMM(timestamp);

CREATE TABLE IF NOT EXISTS flowforge.feature_store
(
    ticker String,
    event_timestamp DateTime,
    feature_name String,
    feature_value Float64,
    version UInt8 DEFAULT 1
) ENGINE = ReplacingMergeTree(version)
ORDER BY (ticker, event_timestamp, feature_name);
