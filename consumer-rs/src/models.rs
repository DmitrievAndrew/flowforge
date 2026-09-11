use serde::{Deserialize, Serialize};

/// Свеча с MOEX. Все поля из JSON продюсера — забираем как есть,
/// даже если сейчас не все используются напрямую: расширяем пул признаков.
#[derive(Debug, Clone, Deserialize)]
pub struct Candle {
    pub open: f64,
    pub close: f64,
    pub high: f64,
    pub low: f64,
    pub value: f64,
    pub volume: i64,
    pub begin: String,
    pub end: String,
    pub ticker: String,
    #[serde(default)]
    pub fetched_at: f64,
}

/// Расширенный набор признаков, вычисленных из свечи и скользящего окна.
#[derive(Debug, Clone, Serialize)]
pub struct Aggregate {
    // --- Идентификация ---
    pub ticker: String,
    pub window_start: String,
    pub window_end: String,

    // --- Сырые OHLCV ---
    pub open: f64,
    pub close: f64,
    pub high: f64,
    pub low: f64,
    pub volume: i64,
    pub value: f64,

    // --- Производные от свечи (технический анализ) ---
    pub body: f64,         // close - open
    pub body_pct: f64,     // (close - open) / open * 100
    pub range: f64,        // high - low
    pub range_pct: f64,    // (high - low) / close * 100
    pub upper_shadow: f64, // high - max(open, close)
    pub lower_shadow: f64, // min(open, close) - low

    // --- Агрегаты по скользящему окну ---
    pub sma_5: Option<f64>,
    pub sma_20: Option<f64>,
    pub volatility: f64,   // stddev(close) за 5
    pub avg_volume: f64,   // avg(volume) за 5
    pub avg_value: f64,    // avg(value) за 5
    pub vwap: Option<f64>, // sum(value) / sum(volume) за 5

    // --- Метрики пайплайна ---
    pub latency_ms: Option<f64>, // задержка от begin свечи до получения

    // --- Служебное ---
    pub processed_at: String,
}
