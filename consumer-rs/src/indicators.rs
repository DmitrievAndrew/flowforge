use std::collections::VecDeque;

/// Скользящее состояние по одному тикеру.
/// Хранит последние N цен закрытия, объёмов и оборотов (value).
pub struct RollingState {
    closes: VecDeque<f64>,
    volumes: VecDeque<f64>,
    values: VecDeque<f64>,
    max_size: usize,
}

impl RollingState {
    pub fn new(max_size: usize) -> Self {
        Self {
            closes: VecDeque::with_capacity(max_size),
            volumes: VecDeque::with_capacity(max_size),
            values: VecDeque::with_capacity(max_size),
            max_size,
        }
    }

    /// Добавляет новое наблюдение, вытесняя самое старое при переполнении.
    pub fn push(&mut self, close: f64, volume: i64, value: f64) {
        if self.closes.len() == self.max_size {
            self.closes.pop_front();
            self.volumes.pop_front();
            self.values.pop_front();
        }
        self.closes.push_back(close);
        self.volumes.push_back(volume as f64);
        self.values.push_back(value);
    }

    /// Простая скользящая средняя по ценам закрытия.
    pub fn sma(&self, window: usize) -> Option<f64> {
        if self.closes.len() < window {
            return None;
        }
        let start = self.closes.len() - window;
        let sum: f64 = self.closes.iter().skip(start).sum();
        Some(sum / window as f64)
    }

    /// Волатильность как стандартное отклонение цен закрытия за окно.
    pub fn volatility(&self, window: usize) -> Option<f64> {
        if self.closes.len() < window {
            return None;
        }
        let start = self.closes.len() - window;
        let slice: Vec<f64> = self.closes.iter().skip(start).copied().collect();
        let mean: f64 = slice.iter().sum::<f64>() / window as f64;
        let variance: f64 = slice.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / window as f64;
        Some(variance.sqrt())
    }

    /// Средний объём за окно (в единицах объёма).
    pub fn avg_volume(&self, window: usize) -> Option<f64> {
        if self.volumes.len() < window {
            return None;
        }
        let start = self.volumes.len() - window;
        let sum: f64 = self.volumes.iter().skip(start).sum();
        Some(sum / window as f64)
    }

    /// Средний оборот в рублях за окно.
    pub fn avg_value(&self, window: usize) -> Option<f64> {
        if self.values.len() < window {
            return None;
        }
        let start = self.values.len() - window;
        let sum: f64 = self.values.iter().skip(start).sum();
        Some(sum / window as f64)
    }

    /// VWAP (Volume Weighted Average Price) за окно.
    /// sum(value) / sum(volume). Возвращает None, если суммарный объём равен нулю.
    pub fn vwap(&self, window: usize) -> Option<f64> {
        if self.values.len() < window || self.volumes.len() < window {
            return None;
        }
        let start = self.values.len() - window;
        let total_value: f64 = self.values.iter().skip(start).sum();
        let total_volume: f64 = self.volumes.iter().skip(start).sum();
        if total_volume == 0.0 {
            return None;
        }
        Some(total_value / total_volume)
    }
}
