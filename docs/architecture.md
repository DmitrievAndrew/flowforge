# Архитектура FlowForge

**Real-time market intelligence & ML platform для алготрейдинга на MOEX.**

Документ описывает текущее состояние архитектуры, назначение каждого компонента, потоки данных и технический долг. Обновляется по мере развития проекта.

**Последнее обновление:** сентябрь 2026

---

## 1. Общая схема

    MOEX ISS REST API
           |
           v
    +-----------------------+
    |  Python Producer      |
    |  (Docker container)   |
    +-----------+-----------+
                |  raw-trades
                v
    +-----------------------+
    |       Kafka           |
    |   (event bus)         |
    +-----------+-----------+
                |  raw-trades
                v
    +-----------------------+
    |   Rust Consumer       |
    |  (rdkafka + tokio)    |
    |  SMA, volatility, ... |
    +-----------+-----------+
                |
      +---------+---------+
      |         |         |
      v         v         v
    [MongoDB] [ClickHouse] [Redis]
     raw      aggregates   online
     events,  offline      feature
     audit,   store        store
     predict  for Feast
      |         |         ^
      +---------+---------+
                |
                v
      +---------------------+
      | Feast Feature Store |
      | (unified features)  |
      +----------+----------+
                 |
                 v
      +---------------------+
      | ML Model (XGBoost)  |
      | train + serve       |
      +---------------------+

---

## 2. Компоненты системы

### 2.1. Kafka (Apache Kafka)

- **Роль:** центральный брокер сообщений, обеспечивает гарантированную доставку данных между продюсером и консьюмером, позволяет переигрывать поток при обнаружении багов.
- **Реализация:** контейнер `confluentinc/cp-kafka:7.5.0`.
- **Конфигурация:**
  - Два листенера: `PLAINTEXT://localhost:9092` (для доступа с хоста) и `PLAINTEXT_INTERNAL://kafka:9093` (для обмена внутри Docker-сети).
  - Репликация = 1 (для локальной разработки).
  - `KAFKA_AUTO_CREATE_TOPICS_ENABLE=false` - топики создаются явно.
  - Healthcheck через `kafka-broker-api-versions`.
- **Статус:** работает.

#### 2.1.1. Топики Kafka

Созданы скриптом `scripts/init_kafka_topics.sh`:

| Топик | Назначение | Потребители | Статус |
|-------|------------|-------------|--------|
| `raw-trades` | Свечные данные (1-min) с MOEX | Rust Consumer | Активен |
| `raw-quotes` | Данные стакана/котировок (резерв) | - | Резерв |
| `enriched-events` | Данные с вычисленными индикаторами | Feast, ML | Планируется |
| `inference-requests` | Запросы на предсказание | Инференс-сервис | Планируется |
| `trading-signals` | Торговые сигналы (Buy/Sell) | Торговый модуль | Планируется |

Партиционирование: 3 партиции на топик. Репликация: 1.

### 2.2. Продюсер (Python -> Kafka)

- **Роль:** сбор данных с MOEX ISS REST API и публикация в Kafka.
- **Реализация:** `src/producers/moex_producer.py` (Python 3.12), упакован в Docker-образ на базе `python:3.12-slim`.
- **Зависимости:** только `requirements/producer.txt` (`kafka-python`, `requests`, `python-dotenv`, `pymongo`). Без ML-стека - образ весит ~150 МБ.
- **Источник:** `https://iss.moex.com/iss/engines/stock/markets/shares/boards/TQBR/securities/{ticker}/candles.json`.
- **Параметры:**
  - Запрашивает 1-минутные свечи за текущий день.
  - Список тикеров читается из `config/tickers.txt` (15 голубых фишек MOEX).
  - Интервал опроса: 60 секунд (настраивается через `.env`).
- **Обработка ошибок:** конкретные исключения (`FileNotFoundError`, `PermissionError`, `OSError`, `RequestException`), логирование, graceful shutdown.
- **Типизация:** полная, без `Any` и `Unknown` (соответствует BasedPyright strict).
- **Статус:** работает, данные поступают в `raw-trades`.

### 2.3. Консьюмер (Rust)

- **Роль:** потоковая обработка свечей, расчет технических индикаторов, запись в хранилища.
- **Реализация:** `consumer-rs/` на Rust 1.98 (edition 2024), упакован в multi-stage Docker-образ:
  - **Stage 1 (build):** `rust:1.98-slim-bookworm` + `librdkafka-dev`, `cmake`, `pkg-config`.
  - **Stage 2 (runtime):** `debian:bookworm-slim` + `librdkafka1`. Итоговый образ ~80 МБ.
- **Крейты:**
  - `rdkafka` (Kafka consumer)
  - `tokio` (async runtime)
  - `mongodb` (официальный драйвер)
  - `serde` + `serde_json` (десериализация)
  - `chrono` (работа с датами)
  - `tracing` + `tracing-subscriber` (логирование)
  - `dotenvy` (чтение `.env`)
  - `anyhow` (обработка ошибок)
- **Что делает:**
  1. Читает из топика `raw-trades`.
  2. Парсит JSON в структуру `Candle`.
  3. Ведет скользящее окно последних 20 свечей по каждому тикеру.
  4. Вычисляет расширенный набор признаков (см. раздел 3).
  5. Пишет агрегаты в MongoDB (коллекция `processed_aggregates`).
- **Почему Rust, а не Spark:** см. раздел 7.1.
- **Статус:** работает в Docker.

### 2.4. MongoDB

- **Роль:** хранилище сырых событий, audit log, логи предсказаний.
- **Реализация:** контейнер `mongo:6.0`.
- **Коллекции:**
  - `raw_trades` - дублирование сырых данных из Kafka для восстановления и аудита.
    *Статус: создана, наполнение запланировано на этап 3.*
  - `processed_aggregates` - агрегированные данные с признаками (см. раздел 3).
    *Статус: временно используется, см. технический долг (раздел 8). Планируется перенос в ClickHouse.*
  - `enriched_events` - данные с полным набором признаков для обучения.
    *Статус: планируется на этапе 4.*
  - `predictions` - логи предсказаний модели.
    *Статус: планируется на этапе 5.*
- **Индексы:** по `ticker` + `timestamp`, TTL 30 дней для сырых данных.
- **Статус:** работает.

### 2.5. ClickHouse

- **Роль:** аналитическая витрина, offline store для Feast.
- **Реализация:** контейнер `clickhouse/clickhouse-server:23.8`.
- **Таблицы** (см. `scripts/init_clickhouse_tables.sql`):
  - `minute_aggregates` - агрегаты по минутам (OHLCV + индикаторы).
  - `predictions_log` - логи предсказаний модели.
  - `feature_store` - таблица для offline-фичей Feast.
- **Статус:** контейнер поднимается, но запись из консьюмера пока не настроена. Работа запланирована на этапе 3.

### 2.6. Redis

- **Роль:** online store для Feast (низколатентный доступ к фичам в инференсе).
- **Реализация:** контейнер `redis:7.2-alpine`.
- **Аутентификация:** пароль `redispass` (задается в `.env`).
- **Статус:** работает, будет использован на этапе 4.

### 2.7. Feast (Feature Store)

- **Роль:** единый источник истины для признаков. Устраняет training-serving skew, обеспечивает point-in-time корректность.
- **Реализация:** `feast==0.54.0` (совместим с NumPy 1.x).
- **Конфигурация:**
  - Offline store: ClickHouse (в разработке) или Parquet (fallback).
  - Online store: Redis.
- **Статус:** планируется на этапе 4.

### 2.8. Grafana

- **Роль:** визуализация метрик и дашбордов (данные из ClickHouse).
- **Реализация:** контейнер `grafana/grafana:10.2.0`.
- **Статус:** работает, дашборды будут настроены на этапе 8.

### 2.9. Kafka UI

- **Роль:** веб-интерфейс для мониторинга Kafka (топики, сообщения, лаг).
- **Реализация:** контейнер `provectuslabs/kafka-ui:latest`.
- **Подключение:** через внутренний листенер `kafka:9093`.
- **Статус:** работает.

### 2.10. ML-модель

- **Роль:** прогнозирование направления цены акции на 5-минутном горизонте.
- **Тип:** XGBoost (baseline), LSTM (опционально, через ONNX Runtime).
- **Обучение:** Python (scikit-learn, XGBoost, Optuna).
- **Инференс:** ONNX Runtime (без зависимости от PyTorch в проде).
- **Статус:** планируется на этапах 5-6.

---

## 3. Набор признаков

После запуска базового пайплайна принято решение **извлекать все доступные поля** из исходных данных и их производные, а не только те, что кажутся нужными сейчас.

### 3.1. Принцип сбора признаков

> **Правило:** консьюмер извлекает **все** поля из исходного JSON и вычисляет максимум производных. Отбор значимых признаков (feature selection) происходит на этапе ML, а не на этапе ingestion.

**Обоснование:** пересобрать данные дороже, чем отбросить ненужные фичи. Лучше хранить "широкую" таблицу и потом выбирать столбцы, чем осознать через месяц, что нужного признака нет в истории.

### 3.2. Текущий набор признаков в processed_aggregates

**Сырые OHLCV:**
- `open`, `close`, `high`, `low` - цены
- `volume` - объем в единицах
- `value` - оборот в рублях

**Производные от одной свечи:**
- `body` = close - open
- `body_pct` = body / open * 100
- `range` = high - low
- `range_pct` = range / close * 100
- `upper_shadow` = high - max(open, close)
- `lower_shadow` = min(open, close) - low

**Агрегаты по скользящему окну (20 свечей):**
- `sma_5`, `sma_20` - простые скользящие средние
- `volatility` - stddev(close) за 5
- `avg_volume` - средний объем за 5
- `avg_value` - средний оборот за 5
- `vwap` - volume-weighted average price за 5

**Метрики пайплайна:**
- `latency_ms` - задержка от времени свечи до получения
- `processed_at` - время обработки

### 3.3. Что это дает

1. **Гибкость для ML.** Можно экспериментировать с фичами без пересбора данных.
2. **Мониторинг.** `latency_ms` позволяет отслеживать здоровье пайплайна.
3. **Будущие признаки.** Если добавим новые индикаторы (RSI, MACD), они допишутся в ту же коллекцию.

---

## 4. Потоки данных

1. **Producer** (Docker-контейнер) опрашивает MOEX ISS каждые 60 секунд и отправляет свечи в топик `raw-trades`.
2. **Kafka** буферизирует сообщения, обеспечивая отказоустойчивость и возможность replay.
3. **Rust Consumer** (Docker-контейнер) читает из Kafka, обновляет скользящее окно по каждому тикеру, считает индикаторы и записывает агрегаты в MongoDB.
4. *(Планируется)* Агрегаты дублируются в ClickHouse для аналитики и Feast Offline Store.
5. *(Планируется)* Feast материализует фичи в Redis для онлайн-инференса.
6. *(Планируется)* ML-модель получает фичи из Feast, делает предсказание, пишет результат в топик `trading-signals` и в `predictions_log`.

---

## 5. Технологический стек

| Слой | Технология | Назначение |
|------|------------|------------|
| Источник данных | MOEX ISS REST API | Биржевые данные |
| Брокер | Apache Kafka 7.5.0 | Event streaming |
| Продюсер | Python 3.12 + kafka-python | Ingestion |
| Консьюмер | Rust 1.98 + rdkafka, tokio, mongodb | Low-latency processing |
| Хранилище документов | MongoDB 6.0 | Raw events, audit |
| Аналитика | ClickHouse 23.8 | Aggregates, offline store |
| Кеш | Redis 7.2 | Online store |
| Feature Store | Feast 0.54.0 | Unified features |
| ML | XGBoost, scikit-learn, Optuna, ONNX Runtime | Training + inference |
| Визуализация | Grafana 10.2.0 | Dashboards |
| Оркестрация | Docker Compose | One-command startup |

---

## 6. Структура репозитория

    flowforge/
    |-- consumer-rs/              # Rust stream consumer
    |   |-- Cargo.toml
    |   |-- Cargo.lock
    |   +-- src/
    |       |-- main.rs
    |       |-- config.rs
    |       |-- models.rs
    |       |-- indicators.rs
    |       |-- kafka_consumer.rs
    |       +-- mongo_writer.rs
    |-- src/                      # Python code
    |   |-- producers/            # MOEX producer
    |   |-- feature_store/        # Feast definitions
    |   |-- ml/                   # Training & inference
    |   +-- common/               # Shared utilities
    |-- docker/
    |   |-- docker-compose.yml
    |   +-- services/
    |       |-- producer/         # Python producer Dockerfile
    |       +-- consumer-rs/      # Rust consumer Dockerfile (multi-stage)
    |-- requirements/
    |   |-- producer.txt          # Producer only
    |   |-- ml.txt                # ML / Feast / inference
    |   |-- dev.txt               # Local development
    |   +-- all.txt               # CI meta-file
    |-- config/
    |   +-- tickers.txt           # Tickers to track
    |-- docs/
    |   |-- architecture.md       # Этот документ
    |   |-- deployment_guide.md
    |   |-- monitoring_guide.md
    |   +-- disaster_recovery.md
    |-- scripts/                  # Init scripts
    |-- Makefile
    +-- README.md

---

## 7. Изменения в архитектуре

### 7.1. Отказ от Spark в пользу Rust

**Изначальный план:** Apache Spark Structured Streaming для потоковой обработки.

**Решение:** отказаться от Spark. Консьюмер реализован на Rust.

**Обоснование:**

| Критерий | Spark | Rust |
|----------|-------|------|
| Память в покое | 2+ ГБ | 5-15 МБ |
| Задержка обработки | 100+ мс | < 1 мс |
| Время старта | ~30 секунд | мгновенно |
| Зависимости | JVM + коннекторы | один бинарник |
| Типизация | плохая с basedpyright | строгая, compile-time |

Для нагрузки 15 тикеров x 1 свеча/мин = ~500 сообщений/мин Spark избыточен на порядки. Rust дает лучшую производительность при меньшей сложности.

**Что осталось на Python:** продюсер (не в критичном пути) и ML (стандарт индустрии).

### 7.2. Уточнение ролей хранилищ

После ревизии архитектуры роли хранилищ разделены:

- **MongoDB** - сырые события, audit log, логи предсказаний.
- **ClickHouse** - агрегаты, offline store для Feast.
- **Redis** - online store для Feast.

**Проблема:** коллекция `processed_aggregates` в MongoDB дублирует данные, которые должны быть в ClickHouse. См. технический долг.

### 7.3. Контейнеризация всего пайплайна

**Решение:** все запускается через Docker Compose, включая продюсер (Python) и консьюмер (Rust).

**Обоснование:** UX для пользователя критичен. Одна команда `make up` поднимает весь пайплайн. Rust не отменяет контейнеризацию - он делает контейнер легче (~15 МБ вместо 2 ГБ).

**Ключевая деталь:** build- и runtime-стадии Docker-образа консьюмера зафиксированы на **одной версии Debian (Bookworm)** - иначе glibc разъезжается, и бинарник не запускается (`GLIBC_2.38 not found`).

### 7.4. Разделение requirements по сервисам

**Решение:** вместо одного `base.txt` - отдельные файлы по сервисам.

- `producer.txt` - только для контейнера продюсера (`kafka-python`, `requests`, `python-dotenv`, `pymongo`).
- `ml.txt` - ML-стек, Feast, инференс.
- `dev.txt` - все для локальной разработки.
- `all.txt` - мета-файл для CI.

**Обоснование:** образ продюсера стал ~150 МБ вместо 2+ ГБ, сборка - 30 секунд вместо 5 минут.

---

## 8. Технический долг

Следующие пункты нужно закрыть до перехода на реальную торговлю:

### Архитектура

- [ ] **Убрать `processed_aggregates` из MongoDB.** Агрегаты должны жить только в ClickHouse. MongoDB - только сырые события и audit.
- [ ] **Настроить запись агрегатов в ClickHouse** из Rust-консьюмера (крейт `clickhouse`).
- [ ] **Зафиксировать JSON-схему свечей.** Создать `docs/schemas/candle.json`. В будущем - перейти на Avro или Protobuf для строгого контракта между продюсером и консьюмером.
- [ ] **Добавить Airflow.** Батч-джобы (материализация фичей, переобучение, бэктестинг) должны управляться оркестратором, а не cron'ом.

### Надежность

- [ ] **Healthcheck'и в docker-compose.** Kafka уже имеет healthcheck, остальным сервисам надо добавить.
- [ ] **Prometheus endpoint `/metrics` в Rust-консьюмере.** Метрики: lag в Kafka, количество обработанных сообщений, ошибки, latency.
- [ ] **Checkpoint в именованный volume.** Если консьюмер перезапустится - offset'ы Kafka должны сохраниться.
- [ ] **Идемпотентная запись.** Upsert по `(ticker, window_start)` вместо `insert_one`, чтобы избежать дубликатов при replay.
- [ ] **Настроить `auto.offset.reset=earliest` при первом запуске** и сохранять offset'ы для корректного восстановления.

### Безопасность

- [ ] **Вынести секреты из `.env`.** Пароли (`admin:password`, `redispass`, `clickhouse`) - в Docker secrets или Vault.
- [ ] **TLS для Kafka.** Сейчас PLAINTEXT - для прода недопустимо.
- [ ] **Аутентификация в Grafana.** Сменить дефолтный `admin/admin`.
- [ ] **Ограничить доступ к портам.** MongoDB (27017), Redis (6379), ClickHouse (8123/9000) не должны быть доступны извне.

### Feature Store

- [ ] **Проверить Feast + ClickHouse offline store.** Плагин `feast-clickhouse` менее обкатан, чем Parquet. Заложить fallback на Parquet-файлы.
- [ ] **Определить point-in-time корректность.** Убедиться, что признаки при обучении не содержат data leakage.

### ML

- [ ] **Экспорт модели в ONNX.** Чтобы не тащить PyTorch в прод.
- [ ] **Реализовать риск-менеджмент.** Ограничение размера позиции, стоп-лоссы, аварийный рубильник.
- [ ] **Бэктестинг на исторических данных.** Обязательный этап перед реальной торговлей.

---

## 9. Следующие шаги

1. **Этап 3:** включить ClickHouse, настроить запись агрегатов из Rust-консьюмера, убрать `processed_aggregates` из MongoDB.
2. **Этап 4:** развернуть Feast, определить сущности и признаки, материализовать фичи в Redis.
3. **Этап 5:** обучить XGBoost-модель, экспортировать в ONNX.
4. **Этап 6:** реализовать инференс в реальном времени.
5. **Этап 7:** добавить риск-менеджмент и бэктестинг.
6. **Этап 8:** настроить Prometheus + Grafana для мониторинга.
7. **Этап 9:** добавить Airflow для батч-джобов.
8. **Этап 10:** перенести на VPS, запустить в paper trading.
