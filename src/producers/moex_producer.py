"""
Producer for MOEX ISS API.
Fetches candle data for specified tickers from a config file and sends to Kafka.

Stateless design:
- Fetches a rolling window (last N hours) on every iteration.
- Idempotent from Kafka's perspective (message key = ticker:begin).
- No MongoDB writes - Kafka is the only sink.
"""

import json
import logging
import os
import time
from datetime import datetime, timedelta
from typing import cast
from zoneinfo import ZoneInfo

import requests
from dotenv import load_dotenv
from kafka import KafkaProducer

_ = load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
MOEX_BASE_URL = os.getenv("MOEX_BASE_URL", "https://iss.moex.com/iss")
TICKERS_FILE = os.getenv("TICKERS_FILE", "config/tickers.txt")
INTERVAL_SECONDS = int(os.getenv("MOEX_INTERVAL_SECONDS", "60"))
LOOKBACK_HOURS = int(os.getenv("MOEX_LOOKBACK_HOURS", "4"))
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_RAW_TRADES", "raw-trades")

MAX_RETRIES = 5
INITIAL_BACKOFF = 1.0

MSK = ZoneInfo("Europe/Moscow")

CandleDict = dict[str, float | int | str]


def load_tickers(file_path: str) -> list[str]:
    """Загружает список тикеров из файла, игнорируя пустые строки и комментарии."""
    tickers: list[str] = []
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                tickers.append(line)
        if not tickers:
            logger.warning(f"No tickers loaded from {file_path}. Using fallback.")
            return ["SBER", "GAZP", "LKOH"]
        logger.info(f"Loaded {len(tickers)} tickers from {file_path}")
        return tickers
    except FileNotFoundError:
        logger.error(f"Tickers file {file_path} not found. Using fallback.")
        return ["SBER", "GAZP", "LKOH"]
    except PermissionError:
        logger.error(f"Permission denied reading {file_path}. Using fallback.")
        return ["SBER", "GAZP", "LKOH"]
    except OSError as e:
        logger.error(f"OS error reading {file_path}: {e}. Using fallback.")
        return ["SBER", "GAZP", "LKOH"]


def _serialize_value(v: object) -> bytes:
    """Сериализует значение сообщения в JSON-байты."""
    return json.dumps(v).encode("utf-8")


def _serialize_key(k: object) -> bytes:
    """Сериализует ключ сообщения в байты UTF-8."""
    if not isinstance(k, str):
        raise TypeError(f"Kafka key must be str, got {type(k).__name__}")
    return k.encode("utf-8")


def create_kafka_producer() -> KafkaProducer:
    """Создаёт продюсера Kafka с проверкой соединения и retry."""
    backoff = INITIAL_BACKOFF
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=_serialize_value,
                key_serializer=_serialize_key,
                retries=5,
                request_timeout_ms=10000,
            )
            logger.info(f"Kafka producer created for {KAFKA_BOOTSTRAP_SERVERS}")
            return producer
        except Exception as e:
            logger.error(
                f"Failed to create Kafka producer (attempt {attempt}/{MAX_RETRIES}): {e}"
            )
            if attempt == MAX_RETRIES:
                raise
            time.sleep(backoff)
            backoff *= 2
    raise RuntimeError("Unreachable")


def fetch_candles(ticker: str, interval: int = 1) -> list[CandleDict]:
    """
    Получает свечи для указанного тикера за последние LOOKBACK_HOURS часов.
    Retry с exponential backoff при ошибках сети.
    """
    now = datetime.now(MSK)
    from_dt = now - timedelta(hours=LOOKBACK_HOURS)

    url = (
        f"{MOEX_BASE_URL}/engines/stock/markets/shares/boards/TQBR"
        f"/securities/{ticker}/candles.json"
    )
    params = {
        "from": from_dt.strftime("%Y-%m-%d %H:%M:%S"),
        "till": now.strftime("%Y-%m-%d %H:%M:%S"),
        "interval": interval,
        "start": 0,
    }

    backoff = INITIAL_BACKOFF
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = cast(dict[str, object], response.json())

            candles_data_raw = data.get("candles")
            if not isinstance(candles_data_raw, dict):
                logger.error(
                    f"Unexpected response format for {ticker}: 'candles' is not a dict"
                )
                return []
            candles_data = cast(dict[str, object], candles_data_raw)

            columns_obj = candles_data.get("columns")
            rows_obj = candles_data.get("data")
            if not isinstance(columns_obj, list) or not isinstance(rows_obj, list):
                logger.error(f"Invalid columns or data format for {ticker}")
                return []

            columns = cast(list[str], columns_obj)
            rows = cast(list[list[float | int | str]], rows_obj)

            candles: list[CandleDict] = []
            for row in rows:
                if len(row) != len(columns):
                    continue
                candle: CandleDict = {}
                for col, val in zip(columns, row):
                    candle[col] = val
                candle["ticker"] = ticker
                candles.append(candle)
            return candles

        except requests.exceptions.RequestException as e:
            logger.warning(
                f"MOEX request failed for {ticker} (attempt {attempt}/{MAX_RETRIES}): {e}"
            )
            if attempt == MAX_RETRIES:
                logger.error(f"Giving up on {ticker} after {MAX_RETRIES} attempts")
                return []
            time.sleep(backoff)
            backoff *= 2
        except (KeyError, ValueError) as e:
            logger.error(f"Unexpected response structure for {ticker}: {e}")
            return []

    return []


def send_with_retry(
    producer: KafkaProducer,
    ticker: str,
    begin: str,
    candle: CandleDict,
) -> bool:
    """Отправляет сообщение в Kafka с retry. Ключ = 'ticker:begin'."""
    key = f"{ticker}:{begin}"
    backoff = INITIAL_BACKOFF

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            _ = producer.send(KAFKA_TOPIC, key=key, value=candle)
            return True
        except Exception as e:  # noqa: BLE001
            logger.warning(
                f"Kafka send failed for {key} attempt {attempt}/{MAX_RETRIES}): {e}"
            )
            if attempt == MAX_RETRIES:
                logger.error(f"Giving up on {key} after {MAX_RETRIES} attempts")
                return False
            time.sleep(backoff)
            backoff *= 2

    return False


def main() -> None:
    """Основной цикл продюсера: опрос MOEX и отправка в Kafka."""
    tickers = load_tickers(TICKERS_FILE)
    logger.info(f"Starting MOEX producer for tickers: {tickers}")
    logger.info(
        f"Interval: {INTERVAL_SECONDS}s, lookback: {LOOKBACK_HOURS}h, Kafka topic: {KAFKA_TOPIC}"
    )

    producer = create_kafka_producer()

    while True:
        total_sent = 0
        total_failed = 0

        for ticker in tickers:
            candles = fetch_candles(ticker)
            if not candles:
                logger.warning(f"No candles received for {ticker}")
                continue

            for candle in candles:
                begin = str(candle.get("begin", ""))
                if not begin:
                    continue
                candle["fetched_at"] = time.time()
                if send_with_retry(producer, ticker, begin, candle):
                    total_sent += 1
                else:
                    total_failed += 1

            producer.flush()

        logger.info(
            f"Cycle complete: sent={total_sent}, failed={total_failed}. Sleeping for {INTERVAL_SECONDS}s..."
        )
        time.sleep(INTERVAL_SECONDS)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Producer interrupted by user.")
    except Exception:
        logger.exception("Unhandled exception")
    logger.info("Producer stopped.")
