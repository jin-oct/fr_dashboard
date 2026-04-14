#!/usr/bin/env python3
# 目的:
# ゴールド系・原油系の Funding データを複数取引所から収集し、
# 完全ローカル閲覧用の単一 HTML ダッシュボードを生成する。
#
# 処理概要:
# 1. 既存のローカルキャッシュを読み込む
# 2. API とローカル raw から不足分・最新分を取得する
# 3. 取引所ごとの Funding を正規化して履歴へマージする
# 4. JSON キャッシュへ保存する
# 5. データを内包した index.html を生成する

from __future__ import annotations

import gzip
import json
import re
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import requests
from loguru import logger


# 設定値
BASE_DIR = Path(__file__).resolve().parent
TEMPLATE_HTML_PATH = BASE_DIR / "template.html"
OUTPUT_HTML_PATH = BASE_DIR / "index.html"
CACHE_JSON_PATH = BASE_DIR / "funding_dataset.json"
DOCS_DIR = BASE_DIR / "docs"
DOCS_INDEX_PATH = DOCS_DIR / "index.html"
DOCS_NOJEKYLL_PATH = DOCS_DIR / ".nojekyll"
ROOT_DATA_JSON_PATH = BASE_DIR / "data.json"
DOCS_DATA_JSON_PATH = DOCS_DIR / "data.json"

REQUEST_TIMEOUT_SECONDS = 20
BITGET_PRODUCT_TYPE = "USDT-FUTURES"
BOOTSTRAP_START_MS = 0
LIGHTER_LOCAL_RAW_DIR = Path(r"D:\CODEX\DATA_COLLECTOR\collector_market_data\raw\lighter_funding_rates")
LIGHTER_MARKET_STATS_RAW_DIR = Path(r"D:\CODEX\DATA_COLLECTOR\collector_market_data\raw\lighter_market_stats")
SCRIPT_NAME = Path(__file__).name

TARGET_EXCHANGES = (
    "Bitget",
    "Hyperliquid",
    "Hyperliquid(XYZ)",
    "Aster",
    "edgeX",
    "Lighter",
    "StandX",
    "Pacifica",
)

TAKER_FEE_RATES = {
    "Bitget": 0.0006,
    "Hyperliquid": 0.00045,
    "Aster": 0.0005,
    "edgeX": 0.0005,
    "Lighter": 0.0005,
    "StandX": 0.0005,
    "Pacifica": 0.0005,
}

GOLD_EXACT = {"PAXG", "XAU", "XAUT"}
GOLD_CONTAINS = ("PAXG", "XAU", "XAUT")
OIL_EXACT_NORMALIZED = {
    "CL", "CLUSD", "CLUSD1", "CLUSDT", "CLUSDT", "CLUSDTPERP",
    "WTI", "WTIOILUSDC", "BRENT", "BRENTOIL", "XYZBRENTOIL",
}
OIL_EXACT_RAW = {"CL", "CL-USD", "WTI", "WTIOIL-USDC", "BRENT", "BRENTOIL", "xyz:BRENTOIL"}
BTC_EXACT = {"BTC", "XBT"}
ETH_EXACT = {"ETH"}
SOL_EXACT = {"SOL"}
XAG_EXACT = {"XAG"}

ALLOWED_NORMALIZED_SYMBOLS = {
    "gold": {"PAXG", "PAXGUSD", "PAXGUSDT", "XAU", "XAUUSD", "XAUUSD1", "XAUUSDT", "XAUT", "XAUTUSD", "XAUTUSDT", "XAUUSDTPERP"},
    "oil": {"CL", "XYZCL", "CLUSD", "CLUSD1", "CLUSDT", "CLUSDTPERP", "WTI", "WTIOILUSDC", "BRENT", "BRENTOIL", "XYZBRENTOIL"},
    "btc": {"BTC", "BTCUSD", "BTCUSD1", "BTCUSDT", "XBT", "XBTUSD", "BTCUSDTPERP"},
    "eth": {"ETH", "ETHUSD", "ETHUSD1", "ETHUSDT", "ETHUSDTPERP"},
    "sol": {"SOL", "SOLUSD", "SOLUSD1", "SOLUSDT", "SOLUSDTPERP"},
    "xag": {"XAG", "XAGUSD", "XAGUSD1", "XAGUSDT", "XYZSILVER"},
    "coin": {"COIN", "COINUSDT", "COINUSDTPERP", "XYZCOIN"},
    "mstr": {"MSTR", "MSTRUSDT", "MSTRUSDTPERP", "XYZMSTR"},
    "tsla": {"TSLA", "TSLAUSDT", "TSLAUSDTPERP", "XYZTSLA"},
    "nvda": {"NVDA", "NVDAUSDT", "NVDAUSDTPERP", "XYZNVDA"},
}


def configure_logger() -> None:
    logger.remove()
    logger.add(
        sys.stderr,
        level="INFO",
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level:<7}</level> | {message}",
    )


def make_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": "FRDashboardLocalGenerator/2.0", "Accept": "application/json"})
    return session


def normalize_symbol(symbol: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", symbol.upper())


def detect_asset_group(symbol: str) -> str | None:
    raw = symbol.upper()
    normalized = normalize_symbol(symbol)

    if normalized in ALLOWED_NORMALIZED_SYMBOLS["gold"] or raw in {"XAU-USD", "PAXG", "XAU", "XAUT"}:
        return "gold"
    if normalized in ALLOWED_NORMALIZED_SYMBOLS["xag"] or raw == "XAG-USD":
        return "xag"
    if normalized in ALLOWED_NORMALIZED_SYMBOLS["btc"] or raw == "BTC-USD":
        return "btc"
    if normalized in ALLOWED_NORMALIZED_SYMBOLS["eth"] or raw == "ETH-USD":
        return "eth"
    if normalized in ALLOWED_NORMALIZED_SYMBOLS["sol"] or raw == "SOL-USD":
        return "sol"
    if normalized in ALLOWED_NORMALIZED_SYMBOLS["oil"] or raw in OIL_EXACT_RAW:
        return "oil"
    if normalized in ALLOWED_NORMALIZED_SYMBOLS["coin"]:
        return "coin"
    if normalized in ALLOWED_NORMALIZED_SYMBOLS["mstr"]:
        return "mstr"
    if normalized in ALLOWED_NORMALIZED_SYMBOLS["tsla"]:
        return "tsla"
    if normalized in ALLOWED_NORMALIZED_SYMBOLS["nvda"]:
        return "nvda"
    return None


def to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def to_int(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


def iso_to_unix_ms(value: str) -> int:
    return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() * 1000)


def unix_ms_to_utc_date(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).strftime("%Y-%m-%d")


def build_record(
    exchange: str,
    symbol: str,
    funding_rate: float,
    funding_interval_hours: float,
    timestamp_ms: int,
) -> dict[str, Any] | None:
    asset_group = detect_asset_group(symbol)
    if not asset_group:
        return None
    interval = max(float(funding_interval_hours), 1e-12)
    hourly = funding_rate / interval
    return {
        "exchange": exchange,
        "symbol": symbol,
        "assetGroup": asset_group,
        "fundingRate": float(funding_rate),
        "fundingIntervalHours": interval,
        "fundingRateHourly": hourly,
        "timestamp": int(timestamp_ms),
    }


def build_price_record(
    source_exchange: str,
    symbol: str,
    price: float,
    timestamp_ms: int,
) -> dict[str, Any] | None:
    asset_group = detect_asset_group(symbol)
    if not asset_group or price <= 0:
        return None
    return {
        "sourceExchange": source_exchange,
        "symbol": symbol,
        "assetGroup": asset_group,
        "price": float(price),
        "timestamp": int(timestamp_ms),
    }


def build_latest_price_record(exchange: str, symbol: str, price: float, timestamp_ms: int) -> dict[str, Any] | None:
    asset_group = detect_asset_group(symbol)
    if not asset_group or price <= 0:
        return None
    return {
        "exchange": exchange,
        "symbol": symbol,
        "assetGroup": asset_group,
        "price": float(price),
        "timestamp": int(timestamp_ms),
    }


def compact_record(record: dict[str, Any]) -> dict[str, Any]:
    return {
        "exchange": record["exchange"],
        "symbol": record["symbol"],
        "assetGroup": record["assetGroup"],
        "fundingRate": round(float(record["fundingRate"]), 12),
        "fundingIntervalHours": round(float(record["fundingIntervalHours"]), 6),
        "fundingRateHourly": round(float(record["fundingRateHourly"]), 12),
        "timestamp": int(record["timestamp"]),
    }


def compact_price_record(record: dict[str, Any]) -> dict[str, Any]:
    return {
        "sourceExchange": record["sourceExchange"],
        "symbol": record["symbol"],
        "assetGroup": record["assetGroup"],
        "price": round(float(record["price"]), 10),
        "timestamp": int(record["timestamp"]),
    }


def compact_latest_price_record(record: dict[str, Any]) -> dict[str, Any]:
    return {
        "exchange": record["exchange"],
        "symbol": record["symbol"],
        "assetGroup": record["assetGroup"],
        "price": round(float(record["price"]), 10),
        "timestamp": int(record["timestamp"]),
    }


def fetch_json(session: requests.Session, method: str, url: str, **kwargs: Any) -> Any:
    response = session.request(method, url, timeout=REQUEST_TIMEOUT_SECONDS, **kwargs)
    response.raise_for_status()
    return response.json()


def load_dataset_from_html(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"records": [], "lastUpdated": 0}
    text = path.read_text(encoding="utf-8")
    match = re.search(r'<script id="(?:dataset-json|inline-data-json)" type="application/json">\s*(.*?)\s*</script>', text, re.DOTALL)
    if not match:
        return {"records": [], "lastUpdated": 0}
    try:
        return json.loads(match.group(1))
    except json.JSONDecodeError:
        return {"records": [], "lastUpdated": 0}


def load_existing_dataset() -> dict[str, Any]:
    if CACHE_JSON_PATH.exists():
        try:
            data = json.loads(CACHE_JSON_PATH.read_text(encoding="utf-8"))
            if isinstance(data.get("records"), list):
                logger.info("既存キャッシュを読み込みました: {}", CACHE_JSON_PATH)
                return {
                    "records": data.get("records", []),
                    "priceRecords": data.get("priceRecords", []),
                    "latestPrices": data.get("latestPrices", []),
                    "lastUpdated": to_int(data.get("lastUpdated"), 0),
                }
        except Exception as exc:
            logger.error("キャッシュ読み込み失敗: {}", exc)

    html_data = load_dataset_from_html(OUTPUT_HTML_PATH)
    if html_data.get("records"):
        logger.info("index.html 内の埋め込みデータを再利用します")
        return {
            "records": html_data.get("records", []),
            "priceRecords": html_data.get("priceRecords", []),
            "latestPrices": html_data.get("latestPrices", []),
            "lastUpdated": to_int(html_data.get("lastUpdated"), 0),
        }

    logger.info("既存データがないため、新規作成します")
    return {"records": [], "priceRecords": [], "latestPrices": [], "lastUpdated": 0}


def save_dataset_json(dataset: dict[str, Any]) -> None:
    CACHE_JSON_PATH.write_text(
        json.dumps(dataset, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )
    logger.success("ローカルキャッシュ保存完了: {}", CACHE_JSON_PATH)


def compress_records(records: list[dict[str, Any]], now_ms: int) -> list[dict[str, Any]]:
    compressed: list[dict[str, Any]] = []
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in records:
        grouped[(row["exchange"], row["symbol"])].append(row)

    for (_, _), rows in grouped.items():
        rows.sort(key=lambda item: item["timestamp"])
        bucket_latest: dict[int, dict[str, Any]] = {}
        for row in rows:
            age_days = (now_ms - int(row["timestamp"])) / 86_400_000
            if age_days <= 30:
                bucket_ms = 0
            elif age_days <= 180:
                bucket_ms = 4 * 3_600_000
            else:
                bucket_ms = 24 * 3_600_000

            if bucket_ms == 0:
                compressed.append(row)
                continue

            bucket_key = int(row["timestamp"]) // bucket_ms
            bucket_latest[bucket_key] = row

        if bucket_latest:
            compressed.extend(sorted(bucket_latest.values(), key=lambda item: item["timestamp"]))

    result = sorted(compressed, key=lambda row: (row["assetGroup"], row["symbol"], row["exchange"], row["timestamp"]))
    logger.info("履歴圧縮結果: {} -> {} 件", len(records), len(result))
    return result


def compress_price_records(records: list[dict[str, Any]], now_ms: int) -> list[dict[str, Any]]:
    compressed: list[dict[str, Any]] = []
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in records:
        grouped[row["symbol"]].append(row)

    for _, rows in grouped.items():
        rows.sort(key=lambda item: item["timestamp"])
        bucket_latest: dict[int, dict[str, Any]] = {}
        for row in rows:
            age_days = (now_ms - int(row["timestamp"])) / 86_400_000
            if age_days <= 30:
                bucket_ms = 0
            elif age_days <= 180:
                bucket_ms = 4 * 3_600_000
            else:
                bucket_ms = 24 * 3_600_000

            if bucket_ms == 0:
                compressed.append(compact_price_record(row))
                continue

            bucket_key = int(row["timestamp"]) // bucket_ms
            bucket_latest[bucket_key] = compact_price_record(row)

        if bucket_latest:
            compressed.extend(sorted(bucket_latest.values(), key=lambda item: item["timestamp"]))

    result = sorted(compressed, key=lambda row: (row["assetGroup"], row["symbol"], row["timestamp"]))
    logger.info("価格履歴圧縮結果: {} -> {} 件", len(records), len(result))
    return result


def merge_records(existing_records: list[dict[str, Any]], new_records: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: dict[tuple[str, str, int], dict[str, Any]] = {}
    for record in existing_records:
        key = (record["exchange"], record["symbol"], int(record["timestamp"]))
        merged[key] = compact_record(record)
    before_count = len(merged)
    for record in new_records:
        key = (record["exchange"], record["symbol"], int(record["timestamp"]))
        merged[key] = compact_record(record)
    after_count = len(merged)
    logger.info("履歴マージ結果: 追加 {} 件", after_count - before_count)
    return sorted(merged.values(), key=lambda row: (row["assetGroup"], row["symbol"], row["exchange"], row["timestamp"]))


def replace_exchange_records(
    existing_records: list[dict[str, Any]],
    exchange: str,
    replacement_records: Iterable[dict[str, Any]],
) -> list[dict[str, Any]]:
    filtered = [row for row in existing_records if row["exchange"] != exchange]
    logger.info("{} 履歴を全置換します: 旧 {} 件", exchange, len(existing_records) - len(filtered))
    return merge_records(filtered, replacement_records)


def drop_invalid_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    filtered = [row for row in records if not (row["exchange"] == "Aster" and str(row["symbol"]).endswith("USD1"))]
    removed = len(records) - len(filtered)
    if removed:
        logger.info("Aster USD1 履歴を削除しました: {} 件", removed)
    return filtered


def symbols_for_exchange(records: list[dict[str, Any]], exchange: str) -> set[str]:
    return {row["symbol"] for row in records if row["exchange"] == exchange}


def need_bootstrap(records: list[dict[str, Any]], exchange: str, symbol: str) -> bool:
    return not any(row["exchange"] == exchange and row["symbol"] == symbol for row in records)


def latest_timestamp(records: list[dict[str, Any]], exchange: str, symbol: str) -> int:
    values = [int(row["timestamp"]) for row in records if row["exchange"] == exchange and row["symbol"] == symbol]
    return max(values) if values else 0


def fetch_bitget_symbols(session: requests.Session) -> list[str]:
    payload = fetch_json(
        session,
        "GET",
        "https://api.bitget.com/api/v2/mix/market/contracts",
        params={"productType": BITGET_PRODUCT_TYPE},
    )
    return [
        str(row.get("symbol", ""))
        for row in payload.get("data", [])
        if detect_asset_group(str(row.get("symbol", "")))
    ]


def fetch_bitget_history(session: requests.Session, symbol: str, start_ms: int) -> list[dict[str, Any]]:
    current = fetch_json(
        session,
        "GET",
        "https://api.bitget.com/api/v2/mix/market/current-fund-rate",
        params={"symbol": symbol, "productType": BITGET_PRODUCT_TYPE},
    )
    interval_hours = to_float(current.get("data", [{}])[0].get("fundingRateInterval"), 4.0)
    results: list[dict[str, Any]] = []
    page_no = 1
    while True:
        payload = fetch_json(
            session,
            "GET",
            "https://api.bitget.com/api/v2/mix/market/history-fund-rate",
            params={
                "symbol": symbol,
                "productType": BITGET_PRODUCT_TYPE,
                "pageSize": 100,
                "pageNo": page_no,
            },
        )
        rows = payload.get("data", [])
        if not rows:
            break
        stop = False
        for row in rows:
            ts_ms = to_int(row.get("fundingTime"))
            if start_ms and ts_ms <= start_ms:
                stop = True
                continue
            record = build_record("Bitget", symbol, to_float(row.get("fundingRate")), interval_hours, ts_ms)
            if record:
                results.append(record)
        if stop or len(rows) < 100:
            break
        page_no += 1
    return results


def fetch_hyperliquid_symbols(session: requests.Session) -> list[str]:
    payload = fetch_json(session, "POST", "https://api.hyperliquid.xyz/info", json={"type": "metaAndAssetCtxs"})
    return [
        str(row.get("name", ""))
        for row in payload[0].get("universe", [])
        if detect_asset_group(str(row.get("name", "")))
    ]


def fetch_hyperliquid_history(session: requests.Session, symbol: str, start_ms: int) -> list[dict[str, Any]]:
    payload = fetch_json(
        session,
        "POST",
        "https://api.hyperliquid.xyz/info",
        json={"type": "fundingHistory", "coin": symbol, "startTime": max(start_ms + 1, 0)},
    )
    results = []
    for row in payload:
        record = build_record("Hyperliquid", symbol, to_float(row.get("fundingRate")), 1.0, to_int(row.get("time")))
        if record:
            results.append(record)
    return results


def fetch_hyperliquid_xyz_symbols(session: requests.Session) -> list[str]:
    payload = fetch_json(session, "POST", "https://api.hyperliquid.xyz/info", json={"type": "metaAndAssetCtxs", "dex": "xyz"})
    return [
        str(row.get("name", ""))
        for row in payload[0].get("universe", [])
        if detect_asset_group(str(row.get("name", "")))
    ]


def fetch_hyperliquid_xyz_history(session: requests.Session, symbol: str, start_ms: int) -> list[dict[str, Any]]:
    payload = fetch_json(
        session,
        "POST",
        "https://api.hyperliquid.xyz/info",
        json={"type": "fundingHistory", "coin": symbol, "startTime": max(start_ms + 1, 0), "dex": "xyz"},
    )
    results = []
    for row in payload:
        record = build_record("Hyperliquid(XYZ)", symbol, to_float(row.get("fundingRate")), 1.0, to_int(row.get("time")))
        if record:
            results.append(record)
    return results


def fetch_aster_symbols_and_intervals(session: requests.Session) -> dict[str, float]:
    payload = fetch_json(session, "GET", "https://fapi.asterdex.com/fapi/v1/fundingInfo")
    result: dict[str, float] = {}
    for row in payload:
        symbol = str(row.get("symbol", ""))
        if symbol.endswith("USD1"):
            continue
        if detect_asset_group(symbol):
            result[symbol] = to_float(row.get("fundingIntervalHours"), 8.0)
    return result


def fetch_aster_history(session: requests.Session, symbol: str, interval_hours: float, start_ms: int) -> list[dict[str, Any]]:
    rows = fetch_json(
        session,
        "GET",
        "https://fapi.asterdex.com/fapi/v1/fundingRate",
        params={"symbol": symbol, "startTime": max(start_ms + 1, 0), "endTime": int(time.time() * 1000), "limit": 1000},
    )
    results = []
    for row in rows:
        record = build_record("Aster", symbol, to_float(row.get("fundingRate")), interval_hours, to_int(row.get("fundingTime")))
        if record:
            results.append(record)
    return results


def fetch_edgex_contracts(session: requests.Session) -> list[dict[str, Any]]:
    payload = fetch_json(session, "GET", "https://pro.edgex.exchange/api/v1/public/meta/getMetaData")
    return [
        row
        for row in payload.get("data", {}).get("contractList", [])
        if detect_asset_group(str(row.get("contractName", "")))
    ]


def fetch_edgex_history(session: requests.Session, contract_id: str, symbol: str, start_ms: int) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    offset = ""
    while True:
        payload = fetch_json(
            session,
            "GET",
            "https://pro.edgex.exchange/api/v1/public/funding/getFundingRatePage",
            params={
                "contractId": contract_id,
                "size": 100,
                "offsetData": offset,
                "filterSettlementFundingRate": "true",
                "filterBeginTimeInclusive": max(start_ms + 1, 0),
            },
        )
        data = payload.get("data", {})
        rows = data.get("dataList", [])
        if not rows:
            break
        for row in rows:
            interval_hours = max(to_float(row.get("fundingRateIntervalMin"), 240.0) / 60.0, 1.0)
            record = build_record("edgeX", symbol, to_float(row.get("fundingRate")), interval_hours, to_int(row.get("fundingTime")))
            if record:
                results.append(record)
        offset = data.get("nextPageOffsetData") or ""
        if not offset:
            break
    return results


def fetch_standx_symbols(session: requests.Session) -> list[str]:
    rows = fetch_json(session, "GET", "https://perps.standx.com/api/query_symbol_info")
    return [str(row.get("symbol", "")) for row in rows if detect_asset_group(str(row.get("symbol", "")))]


def fetch_standx_history(session: requests.Session, symbol: str, start_ms: int) -> list[dict[str, Any]]:
    rows = fetch_json(
        session,
        "GET",
        "https://perps.standx.com/api/query_funding_rates",
        params={"symbol": symbol, "start_time": max(start_ms + 1, 0), "end_time": int(time.time() * 1000)},
    )
    results = []
    for row in rows:
        record = build_record("StandX", symbol, to_float(row.get("funding_rate")), 1.0, iso_to_unix_ms(row.get("time")))
        if record:
            results.append(record)
    return results


def fetch_pacifica_symbols(session: requests.Session) -> list[str]:
    rows = fetch_json(session, "GET", "https://api.pacifica.fi/api/v1/info/prices").get("data", [])
    return [str(row.get("symbol", "")) for row in rows if detect_asset_group(str(row.get("symbol", "")))]


def fetch_pacifica_history(session: requests.Session, symbol: str, start_ms: int) -> list[dict[str, Any]]:
    rows = fetch_json(
        session,
        "GET",
        "https://api.pacifica.fi/api/v1/funding_rate/history",
        params={"symbol": symbol, "limit": 1000},
    ).get("data", [])
    results = []
    for row in rows:
        ts_ms = to_int(row.get("created_at"))
        if start_ms and ts_ms <= start_ms:
            continue
        record = build_record("Pacifica", symbol, to_float(row.get("funding_rate")), 1.0, ts_ms)
        if record:
            results.append(record)
    return results


def fetch_lighter_symbols_from_http(session: requests.Session) -> list[str]:
    rows = fetch_json(session, "GET", "https://mainnet.zklighter.elliot.ai/api/v1/orderBookDetails").get("order_book_details", [])
    return [
        str(row.get("symbol", ""))
        for row in rows
        if str(row.get("market_type", "")) == "perp"
        and str(row.get("status", "")) == "active"
        and detect_asset_group(str(row.get("symbol", "")))
    ]


def iter_raw_files(base_dir: Path, start_ms: int) -> list[Path]:
    if not base_dir.exists():
        return []
    min_date = unix_ms_to_utc_date(start_ms) if start_ms > 0 else "0001-01-01"
    files = []
    for path in sorted(base_dir.iterdir()):
        if not path.is_file():
            continue
        date_part = path.name.split(".")[0]
        if date_part >= min_date:
            files.append(path)
    return files


def open_text_file(path: Path):
    if path.suffix == ".gz":
        return gzip.open(path, "rt", encoding="utf-8")
    return path.open("r", encoding="utf-8")


def fetch_lighter_history_from_local(start_ms: int) -> list[dict[str, Any]]:
    target_symbols = {"PAXG", "XAU", "WTI", "BRENTOIL", "COIN", "MSTR", "TSLA", "NVDA"}
    hourly_last: dict[tuple[str, int], dict[str, Any]] = {}
    files = iter_raw_files(LIGHTER_MARKET_STATS_RAW_DIR, start_ms)
    logger.info("Lighter market_stats raw 走査開始: files={}", len(files))
    for path in files:
        with open_text_file(path) as fp:
            for line in fp:
                line = line.strip()
                if not line:
                    continue
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    continue
                ts_ms = iso_to_unix_ms(str(row.get("timestamp_utc")))
                if start_ms and ts_ms <= start_ms:
                    continue
                hour_bucket_ms = (ts_ms // 3_600_000) * 3_600_000
                for symbol, entry in (row.get("data") or {}).items():
                    if symbol not in target_symbols:
                        continue
                    # current_funding_rate は UI 表示と同じ percent 値のため、内部用 decimal へ 100 で割る。
                    funding_rate_decimal = to_float(entry.get("current_funding_rate")) / 100.0
                    record = build_record("Lighter", symbol, funding_rate_decimal, 1.0, hour_bucket_ms)
                    if record:
                        hourly_last[(symbol, hour_bucket_ms)] = record
    results = sorted(hourly_last.values(), key=lambda row: (row["symbol"], row["timestamp"]))
    logger.success("Lighter market_stats raw 取り込み成功: {} 件", len(results))
    return results


def fetch_price_history_from_lighter_market_stats(start_ms: int) -> list[dict[str, Any]]:
    target_symbols = {"BTC", "ETH", "SOL", "PAXG", "XAU", "WTI", "BRENTOIL", "XAG", "COIN", "MSTR", "TSLA", "NVDA"}
    latest_by_bucket: dict[tuple[str, int], dict[str, Any]] = {}
    files = iter_raw_files(LIGHTER_MARKET_STATS_RAW_DIR, start_ms)
    logger.info("Lighter price raw 走査開始: files={}", len(files))
    for path in files:
        with open_text_file(path) as fp:
            for line in fp:
                line = line.strip()
                if not line:
                    continue
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    continue
                ts_ms = iso_to_unix_ms(str(row.get("timestamp_utc")))
                if start_ms and ts_ms <= start_ms:
                    continue
                hour_bucket_ms = (ts_ms // 3_600_000) * 3_600_000
                for symbol, entry in (row.get("data") or {}).items():
                    if symbol not in target_symbols:
                        continue
                    price = to_float(entry.get("mark_price"))
                    if price <= 0:
                        price = to_float(entry.get("index_price"))
                    if price <= 0:
                        price = to_float(entry.get("last_trade_price"))
                    record = build_price_record("Lighter", symbol, price, hour_bucket_ms)
                    if record:
                        latest_by_bucket[(symbol, hour_bucket_ms)] = record
    results = sorted(latest_by_bucket.values(), key=lambda row: (row["symbol"], row["timestamp"]))
    logger.success("Lighter price raw 取り込み成功: {} 件", len(results))
    return results


def fetch_bitget_latest_prices(session: requests.Session) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    now_ms = int(time.time() * 1000)
    for symbol in fetch_bitget_symbols(session):
        payload = fetch_json(
            session,
            "GET",
            "https://api.bitget.com/api/v2/mix/market/ticker",
            params={"symbol": symbol, "productType": BITGET_PRODUCT_TYPE},
        )
        data = (payload.get("data") or [{}])[0]
        price = to_float(data.get("markPrice")) or to_float(data.get("indexPrice")) or to_float(data.get("lastPr"))
        record = build_latest_price_record("Bitget", symbol, price, now_ms)
        if record:
            rows.append(record)
    return rows


def fetch_hyperliquid_latest_prices(session: requests.Session) -> list[dict[str, Any]]:
    payload = fetch_json(session, "POST", "https://api.hyperliquid.xyz/info", json={"type": "metaAndAssetCtxs"})
    universe = payload[0].get("universe", [])
    ctxs = payload[1]
    rows: list[dict[str, Any]] = []
    now_ms = int(time.time() * 1000)
    for meta, ctx in zip(universe, ctxs):
        symbol = str(meta.get("name", ""))
        if not detect_asset_group(symbol):
            continue
        price = to_float(ctx.get("markPx")) or to_float(ctx.get("oraclePx")) or to_float(ctx.get("midPx"))
        record = build_latest_price_record("Hyperliquid", symbol, price, now_ms)
        if record:
            rows.append(record)
    return rows


def fetch_hyperliquid_xyz_latest_prices(session: requests.Session) -> list[dict[str, Any]]:
    payload = fetch_json(session, "POST", "https://api.hyperliquid.xyz/info", json={"type": "metaAndAssetCtxs", "dex": "xyz"})
    universe = payload[0].get("universe", [])
    ctxs = payload[1]
    rows: list[dict[str, Any]] = []
    now_ms = int(time.time() * 1000)
    for meta, ctx in zip(universe, ctxs):
        symbol = str(meta.get("name", ""))
        if not detect_asset_group(symbol):
            continue
        price = to_float(ctx.get("markPx")) or to_float(ctx.get("oraclePx")) or to_float(ctx.get("midPx"))
        record = build_latest_price_record("Hyperliquid(XYZ)", symbol, price, now_ms)
        if record:
            rows.append(record)
    return rows


def fetch_aster_latest_prices(session: requests.Session) -> list[dict[str, Any]]:
    payload = fetch_json(session, "GET", "https://fapi.asterdex.com/fapi/v1/premiumIndex")
    rows: list[dict[str, Any]] = []
    now_ms = int(time.time() * 1000)
    for row in payload:
        symbol = str(row.get("symbol", ""))
        if symbol.endswith("USD1") or not detect_asset_group(symbol):
            continue
        price = to_float(row.get("markPrice")) or to_float(row.get("indexPrice"))
        record = build_latest_price_record("Aster", symbol, price, now_ms)
        if record:
            rows.append(record)
    return rows


def fetch_lighter_latest_prices() -> list[dict[str, Any]]:
    rows = fetch_price_history_from_lighter_market_stats(BOOTSTRAP_START_MS)
    latest_by_symbol: dict[str, dict[str, Any]] = {}
    for row in rows:
        latest_by_symbol[row["symbol"]] = build_latest_price_record("Lighter", row["symbol"], row["price"], row["timestamp"]) or latest_by_symbol.get(row["symbol"])
    return [compact_latest_price_record(row) for row in latest_by_symbol.values() if row]


def fetch_pacifica_latest_prices(session: requests.Session) -> list[dict[str, Any]]:
    payload = fetch_json(session, "GET", "https://api.pacifica.fi/api/v1/info/prices").get("data", [])
    rows: list[dict[str, Any]] = []
    for row in payload:
        symbol = str(row.get("symbol", ""))
        if not detect_asset_group(symbol):
            continue
        price = to_float(row.get("mark")) or to_float(row.get("oracle")) or to_float(row.get("mid"))
        record = build_latest_price_record("Pacifica", symbol, price, to_int(row.get("timestamp"), int(time.time() * 1000)))
        if record:
            rows.append(record)
    return rows


def collect_latest_prices(session: requests.Session) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for label, fn in (
        ("Bitget", lambda: fetch_bitget_latest_prices(session)),
        ("Hyperliquid", lambda: fetch_hyperliquid_latest_prices(session)),
        ("Hyperliquid(XYZ)", lambda: fetch_hyperliquid_xyz_latest_prices(session)),
        ("Aster", lambda: fetch_aster_latest_prices(session)),
        ("Lighter", fetch_lighter_latest_prices),
        ("Pacifica", lambda: fetch_pacifica_latest_prices(session)),
    ):
        try:
            logger.info("{} 現在価格取得開始", label)
            results.extend(fn())
            logger.success("{} 現在価格取得完了", label)
        except Exception as exc:
            logger.error("{} 現在価格取得失敗: {}", label, exc)
    deduped: dict[tuple[str, str], dict[str, Any]] = {}
    for row in results:
        deduped[(row["exchange"], row["symbol"])] = compact_latest_price_record(row)
    return sorted(deduped.values(), key=lambda row: (row["assetGroup"], row["symbol"], row["exchange"]))


def collect_backfill_records(existing_records: list[dict[str, Any]], session: requests.Session) -> list[dict[str, Any]]:
    backfill: list[dict[str, Any]] = []

    try:
        logger.info("Bitget バックフィル開始")
        for symbol in fetch_bitget_symbols(session):
            if need_bootstrap(existing_records, "Bitget", symbol):
                backfill.extend(fetch_bitget_history(session, symbol, BOOTSTRAP_START_MS))
        logger.success("Bitget バックフィル完了")
    except Exception as exc:
        logger.error("Bitget バックフィル失敗: {}", exc)

    try:
        logger.info("Hyperliquid バックフィル開始")
        for symbol in fetch_hyperliquid_symbols(session):
            if need_bootstrap(existing_records, "Hyperliquid", symbol):
                backfill.extend(fetch_hyperliquid_history(session, symbol, BOOTSTRAP_START_MS))
        logger.success("Hyperliquid バックフィル完了")
    except Exception as exc:
        logger.error("Hyperliquid バックフィル失敗: {}", exc)

    try:
        logger.info("Hyperliquid(XYZ) バックフィル開始")
        for symbol in fetch_hyperliquid_xyz_symbols(session):
            if need_bootstrap(existing_records, "Hyperliquid(XYZ)", symbol):
                backfill.extend(fetch_hyperliquid_xyz_history(session, symbol, BOOTSTRAP_START_MS))
        logger.success("Hyperliquid(XYZ) バックフィル完了")
    except Exception as exc:
        logger.error("Hyperliquid(XYZ) バックフィル失敗: {}", exc)

    try:
        logger.info("Aster バックフィル開始")
        intervals = fetch_aster_symbols_and_intervals(session)
        for symbol, interval_hours in intervals.items():
            if need_bootstrap(existing_records, "Aster", symbol):
                backfill.extend(fetch_aster_history(session, symbol, interval_hours, BOOTSTRAP_START_MS))
        logger.success("Aster バックフィル完了")
    except Exception as exc:
        logger.error("Aster バックフィル失敗: {}", exc)

    try:
        logger.info("edgeX バックフィル開始")
        for row in fetch_edgex_contracts(session):
            symbol = str(row.get("contractName", ""))
            if need_bootstrap(existing_records, "edgeX", symbol):
                backfill.extend(fetch_edgex_history(session, str(row.get("contractId", "")), symbol, BOOTSTRAP_START_MS))
        logger.success("edgeX バックフィル完了")
    except Exception as exc:
        logger.error("edgeX バックフィル失敗: {}", exc)

    try:
        logger.info("StandX バックフィル開始")
        for symbol in fetch_standx_symbols(session):
            if need_bootstrap(existing_records, "StandX", symbol):
                backfill.extend(fetch_standx_history(session, symbol, BOOTSTRAP_START_MS))
        logger.success("StandX バックフィル完了")
    except Exception as exc:
        logger.error("StandX バックフィル失敗: {}", exc)

    try:
        logger.info("Pacifica バックフィル開始")
        for symbol in fetch_pacifica_symbols(session):
            if need_bootstrap(existing_records, "Pacifica", symbol):
                backfill.extend(fetch_pacifica_history(session, symbol, BOOTSTRAP_START_MS))
        logger.success("Pacifica バックフィル完了")
    except Exception as exc:
        logger.error("Pacifica バックフィル失敗: {}", exc)

    try:
        logger.info("Lighter バックフィル開始")
        lighter_existing = symbols_for_exchange(existing_records, "Lighter")
        if {"PAXG", "XAU", "WTI", "BRENTOIL"} - lighter_existing:
            backfill.extend(fetch_lighter_history_from_local(BOOTSTRAP_START_MS))
        logger.success("Lighter バックフィル完了")
    except Exception as exc:
        logger.error("Lighter バックフィル失敗: {}", exc)

    logger.info("バックフィル件数合計: {}", len(backfill))
    return backfill


def rebuild_lighter_history(existing_records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    logger.info("Lighter 履歴の全面再構築を開始します")
    lighter_records = fetch_lighter_history_from_local(BOOTSTRAP_START_MS)
    replaced = replace_exchange_records(existing_records, "Lighter", lighter_records)
    logger.success("Lighter 履歴の全面再構築が完了しました")
    return replaced


def collect_incremental_records(existing_records: list[dict[str, Any]], session: requests.Session) -> list[dict[str, Any]]:
    new_records: list[dict[str, Any]] = []

    try:
        logger.info("Bitget 差分取得開始")
        for symbol in fetch_bitget_symbols(session):
            new_records.extend(fetch_bitget_history(session, symbol, latest_timestamp(existing_records, "Bitget", symbol)))
        logger.success("Bitget 差分取得完了")
    except Exception as exc:
        logger.error("Bitget 差分取得失敗: {}", exc)

    try:
        logger.info("Hyperliquid 差分取得開始")
        for symbol in fetch_hyperliquid_symbols(session):
            new_records.extend(fetch_hyperliquid_history(session, symbol, latest_timestamp(existing_records, "Hyperliquid", symbol)))
        logger.success("Hyperliquid 差分取得完了")
    except Exception as exc:
        logger.error("Hyperliquid 差分取得失敗: {}", exc)

    try:
        logger.info("Hyperliquid(XYZ) 差分取得開始")
        for symbol in fetch_hyperliquid_xyz_symbols(session):
            new_records.extend(fetch_hyperliquid_xyz_history(session, symbol, latest_timestamp(existing_records, "Hyperliquid(XYZ)", symbol)))
        logger.success("Hyperliquid(XYZ) 差分取得完了")
    except Exception as exc:
        logger.error("Hyperliquid(XYZ) 差分取得失敗: {}", exc)

    try:
        logger.info("Aster 差分取得開始")
        intervals = fetch_aster_symbols_and_intervals(session)
        for symbol, interval_hours in intervals.items():
            new_records.extend(fetch_aster_history(session, symbol, interval_hours, latest_timestamp(existing_records, "Aster", symbol)))
        logger.success("Aster 差分取得完了")
    except Exception as exc:
        logger.error("Aster 差分取得失敗: {}", exc)

    try:
        logger.info("edgeX 差分取得開始")
        for row in fetch_edgex_contracts(session):
            symbol = str(row.get("contractName", ""))
            new_records.extend(fetch_edgex_history(session, str(row.get("contractId", "")), symbol, latest_timestamp(existing_records, "edgeX", symbol)))
        logger.success("edgeX 差分取得完了")
    except Exception as exc:
        logger.error("edgeX 差分取得失敗: {}", exc)

    try:
        logger.info("StandX 差分取得開始")
        for symbol in fetch_standx_symbols(session):
            new_records.extend(fetch_standx_history(session, symbol, latest_timestamp(existing_records, "StandX", symbol)))
        logger.success("StandX 差分取得完了")
    except Exception as exc:
        logger.error("StandX 差分取得失敗: {}", exc)

    try:
        logger.info("Pacifica 差分取得開始")
        for symbol in fetch_pacifica_symbols(session):
            new_records.extend(fetch_pacifica_history(session, symbol, latest_timestamp(existing_records, "Pacifica", symbol)))
        logger.success("Pacifica 差分取得完了")
    except Exception as exc:
        logger.error("Pacifica 差分取得失敗: {}", exc)

    try:
        logger.info("Lighter 差分取得開始")
        lighter_start = max(
            [latest_timestamp(existing_records, "Lighter", symbol) for symbol in ("PAXG", "XAU", "WTI", "BRENTOIL")]
            + [0]
        )
        # Lighter は分単位で更新される current rate を時間バケットへ丸めて保持しているため、
        # 同一時間帯の後続更新を取り込めるよう 1時間戻して再読込する。
        new_records.extend(fetch_lighter_history_from_local(max(lighter_start - 3_600_000, 0)))
        logger.success("Lighter 差分取得完了")
    except Exception as exc:
        logger.error("Lighter 差分取得失敗: {}", exc)

    logger.info("差分取得件数合計: {}", len(new_records))
    return new_records


def render_html(dataset: dict[str, Any], inline_data: bool) -> str:
    template = TEMPLATE_HTML_PATH.read_text(encoding="utf-8")
    inline_json = json.dumps(dataset, ensure_ascii=False, separators=(",", ":")) if inline_data else "null"
    html = template.replace("__INLINE_DATA_JSON__", inline_json)
    html = html.replace("__TARGET_EXCHANGES__", json.dumps(TARGET_EXCHANGES, ensure_ascii=False))
    return html


def summarize_records(records: list[dict[str, Any]]) -> dict[str, int]:
    summary: dict[str, int] = defaultdict(int)
    for row in records:
        summary[row["exchange"]] += 1
    return dict(summary)


def main() -> int:
    configure_logger()
    logger.info(
        "{} 設定: OUTPUT_HTML_PATH={} CACHE_JSON_PATH={} REQUEST_TIMEOUT_SECONDS={}",
        SCRIPT_NAME, OUTPUT_HTML_PATH, CACHE_JSON_PATH, REQUEST_TIMEOUT_SECONDS,
    )
    if not TEMPLATE_HTML_PATH.exists():
        logger.error("template.html が存在しません: {}", TEMPLATE_HTML_PATH)
        return 1

    existing_dataset = load_existing_dataset()
    existing_dataset["records"] = drop_invalid_records(existing_dataset["records"])
    session = make_session()

    existing_dataset["records"] = rebuild_lighter_history(existing_dataset["records"])

    if not existing_dataset["records"]:
        backfill_records = collect_backfill_records(existing_dataset["records"], session)
        existing_dataset["records"] = merge_records(existing_dataset["records"], backfill_records)

    incremental_records = collect_incremental_records(existing_dataset["records"], session)
    merged_records = merge_records(existing_dataset["records"], incremental_records)
    now_ms = int(time.time() * 1000)
    merged_records = compress_records(merged_records, now_ms)
    price_records = compress_price_records(fetch_price_history_from_lighter_market_stats(BOOTSTRAP_START_MS), now_ms)
    latest_prices = collect_latest_prices(session)

    dataset = {"records": merged_records, "priceRecords": price_records, "latestPrices": latest_prices, "lastUpdated": now_ms}
    save_dataset_json(dataset)
    web_dataset_json = json.dumps(dataset, ensure_ascii=False, separators=(",", ":"))
    ROOT_DATA_JSON_PATH.write_text(web_dataset_json, encoding="utf-8")
    DOCS_DIR.mkdir(parents=True, exist_ok=True)
    DOCS_DATA_JSON_PATH.write_text(web_dataset_json, encoding="utf-8")
    root_html = render_html(dataset, inline_data=True)
    docs_html = render_html(dataset, inline_data=False)
    OUTPUT_HTML_PATH.write_text(root_html, encoding="utf-8")
    DOCS_INDEX_PATH.write_text(docs_html, encoding="utf-8")
    if not DOCS_NOJEKYLL_PATH.exists():
        DOCS_NOJEKYLL_PATH.write_text("", encoding="utf-8")

    size_bytes = OUTPUT_HTML_PATH.stat().st_size
    logger.success("HTML生成完了: {} ({} bytes)", OUTPUT_HTML_PATH, size_bytes)
    logger.info("取引所別保持件数: {}", summarize_records(dataset["records"]))
    logger.info("assetGroup 件数: {}", dict(defaultdict(int, {k: len([r for r in merged_records if r['assetGroup'] == k]) for k in {'gold', 'oil'}})))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
