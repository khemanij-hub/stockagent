# ib_options_brain_v1.0.py - ADAPTED FOR INTERACTIVE BROKERS US MARKET
# Converted from Kite Connect (Indian markets) to IB API (US markets)
# Uses ib_async library for simplified IB API interaction

# SCORING EXPLANATION:
# ====================
# confidence_score (0-4): Plan quality score calculated during trade plan generation
# Factor 1: Volume Confirmation (+1 if Strong)
# Factor 2: Key Level Break (+1 if Confirmed breakout/breakdown)
# Factor 3: OI Alignment (+1 if DOI direction+confirmation aligns with bias)
# Factor 4: Structure Alignment (+1 if 15m and 60m structures match)
# Factor 5: Counter-trend Penalty (-2 if trading against hourly trend)
# Factor 6: OI stale decisive block flag (+1 informational block flag; not added to confidence_score)

import os
import json
import time
import math
import signal
import socket
import subprocess
import threading
import traceback
import concurrent.futures
import logging
import hashlib
import urllib.parse
import urllib.request

fcntl = None
msvcrt = None
try:
    import fcntl  # For file locking (Unix)
except ImportError:
    if os.name == "nt":
        try:
            import msvcrt  # For file locking (Windows)
        except ImportError:
            pass
from dataclasses import dataclass
from datetime import datetime, date, timedelta, timezone
from typing import Dict, List, Optional, Tuple, Any
import pandas as pd
from dateutil import tz
import re

# Interactive Brokers imports
from ib_async import IB, Stock, Option, util, LimitOrder
import asyncio
from threading import Thread

# US market timezone
ET = tz.gettz("America/New_York")

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BOT_BASE_DIR = os.path.abspath(os.getenv("BOT_BASE_DIR", SCRIPT_DIR))
BOT_DATA_DIR = os.path.abspath(os.getenv("BOT_DATA_DIR", BOT_BASE_DIR))
BOT_LOG_DIR = os.path.abspath(os.getenv("BOT_LOG_DIR", BOT_DATA_DIR))


def _ensure_dir(path: str):
    if path:
        os.makedirs(path, exist_ok=True)


def resolve_runtime_path(path: Optional[str], fallback_name: str = "", base_dir: str = BOT_DATA_DIR) -> str:
    raw = "" if path is None else str(path).strip()
    if not raw:
        raw = fallback_name.strip()
    if not raw:
        return ""
    if os.path.isabs(raw):
        return os.path.abspath(raw)
    return os.path.abspath(os.path.join(base_dir, raw))


_ensure_dir(BOT_BASE_DIR)
_ensure_dir(BOT_DATA_DIR)
_ensure_dir(BOT_LOG_DIR)
LOG_FILE_PATH = resolve_runtime_path(os.getenv("LOG_FILE_PATH"), fallback_name="trading_bot_US.log", base_dir=BOT_LOG_DIR)

class _ETFormatter(logging.Formatter):
    """Force all log timestamps to US Eastern Time regardless of system timezone."""
    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created, tz=ET)
        if datefmt:
            return dt.strftime(datefmt)
        return dt.strftime("%Y-%m-%d %H:%M:%S") + f",{int(record.msecs):03d}"

_log_fmt = _ETFormatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
_file_handler = logging.FileHandler(LOG_FILE_PATH)
_file_handler.setFormatter(_log_fmt)
_stream_handler = logging.StreamHandler()
_stream_handler.setFormatter(_log_fmt)
logging.basicConfig(level=logging.INFO, handlers=[_file_handler, _stream_handler])
logger = logging.getLogger(__name__)

# Suppress noisy IB API logs (portfolio updates, client messages)
logging.getLogger('ib_async.wrapper').setLevel(logging.WARNING)
logging.getLogger('ib_async.client').setLevel(logging.WARNING)


class _IBExpectedErrorFilter(logging.Filter):
    """Suppress expected IB API errors from ib_async wrapper/ib loggers.

    - Error 200: No security definition â€” expected for strikes that exist in
      the merged chain but have no contract for the specific expiry.
    - Error 101: Max tickers â€” expected transiently during the OI subscription
      slot swap (we temporarily free underlying slots, but timing can overlap).
    - Error 300: Can't find EId â€” expected when cancelling a subscription that
      was rejected by Error 101 (the ticker ID was never assigned).
    """
    _SUPPRESSED = (
        'Error 200',
        'Error 101',
        'Error 300',
        'Error 354',
        'Unknown contract',
        'No reqId found for contract',
        'ConnectionRefusedError',
        'Make sure API port on TWS/IBG is open',
    )

    def filter(self, record: logging.LogRecord) -> bool:
        msg = record.getMessage()
        return not any(code in msg for code in self._SUPPRESSED)


logging.getLogger('ib_async.wrapper').addFilter(_IBExpectedErrorFilter())
logging.getLogger('ib_async.ib').addFilter(_IBExpectedErrorFilter())
logging.getLogger('ib_async.client').addFilter(_IBExpectedErrorFilter())

class FileLock:
    """Context manager for file locking to prevent race conditions"""
    MAX_RETRIES = 5
    RETRY_DELAY = 0.5  # seconds

    def __init__(self, filepath: str):
        self.filepath = filepath
        self.lock_file = None

    def __enter__(self):
        last_err = None
        for attempt in range(self.MAX_RETRIES):
            try:
                self.lock_file = open(self.filepath + ".lock", "w")
                if fcntl is not None:
                    fcntl.flock(self.lock_file.fileno(), fcntl.LOCK_EX)
                elif msvcrt is not None:
                    # Windows: lock 1 byte at position 0
                    self.lock_file.write("0")
                    self.lock_file.flush()
                    self.lock_file.seek(0)
                    msvcrt.locking(self.lock_file.fileno(), msvcrt.LK_LOCK, 1)
                return self
            except PermissionError as e:
                last_err = e
                if self.lock_file:
                    try:
                        self.lock_file.close()
                    except Exception:
                        pass
                    self.lock_file = None
                if attempt < self.MAX_RETRIES - 1:
                    time.sleep(self.RETRY_DELAY * (attempt + 1))
                    logger.debug(f"FileLock retry {attempt + 1}/{self.MAX_RETRIES} for {self.filepath}: {e}")
        raise last_err

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.lock_file:
            if fcntl is not None:
                fcntl.flock(self.lock_file.fileno(), fcntl.LOCK_UN)
            elif msvcrt is not None:
                try:
                    self.lock_file.seek(0)
                    msvcrt.locking(self.lock_file.fileno(), msvcrt.LK_UNLCK, 1)
                except Exception as e:
                    logger.debug(f"Failed to unlock file {self.filepath}: {e}")
            self.lock_file.close()
        return False

def parse_env_bool(raw_value: Optional[str], default: bool = False) -> bool:
    """Parse common truthy/falsey env values safely."""
    if raw_value is None:
        return default
    val = str(raw_value).strip().lower()
    if val in ("1", "true", "yes", "y", "on"):
        return True
    if val in ("0", "false", "no", "n", "off"):
        return False
    return default

def parse_symbol_lots_env(raw: Optional[str]) -> Dict[str, int]:
    """
    Parse env-style carry list:
    - AAPL:10,MSFT:5
    - AAPL=10; MSFT=5
    """
    out: Dict[str, int] = {}
    txt = "" if raw is None else str(raw).strip()
    if not txt:
        return out
    for chunk in txt.replace(";", ",").split(","):
        part = str(chunk).strip()
        if not part:
            continue
        if ":" in part:
            sym_raw, lots_raw = part.split(":", 1)
        elif "=" in part:
            sym_raw, lots_raw = part.split("=", 1)
        else:
            continue
        sym = str(sym_raw).upper().strip()
        try:
            lots = int(float(str(lots_raw).strip()))
        except Exception:
            continue
        if sym and lots > 0:
            out[sym] = lots
    return out

def resolve_telegram_enabled() -> bool:
    """Explicit TELEGRAM_ENABLED wins; otherwise auto-enable when token+chat are present."""
    explicit = os.getenv("TELEGRAM_ENABLED")
    if explicit is not None:
        return parse_env_bool(explicit, False)
    token = (os.getenv("TELEGRAM_BOT_TOKEN") or os.getenv("TG_BOT_TOKEN") or "").strip()
    chat_ids = (
        os.getenv("TELEGRAM_CHAT_IDS")
        or os.getenv("TELEGRAM_CHAT_ID")
        or os.getenv("TG_APPROVAL_CHAT_IDS")
        or os.getenv("TG_CHANNELS")
        or ""
    ).strip()
    return bool(token and chat_ids)

CONFIG = {
    "IB_HOST": os.getenv("IB_HOST", "127.0.0.1"),
    "IB_PORT": int(os.getenv("IB_PORT", "7496")),  # 7497 for live , 7496 for paper
    "IB_CLIENT_ID": int(os.getenv("IB_CLIENT_ID", "1")),

    "HIST_PACE_SEC": float(os.getenv("HIST_PACE_SEC", "0.8")),

    "HIST_RETRIES": int(os.getenv("HIST_RETRIES", "5")),

    "HIST_TIMEOUT_SEC": float(os.getenv("HIST_TIMEOUT_SEC", "45")),  # seconds
    "CONTRACT_TIMEOUT_SEC": float(os.getenv("CONTRACT_TIMEOUT_SEC", "20")),
    
    # US Market underlyings â€” read from CSV file (one row of comma-separated tickers)
    "INDEX_UNDERLYINGS": [],  # Major ETFs instead of indices
    "STOCK_UNDERLYINGS_CSV": os.getenv("STOCK_UNDERLYINGS_CSV", "US Stocks.csv"),
    
    "STREAM_MODE": os.getenv("STREAM_MODE", "ATM_ONLY"),
    "MAX_SUBSCRIPTIONS": int(os.getenv("MAX_SUBSCRIPTIONS", "100")),
    "CANDLE_MINUTES": int(os.getenv("CANDLE_MINUTES", "15")),
    "HOURLY_CANDLE_MINUTES": int(os.getenv("HOURLY_CANDLE_MINUTES", "60")),
    "HOURLY_FILTER_ENABLED": os.getenv("HOURLY_FILTER_ENABLED", "true").lower() == "true",
    "OPENING_RANGE_MINUTES": int(os.getenv("OPENING_RANGE_MINUTES", "30")),
    "SWING_LOOKBACK_BARS": int(os.getenv("SWING_LOOKBACK_BARS", "20")),
    "VOLUME_CONFIRM_MULT": float(os.getenv("VOLUME_CONFIRM_MULT", "1.5")),
    "DYNAMIC_VOLUME_CONFIRM_ENABLED": os.getenv("DYNAMIC_VOLUME_CONFIRM_ENABLED", "true").lower() == "true",
    "DYNAMIC_VOLUME_CONFIRM_MIN_MULT": float(os.getenv("DYNAMIC_VOLUME_CONFIRM_MIN_MULT", "1.2")),
    "DYNAMIC_VOLUME_CONFIRM_MAX_MULT": float(os.getenv("DYNAMIC_VOLUME_CONFIRM_MAX_MULT", "1.8")),
    "DYNAMIC_VOLUME_CONFIRM_SHORT_BARS": int(os.getenv("DYNAMIC_VOLUME_CONFIRM_SHORT_BARS", "6")),
    "DYNAMIC_VOLUME_CONFIRM_LONG_BARS": int(os.getenv("DYNAMIC_VOLUME_CONFIRM_LONG_BARS", "24")),
    "DOI_WINDOW_STEPS": int(os.getenv("DOI_WINDOW_STEPS", "2")),
    "DOI_MIN_CONFIDENCE_PCT": float(os.getenv("DOI_MIN_CONFIDENCE_PCT", "30")),
    "DOI_CONFIRM_CONFIDENCE_PCT": float(os.getenv("DOI_CONFIRM_CONFIDENCE_PCT", "40")),
    "DOI_UNWIND_BOOST": float(os.getenv("DOI_UNWIND_BOOST", "0.5")),
    "DOI_MIN_ACTIVITY": float(os.getenv("DOI_MIN_ACTIVITY", "0")),
    "NEAR_ATM_STRIKES_EACH_SIDE": int(os.getenv("NEAR_ATM_STRIKES_EACH_SIDE", "5")),
    "OI_EXPIRY_COUNT": int(os.getenv("OI_EXPIRY_COUNT", "1")),
    "RISK_PER_TRADE_PCT_GUIDELINE": float(os.getenv("RISK_PER_TRADE_PCT_GUIDELINE", "0.5")),
    "MIN_RR": float(os.getenv("MIN_RR", "2.0")),
    
    # CSV paths
    "CSV_LIVE_PATH": os.getenv("CSV_LIVE_PATH", "trade_ideas_live_US.csv"),
    "CSV_ACTIONS_PATH": os.getenv("CSV_ACTIONS_PATH", "trade_ideas_actions_US.csv"),
    "CSV_SUMMARY_PATH": os.getenv("CSV_SUMMARY_PATH", "trade_ideas_summary_US.csv"),
    "OI_SNAPSHOT_PATH": os.getenv("OI_SNAPSHOT_PATH", "oi_snapshot_US.json"),
    "SIGNAL_STATE_PATH": os.getenv("SIGNAL_STATE_PATH", "signal_state_US.json"),
    "ENABLE_STICKY_SIGNALS": os.getenv("ENABLE_STICKY_SIGNALS", "true").lower() == "true",
    "STICKY_KEY_LEVEL_MAX_CANDLES": int(os.getenv("STICKY_KEY_LEVEL_MAX_CANDLES", "3")),
    "KEY_LEVEL_ATR_PERIOD": int(os.getenv("KEY_LEVEL_ATR_PERIOD", "14")),
    "KEY_LEVEL_MIN_BREAK_PCT": float(os.getenv("KEY_LEVEL_MIN_BREAK_PCT", "0.001")),
    "KEY_LEVEL_MIN_BREAK_ATR_MULT": float(os.getenv("KEY_LEVEL_MIN_BREAK_ATR_MULT", "0.2")),
    "KEY_LEVEL_MIN_BREAK_POINTS": float(os.getenv("KEY_LEVEL_MIN_BREAK_POINTS", "0")),
    "ACTIONABLE_MIN_CONFIDENCE": int(os.getenv("ACTIONABLE_MIN_CONFIDENCE", "3")),
    "SUMMARY_NEW_MIN_CONFIDENCE": int(os.getenv("SUMMARY_NEW_MIN_CONFIDENCE", "3")),
    "SUMMARY_INCLUDE_WAIT": os.getenv("SUMMARY_INCLUDE_WAIT", "false").lower() == "true",
    "SUMMARY_SYNC_INTERVAL_SEC": float(os.getenv("SUMMARY_SYNC_INTERVAL_SEC", "120")),
    "AUTO_VM_SHUTDOWN_ON_MARKET_CLOSE": os.getenv("AUTO_VM_SHUTDOWN_ON_MARKET_CLOSE", "false").lower() == "true",
    "VM_SHUTDOWN_GRACE_SECONDS": int(os.getenv("VM_SHUTDOWN_GRACE_SECONDS", "30")),
    "VM_SHUTDOWN_RETRY_SECONDS": float(os.getenv("VM_SHUTDOWN_RETRY_SECONDS", "300")),
    "VM_SHUTDOWN_FORCE_APPS": os.getenv("VM_SHUTDOWN_FORCE_APPS", "true").lower() == "true",
    "VM_SHUTDOWN_COMMAND": os.getenv("VM_SHUTDOWN_COMMAND", ""),
    "VM_SHUTDOWN_CMD_TIMEOUT_SEC": float(os.getenv("VM_SHUTDOWN_CMD_TIMEOUT_SEC", "12")),
    "CARRY_FORWARD_LOTS": parse_symbol_lots_env(os.getenv("CARRY_FORWARD_LOTS", "")),
    "CSV_POLL_SECONDS": int(os.getenv("CSV_POLL_SECONDS", "10")),
    "DISCONNECT_PAUSE_SECONDS": float(os.getenv("DISCONNECT_PAUSE_SECONDS", "10")),
    "WRITE_ALL_SIGNALS": os.getenv("WRITE_ALL_SIGNALS", "true").lower() == "true",  # Write all signals or only when bias changes
    "IDEA_WORKERS": int(os.getenv("IDEA_WORKERS", "3")),
    "OI_POLL_INTERVAL_SEC": float(os.getenv("OI_POLL_INTERVAL_SEC", "0.2")),
    "OI_POLL_MAX_WAIT_SEC": float(os.getenv("OI_POLL_MAX_WAIT_SEC", "2.0")),
    "TYPICAL_VOL_PREFETCH_ENABLED": os.getenv("TYPICAL_VOL_PREFETCH_ENABLED", "false").lower() == "true",
    "TYPICAL_VOL_PREFETCH_PARALLEL": int(os.getenv("TYPICAL_VOL_PREFETCH_PARALLEL", "2")),
    "INCLUDE_PLAN_JSON": os.getenv("INCLUDE_PLAN_JSON", "true").lower() == "true",

    "SEED_HISTORY_DAYS": int(os.getenv("SEED_HISTORY_DAYS", "30")),  # Increased from 10 to 30 to match Kite for better swing level detection
    "SEED_TIMEOUT_SEC": float(os.getenv("SEED_TIMEOUT_SEC", "1200")),
    "SEED_PARALLEL_BATCH": int(os.getenv("SEED_PARALLEL_BATCH", "2")),  # Symbols seeded in parallel (2 = 4 concurrent hist requests; 3+ triggers IB pacing retries)
    "NEWS_FEED_CSV_PATH": os.getenv("NEWS_FEED_CSV_PATH", "news_feed.csv"),
    "NEWS_LOOKBACK_MINUTES": int(os.getenv("NEWS_LOOKBACK_MINUTES", "240")),
    "NEWS_MAX_ITEMS": int(os.getenv("NEWS_MAX_ITEMS", "3")),
    "ENABLE_SCORING_OUTPUT": os.getenv("ENABLE_SCORING_OUTPUT", "false").lower() == "true",
    "LOG_GATE_DIAGNOSTICS": os.getenv("LOG_GATE_DIAGNOSTICS", "true").lower() == "true",
    "LOG_GATE_DIAGNOSTICS_ONLY_BLOCKED": os.getenv("LOG_GATE_DIAGNOSTICS_ONLY_BLOCKED", "true").lower() == "true",
    "API_RETRIES": int(os.getenv("API_RETRIES", "3")),
    "API_RETRY_BACKOFF_SECONDS": float(os.getenv("API_RETRY_BACKOFF_SECONDS", "0.7")),
    "CB_MAX_FAILS": int(os.getenv("CB_MAX_FAILS", "3")),
    "CB_COOLDOWN_MINUTES": int(os.getenv("CB_COOLDOWN_MINUTES", "10")),
    "TELEGRAM_ENABLED": resolve_telegram_enabled(),
    "TELEGRAM_BOT_TOKEN": os.getenv("TELEGRAM_BOT_TOKEN", os.getenv("TG_BOT_TOKEN", "")),
    "TELEGRAM_CHAT_IDS": os.getenv("TELEGRAM_CHAT_IDS", os.getenv("TELEGRAM_CHAT_ID", os.getenv("TG_APPROVAL_CHAT_IDS", os.getenv("TG_CHANNELS", "")))),
    "TELEGRAM_POLL_SECONDS": float(os.getenv("TELEGRAM_POLL_SECONDS", "2.0")),
    "TELEGRAM_STATE_PATH": os.getenv("TELEGRAM_STATE_PATH", "telegram_approvals_state_US.json"),
    "LOG_FILE_PATH": LOG_FILE_PATH,
}

for _path_key in [
    "STOCK_UNDERLYINGS_CSV",
    "CSV_LIVE_PATH",
    "CSV_ACTIONS_PATH",
    "CSV_SUMMARY_PATH",
    "OI_SNAPSHOT_PATH",
    "SIGNAL_STATE_PATH",
    "NEWS_FEED_CSV_PATH",
    "TELEGRAM_STATE_PATH",
]:
    base = BOT_BASE_DIR if _path_key == "STOCK_UNDERLYINGS_CSV" else BOT_DATA_DIR
    CONFIG[_path_key] = resolve_runtime_path(CONFIG.get(_path_key), base_dir=base)
CONFIG["LOG_FILE_PATH"] = resolve_runtime_path(CONFIG.get("LOG_FILE_PATH"), fallback_name="trading_bot_US.log", base_dir=BOT_LOG_DIR)
for _path_key in [
    "CSV_LIVE_PATH",
    "CSV_ACTIONS_PATH",
    "CSV_SUMMARY_PATH",
    "OI_SNAPSHOT_PATH",
    "SIGNAL_STATE_PATH",
    "TELEGRAM_STATE_PATH",
    "LOG_FILE_PATH",
]:
    _ensure_dir(os.path.dirname(str(CONFIG.get(_path_key, "")).strip()))


def _load_underlyings_from_csv(csv_path: str) -> List[str]:
    """Read ticker symbols from a CSV file.

    Supports both single-row comma-separated (e.g. AAPL,MSFT,GOOGL)
    and single-column formats.  Returns an empty list if the file
    doesn't exist or is empty.
    """
    resolved = resolve_runtime_path(csv_path, base_dir=BOT_BASE_DIR)
    if not os.path.isfile(resolved):
        logger.warning(f"Underlyings CSV not found: {resolved}")
        return []
    try:
        with open(resolved, encoding="utf-8-sig") as f:
            raw = f.read().strip()
        if not raw:
            return []
        # Single row of comma-separated tickers or one ticker per line
        # De-duplicate while preserving order to avoid repeated scans.
        symbols_raw = [s.strip() for s in raw.replace("\n", ",").split(",") if s.strip()]
        symbols: List[str] = []
        seen: set = set()
        duplicates: List[str] = []
        for sym in symbols_raw:
            key = str(sym).upper().strip()
            if not key:
                continue
            if key in seen:
                duplicates.append(key)
                continue
            seen.add(key)
            symbols.append(key)
        if duplicates:
            unique_dupes = sorted(set(duplicates))
            preview = ", ".join(unique_dupes[:10])
            extra = "" if len(unique_dupes) <= 10 else f" (+{len(unique_dupes) - 10} more)"
            logger.warning(
                "Underlyings CSV had duplicate tickers; skipped %d duplicate entries. Samples: %s%s",
                len(duplicates),
                preview,
                extra,
            )
        return symbols
    except Exception as e:
        logger.error(f"Error reading underlyings CSV {resolved}: {e}")
        return []


CONFIG["STOCK_UNDERLYINGS_US"] = _load_underlyings_from_csv(CONFIG["STOCK_UNDERLYINGS_CSV"])

logger.info("=" * 60)
logger.info("Configuration loaded successfully")
logger.info(f"BOT_BASE_DIR = {BOT_BASE_DIR}")
logger.info(f"BOT_DATA_DIR = {BOT_DATA_DIR}")
logger.info(f"BOT_LOG_DIR = {BOT_LOG_DIR}")
logger.info(f"CWD = {os.getcwd()}")
logger.info(f"LIVE CSV = {CONFIG['CSV_LIVE_PATH']}")
logger.info(f"ACTIONS CSV = {CONFIG['CSV_ACTIONS_PATH']}")
logger.info(f"SUMMARY CSV = {CONFIG['CSV_SUMMARY_PATH']}")
logger.info(f"LOG FILE = {CONFIG['LOG_FILE_PATH']}")
logger.info(
    "AUTO_VM_SHUTDOWN_ON_MARKET_CLOSE = %s (grace=%ss, retry=%ss)",
    CONFIG.get("AUTO_VM_SHUTDOWN_ON_MARKET_CLOSE", False),
    CONFIG.get("VM_SHUTDOWN_GRACE_SECONDS", 30),
    CONFIG.get("VM_SHUTDOWN_RETRY_SECONDS", 300),
)
_cfg_carry = CONFIG.get("CARRY_FORWARD_LOTS", {})
if isinstance(_cfg_carry, dict) and _cfg_carry:
    _cfg_items = []
    for k, v in sorted(_cfg_carry.items()):
        key_txt = str(k).upper().strip()
        try:
            lots_val = int(float(v))
        except Exception:
            lots_val = 0
        if key_txt and lots_val > 0:
            _cfg_items.append(f"{key_txt}:{lots_val}")
    _cfg_carry_txt = ", ".join(_cfg_items) if _cfg_items else "none"
else:
    _cfg_carry_txt = "none"
logger.info(f"Carry-forward config (CARRY_FORWARD_LOTS): {_cfg_carry_txt}")
logger.info("=" * 60)

FACTOR_COLUMNS = [
    "factor_volume",
    "factor_key_level_break",
    "factor_oi_alignment",
    "factor_structure_alignment",
    "factor_counter_trend_penalty",
    "factor_oi_stale_decisive_block",
]

GATE_COLUMNS = [
    "gate_break_confirmed",
    "gate_volume_strong",
    "gate_key_level_break",
    "gate_oi_alignment",
    "gate_structure_alignment",
    "gate_counter_trend_ok",
    "gate_confidence_ok",
    "gate_oi_freshness_ok",
    "gate_actionable",
    "gate_blockers",
]

CSV_HEADERS = [
    "row_id", "timestamp_et", "session_context",
    "underlying_type", "symbol",
    "expiry", "strike", "right", "tradingsymbol", "contract_multiplier",
    "status", "approve", "kill_switch",
    "ltp", "pct_change", "day_high", "day_low", "prev_close",
    "volume", "vol_vs_20d_avg", "vol_vs_time_of_day",
    "vwap_relation", "day_range",
    "key_support", "key_resistance",
    "opening_range_high", "opening_range_low",
    "prior_day_high", "prior_day_low",
    "swing_high", "swing_low",
    "structure",
    "volume_confirmation",
    "breakout_status", "breakdown_status",
    "expiry_used",
    "top_call_oi_strikes", "top_put_oi_strikes",
    "top_call_doi_strikes", "top_put_doi_strikes",
    "pcr_overall", "pcr_near_atm",
    "oi_support_zone", "oi_resistance_zone",
    "oi_shift_notes", "iv_notes",
    "news_bullets", "news_sources",
    "bias", "setup_type",
    "primary_entry", "primary_sl",
    "primary_t1", "primary_t2",
    "alt_scenario",
    "risk_per_trade_pct", "min_rr",
    "confidence",
    "confidence_score",
    *FACTOR_COLUMNS,
    *GATE_COLUMNS,
    "change_mind_checklist",
    "order_payload_json", "execution_result",
    "plan_json",
]

ACTIONS_HEADERS = [
    "symbol",
    "status",
    "confidence",
    "confidence_score",
    "factor_oi_stale_decisive_block",
    "tradingsymbol",
    "request_id",
    "primary_entry",
    "primary_sl",
    "primary_t1",
    "primary_t2",
    "approve",
    "lots",
    "approval_source",
    "approval_updated_at",
    "telegram_alert_sent_at",
    "execution_status",
    "execution_result",
    "executed_at",
    "executed_row_id",
    "executed_order_id",
]

SUMMARY_HEADERS = [
    "summary_run_ts_et",
    "rank", "row_id", "timestamp_et", "symbol", "underlying_type",
    "ltp", "pct_change",
    "setup_type", "confidence", "confidence_score",
    "breakout_status", "breakdown_status", "volume_confirmation",
    *FACTOR_COLUMNS,
    *GATE_COLUMNS,
    "primary_entry", "primary_sl", "primary_t1", "primary_t2",
    "tradingsymbol", "expiry", "right", "strike",
    "status", "bias",
    "minutes_since_signal", "count",
]
SUMMARY_HEADERS = list(dict.fromkeys(SUMMARY_HEADERS))

def safe_str(x):
    """Safely convert any value to string, handling None/float/int"""
    if x is None or pd.isna(x):
        return ""
    if isinstance(x, (int, float)):
        return str(x)
    return str(x).strip()

def confidence_grade(score: int) -> str:
    return "High" if score >= 3 else "Medium" if score == 2 else "Low"

def now_et() -> datetime:
    return datetime.now(tz=ET)

def to_et(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(ET)

def et_session_context(ts_et: datetime) -> str:
    """US market session times (9:30 AM - 4:00 PM ET)"""
    from datetime import time as dt_time
    t = ts_et.time()
    if t < dt_time(9, 30):
        return "pre-market"
    if t <= dt_time(16, 0):
        return "live"
    if t <= dt_time(20, 0):
        return "after-hours"
    return "closed"

def floor_time(ts_et: datetime, minutes: int) -> datetime:
    m = (ts_et.minute // minutes) * minutes
    return ts_et.replace(minute=m, second=0, microsecond=0)

def safe_float(x, default=None):
    try:
        return float(x)
    except Exception:
        return default

def safe_int(x, default=0):
    try:
        return int(float(x))
    except Exception:
        return default

def round_to_strike_step(price: float, step: float) -> float:
    """Round price to nearest strike step"""
    return round(price / step) * step

def parse_chat_ids(raw: str) -> List[str]:
    ids: List[str] = []
    normalized = safe_str(raw).replace("[", "").replace("]", "").replace("\"", "").replace("'", "")
    for part in normalized.replace(";", ",").split(","):
        chat_id = safe_str(part).strip()
        if chat_id.startswith("@"):
            chat_id = chat_id[1:]
        if chat_id:
            ids.append(chat_id)
    return ids

def build_request_id(symbol: str, tradingsymbol: str) -> str:
    sym = safe_str(symbol).upper().strip()
    ts = safe_str(tradingsymbol).strip()
    return f"{sym}|{(ts if ts else sym)}"

def request_signature(request_id: str) -> str:
    rid = safe_str(request_id)
    if not rid:
        return ""
    return hashlib.sha1(rid.encode("utf-8")).hexdigest()[:8]

class TelegramApprovalBridge:
    def __init__(self, cfg: Dict[str, Any], actions_path: str):
        self.cfg = cfg
        self.actions_path = actions_path
        self.enabled = bool(cfg.get("TELEGRAM_ENABLED", False))
        self.disabled_reason = ""
        self.token = safe_str(cfg.get("TELEGRAM_BOT_TOKEN", ""))
        self.allowed_chat_ids = parse_chat_ids(cfg.get("TELEGRAM_CHAT_IDS", ""))
        self.allowed_chat_lookup = {safe_str(x).lstrip("@") for x in self.allowed_chat_ids}
        self.poll_seconds = max(1.0, float(cfg.get("TELEGRAM_POLL_SECONDS", 2.0)))
        self.state_path = safe_str(cfg.get("TELEGRAM_STATE_PATH", "telegram_approvals_state_US.json")) or "telegram_approvals_state_US.json"
        self.api_timeout = 15
        self.offset = 0
        self.notified_request_ids: Dict[str, str] = {}
        self._alert_decision_last_state: Dict[str, str] = {}

        if not self.enabled:
            self.enabled = False
            self.disabled_reason = (
                "TELEGRAM_ENABLED is false; set TELEGRAM_ENABLED=true or provide "
                "TELEGRAM_BOT_TOKEN + TELEGRAM_CHAT_IDS"
            )
            return
        if not self.token:
            self.enabled = False
            self.disabled_reason = "Missing TELEGRAM_BOT_TOKEN/TG_BOT_TOKEN"
            return
        if not self.allowed_chat_ids:
            self.enabled = False
            self.disabled_reason = (
                "Missing TELEGRAM_CHAT_IDS/TELEGRAM_CHAT_ID/TG_APPROVAL_CHAT_IDS/TG_CHANNELS"
            )
            return

        state = self._load_state()
        self.offset = safe_int(state.get("offset", 0), 0)
        raw_notified = state.get("notified_request_ids", {})
        if isinstance(raw_notified, dict):
            for req_id, ts_text in raw_notified.items():
                rid = safe_str(req_id).strip()
                if rid:
                    self.notified_request_ids[rid] = safe_str(ts_text)
        elif isinstance(raw_notified, list):
            # Backward compatibility for old list-shaped state payloads.
            for req_id in raw_notified:
                rid = safe_str(req_id).strip()
                if rid:
                    self.notified_request_ids[rid] = ""

    def _base_url(self) -> str:
        return f"https://api.telegram.org/bot{self.token}"

    def _load_state(self) -> Dict[str, Any]:
        try:
            if (not os.path.exists(self.state_path)) or os.path.getsize(self.state_path) <= 0:
                return {"offset": 0}
            with open(self.state_path, "r", encoding="utf-8") as f:
                raw = json.load(f)
            if isinstance(raw, dict):
                return raw
        except Exception:
            pass
        return {"offset": 0}

    def _save_state(self):
        try:
            payload = {"offset": self.offset, "notified_request_ids": self.notified_request_ids}
            tmp = f"{self.state_path}.tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=True, separators=(",", ":"))
            os.replace(tmp, self.state_path)
        except Exception as e:
            logger.debug(f"Telegram state save failed: {e}")

    def _remember_notified_request(self, request_id: str, ts_text: str):
        rid = safe_str(request_id).strip()
        if not rid:
            return
        # Move existing keys to the end so pruning drops the oldest first.
        if rid in self.notified_request_ids:
            try:
                del self.notified_request_ids[rid]
            except Exception:
                pass
        self.notified_request_ids[rid] = safe_str(ts_text)
        max_keep = 5000
        if len(self.notified_request_ids) > max_keep:
            overflow = len(self.notified_request_ids) - max_keep
            for old_key in list(self.notified_request_ids.keys())[:overflow]:
                self.notified_request_ids.pop(old_key, None)

    def _log_alert_decision(
        self,
        symbol: str,
        request_id: str,
        decision: str,
        reason: str,
        confidence_score: Optional[int] = None,
        tradingsymbol: str = "",
        note: str = "",
    ):
        sym = safe_str(symbol).upper().strip()
        rid = safe_str(request_id).strip()
        tsym = safe_str(tradingsymbol).strip()
        note_text = safe_str(note).strip()
        score_text = safe_str(confidence_score) if confidence_score is not None else ""
        decision_txt = safe_str(decision).upper().strip()
        reason_txt = safe_str(reason).strip()

        # Log only when the per-request decision state changes.
        state_key = rid if rid else (f"sym:{sym}" if sym else "unknown")
        state_val = f"{decision_txt}|{reason_txt}|{score_text}|{tsym}|{note_text}"
        last_state = safe_str(self._alert_decision_last_state.get(state_key, ""))
        if state_val == last_state:
            return
        if state_key in self._alert_decision_last_state:
            try:
                del self._alert_decision_last_state[state_key]
            except Exception:
                pass
        self._alert_decision_last_state[state_key] = state_val
        if len(self._alert_decision_last_state) > 5000:
            overflow = len(self._alert_decision_last_state) - 5000
            for old_key in list(self._alert_decision_last_state.keys())[:overflow]:
                self._alert_decision_last_state.pop(old_key, None)
        logger.info(
            "Telegram alert decision: symbol=%s req_sig=%s ts=%s score=%s decision=%s reason=%s%s",
            sym if sym else "?",
            request_signature(rid) if rid else "NA",
            tsym,
            score_text,
            decision_txt,
            reason_txt,
            f" | {note_text}" if note_text else "",
        )

    def _api_call(self, method: str, params: Dict[str, Any]) -> Any:
        url = f"{self._base_url()}/{method}"
        payload = {}
        for k, v in params.items():
            if isinstance(v, (dict, list)):
                payload[k] = json.dumps(v, ensure_ascii=True)
            else:
                payload[k] = safe_str(v)
        data = urllib.parse.urlencode(payload).encode("utf-8")
        req = urllib.request.Request(url, data=data)
        with urllib.request.urlopen(req, timeout=self.api_timeout) as resp:
            body = resp.read().decode("utf-8")
        out = json.loads(body)
        if not out.get("ok"):
            raise RuntimeError(f"Telegram API {method} failed: {out}")
        return out.get("result")

    def _send_message(self, chat_id: str, text: str, reply_markup: Optional[Dict[str, Any]] = None):
        target = safe_str(chat_id).strip()
        if target and (not target.lstrip("-").isdigit()) and (not target.startswith("@")):
            target = f"@{target}"
        params: Dict[str, Any] = {"chat_id": target, "text": text}
        if reply_markup:
            params["reply_markup"] = reply_markup
        return self._api_call("sendMessage", params)

    def _answer_callback(self, callback_query_id: str, text: str):
        try:
            self._api_call("answerCallbackQuery", {"callback_query_id": callback_query_id, "text": text, "show_alert": False})
        except Exception as e:
            logger.debug(f"Telegram answerCallbackQuery failed: {e}")

    def _disable_callback_keyboard(self, callback_query: Dict[str, Any]):
        """
        Best-effort: remove inline keyboard after first tap to reduce accidental double clicks.
        """
        try:
            msg = callback_query.get("message") or {}
            chat_id = safe_str((msg.get("chat") or {}).get("id", ""))
            message_id = safe_str(msg.get("message_id", ""))
            if (not chat_id) or (not message_id):
                return
            self._api_call(
                "editMessageReplyMarkup",
                {
                    "chat_id": chat_id,
                    "message_id": message_id,
                    "reply_markup": {"inline_keyboard": []},
                },
            )
        except Exception as e:
            logger.debug(f"Telegram editMessageReplyMarkup failed: {e}")

    def notify_execution(self, symbol: str, status: str, result_text: str, row_id: str = "", order_id: str = "", tradingsymbol: str = ""):
        if not self.enabled:
            return
        msg = (
            "Execution update\n"
            f"Symbol: {safe_str(symbol).upper().strip()}\n"
            f"TS: {safe_str(tradingsymbol)}\n"
            f"Status: {safe_str(status)}\n"
            f"Row: {safe_str(row_id)}\n"
            f"Order ID: {safe_str(order_id) if safe_str(order_id) else 'NA'}\n"
            f"Result: {safe_str(result_text)[:1500]}"
        )
        for chat_id in self.allowed_chat_ids:
            try:
                self._send_message(chat_id, msg)
            except Exception as e:
                logger.warning(f"Telegram execution notify failed for chat {chat_id}: {e}")

    def _chat_allowed(self, chat_id: str) -> bool:
        return safe_str(chat_id).lstrip("@") in self.allowed_chat_lookup

    def _parse_text_approval(self, text: str) -> Optional[Tuple[str, str, int]]:
        raw = safe_str(text).strip()
        if not raw:
            return None
        parts = [p for p in raw.replace("/", " ").split() if p]
        if len(parts) < 2:
            return None
        cmd = parts[0].upper()
        if cmd in ("YES", "APPROVE"):
            if len(parts) < 3:
                return None
            symbol = safe_str(parts[1]).upper().strip()
            lots = max(0, safe_int(parts[2], 0))
            return symbol, "YES", lots
        if cmd in ("NO", "REJECT"):
            symbol = safe_str(parts[1]).upper().strip()
            return symbol, "NO", 0
        return None

    def _apply_approval(self, symbol: str, approve: str, lots: int, source: str) -> str:
        actions = load_actions_map(self.actions_path)
        a = actions.get(symbol)
        if a is None:
            return f"{symbol} not found in actions."
        if approve == "YES" and lots <= 0:
            return "Lots must be > 0 for YES."
        now_text = now_et().strftime("%Y-%m-%d %H:%M:%S")
        update_action_row(
            self.actions_path,
            symbol,
            {
                "approve": approve,
                "lots": lots if approve == "YES" else 0,
                "approval_source": source,
                "approval_updated_at": now_text,
            },
        )
        return f"{symbol} set to {approve} lots={lots if approve == 'YES' else 0}."

    def notify_new_requests(self):
        if not self.enabled:
            return
        actions = load_actions_map(self.actions_path)
        actionable_min_conf = max(0, min(4, safe_int(self.cfg.get("ACTIONABLE_MIN_CONFIDENCE", 3), 3)))
        for symbol, row in actions.items():
            req_id = safe_str(row.get("request_id", ""))
            tradingsymbol = safe_str(row.get("tradingsymbol", ""))
            confidence_score_val = safe_int(row.get("confidence_score", 0), 0)
            if not req_id:
                self._log_alert_decision(
                    symbol, req_id, "SKIP", "missing_request_id",
                    confidence_score=confidence_score_val, tradingsymbol=tradingsymbol
                )
                continue
            exec_status = safe_str(row.get("execution_status", "")).upper().strip()
            if exec_status in ("ORDER_PLACED", "EXECUTED", "FILLED"):
                marker_ts = (
                    safe_str(row.get("executed_at", ""))
                    or safe_str(row.get("telegram_alert_sent_at", ""))
                    or now_et().strftime("%Y-%m-%d %H:%M:%S")
                )
                self._remember_notified_request(req_id, marker_ts)
                self._log_alert_decision(
                    symbol, req_id, "SKIP", "execution_already_recorded",
                    confidence_score=confidence_score_val, tradingsymbol=tradingsymbol, note=exec_status
                )
                continue
            status_val = safe_str(row.get("status", "")).upper().strip()
            if status_val in ("OPEN_CARRY", "CARRY_FORWARD"):
                self._log_alert_decision(
                    symbol, req_id, "SKIP", "carry_forward_open_position",
                    confidence_score=confidence_score_val, tradingsymbol=tradingsymbol
                )
                continue
            remembered_ts = safe_str(self.notified_request_ids.get(req_id, ""))
            row_sent_ts = safe_str(row.get("telegram_alert_sent_at", ""))
            if remembered_ts:
                # Self-heal CSV flag if another writer race cleared it.
                if not row_sent_ts:
                    update_action_row(
                        self.actions_path,
                        symbol,
                        {"telegram_alert_sent_at": remembered_ts},
                    )
                    self._log_alert_decision(
                        symbol, req_id, "SKIP", "dedupe_state_self_heal",
                        confidence_score=confidence_score_val, tradingsymbol=tradingsymbol, note=remembered_ts
                    )
                else:
                    self._log_alert_decision(
                        symbol, req_id, "SKIP", "dedupe_state_already_notified",
                        confidence_score=confidence_score_val, tradingsymbol=tradingsymbol, note=remembered_ts
                    )
                continue
            if row_sent_ts:
                # Seed in-memory dedupe from existing CSV state (supports hot restarts).
                self._remember_notified_request(req_id, row_sent_ts)
                self._log_alert_decision(
                    symbol, req_id, "SKIP", "csv_already_notified",
                    confidence_score=confidence_score_val, tradingsymbol=tradingsymbol, note=row_sent_ts
                )
                continue
            oi_stale_decisive_block = safe_int(row.get("factor_oi_stale_decisive_block", 0), 0) > 0
            if oi_stale_decisive_block:
                self._log_alert_decision(
                    symbol, req_id, "SKIP", "stale_oi_decisive_block",
                    confidence_score=confidence_score_val, tradingsymbol=tradingsymbol
                )
                continue
            # Only actionable setups (default confidence 3/4) should trigger approval prompts.
            if confidence_score_val < actionable_min_conf:
                self._log_alert_decision(
                    symbol, req_id, "SKIP", "low_confidence",
                    confidence_score=confidence_score_val, tradingsymbol=tradingsymbol
                )
                continue
            confidence = safe_str(row.get("confidence", ""))
            confidence_score = safe_str(row.get("confidence_score", ""))
            bias = safe_str(row.get("bias", ""))
            entry = safe_str(row.get("primary_entry", ""))
            sl = safe_str(row.get("primary_sl", ""))
            t1 = safe_str(row.get("primary_t1", ""))
            t2 = safe_str(row.get("primary_t2", ""))
            sig = request_signature(req_id)
            text = (
                f"New trade request\n"
                f"Symbol: {symbol}\n"
                f"TS: {tradingsymbol}\n"
                f"Request: {req_id}\n"
                f"Bias: {bias}\n"
                f"Confidence: {confidence} ({confidence_score})\n"
                f"Entry: {entry}\nSL: {sl}\nT1: {t1}\nT2: {t2}\n\n"
                f"Reply with: YES {symbol} <lots> or NO {symbol}"
            )
            keyboard = {
                "inline_keyboard": [
                    [
                        {"text": "YES x1", "callback_data": f"YES|{symbol}|1|{sig}"},
                        {"text": "YES x2", "callback_data": f"YES|{symbol}|2|{sig}"},
                    ],
                    [
                        {"text": "NO", "callback_data": f"NO|{symbol}|0|{sig}"},
                    ],
                ]
            }
            sent = False
            for chat_id in self.allowed_chat_ids:
                try:
                    self._send_message(chat_id, text, reply_markup=keyboard)
                    sent = True
                except Exception as e:
                    logger.warning(f"Telegram send failed for chat {chat_id}: {e}")
            if sent:
                sent_at = now_et().strftime("%Y-%m-%d %H:%M:%S")
                update_action_row(
                    self.actions_path,
                    symbol,
                    {"telegram_alert_sent_at": sent_at},
                )
                self._remember_notified_request(req_id, sent_at)
                self._save_state()
                self._log_alert_decision(
                    symbol, req_id, "SEND", "telegram_alert_sent",
                    confidence_score=confidence_score_val, tradingsymbol=tradingsymbol, note=sent_at
                )
            else:
                self._log_alert_decision(
                    symbol, req_id, "SKIP", "send_failed_all_chats",
                    confidence_score=confidence_score_val, tradingsymbol=tradingsymbol
                )

    def poll_updates(self):
        if not self.enabled:
            return
        try:
            updates = self._api_call("getUpdates", {"offset": self.offset, "timeout": 0})
        except Exception as e:
            logger.debug(f"Telegram getUpdates failed: {e}")
            return
        if not isinstance(updates, list):
            return
        for upd in updates:
            self.offset = max(self.offset, safe_int(upd.get("update_id", 0), 0) + 1)
            cb = upd.get("callback_query")
            if cb:
                chat_id = safe_str(((cb.get("message") or {}).get("chat") or {}).get("id", ""))
                if not self._chat_allowed(chat_id):
                    self._answer_callback(safe_str(cb.get("id", "")), "Not authorized.")
                    continue
                data = safe_str(cb.get("data", ""))
                parts = data.split("|")
                if len(parts) != 4:
                    self._answer_callback(safe_str(cb.get("id", "")), "Bad action.")
                    continue
                approve = safe_str(parts[0]).upper()
                symbol = safe_str(parts[1]).upper().strip()
                lots = max(0, safe_int(parts[2], 0))
                sig = safe_str(parts[3])
                actions = load_actions_map(self.actions_path)
                current = actions.get(symbol)
                if current is None:
                    self._answer_callback(safe_str(cb.get("id", "")), "Symbol not found.")
                    continue
                req_id = safe_str(current.get("request_id", ""))
                if request_signature(req_id) != sig:
                    self._answer_callback(safe_str(cb.get("id", "")), "Request changed. Use latest alert.")
                    self._disable_callback_keyboard(cb)
                    continue
                cur_approve = safe_str(current.get("approve", "NO")).upper()
                cur_lots = max(0, safe_int(current.get("lots", 0), 0))
                if approve == cur_approve and ((approve != "YES") or (lots == cur_lots)):
                    self._answer_callback(safe_str(cb.get("id", "")), "Already captured.")
                    self._disable_callback_keyboard(cb)
                    continue
                msg = self._apply_approval(symbol, approve, lots, f"TELEGRAM:{chat_id}")
                self._answer_callback(safe_str(cb.get("id", "")), msg[:180])
                self._disable_callback_keyboard(cb)
                continue

            msg = upd.get("message")
            if not msg:
                continue
            chat_id = safe_str((msg.get("chat") or {}).get("id", ""))
            if not self._chat_allowed(chat_id):
                continue
            text = safe_str(msg.get("text", ""))
            parsed = self._parse_text_approval(text)
            if not parsed:
                continue
            symbol, approve, lots = parsed
            try:
                out = self._apply_approval(symbol, approve, lots, f"TELEGRAM:{chat_id}")
                self._send_message(chat_id, out)
            except Exception as e:
                self._send_message(chat_id, f"Approval failed: {e}")
        self._save_state()

def strike_key(x: Any) -> str:
    """Stable string key for strike values across int/float/decimal inputs."""
    v = safe_float(x)
    if v is None:
        return ""
    return f"{v:.4f}".rstrip("0").rstrip(".")

def csv_safe_text(x: Any) -> str:
    if x is None:
        return ""
    if isinstance(x, list):
        x = " | ".join([str(i) for i in x])
    s = str(x)
    s = s.replace("\r\n", " | ").replace("\n", " | ").replace("\r", " | ")
    s = " ".join(s.split())
    return s

def minutes_to_ib_bar_size(minutes: int) -> str:
    """
    Convert minutes to Interactive Brokers compatible bar size format.

    IB requires specific formats:
    - For hourly intervals: "1 hour", "2 hours", etc. (NOT "60 mins")
    - For minute intervals: "1 min", "15 mins", etc.

    Valid IB bar sizes: 1 min, 2 mins, 3 mins, 5 mins, 10 mins, 15 mins,
    20 mins, 30 mins, 1 hour, 2 hours, 3 hours, 4 hours, 8 hours, 1 day, 1W, 1M

    Args:
        minutes: Number of minutes for the bar size

    Returns:
        IB-compatible bar size string
    """
    if minutes >= 60 and minutes % 60 == 0:
        hours = minutes // 60
        return f"{hours} hour" if hours == 1 else f"{hours} hours"
    else:
        return f"{minutes} min" if minutes == 1 else f"{minutes} mins"

def _parse_strike_doi(doi_str: str) -> Dict[float, float]:
    """Parse DOI string like '260:+1234 | 270:-500 | 250:+0' into {260: 1234, 270: -500, 250: 0}"""
    result: Dict[float, float] = {}
    if not doi_str or doi_str == "Unavailable":
        return result
    try:
        for pair in str(doi_str).split(" | "):
            parts = pair.split(":")
            if len(parts) == 2:
                strike = float(parts[0].strip())
                delta = float(parts[1].strip().replace(",", "").replace("+", ""))
                result[strike] = delta
    except Exception:
        pass
    return result

def _infer_step(strikes: list, default_step: float = 5.0) -> float:
    if len(strikes) < 2:
        return default_step
    diffs = [strikes[i+1] - strikes[i] for i in range(len(strikes)-1) if strikes[i+1] > strikes[i]]
    return min(diffs) if diffs else default_step

def parse_zone_bounds(zone_text: str) -> Tuple[Optional[float], Optional[float]]:
    """Parse zone text like '6850-7000' or '6900' into numeric low/high bounds."""
    txt = safe_str(zone_text)
    if (not txt) or txt.upper() == "UNAVAILABLE":
        return None, None
    if "-" not in txt:
        v = safe_float(txt)
        if v is None:
            return None, None
        return float(v), float(v)
    parts = txt.split("-")
    if len(parts) != 2:
        return None, None
    a = safe_float(parts[0].strip())
    b = safe_float(parts[1].strip())
    if a is None or b is None:
        return None, None
    lo = min(a, b)
    hi = max(a, b)
    return float(lo), float(hi)

def doi_signal(
    ltp: float,
    top_call_doi_strikes: str,
    top_put_doi_strikes: str,
    window_steps: int = 2,
    min_conf: float = 30.0,
    confirm_conf: float = 40.0,
    unwind_boost: float = 0.5,
    min_activity: float = 0.0,
) -> Dict[str, Any]:
    call_doi = _parse_strike_doi(top_call_doi_strikes)
    put_doi  = _parse_strike_doi(top_put_doi_strikes)
    all_strikes = sorted(set(call_doi.keys()) | set(put_doi.keys()))
    step = _infer_step(all_strikes)
    atm = round(ltp / step) * step
    window = [atm + i * step for i in range(-window_steps, window_steps + 1)]
    P = sum(put_doi.get(k, 0) for k in window if k <= ltp)
    C = sum(call_doi.get(k, 0) for k in window if k >= ltp)
    call_unwind = sum(-call_doi.get(k, 0) for k in window if k >= ltp and call_doi.get(k, 0) < 0)
    put_unwind  = sum(-put_doi.get(k, 0)  for k in window if k <= ltp and put_doi.get(k, 0)  < 0)
    net = (P - C) + unwind_boost * call_unwind - unwind_boost * put_unwind
    activity = abs(P) + abs(C) + call_unwind + put_unwind + 1e-9
    confidence = 100.0 * (abs(net) / activity)
    if activity < max(0.0, safe_float(min_activity, 0.0)):
        return {"direction": "NEUTRAL", "confirmation": 0, "confidence": 0.0, "net": round(net, 1)}
    mags = [abs(call_doi.get(k, 0)) + abs(put_doi.get(k, 0)) for k in window]
    mags.sort()
    median_mag = mags[len(mags)//2] if mags else 0
    T = max(1.0, 0.25 * median_mag)
    if net > T and confidence >= min_conf:
        direction = "BULLISH"
    elif net < -T and confidence >= min_conf:
        direction = "BEARISH"
    else:
        direction = "NEUTRAL"
    # Confirmation: direction + confidence threshold + dominant side positive
    confirmation = 0
    if direction == "BULLISH" and confidence >= confirm_conf and P > 0:
        confirmation = 1
    elif direction == "BEARISH" and confidence >= confirm_conf and C > 0:
        confirmation = 1
    return {"direction": direction, "confirmation": confirmation, "confidence": round(confidence, 1), "net": round(net, 1)}

@dataclass
class Candle:
    start_et: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int = 0
    vwap_num: float = 0.0
    vwap_den: int = 0

    def vwap(self) -> Optional[float]:
        if self.vwap_den <= 0:
            return None
        return self.vwap_num / self.vwap_den

class CandleBook:
    def __init__(self, candle_minutes: int):
        self.mins = candle_minutes
        self.current: Dict[str, Optional[Candle]] = {}
        self.closed: Dict[str, List[Candle]] = {}
        self.last_cum_vol: Dict[str, Optional[int]] = {}

    def update(self, symbol: str, ltp: float, tick_ts: datetime, cum_vol: Optional[int]) -> Optional[Candle]:
        ts_et = to_et(tick_ts) if tick_ts.tzinfo else tick_ts.replace(tzinfo=ET)
        start = floor_time(ts_et, self.mins)
        
        cur = self.current.get(symbol)
        if cur is None:
            cur = Candle(start_et=start, open=ltp, high=ltp, low=ltp, close=ltp, volume=0)
            self.current[symbol] = cur

        if start > cur.start_et:
            closed_candle = cur
            self.closed.setdefault(symbol, []).append(closed_candle)
            cur = Candle(start_et=start, open=ltp, high=ltp, low=ltp, close=ltp, volume=0)
            self.current[symbol] = cur
            return closed_candle

        cur.high = max(cur.high, ltp)
        cur.low = min(cur.low, ltp)
        cur.close = ltp

        if cum_vol is not None:
            prev = self.last_cum_vol.get(symbol)
            if prev is None:
                self.last_cum_vol[symbol] = cum_vol
            else:
                delta = max(0, cum_vol - prev)
                self.last_cum_vol[symbol] = cum_vol
                cur.volume += delta
                cur.vwap_num += ltp * delta
                cur.vwap_den += delta

        return None

    def last_closed(self, symbol: str, n: int = 1) -> List[Candle]:
        return self.closed.get(symbol, [])[-n:]

def _norm(x: Any) -> str:
    if x is None:
        return ""
    try:
        s = str(x)
    except Exception:
        return ""
    s = s.upper().strip()
    s = re.sub(r"\s+", " ", s)
    return s

def _make_keyword_patterns(keywords: List[str]) -> List[re.Pattern]:
    pats = []
    for k in keywords:
        k = _norm(k)
        if not k:
            continue
        pats.append(re.compile(r"(?<!\w)" + re.escape(k) + r"(?!\w)", re.IGNORECASE))
    return pats

def build_news_keywords_for_symbol(symbol: str) -> List[str]:
    """Build search keywords for US stocks"""
    sym = _norm(symbol)
    keywords = [sym]
    
    # Common stock name mappings
    ALIASES = {
        "AAPL": ["APPLE", "APPLE INC"],
        "MSFT": ["MICROSOFT", "MICROSOFT CORPORATION"],
        "GOOGL": ["GOOGLE", "ALPHABET"],
        "TSLA": ["TESLA", "TESLA INC"],
        "AMZN": ["AMAZON", "AMAZON.COM"],
        "META": ["FACEBOOK", "META PLATFORMS"],
        "NVDA": ["NVIDIA", "NVIDIA CORPORATION"],
        "SPY": ["S&P 500", "SPX", "S&P500"],
        "QQQ": ["NASDAQ", "NASDAQ 100", "NDX"],
        "IWM": ["RUSSELL 2000", "RUSSELL"],
    }
    
    if sym in ALIASES:
        keywords.extend(ALIASES[sym])
    
    out, seen = [], set()
    for k in keywords:
        nk = _norm(k)
        if nk and nk not in seen:
            seen.add(nk)
            out.append(nk)
    return out

_NEWS_WINDOW_CACHE: Dict[str, Any] = {
    "path": "",
    "mtime": None,
    "lookback_minutes": None,
    "minute_bucket": "",
    "df": None,
}
_NEWS_PATTERN_CACHE: Dict[str, List[re.Pattern]] = {}

def _load_news_window_cached(news_csv_path: str, lookback_minutes: int) -> pd.DataFrame:
    if not os.path.exists(news_csv_path):
        return pd.DataFrame()

    try:
        mtime = os.path.getmtime(news_csv_path)
    except Exception:
        return pd.DataFrame()

    minute_bucket = now_et().strftime("%Y-%m-%d %H:%M")
    cached_df = _NEWS_WINDOW_CACHE.get("df")
    if (
        _NEWS_WINDOW_CACHE.get("path") == news_csv_path
        and _NEWS_WINDOW_CACHE.get("mtime") == mtime
        and _NEWS_WINDOW_CACHE.get("lookback_minutes") == lookback_minutes
        and _NEWS_WINDOW_CACHE.get("minute_bucket") == minute_bucket
        and isinstance(cached_df, pd.DataFrame)
    ):
        return cached_df

    try:
        df = pd.read_csv(news_csv_path, dtype=str, keep_default_na=False)
    except Exception:
        return pd.DataFrame()

    if df.empty or "ts_et" not in df.columns:
        _NEWS_WINDOW_CACHE.update(
            {
                "path": news_csv_path,
                "mtime": mtime,
                "lookback_minutes": lookback_minutes,
                "minute_bucket": minute_bucket,
                "df": pd.DataFrame(),
            }
        )
        return pd.DataFrame()

    for col in ["title", "text", "url", "source_name", "symbol_hint"]:
        if col not in df.columns:
            df[col] = ""

    ts = pd.to_datetime(df["ts_et"], format="%Y-%m-%d %H:%M:%S", errors="coerce")
    try:
        if ts.dt.tz is None:
            ts = ts.dt.tz_localize(ET)
        else:
            ts = ts.dt.tz_convert(ET)
    except Exception:
        ts = pd.Series(pd.NaT, index=df.index)
    cutoff = now_et() - timedelta(minutes=lookback_minutes)

    filtered = df[ts.notna()].copy()
    filtered["ts_parsed"] = ts[ts.notna()]
    filtered = filtered[filtered["ts_parsed"] >= cutoff].copy()

    if filtered.empty:
        _NEWS_WINDOW_CACHE.update(
            {
                "path": news_csv_path,
                "mtime": mtime,
                "lookback_minutes": lookback_minutes,
                "minute_bucket": minute_bucket,
                "df": filtered,
            }
        )
        return filtered

    filtered["symbol_hint_norm"] = filtered["symbol_hint"].map(_norm)
    filtered["blob_norm"] = filtered["title"].map(_norm) + " " + filtered["text"].map(_norm)
    filtered = filtered.sort_values("ts_parsed", ascending=False)

    _NEWS_WINDOW_CACHE.update(
        {
            "path": news_csv_path,
            "mtime": mtime,
            "lookback_minutes": lookback_minutes,
            "minute_bucket": minute_bucket,
            "df": filtered,
        }
    )
    return filtered

def load_news_from_feed_csv_for_symbol(news_csv_path: str, symbol: str, lookback_minutes: int = 240, max_items: int = 3) -> Tuple[List[str], List[str]]:
    df = _load_news_window_cached(news_csv_path, lookback_minutes)
    if df.empty:
        return [], []

    sym = _norm(symbol)
    patterns = _NEWS_PATTERN_CACHE.get(sym)
    if patterns is None:
        patterns = _make_keyword_patterns(build_news_keywords_for_symbol(symbol))
        _NEWS_PATTERN_CACHE[sym] = patterns

    hint_mask = df["symbol_hint_norm"] == sym
    blob_mask = pd.Series(False, index=df.index)
    for pat in patterns:
        blob_mask = blob_mask | df["blob_norm"].str.contains(pat, regex=True, na=False)

    matched = df[hint_mask | blob_mask].head(max_items)
    if matched.empty:
        return [], []

    bullets, sources = [], []
    for _, r in matched.iterrows():
        ts = r.get("ts_et", "")
        src = r.get("source_name", "")
        title = r.get("title", "") or ""
        text = r.get("text", "") or ""
        url = r.get("url", "") or ""
        headline = title.strip() if title.strip() else (text[:160].strip())
        bullets.append(csv_safe_text(f"{ts} ET [{src}] {headline}"))
        if url.strip():
            sources.append(csv_safe_text(url.strip()))
    
    return bullets, sources

def _migrate_csv_schema(path: str, required_cols: List[str], label: str):
    with FileLock(path):
        if not os.path.exists(path):
            pd.DataFrame(columns=required_cols).to_csv(path, index=False)
            logger.info(f"Created {label} CSV: {path}")
            return

        try:
            df = pd.read_csv(path, keep_default_na=False)
        except pd.errors.EmptyDataError as e:
            logger.warning(f"{label} CSV temporary empty/unreadable, skipping migrate: {e}")
            return
        except PermissionError as e:
            logger.warning(f"{label} CSV locked, skipping migrate: {e}")
            return
        except Exception:
            bak = path + ".bak"
            try:
                os.replace(path, bak)
                logger.warning(f"{label} CSV unreadable, moved to backup: {bak}")
            except Exception:
                return
            pd.DataFrame(columns=required_cols).to_csv(path, index=False)
            logger.info(f"Created {label} CSV: {path}")
            return

        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            for c in missing:
                df[c] = ""
            # Reorder: required columns first (in schema order), then any extras
            extra = [c for c in df.columns if c not in required_cols]
            df = df[required_cols + extra]
            df.to_csv(path, index=False)
            logger.info(f"Migrated {label} CSV schema, added columns: {missing}")

def ensure_live_csv(path: str):
    _migrate_csv_schema(path, CSV_HEADERS, "LIVE")

def ensure_actions_csv(path: str):
    _migrate_csv_schema(path, ACTIONS_HEADERS, "ACTIONS")

def ensure_summary_csv(path: str):
    _migrate_csv_schema(path, SUMMARY_HEADERS, "SUMMARY")

def _ensure_csv_exists_quick(path: str, headers: List[str]):
    """Fast path: only create file with headers if missing."""
    if os.path.exists(path):
        return
    with FileLock(path):
        if not os.path.exists(path):
            pd.DataFrame(columns=headers).to_csv(path, index=False)

def append_live_row(path: str, row: Dict[str, Any]):
    """Append a row to live CSV with proper value conversion"""
    _ensure_csv_exists_quick(path, CSV_HEADERS)
    converted_row = {}
    for h in CSV_HEADERS:
        val = row.get(h, "")
        converted_row[h] = safe_str(val)
    df = pd.DataFrame([converted_row])
    with FileLock(path):
        df.to_csv(path, mode="a", header=False, index=False)

def load_live(path: str) -> pd.DataFrame:
    _ensure_csv_exists_quick(path, CSV_HEADERS)
    with FileLock(path):
        try:
            return pd.read_csv(path, keep_default_na=False)
        except pd.errors.EmptyDataError:
            return pd.DataFrame(columns=CSV_HEADERS)

def update_live_row(path: str, row_id: str, updates: Dict[str, Any]):
    """Update live CSV row with proper value conversion"""
    _ensure_csv_exists_quick(path, CSV_HEADERS)
    with FileLock(path):
        df = pd.read_csv(path, keep_default_na=False)
        if df.empty or "row_id" not in df.columns:
            return
        m = df["row_id"].astype(str) == str(row_id)
        if not m.any():
            return
        for k, v in updates.items():
            if k in df.columns:
                df.loc[m, k] = safe_str(v)
        df.to_csv(path, index=False)

def bulk_update_live_rows(path: str, updates_by_row_id: Dict[str, Dict[str, Any]]):
    """Batch update multiple live rows and write CSV once."""
    if not updates_by_row_id:
        return
    _ensure_csv_exists_quick(path, CSV_HEADERS)
    with FileLock(path):
        df = pd.read_csv(path, keep_default_na=False)
        if df.empty or "row_id" not in df.columns:
            return
        row_ids = df["row_id"].astype(str)
        for row_id, updates in updates_by_row_id.items():
            m = row_ids == str(row_id)
            if not m.any():
                continue
            for k, v in updates.items():
                if k in df.columns:
                    df.loc[m, k] = safe_str(v)
        df.to_csv(path, index=False)

def load_actions_map(actions_path: str) -> Dict[str, Dict[str, Any]]:
    _ensure_csv_exists_quick(actions_path, ACTIONS_HEADERS)
    with FileLock(actions_path):
        df = pd.read_csv(actions_path, keep_default_na=False)
    if df.empty:
        return {}
    if "symbol" not in df.columns:
        df["symbol"] = ""
    df["symbol"] = df["symbol"].fillna("").astype(str).str.upper().str.strip()
    df = df[df["symbol"] != ""].copy()
    df["approve"] = df.get("approve", "NO").fillna("NO").astype(str).str.upper()
    df["lots"] = pd.to_numeric(df.get("lots", 0), errors="coerce").fillna(0).astype(int)
    df = df.sort_values(by=["symbol"]).drop_duplicates(subset=["symbol"], keep="first")
    out: Dict[str, Dict[str, Any]] = {}
    for _, r in df.iterrows():
        sym = safe_str(r.get("symbol", "")).upper().strip()
        if not sym:
            continue
        out[sym] = r.to_dict()
    return out

def update_action_row(actions_path: str, symbol: str, updates: Dict[str, Any]):
    _ensure_csv_exists_quick(actions_path, ACTIONS_HEADERS)
    sym = safe_str(symbol).upper().strip()
    if not sym:
        return
    with FileLock(actions_path):
        df = pd.read_csv(actions_path, keep_default_na=False)
        if df.empty or "symbol" not in df.columns:
            return
        m = df["symbol"].fillna("").astype(str).str.upper().str.strip() == sym
        if not m.any():
            return
        for k, v in updates.items():
            if k in df.columns:
                if k == "lots":
                    # Keep lots numeric to avoid dtype failures during Telegram approvals.
                    df[k] = pd.to_numeric(df[k], errors="coerce").fillna(0).astype(int)
                    df.loc[m, k] = max(0, safe_int(v, 0))
                else:
                    # Force text columns to text before assignment (handles legacy numeric dtypes).
                    df[k] = df[k].astype(str)
                    df.loc[m, k] = safe_str(v)
        if safe_str(updates.get("approve", "")).upper() == "NO" and "lots" in df.columns:
            df["lots"] = pd.to_numeric(df["lots"], errors="coerce").fillna(0).astype(int)
            df.loc[m, "lots"] = 0
        df.to_csv(actions_path, index=False)

def sync_actions_from_summary(actions_path: str, summary_path: str):
    """
    Keep actions CSV in sync with latest summary rows (1 row per summary symbol).
    Preserve user-controlled and execution fields by symbol.
    """
    _ensure_csv_exists_quick(actions_path, ACTIONS_HEADERS)

    existing_map: Dict[str, Dict[str, Any]] = {}
    try:
        with FileLock(actions_path):
            existing = pd.read_csv(actions_path, keep_default_na=False)
        if not existing.empty:
            if "symbol" not in existing.columns:
                existing["symbol"] = ""
            existing["symbol"] = existing["symbol"].fillna("").astype(str).str.upper().str.strip()
            for _, row in existing.iterrows():
                sym = safe_str(row.get("symbol", "")).upper().strip()
                if sym and sym not in existing_map:
                    existing_map[sym] = row.to_dict()
    except Exception:
        existing_map = {}

    if (not os.path.exists(summary_path)) or os.path.getsize(summary_path) <= 0:
        logger.debug(f"Skipping actions sync; summary file unavailable or empty: {summary_path}")
        return

    try:
        summary_df = pd.read_csv(summary_path, keep_default_na=False)
    except Exception as e:
        logger.warning(f"Skipping actions sync; unable to read summary CSV {summary_path}: {e}")
        return

    if summary_df is None or summary_df.empty:
        logger.debug(f"Skipping actions sync; summary has no rows: {summary_path}")
        return

    work = summary_df.copy()
    for c in ["symbol", "timestamp_et"]:
        if c not in work.columns:
            work[c] = ""
    work["symbol"] = work["symbol"].fillna("").astype(str).str.upper().str.strip()
    work = work[work["symbol"] != ""].copy()

    if work.empty:
        logger.debug(f"Skipping actions sync; summary has no valid symbols: {summary_path}")
        return

    if "rank" in work.columns:
        work["rank_num"] = pd.to_numeric(work["rank"], errors="coerce").fillna(10**9)
        work = work.sort_values(by=["rank_num"], ascending=[True])
    else:
        work["timestamp_et_dt"] = pd.to_datetime(work["timestamp_et"], errors="coerce")
        work = work.sort_values(by=["timestamp_et_dt", "timestamp_et"], ascending=[False, False])
    latest = work.drop_duplicates(subset=["symbol"], keep="first")

    def _fallback_tradingsymbol(r: pd.Series) -> str:
        ts = safe_str(r.get("tradingsymbol", "")).strip()
        if ts:
            return ts
        sym = safe_str(r.get("symbol", "")).upper().strip()
        expiry = safe_str(r.get("expiry", "")).strip()
        strike = safe_str(r.get("strike", "")).strip()
        right = safe_str(r.get("right", "")).upper().strip()
        if sym and expiry and strike and right:
            return f"{sym} {expiry} {strike} {right}"
        return sym

    rows: List[Dict[str, Any]] = []
    for _, r in latest.iterrows():
        sym = safe_str(r.get("symbol", "")).upper().strip()
        prev = existing_map.get(sym, {})
        approve = safe_str(prev.get("approve", "NO")).upper()
        if approve not in ("YES", "NO"):
            approve = "NO"
        lots = max(0, safe_int(prev.get("lots", 0), 0))
        if approve != "YES":
            lots = 0
        new_ts = _fallback_tradingsymbol(r)
        prev_ts = safe_str(prev.get("tradingsymbol", "")).strip()
        keep_previous_plan = bool(prev_ts) and (
            (not new_ts) or (new_ts == prev_ts) or (new_ts.upper() == sym)
        )
        latest_conf = safe_str(r.get("confidence", ""))
        latest_conf_score = safe_str(r.get("confidence_score", ""))
        latest_oi_stale_block = safe_str(r.get("factor_oi_stale_decisive_block", "0"))
        prev_request_id = safe_str(prev.get("request_id", ""))
        out_status = safe_str(r.get("status", "")) or safe_str(prev.get("status", ""))
        out_approval_source = safe_str(prev.get("approval_source", ""))
        out_approval_updated_at = safe_str(prev.get("approval_updated_at", ""))
        out_telegram_alert_sent_at = safe_str(prev.get("telegram_alert_sent_at", ""))
        out_exec_status = safe_str(prev.get("execution_status", ""))
        out_exec_result = safe_str(prev.get("execution_result", ""))
        out_executed_at = safe_str(prev.get("executed_at", ""))
        out_executed_row_id = safe_str(prev.get("executed_row_id", ""))
        out_executed_order_id = safe_str(prev.get("executed_order_id", ""))
        # Clear stale simulation artifacts from older dry-run versions.
        if '"dry_run": true' in out_exec_result.lower():
            out_exec_status = ""
            out_exec_result = ""
            out_executed_at = ""
            out_executed_row_id = ""
            out_executed_order_id = ""
        if keep_previous_plan:
            out_ts = prev_ts
            # Confidence should always reflect latest summary scoring.
            out_conf = latest_conf
            out_entry = safe_str(prev.get("primary_entry", ""))
            out_sl = safe_str(prev.get("primary_sl", ""))
            out_t1 = safe_str(prev.get("primary_t1", ""))
            out_t2 = safe_str(prev.get("primary_t2", ""))
        else:
            out_ts = new_ts
            out_conf = latest_conf
            out_entry = safe_str(r.get("primary_entry", ""))
            out_sl = safe_str(r.get("primary_sl", ""))
            out_t1 = safe_str(r.get("primary_t1", ""))
            out_t2 = safe_str(r.get("primary_t2", ""))
            # New contract picked: previous execution metadata is no longer relevant.
            out_exec_status = ""
            out_exec_result = ""
            out_executed_at = ""
            out_executed_row_id = ""
            out_executed_order_id = ""
        out_request_id = build_request_id(sym, out_ts)
        if out_request_id == prev_request_id:
            prev_status = safe_str(prev.get("status", "")).upper().strip()
            if prev_status in {"CLOSED", "ERROR"}:
                out_status = prev_status
        if out_request_id != prev_request_id:
            approve = "NO"
            lots = 0
            out_approval_source = ""
            out_approval_updated_at = ""
            out_telegram_alert_sent_at = ""
            out_exec_status = ""
            out_exec_result = ""
            out_executed_at = ""
            out_executed_row_id = ""
            out_executed_order_id = ""

        rows.append({
            "symbol": sym,
            "status": out_status,
            "confidence": out_conf,
            "confidence_score": latest_conf_score,
            "factor_oi_stale_decisive_block": latest_oi_stale_block,
            "tradingsymbol": out_ts,
            "request_id": out_request_id,
            "primary_entry": out_entry,
            "primary_sl": out_sl,
            "primary_t1": out_t1,
            "primary_t2": out_t2,
            "approve": approve,
            "lots": lots,
            "approval_source": out_approval_source,
            "approval_updated_at": out_approval_updated_at,
            "telegram_alert_sent_at": out_telegram_alert_sent_at,
            "execution_status": out_exec_status,
            "execution_result": out_exec_result,
            "executed_at": out_executed_at,
            "executed_row_id": out_executed_row_id,
            "executed_order_id": out_executed_order_id,
        })

    out_df = pd.DataFrame(rows, columns=ACTIONS_HEADERS).fillna("")
    with FileLock(actions_path):
        out_df.to_csv(actions_path, index=False)

def _prune_csv_rows_to_today(path: str, headers: List[str], ts_col: str, today: date) -> Tuple[int, int]:
    _ensure_csv_exists_quick(path, headers)
    with FileLock(path):
        try:
            df = pd.read_csv(path, keep_default_na=False)
        except pd.errors.EmptyDataError:
            pd.DataFrame(columns=headers).to_csv(path, index=False)
            return 0, 0
        except Exception as e:
            logger.warning(f"Daily cleanup: failed reading {path}: {e}")
            return 0, 0

        if df.empty:
            return 0, 0
        if ts_col not in df.columns:
            # No timestamp column means we cannot perform day-based pruning safely.
            return 0, len(df)

        ts = pd.to_datetime(df[ts_col], errors="coerce")
        keep_mask = ts.dt.date == today
        kept = df[keep_mask].copy()
        removed = int(len(df) - len(kept))
        if removed > 0:
            kept.to_csv(path, index=False)
        return removed, int(len(kept))

def _reset_json_state_if_prior_day(path: str, today: date) -> bool:
    state_path = safe_str(path)
    if (not state_path) or (not os.path.exists(state_path)) or os.path.getsize(state_path) <= 0:
        return False
    try:
        mtime_date = datetime.fromtimestamp(os.path.getmtime(state_path), tz=ET).date()
        if mtime_date >= today:
            return False
        with FileLock(state_path):
            with open(state_path, "w", encoding="utf-8") as f:
                f.write("{}")
        return True
    except Exception as e:
        logger.warning(f"Daily cleanup: failed resetting state file {state_path}: {e}")
        return False

def _is_file_prior_day(path: str, today: date) -> bool:
    p = safe_str(path)
    if (not p) or (not os.path.exists(p)):
        return False
    try:
        mtime_date = datetime.fromtimestamp(os.path.getmtime(p), tz=ET).date()
        return mtime_date < today
    except Exception:
        return False

def _trim_log_to_today(log_path: str, today_prefix: str) -> int:
    p = safe_str(log_path)
    if not p:
        return 0
    try:
        with open(p, "r", encoding="utf-8", errors="ignore") as f:
            lines = f.readlines()
    except FileNotFoundError:
        return 0
    except Exception as e:
        logger.warning(f"Daily cleanup: failed reading log file {p}: {e}")
        return 0

    keep = [ln for ln in lines if ln.startswith(today_prefix)]
    removed = int(len(lines) - len(keep))
    if removed <= 0:
        return 0

    # Rewrite through the active file handler stream so Windows file-handle semantics remain safe.
    try:
        active_log_path = safe_str(getattr(_file_handler, "baseFilename", ""))
        is_active_log = bool(active_log_path) and (
            os.path.abspath(active_log_path) == os.path.abspath(p)
        )
    except Exception:
        is_active_log = False
    try:
        if is_active_log:
            _file_handler.acquire()
            try:
                _file_handler.flush()
                stream = _file_handler.stream
                stream.seek(0)
                stream.truncate(0)
                stream.writelines(keep)
                stream.flush()
            finally:
                _file_handler.release()
        else:
            with open(p, "w", encoding="utf-8") as f:
                f.writelines(keep)
    except Exception as e:
        logger.warning(f"Daily cleanup: failed trimming log file {p}: {e}")
        return 0
    return removed

def _startup_daily_cleanup(cfg: Dict[str, Any]):
    today = now_et().date()
    live_removed, live_left = _prune_csv_rows_to_today(
        cfg["CSV_LIVE_PATH"], CSV_HEADERS, "timestamp_et", today
    )
    summary_removed, summary_left = _prune_csv_rows_to_today(
        cfg["CSV_SUMMARY_PATH"], SUMMARY_HEADERS, "timestamp_et", today
    )
    oi_state_reset = _reset_json_state_if_prior_day(cfg.get("OI_SNAPSHOT_PATH", ""), today)
    signal_state_reset = _reset_json_state_if_prior_day(cfg.get("SIGNAL_STATE_PATH", ""), today)
    telegram_state_reset = _reset_json_state_if_prior_day(cfg.get("TELEGRAM_STATE_PATH", ""), today)
    actions_prior_day = _is_file_prior_day(cfg.get("CSV_ACTIONS_PATH", ""), today)

    actions_reset = 0
    if (live_removed > 0) or (summary_removed > 0) or actions_prior_day:
        _ensure_csv_exists_quick(cfg["CSV_ACTIONS_PATH"], ACTIONS_HEADERS)
        with FileLock(cfg["CSV_ACTIONS_PATH"]):
            try:
                old_actions = pd.read_csv(cfg["CSV_ACTIONS_PATH"], keep_default_na=False)
                actions_reset = int(len(old_actions))
            except Exception:
                actions_reset = 0
            pd.DataFrame(columns=ACTIONS_HEADERS).to_csv(cfg["CSV_ACTIONS_PATH"], index=False)

        if summary_left > 0:
            try:
                sync_actions_from_summary(cfg["CSV_ACTIONS_PATH"], cfg["CSV_SUMMARY_PATH"])
            except Exception as e:
                logger.warning(f"Daily cleanup: failed rebuilding actions from summary: {e}")

    log_removed = _trim_log_to_today(
        safe_str(cfg.get("LOG_FILE_PATH", "")),
        today.strftime("%Y-%m-%d"),
    )

    if any(x > 0 for x in [live_removed, summary_removed, actions_reset, log_removed, int(oi_state_reset), int(signal_state_reset), int(telegram_state_reset)]):
        logger.info(
            "Daily cleanup complete: live_removed=%d, summary_removed=%d, actions_reset=%d, "
            "oi_state_reset=%d, signal_state_reset=%d, telegram_state_reset=%d, log_lines_removed=%d",
            live_removed, summary_removed, actions_reset, int(oi_state_reset), int(signal_state_reset), int(telegram_state_reset), log_removed,
        )

def write_summary_csv(
    live_path: str,
    summary_path: str,
    top_n: int = 0,
    min_confidence: int = 3,
    include_wait: bool = False,
):
    """
    SUMMARY SELECTION LOGIC
    =======================
    1) Keep exactly one latest record per symbol.
    2) Add new symbols to summary only when confidence_score >= min_confidence.
    3) Summary size is driven by qualifying symbols (no forced top-N backfill).
    4) Keep tracking any symbol that has ever hit the configured threshold in live
       history, and keep updating it with latest info even when confidence drops.
    """
    _ensure_csv_exists_quick(summary_path, SUMMARY_HEADERS)
    live = load_live(live_path)
    if live.empty:
        return

    for c in ["confidence", "breakout_status", "breakdown_status", "volume_confirmation", "bias", "status", "symbol"]:
        if c in live.columns:
            live[c] = live[c].fillna("").astype(str).str.upper().str.strip()
        else:
            live[c] = ""

    for c in ["timestamp_et", "ltp", "pct_change", "confidence_score"]:
        if c not in live.columns:
            live[c] = "" if c != "confidence_score" else 0

    live["timestamp_et_dt"] = pd.to_datetime(live["timestamp_et"], errors="coerce")
    live["confidence_score"] = pd.to_numeric(live.get("confidence_score", 0), errors="coerce").fillna(0).astype(int)
    live = live.sort_values(by=["timestamp_et_dt", "timestamp_et"], ascending=[False, False])
    latest = live.drop_duplicates(subset=["symbol"], keep="first").copy()

    prev_symbols = set()
    prev_summary_by_symbol: Dict[str, Dict[str, Any]] = {}
    try:
        if os.path.exists(summary_path) and os.path.getsize(summary_path) > 0:
            prev = pd.read_csv(summary_path, dtype=str, keep_default_na=False)
            if "symbol" in prev.columns:
                prev["symbol"] = prev["symbol"].fillna("").astype(str).str.upper().str.strip()
                prev_symbols = set(prev["symbol"][prev["symbol"] != ""])
                prev = prev[prev["symbol"] != ""].copy()
                if not prev.empty:
                    prev_summary_by_symbol = {
                        safe_str(r.get("symbol", "")).upper().strip(): r.to_dict()
                        for _, r in prev.iterrows()
                    }
    except Exception:
        prev_symbols = set()
        prev_summary_by_symbol = {}

    latest["symbol"] = latest.get("symbol", "").fillna("").astype(str).str.upper().str.strip()
    latest = latest[latest["symbol"] != ""].copy()

    min_confidence = max(0, min(4, int(min_confidence)))
    _ = include_wait  # retained for compatibility
    eligible_new = latest[latest["confidence_score"] >= min_confidence].copy()
    if top_n and top_n > 0:
        eligible_new = eligible_new.sort_values(
            by=["confidence_score", "timestamp_et_dt", "timestamp_et"], ascending=[False, False, False]
        ).head(top_n)
    selected_symbols = set(eligible_new["symbol"])

    ever_high_symbols = set(
        live[live["confidence_score"] >= min_confidence]["symbol"]
        .dropna()
        .astype(str)
        .str.upper()
        .str.strip()
    )
    tracked_ever = ever_high_symbols
    latest_status = latest.get("status", "").fillna("").astype(str).str.upper().str.strip()
    latest_exec_result = latest.get("execution_result", "").fillna("").astype(str)
    carry_mask = (
        latest_status.isin(["OPEN_CARRY", "CARRY_FORWARD"])
        | latest_exec_result.str.contains("Carry-forward", case=False, na=False, regex=False)
    )
    carry_symbols = set(latest.loc[carry_mask, "symbol"].dropna().astype(str).str.upper().str.strip())
    tracked_symbols = selected_symbols | tracked_ever | carry_symbols
    top = latest[latest["symbol"].isin(tracked_symbols)].copy()

    if top.empty:
        with FileLock(summary_path):
            pd.DataFrame(columns=SUMMARY_HEADERS).to_csv(summary_path, index=False)
        carried_only = tracked_ever - selected_symbols
        logger.info(
            "Summary snapshot: rows=0 (new >=%d symbols=%d, carried-by-history=%d, carry-tracked=%d)",
            min_confidence,
            len(selected_symbols),
            len(carried_only),
            len(carry_symbols),
        )
        return

    if "status" not in top.columns:
        top["status"] = ""
    top["_live_status"] = top["status"].fillna("").astype(str).str.upper().str.strip()
    top["status"] = top.apply(
        lambda r: (
            "OPEN_CARRY"
            if (
                safe_str(r.get("symbol", "")).upper().strip() in carry_symbols
                or safe_str(r.get("_live_status", "")).upper().strip() in {"OPEN_CARRY", "CARRY_FORWARD"}
            )
            else (
                "NEW"
                if (
                    safe_str(r.get("symbol", "")).upper().strip() in selected_symbols
                    and safe_str(r.get("symbol", "")).upper().strip() not in prev_symbols
                )
                else "OLD"
            )
        ),
        axis=1,
    )
    top.drop(columns=["_live_status"], inplace=True, errors="ignore")

    for fc in FACTOR_COLUMNS:
        if fc in top.columns:
            top[fc] = pd.to_numeric(top[fc], errors="coerce").fillna(0).astype(int)
        else:
            top[fc] = 0

    top["confidence_score"] = pd.to_numeric(top.get("confidence_score", 0), errors="coerce").fillna(0).astype(int)
    top["confidence"] = top["confidence_score"].apply(confidence_grade)

    # Calculate freshness from first time a symbol was added to summary.
    # We reconstruct first-added timestamp from previous summary:
    # first_added = previous_summary_run_ts - previous_minutes_since_signal.
    now = now_et()
    summary_origin_by_symbol: Dict[str, datetime] = {}

    def _parse_summary_ts(ts_text: str) -> Optional[datetime]:
        try:
            return datetime.strptime(str(ts_text), "%Y-%m-%d %H:%M:%S").replace(tzinfo=ET)
        except Exception:
            return None

    def _parse_minutes(v: Any) -> int:
        try:
            return int(float(v))
        except Exception:
            return 0

    try:
        if os.path.exists(summary_path) and os.path.getsize(summary_path) > 0:
            prev = pd.read_csv(summary_path, dtype=str, keep_default_na=False)
            if not prev.empty and "symbol" in prev.columns:
                for _, prow in prev.iterrows():
                    sym = safe_str(prow.get("symbol", "")).upper().strip()
                    if not sym:
                        continue
                    prev_run_ts = _parse_summary_ts(prow.get("summary_run_ts_et", ""))
                    prev_mins = _parse_minutes(prow.get("minutes_since_signal", 0))
                    if prev_run_ts is not None and prev_mins >= 0:
                        summary_origin_by_symbol[sym] = prev_run_ts - timedelta(minutes=prev_mins)
    except Exception:
        summary_origin_by_symbol = {}

    def calc_minutes(row):
        sym = safe_str(row.get("symbol", "")).upper().strip()
        if not sym:
            return 0
        first_added_ts = summary_origin_by_symbol.get(sym)
        if first_added_ts is None:
            row_ts = pd.to_datetime(row.get("timestamp_et", ""), errors="coerce")
            if pd.notna(row_ts):
                first_added_ts = row_ts.to_pydatetime().replace(tzinfo=ET)
            else:
                first_added_ts = now
        return max(0, int((now - first_added_ts).total_seconds() / 60))

    top["minutes_since_signal"] = top.apply(calc_minutes, axis=1)

    bias_counts = live.groupby(["symbol", "bias"]).size().reset_index(name="_cnt")
    top = top.merge(bias_counts, on=["symbol", "bias"], how="left")
    top["count"] = top["_cnt"].fillna(0).astype(int)
    top.drop(columns=["_cnt"], inplace=True)

    top = top.sort_values(by=["confidence_score", "timestamp_et_dt", "timestamp_et"], ascending=[False, False, False])
    if top_n and top_n > 0:
        top = top.head(top_n)
    top.insert(0, "rank", range(1, len(top) + 1))

    # Ensure tradingsymbol is never blank in summary/actions views.
    if "tradingsymbol" not in top.columns:
        top["tradingsymbol"] = ""
    for col in ["expiry", "strike", "right", "tradingsymbol", "primary_entry", "primary_sl", "primary_t1", "primary_t2"]:
        if col not in top.columns:
            top[col] = ""
        top[col] = top[col].fillna("").astype(str)

    def _summary_tradingsymbol(row):
        ts = safe_str(row.get("tradingsymbol", "")).strip()
        if ts:
            return ts
        sym = safe_str(row.get("symbol", "")).upper().strip()
        expiry = safe_str(row.get("expiry", "")).strip()
        strike = safe_str(row.get("strike", "")).strip()
        right = safe_str(row.get("right", "")).upper().strip()
        if sym and expiry and strike and right:
            return f"{sym} {expiry} {strike} {right}"
        return sym

    top["tradingsymbol"] = top.apply(_summary_tradingsymbol, axis=1)

    # Freeze trade card fields until a genuinely new tradingsymbol is picked.
    for idx, row in top.iterrows():
        sym = safe_str(row.get("symbol", "")).upper().strip()
        if not sym:
            continue
        prev_row = prev_summary_by_symbol.get(sym)
        if not prev_row:
            continue
        prev_ts = safe_str(prev_row.get("tradingsymbol", "")).strip()
        cur_ts = safe_str(row.get("tradingsymbol", "")).strip()
        keep_previous_plan = bool(prev_ts) and (
            (not cur_ts) or (cur_ts == prev_ts) or (cur_ts.upper() == sym)
        )
        if keep_previous_plan:
            top.at[idx, "tradingsymbol"] = prev_ts
            for col in ["expiry", "strike", "right"]:
                top.at[idx, col] = safe_str(prev_row.get(col, ""))
            for col in ["primary_entry", "primary_sl", "primary_t1", "primary_t2"]:
                top.at[idx, col] = safe_str(prev_row.get(col, ""))

    def _sync_contract_fields_from_ts(row):
        ts = safe_str(row.get("tradingsymbol", "")).strip()
        parts = ts.split()
        if len(parts) >= 4:
            expiry = safe_str(parts[-3]).strip()
            strike = safe_str(parts[-2]).strip()
            right = safe_str(parts[-1]).upper().strip()
            if expiry.isdigit() and len(expiry) == 8 and right in {"C", "P"}:
                row["expiry"] = expiry
                row["strike"] = strike
                row["right"] = right
        return row

    top = top.apply(_sync_contract_fields_from_ts, axis=1)

    run_ts = now.strftime("%Y-%m-%d %H:%M:%S")
    top.insert(0, "summary_run_ts_et", run_ts)
    top.drop(columns=["timestamp_et_dt"], inplace=True, errors="ignore")

    new_rows = pd.DataFrame(columns=SUMMARY_HEADERS)
    for col in SUMMARY_HEADERS:
        if col in top.columns:
            new_rows[col] = top[col].apply(safe_str)
        else:
            new_rows[col] = ""
    new_rows = new_rows.fillna("")

    if "row_id" in new_rows.columns:
        new_rows = new_rows.drop_duplicates(subset=["row_id"], keep="first")
    with FileLock(summary_path):
        new_rows.to_csv(summary_path, index=False)

    low_conf_kept = int((pd.to_numeric(top["confidence_score"], errors="coerce").fillna(0) < min_confidence).sum())
    carried_only = tracked_ever - selected_symbols
    logger.info(
        "Summary snapshot: rows=%d (new >=%d symbols=%d, carried-by-history=%d, carry-tracked=%d, low-confidence-kept=%d)",
        len(new_rows),
        min_confidence,
        len(selected_symbols),
        len(carried_only),
        len(carry_symbols),
        low_conf_kept,
    )

def compute_structure(candles: List[Candle]) -> str:
    if len(candles) < 6:
        return "NA"
    highs = [c.high for c in candles[-6:]]
    lows = [c.low for c in candles[-6:]]
    if highs[-1] > highs[-3] and lows[-1] > lows[-3]:
        return "HH/HL"
    if highs[-1] < highs[-3] and lows[-1] < lows[-3]:
        return "LH/LL"
    return "Range"

def compute_atr(candles: List[Candle], period: int = 14) -> Optional[float]:
    if len(candles) < 2:
        return None
    lookback = max(2, int(period)) + 1
    window = candles[-lookback:]
    prev_close = safe_float(window[0].close)
    tr_values: List[float] = []
    for c in window[1:]:
        high = safe_float(c.high)
        low = safe_float(c.low)
        close = safe_float(c.close)
        if high is None or low is None:
            prev_close = close if close is not None else prev_close
            continue
        if prev_close is None:
            tr = max(0.0, high - low)
        else:
            tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        tr_values.append(max(0.0, tr))
        prev_close = close if close is not None else prev_close
    if not tr_values:
        return None
    recent = tr_values[-max(1, int(period)):]
    return float(sum(recent) / len(recent))

def swing_levels(candles: List[Candle], lookback: int = 20) -> Tuple[Optional[float], Optional[float]]:
    if len(candles) < 3:
        return None, None
    window = candles[-lookback:] if len(candles) >= lookback else candles
    return max(c.high for c in window), min(c.low for c in window)

def opening_range_levels(candles: List[Candle], opening_range_minutes: int) -> Tuple[Optional[float], Optional[float]]:
    if not candles:
        return None, None
    today = now_et().date()
    todays = [c for c in candles if c.start_et.date() == today]
    if not todays:
        return None, None
    
    if opening_range_minutes == 15:
        first = min(todays, key=lambda c: c.start_et)
        return first.high, first.low
    
    if opening_range_minutes == 30:
        first2 = sorted(todays, key=lambda c: c.start_et)[:2]
        if len(first2) < 2:
            return None, None
        return max(c.high for c in first2), min(c.low for c in first2)
    
    return None, None

def _is_finite_positive(value: Any) -> bool:
    fv = safe_float(value, None)
    return fv is not None and math.isfinite(fv) and fv > 0

def _count_level_touches(
    candles: List[Candle],
    level: float,
    side: str,
    tolerance: float,
    lookback: int = 48,
) -> Tuple[int, int]:
    if not candles:
        return 0, 999
    start = max(0, len(candles) - max(1, int(lookback)))
    touches: List[int] = []
    tol = max(0.0, safe_float(tolerance, 0.0) or 0.0)
    for idx in range(start, len(candles)):
        c = candles[idx]
        hi = safe_float(getattr(c, "high", None), None)
        lo = safe_float(getattr(c, "low", None), None)
        cl = safe_float(getattr(c, "close", None), None)
        if hi is None or lo is None:
            continue
        touched = (lo - tol) <= level <= (hi + tol)
        if (not touched) and cl is not None:
            touched = abs(cl - level) <= tol
        if not touched:
            if side == "res":
                touched = abs(hi - level) <= tol
            else:
                touched = abs(lo - level) <= tol
        if touched:
            touches.append(idx)
    if not touches:
        return 0, 999
    bars_since_touch = max(0, (len(candles) - 1) - touches[-1])
    return min(3, len(touches)), bars_since_touch

def select_key_levels_contextual(
    candles_15: List[Candle],
    eval_px: float,
    atr_15: Optional[float],
    swing_high: Optional[float],
    swing_low: Optional[float],
    prior_day_high: Optional[float],
    prior_day_low: Optional[float],
    opening_range_high: Optional[float],
    opening_range_low: Optional[float],
    oi_support_zone: str = "",
    oi_resistance_zone: str = "",
) -> Dict[str, Any]:
    eval_ref = max(0.0, safe_float(eval_px, 0.0) or 0.0)
    atr_val = safe_float(atr_15, 0.0)
    atr = atr_val if (atr_val is not None and math.isfinite(atr_val) and atr_val > 0) else 0.0

    # Step 3: cluster nearby levels using volatility-aware tolerance.
    cluster_tol = max(0.25 * atr, eval_ref * 0.001, 0.2)
    touch_tol = max(cluster_tol * 0.5, eval_ref * 0.0008, 0.1)
    dist_unit = max(atr, eval_ref * 0.002, 0.5)

    source_weights = {
        "prior_day": 1.0,
        "opening_range": 0.9,
        "swing": 0.75,
        "oi_edge": 0.7,
    }

    raw_support: List[Dict[str, Any]] = []
    raw_resistance: List[Dict[str, Any]] = []

    def _add_candidate(bucket: List[Dict[str, Any]], level: Optional[float], source: str):
        if not _is_finite_positive(level):
            return
        lv = float(level)
        bucket.append({"level": lv, "source": source, "source_weight": float(source_weights.get(source, 0.7))})

    # Step 1: raw candidates from structure + optional OI zone edges.
    _add_candidate(raw_resistance, swing_high, "swing")
    _add_candidate(raw_resistance, prior_day_high, "prior_day")
    _add_candidate(raw_resistance, opening_range_high, "opening_range")
    _add_candidate(raw_support, swing_low, "swing")
    _add_candidate(raw_support, prior_day_low, "prior_day")
    _add_candidate(raw_support, opening_range_low, "opening_range")

    oi_sup_lo, oi_sup_hi = parse_zone_bounds(oi_support_zone)
    oi_res_lo, oi_res_hi = parse_zone_bounds(oi_resistance_zone)
    for lv in [oi_sup_lo, oi_sup_hi]:
        _add_candidate(raw_support, lv, "oi_edge")
    for lv in [oi_res_lo, oi_res_hi]:
        _add_candidate(raw_resistance, lv, "oi_edge")

    def _score_candidates(side: str, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for cand in items:
            level = cand["level"]
            touch_count, bars_since_touch = _count_level_touches(
                candles_15, level, side=side, tolerance=touch_tol
            )
            # Step 2: source weight + touch/recent interaction scoring.
            touch_score = 0.6 * (touch_count / 3.0)
            recency_score = 0.0
            if bars_since_touch < 999:
                recency_score = 0.4 * math.exp(-max(0, bars_since_touch) / 12.0)
            score = cand["source_weight"] + touch_score + recency_score
            out.append({
                "level": level,
                "source": cand["source"],
                "candidate_score": score,
                "touch_count": touch_count,
                "bars_since_touch": bars_since_touch,
            })
        return out

    scored_support = _score_candidates("sup", raw_support)
    scored_resistance = _score_candidates("res", raw_resistance)

    def _cluster(scored: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if not scored:
            return []
        ordered = sorted(scored, key=lambda x: x["level"])
        groups: List[List[Dict[str, Any]]] = []
        current: List[Dict[str, Any]] = [ordered[0]]
        center = ordered[0]["level"]
        for cand in ordered[1:]:
            if abs(cand["level"] - center) <= cluster_tol:
                current.append(cand)
                wsum = sum(max(0.05, x["candidate_score"]) for x in current)
                center = sum(x["level"] * max(0.05, x["candidate_score"]) for x in current) / max(0.05, wsum)
            else:
                groups.append(current)
                current = [cand]
                center = cand["level"]
        groups.append(current)

        out: List[Dict[str, Any]] = []
        for grp in groups:
            wsum = sum(max(0.05, x["candidate_score"]) for x in grp)
            cluster_level = sum(x["level"] * max(0.05, x["candidate_score"]) for x in grp) / max(0.05, wsum)
            distinct_sources = len({x["source"] for x in grp})
            cluster_score = sum(x["candidate_score"] for x in grp) + 0.25 * max(0, distinct_sources - 1)
            # Step 4: distance penalty in ATR units.
            dist_atr = abs(cluster_level - eval_ref) / max(0.0001, dist_unit)
            adjusted = cluster_score - 0.35 * dist_atr
            out.append({
                "level": cluster_level,
                "cluster_score": cluster_score,
                "adjusted_score": adjusted,
                "dist_atr": dist_atr,
                "source_count": distinct_sources,
                "member_count": len(grp),
            })
        return out

    support_clusters = _cluster(scored_support)
    resistance_clusters = _cluster(scored_resistance)

    def _best_cluster(clusters: List[Dict[str, Any]], prefer_below: bool) -> Optional[Dict[str, Any]]:
        if not clusters:
            return None
        if prefer_below:
            primary = [c for c in clusters if c["level"] <= eval_ref]
        else:
            primary = [c for c in clusters if c["level"] >= eval_ref]

        def _pick(pool: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
            if not pool:
                return None
            return max(pool, key=lambda c: (c["adjusted_score"], -abs(c["level"] - eval_ref)))

        if primary:
            # Step 4 soft reject: avoid very far clusters when alternatives exist.
            near = [c for c in primary if c["dist_atr"] <= 3.0]
            chosen = _pick(near if near else primary)
            if chosen is not None:
                return chosen

        # Step 5 fallback: nearest cluster if preferred side absent.
        return min(clusters, key=lambda c: (abs(c["level"] - eval_ref), -c["adjusted_score"]))

    support_pick = _best_cluster(support_clusters, prefer_below=True)
    resistance_pick = _best_cluster(resistance_clusters, prefer_below=False)

    key_sup = support_pick["level"] if support_pick is not None else None
    key_res = resistance_pick["level"] if resistance_pick is not None else None

    # Step 6 guardrails and fallback framing.
    if key_sup is None and eval_ref > 0:
        key_sup = eval_ref - dist_unit
    if key_res is None and eval_ref > 0:
        key_res = eval_ref + dist_unit

    if key_sup is not None and key_res is not None and key_sup >= key_res:
        below = [c for c in support_clusters if c["level"] < eval_ref]
        above = [c for c in resistance_clusters if c["level"] > eval_ref]
        if below and above:
            key_sup = max(below, key=lambda c: c["level"])["level"]
            key_res = min(above, key=lambda c: c["level"])["level"]
        if key_sup >= key_res:
            gap = max(cluster_tol, dist_unit * 0.5)
            mid = eval_ref if eval_ref > 0 else ((key_sup + key_res) / 2.0)
            key_sup = min(key_sup, mid - gap)
            key_res = max(key_res, mid + gap)

    return {
        "key_support": float(key_sup) if key_sup is not None else None,
        "key_resistance": float(key_res) if key_res is not None else None,
        "method": "contextual_scored_v1",
        "cluster_tolerance": float(cluster_tol),
        "support_score": float(support_pick["adjusted_score"]) if support_pick is not None else 0.0,
        "resistance_score": float(resistance_pick["adjusted_score"]) if resistance_pick is not None else 0.0,
        "support_candidates": len(raw_support),
        "resistance_candidates": len(raw_resistance),
    }

def breakout_breakdown_read(
    ltp: float,
    resistance: Optional[float],
    support: Optional[float],
    vol_last_candle: int,
    typical_vol_last_candle: Optional[float],
    vconfirm_mult: float,
    break_buffer: float = 0.0,
) -> Dict[str, str]:
    out = {"breakout_status": "None", "breakdown_status": "None", "volume_confirmation": "NA"}
    break_buf = max(0.0, safe_float(break_buffer, 0.0) or 0.0)

    if typical_vol_last_candle and typical_vol_last_candle > 0:
        out["volume_confirmation"] = "Strong" if vol_last_candle >= vconfirm_mult * typical_vol_last_candle else "Weak"

    if resistance is not None and ltp > (resistance + break_buf):
        out["breakout_status"] = "Confirmed" if out["volume_confirmation"] == "Strong" else "Attempted"

    if support is not None and ltp < (support - break_buf):
        out["breakdown_status"] = "Confirmed" if out["volume_confirmation"] == "Strong" else "Attempted"

    return out

def _median(values: List[float]) -> float:
    vals = sorted([safe_float(v, 0.0) or 0.0 for v in values if safe_float(v, None) is not None])
    if not vals:
        return 0.0
    n = len(vals)
    mid = n // 2
    if n % 2 == 1:
        return float(vals[mid])
    return float((vals[mid - 1] + vals[mid]) / 2.0)

def adaptive_volume_confirm_mult(
    base_mult: float,
    candles_15: List[Candle],
    enabled: bool = True,
    min_mult: float = 1.2,
    max_mult: float = 1.8,
    short_bars: int = 6,
    long_bars: int = 24,
) -> Tuple[float, Dict[str, float]]:
    """
    Adapt volume confirmation threshold by recent volatility regime.
    Lower threshold in compression, raise in expansion, bounded by min/max.
    """
    base = max(0.5, safe_float(base_mult, 1.5) or 1.5)
    lo = max(0.5, safe_float(min_mult, 1.2) or 1.2)
    hi = max(lo, safe_float(max_mult, 1.8) or 1.8)
    out_mult = min(hi, max(lo, base))
    meta = {
        "enabled": 1.0 if enabled else 0.0,
        "base_mult": float(base),
        "used_mult": float(out_mult),
        "regime_raw": 1.0,
        "regime_used": 1.0,
        "short_med_range_pct": 0.0,
        "long_med_range_pct": 0.0,
    }
    if not enabled:
        return round(out_mult, 2), meta

    s_bars = max(3, safe_int(short_bars, 6))
    l_bars = max(s_bars + 2, safe_int(long_bars, 24))
    if len(candles_15) < s_bars + 2:
        return round(out_mult, 2), meta

    window = candles_15[-l_bars:]
    if len(window) < s_bars + 2:
        return round(out_mult, 2), meta

    def _range_pct(c: Candle) -> float:
        hi = safe_float(getattr(c, "high", 0.0), 0.0) or 0.0
        lo_px = safe_float(getattr(c, "low", 0.0), 0.0) or 0.0
        cl = safe_float(getattr(c, "close", 0.0), 0.0) or 0.0
        if cl <= 0:
            return 0.0
        return max(0.0, hi - lo_px) / cl

    long_vals = [_range_pct(c) for c in window]
    short_vals = [_range_pct(c) for c in window[-s_bars:]]
    long_med = _median(long_vals)
    short_med = _median(short_vals)
    if long_med <= 0:
        return round(out_mult, 2), meta

    regime_raw = short_med / long_med
    regime_used = min(1.2, max(0.8, regime_raw))
    dyn = base * regime_used
    used = min(hi, max(lo, dyn))
    meta["regime_raw"] = float(regime_raw)
    meta["regime_used"] = float(regime_used)
    meta["short_med_range_pct"] = float(short_med * 100.0)
    meta["long_med_range_pct"] = float(long_med * 100.0)
    meta["used_mult"] = float(used)
    return round(used, 2), meta

def ib_symbol(symbol: str) -> str:
    """Normalize a ticker symbol for IB API.

    IB uses a space to separate share-class identifiers (e.g. "BRK B"),
    whereas most data feeds use a dot (e.g. "BRK.B").
    """
    return symbol.replace(".", " ")


class IBUniverse:
    def __init__(self, ib: IB):
        self.ib = ib
        self.stock_contracts: Dict[str, Stock] = {}
        self.option_chains: Dict[str, List[Any]] = {}
        
    async def load_stock_contract(self, symbol: str) -> Optional[Stock]:
        """Load and qualify stock/ETF contract.

        IB can reject an unqualified Stock(symbol,'SMART','USD') with error 200 for some symbols
        depending on permissions/routing. We try SMART first, then fallback primary exchanges.
        """
        if symbol in self.stock_contracts:
            return self.stock_contracts[symbol]

        # Normalize symbol for IB (e.g. BRK.B â†’ BRK B)
        ib_sym = ib_symbol(symbol)

        # Candidate primary exchanges to try (keep SMART as exchange; vary primaryExchange)
        primary_candidates = [
            None,           # let IB decide
            "ARCA",         # common for US ETFs
            "NASDAQ",       # common for NMS stocks/ETFs
            "NYSE",         # common for NYSE stocks
            "AMEX",         # some listings/ETFs
        ]

        last_err: Optional[Exception] = None
        for pe in primary_candidates:
            stock = Stock(ib_sym, "SMART", "USD", primaryExchange=pe) if pe else Stock(ib_sym, "SMART", "USD")
            try:
                contracts = await self.ib.qualifyContractsAsync(stock)
                if contracts:
                    # Prefer the one whose primaryExchange matches pe if we specified it
                    chosen = contracts[0]
                    if pe:
                        for c in contracts:
                            if getattr(c, "primaryExchange", "") == pe:
                                chosen = c
                                break
                    self.stock_contracts[symbol] = chosen
                    return chosen
            except Exception as e:
                last_err = e
                # small pacing to avoid rapid-fire contract detail requests
                await asyncio.sleep(0.25)

        logger.warning(f"Failed to qualify {symbol}: {last_err}")
        return None
    
    def strike_step(self, symbol: str, price: float) -> float:
        """Determine strike step based on price"""
        if price < 25:
            return 0.5
        elif price < 200:
            return 1.0
        elif price < 500:
            return 2.5
        else:
            return 5.0
    
    async def get_option_chain(self, symbol: str, underlying_price: float) -> Dict[str, Any]:
        """Get option chain around ATM strikes.

        Caches the raw chain data (expirations, strikes, exchange) from
        reqSecDefOptParams since it doesn't change during the day.  Only
        the ATM-based strike selection is recomputed on each call.
        """
        contract = await self.load_stock_contract(symbol)
        if not contract:
            return {}

        try:
            # Use cached raw chain data if available (static for the day)
            cached = self.option_chains.get(symbol)
            if cached:
                all_expirations = cached['all_expirations']
                all_strikes = cached['all_strikes']
                best_exchange = cached['best_exchange']
                num_exchanges = cached['num_exchanges']
            else:
                # Get available option chains from IB
                chains = await self.ib.reqSecDefOptParamsAsync(
                    contract.symbol, '', contract.secType, contract.conId
                )
                if not chains:
                    return {}

                # IB returns one chain per exchange (NASDAQOM, CBOE, MIAX, etc.).
                # Merge expirations and strikes across ALL exchanges.
                all_expirations: set = set()
                all_strikes: set = set()
                best_exchange = chains[0].exchange
                for chain in chains:
                    all_expirations.update(chain.expirations)
                    all_strikes.update(chain.strikes)
                    if chain.exchange == "SMART":
                        best_exchange = "SMART"
                num_exchanges = len(chains)

                # Cache for subsequent passes
                self.option_chains[symbol] = {
                    'all_expirations': all_expirations,
                    'all_strikes': all_strikes,
                    'best_exchange': best_exchange,
                    'num_exchanges': num_exchanges,
                }

            expirations = sorted(all_expirations)

            if not expirations:
                return {}

            # Skip today's expiry (0-DTE) - use nearest future expiries
            today_str = now_et().strftime("%Y%m%d")
            valid_expiries = [exp for exp in expirations if exp > today_str]
            expiry_count = int(CONFIG.get("OI_EXPIRY_COUNT", 1))

            selected_expiries = []
            if not valid_expiries:
                selected_expiries = [expirations[0]]
            else:
                selected_expiries = valid_expiries[:expiry_count]

            # Determine strike step from actual chain data near ATM,
            # falling back to price-based guess when chain is unavailable.
            chain_strikes_set = set(round(s, 2) for s in all_strikes) if all_strikes else set()
            strikes_each_side = CONFIG["NEAR_ATM_STRIKES_EACH_SIDE"]

            if chain_strikes_set:
                sorted_chain = sorted(chain_strikes_set)
                atm_idx = min(range(len(sorted_chain)), key=lambda i: abs(sorted_chain[i] - underlying_price))
                # Infer step from nearby chain strikes
                lo = max(0, atm_idx - 3)
                hi = min(len(sorted_chain), atm_idx + 4)
                nearby = sorted_chain[lo:hi]
                if len(nearby) >= 2:
                    diffs = [round(nearby[i+1] - nearby[i], 2) for i in range(len(nearby)-1) if nearby[i+1] > nearby[i]]
                    step = min(diffs) if diffs else self.strike_step(symbol, underlying_price)
                else:
                    step = self.strike_step(symbol, underlying_price)

                atm_strike = round_to_strike_step(underlying_price, step)
                # Pick the N nearest actual chain strikes around ATM
                selected_strikes = sorted_chain[max(0, atm_idx - strikes_each_side):
                                                min(len(sorted_chain), atm_idx + strikes_each_side + 1)]
                atm_strike = sorted_chain[atm_idx]
            else:
                step = self.strike_step(symbol, underlying_price)
                atm_strike = round_to_strike_step(underlying_price, step)
                selected_strikes = [round(atm_strike + (i * step), 2) for i in range(-strikes_each_side, strikes_each_side + 1)]

            return {
                "atm_strike": atm_strike,
                "expiries": selected_expiries,
                "expiry": selected_expiries[0],
                "strike_step": step,
                "strikes": selected_strikes,
                "exchange": best_exchange,
            }
            
        except Exception as e:
            logger.error(f"Failed to get option chain for {symbol}: {e}")
            return {}
    
    def contract_multiplier(self, symbol: str) -> int:
        """US options standard multiplier is 100"""
        return 100

class OptionsBrainEngineIB:
    def __init__(self, cfg: Dict[str, Any]):
        self.cfg = cfg
        self.ib = IB()
        
        ensure_live_csv(cfg["CSV_LIVE_PATH"])
        ensure_actions_csv(cfg["CSV_ACTIONS_PATH"])
        ensure_summary_csv(cfg["CSV_SUMMARY_PATH"])
        
        self.uni = IBUniverse(self.ib)
        self.book_15m = CandleBook(cfg["CANDLE_MINUTES"])
        self.book_60m = CandleBook(cfg["HOURLY_CANDLE_MINUTES"])
        
        self.underlying_symbols: List[str] = []
        self.carry_forward_lots: Dict[str, int] = {}
        self.carry_forward_long_lots: Dict[str, int] = {}
        self.carry_forward_short_lots: Dict[str, int] = {}
        self.carry_forward_long_contract: Dict[str, Dict[str, Any]] = {}
        self.carry_forward_short_contract: Dict[str, Dict[str, Any]] = {}
        self.ticker_data: Dict[str, Any] = {}
        self._cache_lock = threading.Lock()
        self.oi_snapshot: Dict[str, Dict[str, int]] = self._load_oi_snapshot_from_disk()
        self._oi_snapshot_dirty = False
        self._oi_snapshot_last_save = time.monotonic()
        self.signal_state: Dict[str, Dict[str, Any]] = self._load_signal_state_from_disk()
        self._signal_state_dirty = False
        self._signal_state_last_save = time.monotonic()
        live_seed_state = self._bootstrap_signal_state_from_live(self.cfg.get("CSV_LIVE_PATH", "trade_ideas_live_US.csv"))
        added_seed = 0
        for sym, st in live_seed_state.items():
            if sym not in self.signal_state:
                self.signal_state[sym] = st
                added_seed += 1
        if added_seed > 0:
            self._signal_state_dirty = True
            logger.info(f"Bootstrapped sticky signal state for {added_seed} symbols from live CSV")
        self.avg20d_volume_cache: Dict[str, float] = {}
        self.typical_intraday_cache: Dict[str, Dict[str, Optional[float]]] = {}
        self._typical_intraday_loaded_keys: set[str] = set()
        self.prior_day_cache: Dict[str, tuple] = {}
        self._oi_cache: Dict[str, Dict[str, Any]] = {}  # last successful OI per symbol
        self._qualified_option_cache: Dict[Tuple[str, str, float, str], Any] = {}
        self._qualified_option_cache_day = now_et().date()
        self._stop_flag = threading.Event()
        self._vm_shutdown_initiated = False
        self._vm_shutdown_last_attempt_ts = 0.0
        self._throttled_log_ts: Dict[str, float] = {}
        self._last_connect_error_log_ts = 0.0
        self._consecutive_conn_failures: int = 0
        self._fail_counts: Dict[str, int] = {}
        self._cooldown_until: Dict[str, datetime] = {}
        self.tg_bridge = TelegramApprovalBridge(cfg, cfg["CSV_ACTIONS_PATH"])
        
        # Event loop for async operations
        self.loop = None
        self.loop_thread = None
        

        # Async tasks running on the loop thread (market data, etc.)
        self._async_tasks: List[asyncio.Task] = []
        # Worker threads (idea/approval loops). Non-daemon so we can join cleanly on shutdown.
        self._worker_threads: List[Thread] = []
    def _start_event_loop(self):
        """Start event loop in separate thread"""
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()
    
    async def _track_task(self, coro):
        """Create a task on the loop, track it for shutdown, and await it."""
        task = asyncio.create_task(coro)
        self._async_tasks.append(task)
        try:
            return await task
        finally:
            try:
                self._async_tasks.remove(task)
            except ValueError:
                pass

    def run_async(self, coro, timeout: float = 300, track: bool = True):
        """Run an async coroutine on the engine loop from sync context.

        All IB API calls should go through this to stay on the loop thread.
        Returns None on timeout or cancellation instead of letting the
        exception propagate (which previously killed the engine mid-run).
        """
        if self.loop is None or self._stop_flag.is_set():
            return None
        try:
            if track:
                fut = asyncio.run_coroutine_threadsafe(self._track_task(coro), self.loop)
            else:
                fut = asyncio.run_coroutine_threadsafe(coro, self.loop)
            return fut.result(timeout=timeout)
        except (asyncio.CancelledError, concurrent.futures.CancelledError):
            logger.warning("Async task cancelled (connection may have dropped)")
            return None
        except concurrent.futures.TimeoutError:
            logger.warning(f"Async task timed out after {timeout}s")
            return None
        except Exception as e:
            logger.error(f"run_async error: {type(e).__name__}: {e}")
            return None

    def _resolve_oi_snapshot_path(self) -> str:
        """Resolve OI snapshot cache path against the configured runtime data directory."""
        path = safe_str(self.cfg.get("OI_SNAPSHOT_PATH", "oi_snapshot_US.json"))
        if not path:
            return ""
        return resolve_runtime_path(path, fallback_name="oi_snapshot_US.json", base_dir=BOT_DATA_DIR)

    def _load_oi_snapshot_from_disk(self) -> Dict[str, Dict[str, int]]:
        """Load OI snapshots so DOI persists across restarts."""
        resolved = self._resolve_oi_snapshot_path()
        if not resolved or not os.path.exists(resolved):
            return {}
        try:
            with open(resolved, "r", encoding="utf-8") as f:
                raw = json.load(f)
            if not isinstance(raw, dict):
                return {}

            loaded: Dict[str, Dict[str, int]] = {}
            for sym, snap in raw.items():
                if not isinstance(sym, str) or not isinstance(snap, dict):
                    continue
                clean_snap: Dict[str, int] = {}
                for k, v in snap.items():
                    if not isinstance(k, str):
                        continue
                    try:
                        clean_snap[k] = int(float(v))
                    except Exception:
                        continue
                if clean_snap:
                    loaded[sym] = clean_snap

            if loaded:
                logger.info(f"Loaded OI snapshot baseline for {len(loaded)} symbols from {resolved}")
            return loaded
        except Exception as e:
            logger.warning(f"Failed loading OI snapshot baseline from {resolved}: {e}")
            return {}

    def _persist_oi_snapshot_to_disk(self, force: bool = False, min_interval_sec: float = 30.0):
        """Persist OI snapshots to keep DOI continuity across engine restarts."""
        if not force:
            if not self._oi_snapshot_dirty:
                return
            if (time.monotonic() - self._oi_snapshot_last_save) < min_interval_sec:
                return

        resolved = self._resolve_oi_snapshot_path()
        if not resolved:
            return

        try:
            payload: Dict[str, Dict[str, int]] = {}
            for sym, snap in self.oi_snapshot.items():
                if not isinstance(sym, str) or not isinstance(snap, dict):
                    continue
                clean_snap: Dict[str, int] = {}
                for k, v in snap.items():
                    if not isinstance(k, str):
                        continue
                    try:
                        clean_snap[k] = int(v)
                    except Exception:
                        continue
                if clean_snap:
                    payload[sym] = clean_snap

            tmp_path = f"{resolved}.tmp"
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=True, separators=(",", ":"))
            os.replace(tmp_path, resolved)
            self._oi_snapshot_dirty = False
            self._oi_snapshot_last_save = time.monotonic()
        except Exception as e:
            logger.warning(f"Failed persisting OI snapshot baseline to {resolved}: {e}")

    def _resolve_signal_state_path(self) -> str:
        """Resolve sticky signal state path against the configured runtime data directory."""
        path = safe_str(self.cfg.get("SIGNAL_STATE_PATH", "signal_state_US.json"))
        if not path:
            return ""
        return resolve_runtime_path(path, fallback_name="signal_state_US.json", base_dir=BOT_DATA_DIR)

    def _load_signal_state_from_disk(self) -> Dict[str, Dict[str, Any]]:
        """Load sticky signal state so confirmed breaks survive restarts."""
        resolved = self._resolve_signal_state_path()
        if not resolved or not os.path.exists(resolved):
            return {}
        try:
            with open(resolved, "r", encoding="utf-8") as f:
                raw = json.load(f)
            if not isinstance(raw, dict):
                return {}

            loaded: Dict[str, Dict[str, Any]] = {}
            for sym, state in raw.items():
                if not isinstance(sym, str) or not isinstance(state, dict):
                    continue
                stype = safe_str(state.get("type")).upper()
                level = safe_float(state.get("level"))
                if stype not in {"BREAKOUT", "BREAKDOWN"} or level is None:
                    continue
                loaded[sym.upper().strip()] = {
                    "type": stype,
                    "level": float(level),
                    "confirmed_at": safe_str(state.get("confirmed_at")),
                }

            if loaded:
                logger.info(f"Loaded sticky signal state for {len(loaded)} symbols from {resolved}")
            return loaded
        except Exception as e:
            logger.warning(f"Failed loading sticky signal state from {resolved}: {e}")
            return {}

    def _bootstrap_signal_state_from_live(self, live_path: str) -> Dict[str, Dict[str, Any]]:
        """
        Restore sticky state from live CSV so restarts keep valid confirmed breaks.
        """
        try:
            resolved = resolve_runtime_path(live_path, base_dir=BOT_DATA_DIR)
            if (not resolved) or (not os.path.exists(resolved)) or os.path.getsize(resolved) == 0:
                return {}
            live = pd.read_csv(resolved, dtype=str, keep_default_na=False)
            if live.empty:
                return {}

            needed = {"symbol", "timestamp_et", "ltp", "breakout_status", "breakdown_status", "key_resistance", "key_support"}
            if not needed.issubset(set(live.columns)):
                return {}

            live["sym"] = live["symbol"].fillna("").astype(str).str.upper().str.strip()
            live = live[live["sym"] != ""].copy()
            if live.empty:
                return {}

            live["ts"] = pd.to_datetime(live["timestamp_et"], errors="coerce")
            live = live.sort_values(by=["sym", "ts", "timestamp_et"], ascending=[True, False, False])
            latest_by_sym = live.drop_duplicates(subset=["sym"], keep="first").copy()
            state: Dict[str, Dict[str, Any]] = {}

            for sym in latest_by_sym["sym"].tolist():
                sym_rows = live[live["sym"] == sym]
                if sym_rows.empty:
                    continue
                latest_row = sym_rows.iloc[0]
                latest_ltp = safe_float(latest_row.get("ltp"))

                confirmed = sym_rows[
                    (sym_rows["breakout_status"].astype(str).str.upper() == "CONFIRMED") |
                    (sym_rows["breakdown_status"].astype(str).str.upper() == "CONFIRMED")
                ]
                if confirmed.empty:
                    continue
                ev = confirmed.iloc[0]

                if safe_str(ev.get("breakdown_status")).upper() == "CONFIRMED":
                    level = safe_float(ev.get("key_support"))
                    if level is not None and latest_ltp is not None and latest_ltp <= level:
                        state[sym] = {
                            "type": "BREAKDOWN",
                            "level": float(level),
                            "confirmed_at": safe_str(ev.get("timestamp_et")),
                        }
                elif safe_str(ev.get("breakout_status")).upper() == "CONFIRMED":
                    level = safe_float(ev.get("key_resistance"))
                    if level is not None and latest_ltp is not None and latest_ltp >= level:
                        state[sym] = {
                            "type": "BREAKOUT",
                            "level": float(level),
                            "confirmed_at": safe_str(ev.get("timestamp_et")),
                        }
            return state
        except Exception as e:
            logger.warning(f"Failed to bootstrap sticky signal state from {live_path}: {e}")
            return {}

    def _persist_signal_state_to_disk(self, force: bool = False, min_interval_sec: float = 30.0):
        """Persist sticky signal state across restarts."""
        if not force:
            if not self._signal_state_dirty:
                return
            if (time.monotonic() - self._signal_state_last_save) < min_interval_sec:
                return

        resolved = self._resolve_signal_state_path()
        if not resolved:
            return

        try:
            payload: Dict[str, Dict[str, Any]] = {}
            for sym, state in self.signal_state.items():
                if not isinstance(sym, str) or not isinstance(state, dict):
                    continue
                stype = safe_str(state.get("type")).upper()
                level = safe_float(state.get("level"))
                if stype not in {"BREAKOUT", "BREAKDOWN"} or level is None:
                    continue
                payload[sym] = {
                    "type": stype,
                    "level": float(level),
                    "confirmed_at": safe_str(state.get("confirmed_at")),
                }

            tmp_path = f"{resolved}.tmp"
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=True, separators=(",", ":"))
            os.replace(tmp_path, resolved)
            self._signal_state_dirty = False
            self._signal_state_last_save = time.monotonic()
        except Exception as e:
            logger.warning(f"Failed persisting sticky signal state to {resolved}: {e}")

    def _apply_sticky_break_state(
        self,
        symbol: str,
        bb: Dict[str, str],
        ltp: Optional[float],
        key_res: Optional[float],
        key_sup: Optional[float],
    ) -> Dict[str, Any]:
        if not self.cfg.get("ENABLE_STICKY_SIGNALS", True):
            return bb

        out = dict(bb)
        out["_fresh_confirmed"] = False
        out["_sticky_confirmed"] = False
        out["_sticky_is_fresh"] = False
        out["_sticky_age_candles"] = -1
        sym = safe_str(symbol).upper().strip()
        if not sym:
            return out

        now_dt = now_et()
        now_ts = now_dt.strftime("%Y-%m-%d %H:%M:%S")
        max_sticky_candles = max(0, int(self.cfg.get("STICKY_KEY_LEVEL_MAX_CANDLES", 3)))
        candle_minutes = max(1, int(self.cfg.get("CANDLE_MINUTES", 15)))
        breakout = safe_str(out.get("breakout_status", "None"))
        breakdown = safe_str(out.get("breakdown_status", "None"))

        with self._cache_lock:
            if breakout == "Confirmed" and key_res is not None:
                self.signal_state[sym] = {"type": "BREAKOUT", "level": float(key_res), "confirmed_at": now_ts}
                self._signal_state_dirty = True
                out["_fresh_confirmed"] = True
                return out
            if breakdown == "Confirmed" and key_sup is not None:
                self.signal_state[sym] = {"type": "BREAKDOWN", "level": float(key_sup), "confirmed_at": now_ts}
                self._signal_state_dirty = True
                out["_fresh_confirmed"] = True
                return out

            state = self.signal_state.get(sym)
            if not isinstance(state, dict):
                return out

            stype = safe_str(state.get("type")).upper()
            level = safe_float(state.get("level"))
            if ltp is None or level is None:
                return out

            confirmed_raw = safe_str(state.get("confirmed_at"))
            confirmed_dt = None
            if confirmed_raw:
                try:
                    confirmed_dt = datetime.strptime(confirmed_raw, "%Y-%m-%d %H:%M:%S").replace(tzinfo=ET)
                except Exception:
                    confirmed_dt = None
            age_candles = -1
            if confirmed_dt is not None:
                age_candles = max(0, int((now_dt - confirmed_dt).total_seconds() // (60 * candle_minutes)))
            sticky_is_fresh = (age_candles >= 0) and (age_candles <= max_sticky_candles)

            if stype == "BREAKDOWN":
                if breakout == "Confirmed" or ltp > level:
                    self.signal_state.pop(sym, None)
                    self._signal_state_dirty = True
                    return out
                out["breakdown_status"] = "Confirmed"
                out["breakout_status"] = "None"
                out["_sticky_confirmed"] = True
                out["_sticky_is_fresh"] = sticky_is_fresh
                out["_sticky_age_candles"] = age_candles
                return out

            if stype == "BREAKOUT":
                if breakdown == "Confirmed" or ltp < level:
                    self.signal_state.pop(sym, None)
                    self._signal_state_dirty = True
                    return out
                out["breakout_status"] = "Confirmed"
                out["breakdown_status"] = "None"
                out["_sticky_confirmed"] = True
                out["_sticky_is_fresh"] = sticky_is_fresh
                out["_sticky_age_candles"] = age_candles
                return out

        return out

    async def _connect_ib(self):
        """Connect to Interactive Brokers"""
        try:
            await self.ib.connectAsync(
                self.cfg["IB_HOST"],
                self.cfg["IB_PORT"],
                clientId=self.cfg["IB_CLIENT_ID"]
            )
            logger.info(f"Connected to IB at {self.cfg['IB_HOST']}:{self.cfg['IB_PORT']}")
            return True
        except Exception as e:
            now_ts = time.monotonic()
            if (now_ts - self._last_connect_error_log_ts) >= 15.0:
                logger.error(f"Failed to connect to IB: {e}")
                self._last_connect_error_log_ts = now_ts
            return False
    

    async def _ensure_connected(self, attempts: int = 3, delay_sec: float = 2.0) -> bool:
        """Ensure IB connection is up; try reconnecting if needed."""
        for i in range(attempts):
            if self.ib.isConnected():
                self._consecutive_conn_failures = 0
                return True
            ok = await self._connect_ib()
            if ok:
                self._consecutive_conn_failures = 0
                return True
            await asyncio.sleep(delay_sec * (i + 1))
        return False

    async def _safe_historical_request(
        self,
        contract,
        endDateTime: str = '',
        durationStr: str = '',
        barSizeSetting: str = '',
        whatToShow: str = 'TRADES',
        useRTH: bool = True,
        formatDate: int = 1,
        retries: Optional[int] = None,
        pace_sec: Optional[float] = None,
        timeout_sec: Optional[float] = None,
    ):
        """Historical request with pacing + reconnect retries.

        Notes:
        - IB can intermittently time out historical queries; we retry with backoff.
        - A timeout commonly triggers IB error 366 ("No historical data query found"); treat as retryable.
        - When many consecutive connection failures have accumulated, retries
          and timeouts are reduced to avoid wasting minutes per symbol.
        """
        retries = retries if retries is not None else int(self.cfg.get("HIST_RETRIES", 5))
        pace_sec = pace_sec if pace_sec is not None else float(self.cfg.get("HIST_PACE_SEC", 0.8))
        timeout_sec = timeout_sec if timeout_sec is not None else float(self.cfg.get("HIST_TIMEOUT_SEC", 45))

        # Adaptive: reduce retries/timeout when IB has been persistently
        # unreachable (e.g. after market close or gateway restart).
        if self._consecutive_conn_failures >= 5:
            retries = min(retries, 2)
            timeout_sec = min(timeout_sec, 8.0)

        last_err: Optional[Exception] = None

        for i in range(retries):
            if self._stop_flag.is_set():
                raise RuntimeError("Stopping")

            if not await self._ensure_connected(attempts=1, delay_sec=1.5):
                last_err = RuntimeError("Not connected")
                self._consecutive_conn_failures += 1
                await asyncio.sleep(0.75 * (i + 1))
                continue

            try:
                coro = self.ib.reqHistoricalDataAsync(
                    contract,
                    endDateTime=endDateTime,
                    durationStr=durationStr,
                    barSizeSetting=barSizeSetting,
                    whatToShow=whatToShow,
                    useRTH=useRTH,
                    formatDate=formatDate,
                )
                bars = await asyncio.wait_for(coro, timeout=timeout_sec)
                self._consecutive_conn_failures = 0  # success resets counter
                await asyncio.sleep(pace_sec)  # pacing to respect IB limits
                return bars
            except asyncio.TimeoutError as e:
                last_err = e
                # Backoff a bit; do NOT spam reconnect on pure timeouts
                await asyncio.sleep(1.25 * (i + 1))
            except Exception as e:
                last_err = e
                self._consecutive_conn_failures += 1
                logger.warning(f"Connection attempt {i+1}/{retries} failed: {e}")
                # Reconnect on socket / connectivity issues
                try:
                    if self.ib.isConnected():
                        self.ib.disconnect()
                except Exception:
                    pass
                await asyncio.sleep(0.9 * (i + 1))

        raise last_err if last_err else RuntimeError("Historical request failed")

    async def _prepare_universe(self):
        """Load all underlying contracts and option chains"""
        if not await self._ensure_connected():
            raise RuntimeError("Not connected (cannot prepare universe)")

        all_underlyings = (
            self.cfg["INDEX_UNDERLYINGS"] +
            self.cfg["STOCK_UNDERLYINGS_US"]
        )

        # Drop empty/whitespace symbols defensively and de-duplicate while preserving order.
        unique_underlyings: List[str] = []
        seen_underlyings = set()
        duplicate_count = 0
        for s in all_underlyings:
            sym = safe_str(s).upper().strip()
            if not sym:
                continue
            if sym in seen_underlyings:
                duplicate_count += 1
                continue
            seen_underlyings.add(sym)
            unique_underlyings.append(sym)
        if duplicate_count > 0:
            logger.warning(
                "Universe had duplicate symbols; skipped %d duplicate entries before contract load.",
                duplicate_count,
            )
        all_underlyings = unique_underlyings
        
        for symbol in all_underlyings:
            try:
                contract = await asyncio.wait_for(
                    self.uni.load_stock_contract(symbol),
                    timeout=float(self.cfg.get("CONTRACT_TIMEOUT_SEC", 20)),
                )
            except asyncio.TimeoutError:
                logger.warning(f"Contract load timeout for {symbol}; skipping.")
                continue
            except Exception as e:
                logger.error(f"Contract load error for {symbol}: {e}")
                continue
            if contract:
                self.underlying_symbols.append(symbol)
        
        if not self.underlying_symbols:
            raise RuntimeError("No underlyings found")
        
        logger.info(f"Loaded {len(self.underlying_symbols)} underlyings: {self.underlying_symbols}")
    
    async def _seed_all_underlyings_history(self, days: int):
        """Seed historical data for all underlyings.

        Processes symbols in small parallel batches to speed up seeding
        while respecting IB's historical data pacing limits.
        """
        batch_size = int(self.cfg.get("SEED_PARALLEL_BATCH", 5))
        symbols = list(self.underlying_symbols)
        for i in range(0, len(symbols), batch_size):
            batch = symbols[i:i + batch_size]
            tasks = []
            for symbol in batch:
                tasks.append(self._seed_symbol_both_timeframes(symbol, days))
            await asyncio.gather(*tasks, return_exceptions=True)
            await asyncio.sleep(0.3)

    async def _seed_symbol_both_timeframes(self, symbol: str, days: int):
        """Seed both 15m and 60m data for a single symbol."""
        await self._seed_underlying_history(symbol, days, self.cfg["CANDLE_MINUTES"], self.book_15m)
        await self._seed_underlying_history(symbol, days, self.cfg["HOURLY_CANDLE_MINUTES"], self.book_60m)
    
    async def _seed_underlying_history(self, symbol: str, days: int, minutes: int, target_book: CandleBook):
        """Fetch and populate historical candle data"""
        contract = self.uni.stock_contracts.get(symbol)
        if not contract:
            return

        try:
            duration = f"{days} D"
            bar_size = minutes_to_ib_bar_size(minutes)

            bars = await self._safe_historical_request(
                contract,
                endDateTime='',
                durationStr=duration,
                barSizeSetting=bar_size,
                whatToShow='TRADES',
                useRTH=True,
                formatDate=1,
                retries=3,
                pace_sec=0.5,
            )

            if not bars:
                logger.warning(f"No bars returned for {symbol} ({bar_size})")
                return

            candles = []
            for bar in bars:
                dt_et = bar.date
                if dt_et.tzinfo is None:
                    dt_et = dt_et.replace(tzinfo=ET)

                vol = int(bar.volume)
                close = float(bar.close)

                candles.append(Candle(
                    start_et=floor_time(dt_et, minutes),
                    open=float(bar.open),
                    high=float(bar.high),
                    low=float(bar.low),
                    close=close,
                    volume=vol,
                    vwap_num=close * vol,
                    vwap_den=vol
                ))

            target_book.closed[symbol] = candles
            logger.info(f"Seeded {len(candles)} bars for {symbol} ({minutes}m using '{bar_size}')")

        except Exception as e:
            logger.error(f"Seed error for {symbol} ({minutes}m): {e}", exc_info=True)
    
    
    def start(self):
        """Start the engine"""
        self.loop_thread = Thread(target=self._start_event_loop, daemon=False, name="ib_async_loop")
        self.loop_thread.start()
        time.sleep(0.5)

        connected = self.run_async(self._connect_ib())
        if not connected:
            logger.error("Failed to connect to IB. Exiting.")
            self.stop()
            return

        self.run_async(self._prepare_universe())
        self._refresh_carry_forward_positions(force_log=True, log_manual_map=True)
        self.run_async(
            self._seed_all_underlyings_history(days=self.cfg["SEED_HISTORY_DAYS"]),
            timeout=float(self.cfg.get("SEED_TIMEOUT_SEC", 900)),
        )

        async def _spawn_tasks():
            await self._subscribe_market_data()
            t = asyncio.create_task(self._ticker_task(poll_sec=2.0))
            self._async_tasks.append(t)

        self.run_async(_spawn_tasks())

        t1 = Thread(target=self._approval_loop, daemon=False, name="approval_loop")
        t2 = Thread(target=self._idea_loop, daemon=False, name="idea_loop")
        self._worker_threads.extend([t1, t2])
        if self.tg_bridge.enabled:
            t3 = Thread(target=self._telegram_loop, daemon=False, name="telegram_loop")
            self._worker_threads.append(t3)
            logger.info("Telegram approvals enabled")
        else:
            reason = safe_str(getattr(self.tg_bridge, "disabled_reason", ""))
            logger.info(
                f"Telegram approvals disabled{(': ' + reason) if reason else ''}"
            )
        for t in self._worker_threads:
            t.start()

        logger.info("Engine started. Press Ctrl+C to stop.")

        try:
            while not self._stop_flag.is_set():
                time.sleep(0.5)
        except KeyboardInterrupt:
            self.stop()

    
    def stop(self):
        """Stop the engine (cleanly)."""
        if self._stop_flag.is_set():
            return
        logger.info("Stopping engine...")
        self._stop_flag.set()

        async def _shutdown_async():
            # Cancel pending tasks we created, then disconnect IB.
            try:
                pending = [t for t in list(self._async_tasks) if not t.done()]
                for t in pending:
                    t.cancel()
                if pending:
                    await asyncio.gather(*pending, return_exceptions=True)
                await asyncio.sleep(0)  # let parent tasks unwind
            except Exception:
                pass

            # Best-effort IB disconnect
            try:
                if self.ib.isConnected():
                    self.ib.disconnect()
            except Exception:
                pass

        if self.loop is not None:
            try:
                # If stop() is called from the loop thread, schedule shutdown and stop loop when done.
                if self.loop_thread is not None and threading.current_thread() is self.loop_thread:
                    async def _shutdown_then_stop():
                        await _shutdown_async()
                        self.loop.stop()
                    asyncio.create_task(_shutdown_then_stop())
                else:
                    # Run shutdown and wait for completion before stopping the loop.
                    try:
                        # Avoid wrapping shutdown in _track_task to prevent cancellation recursion.
                        self.run_async(_shutdown_async(), timeout=60, track=False)
                    finally:
                        try:
                            self.loop.call_soon_threadsafe(self.loop.stop)
                        except Exception:
                            pass
            except Exception:
                # best-effort disconnect
                try:
                    if self.ib.isConnected():
                        self.ib.disconnect()
                except Exception:
                    pass

        # Join worker threads
        for t in list(self._worker_threads):
            try:
                if t.is_alive():
                    t.join(timeout=10)
            except Exception:
                pass

        # Join event loop thread
        if self.loop_thread is not None:
            try:
                self.loop_thread.join(timeout=10)
            except Exception:
                pass

        # Persist baseline on shutdown so next run gets non-zero DOI deltas.
        self._persist_oi_snapshot_to_disk(force=True)
        self._persist_signal_state_to_disk(force=True)

        logger.info("Engine stopped.")

    async def _subscribe_market_data(self):
        """Subscribe to market data, respecting IB's ticker limit.

        IB allows ~100 concurrent streaming tickers on most account types.
        We cap at MAX_SUBSCRIPTIONS to avoid Error 101 floods.
        """
        if not await self._ensure_connected():
            raise RuntimeError("Not connected (cannot subscribe to market data)")

        max_subs = int(self.cfg.get("MAX_SUBSCRIPTIONS", 100))
        subscribed = 0
        for symbol in self.underlying_symbols:
            if subscribed >= max_subs:
                logger.warning(
                    f"Reached MAX_SUBSCRIPTIONS={max_subs}; "
                    f"remaining {len(self.underlying_symbols) - subscribed} symbols will use snapshot data only"
                )
                break
            contract = self.uni.stock_contracts.get(symbol)
            if not contract:
                continue
            try:
                ticker = self.ib.reqMktData(contract, '', False, False)
                self.ticker_data[symbol] = ticker
                subscribed += 1
                await asyncio.sleep(0.05)
            except Exception as e:
                logger.error(f"Market data subscribe error for {symbol}: {e}")
        logger.info(f"Subscribed to market data for {subscribed}/{len(self.underlying_symbols)} symbols")

    async def _ticker_task(self, poll_sec: float = 2.0):
        """Update candle books from latest tick data (runs on loop thread)."""
        if not self.ticker_data:
            await self._subscribe_market_data()

        while not self._stop_flag.is_set():
            try:
                if not self.ib.isConnected():
                    ok = await self._ensure_connected(attempts=1, delay_sec=1.5)
                    if not ok:
                        self._log_throttled(
                            "ticker_disconnected_pause",
                            "Ticker task paused: IB disconnected; retrying shortly.",
                            interval_sec=30.0,
                        )
                        await asyncio.sleep(max(5.0, poll_sec))
                        continue
                    await self._subscribe_market_data()

                ts = datetime.now(ET)
                for symbol, ticker in list(self.ticker_data.items()):
                    last = getattr(ticker, "last", None)
                    if last is None or (isinstance(last, float) and math.isnan(last)):
                        continue
                    ltp = float(last)

                    vol = getattr(ticker, "volume", None)
                    cum_vol = int(vol) if vol is not None else None

                    self.book_15m.update(symbol, ltp, ts, cum_vol)
                    self.book_60m.update(symbol, ltp, ts, cum_vol)

                await asyncio.sleep(poll_sec)
            except asyncio.CancelledError:
                break
            except Exception as e:
                emsg = safe_str(e)
                if "not connected" in emsg.lower():
                    self._log_throttled(
                        "ticker_not_connected",
                        f"Ticker task paused: {emsg}",
                        interval_sec=30.0,
                    )
                else:
                    logger.error(f"Ticker task error: {e}")
                await asyncio.sleep(max(5.0, poll_sec))

    async def avg_20d_daily_volume_async(self, symbol: str) -> Optional[float]:
        """Get 20-day average daily volume"""
        if symbol in self.avg20d_volume_cache:
            return self.avg20d_volume_cache[symbol]

        # Fast-fail: skip expensive historical request when IB is disconnected
        if not self.ib.isConnected():
            return None

        contract = self.uni.stock_contracts.get(symbol)
        if not contract:
            return None

        try:
            bars = await self._safe_historical_request(
                contract,
                endDateTime='',
                durationStr='60 D',
                barSizeSetting='1 day',
                whatToShow='TRADES',
                useRTH=True,
                formatDate=1,
            )
            
            if not bars or len(bars) < 5:
                return None
            
            volumes = [bar.volume for bar in bars[-20:]]
            avg = sum(volumes) / len(volumes)
            self.avg20d_volume_cache[symbol] = avg
            return avg
            
        except Exception as e:
            logger.error(f"Error getting 20d volume for {symbol}: {e}")
            return None
    
    def avg_20d_daily_volume(self, symbol: str) -> Optional[float]:
        """Sync wrapper for avg_20d_daily_volume_async"""
        return self.run_async(self.avg_20d_daily_volume_async(symbol))
    
    async def typical_intraday_volume_same_time_async(self, symbol: str) -> Optional[float]:
        """Get typical intraday volume at same time of day"""
        hhmm = floor_time(now_et(), self.cfg["CANDLE_MINUTES"]).strftime("%H:%M")
        cache_key = f"{symbol}:{now_et().date().isoformat()}"

        with self._cache_lock:
            day_cache = self.typical_intraday_cache.get(cache_key, {})
            if hhmm in day_cache:
                return day_cache.get(hhmm)
            if cache_key in self._typical_intraday_loaded_keys:
                # Profile already built for this symbol/day; missing bucket means unavailable.
                return None

        # Fast-fail: skip expensive historical request when IB is disconnected
        if not self.ib.isConnected():
            return None

        contract = self.uni.stock_contracts.get(symbol)
        if not contract:
            return None

        try:
            bars = await self._safe_historical_request(
                contract,
                endDateTime='',
                durationStr='30 D',
                barSizeSetting=minutes_to_ib_bar_size(self.cfg["CANDLE_MINUTES"]),
                whatToShow='TRADES',
                useRTH=True,
                formatDate=1,
            )

            profile: Dict[str, Optional[float]] = {}
            if bars:
                # Build full intraday profile once (HH:MM -> 20-day mean volume).
                by_hhmm: Dict[str, List[float]] = {}
                for bar in bars:
                    b_hhmm = bar.date.strftime("%H:%M")
                    by_hhmm.setdefault(b_hhmm, []).append(float(bar.volume))

                for b_hhmm, vols in by_hhmm.items():
                    if len(vols) < 5:
                        profile[b_hhmm] = None
                        continue
                    recent = vols[-20:]
                    profile[b_hhmm] = (sum(recent) / len(recent)) if recent else None

            with self._cache_lock:
                self.typical_intraday_cache[cache_key] = profile
                self._typical_intraday_loaded_keys.add(cache_key)
            return profile.get(hhmm)
            
        except Exception as e:
            logger.error(f"Error getting typical volume for {symbol}: {e}")
            with self._cache_lock:
                self.typical_intraday_cache.setdefault(cache_key, {})[hhmm] = None
            return None
    
    def typical_intraday_volume_same_time(self, symbol: str) -> Optional[float]:
        """Sync wrapper"""
        return self.run_async(self.typical_intraday_volume_same_time_async(symbol))

    async def _prefetch_typical_intraday_cache_async(self, symbols: List[str], max_parallel: int = 2):
        """Warm typical intraday volume cache for all symbols in current bucket."""
        if not symbols:
            return

        sem = asyncio.Semaphore(max(1, int(max_parallel)))

        async def _one(sym: str):
            async with sem:
                try:
                    await self.typical_intraday_volume_same_time_async(sym)
                except Exception:
                    pass

        await asyncio.gather(*(_one(sym) for sym in symbols), return_exceptions=True)
    
    async def _batch_qualify_and_snapshot_oi(
        self, contracts: List[Option], batch_size: int = 40
    ) -> Dict[str, int]:
        """Batch-qualify option contracts and fetch OI via streaming market data.

        Returns a dict keyed by '{expiry}_{strike}_{right}' -> OI value.
        Processes in batches to respect IB rate limits while being much faster
        than sequential per-strike requests.

        IMPORTANT: snapshot=True silently ignores generic tick types (like 101
        for Open Interest).  We MUST use snapshot=False (streaming mode) and
        then cancel with cancelMktData after reading the data.

        To avoid Error 101 (max subscriptions), we temporarily free underlying
        subscription slots before requesting option streams, then re-subscribe
        the freed underlyings after each batch.
        """
        results: Dict[str, int] = {}
        today = now_et().date()
        if self._qualified_option_cache_day != today:
            self._qualified_option_cache.clear()
            self._qualified_option_cache_day = today

        # --- Temporarily free underlying subscription slots ---
        # IB allows ~100 concurrent streaming tickers.  The underlyings
        # already consume all slots.  Free up enough for one batch of
        # option contracts so Error 101 doesn't reject them.
        slots_needed = min(len(contracts), batch_size)
        freed_syms: List[str] = []
        all_syms = list(self.ticker_data.keys())
        for sym in reversed(all_syms):
            if len(freed_syms) >= slots_needed:
                break
            contract_stock = self.uni.stock_contracts.get(sym)
            if contract_stock:
                try:
                    self.ib.cancelMktData(contract_stock)
                    freed_syms.append(sym)
                except Exception:
                    pass
        if freed_syms:
            await asyncio.sleep(0.1)  # let cancellations propagate

        try:
            for i in range(0, len(contracts), batch_size):
                batch = contracts[i:i + batch_size]

                qualified_map: List[Tuple[Option, Any]] = []
                to_qualify: List[Option] = []

                for orig in batch:
                    q_key = (orig.symbol, orig.lastTradeDateOrContractMonth, round(float(orig.strike), 4), orig.right)
                    cached_q = self._qualified_option_cache.get(q_key)
                    if cached_q is not None:
                        qualified_map.append((orig, cached_q))
                    else:
                        to_qualify.append(orig)

                if to_qualify:
                    qualified_lists = await asyncio.gather(
                        *(self.ib.qualifyContractsAsync(c) for c in to_qualify),
                        return_exceptions=True,
                    )
                    for orig, result in zip(to_qualify, qualified_lists):
                        if isinstance(result, Exception) or not result:
                            continue
                        q_contract = result[0]
                        q_key = (orig.symbol, orig.lastTradeDateOrContractMonth, round(float(orig.strike), 4), orig.right)
                        self._qualified_option_cache[q_key] = q_contract
                        qualified_map.append((orig, q_contract))

                if not qualified_map:
                    continue

                # Request streaming market data with generic tick 101 (Option OI).
                # Tick 101 delivers BOTH callOpenInterest (tick ID 27) and
                # putOpenInterest (tick ID 28).
                streaming_entries: List[Tuple[Option, Any, Any]] = []
                for orig, q_contract in qualified_map:
                    try:
                        t = self.ib.reqMktData(q_contract, '101', snapshot=False)
                        streaming_entries.append((orig, q_contract, t))
                    except Exception:
                        pass

                if not streaming_entries:
                    continue

                # Poll for OI data with configurable cadence instead of fixed sleep.
                # Lower poll interval reduces baseline per-batch wait.
                _poll_interval = max(0.05, float(self.cfg.get("OI_POLL_INTERVAL_SEC", 0.2)))
                _max_wait = max(_poll_interval, float(self.cfg.get("OI_POLL_MAX_WAIT_SEC", 2.0)))
                _waited = 0.0
                while _waited < _max_wait:
                    await asyncio.sleep(_poll_interval)
                    _waited += _poll_interval
                    # Probe ALL tickers â€” the first might have failed
                    for _so, _sq, _st in streaming_entries:
                        _oi_probe = getattr(_st, 'callOpenInterest', float('nan'))
                        if not (isinstance(_oi_probe, float) and math.isnan(_oi_probe)):
                            break
                    else:
                        continue  # no ticker had data yet, keep polling
                    break  # inner break triggered â†’ exit poll loop

                # Diagnostic: find a ticker with data (not just the first one)
                if streaming_entries and i == 0:
                    sample_orig, sample_qc, sample_t = streaming_entries[0]
                    for _so, _sq, _st in streaming_entries:
                        if not math.isnan(getattr(_st, 'callOpenInterest', float('nan'))):
                            sample_orig, sample_qc, sample_t = _so, _sq, _st
                            break
                    logger.info(
                        f"OI diag [{sample_qc.symbol} {sample_orig.strike}{sample_orig.right}]: "
                        f"callOI={getattr(sample_t, 'callOpenInterest', '?')}, "
                        f"putOI={getattr(sample_t, 'putOpenInterest', '?')}, "
                        f"last={getattr(sample_t, 'last', '?')}, "
                        f"close={getattr(sample_t, 'close', '?')}, "
                        f"volume={getattr(sample_t, 'volume', '?')}, "
                        f"waited={_waited:.1f}s"
                    )

                # Read OI values from populated Ticker objects
                for orig, q_contract, ticker in streaming_entries:
                    strike = strike_key(orig.strike)
                    right = orig.right
                    oi_attr = 'callOpenInterest' if right == 'C' else 'putOpenInterest'
                    oi_val = getattr(ticker, oi_attr, None)
                    if oi_val is None or (isinstance(oi_val, float) and math.isnan(oi_val)):
                        alt_attr = 'putOpenInterest' if right == 'C' else 'callOpenInterest'
                        oi_val = getattr(ticker, alt_attr, None)
                    if oi_val is None or (isinstance(oi_val, float) and math.isnan(oi_val)):
                        oi_val = 0
                    expiry = str(orig.lastTradeDateOrContractMonth)
                    results[f"{expiry}_{strike}_{right}"] = int(oi_val)

                # Cancel option streaming subscriptions to free slots
                for _orig, q_contract, _ticker in streaming_entries:
                    try:
                        self.ib.cancelMktData(q_contract)
                    except Exception:
                        pass
        finally:
            # --- Re-subscribe freed underlyings (always, even on error) ---
            for sym in freed_syms:
                contract_stock = self.uni.stock_contracts.get(sym)
                if contract_stock:
                    try:
                        ticker = self.ib.reqMktData(contract_stock, '', False, False)
                        self.ticker_data[sym] = ticker
                    except Exception:
                        pass
            if freed_syms:
                await asyncio.sleep(0.05)

        return results

    async def options_positioning_async(
        self, symbol: str, underlying_price: float,
        chain_info: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Get option chain OI analysis using batch qualification + parallel snapshots.

        Performance: ~2-4 seconds per symbol (vs ~60s sequential).
        Also tracks DOI (Delta OI) via oi_snapshot cache.
        Accepts optional pre-fetched chain_info to avoid duplicate reqSecDefOptParams calls.
        """
        if chain_info is None:
            chain_info = await self.uni.get_option_chain(symbol, underlying_price)

        _unavailable = {
            "expiry_used": "",
            "top_call_oi_strikes": "Unavailable",
            "top_put_oi_strikes": "Unavailable",
            "top_call_doi_strikes": "Unavailable",
            "top_put_doi_strikes": "Unavailable",
            "pcr_overall": "Unavailable",
            "pcr_near_atm": "Unavailable",
            "oi_support_zone": "Unavailable",
            "oi_resistance_zone": "Unavailable",
            "oi_shift_notes": "Unavailable",
            "iv_notes": "Unavailable",
        }

        if not chain_info:
            return _unavailable

        expiries = chain_info.get("expiries", [chain_info["expiry"]])
        strikes = chain_info["strikes"]
        atm = chain_info["atm_strike"]
        step = chain_info["strike_step"]
        exchange = chain_info.get("exchange", "SMART")

        try:
            t0 = time.monotonic()
            logger.info(
                f"Fetching option OI for {symbol}: {len(expiries)} expiries, "
                f"{len(strikes)} strikes, exchange={exchange}"
            )

            # Build all option contracts upfront for batch processing
            ib_sym = ib_symbol(symbol)
            all_contracts: List[Option] = []
            for expiry in expiries:
                for strike in strikes:
                    all_contracts.append(Option(ib_sym, expiry, strike, 'C', 'SMART'))
                    all_contracts.append(Option(ib_sym, expiry, strike, 'P', 'SMART'))

            # Batch qualify + snapshot (the fast path)
            oi_map = await self._batch_qualify_and_snapshot_oi(all_contracts, batch_size=40)

            # Aggregate OI across expiries
            ce_oi = {s: 0 for s in strikes}
            pe_oi = {s: 0 for s in strikes}
            for expiry in expiries:
                for strike in strikes:
                    sk = strike_key(strike)
                    ce_oi[strike] += oi_map.get(f"{expiry}_{sk}_C", 0)
                    pe_oi[strike] += oi_map.get(f"{expiry}_{sk}_P", 0)

            # --- DOI (Delta OI) tracking via snapshot cache ---
            oi_key = symbol
            prev_snapshot = self.oi_snapshot.get(oi_key, {})

            current_snapshot: Dict[str, int] = {}
            for strike, oi in ce_oi.items():
                current_snapshot[f"ce_{strike_key(strike)}"] = oi
            for strike, oi in pe_oi.items():
                current_snapshot[f"pe_{strike_key(strike)}"] = oi
            self.oi_snapshot[oi_key] = current_snapshot
            if current_snapshot != prev_snapshot:
                self._oi_snapshot_dirty = True
                self._persist_oi_snapshot_to_disk(force=False)

            ce_doi: Dict[float, int] = {}
            pe_doi: Dict[float, int] = {}
            for strike in strikes:
                sk = strike_key(strike)
                ce_doi[strike] = ce_oi.get(strike, 0) - prev_snapshot.get(f"ce_{sk}", ce_oi.get(strike, 0))
                pe_doi[strike] = pe_oi.get(strike, 0) - prev_snapshot.get(f"pe_{sk}", pe_oi.get(strike, 0))

            # --- Compute metrics ---
            total_call_oi = sum(ce_oi.values())
            total_put_oi = sum(pe_oi.values())

            pcr_overall = round(total_put_oi / total_call_oi, 2) if total_call_oi > 0 else "Unavailable"

            near_call_oi = sum(oi for s, oi in ce_oi.items() if abs(s - atm) <= step)
            near_put_oi = sum(oi for s, oi in pe_oi.items() if abs(s - atm) <= step)
            pcr_near_atm = round(near_put_oi / near_call_oi, 2) if near_call_oi > 0 else "Unavailable"

            top_call_oi = sorted(ce_oi.items(), key=lambda x: x[1], reverse=True)[:3]
            top_put_oi = sorted(pe_oi.items(), key=lambda x: x[1], reverse=True)[:3]
            top_call_doi = sorted(ce_doi.items(), key=lambda x: abs(x[1]), reverse=True)[:3]
            top_put_doi = sorted(pe_doi.items(), key=lambda x: abs(x[1]), reverse=True)[:3]

            def _fmt_strikes(items):
                return " | ".join(f"{s}:{int(v):,}" for s, v in items) if items else "Unavailable"

            def _fmt_doi(items):
                return " | ".join(f"{s}:{int(v):+,}" for s, v in items) if items else "Unavailable"

            # OI zones from top strikes (range-style, consistent with Kite engine)
            call_strikes = [strike for strike, _ in top_call_oi]
            put_strikes = [strike for strike, _ in top_put_oi]
            oi_resistance_zone = f"{min(call_strikes)}-{max(call_strikes)}" if call_strikes else "Unavailable"
            oi_support_zone = f"{min(put_strikes)}-{max(put_strikes)}" if put_strikes else "Unavailable"

            # OI shift notes (major changes > 10,000 contracts)
            shift_notes = []
            for strike, doi in sorted(ce_doi.items()):
                if abs(doi) > 10000:
                    shift_notes.append(f"CE {strike}: {doi:+,}")
            for strike, doi in sorted(pe_doi.items()):
                if abs(doi) > 10000:
                    shift_notes.append(f"PE {strike}: {doi:+,}")

            expiries_str = ", ".join(expiries) if len(expiries) > 1 else expiries[0]

            # DOI summary â€” log when any non-zero DOI detected
            doi_nonzero = sum(1 for v in list(ce_doi.values()) + list(pe_doi.values()) if v != 0)
            has_prev = bool(prev_snapshot)
            if not has_prev:
                shift_notes.append("Baseline capture pass (no previous OI snapshot)")

            elapsed = time.monotonic() - t0
            logger.info(
                f"OI done for {symbol}: {len(all_contracts)} contracts, "
                f"{len(oi_map)} qualified, PCR={pcr_overall}, "
                f"DOI: {doi_nonzero} non-zero (prev={'yes' if has_prev else 'no'}), {elapsed:.1f}s"
            )

            return {
                "expiry_used": expiries_str,
                "top_call_oi_strikes": _fmt_strikes(top_call_oi),
                "top_put_oi_strikes": _fmt_strikes(top_put_oi),
                "top_call_doi_strikes": _fmt_doi(top_call_doi),
                "top_put_doi_strikes": _fmt_doi(top_put_doi),
                "pcr_overall": pcr_overall,
                "pcr_near_atm": pcr_near_atm,
                "oi_support_zone": oi_support_zone,
                "oi_resistance_zone": oi_resistance_zone,
                "oi_shift_notes": " | ".join(shift_notes) if shift_notes else "No major shifts",
                "iv_notes": "Future IV analysis",
            }

        except Exception as e:
            logger.error(f"Error getting option positioning for {symbol}: {e}", exc_info=True)
            _unavailable["expiry_used"] = ", ".join(expiries) if expiries else ""
            _unavailable["oi_shift_notes"] = str(e)
            return _unavailable
    
    def options_positioning(
        self, symbol: str, underlying_price: float,
        chain_info: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Sync wrapper â€” returns unavailable dict on failure instead of None."""
        result = self.run_async(self.options_positioning_async(symbol, underlying_price, chain_info=chain_info))
        if result is None:
            return {
                "expiry_used": "", "top_call_oi_strikes": "Unavailable",
                "top_put_oi_strikes": "Unavailable", "top_call_doi_strikes": "Unavailable",
                "top_put_doi_strikes": "Unavailable", "pcr_overall": "Unavailable",
                "pcr_near_atm": "Unavailable", "oi_support_zone": "Unavailable",
                "oi_resistance_zone": "Unavailable", "oi_shift_notes": "Unavailable",
                "iv_notes": "Unavailable",
            }
        return result
    
    def underlying_snapshot(self, symbol: str) -> Dict[str, Any]:
        """Get current market snapshot for underlying"""
        ts = now_et()

        def _num(v: Any) -> Optional[float]:
            fv = safe_float(v)
            if fv is None:
                return None
            if isinstance(fv, float) and math.isnan(fv):
                return None
            return fv

        ticker = self.ticker_data.get(symbol)
        if ticker is None:
            self._log_throttled(
                f"snapshot_ticker_missing:{symbol}",
                f"Ticker unavailable for {symbol}; using candle fallback when possible.",
                interval_sec=30.0,
            )

        ltp = _num(getattr(ticker, "last", None)) if ticker is not None else None
        prev_close = _num(getattr(ticker, "close", None)) if ticker is not None else None
        # Fallback: when market is closed (weekends, after hours), ticker.last
        # is NaN.  Use previous close so the engine can still compute strike
        # ranges and fetch OI data.
        if ltp is None and prev_close is not None:
            ltp = prev_close
        day_high = _num(getattr(ticker, "high", None)) if ticker is not None else None
        day_low = _num(getattr(ticker, "low", None)) if ticker is not None else None
        volume = _num(getattr(ticker, "volume", None)) if ticker is not None else None

        # Fallback to websocket/candlebook data when ticker snapshot is sparse.
        cur = self.book_15m.current.get(symbol)
        closed = self.book_15m.closed.get(symbol, [])
        if ltp is None:
            if cur is not None:
                ltp = _num(cur.close)
            if ltp is None and closed:
                ltp = _num(closed[-1].close)

        today_bars: List[Candle] = [c for c in closed if c.start_et.date() == ts.date()]
        if cur is not None and cur.start_et.date() == ts.date():
            today_bars.append(cur)

        if day_high is None and today_bars:
            highs = [_num(c.high) for c in today_bars]
            highs = [h for h in highs if h is not None]
            if highs:
                day_high = max(highs)
        if day_low is None and today_bars:
            lows = [_num(c.low) for c in today_bars]
            lows = [l for l in lows if l is not None]
            if lows:
                day_low = min(lows)

        if prev_close is None:
            for c in reversed(closed):
                c_close = _num(c.close)
                if c_close is not None and c.start_et.date() < ts.date():
                    prev_close = c_close
                    break
            if prev_close is None and today_bars:
                earliest = min(today_bars, key=lambda c: c.start_et)
                prev_close = _num(earliest.open)

        if volume is None and today_bars:
            vol_sum = 0.0
            for c in today_bars:
                v = _num(c.volume)
                if v is not None:
                    vol_sum += v
            volume = vol_sum

        pct_change = None
        if ltp is not None and prev_close and prev_close > 0:
            pct_change = (ltp - prev_close) / prev_close * 100.0
        
        # VWAP relation — 2-level fallback matching Kite pattern
        vwap_relation = "NA"
        cur = self.book_15m.current.get(symbol)
        v = cur.vwap() if cur is not None else None
        # Fallback to latest closed candle VWAP when current bucket has no volume yet.
        if v is None:
            closed = self.book_15m.closed.get(symbol, [])
            if closed:
                v = closed[-1].vwap()
        if v is not None and ltp is not None:
            vwap_relation = "Above" if ltp >= v else "Below"
        
        day_range = f"{day_low:.2f}-{day_high:.2f}" if day_high is not None and day_low is not None else ""
        
        return {
            "timestamp_et": ts.strftime("%Y-%m-%d %H:%M:%S ET"),
            "session_context": et_session_context(ts),
            "ltp": ltp,
            "prev_close": prev_close,
            "pct_change": round(pct_change, 2) if pct_change is not None else None,
            "day_high": day_high,
            "day_low": day_low,
            "volume": volume,
            "vwap_relation": vwap_relation,
            "day_range": day_range
        }
    
    def build_trade_plan(self, symbol: str) -> Dict[str, Any]:
        """Build complete trade plan with technical and options analysis"""
        snap = self.underlying_snapshot(symbol)
        
        avg20 = self.avg_20d_daily_volume(symbol)
        typical = self.typical_intraday_volume_same_time(symbol)
        
        vol_vs_20d = None
        if snap.get("volume") is not None and avg20 and avg20 > 0:
            vol_vs_20d = safe_float(snap.get("volume")) / avg20
        
        # Technical analysis
        candles_15 = self.book_15m.closed.get(symbol, [])
        struct_15 = compute_structure(candles_15)
        swing_hi, swing_lo = swing_levels(candles_15, self.cfg["SWING_LOOKBACK_BARS"])
        orh, orl = opening_range_levels(candles_15, self.cfg["OPENING_RANGE_MINUTES"])
        
        candles_60 = self.book_60m.closed.get(symbol, [])
        struct_60 = compute_structure(candles_60)
        
        # Prior day high/low (cached per day)
        # Find the most recent trading day from candle data (not timedelta-1,
        # which breaks on Mondays / after holidays).
        prior_day_high, prior_day_low = None, None
        cache_key = f"{symbol}:{now_et().date().isoformat()}"
        with self._cache_lock:
            cached_prior = self.prior_day_cache.get(cache_key)
        if cached_prior is not None:
            prior_day_high, prior_day_low = cached_prior
        elif len(candles_15) > 0:
            today = now_et().date()
            prior_dates = sorted(
                {c.start_et.date() for c in candles_15 if c.start_et.date() < today},
                reverse=True,
            )
            if prior_dates:
                last_trading_day = prior_dates[0]
                yesterday_candles = [c for c in candles_15 if c.start_et.date() == last_trading_day]
                if yesterday_candles:
                    prior_day_high = max(c.high for c in yesterday_candles)
                    prior_day_low = min(c.low for c in yesterday_candles)
            with self._cache_lock:
                self.prior_day_cache[cache_key] = (prior_day_high, prior_day_low)
        
        # Breakout/breakdown analysis
        last_closed_15 = self.book_15m.last_closed(symbol, 1)
        last15 = last_closed_15[0] if last_closed_15 else None
        vol_last_15 = int(last15.volume) if last15 else 0
        close_last_15 = safe_float(last15.close) if last15 else None

        atr_period = max(2, safe_int(self.cfg.get("KEY_LEVEL_ATR_PERIOD", 14), 14))
        atr_15 = compute_atr(candles_15, atr_period)
        key_level_eval_px = close_last_15 if close_last_15 is not None else (safe_float(snap.get("ltp"), 0) or 0.0)
        key_level_eval_source = "close_last_15" if close_last_15 is not None else "ltp_fallback"
        cached_oi_support_zone = ""
        cached_oi_resistance_zone = ""
        with self._cache_lock:
            cached_oi = self._oi_cache.get(symbol)
        if isinstance(cached_oi, dict):
            cached_oi_support_zone = safe_str(cached_oi.get("oi_support_zone", ""))
            cached_oi_resistance_zone = safe_str(cached_oi.get("oi_resistance_zone", ""))
        key_level_meta = select_key_levels_contextual(
            candles_15=candles_15,
            eval_px=key_level_eval_px,
            atr_15=atr_15,
            swing_high=swing_hi,
            swing_low=swing_lo,
            prior_day_high=prior_day_high,
            prior_day_low=prior_day_low,
            opening_range_high=orh,
            opening_range_low=orl,
            oi_support_zone=cached_oi_support_zone,
            oi_resistance_zone=cached_oi_resistance_zone,
        )
        key_res = safe_float(key_level_meta.get("key_resistance"))
        key_sup = safe_float(key_level_meta.get("key_support"))
        ref_px = max(0.0, safe_float(key_level_eval_px, 0.0) or 0.0)
        key_level_break_buffer = max(
            ref_px * max(0.0, safe_float(self.cfg.get("KEY_LEVEL_MIN_BREAK_PCT", 0.001), 0.001) or 0.0),
            (safe_float(atr_15, 0.0) or 0.0) * max(0.0, safe_float(self.cfg.get("KEY_LEVEL_MIN_BREAK_ATR_MULT", 0.2), 0.2) or 0.0),
            max(0.0, safe_float(self.cfg.get("KEY_LEVEL_MIN_BREAK_POINTS", 0.0), 0.0) or 0.0),
        )
        breakout_threshold = (key_res + key_level_break_buffer) if key_res is not None else None
        breakdown_threshold = (key_sup - key_level_break_buffer) if key_sup is not None else None
        dist_to_breakout_threshold = (
            (key_level_eval_px - breakout_threshold)
            if breakout_threshold is not None
            else None
        )
        dist_to_breakdown_threshold = (
            (key_level_eval_px - breakdown_threshold)
            if breakdown_threshold is not None
            else None
        )
        volume_confirm_mult, vol_mult_meta = adaptive_volume_confirm_mult(
            base_mult=float(self.cfg.get("VOLUME_CONFIRM_MULT", 1.5)),
            candles_15=candles_15,
            enabled=bool(self.cfg.get("DYNAMIC_VOLUME_CONFIRM_ENABLED", True)),
            min_mult=float(self.cfg.get("DYNAMIC_VOLUME_CONFIRM_MIN_MULT", 1.2)),
            max_mult=float(self.cfg.get("DYNAMIC_VOLUME_CONFIRM_MAX_MULT", 1.8)),
            short_bars=int(self.cfg.get("DYNAMIC_VOLUME_CONFIRM_SHORT_BARS", 6)),
            long_bars=int(self.cfg.get("DYNAMIC_VOLUME_CONFIRM_LONG_BARS", 24)),
        )
        bb = breakout_breakdown_read(
            ltp=key_level_eval_px,
            resistance=key_res,
            support=key_sup,
            vol_last_candle=vol_last_15,
            typical_vol_last_candle=typical,
            vconfirm_mult=volume_confirm_mult,
            break_buffer=key_level_break_buffer,
        )
        bb = self._apply_sticky_break_state(
            symbol=symbol,
            bb=bb,
            ltp=safe_float(snap.get("ltp")),
            key_res=key_res,
            key_sup=key_sup,
        )
        
        # Options positioning â€” fetch chain once, pass to options_positioning
        ltp_for_chain = snap.get("ltp") or 0
        # Fallback: use last closed candle close if ticker data unavailable
        if ltp_for_chain <= 0 and candles_15:
            ltp_for_chain = candles_15[-1].close
        chain_info = (self.run_async(self.uni.get_option_chain(symbol, ltp_for_chain)) or {}) if ltp_for_chain > 0 else {}
        opt = self.options_positioning(symbol, ltp_for_chain, chain_info=chain_info) if ltp_for_chain > 0 else {}
        oi_source = "unavailable"
        # Cache successful OI; fall back to cached data when fetch fails.
        if opt and opt.get("pcr_overall") != "Unavailable":
            with self._cache_lock:
                self._oi_cache[symbol] = opt
            oi_source = "fetched"
        else:
            with self._cache_lock:
                cached_oi = self._oi_cache.get(symbol)
            if cached_oi is not None:
                opt = cached_oi
                oi_source = "cached"

        ltp_for_doi = safe_float(snap.get("ltp"))
        doi_sig = (
            doi_signal(
                ltp_for_doi,
                opt.get("top_call_doi_strikes", ""),
                opt.get("top_put_doi_strikes", ""),
                window_steps=int(self.cfg.get("DOI_WINDOW_STEPS", 2)),
                min_conf=float(self.cfg.get("DOI_MIN_CONFIDENCE_PCT", 30.0)),
                confirm_conf=float(self.cfg.get("DOI_CONFIRM_CONFIDENCE_PCT", 40.0)),
                unwind_boost=float(self.cfg.get("DOI_UNWIND_BOOST", 0.5)),
                min_activity=float(self.cfg.get("DOI_MIN_ACTIVITY", 0.0)),
            )
            if ltp_for_doi
            else {"direction": "NEUTRAL", "confirmation": 0, "confidence": 0, "net": 0}
        )

        oi_sup_lo, oi_sup_hi = parse_zone_bounds(opt.get("oi_support_zone", ""))
        oi_res_lo, oi_res_hi = parse_zone_bounds(opt.get("oi_resistance_zone", ""))

        recent = candles_15[-6:] if len(candles_15) >= 1 else []
        recent_ranges = [max(0.0, safe_float(c.high, 0.0) - safe_float(c.low, 0.0)) for c in recent]
        recent_moves = [
            abs(safe_float(candles_15[i].close, 0.0) - safe_float(candles_15[i - 1].close, 0.0))
            for i in range(max(1, len(candles_15) - 5), len(candles_15))
        ] if len(candles_15) >= 2 else []
        avg_range = (sum(recent_ranges) / len(recent_ranges)) if recent_ranges else 0.0
        avg_move = (sum(recent_moves) / len(recent_moves)) if recent_moves else 0.0
        px = safe_float(snap.get("ltp"), 0.0) or 0.0
        move_unit = max(avg_range, avg_move, px * 0.0025, 0.5)
        
        # News
        news_bullets, news_sources = load_news_from_feed_csv_for_symbol(
            news_csv_path=self.cfg["NEWS_FEED_CSV_PATH"],
            symbol=symbol,
            lookback_minutes=self.cfg["NEWS_LOOKBACK_MINUTES"],
            max_items=self.cfg["NEWS_MAX_ITEMS"]
        )
        
        news_section = {
            "bullets": news_bullets if news_bullets else ["No matching news found in news_feed.csv (within lookback)."],
            "sources": news_sources if news_sources else [],
        }
        
        # Trading bias and setup
        sticky_enabled = bool(self.cfg.get("ENABLE_STICKY_SIGNALS", True))
        fresh_confirmed = bool(bb.get("_fresh_confirmed", False))
        sticky_is_fresh = bool(bb.get("_sticky_is_fresh", False))
        confirmed_is_actionable = (not sticky_enabled) or fresh_confirmed or sticky_is_fresh

        bias = "Wait"
        setup_type = "Range trade"
        counter_trend_note = ""
        
        if bb["breakout_status"] in ["Confirmed", "Attempted"]:
            bias = "Buy" if (bb["breakout_status"] == "Confirmed" and confirmed_is_actionable) else "Wait"
            setup_type = "Breakout"
        elif bb["breakdown_status"] in ["Confirmed", "Attempted"]:
            bias = "Sell" if (bb["breakdown_status"] == "Confirmed" and confirmed_is_actionable) else "Wait"
            setup_type = "Breakdown"
        
        bias_for_scoring = bias
        # Hourly filter
        if bias == "Buy" and struct_60 == "LH/LL":
            counter_trend_note = "60m trend filter is bearish (LH/LL); buy signal treated as counter-trend -> WAIT."
            bias = "Wait"
            setup_type = "Breakout (counter-trend)"
        elif bias == "Sell" and struct_60 == "HH/HL":
            counter_trend_note = "60m trend filter is bullish (HH/HL); sell signal treated as counter-trend -> WAIT."
            bias = "Wait"
            setup_type = "Breakdown (counter-trend)"
        
        # Entry/exit levels (adaptive by recent movement + OI structure)
        entry = sl = t1 = t2 = None
        rationale = []

        def _valid_levels(levels: List[Optional[float]]) -> List[float]:
            return [float(x) for x in levels if isinstance(x, (int, float))]

        def _nearest_above(ref: float, levels: List[Optional[float]]) -> Optional[float]:
            vals = [x for x in _valid_levels(levels) if x > ref]
            return min(vals) if vals else None

        def _nearest_below(ref: float, levels: List[Optional[float]]) -> Optional[float]:
            vals = [x for x in _valid_levels(levels) if x < ref]
            return max(vals) if vals else None

        if snap.get("ltp") is not None:
            entry_buffer = max(move_unit * 0.15, (px * 0.0006) if px else 0.1)
            sl_buffer = max(move_unit * 0.25, (px * 0.0008) if px else 0.1)

            if setup_type.startswith("Breakout") and key_res is not None:
                entry_level = key_res + entry_buffer
                sl_ref = _nearest_below(entry_level, [key_res, orl, prior_day_low, swing_lo, oi_sup_lo, oi_sup_hi])
                sl_level = (sl_ref - sl_buffer) if sl_ref is not None else (key_res - sl_buffer)

                t1_level = _nearest_above(entry_level, [swing_hi, orh, prior_day_high, oi_res_lo, oi_res_hi, entry_level + move_unit])
                if t1_level is None:
                    t1_level = entry_level + move_unit
                t2_level = _nearest_above(
                    t1_level,
                    [oi_res_hi, prior_day_high, swing_hi, entry_level + (2.0 * move_unit), entry_level + (3.0 * move_unit)],
                )
                if t2_level is None:
                    t2_level = t1_level + move_unit

                min_rr = max(0.5, float(self.cfg.get("MIN_RR", 2.0)))
                rr_adjusted = False
                risk = entry_level - sl_level
                if risk <= 0:
                    risk = max(move_unit * 0.40, (px * 0.001) if px else 0.2)
                    sl_level = entry_level - risk
                    rr_adjusted = True
                min_t1_level = entry_level + (risk * min_rr)
                if t1_level < min_t1_level:
                    t1_level = min_t1_level
                    rr_adjusted = True
                min_t2_level = t1_level + max(risk, move_unit * 0.5)
                if t2_level < min_t2_level:
                    t2_level = min_t2_level
                    rr_adjusted = True

                flow_note = ""
                if doi_sig["direction"] == "BULLISH":
                    flow_note = f"; DOI bullish ({doi_sig['confidence']}%)"

                entry = (
                    f"Enter on 15m close > {entry_level:.2f} "
                    f"({entry_buffer:.2f} above trigger) and volume >= {volume_confirm_mult:.2f}x typical{flow_note}"
                )
                sl = f"SL below {sl_level:.2f} (adaptive buffer {sl_buffer:.2f})"
                t1 = f"T1 near {t1_level:.2f} (OI resistance zone {opt.get('oi_resistance_zone', 'N/A')})"
                t2 = f"T2 near {t2_level:.2f}; trail below fresh 15m swing lows after T1"
                rationale = [
                    "Breakout entry uses adaptive buffer from recent 15m movement.",
                    "Targets blend OI zone bounds with nearest structural resistance levels.",
                ]
                if rr_adjusted:
                    rationale.append(f"Targets adjusted to keep at least {min_rr:.1f}:1 reward-to-risk from entry/SL.")
            elif setup_type.startswith("Breakdown") and key_sup is not None:
                entry_level = key_sup - entry_buffer
                sl_ref = _nearest_above(entry_level, [key_sup, orh, prior_day_high, swing_hi, oi_res_lo, oi_res_hi])
                sl_level = (sl_ref + sl_buffer) if sl_ref is not None else (key_sup + sl_buffer)

                t1_level = _nearest_below(entry_level, [swing_lo, orl, prior_day_low, oi_sup_lo, oi_sup_hi, entry_level - move_unit])
                if t1_level is None:
                    t1_level = entry_level - move_unit
                t2_level = _nearest_below(
                    t1_level,
                    [oi_sup_lo, prior_day_low, swing_lo, entry_level - (2.0 * move_unit), entry_level - (3.0 * move_unit)],
                )
                if t2_level is None:
                    t2_level = t1_level - move_unit

                min_rr = max(0.5, float(self.cfg.get("MIN_RR", 2.0)))
                rr_adjusted = False
                risk = sl_level - entry_level
                if risk <= 0:
                    risk = max(move_unit * 0.40, (px * 0.001) if px else 0.2)
                    sl_level = entry_level + risk
                    rr_adjusted = True
                min_t1_level = entry_level - (risk * min_rr)
                if t1_level > min_t1_level:
                    t1_level = min_t1_level
                    rr_adjusted = True
                min_t2_level = t1_level - max(risk, move_unit * 0.5)
                if t2_level > min_t2_level:
                    t2_level = min_t2_level
                    rr_adjusted = True

                flow_note = ""
                if doi_sig["direction"] == "BEARISH":
                    flow_note = f"; DOI bearish ({doi_sig['confidence']}%)"

                entry = (
                    f"Enter on 15m close < {entry_level:.2f} "
                    f"({entry_buffer:.2f} below trigger) and volume >= {volume_confirm_mult:.2f}x typical{flow_note}"
                )
                sl = f"SL above {sl_level:.2f} (adaptive buffer {sl_buffer:.2f})"
                t1 = f"T1 near {t1_level:.2f} (OI support zone {opt.get('oi_support_zone', 'N/A')})"
                t2 = f"T2 near {t2_level:.2f}; trail above fresh 15m swing highs after T1"
                rationale = [
                    "Breakdown entry uses adaptive buffer from recent 15m movement.",
                    "Targets blend OI zone bounds with nearest structural support levels.",
                ]
                if rr_adjusted:
                    rationale.append(f"Targets adjusted to keep at least {min_rr:.1f}:1 reward-to-risk from entry/SL.")
            else:
                if key_res is not None and key_sup is not None:
                    entry = (
                        f"Wait. Watch for 15m close > {key_res:.2f} or < {key_sup:.2f} "
                        f"with volume >= {volume_confirm_mult:.2f}x typical."
                    )
                else:
                    entry = "Wait"
                rationale = ["Signals not aligned strongly enough."]
        
        alt = "If level breaks but price closes back inside without volume, treat as false move: wait or range trade with tighter stops."
        
        # v3.1 CONFIDENCE SCORING (5 FACTORS WITH DOI) + stale-OI decisive block flag
        enable_output = self.cfg.get("ENABLE_SCORING_OUTPUT", False)

        # Factor 1: Volume Confirmation (+1)
        f_volume = 1 if bb["volume_confirmation"] == "Strong" else 0
        if f_volume and enable_output:
            logger.debug(f"Factor 1 (Volume): Strong volume confirmation ({vol_last_15:,} >= {volume_confirm_mult:.2f}x typical)")

        # Factor 2: Key Level Break (+1)
        key_level_ref_px = close_last_15 if close_last_15 is not None else safe_float(snap.get("ltp"))
        key_level_ref_label = "15m close" if close_last_15 is not None else "LTP"
        fresh_confirmed = bool(bb.get("_fresh_confirmed", False))
        sticky_confirmed = bool(bb.get("_sticky_confirmed", False))
        sticky_is_fresh = bool(bb.get("_sticky_is_fresh", False))
        sticky_age_candles = safe_int(bb.get("_sticky_age_candles", -1), -1)
        sticky_max_candles = max(0, int(self.cfg.get("STICKY_KEY_LEVEL_MAX_CANDLES", 3)))
        broke_resistance = (
            key_res is not None
            and key_level_ref_px is not None
            and key_level_ref_px > (key_res + key_level_break_buffer)
        )
        broke_support = (
            key_sup is not None
            and key_level_ref_px is not None
            and key_level_ref_px < (key_sup - key_level_break_buffer)
        )
        f_key_level = 1 if (fresh_confirmed or broke_resistance or broke_support or (sticky_confirmed and sticky_is_fresh)) else 0
        if f_key_level and enable_output:
            if broke_resistance or broke_support:
                level_type = "resistance" if broke_resistance else "support"
                level_value = key_res if broke_resistance else key_sup
                logger.debug(
                    f"Factor 2 (Key Level Break): {key_level_ref_label} broke {level_type} at "
                    f"{level_value:.2f} (+/- buffer {key_level_break_buffer:.2f})"
                )
            elif fresh_confirmed:
                logger.debug("Factor 2 (Key Level Break): fresh confirmed break state")
            else:
                logger.debug(
                    "Factor 2 (Key Level Break): sticky confirmation retained "
                    f"(age={sticky_age_candles} candles, window={sticky_max_candles})"
                )
        elif sticky_confirmed and (not sticky_is_fresh) and enable_output:
            logger.debug(
                "Factor 2 (Key Level Break): sticky confirmation expired for scoring "
                f"(age={sticky_age_candles} candles, window={sticky_max_candles})"
            )

        # Factor 3: OI Alignment - DOI Based (+1)
        if enable_output and doi_sig["direction"] != "NEUTRAL":
            logger.info(f"DOI signal [{symbol}]: dir={doi_sig['direction']}, conf={doi_sig['confidence']}%, net={doi_sig['net']}, confirm={doi_sig['confirmation']}, bias={bias}, bias_for_scoring={bias_for_scoring}")
        f_oi = 0
        if bias_for_scoring == "Buy" and doi_sig["direction"] == "BULLISH" and doi_sig["confirmation"] == 1:
            f_oi = 1
            if enable_output:
                logger.debug(f"Factor 3 (OI Alignment - DOI): BULLISH confirmed, net={doi_sig['net']}, conf={doi_sig['confidence']}%")
        elif bias_for_scoring == "Sell" and doi_sig["direction"] == "BEARISH" and doi_sig["confirmation"] == 1:
            f_oi = 1
            if enable_output:
                logger.debug(f"Factor 3 (OI Alignment - DOI): BEARISH confirmed, net={doi_sig['net']}, conf={doi_sig['confidence']}%")

        # Factor 4: Structure Alignment (+1)
        structure_bias = "Buy" if struct_15 == "HH/HL" else ("Sell" if struct_15 == "LH/LL" else "Wait")
        f_structure = 1 if (
            struct_15 == struct_60
            and struct_15 in {"HH/HL", "LH/LL"}
            and structure_bias == bias_for_scoring
        ) else 0
        if f_structure and enable_output:
            logger.debug(f"Factor 4 (Structure): 15m and 60m aligned ({struct_15}) with bias {bias_for_scoring}")

        # Factor 5: Counter-trend Penalty (-2)
        f_counter = -2 if counter_trend_note else 0
        if f_counter and enable_output:
            logger.debug(f"Factor 5 (Counter-trend Penalty): -2 points. {counter_trend_note}")

        actionable_min_conf = max(0, min(4, safe_int(self.cfg.get("ACTIONABLE_MIN_CONFIDENCE", 3), 3)))
        score_without_oi = min(4, max(0, f_volume + f_key_level + f_structure + f_counter))
        # Final Confidence Score & Grade
        confidence_score = min(4, max(0, f_volume + f_key_level + f_oi + f_structure + f_counter))
        oi_is_stale_for_scoring = safe_str(oi_source).lower() != "fetched"
        f_oi_stale_decisive_block = 1 if (
            oi_is_stale_for_scoring
            and f_oi > 0
            and confidence_score >= actionable_min_conf
            and score_without_oi < actionable_min_conf
        ) else 0
        confidence = confidence_grade(confidence_score)
        gate_break_confirmed = 1 if (
            (
                safe_str(bb.get("breakout_status", "")).lower() == "confirmed"
                and confirmed_is_actionable
            )
            or (
                safe_str(bb.get("breakdown_status", "")).lower() == "confirmed"
                and confirmed_is_actionable
            )
        ) else 0
        gate_volume_strong = 1 if f_volume > 0 else 0
        gate_key_level_break = 1 if f_key_level > 0 else 0
        gate_oi_alignment = 1 if f_oi > 0 else 0
        gate_structure_alignment = 1 if f_structure > 0 else 0
        gate_counter_trend_ok = 0 if counter_trend_note else 1
        gate_confidence_ok = 1 if confidence_score >= actionable_min_conf else 0
        gate_oi_freshness_ok = 0 if f_oi_stale_decisive_block else 1
        gate_actionable = 1 if (
            safe_str(bias).upper() in {"BUY", "SELL"}
            and gate_confidence_ok
            and gate_oi_freshness_ok
        ) else 0

        gate_blockers: List[str] = []
        if gate_actionable == 0:
            breakout_status = safe_str(bb.get("breakout_status", "None"))
            breakdown_status = safe_str(bb.get("breakdown_status", "None"))
            if not gate_break_confirmed:
                if breakout_status == "Attempted" or breakdown_status == "Attempted":
                    gate_blockers.append("BREAK_NOT_CONFIRMED")
                elif bool(bb.get("_sticky_confirmed", False)) and (not bool(bb.get("_sticky_is_fresh", False))):
                    gate_blockers.append("STICKY_BREAK_EXPIRED")
                else:
                    gate_blockers.append("NO_BREAK_SIGNAL")
            if not gate_volume_strong:
                gate_blockers.append("VOLUME_NOT_STRONG")
            if not gate_key_level_break:
                gate_blockers.append("KEY_LEVEL_GATE_FAIL")
            if bias_for_scoring in {"Buy", "Sell"} and (not gate_oi_alignment):
                gate_blockers.append("OI_NOT_ALIGNED")
            if bias_for_scoring in {"Buy", "Sell"} and (not gate_structure_alignment):
                gate_blockers.append("STRUCTURE_NOT_ALIGNED")
            if not gate_counter_trend_ok:
                gate_blockers.append("COUNTER_TREND_FILTER")
            if not gate_confidence_ok:
                gate_blockers.append(f"SCORE_BELOW_{actionable_min_conf}")
            if not gate_oi_freshness_ok:
                gate_blockers.append("OI_STALE_DECISIVE_BLOCK")
        dedup_blockers: List[str] = []
        seen_blockers: set = set()
        for item in gate_blockers:
            token = safe_str(item).strip()
            if not token or token in seen_blockers:
                continue
            seen_blockers.add(token)
            dedup_blockers.append(token)
        gate_blockers_text = " | ".join(dedup_blockers) if dedup_blockers else "NONE"

        if enable_output:
            logger.debug(f"FINAL SCORE: {confidence_score}/4 -> Confidence: {confidence}")
            if f_oi_stale_decisive_block:
                logger.debug(
                    "Stale OI decisive block: cached/unavailable OI pushed score across actionable "
                    f"threshold {actionable_min_conf} (score_without_oi={score_without_oi})."
                )
        
        wcm = [
            "15m close back inside key level (invalidation)",
            "No volume expansion / momentum stall",
            "OI/PCR shifts against the direction near key strikes",
            "Unexpected event headline increases volatility",
        ]
        
        if counter_trend_note:
            wcm.insert(0, counter_trend_note)
        
        # Build final plan
        plan = {
            "Snapshot (ET timestamp)": {
                "timestamp_et": snap.get("timestamp_et"),
                "session_context": snap.get("session_context"),
                "LTP": snap.get("ltp"),
                "% change": snap.get("pct_change"),
                "day_high": snap.get("day_high"),
                "day_low": snap.get("day_low"),
                "prev_close": snap.get("prev_close"),
                "volume": snap.get("volume"),
                "vol_vs_20d_avg": round(vol_vs_20d, 2) if vol_vs_20d is not None else "Unavailable",
                "vol_vs_time_of_day": round((vol_last_15 / typical), 2) if (typical and typical > 0) else "Unavailable",
                "VWAP relation": snap.get("vwap_relation"),
                "day_range": snap.get("day_range"),
            },
            "Breakout/Breakdown Read": {
                "key_resistance": round(key_res, 2) if key_res is not None else "Unavailable",
                "key_support": round(key_sup, 2) if key_sup is not None else "Unavailable",
                "structure": struct_15,
                "trend_filter_60m": struct_60,
                "opening_range_high": round(orh, 2) if orh is not None else "Unavailable",
                "opening_range_low": round(orl, 2) if orl is not None else "Unavailable",
                "prior_day_high": round(prior_day_high, 2) if prior_day_high is not None else "Unavailable",
                "prior_day_low": round(prior_day_low, 2) if prior_day_low is not None else "Unavailable",
                "swing_high": round(swing_hi, 2) if swing_hi is not None else "Unavailable",
                "swing_low": round(swing_lo, 2) if swing_lo is not None else "Unavailable",
                "atr_15m": round(atr_15, 2) if atr_15 is not None else "Unavailable",
                "key_level_method": safe_str(key_level_meta.get("method", "contextual_scored_v1")),
                "key_level_cluster_tolerance": round(safe_float(key_level_meta.get("cluster_tolerance", 0.0), 0.0), 2),
                "key_level_support_score": round(safe_float(key_level_meta.get("support_score", 0.0), 0.0), 3),
                "key_level_resistance_score": round(safe_float(key_level_meta.get("resistance_score", 0.0), 0.0), 3),
                "key_level_support_candidates": safe_int(key_level_meta.get("support_candidates", 0), 0),
                "key_level_resistance_candidates": safe_int(key_level_meta.get("resistance_candidates", 0), 0),
                "key_level_eval_px": round(key_level_eval_px, 2),
                "key_level_eval_source": key_level_eval_source,
                "key_level_break_buffer": round(key_level_break_buffer, 2),
                "breakout_threshold": round(breakout_threshold, 2) if breakout_threshold is not None else "Unavailable",
                "breakdown_threshold": round(breakdown_threshold, 2) if breakdown_threshold is not None else "Unavailable",
                "distance_to_breakout_threshold": round(dist_to_breakout_threshold, 2) if dist_to_breakout_threshold is not None else "Unavailable",
                "distance_to_breakdown_threshold": round(dist_to_breakdown_threshold, 2) if dist_to_breakdown_threshold is not None else "Unavailable",
                "volume_confirm_mult_used": round(volume_confirm_mult, 2),
                "volume_confirm_regime_ratio": round(safe_float(vol_mult_meta.get("regime_raw", 1.0), 1.0), 3),
                "volume_confirm_regime_used": round(safe_float(vol_mult_meta.get("regime_used", 1.0), 1.0), 3),
                "volume_confirmation_status": bb["volume_confirmation"],
                "breakout_status": bb["breakout_status"],
                "breakdown_status": bb["breakdown_status"],
            },
            "Options-Derived Levels": {
                "expiry_used": opt.get("expiry_used", ""),
                "Top Call OI (resistance zones)": opt.get("top_call_oi_strikes", "Unavailable"),
                "Top Put OI (support zones)": opt.get("top_put_oi_strikes", "Unavailable"),
                "Top Call DOI": opt.get("top_call_doi_strikes", "Unavailable"),
                "Top Put DOI": opt.get("top_put_doi_strikes", "Unavailable"),
                "PCR overall": opt.get("pcr_overall", "Unavailable"),
                "PCR near ATM": opt.get("pcr_near_atm", "Unavailable"),
                "OI Support Zone": opt.get("oi_support_zone", "Unavailable"),
                "OI Resistance Zone": opt.get("oi_resistance_zone", "Unavailable"),
                "OI shift notes": opt.get("oi_shift_notes", "Unavailable"),
                "IV notes": opt.get("iv_notes", "Unavailable"),
            },
            "News Check": news_section,
            "Strategy": {
                "Educational notice": "Educational analysis only; not investment advice. No guaranteed outcomes.",
                "Primary scenario": {
                    "Bias": bias,
                    "Setup type": setup_type,
                    "Entry trigger": entry or "Wait",
                    "Stop-loss": sl or "NA",
                    "Targets": {"T1": t1 or "NA", "T2": t2 or "NA"},
                    "Rationale": rationale,
                },
                "Alternative scenario": alt,
                "Risk notes": {
                    "Suggested risk per trade (% of capital)": self.cfg["RISK_PER_TRADE_PCT_GUIDELINE"],
                    "Minimum R:R threshold": self.cfg["MIN_RR"],
                },
                "Confidence grade": confidence,
                "Confidence score": confidence_score,
                "volume_confirm_mult_used": round(volume_confirm_mult, 2),
                "volume_confirm_regime_ratio": round(safe_float(vol_mult_meta.get("regime_raw", 1.0), 1.0), 3),
                "volume_confirm_regime_used": round(safe_float(vol_mult_meta.get("regime_used", 1.0), 1.0), 3),
                "factor_volume": f_volume,
                "factor_key_level_break": f_key_level,
                "factor_oi_alignment": f_oi,
                "factor_structure_alignment": f_structure,
                "factor_counter_trend_penalty": f_counter,
                "factor_oi_stale_decisive_block": f_oi_stale_decisive_block,
                "gate_break_confirmed": gate_break_confirmed,
                "gate_volume_strong": gate_volume_strong,
                "gate_key_level_break": gate_key_level_break,
                "gate_oi_alignment": gate_oi_alignment,
                "gate_structure_alignment": gate_structure_alignment,
                "gate_counter_trend_ok": gate_counter_trend_ok,
                "gate_confidence_ok": gate_confidence_ok,
                "gate_oi_freshness_ok": gate_oi_freshness_ok,
                "gate_actionable": gate_actionable,
                "gate_blockers": gate_blockers_text,
                "What would change my mind": wcm,
            },
            "Sources": {
                "Price/volume": "Interactive Brokers API (real-time + historical data)",
                "Options OI": "IB API (option chains - requires market data subscription for OI)",
                "News": news_sources if news_sources else "news_feed.csv (no URLs matched)",
            },
            "_chain_info": chain_info,
            "_oi_source": oi_source,
        }

        return plan

    def _record_failure(self, key: str):
        if not key:
            return
        self._fail_counts[key] = self._fail_counts.get(key, 0) + 1
        if self._fail_counts[key] >= int(self.cfg.get("CB_MAX_FAILS", 3)):
            cooldown = int(self.cfg.get("CB_COOLDOWN_MINUTES", 10))
            self._cooldown_until[key] = now_et() + timedelta(minutes=cooldown)

    def _record_success(self, key: str):
        if not key:
            return
        self._fail_counts[key] = 0
        if key in self._cooldown_until:
            del self._cooldown_until[key]

    def _is_in_cooldown(self, key: str) -> bool:
        until = self._cooldown_until.get(key)
        if not until:
            return False
        if now_et() >= until:
            del self._cooldown_until[key]
            return False
        return True

    def _log_throttled(self, key: str, msg: str, level: str = "warning", interval_sec: float = 30.0):
        now_ts = time.monotonic()
        last_ts = safe_float(self._throttled_log_ts.get(key, 0.0), 0.0) or 0.0
        if (now_ts - last_ts) < max(0.0, float(interval_sec)):
            return
        self._throttled_log_ts[key] = now_ts
        if level == "info":
            logger.info(msg)
        else:
            logger.warning(msg)

    def _is_transient_network_error(self, err: Exception) -> bool:
        """Best-effort classifier for transport/connectivity failures."""
        def _txt(x: Any) -> str:
            try:
                return str(x).strip().lower()
            except Exception:
                return ""

        transient_terms = (
            "not connected",
            "connection aborted",
            "connection reset",
            "connection refused",
            "remote end closed connection",
            "timed out",
            "timeout",
            "temporary failure",
            "temporarily unavailable",
            "max retries exceeded",
            "broken pipe",
            "network",
            "socket",
            "econn",
            "async task timed out",
        )
        transient_names = {
            "connectionerror",
            "timeout",
            "timeouterror",
            "protocolerror",
            "remotedisconnected",
            "readtimeout",
            "connecttimeout",
            "maxretryerror",
            "newconnectionerror",
            "chunkedencodingerror",
            "networkexception",
            "proxyerror",
            "sslerror",
            "cancellederror",
        }

        stack: List[Any] = [err]
        seen: set = set()
        while stack:
            cur = stack.pop()
            if cur is None:
                continue
            cid = id(cur)
            if cid in seen:
                continue
            seen.add(cid)

            if isinstance(cur, (ConnectionError, TimeoutError, OSError, asyncio.TimeoutError, asyncio.CancelledError)):
                return True

            mod = _txt(getattr(type(cur), "__module__", ""))
            name = _txt(getattr(type(cur), "__name__", ""))
            msg = _txt(cur)
            if (
                "requests" in mod
                or "urllib3" in mod
                or "http.client" in mod
                or "socket" in mod
                or "asyncio" in mod
                or name in transient_names
                or any(term in msg for term in transient_terms)
            ):
                return True

            cause = getattr(cur, "__cause__", None)
            context = getattr(cur, "__context__", None)
            if cause is not None:
                stack.append(cause)
            if context is not None:
                stack.append(context)
            for arg in getattr(cur, "args", ()) or ():
                if isinstance(arg, BaseException):
                    stack.append(arg)
                else:
                    arg_txt = _txt(arg)
                    if any(term in arg_txt for term in transient_terms):
                        return True

        return False

    def _handle_plan_failure(self, symbol: str, err: Exception):
        self._record_failure(symbol)
        if self._is_transient_network_error(err):
            self._log_throttled(
                f"plan_net_err:{symbol}",
                (
                    f"Plan generation skipped for {symbol} due to transient API/network issue: "
                    f"{type(err).__name__}: {err}"
                ),
                level="warning",
                interval_sec=20.0,
            )
            return
        logger.error(f"Plan generation error for {symbol}: {err}", exc_info=True)

    def _is_market_hours(self) -> bool:
        """Check if US equity market is open (09:30-16:00 ET, Mon-Fri)."""
        now = now_et()
        if now.weekday() >= 5:  # Saturday / Sunday
            return False
        t = now.time()
        return datetime(now.year, now.month, now.day, 9, 30, tzinfo=ET).time() <= t <= datetime(now.year, now.month, now.day, 16, 0, tzinfo=ET).time()

    def _is_market_close_cutoff_reached(self) -> bool:
        now = now_et()
        if now.weekday() >= 5:
            return True
        close_t = datetime(now.year, now.month, now.day, 16, 0, tzinfo=ET).time()
        return now.time() > close_t

    def _resolve_vm_shutdown_command(self) -> str:
        raw_cmd = safe_str(self.cfg.get("VM_SHUTDOWN_COMMAND", "")).strip()
        if raw_cmd:
            return raw_cmd
        grace_sec = max(0, safe_int(self.cfg.get("VM_SHUTDOWN_GRACE_SECONDS", 30), 30))
        force_apps = bool(self.cfg.get("VM_SHUTDOWN_FORCE_APPS", True))
        if os.name == "nt":
            force_flag = " /f" if force_apps else ""
            return f"shutdown /s /t {grace_sec}{force_flag}"
        grace_min = max(0, int(math.ceil(grace_sec / 60.0)))
        if grace_min <= 0:
            return "shutdown -h now"
        return f"shutdown -h +{grace_min}"

    def _maybe_auto_shutdown_host_after_close(self) -> bool:
        if not bool(self.cfg.get("AUTO_VM_SHUTDOWN_ON_MARKET_CLOSE", False)):
            return False
        if self._vm_shutdown_initiated:
            return True
        if not self._is_market_close_cutoff_reached():
            return False

        retry_sec = max(15.0, float(self.cfg.get("VM_SHUTDOWN_RETRY_SECONDS", 300.0)))
        now_mono = time.monotonic()
        if (now_mono - self._vm_shutdown_last_attempt_ts) < retry_sec:
            return False
        self._vm_shutdown_last_attempt_ts = now_mono

        cmd = self._resolve_vm_shutdown_command()
        if not cmd:
            logger.error("AUTO_VM_SHUTDOWN_ON_MARKET_CLOSE enabled but no VM shutdown command resolved.")
            return False

        try:
            self._persist_signal_state_to_disk(force=True)
            self._persist_oi_snapshot_to_disk(force=True)
        except Exception as e:
            logger.warning(f"State flush before VM shutdown failed: {e}")

        timeout_sec = max(5.0, float(self.cfg.get("VM_SHUTDOWN_CMD_TIMEOUT_SEC", 12.0)))
        logger.critical("Market close cutoff reached. Triggering VM shutdown command: %s", cmd)
        try:
            result = subprocess.run(
                cmd,
                shell=True,
                capture_output=True,
                text=True,
                timeout=timeout_sec,
            )
            if result.returncode != 0:
                stderr = safe_str(result.stderr)
                stdout = safe_str(result.stdout)
                logger.error(
                    "VM shutdown command failed (rc=%s). stdout=%s stderr=%s",
                    result.returncode,
                    stdout[:300] if stdout else "-",
                    stderr[:300] if stderr else "-",
                )
                return False
        except Exception as e:
            logger.error(f"VM shutdown command execution error: {e}")
            return False

        self._vm_shutdown_initiated = True
        self._stop_flag.set()
        return True

    def _resolve_underlying_key(self, raw_symbol: str) -> Optional[str]:
        sym = safe_str(raw_symbol).upper().strip()
        if not sym:
            return None
        for key in self.underlying_symbols:
            key_txt = safe_str(key).upper().strip()
            key_ib = safe_str(ib_symbol(key)).upper().strip()
            if sym == key_txt or sym == key_ib:
                return key
        # Accept near matches from localSymbol prefixes (e.g. "BRK B   260220C00500000")
        for key in sorted(self.underlying_symbols, key=lambda x: len(safe_str(ib_symbol(x))), reverse=True):
            key_txt = safe_str(key).upper().strip()
            key_ib = safe_str(ib_symbol(key)).upper().strip()
            if key_ib and (sym.startswith(key_ib) or key_ib.startswith(sym)):
                return key
            if key_txt and (sym.startswith(key_txt) or key_txt.startswith(sym)):
                return key
        return None

    def _refresh_carry_forward_positions(self, force_log: bool = False, log_manual_map: bool = False):
        manual_raw = self.cfg.get("CARRY_FORWARD_LOTS", {})
        manual_map_in = manual_raw if isinstance(manual_raw, dict) else {}
        manual_map: Dict[str, int] = {}
        for k, v in manual_map_in.items():
            resolved = self._resolve_underlying_key(safe_str(k))
            lots = max(0, safe_int(v, 0))
            if resolved and lots > 0:
                manual_map[resolved] = lots
        if log_manual_map:
            if manual_map:
                manual_txt = ", ".join([f"{k}:{v}" for k, v in sorted(manual_map.items())])
            else:
                manual_txt = "none"
            logger.info(f"Carry-forward manual map (resolved): {manual_txt}")

        broker_long_map: Dict[str, int] = {}
        broker_short_map: Dict[str, int] = {}
        broker_long_contract: Dict[str, Dict[str, Any]] = {}
        broker_short_contract: Dict[str, Dict[str, Any]] = {}

        def _expiry_sort_key(expiry_text: str) -> str:
            txt = safe_str(expiry_text).strip()
            return txt if (txt.isdigit() and len(txt) == 8) else "99999999"

        def _pick_contract(
            dest: Dict[str, Dict[str, Any]],
            sym: str,
            qty_abs: int,
            expiry: str,
            strike: float,
            right: str,
            local_symbol: str,
        ) -> None:
            candidate = {
                "qty": int(max(0, qty_abs)),
                "expiry": safe_str(expiry).strip(),
                "strike": float(strike),
                "right": safe_str(right).upper().strip(),
                "local_symbol": safe_str(local_symbol).strip(),
            }
            prev = dest.get(sym)
            if not isinstance(prev, dict):
                dest[sym] = candidate
                return
            prev_qty = max(0, safe_int(prev.get("qty", 0), 0))
            cand_qty = max(0, safe_int(candidate.get("qty", 0), 0))
            if cand_qty > prev_qty:
                dest[sym] = candidate
                return
            if cand_qty < prev_qty:
                return
            prev_exp = _expiry_sort_key(safe_str(prev.get("expiry", "")))
            cand_exp = _expiry_sort_key(safe_str(candidate.get("expiry", "")))
            if cand_exp < prev_exp:
                dest[sym] = candidate

        if self.ib.isConnected():
            try:
                positions = list(self.ib.positions())
                for p in positions:
                    contract = getattr(p, "contract", None)
                    if contract is None:
                        continue
                    sec_type = safe_str(getattr(contract, "secType", "")).upper().strip()
                    qty = safe_int(getattr(p, "position", 0), 0)
                    right = safe_str(getattr(contract, "right", "")).upper().strip()
                    if sec_type != "OPT" or right not in ("C", "P") or qty == 0:
                        continue
                    c_sym = safe_str(getattr(contract, "symbol", "")).upper().strip()
                    local_sym = safe_str(getattr(contract, "localSymbol", "")).upper().strip()
                    underlying = self._resolve_underlying_key(c_sym) or self._resolve_underlying_key(local_sym)
                    if not underlying:
                        continue
                    expiry = safe_str(getattr(contract, "lastTradeDateOrContractMonth", "")).replace("-", "")
                    strike = safe_float(getattr(contract, "strike", None))
                    lots = max(0, abs(int(qty)))
                    if lots > 0:
                        if qty > 0:
                            broker_long_map[underlying] = broker_long_map.get(underlying, 0) + lots
                            if expiry and (strike is not None):
                                _pick_contract(
                                    broker_long_contract,
                                    underlying,
                                    lots,
                                    expiry,
                                    float(strike),
                                    right,
                                    local_sym,
                                )
                        else:
                            broker_short_map[underlying] = broker_short_map.get(underlying, 0) + lots
                            if expiry and (strike is not None):
                                _pick_contract(
                                    broker_short_contract,
                                    underlying,
                                    lots,
                                    expiry,
                                    float(strike),
                                    right,
                                    local_sym,
                                )
            except Exception as e:
                self._log_throttled(
                    "carry_forward_positions",
                    f"Carry-forward position refresh failed: {type(e).__name__}: {e}",
                    level="warning",
                    interval_sec=60.0,
                )

        merged_long: Dict[str, int] = dict(broker_long_map)
        merged_short: Dict[str, int] = dict(broker_short_map)
        for sym, lots in manual_map.items():
            merged_long[sym] = max(merged_long.get(sym, 0), lots)

        merged: Dict[str, int] = {}
        all_syms = set(merged_long.keys()) | set(merged_short.keys())
        for sym in all_syms:
            total = max(0, merged_long.get(sym, 0)) + max(0, merged_short.get(sym, 0))
            if total > 0:
                merged[sym] = total

        changed = (
            (merged != self.carry_forward_lots)
            or (merged_long != self.carry_forward_long_lots)
            or (merged_short != self.carry_forward_short_lots)
            or (broker_long_contract != self.carry_forward_long_contract)
            or (broker_short_contract != self.carry_forward_short_contract)
        )
        self.carry_forward_lots = merged
        self.carry_forward_long_lots = merged_long
        self.carry_forward_short_lots = merged_short
        self.carry_forward_long_contract = broker_long_contract
        self.carry_forward_short_contract = broker_short_contract
        if changed or force_log:
            if merged:
                parts: List[str] = []
                for sym in sorted(merged.keys()):
                    ll = max(0, safe_int(merged_long.get(sym, 0), 0))
                    ss = max(0, safe_int(merged_short.get(sym, 0), 0))
                    if ll > 0 and ss > 0:
                        parts.append(f"{sym}:L{ll}/S{ss}")
                    elif ll > 0:
                        parts.append(f"{sym}:L{ll}")
                    else:
                        parts.append(f"{sym}:S{ss}")
                txt = ", ".join(parts)
                logger.info(f"Carry-forward positions active: {txt}")
            else:
                logger.info("Carry-forward positions active: none")

    def _idea_loop(self):
        last_bucket = None
        last_state: Dict[str, Tuple[str, int, str, str, str]] = {}  # (bias, conf_score, setup, breakout, breakdown)
        first_run = True  # Generate signals on startup
        disconnect_pause_sec = max(3.0, float(self.cfg.get("DISCONNECT_PAUSE_SECONDS", 10.0)))
        after_hours_pause_sec = max(30.0, float(self.cfg.get("AFTER_HOURS_PAUSE_SECONDS", 60.0)))
        actionable_min_conf = max(0, min(4, safe_int(self.cfg.get("ACTIONABLE_MIN_CONFIDENCE", 3), 3)))
        gate_diag_enabled = bool(self.cfg.get("LOG_GATE_DIAGNOSTICS", True))
        gate_diag_only_blocked = bool(self.cfg.get("LOG_GATE_DIAGNOSTICS_ONLY_BLOCKED", True))
        while not self._stop_flag.is_set():
            # Outside market hours: pause longer to avoid burning CPU/log I/O
            if not self._is_market_hours():
                if self._maybe_auto_shutdown_host_after_close():
                    return
                self._log_throttled(
                    "idea_after_hours_pause",
                    f"Market closed; idea loop paused for {int(after_hours_pause_sec)}s.",
                    level="info",
                    interval_sec=300.0,
                )
                time.sleep(after_hours_pause_sec)
                continue
            if not self.ib.isConnected():
                self._log_throttled(
                    "idea_disconnected_pause",
                    f"IB disconnected; idea loop paused for {int(disconnect_pause_sec)}s.",
                    interval_sec=30.0,
                )
                time.sleep(disconnect_pause_sec)
                continue
            bucket = floor_time(now_et(), self.cfg["CANDLE_MINUTES"])
            if last_bucket is None:
                last_bucket = bucket

            # Generate signals on first run OR when new candle closes
            if first_run or bucket > last_bucket:
                if first_run:
                    logger.info("Generating initial trade ideas from current market state...")
                    first_run = False
                last_bucket = bucket
                self._refresh_carry_forward_positions()
                pass_start = time.monotonic()
                processed = 0
                oi_fetched = 0
                oi_cached = 0
                diag_stats = {
                    "samples": 0,
                    "score_counts": {0: 0, 1: 0, 2: 0, 3: 0, 4: 0},
                    "factor_hits": {
                        "volume": 0,
                        "key_level": 0,
                        "oi": 0,
                        "structure": 0,
                        "counter_penalty": 0,
                        "oi_stale_block": 0,
                    },
                    "gate_actionable": 0,
                    "gate_blocked": 0,
                    "blocker_counts": {},
                }
                summary_sync_interval = max(15.0, float(self.cfg.get("SUMMARY_SYNC_INTERVAL_SEC", 120.0)))
                last_summary_sync_ts = time.monotonic()
                summary_sync_count = 0
                symbols_to_process = [s for s in list(self.underlying_symbols) if not self._is_in_cooldown(s)]
                cooldown_skipped = max(0, len(self.underlying_symbols) - len(symbols_to_process))

                # Optional warmup: disabled by default to avoid startup blocking delays.
                if self.cfg.get("TYPICAL_VOL_PREFETCH_ENABLED", False) and symbols_to_process:
                    prefetch_parallel = int(self.cfg.get("TYPICAL_VOL_PREFETCH_PARALLEL", 2))
                    cache_day = now_et().date().isoformat()
                    with self._cache_lock:
                        prefetch_symbols = [
                            s for s in symbols_to_process
                            if f"{s}:{cache_day}" not in self._typical_intraday_loaded_keys
                        ]
                    if prefetch_symbols:
                        self.run_async(
                            self._prefetch_typical_intraday_cache_async(prefetch_symbols, max_parallel=prefetch_parallel),
                            timeout=max(60.0, float(len(prefetch_symbols) * 5)),
                        )

                workers = max(1, int(self.cfg.get("IDEA_WORKERS", 3)))
                workers = min(workers, max(1, len(symbols_to_process)))
                logger.info(
                    "Idea loop: scanning %d symbols (eligible=%d, cooldown-skipped=%d, workers=%d)...",
                    len(self.underlying_symbols),
                    len(symbols_to_process),
                    cooldown_skipped,
                    workers,
                )
                first_diag_logged = False

                def _refresh_summary(force: bool = False):
                    nonlocal last_summary_sync_ts, summary_sync_count
                    now_ts = time.monotonic()
                    if (not force) and ((now_ts - last_summary_sync_ts) < summary_sync_interval):
                        return
                    try:
                        write_summary_csv(
                            self.cfg["CSV_LIVE_PATH"],
                            self.cfg["CSV_SUMMARY_PATH"],
                            top_n=0,
                            min_confidence=self.cfg.get("SUMMARY_NEW_MIN_CONFIDENCE", 3),
                            include_wait=self.cfg.get("SUMMARY_INCLUDE_WAIT", False),
                        )
                        sync_actions_from_summary(
                            self.cfg["CSV_ACTIONS_PATH"],
                            self.cfg["CSV_SUMMARY_PATH"],
                        )
                        logger.info("Summary CSV updated successfully")
                        last_summary_sync_ts = now_ts
                        summary_sync_count += 1
                    except Exception as e:
                        logger.error(f"Summary update error: {e}", exc_info=True)

                def _build_for_symbol(sym: str):
                    retries = max(1, int(self.cfg.get("PLAN_SYMBOL_RETRIES", 2)))
                    backoff = max(0.1, float(self.cfg.get("PLAN_SYMBOL_RETRY_BACKOFF_SECONDS", 1.0)))
                    last_err: Optional[Exception] = None
                    for attempt in range(1, retries + 1):
                        try:
                            plan = self.build_trade_plan(sym)
                            strategy = plan.get("Strategy", {})
                            primary = strategy.get("Primary scenario", {})
                            bias = safe_str(primary.get("Bias", ""))
                            pcr = plan.get("Options-Derived Levels", {}).get("PCR overall", "Unavailable")
                            oi_source = safe_str(plan.get("_oi_source", "")).lower()
                            bb = plan.get("Breakout/Breakdown Read", plan.get("Breakout\\Breakdown Read", {}))
                            conf_score = int(safe_float(strategy.get("Confidence score"), 0) or 0)
                            setup = safe_str(primary.get("Setup type", ""))
                            brk = safe_str(bb.get("breakout_status", ""))
                            brd = safe_str(bb.get("breakdown_status", ""))
                            state_sig = (safe_str(bias), conf_score, setup, brk, brd)
                            return sym, plan, bias, pcr, state_sig, oi_source
                        except Exception as e:
                            last_err = e
                            if self._is_transient_network_error(e) and attempt < retries:
                                wait = backoff * attempt
                                logger.warning(
                                    "Transient plan error for %s (attempt %d/%d), retrying in %.1fs: %s",
                                    sym,
                                    attempt,
                                    retries,
                                    wait,
                                    type(e).__name__,
                                )
                                time.sleep(wait)
                                continue
                            raise
                    if last_err is not None:
                        raise last_err
                    raise RuntimeError(f"Plan generation failed for {sym}")

                def _record_score_diag(sym: str, plan: Dict[str, Any], bias: str):
                    strategy = plan.get("Strategy", {})
                    conf_score = int(safe_float(strategy.get("Confidence score"), 0) or 0)
                    conf_idx = max(0, min(4, conf_score))
                    diag_stats["samples"] += 1
                    diag_stats["score_counts"][conf_idx] = diag_stats["score_counts"].get(conf_idx, 0) + 1
                    if safe_float(strategy.get("factor_volume"), 0) > 0:
                        diag_stats["factor_hits"]["volume"] += 1
                    if safe_float(strategy.get("factor_key_level_break"), 0) > 0:
                        diag_stats["factor_hits"]["key_level"] += 1
                    if safe_float(strategy.get("factor_oi_alignment"), 0) > 0:
                        diag_stats["factor_hits"]["oi"] += 1
                    if safe_float(strategy.get("factor_structure_alignment"), 0) > 0:
                        diag_stats["factor_hits"]["structure"] += 1
                    if safe_float(strategy.get("factor_counter_trend_penalty"), 0) < 0:
                        diag_stats["factor_hits"]["counter_penalty"] += 1
                    if safe_float(strategy.get("factor_oi_stale_decisive_block"), 0) > 0:
                        diag_stats["factor_hits"]["oi_stale_block"] += 1
                    gate_actionable = 1 if safe_int(strategy.get("gate_actionable", 0), 0) > 0 else 0
                    gate_blockers_text = safe_str(strategy.get("gate_blockers", "NONE"))
                    if gate_actionable:
                        diag_stats["gate_actionable"] += 1
                    else:
                        diag_stats["gate_blocked"] += 1
                    gate_tokens = [
                        tok.strip()
                        for tok in gate_blockers_text.split("|")
                        if tok and tok.strip() and tok.strip().upper() != "NONE"
                    ]
                    for tok in gate_tokens:
                        diag_stats["blocker_counts"][tok] = diag_stats["blocker_counts"].get(tok, 0) + 1
                    bb = plan.get("Breakout/Breakdown Read", plan.get("Breakout\\Breakdown Read", {}))
                    primary = strategy.get("Primary scenario", {})
                    setup = safe_str(primary.get("Setup type", ""))
                    brk = safe_str(bb.get("breakout_status", ""))
                    brd = safe_str(bb.get("breakdown_status", ""))
                    vol_mult_used = safe_float(strategy.get("volume_confirm_mult_used"), safe_float(self.cfg.get("VOLUME_CONFIRM_MULT", 1.5), 1.5)) or 1.5
                    vol_regime_used = safe_float(strategy.get("volume_confirm_regime_used"), 1.0) or 1.0
                    key_level_method = safe_str(bb.get("key_level_method", "-")) or "-"

                    def _fmt_opt(val: Any, digits: int = 2) -> str:
                        fv = safe_float(val, None)
                        if fv is None or (isinstance(fv, float) and (math.isnan(fv) or math.isinf(fv))):
                            return "NA"
                        return f"{fv:.{digits}f}"

                    if gate_diag_enabled and ((not gate_diag_only_blocked) or (not gate_actionable)):
                        logger.info(
                            (
                                "Gate diag [%s]: actionable=%d blockers=%s score=%d/%d bias=%s setup=%s breakout=%s "
                                "breakdown=%s volume=%s vol_mult=%.2f regime=%.2f "
                                "klev=%s(%s) sup=%s res=%s buf=%s up_th=%s dn_th=%s d_up=%s d_dn=%s "
                                "kmodel=%s sup_sc=%s res_sc=%s tol=%s"
                            ),
                            sym,
                            gate_actionable,
                            gate_blockers_text if gate_blockers_text else "NONE",
                            conf_score,
                            actionable_min_conf,
                            safe_str(bias).upper(),
                            setup or "-",
                            brk or "-",
                            brd or "-",
                            safe_str(bb.get("volume_confirmation_status", "")) or "NA",
                            vol_mult_used,
                            vol_regime_used,
                            _fmt_opt(bb.get("key_level_eval_px")),
                            safe_str(bb.get("key_level_eval_source", "-")) or "-",
                            _fmt_opt(bb.get("key_support")),
                            _fmt_opt(bb.get("key_resistance")),
                            _fmt_opt(bb.get("key_level_break_buffer")),
                            _fmt_opt(bb.get("breakout_threshold")),
                            _fmt_opt(bb.get("breakdown_threshold")),
                            _fmt_opt(bb.get("distance_to_breakout_threshold")),
                            _fmt_opt(bb.get("distance_to_breakdown_threshold")),
                            key_level_method,
                            _fmt_opt(bb.get("key_level_support_score"), 3),
                            _fmt_opt(bb.get("key_level_resistance_score"), 3),
                            _fmt_opt(bb.get("key_level_cluster_tolerance")),
                        )

                if workers <= 1:
                    for symbol in symbols_to_process:
                        if self._stop_flag.is_set():
                            break
                        try:
                            sym, plan, bias, pcr, state_sig, oi_source = _build_for_symbol(symbol)
                            _record_score_diag(sym, plan, bias)
                            if oi_source == "fetched":
                                oi_fetched += 1
                            elif oi_source == "cached":
                                oi_cached += 1

                            if not first_diag_logged:
                                snap = self.underlying_snapshot(sym)
                                ticker = self.ticker_data.get(sym)
                                raw_last = getattr(ticker, "last", None) if ticker else None
                                raw_close = getattr(ticker, "close", None) if ticker else None
                                logger.info(
                                    f"First symbol diag [{sym}]: "
                                    f"ticker.last={raw_last}, ticker.close={raw_close}, "
                                    f"snap.ltp={snap.get('ltp')}, "
                                    f"PCR={pcr}, bias={bias}"
                                )
                                first_diag_logged = True

                            if not self.cfg["WRITE_ALL_SIGNALS"] and safe_str(bias) == "Wait" and last_state.get(sym) == state_sig:
                                continue

                            self._write_plan_row(sym, plan)
                            last_state[sym] = state_sig
                            self._record_success(sym)
                            processed += 1
                            _refresh_summary(force=False)
                        except Exception as e:
                            self._handle_plan_failure(symbol, e)
                else:
                    with concurrent.futures.ThreadPoolExecutor(max_workers=workers, thread_name_prefix="idea_worker") as ex:
                        futures = {ex.submit(_build_for_symbol, s): s for s in symbols_to_process}
                        for fut in concurrent.futures.as_completed(futures):
                            if self._stop_flag.is_set():
                                break
                            symbol = futures[fut]
                            try:
                                sym, plan, bias, pcr, state_sig, oi_source = fut.result()
                                _record_score_diag(sym, plan, bias)

                                if oi_source == "fetched":
                                    oi_fetched += 1
                                elif oi_source == "cached":
                                    oi_cached += 1

                                if not first_diag_logged:
                                    snap = self.underlying_snapshot(sym)
                                    ticker = self.ticker_data.get(sym)
                                    raw_last = getattr(ticker, "last", None) if ticker else None
                                    raw_close = getattr(ticker, "close", None) if ticker else None
                                    logger.info(
                                        f"First symbol diag [{sym}]: "
                                        f"ticker.last={raw_last}, ticker.close={raw_close}, "
                                        f"snap.ltp={snap.get('ltp')}, "
                                        f"PCR={pcr}, bias={bias}"
                                    )
                                    first_diag_logged = True

                                if not self.cfg["WRITE_ALL_SIGNALS"] and safe_str(bias) == "Wait" and last_state.get(sym) == state_sig:
                                    continue

                                self._write_plan_row(sym, plan)
                                last_state[sym] = state_sig
                                self._record_success(sym)
                                processed += 1
                                _refresh_summary(force=False)
                            except Exception as e:
                                self._handle_plan_failure(symbol, e)
                pass_elapsed = time.monotonic() - pass_start
                built = diag_stats["samples"]
                build_rate = (built / pass_elapsed) if pass_elapsed > 0 else 0.0
                write_rate = (processed / pass_elapsed) if pass_elapsed > 0 else 0.0
                avg_sec_per_symbol = (pass_elapsed / built) if built > 0 else 0.0
                not_scanned = max(0, len(symbols_to_process) - built)
                if diag_stats["samples"] > 0:
                    dist = diag_stats["score_counts"]
                    fh = diag_stats["factor_hits"]
                    actionable = dist.get(3, 0) + dist.get(4, 0)
                    logger.info(
                        "Confidence distribution: n=%d | 0=%d 1=%d 2=%d 3=%d 4=%d | >=3=%d (%.1f%%) | "
                        "factors: vol=%d key=%d oi=%d struct=%d counter=%d stale_oi_block=%d",
                        diag_stats["samples"],
                        dist.get(0, 0),
                        dist.get(1, 0),
                        dist.get(2, 0),
                        dist.get(3, 0),
                        dist.get(4, 0),
                        actionable,
                        (100.0 * actionable / max(diag_stats["samples"], 1)),
                        fh["volume"],
                        fh["key_level"],
                        fh["oi"],
                        fh["structure"],
                        fh["counter_penalty"],
                        fh["oi_stale_block"],
                    )
                    logger.info(
                        "Gate diagnostics: actionable=%d blocked=%d",
                        diag_stats["gate_actionable"],
                        diag_stats["gate_blocked"],
                    )
                    blocker_counts = diag_stats.get("blocker_counts", {})
                    if blocker_counts:
                        blocker_items = sorted(
                            blocker_counts.items(),
                            key=lambda kv: (-safe_int(kv[1], 0), safe_str(kv[0])),
                        )
                        blocker_text = " | ".join([f"{k}={v}" for k, v in blocker_items])
                        logger.info("Gate blocker distribution: %s", blocker_text)
                logger.info(
                    "Idea loop performance: elapsed=%.2fs | built=%d/%d eligible (%d total, %d cooldown-skipped, %d not-scanned) | "
                    "rows=%d | rate=%.2f sym/s, %.2f rows/s | avg=%.2fs/symbol | OI fetched=%d, OI cached=%d | summary-syncs=%d",
                    pass_elapsed,
                    built,
                    len(symbols_to_process),
                    len(self.underlying_symbols),
                    cooldown_skipped,
                    not_scanned,
                    processed,
                    build_rate,
                    write_rate,
                    avg_sec_per_symbol,
                    oi_fetched,
                    oi_cached,
                    summary_sync_count,
                )
                if processed > 0:
                    _refresh_summary(force=True)
                else:
                    self._log_throttled(
                        "summary_skip_no_rows",
                        "Skipping summary refresh: no fresh live rows in this pass.",
                        level="info",
                        interval_sec=120.0,
                    )
                self._persist_oi_snapshot_to_disk(force=True)
                self._persist_signal_state_to_disk(force=True)
            time.sleep(3)

    def _write_plan_row(self, symbol: str, plan: Dict[str, Any]):
        """Write trade plan to CSV"""
        ts = now_et()
        
        # Reuse chain_info already fetched during build_trade_plan
        chain_info = plan.get("_chain_info", {})
        
        atm = chain_info.get("atm_strike", "")
        expiry = chain_info.get("expiry", "")
        
        bias = plan["Strategy"]["Primary scenario"]["Bias"]
        setup_type = safe_str(plan["Strategy"]["Primary scenario"].get("Setup type", ""))
        
        # Create option symbol
        chosen_ts = ""
        right = ""
        if bias == "Buy":
            right = "C"
        elif bias == "Sell":
            right = "P"
        elif setup_type.startswith("Breakdown"):
            right = "P"
        else:
            right = "C"

        if chain_info and expiry and safe_str(atm):
            chosen_ts = f"{symbol} {expiry} {atm} {right}"
        if not chosen_ts:
            chosen_ts = safe_str(symbol)
        
        contract_multiplier = self.uni.contract_multiplier(symbol)
        
        snap = plan["Snapshot (ET timestamp)"]
        bb = plan["Breakout/Breakdown Read"]
        opt = plan["Options-Derived Levels"]
        
        row_id = f"{symbol}-{int(time.time())}"
        carry_lots = max(0, safe_int(self.carry_forward_lots.get(symbol, 0), 0))
        carry_long_lots = max(0, safe_int(self.carry_forward_long_lots.get(symbol, 0), 0))
        carry_short_lots = max(0, safe_int(self.carry_forward_short_lots.get(symbol, 0), 0))
        is_carry = carry_lots > 0
        is_carry_sell_exit_signal = is_carry and (carry_long_lots > 0) and safe_str(bias).upper().strip() == "SELL"
        is_carry_buy_exit_signal = is_carry and (carry_short_lots > 0) and safe_str(bias).upper().strip() == "BUY"
        row_status = "NEW" if ((not is_carry) or is_carry_sell_exit_signal or is_carry_buy_exit_signal) else "OPEN_CARRY"
        if is_carry_sell_exit_signal:
            carry_note = (
                f"Carry-forward LONG position detected: {carry_long_lots} lots (overnight/open). "
                "SELL signal eligible for approval-gated exit."
            )
        elif is_carry_buy_exit_signal:
            carry_note = (
                f"Carry-forward SHORT position detected: {carry_short_lots} lots (overnight/open). "
                "BUY signal eligible for approval-gated exit."
            )
        elif is_carry:
            if carry_long_lots > 0 and carry_short_lots > 0:
                side_note = f"LONG={carry_long_lots}, SHORT={carry_short_lots}"
            elif carry_long_lots > 0:
                side_note = f"LONG={carry_long_lots}"
            else:
                side_note = f"SHORT={carry_short_lots}"
            carry_note = (
                f"Carry-forward position detected: {carry_lots} lots ({side_note}, overnight/open). "
                "No fresh entry approval requested for this symbol."
            )
        else:
            carry_note = ""

        carry_contract = None
        if is_carry_sell_exit_signal:
            carry_contract = self.carry_forward_long_contract.get(symbol)
        elif is_carry_buy_exit_signal:
            carry_contract = self.carry_forward_short_contract.get(symbol)
        elif row_status == "OPEN_CARRY":
            # For passive carry tracking rows, prefer representative long contract,
            # else short contract, so summary/actions reflect the held leg.
            carry_contract = (
                self.carry_forward_long_contract.get(symbol)
                or self.carry_forward_short_contract.get(symbol)
            )

        if isinstance(carry_contract, dict):
            cc_expiry = safe_str(carry_contract.get("expiry", "")).strip()
            cc_strike = safe_float(carry_contract.get("strike", None))
            cc_right = safe_str(carry_contract.get("right", "")).upper().strip()
            if cc_expiry:
                expiry = cc_expiry
            if cc_strike is not None:
                atm = float(cc_strike)
            if cc_right in ("C", "P"):
                right = cc_right
            if expiry and safe_str(atm):
                chosen_ts = f"{symbol} {expiry} {atm} {right}"

        # Default to buying the selected option leg; avoids accidental naked short options.
        txn = "BUY" if right in ("C", "P") else ""
        order_payload = {
            "symbol": symbol,
            "expiry": expiry,
            "strike": atm,
            "right": right,
            "action": txn,
            "orderType": "LMT",
            "totalQuantity": 0,
        }
        
        row = {
            "row_id": row_id,
            "timestamp_et": ts.strftime("%Y-%m-%d %H:%M:%S"),
            "session_context": et_session_context(ts),
            "underlying_type": "ETF" if symbol in ["SPY", "QQQ", "IWM"] else "STOCK",
            "symbol": symbol,
            "expiry": expiry,
            "strike": atm,
            "right": right,
            "tradingsymbol": chosen_ts,
            "contract_multiplier": contract_multiplier,
            "status": row_status,
            "approve": "NO",
            "ltp": snap.get("LTP"),
            "pct_change": snap.get("% change"),
            "day_high": snap.get("day_high"),
            "day_low": snap.get("day_low"),
            "prev_close": snap.get("prev_close"),
            "volume": snap.get("volume"),
            "vol_vs_20d_avg": snap.get("vol_vs_20d_avg"),
            "vol_vs_time_of_day": snap.get("vol_vs_time_of_day"),
            "vwap_relation": snap.get("VWAP relation"),
            "day_range": snap.get("day_range"),
            "key_support": bb.get("key_support"),
            "key_resistance": bb.get("key_resistance"),
            "opening_range_high": bb.get("opening_range_high"),
            "opening_range_low": bb.get("opening_range_low"),
            "prior_day_high": bb.get("prior_day_high"),
            "prior_day_low": bb.get("prior_day_low"),
            "swing_high": bb.get("swing_high"),
            "swing_low": bb.get("swing_low"),
            "structure": csv_safe_text(bb.get("structure", "")),
            "volume_confirmation": csv_safe_text(bb.get("volume_confirmation_status", "NA")),
            "breakout_status": csv_safe_text(bb.get("breakout_status", "None")),
            "breakdown_status": csv_safe_text(bb.get("breakdown_status", "None")),
            "expiry_used": opt.get("expiry_used", ""),
            "top_call_oi_strikes": opt.get("Top Call OI (resistance zones)", ""),
            "top_put_oi_strikes": opt.get("Top Put OI (support zones)", ""),
            "top_call_doi_strikes": opt.get("Top Call DOI", ""),
            "top_put_doi_strikes": opt.get("Top Put DOI", ""),
            "pcr_overall": opt.get("PCR overall", ""),
            "pcr_near_atm": opt.get("PCR near ATM", ""),
            "oi_support_zone": opt.get("OI Support Zone", ""),
            "oi_resistance_zone": opt.get("OI Resistance Zone", ""),
            "oi_shift_notes": opt.get("OI shift notes", ""),
            "iv_notes": opt.get("IV notes", ""),
            "news_bullets": csv_safe_text(plan["News Check"]["bullets"]),
            "news_sources": csv_safe_text(plan["News Check"]["sources"]),
            "bias": bias,
            "setup_type": plan["Strategy"]["Primary scenario"]["Setup type"],
            "primary_entry": plan["Strategy"]["Primary scenario"]["Entry trigger"],
            "primary_sl": plan["Strategy"]["Primary scenario"]["Stop-loss"],
            "primary_t1": plan["Strategy"]["Primary scenario"]["Targets"]["T1"],
            "primary_t2": plan["Strategy"]["Primary scenario"]["Targets"]["T2"],
            "alt_scenario": plan["Strategy"]["Alternative scenario"],
            "risk_per_trade_pct": self.cfg["RISK_PER_TRADE_PCT_GUIDELINE"],
            "min_rr": self.cfg["MIN_RR"],
            "confidence": plan["Strategy"]["Confidence grade"],
            "confidence_score": plan["Strategy"]["Confidence score"],
            **{fc: plan["Strategy"][fc] for fc in FACTOR_COLUMNS},
            **{gc: plan["Strategy"].get(gc, "") for gc in GATE_COLUMNS},
            "change_mind_checklist": csv_safe_text(plan["Strategy"]["What would change my mind"]),
            "order_payload_json": json.dumps(order_payload, default=str),
            "execution_result": carry_note,
            "plan_json": (
                json.dumps({k: v for k, v in plan.items() if not k.startswith("_")}, default=str)
                if self.cfg.get("INCLUDE_PLAN_JSON", True)
                else ""
            ),
        }
        
        append_live_row(self.cfg["CSV_LIVE_PATH"], row)
    
    def _telegram_loop(self):
        while not self._stop_flag.is_set():
            try:
                self.tg_bridge.notify_new_requests()
                self.tg_bridge.poll_updates()
            except Exception as e:
                logger.error(f"Telegram loop error: {e}", exc_info=True)
            finally:
                time.sleep(self.tg_bridge.poll_seconds if self.tg_bridge.enabled else 2.0)

    async def _execute_sell_exit_for_symbol_async(self, symbol: str) -> Dict[str, Any]:
        """Close open long call option positions for symbol (approved SELL exit)."""
        if not await self._ensure_connected(attempts=1, delay_sec=1.0):
            return {"ok": False, "error": "IB not connected; sell-exit skipped", "symbol": symbol}
        target = safe_str(ib_symbol(symbol)).upper().strip()
        if not target:
            return {"ok": False, "error": "invalid_symbol", "symbol": symbol}

        try:
            positions = list(self.ib.positions())
        except Exception as e:
            return {"ok": False, "error": f"positions_fetch_failed: {e}", "symbol": symbol}

        exit_payloads: List[Dict[str, Any]] = []
        for p in positions:
            contract = getattr(p, "contract", None)
            if contract is None:
                continue
            sec_type = safe_str(getattr(contract, "secType", "")).upper().strip()
            right = safe_str(getattr(contract, "right", "")).upper().strip()
            c_sym = safe_str(getattr(contract, "symbol", "")).upper().strip()
            local_sym = safe_str(getattr(contract, "localSymbol", "")).upper().strip()
            qty = safe_int(getattr(p, "position", 0), 0)
            if sec_type != "OPT" or right != "C" or qty <= 0:
                continue
            if c_sym != target and (not local_sym.startswith(target)):
                continue
            expiry = safe_str(getattr(contract, "lastTradeDateOrContractMonth", "")).replace("-", "")
            strike = safe_float(getattr(contract, "strike", None))
            if (not expiry) or (strike is None):
                continue
            exit_payloads.append(
                {
                    "symbol": symbol,
                    "expiry": expiry,
                    "strike": float(strike),
                    "right": "C",
                    "action": "SELL",
                    "orderType": "LMT",
                    "totalQuantity": int(qty),
                }
            )

        if not exit_payloads:
            return {"ok": False, "error": "no_open_long_call_positions_for_symbol", "symbol": symbol}

        orders: List[Dict[str, Any]] = []
        all_ok = True
        for payload in exit_payloads:
            res = await self._execute_order_async(payload)
            res = res if isinstance(res, dict) else {"ok": False, "error": "sell_exit_order_failed_no_response"}
            ok = bool(res.get("ok"))
            all_ok = all_ok and ok
            orders.append(
                {
                    "symbol": safe_str(payload.get("symbol", "")),
                    "expiry": safe_str(payload.get("expiry", "")),
                    "strike": payload.get("strike"),
                    "right": safe_str(payload.get("right", "")),
                    "qty": safe_int(payload.get("totalQuantity", 0), 0),
                    "ok": ok,
                    "order_id": safe_str(res.get("order_id", "")),
                    "status": safe_str(res.get("status", "")),
                    "error": safe_str(res.get("error", "")),
                }
            )
        return {"ok": all_ok, "symbol": symbol, "closed_count": len(orders), "orders": orders}

    def _execute_sell_exit_for_symbol(self, symbol: str) -> Dict[str, Any]:
        out = self.run_async(self._execute_sell_exit_for_symbol_async(symbol))
        if isinstance(out, dict):
            return out
        return {"ok": False, "error": "sell_exit_async_failed_or_timed_out", "symbol": symbol}

    async def _execute_buy_exit_for_symbol_async(self, symbol: str) -> Dict[str, Any]:
        """Close open short option positions for symbol (approved BUY exit)."""
        if not await self._ensure_connected(attempts=1, delay_sec=1.0):
            return {"ok": False, "error": "IB not connected; buy-exit skipped", "symbol": symbol}
        target = safe_str(ib_symbol(symbol)).upper().strip()
        if not target:
            return {"ok": False, "error": "invalid_symbol", "symbol": symbol}

        try:
            positions = list(self.ib.positions())
        except Exception as e:
            return {"ok": False, "error": f"positions_fetch_failed: {e}", "symbol": symbol}

        exit_payloads: List[Dict[str, Any]] = []
        for p in positions:
            contract = getattr(p, "contract", None)
            if contract is None:
                continue
            sec_type = safe_str(getattr(contract, "secType", "")).upper().strip()
            right = safe_str(getattr(contract, "right", "")).upper().strip()
            c_sym = safe_str(getattr(contract, "symbol", "")).upper().strip()
            local_sym = safe_str(getattr(contract, "localSymbol", "")).upper().strip()
            qty = safe_int(getattr(p, "position", 0), 0)
            if sec_type != "OPT" or right not in ("C", "P") or qty >= 0:
                continue
            if c_sym != target and (not local_sym.startswith(target)):
                continue
            expiry = safe_str(getattr(contract, "lastTradeDateOrContractMonth", "")).replace("-", "")
            strike = safe_float(getattr(contract, "strike", None))
            if (not expiry) or (strike is None):
                continue
            exit_payloads.append(
                {
                    "symbol": symbol,
                    "expiry": expiry,
                    "strike": float(strike),
                    "right": right,
                    "action": "BUY",
                    "orderType": "LMT",
                    "totalQuantity": abs(int(qty)),
                }
            )

        if not exit_payloads:
            return {"ok": False, "error": "no_open_short_option_positions_for_symbol", "symbol": symbol}

        orders: List[Dict[str, Any]] = []
        all_ok = True
        for payload in exit_payloads:
            res = await self._execute_order_async(payload)
            res = res if isinstance(res, dict) else {"ok": False, "error": "buy_exit_order_failed_no_response"}
            ok = bool(res.get("ok"))
            all_ok = all_ok and ok
            orders.append(
                {
                    "symbol": safe_str(payload.get("symbol", "")),
                    "expiry": safe_str(payload.get("expiry", "")),
                    "strike": payload.get("strike"),
                    "right": safe_str(payload.get("right", "")),
                    "qty": safe_int(payload.get("totalQuantity", 0), 0),
                    "ok": ok,
                    "order_id": safe_str(res.get("order_id", "")),
                    "status": safe_str(res.get("status", "")),
                    "error": safe_str(res.get("error", "")),
                }
            )
        return {"ok": all_ok, "symbol": symbol, "closed_count": len(orders), "orders": orders}

    def _execute_buy_exit_for_symbol(self, symbol: str) -> Dict[str, Any]:
        out = self.run_async(self._execute_buy_exit_for_symbol_async(symbol))
        if isinstance(out, dict):
            return out
        return {"ok": False, "error": "buy_exit_async_failed_or_timed_out", "symbol": symbol}

    def _approval_loop(self):
        """Check for approved trades and execute"""
        while not self._stop_flag.is_set():
            pending_updates: Dict[str, Dict[str, Any]] = {}
            needs_summary_sync = False
            try:
                if not self.ib.isConnected():
                    self._log_throttled(
                        "approval_disconnected_pause",
                        "IB disconnected; approval execution paused until reconnect.",
                        interval_sec=30.0,
                    )
                    continue
                live_df = load_live(self.cfg["CSV_LIVE_PATH"])
                sync_actions_from_summary(self.cfg["CSV_ACTIONS_PATH"], self.cfg["CSV_SUMMARY_PATH"])
                actions = load_actions_map(self.cfg["CSV_ACTIONS_PATH"])
                actionable_min_conf = max(0, min(4, safe_int(self.cfg.get("ACTIONABLE_MIN_CONFIDENCE", 3), 3)))
                
                if live_df.empty or "status" not in live_df.columns or "symbol" not in live_df.columns:
                    continue
                
                live_df["status"] = live_df["status"].astype(str).str.upper()
                live_df["symbol"] = live_df["symbol"].fillna("").astype(str).str.upper().str.strip()
                live_df["timestamp_et_dt"] = pd.to_datetime(live_df.get("timestamp_et", ""), errors="coerce")
                all_rows_df = (
                    live_df[live_df["symbol"] != ""]
                    .sort_values(by=["timestamp_et_dt", "timestamp_et"], ascending=[False, False])
                ).copy()
                if all_rows_df.empty:
                    continue
                all_rows_df["request_id_live"] = all_rows_df.apply(
                    lambda rr: build_request_id(rr.get("symbol", ""), rr.get("tradingsymbol", "")),
                    axis=1,
                )
                candidates_df = (
                    live_df[(live_df["status"] == "NEW") & (live_df["symbol"] != "")]
                    .sort_values(by=["timestamp_et_dt", "timestamp_et"], ascending=[False, False])
                ).copy()
                candidates_df["request_id_live"] = candidates_df.apply(
                    lambda rr: build_request_id(rr.get("symbol", ""), rr.get("tradingsymbol", "")),
                    axis=1,
                )
                rows_by_symbol: Dict[str, List[Dict[str, Any]]] = {}
                for rec in candidates_df.to_dict(orient="records"):
                    sym_key = safe_str(rec.get("symbol", "")).upper().strip()
                    if sym_key:
                        rows_by_symbol.setdefault(sym_key, []).append(rec)
                all_rows_by_symbol: Dict[str, List[Dict[str, Any]]] = {}
                for rec in all_rows_df.to_dict(orient="records"):
                    sym_key = safe_str(rec.get("symbol", "")).upper().strip()
                    if sym_key:
                        all_rows_by_symbol.setdefault(sym_key, []).append(rec)

                def queue_update(row_id: str, updates: Dict[str, Any]):
                    pending_updates.setdefault(row_id, {}).update(updates)
                
                approved_actions = [
                    (safe_str(sym).upper().strip(), a)
                    for sym, a in actions.items()
                    if safe_str(a.get("approve", "")).upper() == "YES"
                ]

                for symbol, a in approved_actions:
                    if not symbol:
                        continue
                    symbol_rows_all = all_rows_by_symbol.get(symbol, [])
                    if not symbol_rows_all:
                        continue
                    symbol_rows = rows_by_symbol.get(symbol, [])
                    req_id_action = safe_str(a.get("request_id", ""))
                    r: Optional[Dict[str, Any]] = None
                    if req_id_action and symbol_rows:
                        for cand in symbol_rows:
                            if safe_str(cand.get("request_id_live", "")) == req_id_action:
                                r = cand
                                break
                    if r is None:
                        latest = symbol_rows_all[0]
                        latest_req = safe_str(latest.get("request_id_live", ""))
                        if req_id_action and latest_req and latest_req != req_id_action:
                            now_text = now_et().strftime("%Y-%m-%d %H:%M:%S")
                            msg = (
                                f"Approval stale. Approved request_id={req_id_action} "
                                f"but latest live request_id={latest_req}. Re-approve current setup."
                            )
                            update_action_row(
                                self.cfg["CSV_ACTIONS_PATH"],
                                symbol,
                                {
                                    "approve": "NO",
                                    "approval_source": "auto_stale_request",
                                    "approval_updated_at": now_text,
                                    "execution_status": "ERROR",
                                    "execution_result": msg,
                                },
                            )
                            try:
                                self.tg_bridge.notify_execution(
                                    symbol=symbol,
                                    status="ERROR",
                                    result_text=msg,
                                    row_id=safe_str(latest.get("row_id", "")),
                                    order_id="",
                                    tradingsymbol=safe_str(latest.get("tradingsymbol", "")),
                                )
                            except Exception:
                                pass
                            continue
                        # No stale mismatch, but nothing executable yet for this approved request.
                        if req_id_action:
                            continue
                        if symbol_rows:
                            r = symbol_rows[0]
                        else:
                            continue

                    row_id = safe_str(r.get("row_id", ""))
                    if not row_id:
                        continue
                    
                    def _mark_action(
                        exec_status: str,
                        exec_result: str,
                        order_id: str = "",
                        action_status: str = "",
                    ):
                        nonlocal needs_summary_sync
                        update_action_row(
                            self.cfg["CSV_ACTIONS_PATH"],
                            symbol,
                            {
                                # Consume approval after one execution attempt to avoid repeat firing
                                # on subsequent polling cycles for the same request.
                                "approve": "NO",
                                "lots": 0,
                                "status": safe_str(action_status) or ("CLOSED" if exec_status == "ORDER_PLACED" else "ERROR"),
                                "execution_status": exec_status,
                                "execution_result": exec_result,
                                "executed_at": now_et().strftime("%Y-%m-%d %H:%M:%S"),
                                "executed_row_id": row_id,
                                "executed_order_id": order_id,
                            },
                        )
                        needs_summary_sync = True
                        try:
                            self.tg_bridge.notify_execution(
                                symbol=symbol,
                                status=exec_status,
                                result_text=exec_result,
                                row_id=row_id,
                                order_id=order_id,
                                tradingsymbol=safe_str(r.get("tradingsymbol", "")),
                            )
                        except Exception as e:
                            logger.warning(f"Telegram execution notify error for {symbol}: {e}")

                    row_bias = safe_str(r.get("bias", "")).upper().strip()
                    row_conf_score = safe_int(r.get("confidence_score", 0), 0)
                    row_oi_stale_block = safe_int(r.get("factor_oi_stale_decisive_block", 0), 0) > 0
                    if row_oi_stale_block:
                        msg = "Execution blocked: stale OI was decisive for actionable confidence."
                        queue_update(row_id, {"status": "ERROR", "execution_result": msg})
                        _mark_action("ERROR", msg, action_status="ERROR")
                        continue
                    if row_bias == "SELL":
                        if row_conf_score < actionable_min_conf:
                            msg = f"SELL execution blocked: confidence_score must be >= {actionable_min_conf}."
                            queue_update(row_id, {"status": "ERROR", "execution_result": msg})
                            _mark_action("ERROR", msg, action_status="ERROR")
                            continue
                        if not self._is_market_hours():
                            msg = "SELL execution blocked: market is closed."
                            queue_update(row_id, {"status": "ERROR", "execution_result": msg})
                            _mark_action("ERROR", msg, action_status="ERROR")
                            continue

                        exit_result = self._execute_sell_exit_for_symbol(symbol)
                        exec_ok = bool(exit_result.get("ok"))
                        exec_result = json.dumps(exit_result, default=str)
                        orders = exit_result.get("orders", []) if isinstance(exit_result, dict) else []
                        first_order_id = ""
                        if isinstance(orders, list) and orders:
                            first_order_id = safe_str((orders[0] or {}).get("order_id", ""))
                        queue_update(
                            row_id,
                            {
                                "status": "CLOSED" if exec_ok else "ERROR",
                                "execution_result": exec_result,
                            },
                        )
                        _mark_action(
                            "ORDER_PLACED" if exec_ok else "ERROR",
                            exec_result,
                            first_order_id,
                            action_status=("CLOSED" if exec_ok else "ERROR"),
                        )
                        if exec_ok:
                            logger.info(
                                "Approved SELL executed for %s (confidence_score=%d): %s",
                                symbol,
                                row_conf_score,
                                exec_result,
                            )
                        continue

                    is_short_carry_exit = (
                        row_bias == "BUY"
                        and (
                            max(0, safe_int(self.carry_forward_short_lots.get(symbol, 0), 0)) > 0
                            or ("BUY signal eligible for approval-gated exit" in safe_str(r.get("execution_result", "")))
                        )
                    )
                    if is_short_carry_exit:
                        if row_conf_score < actionable_min_conf:
                            msg = f"BUY execution blocked: confidence_score must be >= {actionable_min_conf}."
                            queue_update(row_id, {"status": "ERROR", "execution_result": msg})
                            _mark_action("ERROR", msg, action_status="ERROR")
                            continue
                        if not self._is_market_hours():
                            msg = "BUY execution blocked: market is closed."
                            queue_update(row_id, {"status": "ERROR", "execution_result": msg})
                            _mark_action("ERROR", msg, action_status="ERROR")
                            continue

                        exit_result = self._execute_buy_exit_for_symbol(symbol)
                        exec_ok = bool(exit_result.get("ok"))
                        exec_result = json.dumps(exit_result, default=str)
                        orders = exit_result.get("orders", []) if isinstance(exit_result, dict) else []
                        first_order_id = ""
                        if isinstance(orders, list) and orders:
                            first_order_id = safe_str((orders[0] or {}).get("order_id", ""))
                        queue_update(
                            row_id,
                            {
                                "status": "CLOSED" if exec_ok else "ERROR",
                                "execution_result": exec_result,
                            },
                        )
                        _mark_action(
                            "ORDER_PLACED" if exec_ok else "ERROR",
                            exec_result,
                            first_order_id,
                            action_status=("CLOSED" if exec_ok else "ERROR"),
                        )
                        if exec_ok:
                            logger.info(
                                "Approved BUY short-exit executed for %s (confidence_score=%d): %s",
                                symbol,
                                row_conf_score,
                                exec_result,
                            )
                        continue
                    
                    lots = safe_int(a.get("lots", 0), 0)
                    if lots <= 0:
                        msg = "Lots must be >0"
                        queue_update(row_id, {
                            "status": "ERROR",
                            "execution_result": msg
                        })
                        _mark_action("ERROR", msg, action_status="ERROR")
                        continue

                    try:
                        payload = json.loads(r.get("order_payload_json", "{}"))
                    except (json.JSONDecodeError, TypeError) as e:
                        msg = f"Invalid order_payload_json: {e}"
                        queue_update(row_id, {
                            "status": "ERROR",
                            "execution_result": msg
                        })
                        _mark_action("ERROR", msg, action_status="ERROR")
                        continue
                    if not isinstance(payload, dict):
                        msg = "Invalid order_payload_json: not an object"
                        queue_update(row_id, {
                            "status": "ERROR",
                            "execution_result": msg
                        })
                        _mark_action("ERROR", msg, action_status="ERROR")
                        continue
                    payload["totalQuantity"] = lots
                    
                    action = safe_str(payload.get("action", "")).upper().strip()
                    if action not in ("BUY", "SELL"):
                        # Backward compatibility: infer BUY for option legs when old rows have blank action.
                        row_right = safe_str(r.get("right", "")).upper().strip()
                        if row_right in ("C", "P"):
                            action = "BUY"
                        if action:
                            payload["action"] = action
                        else:
                            msg = "Missing/invalid action"
                            queue_update(row_id, {
                                "status": "ERROR",
                                "execution_result": msg
                            })
                            _mark_action("ERROR", msg, action_status="ERROR")
                            continue
                    row_right = safe_str(r.get("right", "")).upper().strip()
                    if row_right in ("C", "P") and action == "SELL":
                        logger.warning(
                            f"Converting legacy SELL option action to BUY for {symbol} "
                            f"{safe_str(r.get('tradingsymbol', ''))}"
                        )
                        action = "BUY"
                        payload["action"] = action
                    
                    result = self._execute_order(payload)
                    result = result if isinstance(result, dict) else {"ok": False, "error": "order_execution_failed_no_response"}
                    exec_ok = bool(result.get("ok"))
                    exec_status = "EXECUTED" if exec_ok else "ERROR"
                    exec_result = json.dumps(result, default=str)
                    
                    queue_update(row_id, {
                        "status": "CLOSED" if exec_ok else "ERROR",
                        "execution_result": exec_result,
                        "order_payload_json": json.dumps(payload, default=str)
                    })
                    action_exec_status = "ORDER_PLACED" if exec_ok else "ERROR"
                    _mark_action(
                        action_exec_status,
                        exec_result,
                        safe_str(result.get("order_id", "")),
                        action_status=("CLOSED" if exec_ok else "ERROR"),
                    )
            except Exception as e:
                logger.error(f"Approval loop error: {e}", exc_info=True)
            finally:
                if pending_updates:
                    try:
                        bulk_update_live_rows(self.cfg["CSV_LIVE_PATH"], pending_updates)
                    except Exception as e:
                        logger.error(f"Approval loop bulk update error: {e}", exc_info=True)
                if needs_summary_sync:
                    try:
                        write_summary_csv(
                            self.cfg["CSV_LIVE_PATH"],
                            self.cfg["CSV_SUMMARY_PATH"],
                            top_n=0,
                            min_confidence=self.cfg.get("SUMMARY_NEW_MIN_CONFIDENCE", 3),
                            include_wait=self.cfg.get("SUMMARY_INCLUDE_WAIT", False),
                        )
                        sync_actions_from_summary(self.cfg["CSV_ACTIONS_PATH"], self.cfg["CSV_SUMMARY_PATH"])
                    except Exception as e:
                        logger.error(f"Approval loop summary sync error: {e}", exc_info=True)
                time.sleep(self.cfg["CSV_POLL_SECONDS"])
    
    
    async def _execute_order_async(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Execute order via IB API (runs on loop thread)."""
        try:
            if not await self._ensure_connected(attempts=1, delay_sec=1.0):
                return {"ok": False, "error": "IB not connected; order skipped", "payload": payload}
            symbol = payload["symbol"]
            expiry = payload["expiry"]
            strike = payload["strike"]
            right = payload["right"]

            contract = Option(ib_symbol(symbol), expiry, strike, right, 'SMART')

            contracts = await self.ib.qualifyContractsAsync(contract)
            if not contracts:
                return {"ok": False, "error": "Contract qualification failed", "payload": payload}

            qualified_contract = contracts[0]

            action = payload["action"]
            quantity = int(payload["totalQuantity"])
            tickers = await self.ib.reqTickersAsync(qualified_contract)
            ticker = tickers[0] if tickers else None
            bid = safe_float(getattr(ticker, "bid", None), None)
            ask = safe_float(getattr(ticker, "ask", None), None)
            last = safe_float(getattr(ticker, "last", None), None)
            close = safe_float(getattr(ticker, "close", None), None)

            limit_price: Optional[float] = None
            if bid is not None and ask is not None and bid > 0 and ask > 0 and ask >= bid:
                limit_price = round((bid + ask) / 2.0, 2)
            elif action == "BUY" and ask is not None and ask > 0:
                limit_price = round(ask, 2)
            elif action == "SELL" and bid is not None and bid > 0:
                limit_price = round(bid, 2)
            elif last is not None and last > 0:
                limit_price = round(last, 2)
            elif close is not None and close > 0:
                limit_price = round(close, 2)
            else:
                return {
                    "ok": False,
                    "error": "Unable to compute limit price (bid/ask/last unavailable)",
                    "payload": payload,
                }

            order = LimitOrder(action, quantity, limit_price)
            trade = self.ib.placeOrder(qualified_contract, order)

            timeout = 30.0
            start_time = time.time()
            while time.time() - start_time < timeout:
                status = getattr(trade.orderStatus, "status", "")
                if status in ['Filled', 'Cancelled', 'Inactive']:
                    break
                await asyncio.sleep(0.5)

            status = getattr(trade.orderStatus, "status", "")
            return {
                "ok": status == 'Filled',
                "order_id": getattr(trade.order, "orderId", None),
                "status": status,
                "limit_price": limit_price,
                "bid": bid,
                "ask": ask,
                "filled_qty": getattr(trade.orderStatus, "filled", None),
                "avg_fill_price": getattr(trade.orderStatus, "avgFillPrice", None),
            }

        except Exception as e:
            logger.error(f"Order execution error: {e}")
            return {"ok": False, "error": str(e), "payload": payload}

    def _execute_order(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Sync wrapper that routes order placement onto the loop thread."""
        try:
            return self.run_async(self._execute_order_async(payload))
        except Exception as e:
            return {"ok": False, "error": f"Order execution wrapper failed: {e}", "payload": payload}

def _ib_startup_preflight(cfg: Dict[str, Any], timeout_sec: float = 3.0) -> bool:
    host = safe_str(cfg.get("IB_HOST", "")).strip()
    try:
        port = int(cfg.get("IB_PORT", 0))
    except Exception:
        port = 0
    client_id = cfg.get("IB_CLIENT_ID")

    logger.info("IB preflight: host=%s port=%s clientId=%s timeout=%.1fs", host, port, client_id, timeout_sec)
    if (not host) or port <= 0:
        logger.error("IB preflight failed: invalid IB_HOST/IB_PORT configuration.")
        return False

    try:
        with socket.create_connection((host, port), timeout=timeout_sec):
            pass
        logger.info("IB preflight socket check passed: %s:%s", host, port)
        return True
    except Exception as e:
        logger.error("IB preflight failed: cannot reach %s:%s (%s: %s)", host, port, type(e).__name__, e)
        logger.error("Checklist: ensure TWS/IB Gateway is running, API socket is enabled, and host/port/firewall are correct.")
        return False

def main():
    engine: Optional[OptionsBrainEngineIB] = None
    _registered_signals: List[int] = []

    def _handle_shutdown_signal(signum, _frame):
        signame = str(signum)
        try:
            signame = signal.Signals(signum).name
        except Exception:
            pass
        logger.info("Received %s. Initiating graceful shutdown...", signame)
        if engine is not None:
            try:
                engine.stop()
            except Exception as e:
                logger.warning(f"Signal shutdown handler failed: {e}")

    try:
        for sig_name in ("SIGINT", "SIGTERM"):
            sig = getattr(signal, sig_name, None)
            if sig is None:
                continue
            signal.signal(sig, _handle_shutdown_signal)
            _registered_signals.append(sig)

        _startup_daily_cleanup(CONFIG)

        logger.info("=" * 60)
        logger.info("IB OPTIONS BRAIN - US MARKET VERSION")
        logger.info("=" * 60)
        logger.info(f"Connecting to IB at {CONFIG['IB_HOST']}:{CONFIG['IB_PORT']}")
        logger.info(f"Index ETFs: {CONFIG['INDEX_UNDERLYINGS']}")
        logger.info(f"Stock Underlyings: {CONFIG['STOCK_UNDERLYINGS_US']}")
        logger.info("=" * 60)

        if CONFIG["IB_HOST"] in ["127.0.0.1", "localhost"] and CONFIG["IB_PORT"] in [7496, 7497, 4001, 4002]:
            logger.info("Make sure TWS or IB Gateway is running and API is enabled.")
            logger.info("  TWS: File > Global Configuration > API > Settings")
            logger.info("  Enable ActiveX and Socket Clients")
            logger.info("  Common ports: 7496/7497 (TWS), 4001/4002 (Gateway)")

        if not _ib_startup_preflight(CONFIG):
            logger.critical("Startup aborted due to IB connectivity preflight failure.")
            return

        engine = OptionsBrainEngineIB(CONFIG)
        engine.start()
    except KeyboardInterrupt:
        logger.info("Received interrupt signal...")
    except (asyncio.CancelledError, concurrent.futures.CancelledError):
        # Treat cancellations as a normal shutdown path.
        logger.info("Shutdown cancelled tasks.")
    except Exception as e:
        logger.error(f"Fatal error: {type(e).__name__}: {e}", exc_info=True)
    finally:
        for sig in _registered_signals:
            try:
                signal.signal(sig, signal.SIG_DFL)
            except Exception:
                pass
        if engine is not None:
            engine.stop()

if __name__ == "__main__":
    main()
