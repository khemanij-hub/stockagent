# kite_brain_v3.2_fixed.py - INTEGRATED WITH DOI + UNIFIED SCORING + BUG FIXES
# v3.2 - Fixed bare except clauses, improved error handling, code quality improvements
# Previous: v3.1 - Aligned scoring system across live & summary files
#
# SCORING EXPLANATION:
# ====================
# confidence_score (0-4): Plan quality score calculated during trade plan generation
#   Factor 1: Volume Confirmation (+1 if Strong)
#   Factor 2: Key Level Break (+1 if Confirmed breakout/breakdown)
#   Factor 3: OI Alignment (+1 if DOI direction+confirmation aligns with bias)
#   Factor 4: Structure Alignment (+1 if 15m and 60m structures match)
#   Factor 5: Counter-trend Penalty (-2 if trading against hourly trend)
#   Factor 6: OI stale decisive block flag (+1 informational block flag; not added to confidence_score)

import os
import json
import time
import sys
import signal
import math
import subprocess
import threading
import concurrent.futures
import logging
import hashlib
import urllib.parse
import urllib.request
try:
    import fcntl  # For file locking (Unix)
except ImportError:
    fcntl = None
    if os.name == "nt":
        import msvcrt  # For file locking (Windows)
from dataclasses import dataclass
from datetime import datetime, date, timedelta, timezone
from typing import Dict, List, Optional, Tuple, Any
import pandas as pd
from dateutil import tz
from kiteconnect import KiteConnect, KiteTicker
from kiteconnect.exceptions import TokenException, InputException
import webbrowser
import re

IST = tz.gettz("Asia/Kolkata")

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
LOG_FILE_PATH = resolve_runtime_path(os.getenv("LOG_FILE_PATH"), fallback_name="trading_bot.log", base_dir=BOT_LOG_DIR)

# Configure logging with IST timestamps (matching exchange timezone)
class _ISTFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created, tz=IST)
        if datefmt:
            return dt.strftime(datefmt)
        return dt.strftime("%Y-%m-%d %H:%M:%S") + f",{int(record.msecs):03d}"

_log_fmt = _ISTFormatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
_file_handler = logging.FileHandler(LOG_FILE_PATH)
_file_handler.setFormatter(_log_fmt)
_stream_handler = logging.StreamHandler()
_stream_handler.setFormatter(_log_fmt)
logging.basicConfig(level=logging.INFO, handlers=[_file_handler, _stream_handler])
logger = logging.getLogger(__name__)

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
                else:
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
            else:
                try:
                    self.lock_file.seek(0)
                    msvcrt.locking(self.lock_file.fileno(), msvcrt.LK_UNLCK, 1)
                except Exception as e:
                    logger.debug(f"Failed to unlock file {self.filepath}: {e}")
            self.lock_file.close()
        return False

# Validate critical environment variables
def get_required_env(key: str) -> str:
    """Get required environment variable or raise error"""
    value = os.getenv(key)
    if not value or value in ["YOUR_API_KEY", "YOUR_API_SECRET"]:
        raise ValueError(f"{key} environment variable is required and must be set properly")
    return value

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

def load_stock_underlyings_from_csv(csv_path: str) -> List[str]:
    """Load stock symbols from a CSV file (comma-separated on one line)"""
    try:
        with open(csv_path, "r", encoding="utf-8-sig") as f:
            content = f.read().strip()
        return [s.strip() for s in content.split(",") if s.strip()]
    except FileNotFoundError:
        logger.warning(f"Stock underlyings CSV not found: {csv_path}. Falling back to empty list.")
        return []

def parse_symbol_lots_env(raw: Optional[str]) -> Dict[str, int]:
    """
    Parse env-style carry list:
    - ADANIPORTS:10,DELHIVERY:10,UNITDSPR:10
    - ADANIPORTS=10; DELHIVERY=10
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

CONFIG = {
    "KITE_API_KEY": get_required_env("KITE_API_KEY"),
    "KITE_API_SECRET": get_required_env("KITE_API_SECRET"),
    "INDEX_UNDERLYINGS": ["NIFTY 50", "NIFTY BANK"],
    "STOCK_UNDERLYINGS": load_stock_underlyings_from_csv(
        resolve_runtime_path(
            os.getenv("STOCK_UNDERLYINGS_CSV", "India Stocks.csv"),
            base_dir=BOT_BASE_DIR,
        )
    ),
    "STREAM_MODE": os.getenv("STREAM_MODE", "ATM_ONLY"),
    "MAX_WS_TOKENS": int(os.getenv("MAX_WS_TOKENS", "3900")),
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
    "NEAR_ATM_STRIKES_EACH_SIDE": int(os.getenv("NEAR_ATM_STRIKES_EACH_SIDE", "10")),
    "OI_EXPIRY_COUNT": int(os.getenv("OI_EXPIRY_COUNT", "1")),
    "RISK_PER_TRADE_PCT_GUIDELINE": float(os.getenv("RISK_PER_TRADE_PCT_GUIDELINE", "0.5")),
    "MIN_RR": float(os.getenv("MIN_RR", "2.0")),
    "CSV_LIVE_PATH": os.getenv("CSV_LIVE_PATH", "trade_ideas_live.csv"),
    "CSV_ACTIONS_PATH": os.getenv("CSV_ACTIONS_PATH", "trade_ideas_actions.csv"),
    "CSV_SUMMARY_PATH": os.getenv("CSV_SUMMARY_PATH", "trade_ideas_summary.csv"),
    "CSV_POLL_SECONDS": int(os.getenv("CSV_POLL_SECONDS", "10")),
    "TOKEN_FILE": os.getenv("KITE_TOKEN_FILE", "kite_token.json"),
    "KITE_AUTH_MODE": os.getenv("KITE_AUTH_MODE", "auto"),
    "KITE_REQUEST_TOKEN": os.getenv("KITE_REQUEST_TOKEN", ""),
    "KITE_OPEN_BROWSER": parse_env_bool(os.getenv("KITE_OPEN_BROWSER"), True),
    "SEED_HISTORY_DAYS": int(os.getenv("SEED_HISTORY_DAYS", "30")),
    "NEWS_FEED_CSV_PATH": os.getenv("NEWS_FEED_CSV_PATH", "news_feed.csv"),
    "NEWS_LOOKBACK_MINUTES": int(os.getenv("NEWS_LOOKBACK_MINUTES", "240")),
    "NEWS_MAX_ITEMS": int(os.getenv("NEWS_MAX_ITEMS", "3")),
    "ENABLE_SCORING_OUTPUT": os.getenv("ENABLE_SCORING_OUTPUT", "false").lower() == "true",
    "LOG_GATE_DIAGNOSTICS": os.getenv("LOG_GATE_DIAGNOSTICS", "true").lower() == "true",
    "LOG_GATE_DIAGNOSTICS_ONLY_BLOCKED": os.getenv("LOG_GATE_DIAGNOSTICS_ONLY_BLOCKED", "true").lower() == "true",
    "API_RETRIES": int(os.getenv("API_RETRIES", "3")),
    "API_RETRY_BACKOFF_SECONDS": float(os.getenv("API_RETRY_BACKOFF_SECONDS", "0.7")),
    "API_MIN_INTERVAL_SEC": float(os.getenv("API_MIN_INTERVAL_SEC", "0.08")),
    "CB_MAX_FAILS": int(os.getenv("CB_MAX_FAILS", "3")),
    "CB_COOLDOWN_MINUTES": int(os.getenv("CB_COOLDOWN_MINUTES", "10")),
    "IDEA_BATCH_SIZE": int(os.getenv("IDEA_BATCH_SIZE", "50")),
    "IDEA_WORKERS": int(os.getenv("IDEA_WORKERS", "3")),
    "WRITE_ALL_SIGNALS": os.getenv("WRITE_ALL_SIGNALS", "true").lower() == "true",  # Write all signals or only when bias changes
    "TYPICAL_VOL_PREFETCH_ENABLED": os.getenv("TYPICAL_VOL_PREFETCH_ENABLED", "false").lower() == "true",
    "TYPICAL_VOL_PREFETCH_PARALLEL": int(os.getenv("TYPICAL_VOL_PREFETCH_PARALLEL", "2")),
    "INCLUDE_PLAN_JSON": os.getenv("INCLUDE_PLAN_JSON", "true").lower() == "true",
    "OI_QUOTE_BATCH_SIZE": int(os.getenv("OI_QUOTE_BATCH_SIZE", "200")),
    "OI_QUOTE_BATCH_PAUSE_SEC": float(os.getenv("OI_QUOTE_BATCH_PAUSE_SEC", "0.0")),
    "OI_CHAIN_REFRESH_MINUTES": int(os.getenv("OI_CHAIN_REFRESH_MINUTES", "30")),
    "OI_CHAIN_REFRESH_ATM_STEPS": int(os.getenv("OI_CHAIN_REFRESH_ATM_STEPS", "2")),
    "OI_SNAPSHOT_PATH": os.getenv("OI_SNAPSHOT_PATH", "oi_snapshot_cache.json"),
    "SIGNAL_STATE_PATH": os.getenv("SIGNAL_STATE_PATH", "signal_state_cache.json"),
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
    "MARKET_PROTECTION": os.getenv("MARKET_PROTECTION", "2"),
    "CARRY_FORWARD_LOTS": parse_symbol_lots_env(os.getenv("CARRY_FORWARD_LOTS", "")),
    "TELEGRAM_ENABLED": resolve_telegram_enabled(),
    "TELEGRAM_BOT_TOKEN": os.getenv("TELEGRAM_BOT_TOKEN", os.getenv("TG_BOT_TOKEN", "")),
    "TELEGRAM_CHAT_IDS": os.getenv("TELEGRAM_CHAT_IDS", os.getenv("TELEGRAM_CHAT_ID", os.getenv("TG_APPROVAL_CHAT_IDS", os.getenv("TG_CHANNELS", "")))),
    "TELEGRAM_POLL_SECONDS": float(os.getenv("TELEGRAM_POLL_SECONDS", "2.0")),
    "TELEGRAM_STATE_PATH": os.getenv("TELEGRAM_STATE_PATH", "telegram_approvals_state.json"),
    "LOG_FILE_PATH": LOG_FILE_PATH,
}

for _path_key in [
    "CSV_LIVE_PATH",
    "CSV_ACTIONS_PATH",
    "CSV_SUMMARY_PATH",
    "TOKEN_FILE",
    "NEWS_FEED_CSV_PATH",
    "OI_SNAPSHOT_PATH",
    "SIGNAL_STATE_PATH",
    "TELEGRAM_STATE_PATH",
]:
    CONFIG[_path_key] = resolve_runtime_path(CONFIG.get(_path_key))
CONFIG["LOG_FILE_PATH"] = resolve_runtime_path(CONFIG.get("LOG_FILE_PATH"), fallback_name="trading_bot.log", base_dir=BOT_LOG_DIR)
for _path_key in [
    "CSV_LIVE_PATH",
    "CSV_ACTIONS_PATH",
    "CSV_SUMMARY_PATH",
    "TOKEN_FILE",
    "OI_SNAPSHOT_PATH",
    "SIGNAL_STATE_PATH",
    "TELEGRAM_STATE_PATH",
    "LOG_FILE_PATH",
]:
    _ensure_dir(os.path.dirname(str(CONFIG.get(_path_key, "")).strip()))

logger.info("="*60)
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
logger.info("="*60)

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
    "row_id", "timestamp_ist", "session_context",
    "underlying_type", "symbol",
    "expiry", "strike", "right", "tradingsymbol", "lot_size",
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
    "summary_run_ts_ist",
    "rank", "row_id", "timestamp_ist", "symbol", "underlying_type",
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
    """Safely convert any value to string, handling None/float/int/NaN"""
    if x is None or pd.isna(x):
        return ""
    if isinstance(x, (int, float)):
        return str(x)
    return str(x).strip()

def confidence_grade(score: int) -> str:
    return "High" if score >= 3 else "Medium" if score == 2 else "Low"

def now_ist() -> datetime:
    return datetime.now(tz=IST)

def to_ist(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(IST)

def ist_session_context(ts_ist: datetime) -> str:
    t = ts_ist.time()
    if t < datetime(ts_ist.year, ts_ist.month, ts_ist.day, 9, 0, tzinfo=IST).time():
        return "pre-open"
    if datetime(ts_ist.year, ts_ist.month, ts_ist.day, 9, 15, tzinfo=IST).time() <= t <= datetime(ts_ist.year, ts_ist.month, ts_ist.day, 15, 30, tzinfo=IST).time():
        return "live"
    if t <= datetime(ts_ist.year, ts_ist.month, ts_ist.day, 16, 0, tzinfo=IST).time():
        return "closing"
    return "after-hours"

def floor_time(ts_ist: datetime, minutes: int) -> datetime:
    m = (ts_ist.minute // minutes) * minutes
    return ts_ist.replace(minute=m, second=0, microsecond=0)

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

def parse_market_protection(x, default=2):
    raw = safe_str(x).upper().strip()
    if raw in ("ON", "TRUE", "YES"):
        return 2
    if raw in ("OFF", "FALSE", "NO"):
        return -1
    return safe_int(x, default)

def clamp_round_to_step(price: float, step: float) -> float:
    if step <= 0:
        return round(price)
    return round(round(price / step) * step, 2)

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
        self.state_path = safe_str(cfg.get("TELEGRAM_STATE_PATH", "telegram_approvals_state.json")) or "telegram_approvals_state.json"
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
        now_text = now_ist().strftime("%Y-%m-%d %H:%M:%S")
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
                    or now_ist().strftime("%Y-%m-%d %H:%M:%S")
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
                        {"text": "YES x5", "callback_data": f"YES|{symbol}|5|{sig}"},
                        {"text": "YES x10", "callback_data": f"YES|{symbol}|10|{sig}"},
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
                sent_at = now_ist().strftime("%Y-%m-%d %H:%M:%S")
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
    """Infer the strike step from a sorted list of strikes."""
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
    """
    Compute OI alignment from Delta-OI strings.
    Returns {"direction": BULLISH|BEARISH|NEUTRAL, "confirmation": 0|1, "confidence": float, "net": float}
    """
    call_doi = _parse_strike_doi(top_call_doi_strikes)
    put_doi  = _parse_strike_doi(top_put_doi_strikes)

    all_strikes = sorted(set(call_doi.keys()) | set(put_doi.keys()))
    step = _infer_step(all_strikes)

    atm = round(ltp / step) * step
    window = [atm + i * step for i in range(-window_steps, window_steps + 1)]

    # Core DOI sums
    P = sum(put_doi.get(k, 0) for k in window if k <= ltp)
    C = sum(call_doi.get(k, 0) for k in window if k >= ltp)

    call_unwind = sum(-call_doi.get(k, 0) for k in window if k >= ltp and call_doi.get(k, 0) < 0)
    put_unwind  = sum(-put_doi.get(k, 0)  for k in window if k <= ltp and put_doi.get(k, 0)  < 0)

    net = (P - C) + unwind_boost * call_unwind - unwind_boost * put_unwind

    activity = abs(P) + abs(C) + call_unwind + put_unwind + 1e-9
    confidence = 100.0 * (abs(net) / activity)

    if activity < max(0.0, safe_float(min_activity, 0.0)):
        return {"direction": "NEUTRAL", "confirmation": 0, "confidence": 0.0, "net": round(net, 1)}

    # Dynamic noise threshold
    mags = [abs(call_doi.get(k, 0)) + abs(put_doi.get(k, 0)) for k in window]
    mags.sort()
    median_mag = mags[len(mags) // 2] if mags else 0
    T = max(1.0, 0.25 * median_mag)

    # Direction
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

def csv_safe_text(x: Any) -> str:
    if x is None:
        return ""
    if isinstance(x, list):
        x = " | ".join([str(i) for i in x])
    s = str(x)
    s = s.replace("\r\n", " | ").replace("\n", " | ").replace("\r", " | ")
    s = " ".join(s.split())
    return s

def load_saved_token(token_file: str) -> Optional[str]:
    if not os.path.exists(token_file):
        return None
    try:
        with open(token_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        return (data.get("access_token") or "").strip() or None
    except Exception as e:
        logger.warning(f"Failed to load token from {token_file}: {e}")
        return None

def save_token(token_file: str, access_token: str):
    payload = {"access_token": access_token, "saved_at_ist": now_ist().strftime("%Y-%m-%d %H:%M:%S")}
    with open(token_file, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

def _normalize_kite_auth_mode(raw_mode: Optional[str]) -> str:
    mode = safe_str(raw_mode).lower().replace("-", "_")
    if mode in ("", "auto"):
        return "auto"
    if mode in ("interactive", "prompt"):
        return "interactive"
    if mode in ("non_interactive", "noninteractive", "headless"):
        return "non_interactive"
    logger.warning(f"Unknown KITE_AUTH_MODE '{raw_mode}', defaulting to auto.")
    return "auto"


def _stdin_is_interactive() -> bool:
    try:
        return bool(sys.stdin) and bool(sys.stdin.isatty())
    except Exception:
        return False


def _generate_token_from_request_token(
    kite: KiteConnect,
    api_secret: str,
    token_file: str,
    request_token: str,
) -> str:
    req = safe_str(request_token).strip()
    if not req:
        raise ValueError("Empty KITE_REQUEST_TOKEN")
    data = kite.generate_session(req, api_secret=api_secret)
    access_token = safe_str(data.get("access_token"))
    if not access_token:
        raise RuntimeError("Kite session response missing access_token")
    kite.set_access_token(access_token)
    save_token(token_file, access_token)
    return access_token


def interactive_login_and_get_token(
    kite: KiteConnect,
    api_secret: str,
    token_file: str,
    open_browser: bool = True,
) -> str:
    if not _stdin_is_interactive():
        raise RuntimeError(
            "Interactive Kite login is unavailable without a TTY. "
            "Set KITE_AUTH_MODE=non_interactive and provide KITE_ACCESS_TOKEN "
            "or KITE_REQUEST_TOKEN."
        )
    login_url = kite.login_url()
    print("\n=== Zerodha Login Required ===")
    print("Open and login:", login_url)
    if open_browser:
        try:
            webbrowser.open(login_url)
        except Exception:
            pass
    print("\nAfter login, copy request_token from redirect URL (request_token=XXXX).")
    request_token = input("Paste request_token here: ").strip()
    access_token = _generate_token_from_request_token(
        kite=kite,
        api_secret=api_secret,
        token_file=token_file,
        request_token=request_token,
    )
    print("Access token saved:", token_file)
    return access_token

def make_kite_client(
    api_key: str,
    api_secret: str,
    token_file: str,
    auth_mode: str = "auto",
    request_token_env: str = "",
    open_browser: bool = True,
) -> Tuple[KiteConnect, str]:
    kite = KiteConnect(api_key=api_key)
    mode = _normalize_kite_auth_mode(auth_mode)
    env_token = (os.getenv("KITE_ACCESS_TOKEN", "") or "").strip() or None
    file_token = load_saved_token(token_file)
    access_token = env_token or file_token

    def validate(token: Optional[str]) -> bool:
        if not token:
            return False
        kite.set_access_token(token)
        kite.profile()
        return True

    last_error: Optional[Exception] = None

    try:
        if validate(access_token):
            print("Kite auth OK using", ("ENV" if env_token else "TOKEN_FILE"))
            return kite, access_token
    except (TokenException, InputException) as e:
        print(f"Token invalid ({type(e).__name__}: {e}). Re-login required...")
        last_error = e

    request_token = safe_str(request_token_env).strip() or safe_str(os.getenv("KITE_REQUEST_TOKEN", "")).strip()
    if request_token:
        try:
            new_token = _generate_token_from_request_token(
                kite=kite,
                api_secret=api_secret,
                token_file=token_file,
                request_token=request_token,
            )
            kite.profile()
            print("Kite auth OK using KITE_REQUEST_TOKEN.")
            return kite, new_token
        except Exception as e:
            last_error = e
            logger.error(f"KITE_REQUEST_TOKEN login failed: {type(e).__name__}: {e}")
            if mode == "non_interactive":
                raise RuntimeError(
                    "Kite non-interactive auth failed using KITE_REQUEST_TOKEN. "
                    "Provide a fresh request token or a valid KITE_ACCESS_TOKEN."
                ) from e

    if mode == "non_interactive":
        msg = (
            "Kite auth failed in non-interactive mode. "
            "Set KITE_ACCESS_TOKEN or KITE_REQUEST_TOKEN before startup."
        )
        if last_error is not None:
            msg += f" Last error: {type(last_error).__name__}: {last_error}"
        raise RuntimeError(msg)

    if mode == "auto" and (not _stdin_is_interactive()):
        raise RuntimeError(
            "Kite auth requires interactive login but no TTY is available. "
            "Use KITE_AUTH_MODE=non_interactive with KITE_ACCESS_TOKEN or KITE_REQUEST_TOKEN."
        )

    new_token = interactive_login_and_get_token(
        kite=kite,
        api_secret=api_secret,
        token_file=token_file,
        open_browser=open_browser,
    )
    kite.profile()
    print("Kite auth OK after interactive login.")
    return kite, new_token

@dataclass
class Candle:
    start_ist: datetime
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
        self.current: Dict[int, Optional[Candle]] = {}
        self.closed: Dict[int, List[Candle]] = {}
        self.last_cum_vol: Dict[int, Optional[int]] = {}

    def update(self, token: int, ltp: float, tick_ts_utc: datetime, cum_vol: Optional[int]) -> Optional[Candle]:
        ts_ist = to_ist(tick_ts_utc)
        start = floor_time(ts_ist, self.mins)
        cur = self.current.get(token)
        if cur is None:
            cur = Candle(start_ist=start, open=ltp, high=ltp, low=ltp, close=ltp, volume=0)
            self.current[token] = cur
        if start > cur.start_ist:
            closed_candle = cur
            self.closed.setdefault(token, []).append(closed_candle)
            cur = Candle(start_ist=start, open=ltp, high=ltp, low=ltp, close=ltp, volume=0)
            self.current[token] = cur
            return closed_candle
        cur.high = max(cur.high, ltp)
        cur.low = min(cur.low, ltp)
        cur.close = ltp
        if cum_vol is not None:
            prev = self.last_cum_vol.get(token)
            if prev is None:
                self.last_cum_vol[token] = cum_vol
            else:
                delta = max(0, cum_vol - prev)
                self.last_cum_vol[token] = cum_vol
                cur.volume += delta
                cur.vwap_num += ltp * delta
                cur.vwap_den += delta
        return None

    def last_closed(self, token: int, n: int = 1) -> List[Candle]:
        return self.closed.get(token, [])[-n:]

def _norm(x: Any) -> str:
    if x is None:
        return ""
    try:
        s = str(x)
    except Exception as e:
        logger.debug(f"Failed to normalize value {type(x).__name__}: {e}")
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

def build_news_keywords_for_symbol(symbol: str, uni) -> List[str]:
    sym = _norm(symbol)
    if sym in ["NIFTY 50", "NIFTY"]:
        return ["NIFTY", "NIFTY50", "NIFTY 50"]
    if sym in ["NIFTY BANK", "BANKNIFTY", "BANK NIFTY"]:
        return ["BANKNIFTY", "NIFTY BANK", "BANK NIFTY"]
    keywords = [sym]
    try:
        df = uni.instr_nse
        m = df[df["tradingsymbol"].astype(str).str.upper() == sym]
        if not m.empty:
            name = m.iloc[0].get("name", "")
            if name:
                keywords.append(str(name))
                keywords.append(str(name).replace("LTD", "LIMITED"))
    except Exception as e:
        logger.debug(f"Failed to build keywords for {symbol}: {e}")
    ALIASES = {"RELIANCE": ["RIL", "RELIANCE INDUSTRIES"], "INFY": ["INFOSYS"], "TCS": ["TATA CONSULTANCY SERVICES"]}
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

    minute_bucket = now_ist().strftime("%Y-%m-%d %H:%M")
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

    if df.empty or "ts_ist" not in df.columns:
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

    ts = pd.to_datetime(df["ts_ist"], format="%Y-%m-%d %H:%M:%S", errors="coerce")
    try:
        if ts.dt.tz is None:
            ts = ts.dt.tz_localize(IST)
        else:
            ts = ts.dt.tz_convert(IST)
    except Exception:
        ts = pd.Series(pd.NaT, index=df.index)
    cutoff = now_ist() - timedelta(minutes=lookback_minutes)

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

def load_news_from_feed_csv_for_symbol(news_csv_path: str, uni, symbol: str, lookback_minutes: int = 240, max_items: int = 3) -> Tuple[List[str], List[str]]:
    df = _load_news_window_cached(news_csv_path, lookback_minutes)
    if df.empty:
        return [], []

    sym = _norm(symbol)
    patterns = _NEWS_PATTERN_CACHE.get(sym)
    if patterns is None:
        patterns = _make_keyword_patterns(build_news_keywords_for_symbol(symbol, uni))
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
        ts = r.get("ts_ist", "")
        src = r.get("source_name", "")
        title = r.get("title", "") or ""
        text = r.get("text", "") or ""
        url = r.get("url", "") or ""
        headline = title.strip() if title.strip() else (text[:160].strip())
        bullets.append(csv_safe_text(f"{ts} IST [{src}] {headline}"))
        if url.strip():
            sources.append(csv_safe_text(url.strip()))

    return bullets, sources

def _migrate_csv_schema(path: str, required_cols: List[str], label: str):
    with FileLock(path):
        if not os.path.exists(path):
            pd.DataFrame(columns=required_cols).to_csv(path, index=False)
            print(f"Created {label} CSV: {path}")
            return
        try:
            df = pd.read_csv(path, keep_default_na=False)
        except pd.errors.EmptyDataError as e:
            # Transient empty-read can happen if another tool briefly truncates/locks the file.
            logger.warning(f"{label} CSV temporary empty/unreadable, skipping migrate: {e}")
            return
        except PermissionError as e:
            logger.warning(f"{label} CSV locked, skipping migrate: {e}")
            return
        except Exception as e:
            logger.warning(f"{label} CSV unreadable: {e}")
            bak = path + ".bak"
            try:
                os.replace(path, bak)
                print(f"{label} CSV unreadable, moved to backup: {bak}")
            except Exception as backup_err:
                logger.error(f"Failed to backup corrupt CSV {path}: {backup_err}")
                return
            pd.DataFrame(columns=required_cols).to_csv(path, index=False)
            print(f"Created {label} CSV: {path}")
            return
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            for c in missing:
                df[c] = ""
            # Reorder: required columns first (in schema order), then any extras
            extra = [c for c in df.columns if c not in required_cols]
            df = df[required_cols + extra]
            df.to_csv(path, index=False)
            print(f"Migrated {label} CSV schema, added columns: {missing}")

def _ensure_csv_exists_quick(path: str, headers: List[str]):
    """Fast path for runtime loops: only create file if missing."""
    if os.path.exists(path):
        return
    with FileLock(path):
        if not os.path.exists(path):
            pd.DataFrame(columns=headers).to_csv(path, index=False)

def ensure_live_csv(path: str): _migrate_csv_schema(path, CSV_HEADERS, "LIVE")
def ensure_actions_csv(path: str): _migrate_csv_schema(path, ACTIONS_HEADERS, "ACTIONS")
def ensure_summary_csv(path: str): _migrate_csv_schema(path, SUMMARY_HEADERS, "SUMMARY")

def append_live_row(path: str, row: Dict[str, Any]):
    """Append a row to live CSV with proper value conversion and file locking"""
    _ensure_csv_exists_quick(path, CSV_HEADERS)
    
    # Convert all values to appropriate string representations
    converted_row = {}
    for h in CSV_HEADERS:
        val = row.get(h, "")
        converted_row[h] = safe_str(val)
    
    with FileLock(path):
        df = pd.DataFrame([converted_row])
        df.to_csv(path, mode="a", header=False, index=False)

def load_live(path: str) -> pd.DataFrame:
    _ensure_csv_exists_quick(path, CSV_HEADERS)
    with FileLock(path):
        try:
            return pd.read_csv(path, keep_default_na=False)
        except pd.errors.EmptyDataError:
            return pd.DataFrame(columns=CSV_HEADERS)

def update_live_row(path: str, row_id: str, updates: Dict[str, Any]):
    """Update live CSV row with proper value conversion and file locking"""
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
    df["approve"] = (df["approve"] if "approve" in df.columns else pd.Series("NO", index=df.index)).fillna("NO").astype(str).str.upper()
    df["lots"] = pd.to_numeric(df["lots"] if "lots" in df.columns else pd.Series(0, index=df.index), errors="coerce").fillna(0).astype(int)
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

    # Preserve existing manual controls.
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
    for c in ["symbol", "timestamp_ist"]:
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
        work["timestamp_ist_dt"] = pd.to_datetime(work["timestamp_ist"], errors="coerce")
        work = work.sort_values(by=["timestamp_ist_dt", "timestamp_ist"], ascending=[False, False])
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
        mtime_date = datetime.fromtimestamp(os.path.getmtime(state_path), tz=IST).date()
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
        mtime_date = datetime.fromtimestamp(os.path.getmtime(p), tz=IST).date()
        return mtime_date < today
    except Exception:
        return False

def _truncate_log_file(log_path: str) -> bool:
    p = safe_str(log_path)
    if (not p) or (not os.path.exists(p)):
        return False
    try:
        active_log_path = safe_str(getattr(_file_handler, "baseFilename", ""))
        is_active_log = bool(active_log_path) and (
            os.path.abspath(active_log_path) == os.path.abspath(p)
        )
    except Exception:
        is_active_log = False
    try:
        if is_active_log:
            # Rewrite through the active file handler stream so Windows file-handle semantics remain safe.
            _file_handler.acquire()
            try:
                _file_handler.flush()
                stream = _file_handler.stream
                stream.seek(0)
                stream.truncate(0)
                stream.flush()
            finally:
                _file_handler.release()
        else:
            with open(p, "w", encoding="utf-8"):
                pass
        return True
    except Exception as e:
        logger.warning(f"Daily cleanup: failed truncating log file {p}: {e}")
        return False

def _clear_log_if_prior_day(log_path: str, today: date) -> bool:
    p = safe_str(log_path)
    if (not p) or (not os.path.exists(p)):
        return False
    today_prefix = today.strftime("%Y-%m-%d")
    try:
        with open(p, "r", encoding="utf-8", errors="ignore") as f:
            lines = f.readlines()
    except FileNotFoundError:
        return False
    except Exception as e:
        logger.warning(f"Daily cleanup: failed reading log file {p}: {e}")
        return False

    # We treat this as a new trading day if either:
    # 1) file mtime is from a prior IST date, or
    # 2) any date-prefixed log line belongs to a prior date.
    prior_day_by_mtime = _is_file_prior_day(p, today)
    has_prior_dated_line = False
    for ln in lines:
        if re.match(r"^\d{4}-\d{2}-\d{2}", ln):
            if not ln.startswith(today_prefix):
                has_prior_dated_line = True
                break

    if not (prior_day_by_mtime or has_prior_dated_line):
        return False
    return _truncate_log_file(p)

def _clear_daily_logs_and_json(cfg: Dict[str, Any], today: date) -> Tuple[int, int]:
    cleared_json = 0
    cleared_logs = 0
    json_targets = [
        safe_str(cfg.get("OI_SNAPSHOT_PATH", "")),
        safe_str(cfg.get("SIGNAL_STATE_PATH", "")),
        safe_str(cfg.get("TELEGRAM_STATE_PATH", "")),
    ]
    for p in sorted({p for p in json_targets if p}):
        if _reset_json_state_if_prior_day(p, today):
            cleared_json += 1

    log_targets = [safe_str(cfg.get("LOG_FILE_PATH", ""))]
    for p in sorted({p for p in log_targets if p}):
        if _clear_log_if_prior_day(p, today):
            cleared_logs += 1

    return cleared_json, cleared_logs

def _startup_daily_cleanup(cfg: Dict[str, Any]):
    today = now_ist().date()
    live_removed, live_left = _prune_csv_rows_to_today(
        cfg["CSV_LIVE_PATH"], CSV_HEADERS, "timestamp_ist", today
    )
    summary_removed, summary_left = _prune_csv_rows_to_today(
        cfg["CSV_SUMMARY_PATH"], SUMMARY_HEADERS, "timestamp_ist", today
    )
    json_cleared, logs_cleared = _clear_daily_logs_and_json(cfg, today)
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

    if any(x > 0 for x in [live_removed, summary_removed, actions_reset, json_cleared, logs_cleared]):
        logger.info(
            "Daily cleanup complete: live_removed=%d, summary_removed=%d, actions_reset=%d, "
            "json_files_cleared=%d, log_files_cleared=%d",
            live_removed, summary_removed, actions_reset, json_cleared, logs_cleared,
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
    3) Summary size is driven by qualifying symbols (no forced top-10 backfill).
    4) Keep tracking any symbol that has ever hit the configured threshold in live
       history, and keep updating it with latest info even when confidence drops.
    """
    ensure_summary_csv(summary_path)
    live = load_live(live_path)
    if live.empty:
        return

    # Normalize text fields
    for c in ["confidence", "breakout_status", "breakdown_status", "volume_confirmation", "bias", "status", "symbol"]:
        if c in live.columns:
            live[c] = live[c].fillna("").astype(str).str.upper().str.strip()
        else:
            live[c] = ""

    for c in ["timestamp_ist", "ltp", "pct_change", "confidence_score"]:
        if c not in live.columns:
            live[c] = "" if c != "confidence_score" else 0

    # Parse timestamp and keep only the latest row per symbol.
    live["timestamp_ist_dt"] = pd.to_datetime(live["timestamp_ist"], errors="coerce")
    live["confidence_score"] = pd.to_numeric(live.get("confidence_score", 0), errors="coerce").fillna(0).astype(int)
    live = live.sort_values(by=["timestamp_ist_dt", "timestamp_ist"], ascending=[False, False])
    latest = live.drop_duplicates(subset=["symbol"], keep="first").copy()

    # Symbols previously present in summary stay tracked so trend changes are visible.
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

    # New picks use configurable threshold (bounded to valid score range).
    min_confidence = max(0, min(4, int(min_confidence)))
    _ = include_wait  # retained as function arg for backward compatibility
    eligible_new = latest[latest["confidence_score"] >= min_confidence].copy()
    if top_n and top_n > 0:
        eligible_new = eligible_new.sort_values(
            by=["confidence_score", "timestamp_ist_dt", "timestamp_ist"], ascending=[False, False, False]
        ).head(top_n)
    selected_symbols = set(eligible_new["symbol"])

    # Keep tracking any symbol that has ever qualified at/above threshold.
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

    # Factor columns read from live CSV (computed once in build_trade_plan)
    for fc in FACTOR_COLUMNS:
        if fc in top.columns:
            top[fc] = pd.to_numeric(top[fc], errors="coerce").fillna(0).astype(int)
        else:
            top[fc] = 0

    # Use latest run confidence score directly from live row.
    top["confidence_score"] = pd.to_numeric(top.get("confidence_score", 0), errors="coerce").fillna(0).astype(int)
    top["confidence"] = top["confidence_score"].apply(confidence_grade)

    # Calculate freshness from first time a symbol was added to summary.
    # We reconstruct first-added timestamp from previous summary:
    # first_added = previous_summary_run_ts - previous_minutes_since_signal.
    now = now_ist()
    summary_origin_by_symbol: Dict[str, datetime] = {}

    def _parse_summary_ts(ts_text: str) -> Optional[datetime]:
        try:
            return datetime.strptime(str(ts_text), "%Y-%m-%d %H:%M:%S").replace(tzinfo=IST)
        except (ValueError, TypeError, AttributeError):
            return None

    def _parse_minutes(v: Any) -> int:
        try:
            return int(float(v))
        except (ValueError, TypeError):
            return 0

    try:
        if os.path.exists(summary_path) and os.path.getsize(summary_path) > 0:
            prev = pd.read_csv(summary_path, dtype=str, keep_default_na=False)
            if not prev.empty and "symbol" in prev.columns:
                for _, prow in prev.iterrows():
                    sym = safe_str(prow.get("symbol", "")).upper().strip()
                    if not sym:
                        continue
                    prev_run_ts = _parse_summary_ts(prow.get("summary_run_ts_ist", ""))
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
            row_ts = pd.to_datetime(row.get("timestamp_ist", ""), errors="coerce")
            if pd.notna(row_ts):
                first_added_ts = row_ts.to_pydatetime().replace(tzinfo=IST)
            else:
                first_added_ts = now
        return max(0, int((now - first_added_ts).total_seconds() / 60))

    top["minutes_since_signal"] = top.apply(calc_minutes, axis=1)

    # Count how many times each symbol was picked with same bias across all scans
    bias_counts = live.groupby(["symbol", "bias"]).size().reset_index(name="_cnt")
    top = top.merge(bias_counts, on=["symbol", "bias"], how="left")
    top["count"] = top["_cnt"].fillna(0).astype(int)
    top.drop(columns=["_cnt"], inplace=True)

    # Rank current snapshot by latest confidence and recency.
    top = top.sort_values(by=["confidence_score", "timestamp_ist_dt", "timestamp_ist"], ascending=[False, False, False])
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

    run_ts = now_ist().strftime("%Y-%m-%d %H:%M:%S")
    top.insert(0, "summary_run_ts_ist", run_ts)
    top.drop(columns=["timestamp_ist_dt"], inplace=True, errors="ignore")

    # Prepare output - convert all values to strings
    new_rows = pd.DataFrame(columns=SUMMARY_HEADERS)
    for col in SUMMARY_HEADERS:
        if col in top.columns:
            new_rows[col] = top[col].apply(safe_str)
        else:
            new_rows[col] = ""
    new_rows = new_rows.fillna("")

    # Save current snapshot only (one latest row per tracked symbol).
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
    today = now_ist().date()
    todays = [c for c in candles if c.start_ist.date() == today]
    if not todays:
        return None, None
    if opening_range_minutes == 15:
        first = min(todays, key=lambda c: c.start_ist)
        return first.high, first.low
    if opening_range_minutes == 30:
        first2 = sorted(todays, key=lambda c: c.start_ist)[:2]
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

class KiteUniverse:
    def __init__(self, kite: KiteConnect):
        self.kite = kite
        self.instr_nse: pd.DataFrame = pd.DataFrame()
        self.instr_nfo: pd.DataFrame = pd.DataFrame()
        self._nfo_by_ts_upper: Dict[str, Dict[str, Any]] = {}
        self._load_instruments()

    def _load_instruments(self):
        print("Loading instruments (NSE, NFO)...")
        self.instr_nse = pd.DataFrame(self.kite.instruments("NSE"))
        self.instr_nfo = pd.DataFrame(self.kite.instruments("NFO"))
        out: Dict[str, Dict[str, Any]] = {}
        try:
            for _, row in self.instr_nfo.iterrows():
                ts = safe_str(row.get("tradingsymbol", "")).upper().strip()
                if not ts:
                    continue
                out[ts] = {
                    "tradingsymbol": safe_str(row.get("tradingsymbol", "")).upper().strip(),
                    "instrument_type": safe_str(row.get("instrument_type", "")).upper().strip(),
                    "expiry": row.get("expiry", ""),
                    "strike": row.get("strike", ""),
                }
        except Exception as e:
            logger.debug(f"Failed building NFO tradingsymbol lookup cache: {e}")
            out = {}
        self._nfo_by_ts_upper = out
        print(f"Loaded NSE instruments: {len(self.instr_nse)} | NFO instruments: {len(self.instr_nfo)}")

    def find_underlying_token_nse(self, tradingsymbol: str) -> Optional[int]:
        df = self.instr_nse
        m = df[df["tradingsymbol"] == tradingsymbol]
        if m.empty:
            m = df[df["name"].astype(str).str.upper() == tradingsymbol.upper()]
        if m.empty:
            return None
        return int(m.iloc[0]["instrument_token"])

    def strike_step(self, underlying_nfo_name: str) -> float:
        if underlying_nfo_name == "NIFTY": return 50
        if underlying_nfo_name == "BANKNIFTY": return 100
        # Derive strike step from actual NFO instrument data
        df = self.instr_nfo
        d = df[(df["name"] == underlying_nfo_name) & (df["instrument_type"].isin(["CE", "PE"]))]
        if d.empty:
            return 10
        strikes = sorted(d["strike"].unique())
        if len(strikes) < 2:
            return 10
        diffs = set()
        for i in range(len(strikes) - 1):
            diff = strikes[i + 1] - strikes[i]
            if diff > 0:
                diffs.add(diff)
        step = min(diffs) if diffs else 10
        return max(0.5, step)  # Guard against zero/tiny steps

    def nearest_expiry_list(self, underlying_nfo_name: str, count: int = 1) -> List[date]:
        df = self.instr_nfo
        d = df[(df["name"] == underlying_nfo_name) & (df["instrument_type"].isin(["CE", "PE"]))]
        if d.empty:
            return []
        exp = sorted({pd.to_datetime(x).date() for x in d["expiry"].unique()})
        if not exp:
            return []
        today = now_ist().date()
        future = [e for e in exp if e >= today]
        pool = future if future else exp
        n = max(1, int(count))
        return pool[:n]

    def nearest_expiries(self, underlying_nfo_name: str) -> Tuple[Optional[date], Optional[date]]:
        exp = self.nearest_expiry_list(underlying_nfo_name, count=1000)
        if not exp:
            return None, None
        nearest = exp[0]
        nextm = None
        for e in exp[1:]:
            if e.month != nearest.month:
                nextm = e
                break
        if nextm is None and len(exp) >= 2:
            nextm = exp[1]
        return nearest, nextm

    def select_atm_option_tokens(self, underlying_nfo_name: str, underlying_ltp: float, expiry: date, strikes_each_side: int) -> Dict[str, Any]:
        step = self.strike_step(underlying_nfo_name)
        atm = clamp_round_to_step(underlying_ltp, step)
        strikes = [atm + i * step for i in range(-strikes_each_side, strikes_each_side + 1)]
        df = self.instr_nfo
        d = df[(df["name"] == underlying_nfo_name) & (pd.to_datetime(df["expiry"]).dt.date == expiry) & (df["instrument_type"].isin(["CE", "PE"])) & (df["strike"].isin(strikes))].copy()
        ce = d[d["instrument_type"] == "CE"].sort_values("strike")
        pe = d[d["instrument_type"] == "PE"].sort_values("strike")
        ce_list = [(r["tradingsymbol"], int(r["instrument_token"]), float(r["strike"])) for _, r in ce.iterrows()]
        pe_list = [(r["tradingsymbol"], int(r["instrument_token"]), float(r["strike"])) for _, r in pe.iterrows()]
        return {"atm_strike": atm, "expiry": expiry.isoformat(), "strike_step": step, "ce": ce_list, "pe": pe_list}

    def lot_size_for_tradingsymbol(self, tradingsymbol: str) -> Optional[int]:
        df = self.instr_nfo
        m = df[df["tradingsymbol"] == tradingsymbol]
        if m.empty: return None
        if "lot_size" in m.columns and pd.notnull(m.iloc[0]["lot_size"]):
            return int(m.iloc[0]["lot_size"])
        return None

    def option_contract_fields_for_tradingsymbol(self, tradingsymbol: str) -> Dict[str, Any]:
        ts = safe_str(tradingsymbol).upper().strip()
        if not ts:
            return {}
        row = self._nfo_by_ts_upper.get(ts, {})
        if not isinstance(row, dict) or not row:
            return {
                "tradingsymbol": ts,
                "expiry": "",
                "strike": None,
                "right": ("CE" if ts.endswith("CE") else ("PE" if ts.endswith("PE") else "")),
            }
        expiry_raw = row.get("expiry", "")
        expiry_dt = pd.to_datetime(expiry_raw, errors="coerce")
        expiry_txt = expiry_dt.date().isoformat() if pd.notna(expiry_dt) else safe_str(expiry_raw)
        strike_val = safe_float(row.get("strike", None))
        right_txt = safe_str(row.get("instrument_type", "")).upper().strip()
        if right_txt not in ("CE", "PE"):
            right_txt = "CE" if ts.endswith("CE") else ("PE" if ts.endswith("PE") else "")
        return {
            "tradingsymbol": safe_str(row.get("tradingsymbol", ts)).upper().strip() or ts,
            "expiry": expiry_txt,
            "strike": strike_val,
            "right": right_txt,
        }

class OptionsBrainEngineCSV:
    def __init__(self, cfg: Dict[str, Any]):
        self.cfg = cfg
        self.kite, self.access_token = make_kite_client(
            cfg["KITE_API_KEY"],
            cfg["KITE_API_SECRET"],
            cfg["TOKEN_FILE"],
            auth_mode=cfg.get("KITE_AUTH_MODE", "auto"),
            request_token_env=cfg.get("KITE_REQUEST_TOKEN", ""),
            open_browser=bool(cfg.get("KITE_OPEN_BROWSER", True)),
        )
        ensure_live_csv(cfg["CSV_LIVE_PATH"])
        ensure_actions_csv(cfg["CSV_ACTIONS_PATH"])
        ensure_summary_csv(cfg["CSV_SUMMARY_PATH"])
        self.uni = KiteUniverse(self.kite)
        self.book_15m = CandleBook(cfg["CANDLE_MINUTES"])
        self.book_60m = CandleBook(cfg["HOURLY_CANDLE_MINUTES"])
        self.kws = KiteTicker(cfg["KITE_API_KEY"], self.access_token)
        self.kws.on_connect = self._on_connect
        self.kws.on_ticks = self._on_ticks
        self.kws.on_close = self._on_close
        self.kws.on_error = self._on_error
        self.underlying_tokens: Dict[str, int] = {}
        self.index_volume_proxy_tokens: Dict[str, Optional[int]] = {}
        self.index_volume_proxy_quote_symbols: Dict[str, Optional[str]] = {}
        self._nfo_name_by_tradingsymbol: Dict[str, str] = {}
        self.option_chain_tokens: Dict[str, Dict[str, Any]] = {}
        self.option_chain_meta: Dict[str, Dict[str, Any]] = {}
        self.stream_tokens: List[int] = []
        self._state_lock = threading.Lock()
        self._state_dirty = False
        self.oi_snapshot: Dict[str, Dict[str, float]] = self._load_oi_snapshot(self.cfg.get("OI_SNAPSHOT_PATH", "oi_snapshot_cache.json"))
        self.signal_state: Dict[str, Dict[str, Any]] = self._load_signal_state(self.cfg.get("SIGNAL_STATE_PATH", "signal_state_cache.json"))
        live_seed_state = self._bootstrap_signal_state_from_live(self.cfg.get("CSV_LIVE_PATH", "trade_ideas_live.csv"))
        added_seed = 0
        for sym, st in live_seed_state.items():
            if sym not in self.signal_state:
                self.signal_state[sym] = st
                added_seed += 1
        if added_seed > 0:
            self._state_dirty = True
            logger.info(f"Bootstrapped sticky signal state for {added_seed} symbols from live CSV")
        self.avg20d_volume_cache: Dict[str, float] = {}
        self.typical_intraday_cache: Dict[str, Dict[str, float]] = {}
        self.prior_day_cache: Dict[str, tuple] = {}  # key -> (high, low)
        self._oi_cache: Dict[str, Dict[str, Any]] = {}  # last successful OI per symbol
        self._stop_flag = threading.Event()
        self._consecutive_api_failures: int = 0
        self._throttled_log_ts: Dict[str, float] = {}
        self._fail_counts: Dict[str, int] = {}
        self._cooldown_until: Dict[str, datetime] = {}
        self._api_lock = threading.Lock()
        self._last_api_call_ts = 0.0
        self._vm_shutdown_initiated = False
        self._vm_shutdown_last_attempt_ts = 0.0
        self.carry_forward_lots: Dict[str, int] = {}
        self.carry_forward_long_lots: Dict[str, int] = {}
        self.carry_forward_short_lots: Dict[str, int] = {}
        self.carry_forward_long_contract: Dict[str, Dict[str, Any]] = {}
        self.carry_forward_short_contract: Dict[str, Dict[str, Any]] = {}
        self.tg_bridge = TelegramApprovalBridge(cfg, cfg["CSV_ACTIONS_PATH"])
        self._build_nfo_name_lookup()
        self._prepare_universe()
        self._refresh_carry_forward_positions(force_log=True, log_manual_map=True)
        self._seed_all_underlyings_history(days=cfg["SEED_HISTORY_DAYS"])
        if self.tg_bridge.enabled:
            logger.info("Telegram approvals enabled")
            threading.Thread(target=self._telegram_loop, daemon=True).start()
        else:
            reason = safe_str(getattr(self.tg_bridge, "disabled_reason", ""))
            logger.info(
                f"Telegram approvals disabled{(': ' + reason) if reason else ''}"
            )
        threading.Thread(target=self._approval_loop, daemon=True).start()
        threading.Thread(target=self._idea_loop, daemon=True).start()

    def _nse_tradingsymbol(self, underlying_key: str) -> str: return underlying_key
    def _nfo_name(self, underlying_key: str) -> str:
        if underlying_key == "NIFTY 50": return "NIFTY"
        if underlying_key == "NIFTY BANK": return "BANKNIFTY"
        return underlying_key

    def _build_nfo_name_lookup(self):
        try:
            df = self.uni.instr_nfo
            if df is None or df.empty:
                self._nfo_name_by_tradingsymbol = {}
                return
            out: Dict[str, str] = {}
            for _, row in df.iterrows():
                ts = safe_str(row.get("tradingsymbol", "")).upper().strip()
                nm = safe_str(row.get("name", "")).upper().strip()
                if ts and nm:
                    out[ts] = nm
            self._nfo_name_by_tradingsymbol = out
        except Exception as e:
            logger.debug(f"Failed building NFO tradingsymbol lookup: {e}")
            self._nfo_name_by_tradingsymbol = {}

    def _position_matches_underlying(self, underlying_key: str, pos_row: Dict[str, Any]) -> bool:
        target = safe_str(self._nfo_name(underlying_key)).upper().strip()
        if not target:
            return False

        ts = safe_str((pos_row or {}).get("tradingsymbol", "")).upper().strip()
        if not ts:
            return False

        pos_name = safe_str((pos_row or {}).get("name", "")).upper().strip()
        if pos_name and pos_name == target:
            return True

        lookup_name = self._nfo_name_by_tradingsymbol.get(ts, "")
        if lookup_name and lookup_name == target:
            return True

        return ts.startswith(target)

    def _resolve_underlying_key(self, raw_symbol: str) -> Optional[str]:
        sym = safe_str(raw_symbol).upper().strip()
        if not sym:
            return None
        for key in self.underlying_tokens.keys():
            if safe_str(key).upper().strip() == sym:
                return key
        # Accept near matches like ADANIPORT -> ADANIPORTS
        for key in sorted(self.underlying_tokens.keys(), key=lambda x: len(safe_str(x)), reverse=True):
            k = safe_str(key).upper().strip()
            if (k == sym) or (k.startswith(sym)) or (sym.startswith(k)):
                return key
        return None

    def _guess_underlying_from_option_ts(self, tradingsymbol: str) -> Optional[str]:
        ts = safe_str(tradingsymbol).upper().strip()
        if not ts:
            return None
        if ts.startswith("BANKNIFTY"):
            return "NIFTY BANK" if "NIFTY BANK" in self.underlying_tokens else self._resolve_underlying_key("BANKNIFTY")
        if ts.startswith("NIFTY"):
            return "NIFTY 50" if "NIFTY 50" in self.underlying_tokens else self._resolve_underlying_key("NIFTY")
        for key in sorted(self.underlying_tokens.keys(), key=lambda x: len(safe_str(x)), reverse=True):
            k = safe_str(key).upper().strip()
            if k and ts.startswith(k):
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
            txt = safe_str(expiry_text).replace("-", "").strip()
            return txt if (txt.isdigit() and len(txt) == 8) else "99999999"

        def _pick_contract(
            dest: Dict[str, Dict[str, Any]],
            sym: str,
            qty_abs: int,
            contract: Dict[str, Any],
        ) -> None:
            ts = safe_str((contract or {}).get("tradingsymbol", "")).upper().strip()
            if not ts:
                return
            candidate = {
                "qty": int(max(0, qty_abs)),
                "expiry": safe_str((contract or {}).get("expiry", "")).strip(),
                "strike": safe_float((contract or {}).get("strike", None)),
                "right": safe_str((contract or {}).get("right", "")).upper().strip(),
                "tradingsymbol": ts,
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

        try:
            pos_payload = self._kite_call("positions", self.kite.positions)
            net_positions = pos_payload.get("net", []) if isinstance(pos_payload, dict) else []
            for p in (net_positions if isinstance(net_positions, list) else []):
                exch = safe_str((p or {}).get("exchange", "")).upper().strip()
                ts = safe_str((p or {}).get("tradingsymbol", "")).upper().strip()
                if exch != "NFO" or (not ts) or (not (ts.endswith("CE") or ts.endswith("PE"))):
                    continue
                product = safe_str((p or {}).get("product", "")).upper().strip()
                overnight_qty = safe_int((p or {}).get("overnight_quantity", 0), 0)
                qty = safe_int((p or {}).get("quantity", 0), 0)
                # Prefer true overnight carry. If absent, fall back to currently open non-MIS qty
                # so intraday-open positions that user intends to carry are still tracked.
                if overnight_qty != 0:
                    open_qty = int((1 if overnight_qty > 0 else -1) * min(abs(qty), abs(overnight_qty)))
                elif qty != 0 and product != "MIS":
                    open_qty = qty
                else:
                    open_qty = 0
                if open_qty == 0:
                    continue
                underlying = self._guess_underlying_from_option_ts(ts)
                if not underlying:
                    continue
                lot_size = self.uni.lot_size_for_tradingsymbol(ts) or safe_int((p or {}).get("lot_size", 0), 0)
                abs_qty = abs(int(open_qty))
                lots = int(abs_qty // lot_size) if lot_size > 0 else int(abs_qty)
                if lots <= 0 and abs_qty > 0:
                    lots = 1
                if lots > 0:
                    contract = self.uni.option_contract_fields_for_tradingsymbol(ts)
                    if open_qty > 0:
                        broker_long_map[underlying] = broker_long_map.get(underlying, 0) + lots
                        _pick_contract(broker_long_contract, underlying, lots, contract)
                    else:
                        broker_short_map[underlying] = broker_short_map.get(underlying, 0) + lots
                        _pick_contract(broker_short_contract, underlying, lots, contract)
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

    def _ensure_index_volume_proxy(self, underlying_key: str):
        if underlying_key in self.index_volume_proxy_tokens:
            return
        if underlying_key not in self.cfg.get("INDEX_UNDERLYINGS", []):
            self.index_volume_proxy_tokens[underlying_key] = None
            self.index_volume_proxy_quote_symbols[underlying_key] = None
            return

        nfo_name = self._nfo_name(underlying_key)
        df = self.uni.instr_nfo
        if df is None or df.empty:
            self.index_volume_proxy_tokens[underlying_key] = None
            self.index_volume_proxy_quote_symbols[underlying_key] = None
            return

        fut = df[(df["name"] == nfo_name) & (df["instrument_type"] == "FUT")].copy()
        if fut.empty:
            logger.warning(f"No NFO futures found for index volume proxy: {underlying_key}")
            self.index_volume_proxy_tokens[underlying_key] = None
            self.index_volume_proxy_quote_symbols[underlying_key] = None
            return

        fut["expiry_date"] = pd.to_datetime(fut["expiry"], errors="coerce").dt.date
        fut = fut.dropna(subset=["expiry_date"])
        if fut.empty:
            self.index_volume_proxy_tokens[underlying_key] = None
            self.index_volume_proxy_quote_symbols[underlying_key] = None
            return

        today = now_ist().date()
        fut_live = fut[fut["expiry_date"] >= today].sort_values("expiry_date")
        row = fut_live.iloc[0] if not fut_live.empty else fut.sort_values("expiry_date").iloc[-1]

        token = safe_int(row.get("instrument_token"), 0)
        tradingsymbol = safe_str(row.get("tradingsymbol"))
        if token <= 0 or not tradingsymbol:
            self.index_volume_proxy_tokens[underlying_key] = None
            self.index_volume_proxy_quote_symbols[underlying_key] = None
            return

        self.index_volume_proxy_tokens[underlying_key] = token
        self.index_volume_proxy_quote_symbols[underlying_key] = f"NFO:{tradingsymbol}"
        logger.info(
            f"Index volume proxy for {underlying_key}: {tradingsymbol} (token={token})"
        )

    def _volume_proxy_quote_symbol(self, underlying_key: str) -> Optional[str]:
        if underlying_key not in self.cfg.get("INDEX_UNDERLYINGS", []):
            return None
        self._ensure_index_volume_proxy(underlying_key)
        return self.index_volume_proxy_quote_symbols.get(underlying_key)

    def _volume_analysis_token(self, underlying_key: str) -> Optional[int]:
        if underlying_key not in self.cfg.get("INDEX_UNDERLYINGS", []):
            return self.underlying_tokens.get(underlying_key)
        self._ensure_index_volume_proxy(underlying_key)
        proxy_token = self.index_volume_proxy_tokens.get(underlying_key)
        if proxy_token:
            return proxy_token
        return self.underlying_tokens.get(underlying_key)

    def _norm_symbol(self, symbol: Any) -> str:
        return safe_str(symbol).upper().strip()

    def _read_json_file(self, path: str) -> Dict[str, Any]:
        try:
            if not path or (not os.path.exists(path)) or os.path.getsize(path) == 0:
                return {}
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            return data if isinstance(data, dict) else {}
        except Exception as e:
            logger.warning(f"Failed to read state file {path}: {e}")
            return {}

    def _write_json_atomic(self, path: str, payload: Dict[str, Any]):
        if not path:
            return
        tmp = f"{path}.tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=True, separators=(",", ":"))
        os.replace(tmp, path)

    def _load_oi_snapshot(self, path: str) -> Dict[str, Dict[str, float]]:
        raw = self._read_json_file(path)
        out: Dict[str, Dict[str, float]] = {}
        for sym, snapshot in raw.items():
            if not isinstance(snapshot, dict):
                continue
            norm_sym = self._norm_symbol(sym)
            if not norm_sym:
                continue
            clean: Dict[str, float] = {}
            for k, v in snapshot.items():
                fv = safe_float(v)
                if fv is not None:
                    clean[safe_str(k)] = float(fv)
            if clean:
                out[norm_sym] = clean
        if out:
            logger.info(f"Loaded OI snapshot cache for {len(out)} symbols from {path}")
        return out

    def _load_signal_state(self, path: str) -> Dict[str, Dict[str, Any]]:
        raw = self._read_json_file(path)
        out: Dict[str, Dict[str, Any]] = {}
        for sym, state in raw.items():
            if not isinstance(state, dict):
                continue
            norm_sym = self._norm_symbol(sym)
            stype = safe_str(state.get("type")).upper()
            level = safe_float(state.get("level"))
            if not norm_sym or stype not in {"BREAKOUT", "BREAKDOWN"} or level is None:
                continue
            out[norm_sym] = {
                "type": stype,
                "level": float(level),
                "confirmed_at": safe_str(state.get("confirmed_at")),
            }
        if out:
            logger.info(f"Loaded sticky signal state for {len(out)} symbols from {path}")
        return out

    def _bootstrap_signal_state_from_live(self, live_path: str) -> Dict[str, Dict[str, Any]]:
        """
        Restore sticky signal state from live CSV so restarts don't lose valid
        confirmed breaks that are still not invalidated by latest price.
        """
        try:
            if (not live_path) or (not os.path.exists(live_path)) or os.path.getsize(live_path) == 0:
                return {}
            live = pd.read_csv(live_path, dtype=str, keep_default_na=False)
            if live.empty:
                return {}

            needed = {"symbol", "timestamp_ist", "ltp", "breakout_status", "breakdown_status", "key_resistance", "key_support"}
            if not needed.issubset(set(live.columns)):
                return {}

            live["sym"] = live["symbol"].fillna("").astype(str).str.upper().str.strip()
            live = live[live["sym"] != ""].copy()
            if live.empty:
                return {}

            live["ts"] = pd.to_datetime(live["timestamp_ist"], errors="coerce")
            live = live.sort_values(by=["sym", "ts", "timestamp_ist"], ascending=[True, False, False])

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
                            "confirmed_at": safe_str(ev.get("timestamp_ist")),
                        }
                elif safe_str(ev.get("breakout_status")).upper() == "CONFIRMED":
                    level = safe_float(ev.get("key_resistance"))
                    if level is not None and latest_ltp is not None and latest_ltp >= level:
                        state[sym] = {
                            "type": "BREAKOUT",
                            "level": float(level),
                            "confirmed_at": safe_str(ev.get("timestamp_ist")),
                        }

            return state
        except Exception as e:
            logger.warning(f"Failed to bootstrap sticky signal state from {live_path}: {e}")
            return {}

    def _flush_state_cache(self, force: bool = False):
        with self._state_lock:
            if (not force) and (not self._state_dirty):
                return
            oi_payload = {
                sym: {k: float(v) for k, v in snap.items()}
                for sym, snap in self.oi_snapshot.items()
            }
            signal_payload = {
                sym: {
                    "type": safe_str(state.get("type")).upper(),
                    "level": float(safe_float(state.get("level"), 0.0) or 0.0),
                    "confirmed_at": safe_str(state.get("confirmed_at")),
                }
                for sym, state in self.signal_state.items()
                if isinstance(state, dict)
            }
            self._write_json_atomic(self.cfg.get("OI_SNAPSHOT_PATH", "oi_snapshot_cache.json"), oi_payload)
            self._write_json_atomic(self.cfg.get("SIGNAL_STATE_PATH", "signal_state_cache.json"), signal_payload)
            self._state_dirty = False

    def _apply_sticky_break_state(
        self,
        underlying_key: str,
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
        sym = self._norm_symbol(underlying_key)
        if not sym:
            return out

        now_dt = now_ist()
        now_ts = now_dt.strftime("%Y-%m-%d %H:%M:%S")
        max_sticky_candles = max(0, int(self.cfg.get("STICKY_KEY_LEVEL_MAX_CANDLES", 3)))
        candle_minutes = max(1, int(self.cfg.get("CANDLE_MINUTES", 15)))
        breakout = safe_str(out.get("breakout_status", "None"))
        breakdown = safe_str(out.get("breakdown_status", "None"))

        with self._state_lock:
            # New confirmed event overwrites prior state.
            if breakout == "Confirmed" and key_res is not None:
                self.signal_state[sym] = {"type": "BREAKOUT", "level": float(key_res), "confirmed_at": now_ts}
                self._state_dirty = True
                out["_fresh_confirmed"] = True
                return out
            if breakdown == "Confirmed" and key_sup is not None:
                self.signal_state[sym] = {"type": "BREAKDOWN", "level": float(key_sup), "confirmed_at": now_ts}
                self._state_dirty = True
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
                    confirmed_dt = datetime.strptime(confirmed_raw, "%Y-%m-%d %H:%M:%S").replace(tzinfo=IST)
                except Exception:
                    confirmed_dt = None
            age_candles = -1
            if confirmed_dt is not None:
                age_candles = max(0, int((now_dt - confirmed_dt).total_seconds() // (60 * candle_minutes)))
            sticky_is_fresh = (age_candles >= 0) and (age_candles <= max_sticky_candles)

            # Invalidation: price reclaims/loses the original break level or opposite side confirms.
            if stype == "BREAKDOWN":
                if breakout == "Confirmed" or ltp > level:
                    self.signal_state.pop(sym, None)
                    self._state_dirty = True
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
                    self._state_dirty = True
                    return out
                out["breakout_status"] = "Confirmed"
                out["breakdown_status"] = "None"
                out["_sticky_confirmed"] = True
                out["_sticky_is_fresh"] = sticky_is_fresh
                out["_sticky_age_candles"] = age_candles
                return out

        return out

    def _refresh_option_chain_for_underlying(self, underlying_key: str, ltp: float) -> bool:
        """Refresh sampled option chain around current ATM for a symbol."""
        nfo_name = self._nfo_name(underlying_key)
        expiry_count = max(1, int(self.cfg.get("OI_EXPIRY_COUNT", 1)))
        expiries = self.uni.nearest_expiry_list(nfo_name, expiry_count)
        if not expiries:
            return False
        combined_chain: Dict[str, Any] = {
            "atm_strike": None,
            "expiry": "",
            "expiry_used": "",
            "expiries": [],
            "strike_step": None,
            "ce": [],
            "pe": [],
        }
        for exp in expiries:
            chain = self.uni.select_atm_option_tokens(
                nfo_name, ltp, exp, self.cfg["NEAR_ATM_STRIKES_EACH_SIDE"]
            )
            if not chain:
                continue
            if combined_chain["atm_strike"] is None:
                combined_chain["atm_strike"] = chain.get("atm_strike")
                combined_chain["strike_step"] = chain.get("strike_step")
                combined_chain["expiry"] = chain.get("expiry", "")
            combined_chain["ce"].extend(chain.get("ce", []))
            combined_chain["pe"].extend(chain.get("pe", []))
            expiry_txt = safe_str(chain.get("expiry", ""))
            if expiry_txt:
                combined_chain["expiries"].append(expiry_txt)

        if (not combined_chain["ce"]) and (not combined_chain["pe"]):
            return False
        if combined_chain["expiry_used"] == "":
            expiry_list = [e for e in combined_chain["expiries"] if e]
            combined_chain["expiry_used"] = ", ".join(expiry_list) if expiry_list else safe_str(combined_chain["expiry"])
        self.option_chain_tokens[underlying_key] = combined_chain
        self.option_chain_meta[underlying_key] = {
            "updated_at": now_ist(),
            "nfo_name": nfo_name,
        }
        return True

    def _maybe_refresh_option_chain(self, underlying_key: str, ltp: Optional[float]):
        """Refresh chain when stale or ATM has drifted significantly."""
        if ltp is None:
            return

        chain = self.option_chain_tokens.get(underlying_key)
        meta = self.option_chain_meta.get(underlying_key, {})
        refresh_minutes = max(1, int(self.cfg.get("OI_CHAIN_REFRESH_MINUTES", 30)))
        drift_steps = max(1, int(self.cfg.get("OI_CHAIN_REFRESH_ATM_STEPS", 2)))

        needs_refresh = chain is None or not chain
        updated_at = meta.get("updated_at")
        if not needs_refresh and isinstance(updated_at, datetime):
            if (now_ist() - updated_at).total_seconds() >= refresh_minutes * 60:
                needs_refresh = True

        if not needs_refresh and chain:
            step = safe_float(chain.get("strike_step")) or 0
            atm_old = safe_float(chain.get("atm_strike"))
            if step > 0 and atm_old is not None:
                atm_now = clamp_round_to_step(ltp, step)
                if abs(atm_now - atm_old) >= (step * drift_steps):
                    needs_refresh = True

        if needs_refresh:
            self._refresh_option_chain_for_underlying(underlying_key, ltp)

    def _prepare_universe(self):
        for idx in self.cfg["INDEX_UNDERLYINGS"]:
            tok = self.uni.find_underlying_token_nse(idx)
            if tok is not None:
                self.underlying_tokens[idx] = tok
            else:
                logger.warning(f"Index underlying '{idx}' not found on NSE, skipping (BSE-only indices like SENSEX are not supported)")
        for sym in self.cfg["STOCK_UNDERLYINGS"]:
            tok = self.uni.find_underlying_token_nse(sym)
            if tok is not None:
                self.underlying_tokens[sym] = tok
            else:
                logger.warning(f"Stock underlying '{sym}' not found on NSE, skipping")
        logger.info(f"Universe prepared: {len(self.underlying_tokens)} underlyings resolved out of {len(self.cfg['INDEX_UNDERLYINGS']) + len(self.cfg['STOCK_UNDERLYINGS'])} configured")
        if not self.underlying_tokens:
            raise RuntimeError("No underlyings found.")
        for idx in self.cfg["INDEX_UNDERLYINGS"]:
            if idx in self.underlying_tokens:
                self._ensure_index_volume_proxy(idx)
        quote_keys = [f"NSE:{self._nse_tradingsymbol(k)}" for k in self.underlying_tokens.keys()]
        quotes = self._kite_call("quote", self.kite.quote, quote_keys)
        for u in self.underlying_tokens.keys():
            sym = f"NSE:{self._nse_tradingsymbol(u)}"
            ltp = safe_float(quotes.get(sym, {}).get("last_price"))
            if ltp is None: continue
            self._refresh_option_chain_for_underlying(u, ltp)
        toks = set(self.underlying_tokens.values())
        for idx in self.cfg["INDEX_UNDERLYINGS"]:
            proxy_tok = self.index_volume_proxy_tokens.get(idx)
            if proxy_tok:
                toks.add(int(proxy_tok))
        mode = self.cfg.get("STREAM_MODE", "ATM_ONLY").upper()
        if mode == "ATM_ONLY":
            for _, chain in self.option_chain_tokens.items():
                atm = chain.get("atm_strike")
                ce_tok = pe_tok = None
                for (_, tok, strike) in chain.get("ce", []):
                    if atm is not None and int(strike) == int(atm): ce_tok = tok; break
                for (_, tok, strike) in chain.get("pe", []):
                    if atm is not None and int(strike) == int(atm): pe_tok = tok; break
                if ce_tok is None and chain.get("ce"): ce_tok = chain["ce"][len(chain["ce"]) // 2][1]
                if pe_tok is None and chain.get("pe"): pe_tok = chain["pe"][len(chain["pe"]) // 2][1]
                if ce_tok is not None: toks.add(int(ce_tok))
                if pe_tok is not None: toks.add(int(pe_tok))
        else:
            for _, chain in self.option_chain_tokens.items():
                for (_, tok, _) in chain.get("ce", []): toks.add(int(tok))
                for (_, tok, _) in chain.get("pe", []): toks.add(int(tok))
        toks_list = sorted(toks)
        if len(toks_list) > int(self.cfg.get("MAX_WS_TOKENS", 3900)):
            toks_list = toks_list[:int(self.cfg.get("MAX_WS_TOKENS", 3900))]
        self.stream_tokens = toks_list
        print(f"Streaming {len(self.stream_tokens)} tokens.")

    def _seed_all_underlyings_history(self, days: int):
        items = list(self.underlying_tokens.items())
        existing_tokens = {tok for _, tok in items}
        for idx in self.cfg["INDEX_UNDERLYINGS"]:
            proxy_tok = self.index_volume_proxy_tokens.get(idx)
            if proxy_tok and proxy_tok not in existing_tokens:
                items.append((f"{idx}__VOL_PROXY", int(proxy_tok)))
                existing_tokens.add(int(proxy_tok))
        total = len(items)
        logger.info(f"Seeding {total} underlyings with {days} days of history (15m + 60m)...")
        for idx, (name, tok) in enumerate(items):
            if self._stop_flag.is_set():
                break
            self._seed_underlying_history(tok, days, self.cfg["CANDLE_MINUTES"], self.book_15m)
            self._seed_underlying_history(tok, days, self.cfg["HOURLY_CANDLE_MINUTES"], self.book_60m)
            if (idx + 1) % 10 == 0 or idx == total - 1:
                logger.info(f"Seeded history: {idx + 1}/{total} underlyings")
            time.sleep(0.35)  # Rate limit: ~3 req/sec for the 2 API calls above

    def _seed_underlying_history(self, token: int, days: int, minutes: int, target_book: CandleBook):
        try:
            end_dt = now_ist()
            start_dt = end_dt - timedelta(days=days)
            hist = self._kite_call(
                "historical_data",
                self.kite.historical_data,
                token, start_dt, end_dt,
                interval=f"{minutes}minute", continuous=False, oi=False
            )
            if not hist: return
            candles: List[Candle] = []
            for bar in hist:
                dt = pd.to_datetime(bar["date"])
                if dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
                dt_ist = dt.astimezone(IST)
                vol = int(bar.get("volume", 0) or 0)
                close = float(bar["close"])
                candles.append(Candle(
                    start_ist=floor_time(dt_ist, minutes),
                    open=float(bar["open"]), high=float(bar["high"]), low=float(bar["low"]), close=close,
                    volume=vol, vwap_num=close * vol, vwap_den=vol
                ))
            target_book.closed[token] = candles
        except Exception as e:
            logger.error(f"Failed to seed history for token {token}: {e}", exc_info=True)

    def start(self):
        self.kws.connect(threaded=False)

    def stop(self):
        self._stop_flag.set()
        try:
            self._flush_state_cache(force=True)
        except Exception as e:
            logger.warning(f"Failed to flush state cache on stop: {e}")
        try:
            self.kws.close()
        except Exception as e:
            logger.warning(f"Error closing WebSocket: {e}")

    def _on_connect(self, ws, response):
        ws.subscribe(self.stream_tokens)
        ws.set_mode(ws.MODE_FULL, self.stream_tokens)
        logger.info(f"WebSocket connected. Subscribed to {len(self.stream_tokens)} tokens.")

    def _on_ticks(self, ws, ticks):
        for t in ticks:
            token = t.get("instrument_token")
            ltp = t.get("last_price")
            ts = t.get("timestamp")
            cum_vol = t.get("volume_traded")
            if token is None or ltp is None or ts is None: continue
            if ts.tzinfo is None: ts = ts.replace(tzinfo=timezone.utc)
            self.book_15m.update(int(token), float(ltp), ts, cum_vol)
            self.book_60m.update(int(token), float(ltp), ts, cum_vol)

    def _on_close(self, ws, code, reason):
        logger.warning(f"WebSocket closed: code={code}, reason={reason}")

    def _on_error(self, ws, code, reason):
        logger.error(f"WebSocket error: code={code}, reason={reason}")

    def avg_20d_daily_volume(self, underlying_key: str, token: int) -> Optional[float]:
        if underlying_key in self.avg20d_volume_cache: return self.avg20d_volume_cache[underlying_key]
        # Fast-fail: skip expensive API call when Kite has been persistently unreachable
        if self._consecutive_api_failures >= 5:
            return None
        try:
            end = now_ist().date(); start = end - timedelta(days=60)
            hist = self._kite_call(
                "historical_data",
                self.kite.historical_data,
                token, start, end,
                interval="day", continuous=False, oi=False
            )
            df = pd.DataFrame(hist)
            if df.empty or "volume" not in df.columns: return None
            vol = df["volume"].tail(20)
            if len(vol) < 5: return None
            avg = float(vol.mean())
            self.avg20d_volume_cache[underlying_key] = avg
            return avg
        except Exception as e:
            logger.debug(f"Failed to calculate 20d avg volume for {underlying_key}: {e}")
            return None

    def typical_intraday_volume_same_time(self, underlying_key: str, token: int) -> Optional[float]:
        hhmm = floor_time(now_ist(), self.cfg["CANDLE_MINUTES"]).strftime("%H:%M")
        cache_key = f"{underlying_key}:{now_ist().date().isoformat()}"
        cached_map = self.typical_intraday_cache.get(cache_key)
        if cached_map is not None:
            return cached_map.get(hhmm)
        # Fast-fail: skip expensive API call when Kite has been persistently unreachable
        if self._consecutive_api_failures >= 5:
            return None
        try:
            end_dt = now_ist(); start_dt = end_dt - timedelta(days=30)
            hist = self._kite_call(
                "historical_data",
                self.kite.historical_data,
                token, start_dt, end_dt,
                interval=f"{self.cfg['CANDLE_MINUTES']}minute", continuous=False, oi=False
            )
            df = pd.DataFrame(hist)
            if df.empty: return None
            df["date"] = pd.to_datetime(df["date"]).dt.tz_convert(IST)
            df["hhmm"] = df["date"].dt.strftime("%H:%M")
            df["d"] = df["date"].dt.date
            if "volume" not in df.columns:
                return None

            recent_days = sorted(df["d"].unique())[-20:]
            sub = df[df["d"].isin(recent_days)]
            if sub.empty:
                return None

            typical_map = (
                sub.groupby("hhmm")["volume"]
                .mean()
                .dropna()
                .astype(float)
                .to_dict()
            )
            self.typical_intraday_cache[cache_key] = typical_map
            return typical_map.get(hhmm)
        except Exception as e:
            logger.debug(f"Failed to calculate typical intraday volume for {underlying_key} at {hhmm}: {e}")
            return None

    def options_positioning(self, underlying_key: str) -> Dict[str, Any]:
        """v3.0 FULL OI ANALYSIS - PCR, Delta OI, Top strikes, OI zones"""
        chain = self.option_chain_tokens.get(underlying_key, {})
        if not chain:
            return {"expiry_used": "", "top_call_oi_strikes": "Unavailable", "top_put_oi_strikes": "Unavailable",
                    "top_call_doi_strikes": "Unavailable", "top_put_doi_strikes": "Unavailable",
                    "pcr_overall": "Unavailable", "pcr_near_atm": "Unavailable",
                    "oi_support_zone": "Unavailable", "oi_resistance_zone": "Unavailable",
                    "oi_shift_notes": "Unavailable", "iv_notes": "Unavailable"}

        quote_symbols = []
        for option_type in ['ce', 'pe']:
            for tsym, tok, strike in chain.get(option_type, []):
                quote_symbols.append(f"NFO:{tsym}")

        quotes = {}
        batch_size = max(50, int(self.cfg.get("OI_QUOTE_BATCH_SIZE", 200)))
        batch_pause_sec = max(0.0, float(self.cfg.get("OI_QUOTE_BATCH_PAUSE_SEC", 0.0)))
        for i in range(0, len(quote_symbols), batch_size):
            batch = quote_symbols[i:i+batch_size]
            try:
                batch_quotes = self._kite_call("quote", self.kite.quote, batch)
                for sym, data in batch_quotes.items():
                    oi = safe_float(data.get('oi', 0))
                    quotes[f"{sym}_oi"] = oi if oi else 0
                if batch_pause_sec > 0 and (i + batch_size) < len(quote_symbols):
                    time.sleep(batch_pause_sec)
            except Exception as e:
                logger.error(f"Quote error: {e}")

        ce_oi = {}
        pe_oi = {}
        for tsym, tok, strike in chain.get('ce', []):
            oi = quotes.get(f"NFO:{tsym}_oi", 0)
            sk = int(strike)
            ce_oi[sk] = ce_oi.get(sk, 0) + oi

        for tsym, tok, strike in chain.get('pe', []):
            oi = quotes.get(f"NFO:{tsym}_oi", 0)
            sk = int(strike)
            pe_oi[sk] = pe_oi.get(sk, 0) + oi

        sym = self._norm_symbol(underlying_key)
        with self._state_lock:
            prev_oi_snapshot = dict(self.oi_snapshot.get(sym, {}))
        
        current_oi_snapshot = {}
        for strike, oi in ce_oi.items():
            current_oi_snapshot[f"ce_{strike}"] = oi
        for strike, oi in pe_oi.items():
            current_oi_snapshot[f"pe_{strike}"] = oi
        
        with self._state_lock:
            self.oi_snapshot[sym] = current_oi_snapshot
            self._state_dirty = True

        ce_doi = {}
        pe_doi = {}
        
        for strike, curr_oi in ce_oi.items():
            prev_oi = prev_oi_snapshot.get(f"ce_{strike}", curr_oi)
            ce_doi[strike] = curr_oi - prev_oi
        
        for strike, curr_oi in pe_oi.items():
            prev_oi = prev_oi_snapshot.get(f"pe_{strike}", curr_oi)
            pe_doi[strike] = curr_oi - prev_oi

        top_call_doi = sorted(ce_doi.items(), key=lambda x: abs(x[1]), reverse=True)[:3]
        top_put_doi = sorted(pe_doi.items(), key=lambda x: abs(x[1]), reverse=True)[:3]

        top_call_doi_strikes = " | ".join([f"{strike}:{int(delta):+,}" for strike, delta in top_call_doi]) if top_call_doi else "Unavailable"
        top_put_doi_strikes = " | ".join([f"{strike}:{int(delta):+,}" for strike, delta in top_put_doi]) if top_put_doi else "Unavailable"

        top_call_oi = sorted(ce_oi.items(), key=lambda x: x[1], reverse=True)[:3]
        top_put_oi = sorted(pe_oi.items(), key=lambda x: x[1], reverse=True)[:3]

        top_call_oi_strikes = " | ".join([f"{strike}:{int(oi):,}" for strike, oi in top_call_oi]) if top_call_oi else "Unavailable"
        top_put_oi_strikes = " | ".join([f"{strike}:{int(oi):,}" for strike, oi in top_put_oi]) if top_put_oi else "Unavailable"

        total_call_oi = sum(ce_oi.values())
        total_put_oi = sum(pe_oi.values())
        pcr_overall = round(total_put_oi / total_call_oi, 2) if total_call_oi > 0 else "Unavailable"

        atm = chain.get('atm_strike', 0)
        step = chain.get('strike_step', 50)
        near_call_oi = sum(oi for strike, oi in ce_oi.items() if abs(strike - atm) <= step)
        near_put_oi = sum(oi for strike, oi in pe_oi.items() if abs(strike - atm) <= step)
        pcr_near_atm = round(near_put_oi / near_call_oi, 2) if near_call_oi > 0 else "Unavailable"

        call_strikes = [strike for strike, _ in top_call_oi]
        put_strikes = [strike for strike, _ in top_put_oi]
        oi_resistance_zone = f"{min(call_strikes)}-{max(call_strikes)}" if call_strikes else "Unavailable"
        oi_support_zone = f"{min(put_strikes)}-{max(put_strikes)}" if put_strikes else "Unavailable"

        oi_shift_notes = []
        for strike, doi in sorted(ce_doi.items()):
            if abs(doi) > 10000:
                oi_shift_notes.append(f"CE {strike}: {int(doi):+,}")
        for strike, doi in sorted(pe_doi.items()):
            if abs(doi) > 10000:
                oi_shift_notes.append(f"PE {strike}: {int(doi):+,}")

        oi_shift_notes_text = " | ".join(oi_shift_notes) if oi_shift_notes else "No major shifts"

        return {
            "expiry_used": chain.get('expiry_used', chain.get('expiry', '')),
            "top_call_oi_strikes": top_call_oi_strikes,
            "top_put_oi_strikes": top_put_oi_strikes,
            "top_call_doi_strikes": top_call_doi_strikes,
            "top_put_doi_strikes": top_put_doi_strikes,
            "pcr_overall": pcr_overall,
            "pcr_near_atm": pcr_near_atm,
            "oi_support_zone": oi_support_zone,
            "oi_resistance_zone": oi_resistance_zone,
            "oi_shift_notes": oi_shift_notes_text,
            "iv_notes": "Future IV analysis"
        }

    def underlying_snapshot(self, underlying_key: str) -> Dict[str, Any]:
        ts = now_ist()
        sym = f"NSE:{self._nse_tradingsymbol(underlying_key)}"
        q: Dict[str, Any] = {}
        q_volume_proxy: Dict[str, Any] = {}
        quote_err: Optional[Exception] = None
        vol_proxy_sym = self._volume_proxy_quote_symbol(underlying_key)
        quote_keys = [sym]
        if vol_proxy_sym and vol_proxy_sym != sym:
            quote_keys.append(vol_proxy_sym)
        try:
            qmap = self._kite_call("quote", self.kite.quote, quote_keys)
            q = qmap.get(sym, {})
            if vol_proxy_sym:
                q_volume_proxy = qmap.get(vol_proxy_sym, {})
        except Exception as e:
            quote_err = e
            if self._is_transient_network_error(e):
                self._log_throttled(
                    f"quote_fallback:{underlying_key}",
                    (
                        f"Quote API failed for {underlying_key}; using websocket/candle fallback when available: "
                        f"{type(e).__name__}: {e}"
                    ),
                    level="warning",
                    interval_sec=20.0,
                )
            else:
                raise
        ltp = safe_float(q.get("last_price"))
        ohlc = q.get("ohlc", {}) if isinstance(q.get("ohlc"), dict) else {}
        prev_close = safe_float(ohlc.get("close"))
        day_high = safe_float(ohlc.get("high"))
        day_low = safe_float(ohlc.get("low"))
        token = self.underlying_tokens.get(underlying_key)

        # Fallback to websocket-driven candles when quote endpoint is temporarily unavailable.
        if token is not None and (ltp is None or prev_close is None or day_high is None or day_low is None):
            cur = self.book_15m.current.get(token)
            closed = self.book_15m.closed.get(token, [])

            if ltp is None:
                if cur is not None:
                    ltp = safe_float(cur.close)
                if ltp is None and closed:
                    ltp = safe_float(closed[-1].close)

            today_bars: List[Candle] = [c for c in closed if c.start_ist.date() == ts.date()]
            if cur is not None and cur.start_ist.date() == ts.date():
                today_bars.append(cur)
            if today_bars:
                highs = [safe_float(c.high) for c in today_bars]
                lows = [safe_float(c.low) for c in today_bars]
                highs = [h for h in highs if h is not None]
                lows = [l for l in lows if l is not None]
                if day_high is None and highs:
                    day_high = max(highs)
                if day_low is None and lows:
                    day_low = min(lows)

            if prev_close is None:
                for c in reversed(closed):
                    c_close = safe_float(c.close)
                    if c_close is not None and c.start_ist.date() < ts.date():
                        prev_close = c_close
                        break
                if prev_close is None and today_bars:
                    earliest = min(today_bars, key=lambda c: c.start_ist)
                    prev_close = safe_float(earliest.open)

        if ltp is None and quote_err is not None:
            # No quote and no local fallback available -> skip symbol for this pass.
            raise quote_err

        pct_change = None
        if ltp is not None and prev_close:
            pct_change = (ltp - prev_close) / prev_close * 100.0
        vwap_relation = "NA"
        if token is not None:
            cur = self.book_15m.current.get(token)
            v = cur.vwap() if cur is not None else None
            # Fallback to latest closed candle VWAP when current bucket has no volume yet.
            if v is None:
                closed = self.book_15m.closed.get(token, [])
                if closed:
                    v = closed[-1].vwap()
            # Last fallback from quote payload when websocket volumes are sparse.
            if v is None:
                v = safe_float(q.get("average_price"))
            if v is not None and ltp is not None:
                vwap_relation = "Above" if ltp >= v else "Below"
        volume = None

        def _quote_volume(qobj: Dict[str, Any]) -> Optional[int]:
            if not isinstance(qobj, dict):
                return None
            for k in ("volume_traded", "volume"):
                v = safe_float(qobj.get(k))
                if v is not None and v > 0:
                    return int(v)
            return None

        # Index symbols often do not publish traded volume; use front-month future as proxy.
        volume = _quote_volume(q) or _quote_volume(q_volume_proxy)
        candle_volume = None
        volume_token = self._volume_analysis_token(underlying_key)
        volume_book_token = volume_token if volume_token is not None else token
        if volume_book_token is not None:
            vols = 0
            for c in self.book_15m.closed.get(volume_book_token, []):
                if c.start_ist.date() == ts.date(): vols += c.volume
            cur = self.book_15m.current.get(volume_book_token)
            if cur and cur.start_ist.date() == ts.date(): vols += cur.volume
            candle_volume = vols
        if volume is None and candle_volume is not None:
            if candle_volume > 0 or underlying_key not in self.cfg.get("INDEX_UNDERLYINGS", []):
                volume = candle_volume
        day_range = f"{day_low:.2f}-{day_high:.2f}" if day_high is not None and day_low is not None else ""
        return {"timestamp_ist": ts.strftime("%Y-%m-%d %H:%M:%S IST"), "session_context": ist_session_context(ts),
                "ltp": ltp, "prev_close": prev_close, "pct_change": round(pct_change,2) if pct_change is not None else None,
                "day_high": day_high, "day_low": day_low, "volume": volume, "vwap_relation": vwap_relation, "day_range": day_range}

    def build_trade_plan(self, underlying_key: str) -> Dict[str, Any]:
        snap = self.underlying_snapshot(underlying_key)
        ltp_now = safe_float(snap.get("ltp"))
        token = self.underlying_tokens.get(underlying_key)
        volume_token = self._volume_analysis_token(underlying_key)
        avg20 = self.avg_20d_daily_volume(underlying_key, volume_token) if volume_token else None
        typical = self.typical_intraday_volume_same_time(underlying_key, volume_token) if volume_token else None
        vol_vs_20d = None
        if snap.get("volume") is not None and avg20 and avg20 > 0:
            vol_vs_20d = safe_float(snap.get("volume")) / avg20

        candles_15 = self.book_15m.closed.get(token, []) if token else []
        struct_15 = compute_structure(candles_15)
        swing_hi, swing_lo = swing_levels(candles_15, self.cfg["SWING_LOOKBACK_BARS"])
        orh, orl = opening_range_levels(candles_15, self.cfg["OPENING_RANGE_MINUTES"])

        candles_60 = self.book_60m.closed.get(token, []) if token else []
        struct_60 = compute_structure(candles_60)

        prior_day_high, prior_day_low = None, None
        if token:
            cache_key = f"{underlying_key}:{now_ist().date().isoformat()}"
            if cache_key in self.prior_day_cache:
                prior_day_high, prior_day_low = self.prior_day_cache[cache_key]
            else:
                try:
                    end = now_ist().date()
                    start = end - timedelta(days=15)
                    hist = self._kite_call(
                        "historical_data",
                        self.kite.historical_data,
                        token, start, end,
                        interval="day"
                    )
                    df = pd.DataFrame(hist or [])
                    if len(df) >= 2:
                        prior = df.iloc[-2]
                        prior_day_high = safe_float(prior.get("high"))
                        prior_day_low = safe_float(prior.get("low"))
                    self.prior_day_cache[cache_key] = (prior_day_high, prior_day_low)
                except Exception as e:
                    logger.debug(f"Failed to fetch prior day high/low for {underlying_key}: {e}")

        volume_book_token = volume_token if volume_token is not None else token
        last15 = (
            self.book_15m.last_closed(volume_book_token, 1)[0]
            if volume_book_token and self.book_15m.last_closed(volume_book_token, 1)
            else None
        )
        vol_last_15 = int(last15.volume) if last15 else 0
        close_last_15 = safe_float(last15.close) if last15 else None

        atr_period = max(2, safe_int(self.cfg.get("KEY_LEVEL_ATR_PERIOD", 14), 14))
        atr_15 = compute_atr(candles_15, atr_period)
        key_level_eval_px = close_last_15 if close_last_15 is not None else (ltp_now or 0.0)
        key_level_eval_source = "close_last_15" if close_last_15 is not None else "ltp_fallback"
        cached_oi_support_zone = ""
        cached_oi_resistance_zone = ""
        with self._state_lock:
            cached_oi = self._oi_cache.get(underlying_key)
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
            underlying_key=underlying_key,
            bb=bb,
            ltp=ltp_now,
            key_res=key_res,
            key_sup=key_sup,
        )

        self._maybe_refresh_option_chain(underlying_key, ltp_now)
        opt = self.options_positioning(underlying_key)
        oi_source = "unavailable"
        # Cache successful OI; fall back to cached data when fresh fetch is unavailable.
        if opt and opt.get("pcr_overall") != "Unavailable":
            with self._state_lock:
                self._oi_cache[underlying_key] = dict(opt)
            oi_source = "fetched"
        else:
            with self._state_lock:
                cached_oi = self._oi_cache.get(underlying_key)
            if cached_oi is not None:
                opt = dict(cached_oi)
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

        news_bullets, news_sources = load_news_from_feed_csv_for_symbol(
            news_csv_path=self.cfg["NEWS_FEED_CSV_PATH"],
            uni=self.uni,
            symbol=("NIFTY" if underlying_key == "NIFTY 50" else ("BANKNIFTY" if underlying_key == "NIFTY BANK" else underlying_key)),
            lookback_minutes=self.cfg["NEWS_LOOKBACK_MINUTES"],
            max_items=self.cfg["NEWS_MAX_ITEMS"]
        )

        news_section = {
            "bullets": news_bullets if news_bullets else ["No matching news found in news_feed.csv (within lookback)."],
            "sources": news_sources if news_sources else [],
        }

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
        if bias == "Buy" and struct_60 == "LH/LL":
            counter_trend_note = "60m trend filter is bearish (LH/LL); buy signal treated as counter-trend -> WAIT."
            bias = "Wait"
            setup_type = "Breakout (counter-trend)"
        elif bias == "Sell" and struct_60 == "HH/HL":
            counter_trend_note = "60m trend filter is bullish (HH/HL); sell signal treated as counter-trend -> WAIT."
            bias = "Wait"
            setup_type = "Breakdown (counter-trend)"

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
                t1 = f"T1 near {t1_level:.2f} (OI resistance zone {opt.get('oi_resistance_zone')})"
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
                t1 = f"T1 near {t1_level:.2f} (OI support zone {opt.get('oi_support_zone')})"
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
            print(f"[OK] Factor 1 (Volume): Strong volume confirmation ({vol_last_15:,} >= {volume_confirm_mult:.2f}x typical)")

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
                print(
                    f"[OK] Factor 2 (Key Level Break): {key_level_ref_label} broke {level_type} "
                    f"at {level_value:.2f} (+/- buffer {key_level_break_buffer:.2f})"
                )
            elif fresh_confirmed:
                print("[OK] Factor 2 (Key Level Break): fresh confirmed break state")
            else:
                print(
                    f"[OK] Factor 2 (Key Level Break): sticky confirmation retained "
                    f"(age={sticky_age_candles} candles, window={sticky_max_candles})"
                )
        elif sticky_confirmed and (not sticky_is_fresh) and enable_output:
            print(
                f"[INFO] Factor 2 (Key Level Break): sticky confirmation expired for scoring "
                f"(age={sticky_age_candles} candles, window={sticky_max_candles})"
            )

        # Factor 3: OI Alignment - DOI Based (+1)
        f_oi = 0
        if bias_for_scoring == "Buy" and doi_sig["direction"] == "BULLISH" and doi_sig["confirmation"] == 1:
            f_oi = 1
            if enable_output:
                print(f"[OK] Factor 3 (OI Alignment - DOI): BULLISH confirmed, net={doi_sig['net']}, conf={doi_sig['confidence']}%")
        elif bias_for_scoring == "Sell" and doi_sig["direction"] == "BEARISH" and doi_sig["confirmation"] == 1:
            f_oi = 1
            if enable_output:
                print(f"[OK] Factor 3 (OI Alignment - DOI): BEARISH confirmed, net={doi_sig['net']}, conf={doi_sig['confidence']}%")

        # Factor 4: Structure Alignment (+1)
        structure_bias = "Buy" if struct_15 == "HH/HL" else ("Sell" if struct_15 == "LH/LL" else "Wait")
        f_structure = 1 if (
            struct_15 == struct_60
            and struct_15 in {"HH/HL", "LH/LL"}
            and structure_bias == bias_for_scoring
        ) else 0
        if f_structure and enable_output:
            print(f"[OK] Factor 4 (Structure): 15m and 60m aligned ({struct_15}) with bias {bias_for_scoring}")

        # Factor 5: Counter-trend Penalty (-2)
        f_counter = -2 if counter_trend_note else 0
        if f_counter and enable_output:
            print(f"[WARN] Factor 5 (Counter-trend Penalty): -2 points. {counter_trend_note}")

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
            print(f"FINAL SCORE: {confidence_score}/4 -> Confidence: {confidence}")
            if f_oi_stale_decisive_block:
                print(
                    "[WARN] Stale OI decisive block: cached/unavailable OI pushed score across "
                    f"actionable threshold {actionable_min_conf} (score_without_oi={score_without_oi})."
                )

        wcm = [
            "15m close back inside key level (invalidation)",
            "No volume expansion / momentum stall",
            "OI/PCR shifts against the direction near key strikes",
            "Unexpected event headline increases volatility",
        ]

        if counter_trend_note:
            wcm.insert(0, counter_trend_note)

        plan = {
            "Snapshot (IST timestamp)": {
                "timestamp_ist": snap.get("timestamp_ist"),
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
                "Educational notice": "Educational analysis only; not a SEBI-registered advisor. No guaranteed outcomes.",
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
                "Price/volume": "Kite Connect (quotes + websocket ticks + historical_data)",
                "Options OI": "Kite Connect (quote snapshots across sampled strikes)",
                "News": news_sources if news_sources else "news_feed.csv (no URLs matched)",
            },
            "_oi_source": oi_source,
        }

        return plan

    def _kite_call(self, label: str, fn, *args, **kwargs):
        retries = max(1, int(self.cfg.get("API_RETRIES", 3)))
        backoff = float(self.cfg.get("API_RETRY_BACKOFF_SECONDS", 0.7))
        min_interval = max(0.0, float(self.cfg.get("API_MIN_INTERVAL_SEC", 0.08)))

        # Adaptive: reduce retries when Kite API has been persistently
        # unreachable (e.g. after market close or network outage).
        if self._consecutive_api_failures >= 5:
            retries = min(retries, 1)

        last_err = None
        for attempt in range(1, retries + 1):
            try:
                # Shared API pacing: protects against bursty calls across worker threads.
                if min_interval > 0:
                    with self._api_lock:
                        now_mono = time.monotonic()
                        wait = min_interval - (now_mono - self._last_api_call_ts)
                        if wait > 0:
                            time.sleep(wait)
                        self._last_api_call_ts = time.monotonic()
                result = fn(*args, **kwargs)
                self._consecutive_api_failures = 0  # success resets counter
                return result
            except (ConnectionError, OSError) as e:
                # Connection dropped - use longer backoff to let it recover
                last_err = e
                self._consecutive_api_failures += 1
                if attempt < retries:
                    wait = backoff * attempt * 3
                    logger.warning(f"Kite API connection error on {label} (attempt {attempt}/{retries}), retrying in {wait:.1f}s: {type(e).__name__}")
                    time.sleep(wait)
            except Exception as e:
                last_err = e
                if attempt < retries:
                    time.sleep(backoff * attempt)
        if last_err is not None:
            raise last_err
        raise RuntimeError(f"Kite API call failed for {label}")

    def _is_transient_network_error(self, err: Exception) -> bool:
        """Best-effort classifier for transport/API connectivity issues."""
        def _txt(x: Any) -> str:
            try:
                return str(x).strip().lower()
            except Exception:
                return ""

        transient_terms = (
            "connection aborted",
            "remote end closed connection",
            "timed out",
            "timeout",
            "connection reset",
            "temporary failure",
            "temporarily unavailable",
            "max retries exceeded",
            "name or service not known",
            "dns",
            "broken pipe",
            "econn",
        )
        transient_names = {
            "connectionerror",
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
            if isinstance(cur, (ConnectionError, TimeoutError, OSError)):
                return True
            mod = _txt(getattr(type(cur), "__module__", ""))
            name = _txt(getattr(type(cur), "__name__", ""))
            msg = _txt(cur)
            if (
                "requests" in mod
                or "urllib3" in mod
                or "http.client" in mod
                or "socket" in mod
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

    def _handle_plan_failure(self, sym: str, err: Exception):
        self._record_failure(sym)
        if self._is_transient_network_error(err):
            self._log_throttled(
                f"plan_net_err:{sym}",
                (
                    f"Plan generation skipped for {sym} due to transient API/network issue: "
                    f"{type(err).__name__}: {err}"
                ),
                level="warning",
                interval_sec=20.0,
            )
            return
        logger.error(f"Plan generation error for {sym}: {err}", exc_info=True)

    def _record_failure(self, key: str):
        if not key:
            return
        self._fail_counts[key] = self._fail_counts.get(key, 0) + 1
        if self._fail_counts[key] >= int(self.cfg.get("CB_MAX_FAILS", 3)):
            cooldown = int(self.cfg.get("CB_COOLDOWN_MINUTES", 10))
            self._cooldown_until[key] = now_ist() + timedelta(minutes=cooldown)

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
        if now_ist() >= until:
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

    def _is_market_hours(self) -> bool:
        """Check if Indian equity market is open (09:15-15:30 IST, Mon-Fri)."""
        now = now_ist()
        if now.weekday() >= 5:  # Saturday / Sunday
            return False
        t = now.time()
        return datetime(now.year, now.month, now.day, 9, 15, tzinfo=IST).time() <= t <= datetime(now.year, now.month, now.day, 15, 30, tzinfo=IST).time()

    def _is_market_close_cutoff_reached(self) -> bool:
        now = now_ist()
        if now.weekday() >= 5:
            return True
        close_t = datetime(now.year, now.month, now.day, 15, 30, tzinfo=IST).time()
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
            self._flush_state_cache(force=True)
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

    def _idea_loop(self):
        last_bucket = None
        run_now = True  # Process all stocks immediately on startup
        last_state: Dict[str, Tuple[str, int, str, str, str]] = {}
        workers = max(1, int(self.cfg.get("IDEA_WORKERS", 3)))
        batch_size = max(1, int(self.cfg.get("IDEA_BATCH_SIZE", 50)))
        actionable_min_conf = max(0, min(4, safe_int(self.cfg.get("ACTIONABLE_MIN_CONFIDENCE", 3), 3)))
        gate_diag_enabled = bool(self.cfg.get("LOG_GATE_DIAGNOSTICS", True))
        gate_diag_only_blocked = bool(self.cfg.get("LOG_GATE_DIAGNOSTICS_ONLY_BLOCKED", True))

        def _build_for_symbol(sym: str):
            retries = max(1, int(self.cfg.get("PLAN_SYMBOL_RETRIES", 2)))
            backoff = max(0.1, float(self.cfg.get("PLAN_SYMBOL_RETRY_BACKOFF_SECONDS", 1.0)))
            last_err: Optional[Exception] = None
            for attempt in range(1, retries + 1):
                try:
                    plan = self.build_trade_plan(sym)
                    bias = safe_str(plan.get("Strategy", {}).get("Primary scenario", {}).get("Bias", ""))
                    oi_source = safe_str(plan.get("_oi_source", "")).lower()
                    return sym, plan, bias, oi_source
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

        def _handle_plan_result(sym: str, plan: Dict[str, Any], bias: str) -> Tuple[bool, bool]:
            self._record_success(sym)
            strategy = plan.get("Strategy", {})
            primary = strategy.get("Primary scenario", {})
            bb = plan.get("Breakout/Breakdown Read", {})
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
            setup = safe_str(primary.get("Setup type", ""))
            brk = safe_str(bb.get("breakout_status", ""))
            brd = safe_str(bb.get("breakdown_status", ""))
            state_sig = (safe_str(bias), conf_score, setup, brk, brd)
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

            wrote_row = True
            if (
                (not self.cfg.get("WRITE_ALL_SIGNALS", True))
                and safe_str(bias) == "Wait"
                and last_state.get(sym) == state_sig
            ):
                wrote_row = False
            else:
                self._write_plan_row(sym, plan)
            last_state[sym] = state_sig
            return True, wrote_row

        after_hours_pause_sec = max(30.0, float(self.cfg.get("AFTER_HOURS_PAUSE_SECONDS", 60.0)))
        while not self._stop_flag.is_set():
            # Outside market hours: pause longer to avoid burning CPU/API calls
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
            bucket = floor_time(now_ist(), self.cfg["CANDLE_MINUTES"])
            if last_bucket is None:
                last_bucket = bucket
            if run_now or bucket > last_bucket:
                run_now = False
                last_bucket = bucket
                self._refresh_carry_forward_positions()
                pass_start = time.monotonic()
                all_keys = list(self.underlying_tokens.keys())
                keys_to_process = [u for u in all_keys if not self._is_in_cooldown(u)]
                cooldown_skipped = max(0, len(all_keys) - len(keys_to_process))

                # Optional warmup to pre-load typical intraday volume cache for this bucket.
                if self.cfg.get("TYPICAL_VOL_PREFETCH_ENABLED", False) and keys_to_process:
                    prefetch_parallel = max(1, int(self.cfg.get("TYPICAL_VOL_PREFETCH_PARALLEL", 2)))
                    cache_day = now_ist().date().isoformat()
                    prefetch_symbols = [
                        u for u in keys_to_process
                        if f"{u}:{cache_day}" not in self.typical_intraday_cache
                    ]
                    if prefetch_symbols:
                        def _prefetch_one(sym: str):
                            try:
                                tok = self._volume_analysis_token(sym)
                                if tok is not None:
                                    self.typical_intraday_volume_same_time(sym, tok)
                            except Exception:
                                pass
                        with concurrent.futures.ThreadPoolExecutor(max_workers=prefetch_parallel, thread_name_prefix="typical_prefetch") as ex:
                            list(ex.map(_prefetch_one, prefetch_symbols))
                logger.info(
                    "Idea loop: scanning %d underlyings (eligible=%d, cooldown-skipped=%d, workers=%d, batch=%d)...",
                    len(all_keys),
                    len(keys_to_process),
                    cooldown_skipped,
                    workers,
                    batch_size,
                )
                scanned = 0
                rows_written = 0
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

                if workers <= 1:
                    for u in keys_to_process:
                        if self._stop_flag.is_set():
                            break
                        try:
                            sym, plan, bias, oi_source = _build_for_symbol(u)
                            if oi_source == "fetched":
                                oi_fetched += 1
                            elif oi_source == "cached":
                                oi_cached += 1
                            ok, wrote = _handle_plan_result(sym, plan, bias)
                            if ok:
                                scanned += 1
                                _refresh_summary(force=False)
                            if wrote:
                                rows_written += 1
                        except Exception as e:
                            self._handle_plan_failure(u, e)
                else:
                    for i in range(0, len(keys_to_process), batch_size):
                        if self._stop_flag.is_set():
                            break
                        batch = keys_to_process[i:i + batch_size]
                        with concurrent.futures.ThreadPoolExecutor(max_workers=workers, thread_name_prefix="idea_worker") as ex:
                            futures = {ex.submit(_build_for_symbol, sym): sym for sym in batch}
                            for fut in concurrent.futures.as_completed(futures):
                                if self._stop_flag.is_set():
                                    break
                                sym = futures[fut]
                                try:
                                    _sym, plan, bias, oi_source = fut.result()
                                    if oi_source == "fetched":
                                        oi_fetched += 1
                                    elif oi_source == "cached":
                                        oi_cached += 1
                                    ok, wrote = _handle_plan_result(_sym, plan, bias)
                                    if ok:
                                        scanned += 1
                                        _refresh_summary(force=False)
                                    if wrote:
                                        rows_written += 1
                                except Exception as e:
                                    self._handle_plan_failure(sym, e)

                logger.info(
                    f"Idea loop: completed {scanned}/{len(keys_to_process)} eligible underlyings "
                    f"(rows written: {rows_written})"
                )
                pass_elapsed = max(0.0, time.monotonic() - pass_start)
                processed_gap = max(0, len(keys_to_process) - scanned)
                scan_rate = (scanned / pass_elapsed) if pass_elapsed > 0 else 0.0
                write_rate = (rows_written / pass_elapsed) if pass_elapsed > 0 else 0.0
                avg_ms_per_symbol = ((1000.0 * pass_elapsed) / scanned) if scanned > 0 else 0.0
                logger.info(
                    "Idea loop performance: elapsed=%.2fs | scanned=%d/%d eligible (%d total, %d cooldown-skipped, %d not-scanned) | "
                    "rows=%d | rate=%.2f sym/s, %.2f rows/s | avg=%.1f ms/symbol | OI fetched=%d, OI cached=%d | summary-syncs=%d",
                    pass_elapsed,
                    scanned,
                    len(keys_to_process),
                    len(all_keys),
                    cooldown_skipped,
                    processed_gap,
                    rows_written,
                    scan_rate,
                    write_rate,
                    avg_ms_per_symbol,
                    oi_fetched,
                    oi_cached,
                    summary_sync_count,
                )
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
                if rows_written > 0:
                    _refresh_summary(force=True)
                else:
                    logger.info(
                        "Skipping summary refresh: no fresh live rows in this pass."
                    )
                try:
                    self._flush_state_cache()
                except Exception as e:
                    logger.error(f"State cache flush error: {e}", exc_info=True)
                if rows_written == 0 and (not self.cfg.get("WRITE_ALL_SIGNALS", True)):
                    logger.info(
                        "Idea loop wrote 0 rows because WAIT signals were unchanged "
                        "(WRITE_ALL_SIGNALS=false)."
                    )
            time.sleep(3)

    def _telegram_loop(self):
        while not self._stop_flag.is_set():
            try:
                self.tg_bridge.notify_new_requests()
                self.tg_bridge.poll_updates()
            except Exception as e:
                logger.error(f"Telegram loop error: {e}", exc_info=True)
            finally:
                time.sleep(self.tg_bridge.poll_seconds if self.tg_bridge.enabled else 2.0)

    def _write_plan_row(self, underlying_key: str, plan: Dict[str, Any]):
        ts = now_ist()
        chain = self.option_chain_tokens.get(underlying_key, {})
        atm = chain.get("atm_strike", "")
        expiry = chain.get("expiry", "")
        bias = plan["Strategy"]["Primary scenario"]["Bias"]
        setup_type = safe_str(plan["Strategy"]["Primary scenario"].get("Setup type", ""))
        chosen_ts = ""
        right = ""

        def _pick_contract(side: str) -> str:
            side_list = chain.get(side, [])
            if not side_list:
                return ""
            atm_val = safe_float(atm)
            if atm_val is None:
                return side_list[len(side_list) // 2][0]

            atm_rounded = int(round(atm_val))
            for tsym, _tok, strike in side_list:
                strike_val = safe_float(strike)
                if strike_val is not None and int(round(strike_val)) == atm_rounded:
                    return tsym

            # Fallback: nearest strike to ATM when exact strike contract is missing.
            nearest = min(
                side_list,
                key=lambda x: abs((safe_float(x[2]) if safe_float(x[2]) is not None else atm_val) - atm_val),
            )
            return nearest[0]

        preferred_side = ""
        if bias == "Buy":
            preferred_side = "ce"
        elif bias == "Sell":
            preferred_side = "pe"
        elif setup_type.startswith("Breakdown"):
            preferred_side = "pe"
        else:
            preferred_side = "ce"

        if preferred_side == "ce" and chain.get("ce"):
            right = "CE"
            chosen_ts = _pick_contract("ce")
        elif preferred_side == "pe" and chain.get("pe"):
            right = "PE"
            chosen_ts = _pick_contract("pe")

        if (not chosen_ts) and chain.get("ce"):
            right = "CE"
            chosen_ts = _pick_contract("ce")
        if (not chosen_ts) and chain.get("pe"):
            right = "PE"
            chosen_ts = _pick_contract("pe")
        if not chosen_ts:
            chosen_ts = safe_str(underlying_key)
        snap = plan["Snapshot (IST timestamp)"]
        bb = plan["Breakout/Breakdown Read"]
        opt = plan["Options-Derived Levels"]

        row_id = f"{underlying_key}-{int(time.time())}"
        carry_lots = max(0, safe_int(self.carry_forward_lots.get(underlying_key, 0), 0))
        carry_long_lots = max(0, safe_int(self.carry_forward_long_lots.get(underlying_key, 0), 0))
        carry_short_lots = max(0, safe_int(self.carry_forward_short_lots.get(underlying_key, 0), 0))
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
            carry_contract = self.carry_forward_long_contract.get(underlying_key)
        elif is_carry_buy_exit_signal:
            carry_contract = self.carry_forward_short_contract.get(underlying_key)
        elif row_status == "OPEN_CARRY":
            # For passive carry-tracking rows, show whichever held leg is available.
            carry_contract = (
                self.carry_forward_long_contract.get(underlying_key)
                or self.carry_forward_short_contract.get(underlying_key)
            )

        if isinstance(carry_contract, dict):
            cc_ts = safe_str(carry_contract.get("tradingsymbol", "")).upper().strip()
            cc_expiry = safe_str(carry_contract.get("expiry", "")).strip()
            cc_strike = safe_float(carry_contract.get("strike", None))
            cc_right = safe_str(carry_contract.get("right", "")).upper().strip()
            if cc_ts:
                chosen_ts = cc_ts
            if cc_expiry:
                expiry = cc_expiry
            if cc_strike is not None:
                atm = float(cc_strike)
            if cc_right in ("CE", "PE"):
                right = cc_right

        lot_size = self.uni.lot_size_for_tradingsymbol(chosen_ts) if chosen_ts else 0
        # For non-directional/WAIT plans, default to BUY option if a CE/PE leg was selected.
        txn = "BUY" if right in ("CE", "PE") else ""
        order_payload = {
            "tradingsymbol": chosen_ts,
            "exchange": "NFO",
            "transaction_type": txn,
            "order_type": "MARKET",
            "product": "NRML",
            "quantity": 0,
            "validity": "DAY",
        }

        row = {
            "row_id": row_id,
            "timestamp_ist": ts.strftime("%Y-%m-%d %H:%M:%S"),
            "session_context": ist_session_context(ts),
            "underlying_type": "INDEX" if underlying_key in ["NIFTY 50","NIFTY BANK"] else "STOCK",
            "symbol": underlying_key,
            "expiry": expiry,
            "strike": atm,
            "right": right,
            "tradingsymbol": chosen_ts,
            "lot_size": int(lot_size) if lot_size else 0,
            "status": row_status,
            "approve":"NO",
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
            "structure": csv_safe_text(bb.get("structure","")),
            "volume_confirmation": csv_safe_text(bb.get("volume_confirmation_status","NA")),
            "breakout_status": csv_safe_text(bb.get("breakout_status","None")),
            "breakdown_status": csv_safe_text(bb.get("breakdown_status","None")),
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

    def _execute_sell_exit_for_symbol(self, symbol: str) -> Dict[str, Any]:
        """
        Exit logic for approved high-confidence SELL signals:
        close open long CE option positions for the underlying.
        """
        try:
            pos_payload = self._kite_call("positions", self.kite.positions)
            net_positions = pos_payload.get("net", []) if isinstance(pos_payload, dict) else []
        except Exception as e:
            return {"ok": False, "error": f"positions_fetch_failed: {e}"}

        ce_longs: List[Dict[str, Any]] = []
        for p in (net_positions if isinstance(net_positions, list) else []):
            exch = safe_str((p or {}).get("exchange", "")).upper().strip()
            qty = safe_int((p or {}).get("quantity", 0), 0)
            tsym = safe_str((p or {}).get("tradingsymbol", "")).upper().strip()
            if exch != "NFO" or qty <= 0 or (not tsym):
                continue
            if not tsym.endswith("CE"):
                continue
            if not self._position_matches_underlying(symbol, p):
                continue
            ce_longs.append(p)

        if not ce_longs:
            return {"ok": False, "error": "no_open_long_ce_positions_for_symbol", "symbol": symbol}

        orders: List[Dict[str, Any]] = []
        all_ok = True
        for p in ce_longs:
            tsym = safe_str((p or {}).get("tradingsymbol", "")).upper().strip()
            qty = safe_int((p or {}).get("quantity", 0), 0)
            product = safe_str((p or {}).get("product", "")).upper().strip() or "NRML"
            if qty <= 0 or (not tsym):
                continue
            payload = {
                "exchange": "NFO",
                "tradingsymbol": tsym,
                "transaction_type": "SELL",
                "quantity": qty,
                "product": product,
                "order_type": "MARKET",
                "validity": "DAY",
                "market_protection": parse_market_protection(self.cfg.get("MARKET_PROTECTION", 2), 2),
            }
            result = self._execute_order(payload)
            ok = bool(result.get("ok"))
            all_ok = all_ok and ok
            orders.append(
                {
                    "tradingsymbol": tsym,
                    "qty": qty,
                    "ok": ok,
                    "order_id": safe_str(result.get("order_id", "")),
                    "error": safe_str(result.get("error", "")),
                }
            )
        if not orders:
            return {"ok": False, "error": "no_eligible_exit_orders", "symbol": symbol}
        return {"ok": all_ok, "symbol": symbol, "orders": orders, "closed_count": len(orders)}

    def _execute_buy_exit_for_symbol(self, symbol: str) -> Dict[str, Any]:
        """
        Exit logic for approved high-confidence BUY signals:
        close open short option positions (CE/PE) for the underlying.
        """
        try:
            pos_payload = self._kite_call("positions", self.kite.positions)
            net_positions = pos_payload.get("net", []) if isinstance(pos_payload, dict) else []
        except Exception as e:
            return {"ok": False, "error": f"positions_fetch_failed: {e}"}

        short_opts: List[Dict[str, Any]] = []
        for p in (net_positions if isinstance(net_positions, list) else []):
            exch = safe_str((p or {}).get("exchange", "")).upper().strip()
            qty = safe_int((p or {}).get("quantity", 0), 0)
            tsym = safe_str((p or {}).get("tradingsymbol", "")).upper().strip()
            if exch != "NFO" or qty >= 0 or (not tsym):
                continue
            if not (tsym.endswith("CE") or tsym.endswith("PE")):
                continue
            if not self._position_matches_underlying(symbol, p):
                continue
            short_opts.append(p)

        if not short_opts:
            return {"ok": False, "error": "no_open_short_option_positions_for_symbol", "symbol": symbol}

        orders: List[Dict[str, Any]] = []
        all_ok = True
        for p in short_opts:
            tsym = safe_str((p or {}).get("tradingsymbol", "")).upper().strip()
            qty = abs(safe_int((p or {}).get("quantity", 0), 0))
            product = safe_str((p or {}).get("product", "")).upper().strip() or "NRML"
            if qty <= 0 or (not tsym):
                continue
            payload = {
                "exchange": "NFO",
                "tradingsymbol": tsym,
                "transaction_type": "BUY",
                "quantity": qty,
                "product": product,
                "order_type": "MARKET",
                "validity": "DAY",
                "market_protection": parse_market_protection(self.cfg.get("MARKET_PROTECTION", 2), 2),
            }
            result = self._execute_order(payload)
            ok = bool(result.get("ok"))
            all_ok = all_ok and ok
            orders.append(
                {
                    "tradingsymbol": tsym,
                    "qty": qty,
                    "ok": ok,
                    "order_id": safe_str(result.get("order_id", "")),
                    "error": safe_str(result.get("error", "")),
                }
            )
        if not orders:
            return {"ok": False, "error": "no_eligible_buy_exit_orders", "symbol": symbol}
        return {"ok": all_ok, "symbol": symbol, "orders": orders, "closed_count": len(orders)}

    def _approval_loop(self):
        while not self._stop_flag.is_set():
            pending_updates: Dict[str, Dict[str, Any]] = {}
            needs_summary_sync = False
            try:
                live_df = load_live(self.cfg["CSV_LIVE_PATH"])
                sync_actions_from_summary(self.cfg["CSV_ACTIONS_PATH"], self.cfg["CSV_SUMMARY_PATH"])
                actions = load_actions_map(self.cfg["CSV_ACTIONS_PATH"])
                actionable_min_conf = max(0, min(4, safe_int(self.cfg.get("ACTIONABLE_MIN_CONFIDENCE", 3), 3)))
                if live_df.empty or "status" not in live_df.columns or "symbol" not in live_df.columns:
                    continue
                live_df["status"] = live_df["status"].astype(str).str.upper()
                live_df["symbol"] = live_df["symbol"].fillna("").astype(str).str.upper().str.strip()
                live_df["timestamp_ist_dt"] = pd.to_datetime(live_df.get("timestamp_ist", ""), errors="coerce")
                all_rows_df = (
                    live_df[live_df["symbol"] != ""]
                    .sort_values(by=["timestamp_ist_dt", "timestamp_ist"], ascending=[False, False])
                ).copy()
                if all_rows_df.empty:
                    continue
                all_rows_df["request_id_live"] = all_rows_df.apply(
                    lambda rr: build_request_id(rr.get("symbol", ""), rr.get("tradingsymbol", "")),
                    axis=1,
                )

                # Keep execution candidates restricted to fresh NEW rows only.
                candidates_df = (
                    live_df[(live_df["status"] == "NEW") & (live_df["symbol"] != "")]
                    .sort_values(by=["timestamp_ist_dt", "timestamp_ist"], ascending=[False, False])
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
                            now_text = now_ist().strftime("%Y-%m-%d %H:%M:%S")
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
                                "executed_at": now_ist().strftime("%Y-%m-%d %H:%M:%S"),
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
                        queue_update(row_id, {"status": "ERROR", "execution_result": msg})
                        _mark_action("ERROR", msg, action_status="ERROR")
                        continue
                    lot_size = safe_int(r.get("lot_size", 0), 0)
                    if lot_size <= 0:
                        msg = "Lot size missing/0"
                        queue_update(row_id, {"status": "ERROR", "execution_result": msg})
                        _mark_action("ERROR", msg, action_status="ERROR")
                        continue
                    try:
                        payload = json.loads(r.get("order_payload_json", "{}"))
                    except (json.JSONDecodeError, TypeError) as e:
                        msg = f"Invalid order_payload_json: {e}"
                        queue_update(row_id, {"status": "ERROR", "execution_result": msg})
                        _mark_action("ERROR", msg, action_status="ERROR")
                        continue
                    if not isinstance(payload, dict):
                        msg = "Invalid order_payload_json: not an object"
                        queue_update(row_id, {"status": "ERROR", "execution_result": msg})
                        _mark_action("ERROR", msg, action_status="ERROR")
                        continue
                    payload["quantity"] = lots * lot_size
                    payload["order_type"] = "MARKET"
                    # Force NRML to avoid legacy MIS rows from older live snapshots.
                    payload["product"] = "NRML"
                    payload["market_protection"] = parse_market_protection(self.cfg.get("MARKET_PROTECTION", 2), 2)
                    # Always send MARKET orders with market protection enabled.
                    if safe_str(payload.get("order_type", "")).upper() == "MARKET" and safe_int(payload.get("market_protection", -1), -1) < 0:
                        payload["market_protection"] = 2
                    txn = safe_str(payload.get("transaction_type", "")).upper().strip()
                    if txn not in ("BUY", "SELL"):
                        # Backward compatibility: infer BUY for option legs when old rows have blank transaction_type.
                        row_right = safe_str(r.get("right", "")).upper().strip()
                        if row_right in ("CE", "PE"):
                            txn = "BUY"
                        else:
                            tsym = safe_str(payload.get("tradingsymbol", "")).upper().strip()
                            if tsym.endswith("CE") or tsym.endswith("PE"):
                                txn = "BUY"
                        if txn:
                            payload["transaction_type"] = txn
                        else:
                            msg = "Missing/invalid transaction_type"
                            queue_update(row_id, {"status": "ERROR", "execution_result": msg})
                            _mark_action("ERROR", msg, action_status="ERROR")
                            continue
                    row_right = safe_str(r.get("right", "")).upper().strip()
                    if row_right in ("CE", "PE") and txn == "SELL":
                        logger.warning(
                            f"Converting legacy SELL option transaction to BUY for {symbol} "
                            f"{safe_str(r.get('tradingsymbol', ''))}"
                        )
                        txn = "BUY"
                        payload["transaction_type"] = txn
                    result = self._execute_order(payload)
                    exec_ok = bool(result.get("ok"))
                    exec_status = "EXECUTED" if exec_ok else "ERROR"
                    exec_result = json.dumps(result, default=str)
                    queue_update(row_id, {
                        "status": "CLOSED" if exec_ok else "ERROR",
                        "execution_result": exec_result,
                        "order_payload_json": json.dumps(payload, default=str),
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

    def _execute_order(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        try:
            market_protection = safe_int(payload.get("market_protection", -1), -1)
            if market_protection < 0:
                market_protection = 2
            payload["market_protection"] = market_protection
            order_kwargs = {
                "variety": self.kite.VARIETY_REGULAR,
                "exchange": payload["exchange"],
                "tradingsymbol": payload["tradingsymbol"],
                "transaction_type": payload["transaction_type"],
                "quantity": int(payload["quantity"]),
                "product": payload["product"],
                "order_type": payload["order_type"],
                "validity": payload["validity"],
                "price": payload.get("price"),
                "trigger_price": payload.get("trigger_price"),
                "tag": "opt_brain_csv",
            }

            is_market = safe_str(order_kwargs.get("order_type", "")).upper() == "MARKET"

            def _raw_place_with_market_protection(mp: int):
                raw_params = dict(order_kwargs)
                raw_params["market_protection"] = mp
                for k in list(raw_params.keys()):
                    if raw_params[k] is None:
                        del raw_params[k]
                return self.kite._post(
                    "order.place",
                    url_args={"variety": self.kite.VARIETY_REGULAR},
                    params=raw_params,
                )["order_id"]

            def _best_depth_price(side: str, q: Dict[str, Any]) -> Optional[float]:
                depth = q.get("depth", {}) if isinstance(q, dict) else {}
                rows = depth.get(side, []) if isinstance(depth, dict) else []
                for rec in rows:
                    px = safe_float((rec or {}).get("price"))
                    qty = safe_int((rec or {}).get("quantity"), 0)
                    if px is not None and px > 0 and qty > 0:
                        return float(px)
                return None

            def _round_price_to_tick(px: float, side_txn: str, tick: float = 0.05) -> float:
                rounded = clamp_round_to_step(px, tick)
                if side_txn == "BUY" and rounded < px:
                    rounded = round(rounded + tick, 2)
                elif side_txn == "SELL" and rounded > px:
                    rounded = round(max(tick, rounded - tick), 2)
                return max(tick, round(rounded, 2))

            def _place_market_limit_fallback(mp: int) -> Tuple[str, float]:
                """
                RMS may block MARKET orders for stock options even with market protection.
                Fallback to a protected LIMIT order around top-of-book/LTP.
                """
                quote_key = f"{safe_str(order_kwargs.get('exchange', '')).upper()}:{safe_str(order_kwargs.get('tradingsymbol', '')).upper()}"
                qmap = self._kite_call("quote", self.kite.quote, [quote_key])
                q = qmap.get(quote_key, {}) if isinstance(qmap, dict) else {}
                ltp = safe_float(q.get("last_price"), 0.0) or 0.0
                best_bid = _best_depth_price("buy", q)
                best_ask = _best_depth_price("sell", q)
                txn_side = safe_str(order_kwargs.get("transaction_type", "")).upper()
                mp_use = max(0, safe_int(mp, 2))

                raw_limit: Optional[float] = None
                if txn_side == "BUY":
                    cap = (ltp * (1.0 + (mp_use / 100.0))) if ltp > 0 else None
                    raw_limit = best_ask if (best_ask is not None and best_ask > 0) else cap
                    if (raw_limit is None or raw_limit <= 0) and ltp > 0:
                        raw_limit = ltp
                    if cap is not None and raw_limit is not None:
                        raw_limit = min(raw_limit, cap)
                else:
                    floor = (ltp * (1.0 - (mp_use / 100.0))) if ltp > 0 else None
                    raw_limit = best_bid if (best_bid is not None and best_bid > 0) else floor
                    if (raw_limit is None or raw_limit <= 0) and ltp > 0:
                        raw_limit = ltp
                    if floor is not None and raw_limit is not None:
                        raw_limit = max(raw_limit, floor)

                if raw_limit is None or raw_limit <= 0:
                    raise RuntimeError(
                        f"Unable to derive LIMIT fallback price (ltp={ltp}, bid={best_bid}, ask={best_ask})"
                    )

                limit_price = _round_price_to_tick(raw_limit, txn_side if txn_side in ("BUY", "SELL") else "BUY")
                limit_kwargs = dict(order_kwargs)
                limit_kwargs["order_type"] = "LIMIT"
                limit_kwargs["price"] = limit_price
                limit_kwargs["trigger_price"] = None
                for k in list(limit_kwargs.keys()):
                    if limit_kwargs[k] is None:
                        del limit_kwargs[k]
                logger.warning(
                    "MARKET order still blocked after market_protection=%s; placing LIMIT fallback for %s at price=%s (ltp=%s, bid=%s, ask=%s)",
                    mp_use,
                    safe_str(order_kwargs.get("tradingsymbol", "")),
                    limit_price,
                    ltp,
                    best_bid,
                    best_ask,
                )
                order_id = self.kite.place_order(**limit_kwargs)
                return order_id, limit_price

            if market_protection >= 0:
                if is_market:
                    logger.info(
                        "Placing MARKET order with market_protection=%s for %s",
                        market_protection,
                        safe_str(order_kwargs.get("tradingsymbol", "")),
                    )
                    try:
                        order_id = _raw_place_with_market_protection(market_protection)
                    except Exception as e:
                        err = safe_str(e).lower()
                        retry_mp = max(5, market_protection)
                        if (
                            "market orders are blocked for stock options due to illiquidity" in err
                            and retry_mp > market_protection
                        ):
                            logger.warning(
                                "MARKET order blocked with market_protection=%s; retrying once with market_protection=%s",
                                market_protection,
                                retry_mp,
                            )
                            try:
                                order_id = _raw_place_with_market_protection(retry_mp)
                                market_protection = retry_mp
                                payload["market_protection"] = market_protection
                            except Exception as e2:
                                err2 = safe_str(e2).lower()
                                if "market orders are blocked for stock options due to illiquidity" in err2:
                                    order_id, limit_price = _place_market_limit_fallback(retry_mp)
                                    payload["order_type"] = "LIMIT"
                                    payload["price"] = limit_price
                                    payload["market_protection"] = retry_mp
                                    return {
                                        "ok": True,
                                        "order_id": order_id,
                                        "market_protection": retry_mp,
                                        "fallback_order_type": "LIMIT",
                                        "limit_price": limit_price,
                                    }
                                raise
                        else:
                            raise
                else:
                    try:
                        order_id = self.kite.place_order(
                            **order_kwargs,
                            market_protection=market_protection,
                        )
                    except TypeError as te:
                        err = safe_str(te)
                        if ("unexpected keyword argument" in err) and ("market_protection" in err):
                            logger.warning("Kite SDK does not support market_protection argument; using raw order.place with market_protection.")
                            order_id = _raw_place_with_market_protection(market_protection)
                        else:
                            raise
            else:
                order_id = self.kite.place_order(**order_kwargs)
            return {"ok": True, "order_id": order_id, "market_protection": market_protection}
        except Exception as e:
            return {"ok": False, "error": str(e), "payload": payload}

def main():
    """Main entry point with improved error handling and WebSocket auto-reconnect"""
    engine = None
    _registered_signals: List[int] = []
    def _handle_shutdown_signal(signum, _frame):
        signame = str(signum)
        try:
            signame = signal.Signals(signum).name
        except Exception:
            pass
        logger.info(f"Received {signame}. Initiating graceful shutdown...")
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

        logger.info("="*60)
        logger.info("Starting Options Trading Bot v3.2 (Improved)")
        logger.info("="*60)

        engine = OptionsBrainEngineCSV(CONFIG)

        max_ws_retries = 50
        consecutive_instant_failures = 0
        for ws_attempt in range(1, max_ws_retries + 1):
            if engine._stop_flag.is_set():
                break

            # Close old WebSocket cleanly before creating a new one
            try:
                engine.kws.close()
            except Exception:
                pass

            try:
                logger.info(f"Starting WebSocket connection (attempt {ws_attempt}/{max_ws_retries})...")
                engine.kws = KiteTicker(CONFIG["KITE_API_KEY"], engine.access_token)
                engine.kws.on_connect = engine._on_connect
                engine.kws.on_ticks = engine._on_ticks
                engine.kws.on_close = engine._on_close
                engine.kws.on_error = engine._on_error
                t_start = time.time()
                engine.kws.connect(threaded=False)
                connected_duration = time.time() - t_start
            except KeyboardInterrupt:
                raise
            except Exception as e:
                connected_duration = time.time() - t_start
                logger.warning(f"WebSocket disconnected (attempt {ws_attempt}/{max_ws_retries}): {e}")

            if engine._stop_flag.is_set():
                break

            # Detect instant failures vs genuine disconnects
            if connected_duration < 5:
                consecutive_instant_failures += 1
                wait = min(60 * consecutive_instant_failures, 300)  # 60s, 120s, ... up to 5 min
                logger.warning(f"WebSocket failed instantly ({consecutive_instant_failures} in a row). Waiting {wait}s before retry...")
            else:
                consecutive_instant_failures = 0
                wait = 5  # Quick retry after a genuine disconnect
                logger.info(f"WebSocket was connected for {connected_duration:.0f}s. Reconnecting in {wait}s...")

            slept = 0.0
            while slept < float(wait):
                if engine._stop_flag.is_set():
                    break
                nap = min(0.5, float(wait) - slept)
                time.sleep(nap)
                slept += nap

        if not engine._stop_flag.is_set():
            logger.critical(f"WebSocket reconnection failed after {max_ws_retries} attempts. Exiting.")

    except KeyboardInterrupt:
        logger.info("\nKeyboard interrupt received. Shutting down gracefully...")
    except Exception as e:
        logger.critical(f"Fatal error in main: {e}", exc_info=True)
        raise
    finally:
        # Restore default handlers to avoid leaking custom handlers in embedded runs.
        for sig in _registered_signals:
            try:
                signal.signal(sig, signal.SIG_DFL)
            except Exception:
                pass
        if engine is not None:
            engine.stop()
        logger.info("Shutdown complete.")

if __name__ == "__main__":
    main()
