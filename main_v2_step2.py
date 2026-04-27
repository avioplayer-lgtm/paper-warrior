import os
import io
import csv
import time
import math
import uuid
import json
import sqlite3
import logging
import threading
from pathlib import Path
from datetime import datetime, date

import requests
import pandas as pd
import pytz
from dotenv import load_dotenv

load_dotenv()

# ─────────────────────────────────────────
# ENV / CONFIG
# ─────────────────────────────────────────
BOT_TOKEN = os.environ.get("BOT_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")
DHAN_CLIENT_ID = (os.environ.get("DHAN_CLIENT_ID") or "").strip()
DHAN_ACCESS_TOKEN = (os.environ.get("DHAN_ACCESS_TOKEN") or "").strip()

if not BOT_TOKEN or not CHAT_ID:
    raise ValueError("BOT_TOKEN and CHAT_ID must be set.")

DATA_DIR = Path(os.environ.get("DATA_DIR", ".")).resolve()
DATA_DIR.mkdir(parents=True, exist_ok=True)
EXPORT_DIR = DATA_DIR / "exports"
EXPORT_DIR.mkdir(parents=True, exist_ok=True)

CAPITAL = float(os.environ.get("CAPITAL", 30000))
RISK_PCT = float(os.environ.get("RISK_PCT", 0.005))
MAX_DAILY_LOSS = float(os.environ.get("MAX_DAILY_LOSS", CAPITAL * 0.02))
MIN_CONFIDENCE = int(os.environ.get("MIN_CONFIDENCE", 6))
SCAN_INTERVAL_SEC = int(os.environ.get("SCAN_INTERVAL_SEC", 300))
MONITOR_INTERVAL_SEC = int(os.environ.get("MONITOR_INTERVAL_SEC", 30))
ENTRY_CUTOFF = os.environ.get("ENTRY_CUTOFF", "15:00")
EOD_SQUAREOFF = os.environ.get("EOD_SQUAREOFF", "15:15")
TOKEN_REMINDER_TIME = os.environ.get("TOKEN_REMINDER_TIME", "21:30")
PAPER_CHARGES_PCT = float(os.environ.get("PAPER_CHARGES_PCT", 0.0015))
USE_LIVE_PREMIUM = os.environ.get("USE_LIVE_PREMIUM", "1") == "1"
IST = pytz.timezone("Asia/Kolkata")

SYMBOLS = {
    "NIFTY": {"interval": 50, "lot": 65, "expiry_day": 3, "dhan_scrip": "13", "ws_scrip": 13},
    "BANKNIFTY": {"interval": 100, "lot": 30, "expiry_day": 3, "dhan_scrip": "25", "ws_scrip": 25},
}

STRATEGY_RANK = {"TRENDING": 4, "VOLATILE": 3, "SIDEWAYS": 2, "NORMAL": 1}

DHAN_HEADERS = {
    "access-token": DHAN_ACCESS_TOKEN,
    "client-id": DHAN_CLIENT_ID,
    "Content-Type": "application/json",
}

# ─────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────
LOG_PATH = DATA_DIR / "bot.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(LOG_PATH), logging.StreamHandler()],
)
log = logging.getLogger("DhanPaperAlgoV2")

# ─────────────────────────────────────────
# SQLITE
# ─────────────────────────────────────────
DB_PATH = DATA_DIR / "state.db"
conn = sqlite3.connect(DB_PATH, check_same_thread=False)
conn.row_factory = sqlite3.Row
conn.execute("PRAGMA journal_mode=WAL;")
conn.execute("PRAGMA synchronous=NORMAL;")
conn.execute("CREATE TABLE IF NOT EXISTS kv (k TEXT PRIMARY KEY, v TEXT)")
conn.execute(
    """
    CREATE TABLE IF NOT EXISTS trades (
        trade_id TEXT PRIMARY KEY,
        trade_date TEXT,
        symbol TEXT,
        direction TEXT,
        option_type TEXT,
        expiry TEXT,
        atm_strike INTEGER,
        strategy TEXT,
        regime TEXT,
        confidence REAL,
        risk_score REAL,
        score REAL,
        signal_close REAL,
        entry_time TEXT,
        entry_premium REAL,
        qty INTEGER,
        lots INTEGER,
        lot_size INTEGER,
        capital_used REAL,
        initial_sl REAL,
        trail_sl REAL,
        target_anchor REAL,
        highest_premium_seen REAL,
        lowest_premium_seen REAL,
        last_premium REAL,
        exit_time TEXT,
        exit_premium REAL,
        exit_reason TEXT,
        gross_pnl REAL,
        charges REAL,
        net_pnl REAL,
        status TEXT,
        meta_json TEXT
    )
    """
)
conn.execute(
    """
    CREATE TABLE IF NOT EXISTS daily_summary (
        day TEXT PRIMARY KEY,
        total_trades INTEGER,
        wins INTEGER,
        losses INTEGER,
        gross_pnl REAL,
        charges REAL,
        net_pnl REAL,
        peak_capital_used REAL,
        summary_text TEXT,
        csv_path TEXT,
        sent_at TEXT
    )
    """
)
conn.commit()

# ─────────────────────────────────────────
# STATE
# ─────────────────────────────────────────
_lock = threading.Lock()
regime_state = {name: {"last": None, "count": 0} for name in SYMBOLS}
_expiry_cache = {}

state = {
    "active_trades": {},
    "daily_loss": 0.0,
    "realized_pnl": 0.0,
    "current_day": None,
    "rules_sent": {"open": False, "mid": False, "close": False},
    "last_heartbeat_hour": -1,
    "holiday_sent": False,
    "paused": False,
    "token_reminder_sent": False,
    "eod_done": False,
    "peak_capital_used": 0.0,
}


def now_ist():
    return datetime.now(IST)


def time_str():
    return now_ist().strftime("%H:%M")


def _jsonable(v):
    if isinstance(v, (str, int, float, bool)) or v is None:
        return v
    if isinstance(v, (list, tuple)):
        return [_jsonable(x) for x in v]
    if isinstance(v, dict):
        return {k: _jsonable(x) for k, x in v.items()}
    if isinstance(v, (date, datetime)):
        return v.isoformat()
    return str(v)


def _persist_state():
    payload = _jsonable(state)
    with conn:
        conn.execute(
            "INSERT OR REPLACE INTO kv (k, v) VALUES (?, ?)",
            ("state", json.dumps(payload)),
        )


def _load_state():
    row = conn.execute("SELECT v FROM kv WHERE k='state'").fetchone()
    if not row:
        log.info("No saved state found - starting fresh")
        return
    saved = json.loads(row[0])
    with _lock:
        for k, v in saved.items():
            if k in state:
                state[k] = v
    log.info("State restored from SQLite")


def get_st(key):
    with _lock:
        return state[key]


def set_st(key, val):
    with _lock:
        state[key] = val
        _persist_state()


def active_trades():
    with _lock:
        return dict(state["active_trades"])


def active_capital_used():
    return round(sum(float(t.get("capital_used", 0)) for t in active_trades().values()), 2)

# ─────────────────────────────────────────
# TELEGRAM
# ─────────────────────────────────────────
def _tg(endpoint, payload=None, files=None):
    try:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/{endpoint}"
        if files:
            r = requests.post(url, data=payload or {}, files=files, timeout=30)
        else:
            r = requests.post(url, json=payload or {}, timeout=30)
        return r.json()
    except Exception as e:
        log.error(f"Telegram error ({endpoint}): {e}")
        return {}


def send_text(text):
    res = _tg("sendMessage", {"chat_id": CHAT_ID, "text": text, "parse_mode": "Markdown"})
    return res.get("result", {}).get("message_id")


def send_document(file_path, caption=None):
    try:
        with open(file_path, "rb") as f:
            files = {"document": f}
            payload = {"chat_id": CHAT_ID}
            if caption:
                payload["caption"] = caption
            return _tg("sendDocument", payload=payload, files=files)
    except Exception as e:
        log.error(f"send_document error: {e}")
        return {}

# ─────────────────────────────────────────
# DHAN DATA
# ─────────────────────────────────────────
def get_dhan_candles(name):
    scrip_id = SYMBOLS[name]["dhan_scrip"]
    today = now_ist().date().isoformat()
    try:
        resp = requests.post(
            "https://api.dhan.co/v2/charts/intraday",
            headers=DHAN_HEADERS,
            json={
                "securityId": scrip_id,
                "exchangeSegment": "IDX_I",
                "instrument": "INDEX",
                "interval": "5",
                "fromDate": today,
                "toDate": today,
            },
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        if not data.get("open"):
            return None
        df = pd.DataFrame(
            {
                "Open": data["open"],
                "High": data["high"],
                "Low": data["low"],
                "Close": data["close"],
                "Volume": data["volume"],
            },
            index=pd.to_datetime(data["timestamp"], unit="s", utc=True).tz_convert(IST),
        )
        df = df.dropna()
        return df if len(df) >= 20 else None
    except Exception as e:
        log.error(f"{name} get_dhan_candles error: {e}")
        return None


def get_dhan_ltp(name):
    scrip_id = SYMBOLS[name]["ws_scrip"]
    try:
        resp = requests.post(
            "https://api.dhan.co/v2/marketfeed/ltp",
            headers=DHAN_HEADERS,
            json={"NSE_INDEX": [scrip_id]},
            timeout=5,
        )
        resp.raise_for_status()
        data = resp.json()
        ltp = data.get("data", {}).get("NSE_INDEX", {}).get(str(scrip_id), {}).get("last_price")
        return float(ltp) if ltp else None
    except Exception as e:
        log.error(f"{name} get_dhan_ltp error: {e}")
        return None


def get_next_expiry(scrip_id):
    today = now_ist().date().isoformat()
    if scrip_id in _expiry_cache:
        cached_date, cached_expiry = _expiry_cache[scrip_id]
        if cached_date == today:
            return cached_expiry
    try:
        resp = requests.post(
            "https://api.dhan.co/v2/optionchain/expirylist",
            headers=DHAN_HEADERS,
            json={"UnderlyingScrip": int(scrip_id), "UnderlyingSeg": "IDX_I"},
            timeout=10,
        )
        resp.raise_for_status()
        expiries = resp.json().get("data", [])
        today_date = now_ist().date()
        for exp in sorted(expiries):
            if exp >= today_date.isoformat():
                _expiry_cache[scrip_id] = (today, exp)
                return exp
    except Exception as e:
        log.error(f"get_next_expiry error: {e}")
    return None


def days_to_expiry(name):
    exp_day = SYMBOLS.get(name, {}).get("expiry_day")
    today = now_ist().date()
    diff = (exp_day - today.weekday()) % 7
    return diff if diff > 0 else 7


def is_expiry_today(name):
    return now_ist().weekday() == SYMBOLS.get(name, {}).get("expiry_day")


def estimate_premium(spot, strike, opt_type, dte):
    iv = 0.14
    intrinsic = max(0, spot - strike) if opt_type == "CE" else max(0, strike - spot)
    time_val = round(spot * iv * max(dte, 1) / 365)
    return max(10, round(intrinsic + time_val))


def get_live_premium(name, spot, strike, opt_type):
    cfg = SYMBOLS[name]
    scrip_id = cfg["dhan_scrip"]
    if not USE_LIVE_PREMIUM or not DHAN_CLIENT_ID or not DHAN_ACCESS_TOKEN:
        return estimate_premium(spot, strike, opt_type, days_to_expiry(name)), False, None, None
    expiry = get_next_expiry(scrip_id)
    if not expiry:
        return estimate_premium(spot, strike, opt_type, days_to_expiry(name)), False, None, None
    try:
        resp = requests.post(
            "https://api.dhan.co/v2/optionchain",
            headers=DHAN_HEADERS,
            json={
                "UnderlyingScrip": int(scrip_id),
                "UnderlyingSeg": "IDX_I",
                "Expiry": expiry,
            },
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        oc = data.get("data", {}).get("oc", {})
        key = opt_type.lower()
        best_strike_key = None
        best_diff = float("inf")
        for sk in oc:
            try:
                diff = abs(float(sk) - float(strike))
                if diff < best_diff:
                    best_diff = diff
                    best_strike_key = sk
            except Exception:
                pass
        if best_strike_key:
            option_data = oc[best_strike_key].get(key, {})
            ltp = option_data.get("last_price")
            delta = option_data.get("delta")
            iv = option_data.get("implied_volatility")
            if ltp and float(ltp) > 0:
                return round(float(ltp)), True, delta, iv
    except Exception as e:
        log.error(f"get_live_premium error: {e}")
    return estimate_premium(spot, strike, opt_type, days_to_expiry(name)), False, None, None


def get_trade_live_premium(trade):
    spot = get_dhan_ltp(trade["symbol"])
    if spot is None:
        return None
    prem, is_live, delta, iv = get_live_premium(
        trade["symbol"], spot, trade["atm_strike"], trade["direction"]
    )
    return prem

# ─────────────────────────────────────────
# SIGNAL ENGINE
# ─────────────────────────────────────────
def compute_indicators(df):
    df = df.copy()
    df["ema9"] = df["Close"].ewm(span=9, adjust=False).mean()
    df["ema21"] = df["Close"].ewm(span=21, adjust=False).mean()
    delta = df["Close"].diff()
    gain = delta.clip(lower=0).rolling(14).mean()
    loss = (-delta.clip(upper=0)).rolling(14).mean()
    df["rsi"] = 100 - (100 / (1 + gain / loss.replace(0, 1e-9)))
    df["atr"] = (df["High"] - df["Low"]).rolling(10).mean()
    cum_vol = df["Volume"].cumsum()
    cum_pv = (df["Close"] * df["Volume"]).cumsum()
    df["vwap"] = cum_pv / cum_vol.replace(0, 1e-9)
    return df


def detect_regime(df, atr, ema9, ema21):
    recent = df.iloc[-10:]
    rng = recent["High"].max() - recent["Low"].min()
    ema_diff = abs(ema9 - ema21)
    if ema_diff > atr * 0.6 and rng > atr * 4:
        return "TRENDING"
    if ema_diff < atr * 0.3 and rng < atr * 3:
        return "SIDEWAYS"
    if rng > atr * 5:
        return "VOLATILE"
    return "NORMAL"


def confirm_regime(symbol, new_regime):
    rs = regime_state[symbol]
    if rs["last"] == new_regime:
        rs["count"] += 1
    else:
        rs["last"] = new_regime
        rs["count"] = 1
    return new_regime if rs["count"] >= 2 else None


def strategy_breakout(df, atr, ema9, ema21, rsi, vwap):
    orb_high = float(df.iloc[:3]["High"].max())
    orb_low = float(df.iloc[:3]["Low"].min())
    close = float(df.iloc[-1]["Close"])
    conf = 0
    if close > orb_high or close < orb_low:
        conf += 3
    if (ema9 > ema21 and close > orb_high) or (ema9 < ema21 and close < orb_low):
        conf += 3
    if (rsi > 55 and close > orb_high) or (rsi < 45 and close < orb_low):
        conf += 2
    if conf < MIN_CONFIDENCE:
        return None
    if close > orb_high and close > vwap and ema9 > ema21:
        return "CE", close, close - atr, close + atr * 2, conf
    if close < orb_low and close < vwap and ema9 < ema21:
        return "PE", close, close + atr, close - atr * 2, conf
    return None


def strategy_range_trade(df, atr, ema9, ema21, rsi):
    recent = df.iloc[-10:]
    high = float(recent["High"].max())
    low = float(recent["Low"].min())
    close = float(df.iloc[-1]["Close"])
    buffer = atr * 0.3
    conf = 0
    if abs(ema9 - ema21) < atr * 0.3:
        conf += 3
    if close <= low + buffer or close >= high - buffer:
        conf += 3
    if (close <= low + buffer and rsi < 40) or (close >= high - buffer and rsi > 60):
        conf += 2
    if conf < MIN_CONFIDENCE:
        return None
    if close <= low + buffer:
        return "CE", close, close - atr * 0.8, close + atr * 1.2, conf
    if close >= high - buffer:
        return "PE", close, close + atr * 0.8, close - atr * 1.2, conf
    return None


def strategy_momentum(df, atr, rsi):
    last = df.iloc[-1]
    body = abs(float(last["Close"]) - float(last["Open"]))
    rng = float(last["High"]) - float(last["Low"])
    close = float(last["Close"])
    conf = 0
    if rng > 0 and body > rng * 0.7:
        conf += 3
    if atr > float(df["atr"].iloc[-6:-1].mean()) * 1.4:
        conf += 3
    if (last["Close"] > last["Open"] and rsi > 60) or (last["Close"] < last["Open"] and rsi < 40):
        conf += 2
    if conf < MIN_CONFIDENCE:
        return None
    if last["Close"] > last["Open"]:
        return "CE", close, close - atr * 1.3, close + atr * 2.5, conf
    return "PE", close, close + atr * 1.3, close - atr * 2.5, conf


def greeks_warnings(delta, iv):
    warnings = []
    if iv is not None:
        try:
            iv_f = float(iv)
            if iv_f > 25:
                warnings.append(f"High IV ({iv_f:.1f}%) - premium expensive")
            elif iv_f < 8:
                warnings.append(f"Low IV ({iv_f:.1f}%) - low volatility")
        except Exception:
            pass
    if delta is not None:
        try:
            delta_f = abs(float(delta))
            if delta_f < 0.35:
                warnings.append(f"Low Delta ({delta_f:.2f}) - weak directional signal")
        except Exception:
            pass
    return warnings


def compute_trade_score(confidence, strategy_rank, risk_per_lot, cost_per_lot):
    risk_eff = confidence / max(risk_per_lot, 1)
    capital_eff = confidence / max(cost_per_lot, 1)
    return round((confidence * 10) + (strategy_rank * 5) + (risk_eff * 5000) + (capital_eff * 2000), 4)


def scan_symbol(name):
    cfg = SYMBOLS[name]
    interval = cfg["interval"]
    lot = cfg["lot"]
    try:
        df = get_dhan_candles(name)
        if df is None:
            return None
        df = compute_indicators(df)
        last = df.iloc[-1]
        close = float(last["Close"])
        atr = float(last["atr"])
        ema9 = float(last["ema9"])
        ema21 = float(last["ema21"])
        rsi = float(last["rsi"])
        vwap = float(last["vwap"])
        raw_regime = detect_regime(df, atr, ema9, ema21)
        regime = confirm_regime(name, raw_regime)
        if not regime or regime == "NORMAL":
            return None
        if regime == "TRENDING":
            result = strategy_breakout(df, atr, ema9, ema21, rsi, vwap)
            strategy = "ORB Breakout"
        elif regime == "SIDEWAYS":
            result = strategy_range_trade(df, atr, ema9, ema21, rsi)
            strategy = "Range Fade"
        else:
            result = strategy_momentum(df, atr, rsi)
            strategy = "Momentum"
        if result is None:
            return None
        direction, entry, sl_idx, tgt_idx, conf = result
        atm_strike = round(close / interval) * interval
        otm_strike = (atm_strike + interval) if direction == "CE" else (atm_strike - interval)
        dte = days_to_expiry(name)
        atm_prem, atm_is_live, delta, iv = get_live_premium(name, close, atm_strike, direction)
        otm_prem, _, _, _ = get_live_premium(name, close, otm_strike, direction)
        if is_expiry_today(name):
            sl_prem = round(atm_prem * 0.35)
            tgt_prem = round(atm_prem * 1.60)
        else:
            sl_prem = round(atm_prem * 0.45)
            tgt_prem = round(atm_prem * 1.90)
        risk_per_lot = max(1, (atm_prem - sl_prem) * lot)
        cost_per_lot = atm_prem * lot
        strategy_rank = STRATEGY_RANK.get(regime, 1)
        score = compute_trade_score(conf, strategy_rank, risk_per_lot, cost_per_lot)
        risk_score = round(conf / max((atm_prem - sl_prem), 1), 4)
        warnings = greeks_warnings(delta, iv)
        expiry = get_next_expiry(cfg["dhan_scrip"])
        return {
            "id": str(uuid.uuid4())[:8],
            "symbol": name,
            "direction": direction,
            "option_type": direction,
            "confidence": conf,
            "risk_score": risk_score,
            "score": score,
            "regime": regime,
            "strategy": strategy,
            "close": round(close, 2),
            "atm_strike": atm_strike,
            "otm_strike": otm_strike,
            "expiry": expiry,
            "atm_prem": atm_prem,
            "otm_prem": otm_prem,
            "entry_premium": atm_prem,
            "entry_premium_actual": atm_prem,
            "prem_is_live": atm_is_live,
            "delta": delta,
            "iv": iv,
            "greeks_warnings": warnings,
            "sl_prem": sl_prem,
            "sl_premium_current": sl_prem,
            "tgt_prem": tgt_prem,
            "cost_per_lot": cost_per_lot,
            "lot": lot,
            "sl_idx": round(sl_idx, 2),
            "tgt_idx": round(tgt_idx, 2),
            "atr": round(atr, 2),
            "rsi": round(rsi, 1),
            "ema9": round(ema9, 2),
            "ema21": round(ema21, 2),
            "vwap": round(vwap, 2),
            "dte": dte,
            "expiry_today": is_expiry_today(name),
            "risk_per_lot": risk_per_lot,
            "strategy_rank": strategy_rank,
        }
    except Exception as e:
        log.error(f"{name}: scan_symbol error - {e}")
        return None

# ─────────────────────────────────────────
# PAPER EXECUTION / TRADE JOURNAL
# ─────────────────────────────────────────
def save_trade_to_db(trade):
    with conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO trades (
                trade_id, trade_date, symbol, direction, option_type, expiry, atm_strike,
                strategy, regime, confidence, risk_score, score, signal_close,
                entry_time, entry_premium, qty, lots, lot_size, capital_used,
                initial_sl, trail_sl, target_anchor, highest_premium_seen,
                lowest_premium_seen, last_premium, exit_time, exit_premium,
                exit_reason, gross_pnl, charges, net_pnl, status, meta_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                trade["trade_id"], trade["trade_date"], trade["symbol"], trade["direction"], trade["option_type"],
                trade.get("expiry"), trade["atm_strike"], trade["strategy"], trade["regime"], trade["confidence"],
                trade["risk_score"], trade["score"], trade["signal_close"], trade["entry_time"], trade["entry_premium"],
                trade["qty"], trade["lots"], trade["lot_size"], trade["capital_used"], trade["initial_sl"],
                trade["trail_sl"], trade["target_anchor"], trade["highest_premium_seen"], trade["lowest_premium_seen"],
                trade["last_premium"], trade.get("exit_time"), trade.get("exit_premium"), trade.get("exit_reason"),
                trade.get("gross_pnl"), trade.get("charges"), trade.get("net_pnl"), trade["status"],
                json.dumps(_jsonable(trade.get("meta", {})))
            ),
        )


def update_trade_db_exit(trade):
    with conn:
        conn.execute(
            """
            UPDATE trades SET
                trail_sl=?, target_anchor=?, highest_premium_seen=?, lowest_premium_seen=?,
                last_premium=?, exit_time=?, exit_premium=?, exit_reason=?,
                gross_pnl=?, charges=?, net_pnl=?, status=?, meta_json=?
            WHERE trade_id=?
            """,
            (
                trade["trail_sl"], trade["target_anchor"], trade["highest_premium_seen"], trade["lowest_premium_seen"],
                trade["last_premium"], trade.get("exit_time"), trade.get("exit_premium"), trade.get("exit_reason"),
                trade.get("gross_pnl"), trade.get("charges"), trade.get("net_pnl"), trade["status"],
                json.dumps(_jsonable(trade.get("meta", {}))), trade["trade_id"]
            ),
        )


def build_signal_msg(s, lots, capital_used):
    live_tag = "(live)" if s.get("prem_is_live") else "(est.)"
    warn_str = ""
    if s.get("greeks_warnings"):
        warn_str = "\nWarnings:\n" + "\n".join(f"- {w}" for w in s["greeks_warnings"]) + "\n"
    return (
        f"*PAPER TRADE AUTO-TAKEN*\n\n"
        f"*{s['symbol']}* {s['atm_strike']} {s['direction']}\n"
        f"Regime: {s['regime']} | Strategy: {s['strategy']}\n"
        f"Confidence: {s['confidence']}/8 | Score: {s['score']}\n\n"
        f"Entry {live_tag}: Rs.{s['atm_prem']}\n"
        f"SL: Rs.{s['sl_prem']}\n"
        f"Initial target anchor: Rs.{s['tgt_prem']}\n"
        f"Lots: {lots} | Qty: {lots * s['lot']}\n"
        f"Capital used: Rs.{capital_used:,.0f}\n"
        f"Risk/lot: Rs.{s['risk_per_lot']:,.0f}\n"
        f"{warn_str}"
        f"This is a *paper trade* only."
    )


def allocate_and_take_trades(signals):
    if not signals:
        return
    ranked = sorted(signals, key=lambda x: (x["score"], x["confidence"], -x["cost_per_lot"]), reverse=True)
    with _lock:
        active = state["active_trades"]
        used_capital = sum(float(t.get("capital_used", 0)) for t in active.values())
        free_capital = CAPITAL - used_capital
        if free_capital <= 0:
            log.info("No free capital for new paper trades")
            return
        existing_keys = {(t["symbol"], t["atm_strike"], t["direction"]) for t in active.values()}
        for s in ranked:
            sig_key = (s["symbol"], s["atm_strike"], s["direction"])
            if sig_key in existing_keys:
                continue
            max_lots = int(free_capital // s["cost_per_lot"])
            if max_lots <= 0:
                continue
            weight = max(0.5, min(2.0, s["confidence"] / 6.0))
            desired_lots = max(1, int(math.floor(weight)))
            lots = max(1, min(max_lots, desired_lots))
            capital_used = round(lots * s["cost_per_lot"], 2)
            if capital_used > free_capital:
                continue
            trade_id = str(uuid.uuid4())[:12]
            trade = {
                "trade_id": trade_id,
                "trade_date": now_ist().date().isoformat(),
                "symbol": s["symbol"],
                "direction": s["direction"],
                "option_type": s["option_type"],
                "expiry": s.get("expiry"),
                "atm_strike": s["atm_strike"],
                "strategy": s["strategy"],
                "regime": s["regime"],
                "confidence": s["confidence"],
                "risk_score": s["risk_score"],
                "score": s["score"],
                "signal_close": s["close"],
                "entry_time": now_ist().isoformat(),
                "entry_premium": s["atm_prem"],
                "qty": lots * s["lot"],
                "lots": lots,
                "lot_size": s["lot"],
                "capital_used": capital_used,
                "initial_sl": s["sl_prem"],
                "trail_sl": s["sl_prem"],
                "target_anchor": s["tgt_prem"],
                "highest_premium_seen": s["atm_prem"],
                "lowest_premium_seen": s["atm_prem"],
                "last_premium": s["atm_prem"],
                "status": "OPEN",
                "meta": s,
            }
            state["active_trades"][trade_id] = trade
            state["peak_capital_used"] = max(state.get("peak_capital_used", 0.0), used_capital + capital_used)
            save_trade_to_db(trade)
            _persist_state()
            used_capital += capital_used
            free_capital = CAPITAL - used_capital
            existing_keys.add(sig_key)
            send_text(build_signal_msg(s, lots, capital_used))
            log.info(f"Paper trade taken: {trade_id} {s['symbol']} {s['atm_strike']} {s['direction']}")


def close_trade(trade_id, exit_premium, exit_reason):
    with _lock:
        trade = state["active_trades"].get(trade_id)
        if not trade:
            return None
        trade["last_premium"] = exit_premium
        trade["exit_time"] = now_ist().isoformat()
        trade["exit_premium"] = float(exit_premium)
        trade["exit_reason"] = exit_reason
        gross_pnl = (float(exit_premium) - float(trade["entry_premium"])) * int(trade["qty"])
        charges = round((float(trade["entry_premium"]) * int(trade["qty"]) + float(exit_premium) * int(trade["qty"])) * PAPER_CHARGES_PCT, 2)
        net_pnl = round(gross_pnl - charges, 2)
        trade["gross_pnl"] = round(gross_pnl, 2)
        trade["charges"] = charges
        trade["net_pnl"] = net_pnl
        trade["status"] = "CLOSED"
        update_trade_db_exit(trade)
        state["realized_pnl"] += net_pnl
        if net_pnl < 0:
            state["daily_loss"] += abs(net_pnl)
        state["active_trades"].pop(trade_id, None)
        _persist_state()
    send_text(
        f"*PAPER EXIT*\n\n"
        f"{trade['symbol']} {trade['atm_strike']} {trade['direction']}\n"
        f"Exit reason: {exit_reason}\n"
        f"Entry: Rs.{trade['entry_premium']} | Exit: Rs.{exit_premium}\n"
        f"Qty: {trade['qty']}\n"
        f"Net P&L: Rs.{trade['net_pnl']:,.2f}"
    )
    log.info(f"Trade closed: {trade_id} reason={exit_reason} net={trade['net_pnl']}")
    return trade

# ─────────────────────────────────────────
# TRAILING / MONITOR
# ─────────────────────────────────────────
def update_trailing_levels(trade, live_prem):
    entry = float(trade["entry_premium"])
    initial_sl = float(trade["initial_sl"])
    risk_unit = max(1.0, entry - initial_sl)
    highest = max(float(trade.get("highest_premium_seen", live_prem)), float(live_prem))
    lowest = min(float(trade.get("lowest_premium_seen", live_prem)), float(live_prem))
    trade["highest_premium_seen"] = round(highest, 2)
    trade["lowest_premium_seen"] = round(lowest, 2)
    trade["last_premium"] = round(float(live_prem), 2)

    trail_sl = float(trade.get("trail_sl", initial_sl))
    target_anchor = float(trade.get("target_anchor", entry + 2 * risk_unit))

    if highest >= entry + 1.0 * risk_unit:
        trail_sl = max(trail_sl, entry)
    if highest >= entry + 1.5 * risk_unit:
        trail_sl = max(trail_sl, entry + 0.5 * risk_unit)
    if highest >= entry + 2.0 * risk_unit:
        trail_sl = max(trail_sl, highest - 0.8 * risk_unit)
        target_anchor = max(target_anchor, highest + 0.8 * risk_unit)
    if highest >= entry + 3.0 * risk_unit:
        trail_sl = max(trail_sl, highest - 0.6 * risk_unit)
        target_anchor = max(target_anchor, highest + 1.0 * risk_unit)

    trade["trail_sl"] = round(trail_sl, 2)
    trade["target_anchor"] = round(target_anchor, 2)
    return trade


def monitor_open_trades():
    trades = active_trades()
    if not trades:
        return
    for trade_id, trade in trades.items():
        try:
            live_prem = get_trade_live_premium(trade)
            if live_prem is None:
                continue
            with _lock:
                current = state["active_trades"].get(trade_id)
                if not current:
                    continue
                update_trailing_levels(current, live_prem)
                update_trade_db_exit({**current, "status": "OPEN", "meta": current.get("meta", {})})
                _persist_state()
                sl = float(current["trail_sl"])
                target_anchor = float(current["target_anchor"])
            if live_prem <= sl:
                close_trade(trade_id, live_prem, "TRAIL_SL_HIT")
                continue
            if live_prem >= target_anchor:
                with _lock:
                    current = state["active_trades"].get(trade_id)
                    if current:
                        bonus = max(1.0, float(current["entry_premium"]) - float(current["initial_sl"]))
                        current["target_anchor"] = round(live_prem + 0.75 * bonus, 2)
                        current["trail_sl"] = round(max(float(current["trail_sl"]), live_prem - 0.6 * bonus), 2)
                        update_trade_db_exit({**current, "status": "OPEN", "meta": current.get("meta", {})})
                        _persist_state()
        except Exception as e:
            log.error(f"monitor_open_trades error for {trade_id}: {e}")

# ─────────────────────────────────────────
# REPORTS / CSV / EOD
# ─────────────────────────────────────────
def export_day_csv(day_str):
    rows = conn.execute(
        "SELECT * FROM trades WHERE trade_date=? ORDER BY entry_time ASC",
        (day_str,),
    ).fetchall()
    path = EXPORT_DIR / f"trades_{day_str.replace('-', '')}.csv"
    headers = [d[0] for d in conn.execute("SELECT * FROM trades LIMIT 1").description]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        for row in rows:
            writer.writerow([row[h] for h in headers])
    return str(path)


def build_day_summary(day_str):
    rows = conn.execute(
        "SELECT * FROM trades WHERE trade_date=? ORDER BY entry_time ASC",
        (day_str,),
    ).fetchall()
    total = len(rows)
    wins = sum(1 for r in rows if (r["net_pnl"] or 0) > 0)
    losses = sum(1 for r in rows if (r["net_pnl"] or 0) <= 0)
    gross = round(sum((r["gross_pnl"] or 0) for r in rows), 2)
    charges = round(sum((r["charges"] or 0) for r in rows), 2)
    net = round(sum((r["net_pnl"] or 0) for r in rows), 2)
    peak_cap = round(float(get_st("peak_capital_used")), 2)
    lines = [
        f"*EOD PAPER SUMMARY*",
        f"Date: {day_str}",
        f"Trades: {total}",
        f"Wins: {wins} | Losses: {losses}",
        f"Gross P&L: Rs.{gross:,.2f}",
        f"Charges: Rs.{charges:,.2f}",
        f"Net P&L: Rs.{net:,.2f}",
        f"Peak capital used: Rs.{peak_cap:,.2f}",
        "",
    ]
    for r in rows[:20]:
        lines.append(
            f"- {r['symbol']} {r['atm_strike']} {r['direction']} | Entry {r['entry_premium']} | Exit {r['exit_premium']} | Qty {r['qty']} | P&L {r['net_pnl']} | {r['exit_reason']}"
        )
    return "\n".join(lines), {
        "total_trades": total,
        "wins": wins,
        "losses": losses,
        "gross_pnl": gross,
        "charges": charges,
        "net_pnl": net,
        "peak_capital_used": peak_cap,
    }


def send_eod_report(force=False):
    day_str = now_ist().date().isoformat()
    if get_st("eod_done") and not force:
        return
    open_trs = active_trades()
    for trade_id, trade in list(open_trs.items()):
        live = get_trade_live_premium(trade)
        if live is None:
            live = float(trade.get("last_premium") or trade.get("entry_premium"))
        close_trade(trade_id, live, "EOD_EXIT")
    csv_path = export_day_csv(day_str)
    summary_text, stats = build_day_summary(day_str)
    send_text(summary_text)
    send_document(csv_path, caption=f"Paper tradebook {day_str}")
    with conn:
        conn.execute(
            "INSERT OR REPLACE INTO daily_summary (day, total_trades, wins, losses, gross_pnl, charges, net_pnl, peak_capital_used, summary_text, csv_path, sent_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                day_str,
                stats["total_trades"],
                stats["wins"],
                stats["losses"],
                stats["gross_pnl"],
                stats["charges"],
                stats["net_pnl"],
                stats["peak_capital_used"],
                summary_text,
                csv_path,
                now_ist().isoformat(),
            ),
        )
    set_st("eod_done", True)

# ─────────────────────────────────────────
# COMMANDS / POLLING
# ─────────────────────────────────────────
def build_open_trades_text():
    trs = active_trades()
    if not trs:
        return "No open paper trades."
    lines = ["*OPEN PAPER TRADES*", ""]
    for t in trs.values():
        lines.append(
            f"- {t['symbol']} {t['atm_strike']} {t['direction']} | Entry {t['entry_premium']} | Last {t.get('last_premium', t['entry_premium'])} | SL {t['trail_sl']} | Qty {t['qty']}"
        )
    return "\n".join(lines)


def handle_command(message):
    text = message.get("text", "").strip().lower()
    log.info(f"Command received: {text}")
    if text in ("/start", "/help"):
        send_text(
            "*Dhan Paper Algo Bot — Commands*\n\n"
            "/status — Capital, pnl, open trades\n"
            "/open — List open paper trades\n"
            "/today — Send today summary\n"
            "/export — Send today CSV\n"
            "/pause — Pause scanning\n"
            "/resume — Resume scanning\n"
            "/closeall — Force close all paper trades now\n"
            "/help — Show this list"
        )
    elif text == "/status":
        trs = active_trades()
        send_text(
            f"*BOT STATUS*\n\n"
            f"Open trades: {len(trs)}\n"
            f"Capital used: Rs.{active_capital_used():,.2f} / Rs.{CAPITAL:,.2f}\n"
            f"Remaining capital: Rs.{(CAPITAL - active_capital_used()):,.2f}\n"
            f"Realized P&L: Rs.{get_st('realized_pnl'):,.2f}\n"
            f"Daily loss: Rs.{get_st('daily_loss'):,.2f} / Rs.{MAX_DAILY_LOSS:,.2f}\n"
            f"Scanning: {'PAUSED' if get_st('paused') else 'ACTIVE'}\n"
            f"Token: {'SET' if DHAN_ACCESS_TOKEN else 'MISSING'}"
        )
    elif text == "/open":
        send_text(build_open_trades_text())
    elif text == "/today":
        summary_text, _ = build_day_summary(now_ist().date().isoformat())
        send_text(summary_text)
    elif text == "/export":
        path = export_day_csv(now_ist().date().isoformat())
        send_document(path, caption="Today tradebook")
    elif text == "/pause":
        set_st("paused", True)
        send_text("Scanning paused. Open trade monitoring continues.")
    elif text == "/resume":
        set_st("paused", False)
        send_text("Scanning resumed.")
    elif text == "/closeall":
        for trade_id, trade in list(active_trades().items()):
            live = get_trade_live_premium(trade)
            if live is None:
                live = float(trade.get("last_premium") or trade.get("entry_premium"))
            close_trade(trade_id, live, "MANUAL_CLOSEALL")
        send_text("All open paper trades closed.")
    else:
        send_text(f"Unknown command: {text}\nSend /help for list of commands.")


def telegram_polling_thread():
    log.info("Telegram polling thread started")
    offset = 0
    while True:
        try:
            resp = requests.get(
                f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates",
                params={"offset": offset, "timeout": 25, "allowed_updates": ["message"]},
                timeout=30,
            )
            for upd in resp.json().get("result", []):
                offset = upd["update_id"] + 1
                if "message" in upd:
                    msg = upd["message"]
                    if msg.get("text", "").startswith("/"):
                        try:
                            handle_command(msg)
                        except Exception as e:
                            log.error(f"Command error: {e}")
        except requests.exceptions.Timeout:
            pass
        except Exception as e:
            log.error(f"Polling thread error: {e}")
            time.sleep(5)

# ─────────────────────────────────────────
# TIME HELPERS / DAILY RESET
# ─────────────────────────────────────────
def is_weekend():
    return now_ist().weekday() >= 5


def is_trading_window():
    n = now_ist()
    if n.weekday() >= 5:
        return False
    m = n.hour * 60 + n.minute
    return 9 * 60 + 20 <= m <= 15 * 60 + 25


def before_entry_cutoff():
    return time_str() < ENTRY_CUTOFF


def wait_next_5min():
    n = now_ist()
    secs = n.minute * 60 + n.second
    gap = ((secs // 300) + 1) * 300 - secs
    time.sleep(max(1, gap))


def reset_new_day():
    with _lock:
        state.update(
            {
                "current_day": now_ist().date().isoformat(),
                "daily_loss": 0.0,
                "realized_pnl": 0.0,
                "active_trades": {},
                "rules_sent": {"open": False, "mid": False, "close": False},
                "last_heartbeat_hour": -1,
                "holiday_sent": False,
                "paused": False,
                "token_reminder_sent": False,
                "eod_done": False,
                "peak_capital_used": 0.0,
            }
        )
        for nm in regime_state:
            regime_state[nm] = {"last": None, "count": 0}
        _expiry_cache.clear()
        _persist_state()
    log.info(f"New day reset: {now_ist().date()}")

# ─────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────
def main():
    log.info("=" * 60)
    log.info(" Dhan Paper Algo Bot - V2 Step 2")
    log.info("=" * 60)
    log.info(f"DATA_DIR={DATA_DIR}")
    log.info(f"DB_PATH={DB_PATH}")
    log.info(f"BOT_TOKEN={'SET' if BOT_TOKEN else 'MISSING'}")
    log.info(f"DHAN_CLIENT_ID={'SET' if DHAN_CLIENT_ID else 'MISSING'}")
    log.info(f"DHAN_ACCESS_TOKEN={'SET' if DHAN_ACCESS_TOKEN else 'MISSING'}")
    _load_state()
    if get_st("current_day") != now_ist().date().isoformat():
        reset_new_day()

    poll = threading.Thread(target=telegram_polling_thread, daemon=True)
    poll.start()

    last_monitor_check = 0

    while True:
        try:
            n = now_ist()
            t = time_str()

            if is_weekend():
                if not get_st("holiday_sent"):
                    send_text("Market closed today. See you next trading day.")
                    set_st("holiday_sent", True)
                time.sleep(1800)
                continue

            if get_st("current_day") != n.date().isoformat():
                reset_new_day()

            if TOKEN_REMINDER_TIME <= t < "21:31" and not get_st("token_reminder_sent"):
                send_text(
                    "Dhan Token Reminder\n\n"
                    "Generate and update your DHAN_ACCESS_TOKEN before the next trading day."
                )
                set_st("token_reminder_sent", True)

            now_ts = time.time()
            if now_ts - last_monitor_check >= MONITOR_INTERVAL_SEC:
                monitor_open_trades()
                last_monitor_check = now_ts

            if EOD_SQUAREOFF <= t < "15:20" and not get_st("eod_done"):
                send_eod_report(force=False)

            if is_trading_window() and before_entry_cutoff() and not get_st("paused"):
                if get_st("daily_loss") >= MAX_DAILY_LOSS:
                    log.info("Daily loss limit reached - no new entries")
                    wait_next_5min()
                    continue
                signals = []
                for name in SYMBOLS:
                    result = scan_symbol(name)
                    if result:
                        signals.append(result)
                if signals:
                    signals = sorted(signals, key=lambda x: (x["score"], x["confidence"]), reverse=True)
                    if len(signals) > 1:
                        summary = ["*MULTI SIGNAL SCAN*", ""]
                        for s in signals:
                            summary.append(f"- {s['symbol']} {s['atm_strike']} {s['direction']} | Conf {s['confidence']} | Score {s['score']} | Prem Rs.{s['atm_prem']}")
                        send_text("\n".join(summary))
                    allocate_and_take_trades(signals)
                wait_next_5min()
            else:
                time.sleep(5)
        except Exception as e:
            log.error(f"Main loop error: {e}")
            time.sleep(5)


if __name__ == "__main__":
    main()
