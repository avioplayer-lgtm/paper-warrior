import os
import time
import json
import sqlite3
import logging
import threading
import traceback
import requests
import pandas as pd
import pytz
from datetime import datetime, date
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

DATA_DIR = Path(os.getenv("DATA_DIR", ".")).resolve()
DATA_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = DATA_DIR / "state.db"
LOG_PATH = DATA_DIR / "bot.log"

BOT_TOKEN = os.environ.get("BOT_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")
DHAN_CLIENT_ID = (os.environ.get("DHAN_CLIENT_ID") or "").strip()
DHAN_ACCESS_TOKEN = (os.environ.get("DHAN_ACCESS_TOKEN") or "").strip()
PAPER_MODE = os.getenv("PAPER_MODE", "1") == "1"
LIVE_TRADING = os.getenv("LIVE_TRADING", "0") == "1"
STARTUP_DHAN_CHECK = os.getenv("STARTUP_DHAN_CHECK", "1") == "1"
SCAN_INTERVAL_SEC = int(os.getenv("SCAN_INTERVAL_SEC", "300"))

if not BOT_TOKEN or not CHAT_ID:
    raise ValueError("BOT_TOKEN and CHAT_ID must be set as environment variables.")

CAPITAL = float(os.environ.get("CAPITAL", 30000))
MAX_DAILY_LOSS = float(os.environ.get("MAX_DAILY_LOSS", 600))
IST = pytz.timezone("Asia/Kolkata")

SYMBOLS = {
    "NIFTY": {"dhan_scrip": "13", "ws_scrip": 13},
    "BANKNIFTY": {"dhan_scrip": "25", "ws_scrip": 25},
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(LOG_PATH), logging.StreamHandler()],
)
log = logging.getLogger("PaperWarrior")

conn = sqlite3.connect(DB_PATH, check_same_thread=False)
conn.execute("CREATE TABLE IF NOT EXISTS kv (k TEXT PRIMARY KEY, v TEXT)")
conn.commit()
_lock = threading.Lock()

state = {
    "current_day": None,
    "daily_loss": 0.0,
    "paused": False,
    "last_status": "INIT",
}


def now_ist():
    return datetime.now(IST)


def today_str():
    return now_ist().strftime("%Y-%m-%d")


def _persist_state():
    payload = json.dumps(state, default=str)
    with conn:
        conn.execute("INSERT OR REPLACE INTO kv (k, v) VALUES (?, ?)", ("state", payload))


def _load_state():
    try:
        row = conn.execute("SELECT v FROM kv WHERE k='state'").fetchone()
        if not row:
            log.info("No saved state found - starting fresh")
            return
        saved = json.loads(row[0])
        with _lock:
            state.update(saved)
        log.info("State restored from SQLite")
    except Exception as e:
        log.error(f"_load_state error: {e}")


def assert_paper_only():
    if LIVE_TRADING:
        raise RuntimeError("LIVE_TRADING=1 is blocked in this deployment. This build is paper-only.")
    if not PAPER_MODE:
        raise RuntimeError("PAPER_MODE must remain enabled for this deployment.")


def log_dhan_http_error(tag, resp=None, exc=None):
    try:
        if resp is not None:
            body = resp.text[:500] if getattr(resp, "text", None) else "<no-body>"
            log.error(f"{tag} HTTP {resp.status_code} body={body}")
        if exc is not None:
            log.error(f"{tag} exception: {exc}")
            log.error(traceback.format_exc())
    except Exception as log_exc:
        log.error(f"log_dhan_http_error failed: {log_exc}")


def dhan_headers():
    return {
        "Accept": "application/json",
        "access-token": DHAN_ACCESS_TOKEN,
        "client-id": DHAN_CLIENT_ID,
        "Content-Type": "application/json",
    }


def startup_dhan_health_check():
    if not STARTUP_DHAN_CHECK:
        log.info("Startup Dhan check disabled")
        return
    log.info("Running startup Dhan data-only health check...")
    payload = {
        "securityId": "13",
        "exchangeSegment": "IDX_I",
        "instrument": "INDEX",
        "interval": "5",
        "fromDate": f"{today_str()} 09:15:00",
        "toDate": f"{today_str()} 15:30:00",
    }
    try:
        resp = requests.post(
            "https://api.dhan.co/v2/charts/intraday",
            headers=dhan_headers(),
            json=payload,
            timeout=20,
        )
        if resp.status_code == 200:
            log.info(f"Startup Dhan check OK: HTTP 200 body={resp.text[:300]}")
            with _lock:
                state["last_status"] = "DHAN_OK"
                _persist_state()
        else:
            log_dhan_http_error("startup_dhan_health_check", resp=resp)
            with _lock:
                state["last_status"] = f"DHAN_HTTP_{resp.status_code}"
                _persist_state()
    except Exception as exc:
        log_dhan_http_error("startup_dhan_health_check", exc=exc)
        with _lock:
            state["last_status"] = "DHAN_EXCEPTION"
            _persist_state()


def get_dhan_candles(name):
    scrip_id = SYMBOLS[name]["dhan_scrip"]
    payload = {
        "securityId": scrip_id,
        "exchangeSegment": "IDX_I",
        "instrument": "INDEX",
        "interval": "5",
        "fromDate": f"{today_str()} 09:15:00",
        "toDate": f"{today_str()} 15:30:00",
    }
    try:
        resp = requests.post(
            "https://api.dhan.co/v2/charts/intraday",
            headers=dhan_headers(),
            json=payload,
            timeout=20,
        )
        if resp.status_code != 200:
            log_dhan_http_error(f"{name} get_dhan_candles", resp=resp)
            return None
        data = resp.json()
        if not data.get("open"):
            log.warning(f"{name}: Empty candle data from Dhan body={resp.text[:300]}")
            return None
        df = pd.DataFrame({
            "Open": data["open"],
            "High": data["high"],
            "Low": data["low"],
            "Close": data["close"],
            "Volume": data.get("volume", [0] * len(data["open"])),
        }, index=pd.to_datetime(data["timestamp"], unit="s", utc=True).tz_convert(IST))
        log.info(f"{name}: Got {len(df)} candles from Dhan")
        return df
    except Exception as exc:
        log_dhan_http_error(f"{name} get_dhan_candles", exc=exc)
        return None


def _tg(endpoint, payload):
    try:
        r = requests.post(f"https://api.telegram.org/bot{BOT_TOKEN}/{endpoint}", json=payload, timeout=10)
        return r.json()
    except Exception as e:
        log.error(f"Telegram error ({endpoint}): {e}")
        return {}


def send_text(text):
    _tg("sendMessage", {"chat_id": CHAT_ID, "text": text})


def handle_command(message):
    text = message.get("text", "").strip().lower()
    log.info(f"Command received: {text}")
    if text in ("/start", "/help"):
        send_text(
            "Dhan Paper Algo Bot — Paper Only\n\n"
            "/status — Bot status\n"
            "/health — Run Dhan data health check\n"
            "/pause — Pause scan loop\n"
            "/resume — Resume scan loop\n"
            "/help — Show commands"
        )
    elif text == "/status":
        send_text(
            f"BOT STATUS\n\n"
            f"Paper mode: {'ON' if PAPER_MODE else 'OFF'}\n"
            f"Live trading: {'ON' if LIVE_TRADING else 'OFF'}\n"
            f"Scanning: {'PAUSED' if state.get('paused') else 'ACTIVE'}\n"
            f"Last status: {state.get('last_status')}\n"
            f"Daily loss: Rs.{state.get('daily_loss', 0):.2f} / Rs.{MAX_DAILY_LOSS:.2f}\n"
            f"Token: {'SET' if DHAN_ACCESS_TOKEN else 'MISSING'}"
        )
    elif text == "/health":
        startup_dhan_health_check()
        send_text(f"Health check completed. Last status: {state.get('last_status')}")
    elif text == "/pause":
        with _lock:
            state["paused"] = True
            _persist_state()
        send_text("Scanning paused")
    elif text == "/resume":
        with _lock:
            state["paused"] = False
            _persist_state()
        send_text("Scanning resumed")
    else:
        send_text("Unknown command. Send /help")


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
                if "message" in upd and upd["message"].get("text", "").startswith("/"):
                    handle_command(upd["message"])
        except requests.exceptions.Timeout:
            pass
        except Exception as e:
            log.error(f"Polling thread error: {e}")
            time.sleep(5)


def is_trading_window():
    n = now_ist()
    if n.weekday() >= 5:
        return False
    m = n.hour * 60 + n.minute
    return 9 * 60 + 15 <= m <= 15 * 60 + 30


def main_loop():
    while True:
        try:
            if not state.get("paused") and is_trading_window():
                for name in SYMBOLS:
                    get_dhan_candles(name)
            else:
                log.info("Outside trading window or paused - skipping scan")
        except Exception as e:
            log.error(f"Main loop error: {e}")
        time.sleep(SCAN_INTERVAL_SEC)


def main():
    log.info("=" * 60)
    log.info(" Dhan Paper Algo Bot - V2 Paper Only Diagnostic")
    log.info("=" * 60)
    log.info(f"DATA_DIR={DATA_DIR}")
    log.info(f"DB_PATH={DB_PATH}")
    log.info(f"BOT_TOKEN={'SET' if BOT_TOKEN else 'MISSING'}")
    log.info(f"DHAN_CLIENT_ID={'SET' if DHAN_CLIENT_ID else 'MISSING'}")
    log.info(f"DHAN_ACCESS_TOKEN={'SET' if DHAN_ACCESS_TOKEN else 'MISSING'}")
    _load_state()
    assert_paper_only()
    startup_dhan_health_check()
    threading.Thread(target=telegram_polling_thread, daemon=True).start()
    main_loop()


if __name__ == "__main__":
    main()
