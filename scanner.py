#!/usr/bin/env python3
"""
NSE Live Pattern Scanner v3.4
==============================
Fixes from v3.2 (what you saw failing in GitHub Actions):
  1. DB PERSISTENCE — GitHub Actions is stateless. Each run is a fresh container.
     Fix: watchlist saved as watchlist.json (committed back to repo via git).
     DB still used but rebuilt each daily scan from scratch.
  2. NSE UNIVERSE BLOCKED — archives.nseindia.com blocks GitHub IPs frequently.
     Fix: hardcoded NIFTY_500_FALLBACK (500 top liquid NSE stocks). Used when
     live URL fails. Also added alternate URL.
  3. CSV SENT TO TELEGRAM — after daily scan, CSV is sent as a Telegram document
     (not just a message). Uses sendDocument API.
  4. SCHEDULE — 3 daily scans (8 AM, 12:30 PM, 4:30 PM IST) + every 30 min
     during market hours (9:15 AM - 3:30 PM IST).
  5. 28s RUNS = hourly scan was fetching nothing (universe blocked, empty watchlist).
     Fix: hourly now uses hardcoded fallback so it always scans something.
  6. Telegram sends CSV as a file attachment, not just text.

Usage:
  python scanner.py --daily      # full scan (8AM, 12:30PM, 4:30PM)
  python scanner.py --halfhour   # 30-min watchlist + quick-scan
  python scanner.py --dashboard  # Flask web UI
  python scanner.py --healthcheck
  python scanner.py --test
"""

import os, sys, json, time, sqlite3, argparse, logging
from datetime import date, datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import warnings
warnings.filterwarnings("ignore")

# IST = UTC+5:30. GitHub Actions runs UTC — this makes all times correct.
_IST = timezone(timedelta(hours=5, minutes=30))
def _now():  return datetime.now(_IST)
def _ist(fmt="%H:%M IST"): return _now().strftime(fmt)
def _today(): return _now().date()

import yfinance as yf
import pandas as pd
import numpy as np
from scipy.signal import find_peaks

# ================================================================
# PATHS & LOGGING
# ================================================================
BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
LOG_DIR    = os.path.join(BASE_DIR, "logs")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
DB_PATH    = os.path.join(BASE_DIR, "signals.db")
WL_PATH    = os.path.join(BASE_DIR, "watchlist.json")   # persisted via git

for d in [LOG_DIR, OUTPUT_DIR]:
    os.makedirs(d, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-5s %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(
            os.path.join(LOG_DIR, f"scan_{_today()}.log"),
            encoding="utf-8"),
    ],
)
log = logging.getLogger("scanner")

# ================================================================
# CONFIG
# ================================================================
NIFTY_SYM    = "^NSEI"
PERIOD_DAILY = "1y"
PERIOD_QUICK = "3mo"
MAX_WORKERS  = 4
DL_RETRIES   = 3
DL_BACKOFF   = 3.0
QUICK_SIZE   = 300   # stocks scanned in 30-min mode

TG_TOKEN = os.environ.get("TG_BOT_TOKEN", "")
TG_CHAT  = os.environ.get("TG_CHAT_ID", "")

CS = {
    "C_min": 0.25, "A_min": 0.25, "N_max_from_high": 0.15,
    "L_min_rs": 1.10, "I_min_instl": 0.20,
    "buy_strong": 6, "buy_moderate": 4,
}

INTRADAY_DETECTORS = {"MomBurst", "EpisodicPivot", "PocketPivot"}

# ================================================================
# NIFTY 500 FALLBACK — used when NSE URL is blocked (GitHub IPs)
# Top 300 liquid NSE stocks hardcoded so scanner ALWAYS works
# ================================================================
NIFTY_500_FALLBACK = [
    "RELIANCE","TCS","HDFCBANK","INFY","ICICIBANK","HINDUNILVR","ITC","SBIN",
    "BHARTIARTL","KOTAKBANK","LT","AXISBANK","BAJFINANCE","ASIANPAINT","MARUTI",
    "SUNPHARMA","TITAN","WIPRO","ULTRACEMCO","BAJAJFINSV","NESTLEIND","POWERGRID",
    "NTPC","TECHM","TATAMOTORS","HCLTECH","JSWSTEEL","TATASTEEL","ADANIENT","ADANIPORTS",
    "ONGC","COALINDIA","BRITANNIA","DIVISLAB","DRREDDY","EICHERMOT","GRASIM","HDFCLIFE",
    "INDUSINDBK","M&M","SBILIFE","SHREECEM","TATACONSUM","UPL","CIPLA","APOLLOHOSP",
    "BAJAJ-AUTO","BPCL","DABUR","HAVELLS","HEROMOTOCO","HINDPETRO","IOC","LTIM",
    "LUPIN","MARICO","MCDOWELL-N","MUTHOOTFIN","NAUKRI","PIDILITIND","PIIND",
    "SIEMENS","TORNTPHARM","TRENT","VEDL","VOLTAS","ZOMATO","PAYTM","NYKAA","DELHIVERY",
    "IRCTC","LICI","ADANIGREEN","ADANITRANS","ATGL","AWL","CANBK","BANKBARODA",
    "FEDERALBNK","IDFCFIRSTB","INDIGO","IRFC","JSWENERGY","LAURUSLABS","LICHSGFIN",
    "LINDEINDIA","MOTHERSON","MRF","NMDC","OBEROIRLTY","PAGEIND","PETRONET","PFC",
    "POLYCAB","RECLTD","SAIL","SBICARD","TATAPOWER","TIINDIA","TVSMOTOR","VBL",
    "ZYDUSLIFE","ABCAPITAL","ABIRLANUVO","ACC","ADANIPOWER","AEGISCHEM","AIAENG",
    "AJANTPHARM","AKZOINDIA","ALKEM","AMARAJABAT","AMBUJACEM","APLAPOLLO","APLLTD",
    "ASTRAL","ATUL","AUBANK","AUROPHARMA","BALKRISIND","BANDHANBNK","BATAINDIA",
    "BAYERCROP","BERGEPAINT","BIOCON","BLUESTAR","BSOFT","CANFINHOME","CASTROLIND",
    "CEATLTD","CENTURYPLY","CESC","CHOLAFIN","CUMMINSIND","CYIENT","DEEPAKNTR",
    "DIXON","DMART","ESCORTS","EXIDEIND","FINEORG","FLUOROCHEM","FORTIS","GAIL",
    "GLAND","GLAXO","GMRINFRA","GNFC","GODREJCP","GODREJIND","GODREJPROP","GRANULES",
    "GSPL","GUIGAS","HAL","HINDALCO","HINDCOPPER","HONAUT","IBREALEST","ICICIPRULI",
    "IDBI","IEX","IGL","INDHOTEL","INDUSTOWER","INOXWIND","INTELLECT","IPCALAB",
    "JKCEMENT","JUBLFOOD","JUBLINGREA","KAJARIACER","KANSAINER","KPITTECH","KPRMILL",
    "KRBL","LALPATHLAB","LEMONTREE","LICI","LTTS","LUXIND","MAHSEAMLES","MANAPPURAM",
    "MAPMYINDIA","MAXHEALTH","MCX","MEDPLUS","METROBRAND","MFSL","MGLAMINES",
    "MHRIL","MIDHANI","MINDTREE","MKPL","MRPL","NATCOPHARM","NAVINFLUOR","NAUKRI",
    "NBCC","NDTV","NHPC","NLCINDIA","NSLNISP","NUVAMA","OFSS","OLECTRA",
    "OPTIEMUS","ORIENTELEC","PGHH","PHOENIXLTD","PNBHOUSING","POLICYBZR","PRAJIND",
    "PRESTIGE","PRINCEPIPE","PRIVISCL","PSPPROJECT","PVRINOX","RADICO","RAILTEL",
    "RAININD","RAJESHEXPO","RAYMOND","RBLBANK","RCF","REDINGTON","RELAXO",
    "RITES","RKFORGE","ROSSARI","ROUTE","SAFARI","SAPPHIRE","SCHAEFFLER","SEQUENT",
    "SFL","SHYAMMETL","SIGNATURE","SJVN","SKFINDIA","SOBHA","SOLARINDS","SONACOMS",
    "SPANDANA","SPARC","SPIML","SRF","STARCEMENT","SUNTV","SUPRAJIT","SUVEN",
    "SUZLON","SWANENERGY","SYMPHONY","TANLA","TATACHEM","TATACOMM","TATAELXSI",
    "TATAINVEST","TATATECH","TCPL","TEAMLEASE","TEJASNET","THYROCARE","TIMKEN",
    "TTKPRESTIG","UJJIVANSFB","UNITDSPR","UTIAMC","VAIBHAVGBL","VGUARD","VIPIND",
    "VINATIORGA","VSTIND","WABAG","WELCORP","WELSPUNLIV","WESTLIFE","WHIRLPOOL",
    "WIPRO","WOCKPHARMA","ZEEL","ZENTEC","ZFCVINDIA",
]
NIFTY_500_FALLBACK_NS = [s + ".NS" for s in NIFTY_500_FALLBACK]

# ================================================================
# DATABASE (rebuilt each daily run — stateless)
# ================================================================
_db_lock = Lock()

def get_db():
    con = sqlite3.connect(DB_PATH, check_same_thread=False)
    con.execute("PRAGMA journal_mode=WAL")
    con.executescript("""
        CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            scan_date TEXT, scan_time TEXT, scan_mode TEXT,
            stock TEXT, name TEXT, sector TEXT,
            cap_class TEXT, cap_cr REAL,
            pattern TEXT, timeframe TEXT, status TEXT,
            breakout_zone REAL, cmp REAL, stop_loss REAL,
            target_1 REAL, target_2 REAL, target_3 REAL,
            risk_reward REAL, quality REAL, vol_surge REAL,
            canslim_score INTEGER, data_completeness INTEGER,
            converging TEXT, leg TEXT,
            earnings_near INTEGER, ftd_active INTEGER,
            vol_dryup INTEGER, stage TEXT,
            recommendation TEXT,
            m1 REAL, m2 REAL, m3 REAL, m4 REAL, m5 REAL,
            notes TEXT,
            created_at TEXT DEFAULT (datetime('now','localtime'))
        );
        CREATE TABLE IF NOT EXISTS runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            scan_date TEXT, scan_time TEXT, mode TEXT,
            stocks_total INTEGER, stocks_ok INTEGER,
            signals INTEGER, buys INTEGER, elapsed_sec REAL,
            created_at TEXT DEFAULT (datetime('now','localtime'))
        );
        CREATE TABLE IF NOT EXISTS alerts_sent (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            scan_date TEXT, stock TEXT, pattern TEXT, status TEXT,
            created_at TEXT DEFAULT (datetime('now','localtime'))
        );
        CREATE INDEX IF NOT EXISTS idx_sig_date ON signals(scan_date);
        CREATE INDEX IF NOT EXISTS idx_sig_stock ON signals(stock);
        CREATE INDEX IF NOT EXISTS idx_alerts ON alerts_sent(scan_date,stock);
    """)
    con.commit()
    return con

def db_exec(con, sql, params=None):
    with _db_lock:
        con.execute(sql, params or [])
        con.commit()

def db_execmany(con, sql, rows):
    if not rows: return
    with _db_lock:
        con.executemany(sql, rows)
        con.commit()

def db_query(con, sql, params=None):
    cur = con.execute(sql, params or [])
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]

# ================================================================
# WATCHLIST — persisted as JSON so GitHub Actions can commit it
# ================================================================
def load_watchlist():
    if os.path.exists(WL_PATH):
        try:
            with open(WL_PATH) as f:
                data = json.load(f)
            # Filter to today + last 30 days
            cutoff = str(date.today() - timedelta(days=30))
            return [w for w in data if w.get("added_date", "") >= cutoff]
        except Exception:
            pass
    return []

def save_watchlist(items):
    with open(WL_PATH, "w") as f:
        json.dump(items, f, indent=2)
    log.info(f"Watchlist saved: {len(items)} items → {WL_PATH}")

def already_alerted_today(stock, pattern):
    path = os.path.join(OUTPUT_DIR, f"alerts_{_today()}.json")
    if not os.path.exists(path):
        return False
    try:
        with open(path) as f:
            sent = json.load(f)
        return any(a["stock"] == stock and a["pattern"] == pattern for a in sent)
    except Exception:
        return False

def mark_alert_sent(stock, pattern, status):
    path = os.path.join(OUTPUT_DIR, f"alerts_{_today()}.json")
    sent = []
    if os.path.exists(path):
        try:
            with open(path) as f:
                sent = json.load(f)
        except Exception:
            pass
    sent.append({"stock": stock, "pattern": pattern, "status": status,
                 "time": _ist("%H:%M")})
    with open(path, "w") as f:
        json.dump(sent, f)

# ================================================================
# DATA LAYER — with NSE fallback
# ================================================================
def load_universe():
    """Load NSE equity list. Falls back to hardcoded top-300 if URL blocked."""
    urls = [
        "https://archives.nseindia.com/content/equities/EQUITY_L.csv",
        "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv",
    ]
    for url in urls:
        for attempt in range(2):
            try:
                import requests as req
                resp = req.get(url, timeout=20,
                               headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"})
                resp.raise_for_status()
                from io import StringIO
                df = pd.read_csv(StringIO(resp.text)).dropna(subset=["SYMBOL"])
                for col in [" SERIES", "SERIES"]:
                    if col in df.columns:
                        df = df[df[col].str.strip() == "EQ"]; break
                syms = [s.strip() + ".NS" for s in df["SYMBOL"].astype(str).tolist()]
                log.info(f"Universe: {len(syms)} stocks from {url}")
                return syms
            except Exception as e:
                log.warning(f"Universe URL {url} attempt {attempt+1}: {e}")
                time.sleep(2)

    log.warning("NSE URL blocked — using hardcoded Nifty-500 fallback")
    return NIFTY_500_FALLBACK_NS

# ── Yahoo Finance crumb-aware session ──────────────────────────────────────
# Yahoo requires a crumb token tied to a browser-like session cookie.
# On GitHub Actions IPs, plain requests get 401 "Invalid Crumb".
# Fix: curl_cffi impersonates Chrome TLS fingerprint, warm up session once,
# reuse it for all downloads. Reset session automatically on repeated 401s.

_YF_SESSION = None
_YF_SESSION_LOCK = Lock()

def _build_session():
    try:
        from curl_cffi import requests as _cr
        sess = _cr.Session(impersonate="chrome110")
        # warm-up: hit Yahoo Finance to get cookies (crumb lives in cookie jar)
        sess.get("https://finance.yahoo.com", timeout=15)
        log.info("curl_cffi Chrome session ready")
        return sess
    except ImportError:
        log.warning("curl_cffi not installed — Yahoo may 401. pip install curl_cffi")
        return None
    except Exception as e:
        log.warning(f"Session build failed: {e}")
        return None

def _get_session():
    global _YF_SESSION
    with _YF_SESSION_LOCK:
        if _YF_SESSION is None:
            _YF_SESSION = _build_session()
        return _YF_SESSION

def _reset_session():
    global _YF_SESSION
    with _YF_SESSION_LOCK:
        _YF_SESSION = None

def dl(sym, interval="1d", period=PERIOD_DAILY):
    for attempt in range(DL_RETRIES):
        try:
            sess = _get_session()
            kw = {"session": sess} if sess is not None else {}
            df = yf.download(sym, period=period, interval=interval,
                             auto_adjust=True, progress=False, timeout=20, **kw)
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            df = df.dropna()
            return df if len(df) > 20 else None
        except Exception as e:
            msg = str(e)
            if "401" in msg or "Crumb" in msg or "Unauthorized" in msg:
                log.warning(f"401/Crumb on {sym} attempt {attempt+1} — reset session")
                _reset_session()
                time.sleep(5)
            elif attempt < DL_RETRIES - 1:
                time.sleep(DL_BACKOFF * (attempt + 1))
    return None

def dl_fund(sym):
    for attempt in range(DL_RETRIES):
        try:
            sess = _get_session()
            kw = {"session": sess} if sess is not None else {}
            tk = yf.Ticker(sym, **kw)
            info = tk.info or {}
            if not info.get("marketCap"):
                if attempt < DL_RETRIES - 1:
                    time.sleep(3)
                    continue
                return {"_fund_ok": False}
            try:
                cal = tk.calendar
                ne = cal.get("Earnings Date", [None])[0] if cal else None
            except Exception:
                ne = None
            return {
                "_fund_ok": True,
                "marketCap": info.get("marketCap"),
                "earningsQuarterlyGrowth": info.get("earningsQuarterlyGrowth"),
                "earningsGrowth": info.get("earningsGrowth"),
                "heldPercentInstitutions": info.get("heldPercentInstitutions"),
                "sector": info.get("sector"),
                "longName": info.get("longName") or info.get("shortName"),
                "next_earnings": str(ne) if ne else None,
            }
        except Exception as e:
            msg = str(e)
            if "401" in msg or "Crumb" in msg or "Unauthorized" in msg:
                log.warning(f"401/Crumb on fund {sym} attempt {attempt+1} — reset session")
                _reset_session()
                time.sleep(5)
            elif attempt < DL_RETRIES - 1:
                time.sleep(DL_BACKOFF * (attempt + 1))
    return {"_fund_ok": False}

def cap_class(mc):
    if not mc or pd.isna(mc): return "Unknown", None
    cr = mc / 1e7
    if cr >= 20000: return "Large", round(cr)
    if cr >= 5000:  return "Mid", round(cr)
    if cr >= 500:   return "Small", round(cr)
    return "Micro", round(cr)

# ================================================================
# MARKET SIGNALS
# ================================================================
def check_follow_through_day(nifty_df):
    if nifty_df is None or len(nifty_df) < 30:
        return False, "no data"
    c = nifty_df["Close"].values; v = nifty_df["Volume"].values
    low_idx = len(c) - 30 + int(np.argmin(c[-30:]))
    rally = 0
    for i in range(low_idx + 1, len(c)):
        rally = rally + 1 if c[i] > c[i-1] else 0
    gain = (c[-1] - c[-2]) / c[-2] if c[-2] > 0 else 0
    ftd = rally >= 4 and gain >= 0.015 and (v[-1] > v[-2] if len(v) >= 2 else False)
    above_200 = c[-1] > np.mean(c[-min(200, len(c)):]) if len(c) >= 50 else False
    return ftd or above_200, f"rally={rally} gain={gain:.2%}"

def check_market_trend(nc):
    if nc is None or len(nc) < 200: return "Unknown"
    ma50 = np.mean(nc[-50:]); ma200 = np.mean(nc[-200:])
    if nc[-1] > ma50 > ma200: return "Stage2-Bull"
    if nc[-1] > ma200: return "Uptrend"
    if nc[-1] < ma50 < ma200: return "Stage4-Bear"
    return "Choppy"

def check_volume_dryup(vol, lb=25):
    return vol is not None and len(vol) >= lb and vol[-1] <= np.min(vol[-lb:]) * 1.05

def check_weinstein_stage(close, p=150):
    if len(close) < p + 20: return "Unknown"
    ma = np.mean(close[-p:]); ma_p = np.mean(close[-p-20:-20])
    if close[-1] > ma and ma > ma_p: return "Stage2"
    if close[-1] > ma: return "Stage1-Late"
    if close[-1] < ma and ma < ma_p: return "Stage4"
    return "Stage3"

def check_earnings_near(fund, days=14):
    ne = fund.get("next_earnings")
    if not ne: return False
    try:
        ed = datetime.strptime(ne[:10], "%Y-%m-%d").date()
        return 0 <= (ed - date.today()).days <= days
    except Exception:
        return False

def calc_adr(close, p=20):
    if len(close) < p + 1: return 0
    r = [abs(close[i]-close[i-1])/close[i-1] for i in range(-p, 0) if close[i-1] > 0]
    return round(np.mean(r) * 100, 2) if r else 0

# ================================================================
# CANSLIM
# ================================================================
def canslim_score(close, vol, fund, nc, nr):
    n = len(close); idx = n - 1; score = 0; checks = 0
    lb = min(252, idx); hi = np.max(close[max(0,idx-lb):idx+1])
    if hi > 0:
        checks += 1
        if (hi - close[idx]) / hi <= CS["N_max_from_high"]: score += 1
    if nr is not None and idx >= 252 and idx < len(nr) and not np.isnan(nr[idx]):
        checks += 1
        sr = close[idx]/close[idx-252]-1; nrr = nr[idx]
        if (1+nrr) > 0 and (1+sr)/(1+nrr) >= CS["L_min_rs"]: score += 1
    if nc is not None and len(nc) >= 200:
        checks += 1
        if nc[-1] > np.mean(nc[-200:]): score += 1
    if vol is not None and idx >= 20:
        checks += 1
        if np.mean(vol[idx-19:idx+1])*np.mean(close[idx-19:idx+1])/1e7 >= 1.0: score += 1
    for key, th in [("earningsQuarterlyGrowth", CS["C_min"]),
                    ("earningsGrowth", CS["A_min"]),
                    ("heldPercentInstitutions", CS["I_min_instl"])]:
        v = fund.get(key)
        if v is not None:
            checks += 1
            if v >= th: score += 1
    return score, checks

def recommend(status, score, mkt_up):
    bo = any(k in status for k in ["Breakout","Burst","Pivot","Pocket"])
    if bo and score >= CS["buy_strong"] and mkt_up:  return "BUY — strong"
    if bo and score >= CS["buy_moderate"]:            return "BUY — moderate"
    if not bo and score >= CS["buy_strong"]:          return "WATCH — await breakout"
    if score >= CS["buy_moderate"]:                   return "WATCH — mixed"
    return "AVOID"

# ================================================================
# TARGETS
# ================================================================
def calc_targets(pattern, bz, bottom, cmp, adr):
    if not bz or bz <= 0: return None, None, None, None, None
    h = bz - bottom if bottom and bottom > 0 else bz * 0.10
    stop = round(bottom*0.97,2) if bottom and bottom > 0 else round(bz*0.92,2)
    if pattern in ("MomBurst","EpisodicPivot","PocketPivot"):
        t1,t2,t3 = round(cmp*1.05,2), round(cmp*1.10,2), round(cmp*1.15,2)
        stop = round(cmp*0.96,2)
    elif "Flag" in pattern:
        t1,t2,t3 = round(bz+h*0.5,2), round(bz+h,2), round(bz+h*1.5,2)
    else:
        t1,t2,t3 = round(bz+h*0.5,2), round(bz+h,2), round(bz+h*1.618,2)
    risk = cmp-stop if stop < cmp else cmp*0.05
    reward = t2-cmp if t2 > cmp else cmp*0.10
    return stop, t1, t2, t3, round(reward/risk,2) if risk > 0 else 0

def identify_leg(close, bz):
    if len(close) < 50 or not bz: return "Unknown"
    if close[-1] < bz*0.98: return "Pre-breakout"
    g = (close[-1]-bz)/bz
    if g < 0.05: return "Leg1-Early"
    if g < 0.15: return "Leg1-Trending"
    if g < 0.30: return "Leg2-Extended"
    return "Leg3-Climax"

def vsurge(vol, n, lb=20):
    if vol is None or n < lb: return None
    avg = np.mean(vol[-lb:])
    return round(float(vol[-1]/avg),2) if avg > 0 else None

# ================================================================
# ALL 13 DETECTORS (compact but complete)
# ================================================================
# ================================================================
# STOCKBEE RANKING METRICS
# Bonde TI65, 2LYNCH score, composite rank
# ================================================================

def calc_ti65(c):
    """
    Bonde's Trend Intensity 65: avg7d / avg65d.
    >= 1.05 = confirmed uptrend. Range 1.02-1.30 = sweet spot.
    """
    if len(c) < 65: return None
    d = np.mean(c[-65:])
    return round(float(np.mean(c[-7:]) / d), 4) if d > 0 else None

def lynch_score(c, v):
    """
    2LYNCH checklist (Bonde/Stockbee). Returns 0-6.
    2 = Not up 2 consecutive days before breakout
    L = Linear orderly prior move
    Y = Young trend (TI65 in 1.02-1.30)
    N = Narrow/Negative day immediately before breakout
    C = Consolidation quality (Bollinger squeeze)
    H = Closing near High today
    """
    n = len(c); score = 0
    if n < 10: return score
    # 2: not up 2 days in a row before TODAY
    if n >= 4 and not (c[-2] > c[-3] and c[-3] > c[-4]):
        score += 1
    # L: linear = low coefficient of variation of daily moves
    if n >= 21:
        moves = np.abs(np.diff(c[-21:]))
        m_mean = np.mean(moves)
        if m_mean > 0 and np.std(moves) / m_mean < 1.2:
            score += 1
    # Y: young trend
    ti = calc_ti65(c)
    if ti is not None and 1.02 <= ti <= 1.30:
        score += 1
    # N: narrow (<1%) or negative day before breakout
    if n >= 3 and c[-3] > 0:
        pm = (c[-2] - c[-3]) / c[-3]
        if abs(pm) < 0.01 or pm < 0:
            score += 1
    # C: Bollinger band squeeze (tight consolidation)
    if n >= 20:
        bb = np.std(c[-20:]) / (np.mean(c[-20:]) + 1e-9)
        if bb < 0.04:
            score += 1
    # H: closing near high (close strength >= 60%)
    if n >= 5:
        hi5 = np.max(c[-5:])
        lo5 = np.min(c[-5:])
        rng = hi5 - lo5
        cs = (c[-1] - lo5) / rng if rng > 0 else 0.5
        if cs >= 0.60:
            score += 1
    return score

def composite_rank(row):
    """
    Composite score 0-100 for sorting signals.
    Weights: CANSLIM(25) + 2LYNCH(20) + TI65(15) + VolSurge(15) + Quality(15) + ADR(10)
    """
    s = 0
    cs = row.get("canslim_score", 0) or 0
    dc = row.get("data_completeness", 7) or 7
    s += (cs / max(dc, 1)) * 25           # CANSLIM (normalised to data available)
    ls = row.get("lynch_score_val", 0) or 0
    s += (ls / 6) * 20                     # 2LYNCH
    ti = row.get("ti65", 1.0) or 1.0
    ti_score = min(max(ti - 1.0, 0), 0.30) / 0.30
    s += ti_score * 15                     # TI65 (capped at +30%)
    vs = row.get("vol_surge", 1.0) or 1.0
    s += min(vs / 3.0, 1.0) * 15          # vol surge (capped at 3x)
    q = row.get("quality", 0) or 0
    s += min(abs(q), 1.0) * 15            # pattern quality
    adr = row.get("adr_pct", 2.0) or 2.0
    s += min(adr / 5.0, 1.0) * 10         # ADR (capped at 5%)
    return round(s, 1)

def det_cup(c, v):
    n = len(c)
    if n < 50: return None
    s = pd.Series(c).rolling(5, min_periods=1).mean().values
    ti = int(np.argmin(s))
    if not (n*0.20 <= ti <= n*0.80): return None
    lm, rm = np.max(s[:ti+1]), np.max(s[ti:])
    pk, tr = max(lm,rm), s[ti]
    d = (pk-tr)/pk
    if not (0.08 <= d <= 0.55): return None
    sym = abs(lm-rm)/pk
    if sym > 0.22: return None
    rpi = ti + int(np.argmax(s[ti:]))
    if rpi >= n-2 or s[rpi] < pk*0.88: return None
    h = s[rpi:]
    if len(h) < 2: return None
    hd = (np.max(h)-np.min(h))/np.max(h)
    if hd > 0.20 or np.min(h) < (pk+tr)/2*0.92: return None
    r = (n-rpi)/(rpi+1)
    if not (0.10 <= r <= 0.45): return None
    try:
        cx = np.arange(rpi+1); cf = np.polyfit(cx, s[:rpi+1], 2)
        fit = np.polyval(cf, cx)
        r2 = 1 - np.sum((s[:rpi+1]-fit)**2)/np.sum((s[:rpi+1]-np.mean(s[:rpi+1]))**2)
        if cf[0] <= 0 or r2 < 0.50: return None
    except: return None
    vs = vsurge(v, n); bo = c[-1] >= pk*0.97 and (vs is not None and vs >= 1.2)
    return dict(pattern="CupHandle", status="Breakout Ready" if bo else "Forming",
                quality=round(r2-sym,3), bz=round(float(pk),2), bottom=round(float(tr),2),
                last=round(float(c[-1]),2), vs=vs,
                m1=round(d*100,2), m2=round(sym*100,2), m3=round(hd*100,2), m4=round(r,2), m5=round(r2,3))

def det_vcp(c, v):
    n = len(c)
    if n < 40: return None
    atr = np.mean(np.abs(np.diff(c))) if n > 1 else np.mean(c)*0.02
    prom = max(atr*1.5, np.mean(c)*0.01)
    try:
        highs, _ = find_peaks(c, prominence=prom, distance=5)
        lows, _  = find_peaks(-c, prominence=prom, distance=5)
    except: return None
    if len(highs) < 2 or len(lows) < 2: return None
    contractions = []
    hl = list(highs) + [n]
    for i, hi in enumerate(hl[:-1]):
        nh = hl[i+1]
        nl = lows[(lows > hi) & (lows < nh)]
        lo = nl[0] if len(nl)>0 else (hi+int(np.argmin(c[hi:nh])) if nh-hi>=3 else -1)
        if lo < 0 or lo >= n: continue
        depth = (c[hi]-c[lo])/c[hi]
        if depth < 0.03: continue
        contractions.append((hi, lo, depth))
    if len(contractions) < 3: return None
    depths = [ct[2] for ct in contractions]
    if not all(depths[i] <= depths[i-1]*0.85 for i in range(1,len(depths))): return None
    if contractions[-1][1] < n*0.5: return None
    pivot = float(np.max(c[highs])); vs = vsurge(v, n)
    bo = c[-1] >= pivot*0.98 and (vs is not None and vs >= 1.5)
    return dict(pattern="VCP", status="Breakout Ready" if bo else "Forming",
                quality=round(1-depths[-1],3), bz=round(pivot,2),
                bottom=round(float(c[contractions[-1][1]]),2), last=round(float(c[-1]),2), vs=vs,
                m1=round(depths[0]*100,2), m2=round(depths[-1]*100,2),
                m3=round(depths[-1]/depths[0],2) if depths[0]>0 else None, m4=len(contractions), m5=None)

def det_fb(c, v):
    n = len(c)
    if n < 35: return None
    best = None
    for bl in range(15, min(75,n)+1):
        base = c[-bl:]; bh,blo = np.max(base),np.min(base)
        br = (bh-blo)/bh if bh>0 else 1
        if br > 0.20: break
        bs = n-bl; tl = min(80,bs)
        if tl < 15: continue
        pre = c[bs-tl:bs]
        tg = (pre[-1]-np.min(pre))/np.min(pre) if np.min(pre)>0 else 0
        if tg < 0.10: continue
        if best is None or br < best["br"]: best = dict(bl=bl,bh=bh,blo=blo,br=br,tg=tg)
    if best is None: return None
    vs = vsurge(v,n); bo = c[-1]>=best["bh"]*0.99 and (vs is not None and vs>=1.2)
    return dict(pattern="FlatBase", status="Breakout Ready" if bo else "Forming",
                quality=round(best["tg"]-best["br"],3), bz=round(float(best["bh"]),2),
                bottom=round(float(best["blo"]),2), last=round(float(c[-1]),2), vs=vs,
                m1=round(best["br"]*100,2), m2=round(best["tg"]*100,2), m3=best["bl"], m4=None, m5=None)

def det_ihs(c, v):
    n = len(c)
    if n < 40: return None
    atr = np.mean(np.abs(np.diff(c))) if n>1 else np.mean(c)*0.015
    try: troughs,_ = find_peaks(-c, prominence=max(atr*1.2,np.mean(c)*0.008), distance=6)
    except: return None
    if len(troughs) < 3: return None
    hc = troughs[(troughs>n*0.20)&(troughs<n*0.80)]
    if len(hc)==0: return None
    hi = hc[np.argmin(c[hc])]
    hl = [t for t in troughs if t<hi and c[t]>c[hi]]
    hr = [t for t in troughs if t>hi and c[t]>c[hi]]
    if not hl or not hr: return None
    li,ri = hl[-1],hr[0]; ls,hd,rs_ = c[li],c[hi],c[ri]
    sa = (ls+rs_)/2; asym = abs(ls-rs_)/sa
    if asym > 0.18: return None
    hb = (sa-hd)/sa
    if not (0.03<=hb<=0.50): return None
    nl = (np.max(c[li:hi+1])+np.max(c[hi:ri+1]))/2
    if ri>=n-2: return None
    vs = vsurge(v,n); bo = c[-1]>=nl*0.99 and (vs is not None and vs>=1.2)
    return dict(pattern="InvHS", status="Breakout Ready" if bo else "Forming",
                quality=round(hb-asym,3), bz=round(float(nl),2), bottom=round(float(hd),2),
                last=round(float(c[-1]),2), vs=vs, m1=round(hb*100,2), m2=round(asym*100,2), m3=int(ri-li), m4=None, m5=None)

def det_dbot(c, v):
    n = len(c)
    if n < 30: return None
    try: troughs,_ = find_peaks(-c, prominence=0.02*np.mean(c), distance=5)
    except: return None
    if len(troughs)<2: return None
    best = None
    for i in range(len(troughs)):
        for j in range(i+1,len(troughs)):
            sep = troughs[j]-troughs[i]
            if not (10<=sep<=150): continue
            p1,p2 = c[troughs[i]],c[troughs[j]]
            diff = abs(p1-p2)/min(p1,p2)
            if diff>0.08: continue
            mid = np.max(c[troughs[i]:troughs[j]+1])
            mr = (mid-(p1+p2)/2)/((p1+p2)/2)
            if mr<0.06 or troughs[j]>=n-2: continue
            if best is None or mr-diff>best["sc"]:
                best = dict(sc=mr-diff,mid=mid,diff=diff,mr=mr,bottom=min(p1,p2))
    if best is None: return None
    vs = vsurge(v,n); bo = c[-1]>=best["mid"]*0.99 and (vs is not None and vs>=1.2)
    return dict(pattern="DoubleBottom", status="Breakout Ready" if bo else "Forming",
                quality=round(best["sc"],3), bz=round(float(best["mid"]),2),
                bottom=round(float(best["bottom"]),2), last=round(float(c[-1]),2), vs=vs,
                m1=round(best["diff"]*100,2), m2=round(best["mr"]*100,2), m3=None, m4=None, m5=None)

def det_asctri(c, v):
    n = len(c)
    if not (15<=n<=200): return None
    try:
        pks,_ = find_peaks(c, prominence=0.01*np.mean(c), distance=3)
        trs,_ = find_peaks(-c, prominence=0.01*np.mean(c), distance=3)
    except: return None
    if len(pks)<2 or len(trs)<2: return None
    pp=c[pks]; res=np.median(pp)
    if (np.max(pp)-np.min(pp))/res>0.04: return None
    tp=c[trs]
    slopes = [(tp[j]-tp[i])/(trs[j]-trs[i]) for i in range(len(trs)) for j in range(i+1,len(trs)) if trs[j]!=trs[i]]
    if not slopes or np.median(slopes)<=0: return None
    rise=(tp[-1]-tp[0])/tp[0] if tp[0]>0 else 0
    if rise<0.015 or trs[-1]<n*0.4: return None
    vs=vsurge(v,n); bo=c[-1]>=res*0.99 and (vs is not None and vs>=1.2)
    return dict(pattern="AscTriangle", status="Breakout Ready" if bo else "Forming",
                quality=round(rise,3), bz=round(float(res),2), bottom=round(float(tp[0]),2),
                last=round(float(c[-1]),2), vs=vs, m1=round((np.max(pp)-np.min(pp))/res*100,2),
                m2=round(rise*100,2), m3=len(pks), m4=len(trs), m5=None)

def det_flag(c, v):
    n = len(c)
    if n < 10: return None
    best = None
    for pl in range(4,min(25,n-3)+1):
        for fl in range(3,min(20,n-pl)+1):
            tot=pl+fl
            if tot>n: break
            pole=c[n-tot:n-fl]; flag=c[n-fl:]
            if pole[0]<=0: continue
            pg=(pole[-1]-pole[0])/pole[0]
            if not (0.08<=pg<=1.5): continue
            try:
                x=np.arange(pl); cf=np.polyfit(x,pole,1)
                ssr=np.sum((pole-np.polyval(cf,x))**2); sst=np.sum((pole-np.mean(pole))**2)
                r2=1-ssr/sst if sst>0 else 0
            except: continue
            if cf[0]<=0 or r2<0.55: continue
            up=np.sum(np.diff(pole)>0)/(pl-1) if pl>1 else 0
            if up<0.55: continue
            fhi,flo=np.max(flag),np.min(flag)
            fd=(pole[-1]-flo)/pole[-1] if pole[-1]>0 else 1
            if fd>0.25: continue
            ph=pole[-1]-pole[0]; fr=(fhi-flo)/ph if ph>0 else 1
            if fr>0.70: continue
            q=pg*r2*up-fd-fr*0.5
            if best is None or q>best["q"]:
                best=dict(q=q,fhi=fhi,flo=flo,pg=pg,r2=r2,fd=fd,ps=c[n-tot],pt=pole[-1],pl=pl,fl=fl)
    if best is None: return None
    if v is not None and n>=best["fl"]+1:
        fv=np.mean(v[n-best["fl"]:-1]) if best["fl"]>1 else np.mean(v[-best["fl"]:])
        vs=round(float(v[-1]/fv),2) if fv>0 else None
    else: vs=None
    bo=c[-1]>=best["fhi"]*0.995 and (vs is not None and vs>=1.2)
    pname="HighTightFlag" if best["pg"]>=1.0 else "BullFlag"
    return dict(pattern=pname, status="Breakout Ready" if bo else "Flag Forming",
                quality=round(best["q"],3), bz=round(float(best["fhi"]),2),
                bottom=round(float(best["flo"]),2), last=round(float(c[-1]),2), vs=vs,
                m1=round(best["pg"]*100,2), m2=round(best["r2"],3), m3=round(best["fd"]*100,2),
                m4=best["pl"], m5=best["fl"])

def det_fwedge(c, v):
    n = len(c)
    if n<25: return None
    atr=np.mean(np.abs(np.diff(c))) if n>1 else np.mean(c)*0.015
    prom=max(atr*1.2,np.mean(c)*0.008)
    try:
        highs,_ = find_peaks(c, prominence=prom, distance=4)
        lows,_  = find_peaks(-c, prominence=prom, distance=4)
    except: return None
    if len(highs)<2 or len(lows)<2: return None
    try:
        h_sl=np.polyfit(highs.astype(float),c[highs],1)[0]
        l_sl=np.polyfit(lows.astype(float),c[lows],1)[0]
    except: return None
    if h_sl>=0 or l_sl>=h_sl or abs(l_sl)<=abs(h_sl): return None
    upper=np.polyval(np.polyfit(highs.astype(float),c[highs],1),n-1)
    vs=vsurge(v,n); bo=c[-1]>=upper*0.99 and (vs is not None and vs>=1.2)
    return dict(pattern="FallingWedge", status="Breakout Ready" if bo else "Forming",
                quality=round(abs(h_sl),4), bz=round(float(upper),2),
                bottom=round(float(np.min(c[lows])),2), last=round(float(c[-1]),2), vs=vs,
                m1=round(h_sl,4), m2=round(l_sl,4), m3=len(highs), m4=len(lows), m5=None)

def det_momburst(c, v):
    """Stockbee/Bonde exact scan: c/c1>1.04 AND v>v1 AND liquid.
    Plus TI65 uptrend + 2LYNCH narrow-day pre-burst."""
    n = len(c)
    if n < 30 or v is None: return None
    # Bonde primary: today gain > 4%
    if c[-2] <= 0: return None
    day_gain = (c[-1] - c[-2]) / c[-2]
    if day_gain < 0.04: return None
    # Bonde: today volume > yesterday volume
    if v[-1] <= v[-2]: return None
    # Liquidity gate: avg 20d turnover > 1 Cr INR
    if n >= 20:
        avg_to = np.mean(v[-20:]) * np.mean(c[-20:]) / 1e7
        if avg_to < 1.0: return None
    # Must be in uptrend (above 50-MA)
    ma50 = np.mean(c[-min(50, n):])
    if c[-1] < ma50: return None
    # TI65: avg 7d / avg 65d > 1.0 (Stockbee trend intensity)
    ti65 = None
    if n >= 65:
        ti65 = np.mean(c[-7:]) / np.mean(c[-65:]) if np.mean(c[-65:]) > 0 else 0
        if ti65 < 1.0: return None
    # 2LYNCH N: pre-burst day should be narrow or negative
    n_flag = 0
    if n >= 3 and c[-3] > 0:
        prev_range = abs(c[-2] - c[-3]) / c[-3]
        n_flag = 1 if prev_range < 0.02 else 0
    # Vol surge vs yesterday (quality metric)
    vs_yest = round(float(v[-1] / v[-2]), 2) if v[-2] > 0 else 1.0
    vs_20d = vsurge(v, n)
    quality = round(day_gain * vs_yest, 4)
    return dict(
        pattern="MomBurst", status="Burst Active",
        quality=quality, bz=round(float(c[-1]), 2),
        bottom=round(float(c[-2]), 2), last=round(float(c[-1]), 2),
        vs=vs_20d,
        m1=round(day_gain * 100, 2),   # today gain %
        m2=round(vs_yest, 2),           # vol vs yesterday
        m3=round(ti65, 3) if ti65 else None,  # TI65
        m4=float(n_flag),               # narrow pre-burst day
        m5=round((c[-1]-c[-5])/c[-5]*100, 2) if n >= 5 and c[-5] > 0 else None,
    )

def det_epivot(c, v, o=None):
    n = len(c)
    if n<22: return None
    gap=((o[-1]-c[-2])/c[-2]) if o is not None and len(o)==n else ((c[-1]-c[-2])/c[-2])
    if gap<0.05: return None
    vs=vsurge(v,n)
    if vs is None or vs<3.0: return None
    if c[-1]<np.mean(c[-min(200,n):]): return None
    return dict(pattern="EpisodicPivot", status="Breakout Ready",
                quality=round(gap,3), bz=round(float(c[-1]),2), bottom=round(float(c[-2]),2),
                last=round(float(c[-1]),2), vs=vs, m1=round(gap*100,2), m2=vs, m3=None, m4=None, m5=None)

def det_ppivot(c, v):
    """
    Pocket Pivot (Morales/Kacher) — tightened for Indian markets:
    - Up 1%+ today
    - Today vol > max down-day vol of last 10 sessions (by 1.3x)
    - TI65 uptrend
    - Volume above 50d average
    """
    n = len(c)
    if n < 15 or v is None or len(v) < 15: return None
    day_gain = (c[-1] - c[-2]) / c[-2] if c[-2] > 0 else 0
    if day_gain < 0.01: return None                 # must be up 1%+ today
    max_dv = 0.0
    for i in range(2, min(12, n)):
        if c[-i] < c[-i-1]: max_dv = max(max_dv, v[-i])
    if max_dv == 0 or v[-1] <= max_dv * 1.3: return None   # 1.3x threshold
    if c[-1] < np.mean(c[-min(50,n):]): return None
    ti = calc_ti65(c)
    if ti is not None and ti < 1.01: return None
    vol_ma50 = np.mean(v[-min(50,n):])
    if v[-1] < vol_ma50: return None
    vs = round(float(v[-1] / max_dv), 2)
    return dict(pattern="PocketPivot", status="Pocket Pivot",
                quality=round(vs * day_gain * 10, 3),
                bz=round(float(c[-2]), 2), bottom=round(float(c[-2]), 2),
                last=round(float(c[-1]), 2), vs=vs,
                m1=round(day_gain * 100, 2), m2=vs,
                m3=round(ti, 4) if ti else None,
                m4=round(v[-1] / vol_ma50, 2), m5=None)

def det_anticipation(c, v):
    n = len(c)
    if n<30: return None
    ma50=np.mean(c[-min(50,n):])
    if c[-1]<ma50: return None
    ra=np.mean(np.abs(np.diff(c[-10:]))/c[-10:][:-1]) if n>=10 else 1
    aa=np.mean(np.abs(np.diff(c[-50:]))/c[-50:][:-1]) if n>=50 else ra
    if ra>aa*0.7: return None
    ema20=pd.Series(c).ewm(span=20).mean().values[-1]
    if abs(c[-1]-ema20)/ema20>0.03: return None
    if n>=20 and np.std(c[-20:])/np.mean(c[-20:])>0.03: return None
    return dict(pattern="Anticipation", status="Setup Ready",
                quality=round(1-ra/aa if aa>0 else 0,3), bz=round(float(np.max(c[-10:])),2),
                bottom=round(float(np.min(c[-10:])),2), last=round(float(c[-1]),2), vs=vsurge(v,n),
                m1=round(ra*100,4), m2=round(np.std(c[-20:])/np.mean(c[-20:])*100,2) if n>=20 else None,
                m3=round(abs(c[-1]-ema20)/ema20*100,2), m4=None, m5=None)

def det_stage2bo(c, v):
    n = len(c)
    if n<170: return None
    ma150=np.mean(c[-150:]); ma150_prev=np.mean(c[-170:-20])
    if c[-1]<ma150: return None
    recently_below=any(c[i]<np.mean(c[max(0,i-150):i]) for i in range(n-10,n-1))
    if not recently_below or ma150<ma150_prev*0.98: return None
    vs=vsurge(v,n); bo=vs is not None and vs>=1.3
    return dict(pattern="Stage2Breakout", status="Breakout Ready" if bo else "Forming",
                quality=round((c[-1]-ma150)/ma150,3), bz=round(float(ma150),2),
                bottom=round(float(np.min(c[-30:])),2), last=round(float(c[-1]),2), vs=vs,
                m1=round((c[-1]/ma150-1)*100,2), m2=None, m3=None, m4=None, m5=None)

DETECTORS = {
    "CupHandle": (det_cup, [60,80,120,180,250]),
    "VCP": (det_vcp, [60,80,120,180,250]),
    "FlatBase": (det_fb, [40,60,80,120,180]),
    "InvHS": (det_ihs, [60,80,120,180,250]),
    "DoubleBottom": (det_dbot, [40,60,100,150,200]),
    "AscTriangle": (det_asctri, [30,50,80,120,180]),
    "BullFlag": (det_flag, [15,20,30,40,50,60]),
    "FallingWedge": (det_fwedge, [30,50,80,120]),
    "MomBurst": (det_momburst, [30,40,50]),
    "EpisodicPivot": (det_epivot, [30]),
    "PocketPivot": (det_ppivot, [30]),
    "Anticipation": (det_anticipation, [30,50]),
    "Stage2Breakout": (det_stage2bo, [180]),
}

# ================================================================
# SCAN ONE STOCK
# ================================================================
def scan_stock(sym, nifty_d, ftd_active, market_trend,
               period=PERIOD_DAILY, detector_filter=None):
    fund = dl_fund(sym)
    fund_ok = fund.get("_fund_ok", False)
    cc, cr = cap_class(fund.get("marketCap"))
    rows = []; patterns_found = set()

    df = dl(sym, "1d", period)
    if df is None or len(df) < 30: return rows, fund_ok

    close = df["Close"].values.astype(float)
    vol = df["Volume"].values.astype(float) if "Volume" in df.columns else None
    open_p = df["Open"].values.astype(float) if "Open" in df.columns else None

    if nifty_d is not None and len(nifty_d) > 0:
        nc = nifty_d.reindex(df.index, method="ffill")["Close"].values
        nr = np.full(len(nc), np.nan)
        for i in range(252, len(nc)):
            if nc[i-252] > 0: nr[i] = nc[i]/nc[i-252]-1
    else:
        nc = nr = None

    cs, completeness = canslim_score(close, vol, fund, nc, nr)
    stage = check_weinstein_stage(close)
    vdu = check_volume_dryup(vol)
    earnings_near = check_earnings_near(fund)
    adr = calc_adr(close)

    dets = {k:v for k,v in DETECTORS.items()
            if detector_filter is None or k in detector_filter}

    for pat, (detector, windows) in dets.items():
        best = None
        for w in windows:
            if len(close) < w: continue
            seg_c = close[-w:]; seg_v = vol[-w:] if vol is not None else None
            try:
                if pat == "EpisodicPivot":
                    seg_o = open_p[-w:] if open_p is not None else None
                    res = detector(seg_c, seg_v, o=seg_o)
                else:
                    res = detector(seg_c, seg_v)
            except Exception: continue
            if res is None: continue
            if best is None or res["quality"] > best["quality"]:
                best = {**res, "_w": w}
        if best is None: continue

        mkt_up = ftd_active or "Bull" in str(market_trend) or "Uptrend" in str(market_trend)
        rec = recommend(best["status"], cs, mkt_up)
        if rec == "AVOID": continue

        patterns_found.add(pat)
        stop, t1, t2, t3, rr = calc_targets(best["pattern"], best["bz"],
                                              best.get("bottom"), best["last"], adr)
        leg = identify_leg(close, best["bz"])
        notes = " | ".join(filter(None, [
            "EARNINGS SOON" if earnings_near else None,
            "VOL DRY-UP" if vdu else None,
            "STAGE2" if "Stage2" in stage else None,
            f"ADR={adr}%" if adr >= 3.5 else None,
        ]))
        rows.append(dict(
            scan_date=str(_today()), scan_time=_ist("%H:%M"),
            scan_mode="daily", stock=sym.replace(".NS",""), name=fund.get("longName"),
            sector=fund.get("sector"), cap_class=cc, cap_cr=cr,
            pattern=best["pattern"], timeframe="Daily", status=best["status"],
            breakout_zone=best["bz"], cmp=best["last"], stop_loss=stop,
            target_1=t1, target_2=t2, target_3=t3, risk_reward=rr,
            quality=best["quality"], vol_surge=best.get("vs"),
            canslim_score=cs, data_completeness=completeness, converging=None, leg=leg,
            earnings_near=1 if earnings_near else 0, ftd_active=1 if ftd_active else 0,
            vol_dryup=1 if vdu else 0, stage=stage, recommendation=rec,
            m1=best.get("m1"), m2=best.get("m2"), m3=best.get("m3"),
            m4=best.get("m4"), m5=best.get("m5"), notes=notes or None))

    if len(patterns_found) > 1:
        conv = "+".join(sorted(patterns_found))
        for r in rows: r["converging"] = conv
    return rows, fund_ok

# ================================================================
# TELEGRAM — text + CSV file attachment
# ================================================================
def send_telegram(msg):
    if not TG_TOKEN or not TG_CHAT: return
    try:
        import requests as req
        if len(msg) > 4000: msg = msg[:3990] + "\n..."
        req.post(f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
                 data={"chat_id": TG_CHAT, "text": msg, "parse_mode": "HTML"},
                 timeout=15)
    except Exception as e:
        log.error(f"Telegram msg: {e}")

def send_telegram_file(filepath, caption=""):
    """Send a file (CSV) as a Telegram document attachment."""
    if not TG_TOKEN or not TG_CHAT: return
    if not os.path.exists(filepath):
        log.warning(f"File not found for Telegram: {filepath}")
        return
    try:
        import requests as req
        with open(filepath, "rb") as f:
            resp = req.post(
                f"https://api.telegram.org/bot{TG_TOKEN}/sendDocument",
                data={"chat_id": TG_CHAT, "caption": caption[:1024]},
                files={"document": (os.path.basename(filepath), f, "text/csv")},
                timeout=60,
            )
        if resp.ok:
            log.info(f"CSV sent to Telegram: {os.path.basename(filepath)}")
        else:
            log.error(f"Telegram file failed: {resp.status_code} {resp.text[:200]}")
    except Exception as e:
        log.error(f"Telegram file: {e}")

def fmt_daily(df, market_trend, ftd):
    buys = df[df["recommendation"].str.startswith("BUY", na=False)]
    watch = df[df["recommendation"].str.startswith("WATCH", na=False)]
    ftd_str = "YES \u2705" if ftd else "NO"
    lines = [
        f"<b>\U0001f4ca NSE Scanner \u2014 {_today()} {_ist("%H:%M")}</b>",
        f"Market: {market_trend} | FTD: {ftd_str}",
        f"BUY: {len(buys)} | WATCH: {len(watch)}\n",
    ]
    for _, r in buys.head(15).iterrows():
        em = "\U0001f7e2" if "strong" in str(r["recommendation"]) else "\U0001f7e1"
        conv = f" [{r['converging']}]" if r.get("converging") else ""
        notes = f"\n   \u26a0\ufe0f {r['notes']}" if r.get("notes") else ""
        lines.append(
            f"{em} <b>{r['stock']}</b> ({r.get('cap_class','?')}) — {r['pattern']}{conv}\n"
            f"   CMP \u20b9{r['cmp']} | BZ \u20b9{r['breakout_zone']} | SL \u20b9{r.get('stop_loss','?')}\n"
            f"   T1 \u20b9{r.get('target_1','?')} | T2 \u20b9{r.get('target_2','?')} | "
            f"T3 \u20b9{r.get('target_3','?')} | RR {r.get('risk_reward','?')}x\n"
            f"   CANSLIM {r['canslim_score']}/{r.get('data_completeness','?')} | "
            f"{r.get('leg','?')} | {r.get('stage','?')}{notes}"
        )
    return "\n".join(lines)

def fmt_halfhour(alerts):
    if not alerts: return None
    active = sum(1 for a in alerts if a.get("status") in ("BREAKOUT TRIGGERED","Burst Active"))
    lines = [f"<b>\u26a1 30-min — {_ist()}</b>  {len(alerts)} signals ({active} active)\n"]
    for a in alerts:
        em = ("\U0001f6a8" if "BREAKOUT" in a.get("status","")
              else "\U0001f525" if "Burst Active" == a.get("status","")
              else "\U0001f7e1" if "Pivot" in a.get("status","")
              else "\u26a0\ufe0f")
        vs = f" Vol {a['vs']}x" if a.get("vs") else ""
        sl = f" | SL \u20b9{a['stop']}" if a.get("stop") else ""
        t1 = f" | T1 \u20b9{a['t1']}" if a.get("t1") else ""
        rr = f" | RR {a['rr']}x" if a.get("rr") else ""
        cs = f" | CS {a['canslim']}/7" if a.get("canslim") is not None else ""
        lines.append(
            f"{em} <b>{a['stock']}</b> — {a['pattern']} — {a['status']}\n"
            f"   \u20b9{a['cmp']} | BZ \u20b9{a.get('bz','?')}{vs}{sl}{t1}{rr}{cs}"
        )
    return "\n".join(lines)

# ================================================================
# 30-MINUTE MODE
# ================================================================
def halfhour_check(nifty_d):
    alerts = []
    ftd_active = False; market_trend = "Unknown"
    if nifty_d is not None:
        ftd_active, _ = check_follow_through_day(nifty_d)
        market_trend = check_market_trend(nifty_d["Close"].values)

    # Part A: watchlist check
    watchlist = load_watchlist()
    log.info(f"Watchlist: {len(watchlist)} items")
    for item in watchlist:
        sym = item["stock"] + ".NS"
        df = dl(sym, "1d", "5d")
        if df is None: continue
        close = df["Close"].values.astype(float)
        vol = df["Volume"].values.astype(float) if "Volume" in df.columns else None
        cmp = round(float(close[-1]), 2)
        bz = item.get("breakout_zone"); sl = item.get("stop_loss")
        alert_vs = None; status = "watching"
        if bz and cmp >= bz * 0.995:
            alert_vs = vsurge(vol, len(vol), 10) if vol is not None else None
            status = "BREAKOUT TRIGGERED" if (alert_vs and alert_vs >= 1.3) else "AT BREAKOUT ZONE"
        elif sl and cmp <= sl:
            status = "STOP HIT"
        if status != "watching" and not already_alerted_today(item["stock"], item.get("pattern","")):
            alerts.append({"stock": item["stock"], "pattern": item.get("pattern",""),
                           "status": status, "cmp": cmp, "bz": bz, "vs": alert_vs})
            mark_alert_sent(item["stock"], item.get("pattern",""), status)

    # Part B: quick-scan top stocks for same-day signals (always runs)
    log.info(f"Quick-scan {QUICK_SIZE} stocks for same-day signals...")
    stocks = load_universe()[:QUICK_SIZE]
    quick_rows = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = {ex.submit(scan_stock, s, nifty_d, ftd_active, market_trend,
                          PERIOD_QUICK, INTRADAY_DETECTORS): s for s in stocks}
        for fut in as_completed(futs):
            try:
                rows, _ = fut.result()
                if rows: quick_rows.extend(rows)
            except Exception: pass

    log.info(f"Quick-scan: {len(quick_rows)} signals")

    # Build DataFrame of all signals for CSV
    quick_df = None
    if quick_rows:
        quick_df = (pd.DataFrame(quick_rows)
                    .drop_duplicates(subset=["stock","pattern"])
                    .sort_values("quality", ascending=False)
                    .reset_index(drop=True))
        quick_df["scan_time_ist"] = _ist()

    for sig in quick_rows:
        if not already_alerted_today(sig["stock"], sig["pattern"]):
            alerts.append({
                "stock": sig["stock"], "pattern": sig["pattern"],
                "status": sig["status"], "cmp": sig["cmp"],
                "bz": sig.get("breakout_zone"), "vs": sig.get("vol_surge"),
                "canslim": sig.get("canslim_score"),
                "stop": sig.get("stop_loss"),
                "t1": sig.get("target_1"), "rr": sig.get("risk_reward"),
            })
            mark_alert_sent(sig["stock"], sig["pattern"], sig["status"])

    # Sort: BREAKOUT first, Burst Active, Pocket Pivot, others
    prio = lambda a: (0 if "BREAKOUT" in a.get("status","") else
                      1 if "Burst Active" == a.get("status","") else
                      2 if "Pivot" in a.get("status","") else 3)
    alerts.sort(key=prio)
    return alerts, quick_df

# ================================================================
# DASHBOARD
# ================================================================
def run_dashboard():
    try:
        from flask import Flask, render_template_string
    except ImportError:
        log.error("pip install flask"); sys.exit(1)

    app = Flask(__name__)
    TPL = """<!DOCTYPE html><html><head><title>NSE Scanner</title>
<meta http-equiv="refresh" content="300">
<style>
body{font-family:system-ui;margin:0;padding:20px;background:#0f172a;color:#e2e8f0}
h1{color:#38bdf8}h2{color:#94a3b8;font-size:14px;font-weight:400}
table{border-collapse:collapse;width:100%;margin:16px 0;font-size:12px}
th{background:#1e293b;color:#94a3b8;padding:8px 10px;text-align:left;position:sticky;top:0}
td{padding:5px 10px;border-bottom:1px solid #1e293b}tr:hover{background:#1e293b}
.buy-strong{color:#22c55e;font-weight:600}.buy-mod{color:#eab308}.watch{color:#94a3b8}
.tag{padding:2px 6px;border-radius:4px;font-size:11px;margin:1px}
.tc{background:#312e81;color:#a5b4fc}.te{background:#7f1d1d;color:#fca5a5}
.tv{background:#14532d;color:#86efac}
.stat{display:inline-block;background:#1e293b;padding:10px 20px;border-radius:8px;margin:4px;text-align:center}
.sn{font-size:26px;font-weight:700;color:#38bdf8}.sl{font-size:11px;color:#64748b}
.empty{text-align:center;padding:60px;color:#64748b}
</style></head><body>
<h1>NSE Pattern Scanner v3.3</h1>
<h2>{{ scan_time }} | Market: {{ market }}</h2>
<div>
<div class="stat"><div class="sn">{{ buys }}</div><div class="sl">BUY</div></div>
<div class="stat"><div class="sn">{{ watches }}</div><div class="sl">WATCH</div></div>
<div class="stat"><div class="sn">{{ wl_count }}</div><div class="sl">Watchlist</div></div>
<div class="stat"><div class="sn">{{ total }}</div><div class="sl">Signals today</div></div>
</div>
{% if rows %}
<table><tr><th>Stock</th><th>Cap</th><th>Pattern</th><th>Status</th><th>CMP</th>
<th>Breakout</th><th>Stop</th><th>T1</th><th>T2</th><th>T3</th><th>RR</th>
<th>CANSLIM</th><th>Leg</th><th>Stage</th><th>Reco</th><th>Notes</th></tr>
{% for r in rows %}<tr>
<td><b>{{ r.stock }}</b><br><small style="color:#64748b">{{ r.sector or '' }}</small></td>
<td>{{ r.cap_class }}</td>
<td>{{ r.pattern }}{% if r.converging %}<span class="tag tc">{{ r.converging }}</span>{% endif %}</td>
<td>{{ r.status }}</td><td>{{ r.cmp }}</td><td><b>{{ r.breakout_zone }}</b></td>
<td>{{ r.stop_loss }}</td><td>{{ r.target_1 }}</td><td>{{ r.target_2 }}</td><td>{{ r.target_3 }}</td>
<td>{{ r.risk_reward }}x</td><td>{{ r.canslim_score }}/{{ r.data_completeness }}</td>
<td>{{ r.leg }}</td><td>{{ r.stage }}</td>
<td class="{{ 'buy-strong' if 'strong' in (r.recommendation or '') else 'buy-mod' if 'BUY' in (r.recommendation or '') else 'watch' }}">{{ r.recommendation }}</td>
<td>{% if r.earnings_near %}<span class="tag te">EARN</span>{% endif %}
{% if r.vol_dryup %}<span class="tag tv">VDU</span>{% endif %}{{ r.notes or '' }}</td>
</tr>{% endfor %}</table>
{% else %}<div class="empty">No signals yet. Run <code>python scanner.py --daily --telegram</code></div>{% endif %}
</body></html>"""

    @app.route("/")
    def index():
        con = get_db()
        rows = db_query(con, "SELECT * FROM signals WHERE scan_date=? ORDER BY recommendation, canslim_score DESC",
                        (str(_today()),))
        runs = db_query(con, "SELECT * FROM runs ORDER BY id DESC LIMIT 1")
        wl = load_watchlist()
        con.close()
        buys = sum(1 for r in rows if "BUY" in (r.get("recommendation") or ""))
        watches = sum(1 for r in rows if "WATCH" in (r.get("recommendation") or ""))
        st = rows[0].get("stage","?") if rows else "?"
        rt = runs[0].get("scan_time","never") if runs else "never"
        return render_template_string(TPL, rows=rows, buys=buys, watches=watches,
                                      wl_count=len(wl), total=len(rows),
                                      scan_time=rt, market=st)
    log.info("Dashboard → http://localhost:5050")
    app.run(host="0.0.0.0", port=5050, debug=False)

# ================================================================
# HEALTHCHECK
# ================================================================
def healthcheck():
    print("=== Healthcheck v3.3 ===\n")
    ok = True
    print("[1] yfinance...     ", end="")
    df = dl("RELIANCE.NS", "1d", "1mo")
    print(f"OK ({len(df)} bars)" if df else "FAIL"); ok = ok and bool(df)
    print("[2] Fundamentals... ", end="")
    f = dl_fund("RELIANCE.NS")
    print(f"OK mcap={f.get('marketCap')}" if f.get("marketCap") else "WARN — no mcap")
    print("[3] Universe...     ", end="")
    u = load_universe()
    print(f"OK {len(u)} stocks {'(fallback)' if len(u) == len(NIFTY_500_FALLBACK_NS) else '(live)'}")
    print("[4] Detectors...    ", end="")
    np.random.seed(42); p = 0
    for nm, (det, _) in DETECTORS.items():
        try: det(100+np.random.normal(0,2,100), np.ones(100)*1000); p += 1
        except: pass
    print(f"OK {p}/{len(DETECTORS)}")
    print("[5] Database...     ", end="")
    try: con = get_db(); con.close(); print("OK")
    except Exception as e: print(f"FAIL {e}"); ok = False
    print("[6] Telegram...     ", end="")
    print(f"OK chat={TG_CHAT}" if TG_TOKEN and TG_CHAT else "NOT configured")
    print("[7] Watchlist...    ", end="")
    wl = load_watchlist()
    print(f"OK {len(wl)} items in {WL_PATH}" if wl else f"Empty ({WL_PATH})")
    print("[8] Flask...        ", end="")
    try: import flask; print("OK")
    except: print("NOT installed (optional)")
    print(f"\n{'ALL OK — deploy' if ok else 'FIX ISSUES FIRST'}")
    return ok

# ================================================================
# MAIN
# ================================================================
def main():
    ap = argparse.ArgumentParser(description="NSE Scanner v3.3")
    mode = ap.add_mutually_exclusive_group(required=True)
    mode.add_argument("--daily", action="store_true", help="Full scan (8AM, 12:30PM, 4:30PM)")
    mode.add_argument("--halfhour", action="store_true", help="30-min watchlist+quick scan")
    mode.add_argument("--dashboard", action="store_true", help="Flask web UI")
    mode.add_argument("--healthcheck", action="store_true")
    mode.add_argument("--test", action="store_true", help="10 stocks test")
    ap.add_argument("--telegram", action="store_true")
    args = ap.parse_args()

    if args.healthcheck: sys.exit(0 if healthcheck() else 1)
    if args.dashboard: run_dashboard(); return

    # Fetch Nifty
    nifty_d = None
    for sym in ["^NSEI", "NIFTY_IND_NS"]:
        nifty_d = dl(sym, "1d")
        if nifty_d is not None: break
    if nifty_d is None:
        log.warning("Nifty fetch failed — continuing without market signals")

    # ---- 30-MINUTE MODE ----
    if args.halfhour:
        t0 = time.time()
        log.info(f"=== 30-min scan {_ist()} ===")
        alerts, quick_df = halfhour_check(nifty_d)
        log.info(f"{len(alerts)} alerts | {time.time()-t0:.0f}s")
        if alerts:
            if args.telegram:
                msg = fmt_halfhour(alerts[:20])
                if msg: send_telegram(msg)
                # Send CSV of all quick-scan signals
                if quick_df is not None and len(quick_df):
                    csv_path = os.path.join(OUTPUT_DIR,
                        f"halfhour_{_today()}_{_ist('%H%M')}.csv")
                    quick_df.to_csv(csv_path, index=False)
                    n_active = sum(1 for a in alerts
                                   if a.get("status") in ("BREAKOUT TRIGGERED","Burst Active"))
                    cap = (f"30-min {_ist()} | {len(quick_df)} signals | "
                           f"{n_active} active | Market: {market_trend if nifty_d is not None else '?'}")
                    send_telegram_file(csv_path, cap)
        else:
            log.info("No new signals this round")
        return

    # ---- DAILY / TEST MODE ----
    con = get_db()
    t0 = time.time()
    scan_label = "TEST" if args.test else "DAILY"
    log.info(f"=== {scan_label} {_today()} {_ist("%H:%M")} ===")

    stocks = load_universe()
    if args.test:
        stocks = ["RELIANCE.NS","TCS.NS","INFY.NS","HDFCBANK.NS","ADANIENT.NS",
                  "TATAMOTORS.NS","BAJFINANCE.NS","ICICIBANK.NS","SBIN.NS","LT.NS"]

    log.info(f"{len(stocks)} stocks to scan")

    ftd_active = False; market_trend = "Unknown"
    if nifty_d is not None:
        ftd_active, ftd_note = check_follow_through_day(nifty_d)
        market_trend = check_market_trend(nifty_d["Close"].values)
        log.info(f"Market: {market_trend} | FTD: {ftd_active} ({ftd_note})")

    all_rows = []; ok_count = 0; fund_fails = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = {ex.submit(scan_stock, s, nifty_d, ftd_active, market_trend): s for s in stocks}
        for i, fut in enumerate(as_completed(futs)):
            if (i+1) % 200 == 0:
                log.info(f"  {i+1}/{len(stocks)} | signals: {len(all_rows)}")
            try:
                rows, fund_ok = fut.result()
                ok_count += 1
                if not fund_ok: fund_fails += 1
                if rows: all_rows.extend(rows)
            except Exception: pass

    elapsed = time.time() - t0
    log.info(f"Done: {elapsed/60:.1f}m | {ok_count}/{len(stocks)} OK | "
             f"fund_miss={fund_fails} | {len(all_rows)} signals")

    if not all_rows:
        log.info("No signals."); con.close(); return

    raw = (pd.DataFrame(all_rows)
           .drop_duplicates(subset=["stock","pattern","timeframe"])
           .reset_index(drop=True))

    def _score(r):
        cs  = (r.get("canslim_score") or 0)
        q   = (r.get("quality") or 0) * 100
        vs  = min(r.get("vol_surge") or 1, 5) / 5
        rr  = min(r.get("risk_reward") or 0, 5) / 5
        rw  = {"BUY — strong":3,"BUY — moderate":2,
               "WATCH — await breakout":1.5,"WATCH — mixed":1}.get(r.get("recommendation",""),0)
        ti  = 0.3 if (r.get("m3") or 0) >= 1.05 else 0   # TI65 bonus
        nb  = 0.2 if (r.get("m4") or 0) >= 1.0 else 0    # narrow-day bonus
        cb  = 0.5 if r.get("converging") else 0            # convergence bonus
        s2  = 0.3 if "Stage2" in str(r.get("stage","")) else 0
        vd  = 0.2 if r.get("vol_dryup") else 0
        return cs*0.30 + q*0.20 + rw*0.25 + vs*0.10 + rr*0.05 + ti+nb+cb+s2+vd

    raw["_score"] = raw.apply(_score, axis=1)
    df = raw.sort_values("_score", ascending=False).drop(columns=["_score"]).reset_index(drop=True)

    # Save to DB
    db_execmany(con, """INSERT INTO signals
        (scan_date,scan_time,scan_mode,stock,name,sector,cap_class,cap_cr,
         pattern,timeframe,status,breakout_zone,cmp,stop_loss,
         target_1,target_2,target_3,risk_reward,quality,vol_surge,
         canslim_score,data_completeness,converging,leg,
         earnings_near,ftd_active,vol_dryup,stage,recommendation,m1,m2,m3,m4,m5,notes)
        VALUES (:scan_date,:scan_time,:scan_mode,:stock,:name,:sector,:cap_class,:cap_cr,
         :pattern,:timeframe,:status,:breakout_zone,:cmp,:stop_loss,
         :target_1,:target_2,:target_3,:risk_reward,:quality,:vol_surge,
         :canslim_score,:data_completeness,:converging,:leg,
         :earnings_near,:ftd_active,:vol_dryup,:stage,:recommendation,:m1,:m2,:m3,:m4,:m5,:notes)""",
                df.to_dict("records"))

    # Update watchlist JSON (persisted via git commit in workflow)
    wl_items = []
    for _, r in df.iterrows():
        wl_items.append({
            "stock": r["stock"], "name": r.get("name"), "sector": r.get("sector"),
            "cap_class": r.get("cap_class"), "pattern": r["pattern"],
            "breakout_zone": r.get("breakout_zone"), "stop_loss": r.get("stop_loss"),
            "status": r.get("status"), "added_date": str(_today()),
        })
    # Merge with existing (keep entries from last 30 days, dedup by stock+pattern)
    existing_wl = {f"{w['stock']}_{w['pattern']}": w for w in load_watchlist()}
    for item in wl_items:
        existing_wl[f"{item['stock']}_{item['pattern']}"] = item
    save_watchlist(list(existing_wl.values()))

    buys = df[df["recommendation"].str.startswith("BUY", na=False)]
    watches = df[df["recommendation"].str.startswith("WATCH", na=False)]
    db_exec(con, """INSERT INTO runs
        (scan_date,scan_time,mode,stocks_total,stocks_ok,signals,buys,elapsed_sec)
        VALUES (?,?,?,?,?,?,?,?)""",
            (str(_today()), _ist("%H:%M"), scan_label,
             len(stocks), ok_count, len(df), len(buys), round(elapsed,1)))

    # Save CSV
    csv_path = os.path.join(OUTPUT_DIR, f"scan_{_today()}_{datetime.now().strftime('%H%M')}.csv")
    df.to_csv(csv_path, index=False)
    log.info(f"CSV saved → {csv_path}")

    log.info(f"\nBUY: {len(buys)} | WATCH: {len(watches)}")
    log.info(f"\n{df['pattern'].value_counts().to_string()}")

    conv = df[df["converging"].notna()]
    if len(conv):
        log.info("Convergence:")
        for s in conv["stock"].unique():
            log.info(f"  {s}: {conv[conv['stock']==s]['converging'].iloc[0]}")

    if len(buys):
        print("\n--- TOP BUYS ---")
        cols = ["stock","cap_class","pattern","status","cmp","breakout_zone","stop_loss",
                "target_1","target_2","target_3","risk_reward","canslim_score","leg","stage",
                "recommendation","notes"]
        print(buys[[c for c in cols if c in buys.columns]].head(20).to_string(index=False))

    if args.telegram:
        # Send text alert
        send_telegram(fmt_daily(df, market_trend, ftd_active))
        # Send full CSV as file
        caption = (f"NSE Scanner {_today()} {_ist("%H:%M")} | "
                   f"BUY: {len(buys)} | WATCH: {len(watches)} | "
                   f"Total: {len(df)} signals | Market: {market_trend}")
        send_telegram_file(csv_path, caption)

    con.close()

if __name__ == "__main__":
    main()
