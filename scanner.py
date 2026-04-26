#!/usr/bin/env python3
"""
NSE Live Pattern Scanner v3.1 — Production Grade
=================================================
13 Detectors + 5 Confirmation Signals | Hourly + Daily modes
Targets, Legs, Earnings, Follow-Through Day, Dashboard

Fixes in v3.1:
  - dl_fund() now retries up to DL_RETRIES times (was single-attempt, silently failed)
  - MAX_WORKERS reduced to 4 (was 8) — prevents Yahoo Finance rate limiting
  - DL_BACKOFF increased to 3.0s (was 2.0s) — more breathing room between retries
  - scan_stock() returns (rows, fund_ok) tuple for accurate reporting
  - Daily scan logs fund_missing count so you know how many stocks had incomplete data
  - Progress log now shows running signal count every 200 stocks

Architecture:
  DAILY (4:30 PM IST)  → Full scan of all NSE stocks → builds watchlist
  HOURLY (market hours) → Checks watchlist only → triggers & alerts

Usage:
  python scanner.py --daily              # full EOD scan
  python scanner.py --hourly             # watchlist check during market
  python scanner.py --dashboard          # launch web dashboard
  python scanner.py --healthcheck        # verify all components
  python scanner.py --test               # 10 stocks, quick check

Deploy: GitHub Actions (daily) + PythonAnywhere (hourly + dashboard)
"""

import os, sys, json, time, sqlite3, argparse, logging, traceback, math
from datetime import date, datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from functools import lru_cache

import yfinance as yf
import pandas as pd
import numpy as np
from scipy.signal import find_peaks

# ================================================================
# LOGGING
# ================================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(BASE_DIR, "logs")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
for d in [LOG_DIR, OUTPUT_DIR]:
    os.makedirs(d, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-5s %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(os.path.join(LOG_DIR, f"scan_{date.today()}.log"), encoding="utf-8"),
    ],
)
log = logging.getLogger("scanner")

# ================================================================
# CONFIG
# ================================================================
DB_PATH      = os.path.join(BASE_DIR, "signals.db")
NIFTY_SYM    = "^NSEI"
PERIOD_DAILY = "1y"
PERIOD_HOURLY = "5d"
MAX_WORKERS  = 4        # reduced from 8 — prevents Yahoo Finance rate limiting
DL_RETRIES   = 4        # increased from 3
DL_BACKOFF   = 4.0      # increased from 3.0 — better handling of rate limits

TG_TOKEN = os.environ.get("TG_BOT_TOKEN", "")
TG_CHAT  = os.environ.get("TG_CHAT_ID", "")

# Market hours IST
MARKET_OPEN_H, MARKET_OPEN_M = 9, 15
MARKET_CLOSE_H, MARKET_CLOSE_M = 15, 30

# CANSLIM
CS = {
    "C_min": 0.25, "A_min": 0.25, "N_max_from_high": 0.15,
    "L_min_rs": 1.10, "I_min_instl": 0.20,
    "buy_strong": 6, "buy_moderate": 4,
}

# ================================================================
# DATABASE
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
            risk_reward REAL,
            quality REAL, vol_surge REAL,
            canslim_score INTEGER, data_completeness INTEGER,
            converging TEXT,
            leg TEXT,
            earnings_near INTEGER,
            ftd_active INTEGER,
            vol_dryup INTEGER,
            stage TEXT,
            recommendation TEXT,
            m1 REAL, m2 REAL, m3 REAL, m4 REAL, m5 REAL,
            notes TEXT,
            created_at TEXT DEFAULT (datetime('now','localtime'))
        );
        CREATE TABLE IF NOT EXISTS watchlist (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            stock TEXT UNIQUE, name TEXT, sector TEXT,
            cap_class TEXT, pattern TEXT,
            breakout_zone REAL, stop_loss REAL,
            status TEXT, added_date TEXT,
            last_checked TEXT, active INTEGER DEFAULT 1
        );
        CREATE TABLE IF NOT EXISTS runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            scan_date TEXT, scan_time TEXT, mode TEXT,
            stocks_total INTEGER, stocks_ok INTEGER,
            signals INTEGER, buys INTEGER,
            elapsed_sec REAL,
            created_at TEXT DEFAULT (datetime('now','localtime'))
        );
        CREATE INDEX IF NOT EXISTS idx_sig_date ON signals(scan_date);
        CREATE INDEX IF NOT EXISTS idx_sig_stock ON signals(stock);
        CREATE INDEX IF NOT EXISTS idx_wl_stock ON watchlist(stock);
    """)
    con.commit()
    return con

def db_exec(con, sql, params=None):
    with _db_lock:
        if params:
            con.execute(sql, params)
        else:
            con.execute(sql)
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
# DATA
# ================================================================
def load_universe():
    url = "https://archives.nseindia.com/content/equities/EQUITY_L.csv"
    for attempt in range(DL_RETRIES):
        try:
            import requests
            from io import StringIO
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
            df = pd.read_csv(StringIO(resp.text)).dropna(subset=["SYMBOL"])
            for col in [" SERIES", "SERIES"]:
                if col in df.columns:
                    df = df[df[col].str.strip() == "EQ"]; break
            return [s.strip() + ".NS" for s in df["SYMBOL"].astype(str).tolist()]
        except Exception as e:
            log.warning(f"Universe attempt {attempt+1}: {e}")
            time.sleep(DL_BACKOFF * (attempt + 1))
    raise RuntimeError("Cannot load NSE stock list")

def dl(sym, interval="1d", period=PERIOD_DAILY):
    """Download price data with retry logic and better error handling."""
    for attempt in range(DL_RETRIES):
        try:
            df = yf.download(sym, period=period, interval=interval,
                             auto_adjust=True, progress=False)
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            df = df.dropna()
            return df if len(df) > 20 else None
        except Exception as e:
            log.debug(f"dl({sym}) attempt {attempt+1}: {type(e).__name__}: {e}")
            if attempt < DL_RETRIES - 1:
                wait = DL_BACKOFF * (attempt + 1)
                log.debug(f"Retrying in {wait}s...")
                time.sleep(wait)
    log.warning(f"Failed to download {sym} after {DL_RETRIES} attempts")
    return None

def dl_fund(sym):
    """Download fundamentals with retry logic.
    Returns dict with _fund_ok=True if marketCap was successfully fetched,
    _fund_ok=False if all attempts returned empty data (rate limited or delisted).
    All callers should check _fund_ok to gauge data quality.
    """
    for attempt in range(DL_RETRIES):
        try:
            tk = yf.Ticker(sym)
            info = tk.info or {}
            # If marketCap is missing Yahoo likely rate-limited us — retry
            if not info.get("marketCap"):
                if attempt < DL_RETRIES - 1:
                    time.sleep(DL_BACKOFF * (attempt + 1))
                    continue
                # Final attempt also empty — return stub so scan still runs
                return {"_fund_ok": False}
            # Earnings date
            try:
                cal = tk.calendar
                next_earnings = cal.get("Earnings Date", [None])[0] if cal else None
            except Exception:
                next_earnings = None
            return {
                "_fund_ok": True,
                "marketCap":                info.get("marketCap"),
                "earningsQuarterlyGrowth":  info.get("earningsQuarterlyGrowth"),
                "earningsGrowth":           info.get("earningsGrowth"),
                "heldPercentInstitutions":  info.get("heldPercentInstitutions"),
                "sector":                   info.get("sector"),
                "longName":                 info.get("longName") or info.get("shortName"),
                "next_earnings":            str(next_earnings) if next_earnings else None,
            }
        except Exception:
            if attempt < DL_RETRIES - 1:
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
# MARKET-LEVEL SIGNALS
# ================================================================
def check_follow_through_day(nifty_df):
    """O'Neil's Follow-Through Day: after a correction, day 4+ of rally
    with 1.5%+ gain on higher volume than prior day."""
    if nifty_df is None or len(nifty_df) < 30:
        return False, "no data"
    c = nifty_df["Close"].values
    v = nifty_df["Volume"].values
    # find the most recent low point (local minimum in last 30 bars)
    recent = c[-30:]
    low_idx = len(c) - 30 + int(np.argmin(recent))
    # count consecutive up days from the low
    rally_days = 0
    for i in range(low_idx + 1, len(c)):
        if c[i] > c[i-1]: rally_days += 1
        else: rally_days = 0
    # FTD conditions
    today_gain = (c[-1] - c[-2]) / c[-2] if c[-2] > 0 else 0
    vol_up = v[-1] > v[-2] if len(v) >= 2 else False
    ftd = rally_days >= 4 and today_gain >= 0.015 and vol_up
    # Also check if Nifty is above 200 DMA
    above_200 = c[-1] > np.mean(c[-min(200, len(c)):]) if len(c) >= 50 else False
    return ftd or above_200, f"rally_days={rally_days} gain={today_gain:.2%}"

def check_market_trend(nifty_close):
    """Weinstein Stage 2 check for market."""
    if len(nifty_close) < 200: return "Unknown"
    ma50 = np.mean(nifty_close[-50:])
    ma200 = np.mean(nifty_close[-200:])
    if nifty_close[-1] > ma50 > ma200: return "Stage2-Bull"
    if nifty_close[-1] > ma200: return "Uptrend"
    if nifty_close[-1] < ma50 < ma200: return "Stage4-Bear"
    return "Choppy"

# ================================================================
# STOCK-LEVEL SIGNALS
# ================================================================
def check_volume_dryup(vol, lookback=25):
    """Morales/Kacher: is today's volume the lowest in 25 sessions?"""
    if vol is None or len(vol) < lookback: return False
    return vol[-1] <= np.min(vol[-lookback:]) * 1.05

def check_weinstein_stage(close, period=150):
    """Simplified Weinstein stage using 30-week (150-day) MA."""
    if len(close) < period + 20: return "Unknown"
    ma = np.mean(close[-period:])
    ma_prev = np.mean(close[-period-20:-20])
    if close[-1] > ma and ma > ma_prev: return "Stage2"
    if close[-1] > ma and ma <= ma_prev: return "Stage1-Late"
    if close[-1] < ma and ma < ma_prev: return "Stage4"
    return "Stage3"

def check_earnings_near(fund, days=14):
    """Is next earnings within N days?"""
    ne = fund.get("next_earnings")
    if not ne: return False
    try:
        ed = datetime.strptime(ne[:10], "%Y-%m-%d").date()
        return 0 <= (ed - date.today()).days <= days
    except Exception:
        return False

def calc_adr(close, period=20):
    """Average Daily Range as % (Qullamaggie uses this for stock selection)."""
    if len(close) < period + 1: return 0
    ranges = []
    for i in range(-period, 0):
        if close[i-1] > 0:
            ranges.append(abs(close[i] - close[i-1]) / close[i-1])
    return round(np.mean(ranges) * 100, 2) if ranges else 0

# ================================================================
# CANSLIM
# ================================================================
def canslim_score(close, vol, fund, nifty_close, nifty_ret):
    n = len(close)
    idx = n - 1
    score, checks = 0, 0
    # N
    lb = min(252, idx)
    hi = np.max(close[max(0,idx-lb):idx+1])
    if hi > 0:
        checks += 1
        if (hi - close[idx]) / hi <= CS["N_max_from_high"]: score += 1
    # L
    if idx >= 252 and idx < len(nifty_ret) and not np.isnan(nifty_ret[idx]):
        checks += 1
        sr = close[idx] / close[idx-252] - 1
        nr = nifty_ret[idx]
        if (1+nr) > 0 and (1+sr)/(1+nr) >= CS["L_min_rs"]: score += 1
    # M
    if len(nifty_close) >= 200:
        checks += 1
        if nifty_close[-1] > np.mean(nifty_close[-200:]): score += 1
    # S
    if vol is not None and idx >= 20:
        checks += 1
        to = np.mean(vol[idx-19:idx+1]) * np.mean(close[idx-19:idx+1]) / 1e7
        if to >= 1.0: score += 1
    # C, A, I
    for key, thresh in [("earningsQuarterlyGrowth", CS["C_min"]),
                         ("earningsGrowth", CS["A_min"]),
                         ("heldPercentInstitutions", CS["I_min_instl"])]:
        v = fund.get(key)
        if v is not None:
            checks += 1
            if v >= thresh: score += 1
    return score, checks

def recommend(status, score, mkt_up):
    bo = any(k in status for k in ["Breakout", "Burst", "Pivot", "Pocket"])
    if bo and score >= CS["buy_strong"] and mkt_up:  return "BUY — strong"
    if bo and score >= CS["buy_moderate"]:            return "BUY — moderate"
    if not bo and score >= CS["buy_strong"]:          return "WATCH — await breakout"
    if score >= CS["buy_moderate"]:                   return "WATCH — mixed"
    return "AVOID"

# ================================================================
# TARGET & STOP CALCULATION
# ================================================================
def calc_targets(pattern, breakout_zone, bottom, cmp, adr_pct):
    """Pattern-specific target calculation using proven methods."""
    if not breakout_zone or breakout_zone <= 0: return None, None, None, None, None

    # Pattern height (the measured move projection)
    height = breakout_zone - bottom if bottom and bottom > 0 else breakout_zone * 0.10

    # Stop loss: below the pattern's low (with buffer)
    if bottom and bottom > 0:
        stop = round(bottom * 0.97, 2)  # 3% below pattern low
    else:
        stop = round(breakout_zone * 0.92, 2)  # 8% below breakout

    if pattern in ("MomBurst", "EpisodicPivot", "PocketPivot"):
        # Short-term: tighter targets
        t1 = round(cmp * 1.05, 2)        # 5%
        t2 = round(cmp * 1.10, 2)        # 10%
        t3 = round(cmp * 1.15, 2)        # 15%
        stop = round(cmp * 0.96, 2)      # 4% stop
    elif pattern == "BullFlag":
        # Target = flag high + pole height
        t1 = round(breakout_zone + height * 0.5, 2)
        t2 = round(breakout_zone + height, 2)
        t3 = round(breakout_zone + height * 1.5, 2)
    else:
        # Base patterns: measured move
        t1 = round(breakout_zone + height * 0.5, 2)    # 50% of pattern height
        t2 = round(breakout_zone + height, 2)           # 100% (classic target)
        t3 = round(breakout_zone + height * 1.618, 2)   # Fibonacci extension

    # Risk/reward
    risk = cmp - stop if stop < cmp else cmp * 0.05
    reward = t2 - cmp if t2 > cmp else cmp * 0.10
    rr = round(reward / risk, 2) if risk > 0 else 0

    return stop, t1, t2, t3, rr

def identify_leg(close, breakout_zone):
    """Which leg of the move are we in?"""
    if len(close) < 50: return "Unknown"
    # Has the stock broken out?
    if close[-1] < breakout_zone * 0.98:
        return "Pre-breakout"
    # How far above breakout?
    gain = (close[-1] - breakout_zone) / breakout_zone
    if gain < 0.05:
        return "Leg1-Early"
    elif gain < 0.15:
        return "Leg1-Trending"
    elif gain < 0.30:
        return "Leg2-Extended"
    else:
        return "Leg3-Climax"

# ================================================================
# VOLUME HELPERS
# ================================================================
def vsurge(vol, n, lookback=20):
    if vol is None or n < lookback: return None
    avg = np.mean(vol[-lookback:])
    return round(float(vol[-1] / avg), 2) if avg > 0 else None

# ================================================================
# ALL DETECTORS (kept compact — same logic as v2, with fixes)
# ================================================================

def det_cup(c, v):
    n = len(c)
    if n < 50: return None
    s = pd.Series(c).rolling(5, min_periods=1).mean().values
    ti = int(np.argmin(s))
    if not (n*0.20 <= ti <= n*0.80): return None
    lm, rm = np.max(s[:ti+1]), np.max(s[ti:])
    pk, tr = max(lm, rm), s[ti]
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
    cx = np.arange(rpi+1)
    try:
        cf = np.polyfit(cx, s[:rpi+1], 2)
        ssr = np.sum((s[:rpi+1]-np.polyval(cf,cx))**2)
        sst = np.sum((s[:rpi+1]-np.mean(s[:rpi+1]))**2)
        r2 = 1-ssr/sst if sst > 0 else 0
        if cf[0] <= 0 or r2 < 0.50: return None
    except: return None
    vs = vsurge(v, n)
    bo = c[-1] >= pk*0.97 and (vs is not None and vs >= 1.2)
    return dict(pattern="CupHandle", status="Breakout Ready" if bo else "Forming",
                quality=round(r2-sym,3), bz=round(float(pk),2),
                bottom=round(float(tr),2), last=round(float(c[-1]),2), vs=vs,
                m1=round(d*100,2), m2=round(sym*100,2), m3=round(hd*100,2),
                m4=round(r,2), m5=round(r2,3))

def det_vcp(c, v):
    n = len(c)
    if n < 40: return None
    atr = np.mean(np.abs(np.diff(c))) if n > 1 else np.mean(c)*0.02
    prom = max(atr*1.5, np.mean(c)*0.01)
    try:
        highs, _ = find_peaks(c, prominence=prom, distance=5)
        lows, _ = find_peaks(-c, prominence=prom, distance=5)
    except: return None
    if len(highs) < 2 or len(lows) < 2: return None
    contractions = []
    high_list = list(highs) + [n]
    for i, hi in enumerate(high_list[:-1]):
        nh = high_list[i+1]
        nl = lows[(lows > hi) & (lows < nh)]
        if len(nl) == 0:
            stretch = c[hi:nh]
            if len(stretch) < 3: continue
            lo = hi + int(np.argmin(stretch))
        else: lo = nl[0]
        if lo >= n: continue
        depth = (c[hi]-c[lo])/c[hi]
        if depth < 0.03: continue
        contractions.append((hi, lo, depth))
    if len(contractions) < 3: return None
    depths = [ct[2] for ct in contractions]
    if not all(depths[i] <= depths[i-1]*0.85 for i in range(1,len(depths))): return None
    if contractions[-1][1] < n*0.5: return None
    pivot = float(np.max(c[highs]))
    vs = vsurge(v, n)
    bo = c[-1] >= pivot*0.98 and (vs is not None and vs >= 1.5)
    return dict(pattern="VCP", status="Breakout Ready" if bo else "Forming",
                quality=round(1-depths[-1],3), bz=round(pivot,2),
                bottom=round(float(c[contractions[-1][1]]),2),
                last=round(float(c[-1]),2), vs=vs,
                m1=round(depths[0]*100,2), m2=round(depths[-1]*100,2),
                m3=round(depths[-1]/depths[0],2) if depths[0]>0 else None,
                m4=len(contractions), m5=None)

def det_fb(c, v):
    n = len(c)
    if n < 35: return None
    best = None
    for bl in range(15, min(75,n)+1):
        base = c[-bl:]
        bh, blo = np.max(base), np.min(base)
        br = (bh-blo)/bh if bh > 0 else 1
        if br > 0.20: break
        bs = n-bl; tl = min(80, bs)
        if tl < 15: continue
        pre = c[bs-tl:bs]
        tg = (pre[-1]-np.min(pre))/np.min(pre) if np.min(pre) > 0 else 0
        if tg < 0.10: continue
        if best is None or br < best["br"]:
            best = dict(bl=bl, bh=bh, blo=blo, br=br, tg=tg)
    if best is None: return None
    vs = vsurge(v, n)
    bo = c[-1] >= best["bh"]*0.99 and (vs is not None and vs >= 1.2)
    return dict(pattern="FlatBase", status="Breakout Ready" if bo else "Forming",
                quality=round(best["tg"]-best["br"],3), bz=round(float(best["bh"]),2),
                bottom=round(float(best["blo"]),2), last=round(float(c[-1]),2), vs=vs,
                m1=round(best["br"]*100,2), m2=round(best["tg"]*100,2),
                m3=best["bl"], m4=None, m5=None)

def det_ihs(c, v):
    n = len(c)
    if n < 40: return None
    atr = np.mean(np.abs(np.diff(c))) if n > 1 else np.mean(c)*0.015
    prom = max(atr*1.2, np.mean(c)*0.008)
    try: troughs, _ = find_peaks(-c, prominence=prom, distance=6)
    except: return None
    if len(troughs) < 3: return None
    hc = troughs[(troughs > n*0.20) & (troughs < n*0.80)]
    if len(hc) == 0: return None
    hi = hc[np.argmin(c[hc])]
    hl = [t for t in troughs if t < hi and c[t] > c[hi]]
    hr = [t for t in troughs if t > hi and c[t] > c[hi]]
    if not hl or not hr: return None
    li, ri = hl[-1], hr[0]
    ls, hd, rs_ = c[li], c[hi], c[ri]
    sa = (ls+rs_)/2; asym = abs(ls-rs_)/sa
    if asym > 0.18: return None
    hb = (sa-hd)/sa
    if not (0.03 <= hb <= 0.50): return None
    nl = (np.max(c[li:hi+1]) + np.max(c[hi:ri+1]))/2
    if ri >= n-2: return None
    vs = vsurge(v, n)
    bo = c[-1] >= nl*0.99 and (vs is not None and vs >= 1.2)
    return dict(pattern="InvHS", status="Breakout Ready" if bo else "Forming",
                quality=round(hb-asym,3), bz=round(float(nl),2),
                bottom=round(float(hd),2), last=round(float(c[-1]),2), vs=vs,
                m1=round(hb*100,2), m2=round(asym*100,2), m3=int(ri-li),
                m4=None, m5=None)

def det_dbot(c, v):
    n = len(c)
    if n < 30: return None
    try: troughs, _ = find_peaks(-c, prominence=0.02*np.mean(c), distance=5)
    except: return None
    if len(troughs) < 2: return None
    best = None
    for i in range(len(troughs)):
        for j in range(i+1, len(troughs)):
            sep = troughs[j]-troughs[i]
            if not (10<=sep<=150): continue
            p1, p2 = c[troughs[i]], c[troughs[j]]
            diff = abs(p1-p2)/min(p1,p2)
            if diff > 0.08: continue
            mid = np.max(c[troughs[i]:troughs[j]+1])
            mr = (mid-(p1+p2)/2)/((p1+p2)/2)
            if mr < 0.06 or troughs[j] >= n-2: continue
            if best is None or mr-diff > best["sc"]:
                best = dict(sc=mr-diff, mid=mid, diff=diff, mr=mr,
                            bottom=min(p1,p2))
    if best is None: return None
    vs = vsurge(v, n)
    bo = c[-1] >= best["mid"]*0.99 and (vs is not None and vs >= 1.2)
    return dict(pattern="DoubleBottom", status="Breakout Ready" if bo else "Forming",
                quality=round(best["sc"],3), bz=round(float(best["mid"]),2),
                bottom=round(float(best["bottom"]),2),
                last=round(float(c[-1]),2), vs=vs,
                m1=round(best["diff"]*100,2), m2=round(best["mr"]*100,2),
                m3=None, m4=None, m5=None)

def det_asctri(c, v):
    n = len(c)
    if not (15 <= n <= 200): return None
    prom = 0.01*np.mean(c)
    try:
        pks, _ = find_peaks(c, prominence=prom, distance=3)
        trs, _ = find_peaks(-c, prominence=prom, distance=3)
    except: return None
    if len(pks) < 2 or len(trs) < 2: return None
    pp = c[pks]; res = np.median(pp)
    sp = (np.max(pp)-np.min(pp))/res if res > 0 else 1
    if sp > 0.04: return None
    tp = c[trs]
    if len(trs) >= 2:
        slopes = [(tp[j]-tp[i])/(trs[j]-trs[i]) for i in range(len(trs)) for j in range(i+1,len(trs)) if trs[j]!=trs[i]]
        slope = np.median(slopes) if slopes else 0
    else: slope = 0
    rise = (tp[-1]-tp[0])/tp[0] if tp[0] > 0 else 0
    if slope <= 0 or rise < 0.015 or trs[-1] < n*0.4: return None
    vs = vsurge(v, n)
    bo = c[-1] >= res*0.99 and (vs is not None and vs >= 1.2)
    return dict(pattern="AscTriangle", status="Breakout Ready" if bo else "Forming",
                quality=round(rise-sp,3), bz=round(float(res),2),
                bottom=round(float(tp[0]),2), last=round(float(c[-1]),2), vs=vs,
                m1=round(sp*100,2), m2=round(rise*100,2), m3=len(pks),
                m4=len(trs), m5=None)

def det_flag(c, v):
    n = len(c)
    if n < 10: return None
    best = None
    for pl in range(4, min(25,n-3)+1):
        for fl in range(3, min(20,n-pl)+1):
            tot = pl+fl
            if tot > n: break
            pole = c[n-tot:n-fl]; flag = c[n-fl:]
            if pole[0] <= 0: continue
            pg = (pole[-1]-pole[0])/pole[0]
            if not (0.08 <= pg <= 1.5): continue
            x = np.arange(pl)
            try:
                cf = np.polyfit(x, pole, 1)
                ssr = np.sum((pole-np.polyval(cf,x))**2)
                sst = np.sum((pole-np.mean(pole))**2)
                r2 = 1-ssr/sst if sst > 0 else 0
            except: continue
            if cf[0] <= 0 or r2 < 0.55: continue
            up = np.sum(np.diff(pole)>0)/(pl-1) if pl > 1 else 0
            if up < 0.55: continue
            fhi, flo = np.max(flag), np.min(flag)
            fd = (pole[-1]-flo)/pole[-1] if pole[-1] > 0 else 1
            if fd > 0.25: continue
            ph = pole[-1]-pole[0]
            fr = (fhi-flo)/ph if ph > 0 else 1
            if fr > 0.70: continue
            q = pg*r2*up-fd-fr*0.5
            if best is None or q > best["q"]:
                best = dict(q=q, fhi=fhi, flo=flo, pg=pg, r2=r2,
                            fd=fd, ps=c[n-tot], pt=pole[-1], pl=pl, fl=fl)
    if best is None: return None
    # vol vs flag
    if v is not None and n >= best["fl"]+1:
        fv = np.mean(v[n-best["fl"]:-1]) if best["fl"] > 1 else np.mean(v[-best["fl"]:])
        vs = round(float(v[-1]/fv),2) if fv > 0 else None
    else: vs = None
    bo = c[-1] >= best["fhi"]*0.995 and (vs is not None and vs >= 1.2)
    htf = best["pg"] >= 1.0  # High Tight Flag variant
    pname = "HighTightFlag" if htf else "BullFlag"
    return dict(pattern=pname, status="Breakout Ready" if bo else "Flag Forming",
                quality=round(best["q"],3), bz=round(float(best["fhi"]),2),
                bottom=round(float(best["flo"]),2), last=round(float(c[-1]),2), vs=vs,
                m1=round(best["pg"]*100,2), m2=round(best["r2"],3),
                m3=round(best["fd"]*100,2), m4=best["pl"], m5=best["fl"])

def det_fwedge(c, v):
    n = len(c)
    if n < 25: return None
    atr = np.mean(np.abs(np.diff(c))) if n > 1 else np.mean(c)*0.015
    prom = max(atr*1.2, np.mean(c)*0.008)
    try:
        highs, _ = find_peaks(c, prominence=prom, distance=4)
        lows, _ = find_peaks(-c, prominence=prom, distance=4)
    except: return None
    if len(highs) < 2 or len(lows) < 2: return None
    try:
        h_sl = np.polyfit(highs.astype(float), c[highs], 1)[0]
        l_sl = np.polyfit(lows.astype(float), c[lows], 1)[0]
    except: return None
    if h_sl >= 0 or l_sl >= h_sl or abs(l_sl) <= abs(h_sl): return None
    upper = np.polyval(np.polyfit(highs.astype(float), c[highs], 1), n-1)
    vs = vsurge(v, n)
    bo = c[-1] >= upper*0.99 and (vs is not None and vs >= 1.2)
    return dict(pattern="FallingWedge", status="Breakout Ready" if bo else "Forming",
                quality=round(abs(h_sl),4), bz=round(float(upper),2),
                bottom=round(float(np.min(c[lows])),2),
                last=round(float(c[-1]),2), vs=vs,
                m1=round(h_sl,4), m2=round(l_sl,4), m3=len(highs),
                m4=len(lows), m5=None)

def det_momburst(c, v):
    n = len(c)
    if n < 30: return None
    for lb in [5, 7, 10]:
        if n < lb+15: continue
        ret = (c[-1]-c[-lb])/c[-lb]
        if ret < 0.08: continue
        ps, pe = max(0, n-lb-20), n-lb
        if pe-ps < 10: continue
        pre_c = c[ps:pe]
        pre_atr = np.mean(np.abs(np.diff(pre_c))/pre_c[:-1])
        all_atr = np.mean(np.abs(np.diff(c[:pe]))/c[:pe][:-1]) if pe > 2 else pre_atr
        if pre_atr > all_atr*1.1: continue
        ma50 = np.mean(c[-min(50,n):])
        if c[-1] < ma50: continue
        vs = vsurge(v, n)
        vol_ok = vs is not None and vs >= 1.2
        return dict(pattern="MomBurst", status="Burst Active" if vol_ok else "Burst (low vol)",
                    quality=round(ret,3), bz=round(float(c[-1]),2),
                    bottom=round(float(c[-lb]),2), last=round(float(c[-1]),2), vs=vs,
                    m1=round(ret*100,2), m2=round(pre_atr*100,4), m3=lb,
                    m4=None, m5=None)
    return None

def det_epivot(c, v, o=None):
    n = len(c)
    if n < 22: return None
    gap = ((o[-1]-c[-2])/c[-2]) if o is not None and len(o)==n else ((c[-1]-c[-2])/c[-2])
    if gap < 0.05: return None
    vs = vsurge(v, n)
    if vs is None or vs < 3.0: return None
    if c[-1] < np.mean(c[-min(200,n):]): return None
    return dict(pattern="EpisodicPivot", status="Breakout Ready",
                quality=round(gap,3), bz=round(float(c[-1]),2),
                bottom=round(float(c[-2]),2), last=round(float(c[-1]),2), vs=vs,
                m1=round(gap*100,2), m2=vs, m3=None, m4=None, m5=None)

def det_ppivot(c, v):
    n = len(c)
    if n < 12 or v is None: return None
    if c[-1] <= c[-2]: return None
    max_dv = 0.0
    for i in range(2, min(12, n)):
        if i < n and c[-i] < c[-i-1]: max_dv = max(max_dv, v[-i])
    if max_dv == 0 or v[-1] <= max_dv: return None
    if c[-1] < np.mean(c[-min(50,n):]): return None
    vs = round(float(v[-1]/max_dv),2)
    return dict(pattern="PocketPivot", status="Pocket Pivot",
                quality=round(vs,2), bz=round(float(c[-2]),2),
                bottom=round(float(c[-2]),2), last=round(float(c[-1]),2), vs=vs,
                m1=round((c[-1]-c[-2])/c[-2]*100,2), m2=vs,
                m3=None, m4=None, m5=None)

def det_anticipation(c, v):
    """Bonde's Anticipation Setup: low ATR + uptrend + near support."""
    n = len(c)
    if n < 30: return None
    # must be in uptrend
    ma50 = np.mean(c[-min(50,n):])
    if c[-1] < ma50: return None
    # recent 5-10 days must have very low volatility
    recent_atr = np.mean(np.abs(np.diff(c[-10:]))/c[-10:][:-1]) if n >= 10 else 1
    avg_atr = np.mean(np.abs(np.diff(c[-50:]))/c[-50:][:-1]) if n >= 50 else recent_atr
    if recent_atr > avg_atr * 0.7: return None  # must be quieter than average
    # price should be near support (within 3% of 20 EMA)
    ema20 = pd.Series(c).ewm(span=20).mean().values[-1]
    dist = abs(c[-1] - ema20) / ema20
    if dist > 0.03: return None  # must be hugging the MA
    # Bollinger squeeze check
    bb_width = np.std(c[-20:]) / np.mean(c[-20:]) if n >= 20 else 1
    if bb_width > 0.03: return None  # bands must be tight
    return dict(pattern="Anticipation", status="Setup Ready",
                quality=round(1-recent_atr/avg_atr if avg_atr > 0 else 0, 3),
                bz=round(float(np.max(c[-10:])),2),
                bottom=round(float(np.min(c[-10:])),2),
                last=round(float(c[-1]),2), vs=vsurge(v,n),
                m1=round(recent_atr*100,4), m2=round(bb_width*100,2),
                m3=round(dist*100,2), m4=None, m5=None)

def det_stage2bo(c, v):
    """Weinstein Stage 2 Breakout: price crosses above 150-day MA from below."""
    n = len(c)
    if n < 170: return None
    ma150 = np.mean(c[-150:])
    ma150_prev = np.mean(c[-170:-20])
    # currently above MA
    if c[-1] < ma150: return None
    # recently was below (crossed up in last 10 bars)
    recently_below = any(c[i] < np.mean(c[max(0,i-150):i]) for i in range(n-10, n-1))
    if not recently_below: return None
    # MA should be flattening or turning up
    if ma150 < ma150_prev * 0.98: return None  # still declining
    vs = vsurge(v, n)
    bo = vs is not None and vs >= 1.3
    return dict(pattern="Stage2Breakout", status="Breakout Ready" if bo else "Forming",
                quality=round((c[-1]-ma150)/ma150, 3), bz=round(float(ma150),2),
                bottom=round(float(np.min(c[-30:])),2),
                last=round(float(c[-1]),2), vs=vs,
                m1=round((c[-1]/ma150-1)*100,2), m2=None, m3=None, m4=None, m5=None)

# Detector registry
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
def scan_stock(sym, nifty_d, ftd_active, market_trend):
    fund = dl_fund(sym)
    fund_ok = fund.get("_fund_ok", False)
    cc, cr = cap_class(fund.get("marketCap"))
    rows = []
    patterns_found = set()

    df = dl(sym, "1d")
    if df is None or len(df) < 30: return rows, fund_ok

    close = df["Close"].values.astype(float)
    vol = df["Volume"].values.astype(float) if "Volume" in df.columns else None
    open_p = df["Open"].values.astype(float) if "Open" in df.columns else None

    nc = nifty_d.reindex(df.index, method="ffill")["Close"].values
    nr = np.full(len(nc), np.nan)
    for i in range(252, len(nc)):
        if nc[i-252] > 0: nr[i] = nc[i]/nc[i-252]-1

    cs, completeness = canslim_score(close, vol, fund, nc, nr)
    stage = check_weinstein_stage(close)
    vdu = check_volume_dryup(vol)
    earnings_near = check_earnings_near(fund)
    adr = calc_adr(close)

    for pat_name, (detector, windows) in DETECTORS.items():
        best = None
        for w in windows:
            if len(close) < w: continue
            seg_c = close[-w:]
            seg_v = vol[-w:] if vol is not None else None
            try:
                if pat_name == "EpisodicPivot":
                    seg_o = open_p[-w:] if open_p is not None else None
                    res = detector(seg_c, seg_v, o=seg_o)
                else:
                    res = detector(seg_c, seg_v)
            except Exception:
                continue
            if res is None: continue
            if best is None or res["quality"] > best["quality"]:
                best = {**res, "_w": w}
        if best is None: continue

        mkt_up = ftd_active or "Bull" in market_trend or "Uptrend" in market_trend
        rec = recommend(best["status"], cs, mkt_up)
        if rec == "AVOID": continue

        patterns_found.add(pat_name)
        stop, t1, t2, t3, rr = calc_targets(
            best["pattern"], best["bz"], best.get("bottom"), best["last"], adr
        )
        leg = identify_leg(close, best["bz"])

        notes_parts = []
        if earnings_near: notes_parts.append("EARNINGS SOON")
        if vdu: notes_parts.append("VOL DRY-UP")
        if "Stage2" in stage: notes_parts.append("STAGE2")
        if adr >= 3.5: notes_parts.append(f"ADR={adr}%")

        rows.append(dict(
            scan_date=str(date.today()),
            scan_time=datetime.now().strftime("%H:%M"),
            scan_mode="daily",
            stock=sym.replace(".NS",""), name=fund.get("longName"),
            sector=fund.get("sector"), cap_class=cc, cap_cr=cr,
            pattern=best["pattern"], timeframe="Daily",
            status=best["status"], breakout_zone=best["bz"],
            cmp=best["last"], stop_loss=stop,
            target_1=t1, target_2=t2, target_3=t3, risk_reward=rr,
            quality=best["quality"], vol_surge=best.get("vs"),
            canslim_score=cs, data_completeness=completeness,
            converging=None, leg=leg,
            earnings_near=1 if earnings_near else 0,
            ftd_active=1 if ftd_active else 0,
            vol_dryup=1 if vdu else 0, stage=stage,
            recommendation=rec,
            m1=best.get("m1"), m2=best.get("m2"), m3=best.get("m3"),
            m4=best.get("m4"), m5=best.get("m5"),
            notes=" | ".join(notes_parts) if notes_parts else None,
        ))

    if len(patterns_found) > 1:
        conv = "+".join(sorted(patterns_found))
        for r in rows: r["converging"] = conv
    return rows, fund_ok

# ================================================================
# HOURLY MODE — check watchlist only
# ================================================================
def hourly_check(con, nifty_d):
    """Check active watchlist stocks for breakout triggers."""
    watchlist = db_query(con, "SELECT * FROM watchlist WHERE active=1")
    if not watchlist:
        log.info("Watchlist empty — run --daily first")
        return []

    log.info(f"Hourly check: {len(watchlist)} stocks on watchlist")
    alerts = []

    for item in watchlist:
        sym = item["stock"] + ".NS"
        df = dl(sym, "1h", "5d")
        if df is None: continue

        close = df["Close"].values.astype(float)
        vol = df["Volume"].values.astype(float) if "Volume" in df.columns else None
        cmp = round(float(close[-1]), 2)
        bz = item["breakout_zone"]
        sl = item["stop_loss"]

        status = "watching"
        if bz and cmp >= bz * 0.995:
            vs = vsurge(vol, len(vol), 10) if vol is not None else None
            if vs and vs >= 1.3:
                status = "BREAKOUT TRIGGERED"
            else:
                status = "AT BREAKOUT ZONE"
        elif sl and cmp <= sl:
            status = "STOP HIT"
            db_exec(con, "UPDATE watchlist SET active=0 WHERE stock=?", (item["stock"],))

        if status != "watching":
            alerts.append({
                "stock": item["stock"], "pattern": item["pattern"],
                "status": status, "cmp": cmp, "bz": bz, "vs": vs if 'vs' in dir() else None,
            })
            db_exec(con, "UPDATE watchlist SET last_checked=?, status=? WHERE stock=?",
                    (datetime.now().strftime("%H:%M"), status, item["stock"]))

    return alerts

# ================================================================
# TELEGRAM
# ================================================================
def send_telegram(msg):
    if not TG_TOKEN or not TG_CHAT: return
    try:
        import requests
        if len(msg) > 4000: msg = msg[:3990] + "\n..."
        requests.post(f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
                      data={"chat_id": TG_CHAT, "text": msg, "parse_mode": "HTML"},
                      timeout=15)
    except Exception as e:
        log.error(f"Telegram: {e}")

def format_daily_alert(df, market_trend, ftd):
    buys = df[df["recommendation"].str.startswith("BUY", na=False)]
    watch = df[df["recommendation"].str.startswith("WATCH", na=False)]
    lines = [
        f"<b>\U0001f4ca NSE Scanner — {date.today()}</b>",
        f"Market: {market_trend} | FTD: {'YES' if ftd else 'NO'}",
        f"BUY: {len(buys)} | WATCH: {len(watch)}\n",
    ]
    for _, r in buys.head(15).iterrows():
        em = "\U0001f7e2" if "strong" in str(r["recommendation"]) else "\U0001f7e1"
        conv = f" [{r['converging']}]" if r.get("converging") else ""
        notes = f"\n   {r['notes']}" if r.get("notes") else ""
        lines.append(
            f"{em} <b>{r['stock']}</b> ({r['cap_class']}) — {r['pattern']}{conv}\n"
            f"   CMP \u20b9{r['cmp']} | BZ \u20b9{r['breakout_zone']} | SL \u20b9{r.get('stop_loss','?')}\n"
            f"   T1 \u20b9{r.get('target_1','?')} T2 \u20b9{r.get('target_2','?')} | RR {r.get('risk_reward','?')}x\n"
            f"   CANSLIM {r['canslim_score']}/{r.get('data_completeness','?')} | {r.get('leg','?')} | {r.get('stage','?')}"
            f"{notes}"
        )
    return "\n".join(lines)

def format_hourly_alert(alerts):
    if not alerts: return None
    lines = [f"<b>\u26a1 Hourly Update — {datetime.now().strftime('%H:%M')}</b>\n"]
    for a in alerts:
        em = "\U0001f6a8" if "BREAKOUT" in a["status"] else "\u26a0\ufe0f"
        lines.append(f"{em} <b>{a['stock']}</b> — {a['status']}\n"
                      f"   CMP \u20b9{a['cmp']} | BZ \u20b9{a['bz']}")
    return "\n".join(lines)

# ================================================================
# DASHBOARD (simple Flask)
# ================================================================
def run_dashboard():
    try:
        from flask import Flask, render_template_string
    except ImportError:
        log.error("Flask not installed. Run: pip install flask")
        sys.exit(1)

    app = Flask(__name__)
    TEMPLATE = """<!DOCTYPE html>
<html><head><title>NSE Scanner Dashboard</title>
<meta http-equiv="refresh" content="300">
<style>
body{font-family:system-ui;margin:0;padding:20px;background:#0f172a;color:#e2e8f0}
h1{color:#38bdf8;margin-bottom:4px} h2{color:#94a3b8;font-size:14px;font-weight:400}
table{border-collapse:collapse;width:100%;margin:16px 0;font-size:13px}
th{background:#1e293b;color:#94a3b8;padding:8px 12px;text-align:left;position:sticky;top:0}
td{padding:6px 12px;border-bottom:1px solid #1e293b}
tr:hover{background:#1e293b}
.buy-strong{color:#22c55e;font-weight:600} .buy-mod{color:#eab308}
.watch{color:#94a3b8} .tag{background:#1e293b;padding:2px 8px;border-radius:4px;font-size:11px}
.tag-conv{background:#312e81;color:#a5b4fc}
.tag-earnings{background:#7f1d1d;color:#fca5a5}
.tag-vdu{background:#14532d;color:#86efac}
.stat{display:inline-block;background:#1e293b;padding:12px 24px;border-radius:8px;margin:4px;text-align:center}
.stat-num{font-size:28px;font-weight:700;color:#38bdf8}
.stat-label{font-size:11px;color:#64748b;margin-top:4px}
</style></head><body>
<h1>NSE Pattern Scanner</h1>
<h2>Last updated: {{ last_run }} | Market: {{ market_trend }}</h2>
<div>
<div class="stat"><div class="stat-num">{{ buys }}</div><div class="stat-label">BUY signals</div></div>
<div class="stat"><div class="stat-num">{{ watches }}</div><div class="stat-label">WATCH signals</div></div>
<div class="stat"><div class="stat-num">{{ watchlist_count }}</div><div class="stat-label">On watchlist</div></div>
<div class="stat"><div class="stat-num">{{ total }}</div><div class="stat-label">Total scanned</div></div>
</div>
<h2 style="margin-top:24px;color:#38bdf8;font-size:16px;font-weight:600">Today's Signals</h2>
<table><tr>
<th>Stock</th><th>Cap</th><th>Pattern</th><th>Status</th><th>CMP</th>
<th>Breakout</th><th>Stop</th><th>T1</th><th>T2</th><th>RR</th>
<th>CANSLIM</th><th>Leg</th><th>Stage</th><th>Reco</th><th>Notes</th>
</tr>
{% for r in rows %}
<tr>
<td><b>{{ r.stock }}</b><br><span style="color:#64748b;font-size:11px">{{ r.sector or '' }}</span></td>
<td>{{ r.cap_class }}</td>
<td>{{ r.pattern }}{% if r.converging %} <span class="tag tag-conv">{{ r.converging }}</span>{% endif %}</td>
<td>{{ r.status }}</td>
<td>{{ r.cmp }}</td><td>{{ r.breakout_zone }}</td>
<td>{{ r.stop_loss }}</td><td>{{ r.target_1 }}</td><td>{{ r.target_2 }}</td>
<td>{{ r.risk_reward }}x</td>
<td>{{ r.canslim_score }}/{{ r.data_completeness }}</td>
<td>{{ r.leg }}</td><td>{{ r.stage }}</td>
<td class="{{ 'buy-strong' if 'strong' in (r.recommendation or '') else 'buy-mod' if 'BUY' in (r.recommendation or '') else 'watch' }}">{{ r.recommendation }}</td>
<td>{% if r.earnings_near %}<span class="tag tag-earnings">EARNINGS</span> {% endif %}{% if r.vol_dryup %}<span class="tag tag-vdu">VDU</span> {% endif %}{{ r.notes or '' }}</td>
</tr>
{% endfor %}
</table></body></html>"""

    @app.route("/")
    def index():
        con = get_db()
        rows = db_query(con, """SELECT * FROM signals WHERE scan_date=?
                                ORDER BY recommendation, canslim_score DESC, quality DESC""",
                        (str(date.today()),))
        runs = db_query(con, "SELECT * FROM runs ORDER BY id DESC LIMIT 1")
        wl = db_query(con, "SELECT count(*) as c FROM watchlist WHERE active=1")
        con.close()
        buys = sum(1 for r in rows if "BUY" in (r.get("recommendation") or ""))
        watches = sum(1 for r in rows if "WATCH" in (r.get("recommendation") or ""))
        return render_template_string(TEMPLATE,
            rows=rows, buys=buys, watches=watches, total=len(rows),
            watchlist_count=wl[0]["c"] if wl else 0,
            last_run=runs[0]["scan_time"] if runs else "never",
            market_trend=rows[0].get("stage","?") if rows else "?")

    log.info("Dashboard starting on http://localhost:5050")
    app.run(host="0.0.0.0", port=5050, debug=False)

# ================================================================
# HEALTHCHECK
# ================================================================
def healthcheck():
    print("=== Healthcheck ===\n")
    ok = True
    print("[1] yfinance...", end=" ")
    df = dl("RELIANCE.NS", "1d", "1mo")
    print(f"OK ({len(df)} bars)" if df is not None else "FAIL"); ok = ok and df is not None
    print("[2] Fundamentals...", end=" ")
    f = dl_fund("RELIANCE.NS")
    print(f"OK (mcap={f.get('marketCap')})" if f.get("marketCap") else "WARN")
    print("[3] Universe...", end=" ")
    try: u = load_universe(); print(f"OK ({len(u)} stocks)")
    except Exception as e: print(f"FAIL: {e}"); ok = False
    print("[4] Detectors...", end=" ")
    np.random.seed(42); passed = 0
    for name, (det, _) in DETECTORS.items():
        try: det(100+np.random.normal(0,2,100), np.ones(100)*1000); passed += 1
        except: pass
    print(f"OK ({passed}/{len(DETECTORS)} callable)")
    print("[5] Database...", end=" ")
    try: con = get_db(); con.close(); print("OK")
    except Exception as e: print(f"FAIL: {e}"); ok = False
    print("[6] Telegram...", end=" ")
    print(f"OK (chat={TG_CHAT})" if TG_TOKEN and TG_CHAT else "NOT configured")
    print("[7] Flask...", end=" ")
    try: import flask; print(f"OK (v{flask.__version__})")
    except: print("NOT installed (optional)")
    print(f"\n{'READY TO DEPLOY' if ok else 'FIX ISSUES FIRST'}")
    return ok

# ================================================================
# MAIN
# ================================================================
def main():
    ap = argparse.ArgumentParser(description="NSE 15-Pattern Live Scanner")
    mode = ap.add_mutually_exclusive_group(required=True)
    mode.add_argument("--daily", action="store_true", help="Full EOD scan")
    mode.add_argument("--hourly", action="store_true", help="Watchlist check")
    mode.add_argument("--dashboard", action="store_true", help="Web dashboard")
    mode.add_argument("--healthcheck", action="store_true")
    mode.add_argument("--test", action="store_true", help="5 stocks test")
    ap.add_argument("--telegram", action="store_true")
    args = ap.parse_args()

    if args.healthcheck: sys.exit(0 if healthcheck() else 1)
    if args.dashboard: run_dashboard(); return

    con = get_db()
    t0 = time.time()
    now_str = datetime.now().strftime("%H:%M")

    # ---- HOURLY MODE ----
    if args.hourly:
        log.info(f"=== Hourly check {now_str} ===")
        nifty_d = dl(NIFTY_SYM, "1d")
        alerts = hourly_check(con, nifty_d)
        if alerts:
            log.info(f"{len(alerts)} alerts triggered")
            for a in alerts: log.info(f"  {a['stock']}: {a['status']} @ {a['cmp']}")
            if args.telegram:
                msg = format_hourly_alert(alerts)
                if msg: send_telegram(msg)
        else:
            log.info("No triggers this hour")
        con.close()
        return

    # ---- DAILY / TEST MODE ----
    log.info(f"=== {'TEST' if args.test else 'DAILY'} scan {date.today()} ===")

    stocks = load_universe()
    if args.test:
        stocks = ["RELIANCE.NS","TCS.NS","INFY.NS","HDFCBANK.NS","ADANIENT.NS",
                  "TATAMOTORS.NS","BAJFINANCE.NS","ICICIBANK.NS","SBIN.NS","LT.NS"]

    log.info(f"{len(stocks)} stocks")
    
    # Fetch Nifty with more retries and longer backoff for index
    log.info("Fetching Nifty index...")
    nifty_d = None
    for attempt in range(DL_RETRIES + 2):  # Extra attempts for critical index
        nifty_d = dl(NIFTY_SYM, "1d")
        if nifty_d is not None:
            log.info(f"Nifty fetched successfully (attempt {attempt+1})")
            break
        wait = DL_BACKOFF * (attempt + 2)
        log.warning(f"Nifty fetch failed (attempt {attempt+1}), retrying in {wait}s...")
        time.sleep(wait)
    
    if nifty_d is None:
        log.error("Cannot fetch Nifty after all retries"); sys.exit(1)

    ftd_active, ftd_note = check_follow_through_day(nifty_d)
    market_trend = check_market_trend(nifty_d["Close"].values)
    log.info(f"Market: {market_trend} | FTD: {ftd_active} ({ftd_note})")

    all_rows   = []
    ok_count   = 0
    fund_fails = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = {ex.submit(scan_stock, s, nifty_d, ftd_active, market_trend): s for s in stocks}
        for i, fut in enumerate(as_completed(futs)):
            if (i+1) % 200 == 0:
                log.info(f"  {i+1}/{len(stocks)}... signals so far: {len(all_rows)}")
            try:
                rows, fund_ok = fut.result()
                ok_count += 1
                if not fund_ok:
                    fund_fails += 1
                if rows: all_rows.extend(rows)
            except Exception as e:
                log.debug(f"Err {futs[fut]}: {e}")

    elapsed = time.time() - t0
    log.info(f"Done: {elapsed/60:.1f}m | {ok_count}/{len(stocks)} completed | "
             f"fund_missing={fund_fails} | {len(all_rows)} signals")

    if not all_rows:
        log.info("No signals today.")
        con.close(); return

    df = (pd.DataFrame(all_rows)
          .drop_duplicates(subset=["stock","pattern","timeframe"])
          .sort_values(["recommendation","canslim_score","quality"], ascending=[True,False,False])
          .reset_index(drop=True))

    # Save signals
    db_execmany(con, """INSERT INTO signals
        (scan_date,scan_time,scan_mode,stock,name,sector,cap_class,cap_cr,
         pattern,timeframe,status,breakout_zone,cmp,stop_loss,
         target_1,target_2,target_3,risk_reward,quality,vol_surge,
         canslim_score,data_completeness,converging,leg,
         earnings_near,ftd_active,vol_dryup,stage,recommendation,
         m1,m2,m3,m4,m5,notes)
        VALUES (:scan_date,:scan_time,:scan_mode,:stock,:name,:sector,:cap_class,:cap_cr,
         :pattern,:timeframe,:status,:breakout_zone,:cmp,:stop_loss,
         :target_1,:target_2,:target_3,:risk_reward,:quality,:vol_surge,
         :canslim_score,:data_completeness,:converging,:leg,
         :earnings_near,:ftd_active,:vol_dryup,:stage,:recommendation,
         :m1,:m2,:m3,:m4,:m5,:notes)""", df.to_dict("records"))

    # Update watchlist — add new forming patterns, deactivate old
    db_exec(con, "UPDATE watchlist SET active=0 WHERE added_date < ?",
            (str(date.today() - timedelta(days=30)),))
    for _, r in df.iterrows():
        if "Forming" in str(r.get("status","")) or "WATCH" in str(r.get("recommendation","")):
            try:
                db_exec(con, """INSERT OR REPLACE INTO watchlist
                    (stock,name,sector,cap_class,pattern,breakout_zone,stop_loss,
                     status,added_date,last_checked,active)
                    VALUES (?,?,?,?,?,?,?,?,?,?,1)""",
                    (r["stock"],r.get("name"),r.get("sector"),r.get("cap_class"),
                     r["pattern"],r.get("breakout_zone"),r.get("stop_loss"),
                     r.get("status"),str(date.today()),now_str))
            except Exception: pass

    buys = df[df["recommendation"].str.startswith("BUY", na=False)]
    watches = df[df["recommendation"].str.startswith("WATCH", na=False)]
    db_exec(con, "INSERT INTO runs (scan_date,scan_time,mode,stocks_total,stocks_ok,signals,buys,elapsed_sec) VALUES (?,?,?,?,?,?,?,?)",
            (str(date.today()),now_str,"daily",len(stocks),ok_count,len(df),len(buys),round(elapsed,1)))
    log.info(f"Fund data missing for {fund_fails}/{len(stocks)} stocks "
             f"({'%.1f' % (fund_fails/len(stocks)*100)}%) — "
             f"{'high rate limiting, consider re-running' if fund_fails > len(stocks)*0.3 else 'within normal range'}") 

    # CSV
    csv_path = os.path.join(OUTPUT_DIR, f"scan_{date.today()}.csv")
    df.to_csv(csv_path, index=False)
    log.info(f"Saved → {csv_path}")

    # Summary
    log.info(f"\nBUY: {len(buys)} | WATCH: {len(watches)}")
    log.info(f"\n{df['pattern'].value_counts().to_string()}")
    conv = df[df["converging"].notna()]
    if len(conv):
        log.info(f"\nMulti-pattern convergence:")
        for s in conv["stock"].unique():
            log.info(f"  {s}: {conv[conv['stock']==s]['converging'].iloc[0]}")

    if len(buys):
        print("\n--- TOP BUYS ---")
        cols = ["stock","cap_class","pattern","status","cmp","breakout_zone",
                "stop_loss","target_1","target_2","risk_reward","canslim_score",
                "leg","stage","recommendation","notes"]
        print(buys[[c for c in cols if c in buys.columns]].head(20).to_string(index=False))

    if args.telegram:
        msg = format_daily_alert(df, market_trend, ftd_active)
        send_telegram(msg)

    con.close()

if __name__ == "__main__":
    main()
