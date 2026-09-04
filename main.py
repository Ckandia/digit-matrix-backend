import os
import sqlite3
import json
from datetime import datetime
from fastapi import FastAPI, Request, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from starlette.concurrency import run_in_threadpool

from auth import require_api_key
from autotrader import AutoTrader

app = FastAPI(title="Digit Matrix API")

FRONTEND_ORIGIN = "https://digit-matrix-carlos-githaes-projects.vercel.app"

app.add_middleware(
    CORSMiddleware,
    allow_origins=[FRONTEND_ORIGIN],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DB_PATH = os.getenv("DB_PATH", "digit_matrix.db")
DB_DIR = os.path.dirname(DB_PATH)
if DB_DIR and not os.path.exists(DB_DIR):
    os.makedirs(DB_DIR, exist_ok=True)

def get_db_connection():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db_sync():
    conn = get_db_connection()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ticks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            market TEXT NOT NULL,
            price REAL,
            digit INTEGER,
            tick_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_ticks_market_time 
            ON ticks(market, tick_time DESC)
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS trade_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            loginid TEXT,
            market TEXT,
            strategy TEXT,
            contract_type TEXT,
            stake REAL,
            prediction INTEGER,
            profit REAL,
            result TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()

async def _log_trade_to_db(trade: dict):
    def _insert():
        conn = get_db_connection()
        conn.execute(
            """INSERT INTO trade_logs 
               (loginid, market, strategy, contract_type, stake, prediction, profit, result)
               VALUES (?,?,?,?,?,?,?,?)""",
            (
                trade.get("loginid"), trade.get("market"), trade.get("strategy"),
                trade.get("contract_type"), trade.get("stake"), trade.get("prediction"),
                trade.get("profit"), trade.get("result")
            )
        )
        conn.commit()
        conn.close()
    await run_in_threadpool(_insert)

autotrader = AutoTrader(log_trade_fn=_log_trade_to_db)

@app.on_event("startup")
async def startup():
    await run_in_threadpool(init_db_sync)

@app.on_event("shutdown")
async def shutdown():
    await autotrader.stop()

@app.get("/")
async def root():
    return {"status": "Digit Matrix API is running"}

@app.get("/health")
async def health():
    return {"status": "ok"}

@app.post("/api/ticks")
async def receive_tick(tick: dict):
    def _insert():
        conn = get_db_connection()
        price = float(tick.get("quote", 0))
        digit = int(str(price).replace('.', '')[-1]) if price else 0
        symbol = tick.get("symbol", "unknown")
        conn.execute(
            "INSERT INTO ticks (market, price, digit) VALUES (?, ?, ?)",
            (symbol, price, digit)
        )
        conn.commit()
        conn.close()
        return {"status": "received", "market": symbol, "digit": digit}
    return await run_in_threadpool(_insert)

@app.get("/api/ticks/{market}")
async def get_recent_ticks(market: str, limit: int = 20):
    def _fetch():
        conn = get_db_connection()
        cur = conn.execute(
            "SELECT price, digit, tick_time FROM ticks WHERE market = ? ORDER BY tick_time DESC LIMIT ?",
            (market, limit)
        )
        rows = cur.fetchall()
        conn.close()
        return [
            {"quote": float(r["price"]), "digit": r["digit"], "time": r["tick_time"]}
            for r in reversed(rows)
        ]
    return await run_in_threadpool(_fetch)

@app.get("/api/analysis/{market}")
async def get_analysis(market: str, lookback: int = 1000):
    def _analyze():
        conn = get_db_connection()
        cur = conn.execute(
            "SELECT digit FROM ticks WHERE market = ? ORDER BY tick_time DESC LIMIT ?",
            (market, lookback)
        )
        rows = cur.fetchall()
        conn.close()
        
        digits = [r["digit"] for r in rows]
        if not digits:
            return {"error": "No tick data yet. Collecting..."}
        
        total = len(digits)
        freq = {str(i): round(digits.count(i) / total * 100, 1) for i in range(10)}
        even = sum(1 for d in digits if d % 2 == 0)
        hot_digit = max(freq, key=freq.get)
        cold_digit = min(freq, key=freq.get)
        
        max_streak = 1
        current_streak = 1
        streak_digit = digits[0]
        for i in range(1, len(digits)):
            if digits[i] == digits[i-1]:
                current_streak += 1
                if current_streak > max_streak:
                    max_streak = current_streak
                    streak_digit = digits[i]
            else:
                current_streak = 1
        
        return {
            "market": market,
            "lookback": total,
            "frequency_percent": freq,
            "even_odd": {
                "even": even, 
                "odd": total - even, 
                "even_pct": round(even/total*100, 1)
            },
            "hot_digit": hot_digit,
            "cold_digit": cold_digit,
            "hot_pct": freq[hot_digit],
            "cold_pct": freq[cold_digit],
            "max_streak": {"digit": streak_digit, "length": max_streak},
            "last_20_digits": digits[:20],
        }
    return await run_in_threadpool(_analyze)

@app.get("/api/debug/count")
async def tick_count():
    def _count():
        conn = get_db_connection()
        cur = conn.execute("SELECT COUNT(*) as c FROM ticks")
        row = cur.fetchone()
        cur = conn.execute("SELECT market, COUNT(*) as c FROM ticks GROUP BY market")
        by_market = cur.fetchall()
        conn.close()
        return {
            "total_ticks": row["c"] if row else 0,
            "by_market": {r["market"]: r["c"] for r in by_market}
        }
    return await run_in_threadpool(_count)

@app.post("/api/trades")
async def log_trade(trade: dict):
    await _log_trade_to_db(trade)
    return {"status": "logged"}

@app.get("/api/trades/{loginid}")
async def get_trades(loginid: str, limit: int = 50):
    def _fetch():
        conn = get_db_connection()
        cur = conn.execute(
            "SELECT * FROM trade_logs WHERE loginid = ? ORDER BY created_at DESC LIMIT ?",
            (loginid, limit)
        )
        rows = cur.fetchall()
        conn.close()
        return [dict(r) for r in rows]
    return await run_in_threadpool(_fetch)

@app.get("/api/stats/session/{loginid}")
async def session_stats(loginid: str):
    def _stats():
        conn = get_db_connection()
        cur = conn.execute(
            """SELECT 
                COUNT(*) as total_trades,
                SUM(CASE WHEN profit > 0 THEN 1 ELSE 0 END) as wins,
                SUM(profit) as net_pnl
               FROM trade_logs WHERE loginid = ?""",
            (loginid,)
        )
        row = cur.fetchone()
        conn.close()
        total = row["total_trades"] or 0
        wins = row["wins"] or 0
        return {
            "total_trades": total,
            "wins": wins,
            "losses": total - wins,
            "win_rate": round(wins/total*100, 1) if total else 0,
            "net_pnl": float(row["net_pnl"] or 0),
        }
    return await run_in_threadpool(_stats)

@app.post("/api/autotrader/start", dependencies=[Depends(require_api_key)])
async def start_autotrader(body: dict):
    access_token = body.get("access_token")
    loginid = body.get("loginid")
    environment = body.get("environment", "production")

    if not access_token or not loginid:
        raise HTTPException(
            status_code=400,
            detail="access_token and loginid are required"
        )

    await autotrader.start(access_token, loginid, environment)
    return autotrader.status()

@app.post("/api/autotrader/stop", dependencies=[Depends(require_api_key)])
async def stop_autotrader():
    await autotrader.stop()
    return autotrader.status()

@app.post("/api/autotrader/resume", dependencies=[Depends(require_api_key)])
async def resume_autotrader():
    autotrader.resume_after_halt()
    return autotrader.status()

@app.get("/api/autotrader/status", dependencies=[Depends(require_api_key)])
async def autotrader_status():
    return autotrader.status()
