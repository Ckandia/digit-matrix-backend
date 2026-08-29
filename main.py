import os
import asyncio
import asyncpg
from datetime import datetime
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="Digit Matrix API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DATABASE_URL = os.getenv("DATABASE_URL")

async def init_db():
    conn = await asyncpg.connect(DATABASE_URL)
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS ticks (
            id SERIAL PRIMARY KEY,
            market TEXT NOT NULL,
            price NUMERIC,
            digit INTEGER,
            tick_time TIMESTAMP DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_ticks_market_time 
            ON ticks(market, tick_time DESC);
    """)
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS trade_logs (
            id SERIAL PRIMARY KEY,
            loginid TEXT,
            market TEXT,
            strategy TEXT,
            contract_type TEXT,
            stake NUMERIC,
            prediction INTEGER,
            profit NUMERIC,
            result TEXT,
            created_at TIMESTAMP DEFAULT NOW()
        );
    """)
    await conn.close()

async def cleanup_old_ticks():
    while True:
        await asyncio.sleep(3600)
        try:
            conn = await asyncpg.connect(DATABASE_URL)
            await conn.execute("DELETE FROM ticks WHERE tick_time < NOW() - INTERVAL '7 days'")
            await conn.close()
        except Exception:
            pass

@app.on_event("startup")
async def startup():
    await init_db()
    asyncio.create_task(cleanup_old_ticks())

@app.get("/")
async def root():
    return {"status": "Digit Matrix API is running"}

# Receive ticks from frontend
@app.post("/api/ticks")
async def receive_tick(tick: dict):
    try:
        conn = await asyncpg.connect(DATABASE_URL)
        price = float(tick.get("quote", 0))
        digit = int(str(price).replace('.', '')[-1]) if price else 0
        symbol = tick.get("symbol", "unknown")
        await conn.execute(
            "INSERT INTO ticks (market, price, digit) VALUES ($1, $2, $3)",
            symbol, price, digit
        )
        await conn.close()
        return {"status": "received", "market": symbol, "digit": digit}
    except Exception as e:
        return {"error": str(e)}

# Get recent ticks (for signal computation)
@app.get("/api/ticks/{market}")
async def get_recent_ticks(market: str, limit: int = 20):
    conn = await asyncpg.connect(DATABASE_URL)
    rows = await conn.fetch(
        "SELECT price, digit, tick_time FROM ticks WHERE market = $1 ORDER BY tick_time DESC LIMIT $2",
        market, limit
    )
    await conn.close()
    return [
        {"quote": float(r["price"]), "digit": r["digit"], "time": r["tick_time"].isoformat()}
        for r in reversed(rows)
    ]

# 1000-tick deep analysis
@app.get("/api/analysis/{market}")
async def get_analysis(market: str, lookback: int = 1000):
    conn = await asyncpg.connect(DATABASE_URL)
    rows = await conn.fetch(
        "SELECT digit FROM ticks WHERE market = $1 ORDER BY tick_time DESC LIMIT $2",
        market, lookback
    )
    await conn.close()
    
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

# DEBUG: verify ticks are arriving
@app.get("/api/debug/count")
async def tick_count():
    conn = await asyncpg.connect(DATABASE_URL)
    row = await conn.fetchrow("SELECT COUNT(*) as c FROM ticks")
    by_market = await conn.fetch("SELECT market, COUNT(*) as c FROM ticks GROUP BY market")
    await conn.close()
    return {
        "total_ticks": row["c"],
        "by_market": {r["market"]: r["c"] for r in by_market}
    }

@app.post("/api/trades")
async def log_trade(trade: dict):
    conn = await asyncpg.connect(DATABASE_URL)
    await conn.execute(
        """INSERT INTO trade_logs 
           (loginid, market, strategy, contract_type, stake, prediction, profit, result)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8)""",
        trade.get("loginid"), trade.get("market"), trade.get("strategy"),
        trade.get("contract_type"), trade.get("stake"), trade.get("prediction"),
        trade.get("profit"), trade.get("result")
    )
    await conn.close()
    return {"status": "logged"}

@app.get("/api/trades/{loginid}")
async def get_trades(loginid: str, limit: int = 50):
    conn = await asyncpg.connect(DATABASE_URL)
    rows = await conn.fetch(
        "SELECT * FROM trade_logs WHERE loginid = $1 ORDER BY created_at DESC LIMIT $2",
        loginid, limit
    )
    await conn.close()
    return [dict(r) for r in rows]

@app.get("/api/stats/session/{loginid}")
async def session_stats(loginid: str):
    conn = await asyncpg.connect(DATABASE_URL)
    row = await conn.fetchrow(
        """SELECT 
            COUNT(*) as total_trades,
            SUM(CASE WHEN profit > 0 THEN 1 ELSE 0 END) as wins,
            SUM(profit) as net_pnl
           FROM trade_logs WHERE loginid = $1""",
        loginid
    )
    await conn.close()
    total = row["total_trades"] or 0
    wins = row["wins"] or 0
    return {
        "total_trades": total,
        "wins": wins,
        "losses": total - wins,
        "win_rate": round(wins/total*100, 1) if total else 0,
        "net_pnl": float(row["net_pnl"] or 0),
    }
