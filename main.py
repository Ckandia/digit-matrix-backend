import os
import json
import asyncio
import asyncpg
import websockets
from datetime import datetime
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="Digit Matrix API")

# Allow your Vercel frontend to talk to this backend
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://digit-matrix-carlos-githaes-projects.vercel.app",
        "http://localhost:4003",
        "http://localhost:3000",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DATABASE_URL = os.getenv("DATABASE_URL")
DERIV_APP_ID = os.getenv("DERIV_APP_ID", "1089")

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

async def deriv_ws_listener():
    uri = f"wss://ws.derivws.com/websockets/v3?app_id={DERIV_APP_ID}"
    markets = ["R_10", "R_25", "R_50", "R_75", "R_100"]
    
    while True:
        try:
            async with websockets.connect(uri) as ws:
                for m in markets:
                    await ws.send(json.dumps({
                        "ticks": m,
                        "subscribe": 1
                    }))
                
                async for message in ws:
                    data = json.loads(message)
                    if data.get("tick"):
                        tick = data["tick"]
                        price = float(tick["quote"])
                        digit = int(str(price).replace('.', '')[-1])
                        market = tick["symbol"]
                        
                        conn = await asyncpg.connect(DATABASE_URL)
                        await conn.execute(
                            "INSERT INTO ticks (market, price, digit) VALUES ($1, $2, $3)",
                            market, price, digit
                        )
                        await conn.close()
        except Exception as e:
            print(f"[WS Error] {e} — reconnecting in 5s...")
            await asyncio.sleep(5)

@app.on_event("startup")
async def startup():
    await init_db()
    asyncio.create_task(deriv_ws_listener())

@app.get("/")
async def root():
    return {"status": "Digit Matrix API is running"}

@app.get("/api/analysis/{market}")
async def get_analysis(market: str, lookback: int = 100):
    conn = await asyncpg.connect(DATABASE_URL)
    rows = await conn.fetch(
        "SELECT digit FROM ticks WHERE market = $1 ORDER BY tick_time DESC LIMIT $2",
        market, lookback
    )
    await conn.close()
    
    digits = [r["digit"] for r in rows]
    if not digits:
        return {"error": "No tick data yet. Collecting..."}
    
    freq = {str(i): round(digits.count(i) / len(digits) * 100, 1) for i in range(10)}
    even = sum(1 for d in digits if d % 2 == 0)
    
    return {
        "market": market,
        "lookback": len(digits),
        "frequency_percent": freq,
        "even_odd": {
            "even": even, 
            "odd": len(digits) - even, 
            "even_pct": round(even/len(digits)*100, 1)
        },
        "hot_digit": max(freq, key=freq.get),
        "cold_digit": min(freq, key=freq.get),
        "last_10_digits": digits[:10],
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
