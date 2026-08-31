import express from "express";
import cors from "cors";
import WebSocket from "ws";

const app = express();
app.use(cors());
app.use(express.json());

const DERIV_TOKEN = process.env.DERIV_TOKEN || "1C23qDvmR9JjMC3";

const tickStore = {};
const tradeLog = [];

const getLastDigit = (quote) => {
    const str = quote.toString();
    return parseInt(str[str.length - 1], 10);
};

app.get("/", (req, res) => {
    res.json({ status: "ok", endpoints: ["/api/balance", "/api/ticks", "/api/ticks/:symbol", "/api/analysis/:symbol", "/api/trades"] });
});

app.post("/api/ticks", (req, res) => {
    const { symbol, quote } = req.body;
    if (!symbol || quote === undefined) return res.status(400).json({ error: "symbol and quote required" });
    if (!tickStore[symbol]) tickStore[symbol] = [];
    const digit = getLastDigit(quote);
    tickStore[symbol].push({ quote, digit, timestamp: Date.now() });
    if (tickStore[symbol].length > 2000) tickStore[symbol] = tickStore[symbol].slice(-2000);
    res.json({ success: true, count: tickStore[symbol].length });
});

app.get("/api/ticks/:symbol", (req, res) => {
    const { symbol } = req.params;
    const limit = parseInt(req.query.limit, 10) || 20;
    const ticks = tickStore[symbol] || [];
    res.json(ticks.slice(-limit));
});

app.get("/api/analysis/:symbol", (req, res) => {
    const { symbol } = req.params;
    const lookback = parseInt(req.query.lookback, 10) || 1000;
    const ticks = tickStore[symbol] || [];
    const sample = ticks.slice(-lookback);
    if (sample.length === 0) return res.status(404).json({ error: "No tick data yet", lookback: 0 });

    const freq = Array(10).fill(0);
    let evenCount = 0;
    sample.forEach(t => {
        const d = t.digit ?? getLastDigit(t.quote);
        freq[d] = (freq[d] || 0) + 1;
        if (d % 2 === 0) evenCount++;
    });

    let hotDigit = 0, hotPct = 0, coldDigit = 0, coldPct = 100;
    for (let i = 0; i <= 9; i++) {
        const pct = (freq[i] / sample.length) * 100;
        if (pct > hotPct) { hotDigit = i; hotPct = pct; }
        if (pct < coldPct) { coldDigit = i; coldPct = pct; }
    }

    let maxStreak = { digit: null, length: 0 }, current = { digit: null, length: 0 };
    sample.forEach(t => {
        const d = t.digit ?? getLastDigit(t.quote);
        if (d === current.digit) current.length++;
        else {
            if (current.length > maxStreak.length) maxStreak = { ...current };
            current = { digit: d, length: 1 };
        }
    });
    if (current.length > maxStreak.length) maxStreak = { ...current };

    res.json({
        lookback: sample.length,
        hot_digit: hotDigit,
        hot_pct: parseFloat(hotPct.toFixed(1)),
        cold_digit: coldDigit,
        cold_pct: parseFloat(coldPct.toFixed(1)),
        even_odd: { even_pct: parseFloat(((evenCount / sample.length) * 100).toFixed(1)) },
        max_streak: maxStreak,
        last_20_digits: ticks.slice(-20).map(t => t.digit ?? getLastDigit(t.quote)),
    });
});

app.post("/api/trades", (req, res) => {
    const trade = req.body;
    if (!trade || !trade.market) return res.status(400).json({ error: "Invalid trade payload" });
    tradeLog.push({ ...trade, logged_at: new Date().toISOString() });
    console.log("Trade logged:", trade);
    res.json({ success: true, total_trades: tradeLog.length });
});

app.get("/api/balance", async (req, res) => {
    const ws = new WebSocket("wss://ws.binaryws.com/websockets/v3?app_id=1089");
    const timeout = setTimeout(() => { ws.close(); res.status(504).json({ balance: "error" }); }, 10000);
    ws.onopen = () => ws.send(JSON.stringify({ authorize: DERIV_TOKEN }));
    ws.onmessage = (msg) => {
        const data = JSON.parse(msg.toString());
        if (data.msg_type === "authorize") ws.send(JSON.stringify({ balance: 1 }));
        if (data.msg_type === "balance") { clearTimeout(timeout); ws.close(); res.json({ balance: data.balance }); }
    };
    ws.onerror = () => { clearTimeout(timeout); res.status(500).json({ balance: "error" }); };
});

const PORT = process.env.PORT || 10000;
app.listen(PORT, () => console.log(`Server running on port ${PORT}`));
