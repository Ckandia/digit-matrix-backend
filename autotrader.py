import asyncio
import json
import os
import time
import websockets

from safety import SafetyGuard, SafetyConfig

DERIV_WS_URL = "wss://ws.derivws.com/websockets/v3?app_id=1089"  # swap 1089 for your own app_id
DERIV_API_TOKEN = os.getenv("DERIV_API_TOKEN")  # a Deriv API token, scoped to trade — use a DEMO token first

# The watchlist: what to trade, not how to "predict" it. Each entry runs on its
# own schedule. Keep this rule-based and boring on purpose — see signal.ts for
# why treating digit contracts as predictable is a mistake to avoid repeating here.
WATCHLIST = [
    {"market": "R_10", "contract_type": "DIGITEVEN", "stake": 0.5, "duration": 1},
    {"market": "R_25", "contract_type": "DIGITODD", "stake": 0.5, "duration": 1},
]

TRADE_INTERVAL_SECONDS = 30  # how often each watchlist entry gets a chance to fire


class AutoTrader:
    def __init__(self, log_trade_fn):
        """
        log_trade_fn: an async callable(trade: dict) -> None, used to persist
        each result the same way /api/trades already does, so this shows up in
        the same trade_logs table and the existing session-stats endpoint.
        """
        self.guard = SafetyGuard(SafetyConfig())
        self.log_trade_fn = log_trade_fn
        self._task: asyncio.Task | None = None
        self._ws = None
        self._running = False

    @property
    def is_running(self) -> bool:
        return self._running

    def status(self) -> dict:
        return {"running": self._running, **self.guard.status()}

    async def start(self):
        if self._running:
            return
        if not DERIV_API_TOKEN:
            raise RuntimeError("DERIV_API_TOKEN not set")
        self._running = True
        self._task = asyncio.create_task(self._run_loop())

    async def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
        if self._ws:
            await self._ws.close()

    def resume_after_halt(self):
        self.guard.reset_halt()

    async def _run_loop(self):
        async with websockets.connect(DERIV_WS_URL) as ws:
            self._ws = ws
            await ws.send(json.dumps({"authorize": DERIV_API_TOKEN}))
            auth_resp = json.loads(await ws.recv())
            if "error" in auth_resp:
                self._running = False
                raise RuntimeError(f"Deriv auth failed: {auth_resp['error']}")

            last_fired = {entry["market"]: 0.0 for entry in WATCHLIST}

            while self._running:
                now = time.time()
                for entry in WATCHLIST:
                    if now - last_fired[entry["market"]] < TRADE_INTERVAL_SECONDS:
                        continue

                    ok, reason = self.guard.can_trade(entry["market"])
                    if not ok:
                        # Halted or capped — skip silently, status endpoint shows why
                        continue

                    try:
                        profit = await self._place_and_wait(ws, entry)
                        self.guard.record_result(entry["market"], profit)
                        await self.log_trade_fn({
                            "loginid": None,  # fill in from your account context if trading multiple accounts
                            "market": entry["market"],
                            "strategy": "autotrader",
                            "contract_type": entry["contract_type"],
                            "stake": entry["stake"],
                            "prediction": None,
                            "profit": profit,
                            "result": "win" if profit > 0 else "loss",
                        })
                    except Exception as e:
                        # A single failed trade shouldn't kill the whole worker —
                        # log and keep going, but don't silently retry-spam Deriv.
                        print(f"[autotrader] trade error on {entry['market']}: {e}")

                    last_fired[entry["market"]] = now

                if self.guard.status()["halted"]:
                    self._running = False
                    break

                await asyncio.sleep(1)

    async def _place_and_wait(self, ws, entry: dict) -> float:
        """
        Sends a proposal, buys it, then waits for the contract to settle and
        returns realized profit. This is the minimal Deriv contract-buy flow —
        test it thoroughly against a DEMO account before ever pointing
        DERIV_API_TOKEN at a real one.
        """
        await ws.send(json.dumps({
            "proposal": 1,
            "amount": entry["stake"],
            "basis": "stake",
            "contract_type": entry["contract_type"],
            "currency": "USD",
            "duration": entry["duration"],
            "duration_unit": "t",
            "symbol": entry["market"],
        }))
        proposal_resp = json.loads(await ws.recv())
        if "error" in proposal_resp:
            raise RuntimeError(proposal_resp["error"]["message"])
        proposal_id = proposal_resp["proposal"]["id"]
        ask_price = proposal_resp["proposal"]["ask_price"]

        await ws.send(json.dumps({"buy": proposal_id, "price": ask_price}))
        buy_resp = json.loads(await ws.recv())
        if "error" in buy_resp:
            raise RuntimeError(buy_resp["error"]["message"])
        contract_id = buy_resp["buy"]["contract_id"]

        await ws.send(json.dumps({
            "proposal_open_contract": 1,
            "contract_id": contract_id,
            "subscribe": 1,
        }))
        while True:
            update = json.loads(await ws.recv())
            contract = update.get("proposal_open_contract", {})
            if contract.get("is_sold"):
                await ws.send(json.dumps({
                    "forget_all": "proposal_open_contract"
                }))
                return float(contract.get("profit", 0))
