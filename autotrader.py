import asyncio
import json
import time
import websockets
import httpx

from safety import SafetyGuard, SafetyConfig

DERIV_REST_BASE_URL = {
    "production": "https://api.derivws.com/trading/v1/",
    "staging": "https://staging-api.derivws.com/trading/v1/",
}

WATCHLIST = [
    {"market": "R_10", "contract_type": "DIGITEVEN", "stake": 0.5, "duration": 1},
    {"market": "R_25", "contract_type": "DIGITODD", "stake": 0.5, "duration": 1},
]

TRADE_INTERVAL_SECONDS = 30


class AutoTrader:
    def __init__(self, log_trade_fn):
        self.guard = SafetyGuard(SafetyConfig())
        self.log_trade_fn = log_trade_fn
        self._task: asyncio.Task | None = None
        self._ws = None
        self._running = False

        self.access_token: str | None = None
        self.loginid: str | None = None
        self.environment: str = "production"

    @property
    def is_running(self) -> bool:
        return self._running

    def status(self) -> dict:
        return {
            "running": self._running,
            "loginid": self.loginid,
            "environment": self.environment,
            **self.guard.status(),
        }

    async def start(self, access_token: str, loginid: str, environment: str = "production"):
        if self._running:
            return
        if not access_token:
            raise RuntimeError("access_token is required")
        if not loginid:
            raise RuntimeError("loginid is required")

        self.access_token = access_token
        self.loginid = loginid
        self.environment = environment
        self._running = True
        self._task = asyncio.create_task(self._run_loop())

    async def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        if self._ws:
            await self._ws.close()
            self._ws = None

        self.access_token = None
        self.loginid = None

    def resume_after_halt(self):
        self.guard.reset_halt()

    async def _get_otp_url(self) -> str:
        base_url = DERIV_REST_BASE_URL.get(
            self.environment, DERIV_REST_BASE_URL["production"]
        )
        endpoint = f"{base_url}options/accounts/{self.loginid}/otp"

        async with httpx.AsyncClient() as client:
            response = await client.post(
                endpoint,
                headers={"Authorization": f"Bearer {self.access_token}"},
                timeout=15.0,
            )
            response.raise_for_status()
            data = response.json()

        websocket_url = data.get("data", {}).get("url")
        if not websocket_url:
            raise RuntimeError("No WebSocket URL in OTP response")
        return websocket_url

    async def _run_loop(self):
        try:
            ws_url = await self._get_otp_url()
        except Exception as e:
            self._running = False
            raise RuntimeError(f"Failed to get OTP URL: {e}")

        try:
            async with websockets.connect(ws_url) as ws:
                self._ws = ws
                last_fired = {entry["market"]: 0.0 for entry in WATCHLIST}

                while self._running:
                    now = time.time()
                    for entry in WATCHLIST:
                        if now - last_fired[entry["market"]] < TRADE_INTERVAL_SECONDS:
                            continue

                        ok, reason = self.guard.can_trade(entry["market"])
                        if not ok:
                            continue

                        try:
                            profit = await self._place_and_wait(ws, entry)
                            self.guard.record_result(entry["market"], profit)
                            await self.log_trade_fn({
                                "loginid": self.loginid,
                                "market": entry["market"],
                                "strategy": "autotrader",
                                "contract_type": entry["contract_type"],
                                "stake": entry["stake"],
                                "prediction": None,
                                "profit": profit,
                                "result": "win" if profit > 0 else "loss",
                            })
                        except Exception as e:
                            print(f"[autotrader] trade error on {entry['market']}: {e}")

                        last_fired[entry["market"]] = now

                    if self.guard.status()["halted"]:
                        self._running = False
                        break

                    await asyncio.sleep(1)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(f"[autotrader] connection lost: {e}")
            self._running = False
            raise

    async def _place_and_wait(self, ws, entry: dict) -> float:
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
