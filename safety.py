from dataclasses import dataclass, field
from datetime import date


@dataclass
class SafetyConfig:
    daily_loss_limit: float = 20.0       # stop everything once today's net P&L hits -$20
    per_market_loss_limit: float = 8.0   # stop a single market once it hits -$8 today
    max_consecutive_losses: int = 6      # stop everything after N losses in a row, any market
    max_trades_per_day: int = 500        # hard ceiling so a bug can't fire forever


@dataclass
class SafetyState:
    day: date = field(default_factory=date.today)
    daily_pnl: float = 0.0
    daily_trade_count: int = 0
    pnl_by_market: dict = field(default_factory=dict)
    consecutive_losses: int = 0
    halted: bool = False
    halt_reason: str = ""

    def _roll_day_if_needed(self):
        today = date.today()
        if today != self.day:
            self.day = today
            self.daily_pnl = 0.0
            self.daily_trade_count = 0
            self.pnl_by_market = {}
            self.consecutive_losses = 0
            self.halted = False
            self.halt_reason = ""


class SafetyGuard:
    """
    Every trade the worker wants to place goes through can_trade() first.
    Every trade result goes through record_result() after.
    This is the only thing standing between the autotrader and an unattended
    losing streak, so keep it simple and keep it in front of every trade —
    don't let the worker bypass it "just this once."
    """

    def __init__(self, config: SafetyConfig):
        self.config = config
        self.state = SafetyState()

    def can_trade(self, market: str) -> tuple[bool, str]:
        self.state._roll_day_if_needed()
        if self.state.halted:
            return False, self.state.halt_reason
        if self.state.daily_trade_count >= self.config.max_trades_per_day:
            return False, "Daily trade count ceiling reached"
        if self.state.daily_pnl <= -abs(self.config.daily_loss_limit):
            return False, "Daily loss limit reached"
        market_pnl = self.state.pnl_by_market.get(market, 0.0)
        if market_pnl <= -abs(self.config.per_market_loss_limit):
            return False, f"Per-market loss limit reached for {market}"
        return True, ""

    def record_result(self, market: str, profit: float):
        self.state._roll_day_if_needed()
        self.state.daily_trade_count += 1
        self.state.daily_pnl += profit
        self.state.pnl_by_market[market] = self.state.pnl_by_market.get(market, 0.0) + profit

        if profit < 0:
            self.state.consecutive_losses += 1
        else:
            self.state.consecutive_losses = 0

        if self.state.daily_pnl <= -abs(self.config.daily_loss_limit):
            self.state.halted = True
            self.state.halt_reason = f"Daily loss limit hit ({self.state.daily_pnl:.2f})"
        elif self.state.consecutive_losses >= self.config.max_consecutive_losses:
            self.state.halted = True
            self.state.halt_reason = f"{self.state.consecutive_losses} consecutive losses"

    def status(self) -> dict:
        self.state._roll_day_if_needed()
        return {
            "halted": self.state.halted,
            "halt_reason": self.state.halt_reason,
            "daily_pnl": round(self.state.daily_pnl, 2),
            "daily_trade_count": self.state.daily_trade_count,
            "consecutive_losses": self.state.consecutive_losses,
            "pnl_by_market": {k: round(v, 2) for k, v in self.state.pnl_by_market.items()},
        }

    def reset_halt(self):
        """Manual resume after a human has reviewed why it halted."""
        self.state.halted = False
        self.state.halt_reason = ""
