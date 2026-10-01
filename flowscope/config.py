import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    symbols: tuple[str, ...]
    feed: str               # binance | sim | replay
    ws_base: str
    rest_base: str
    replay_path: str | None
    replay_speed: float
    record_path: str | None
    history_sec: int
    sim_warmup: int


def load() -> Settings:
    syms = os.getenv("SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT")
    return Settings(
        symbols=tuple(s.strip().upper() for s in syms.split(",") if s.strip()),
        feed=os.getenv("FEED", "binance").lower(),
        ws_base=os.getenv("BINANCE_WS_URL", "wss://fstream.binance.com").rstrip("/"),
        rest_base=os.getenv("BINANCE_REST_URL", "https://fapi.binance.com").rstrip("/"),
        replay_path=os.getenv("REPLAY_PATH") or None,
        replay_speed=float(os.getenv("REPLAY_SPEED", "1")),
        record_path=os.getenv("RECORD_PATH") or None,
        history_sec=int(os.getenv("HISTORY_SEC", "7200")),
        sim_warmup=int(os.getenv("SIM_WARMUP", "2400")),
    )
