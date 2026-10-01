"""Market data feeds. All feeds emit Binance combined-stream messages."""
from __future__ import annotations

import asyncio
import json
import logging
import math
import random
import time
from typing import Callable

import websockets

log = logging.getLogger("flowscope.feed")
Handler = Callable[[dict, int], None]

DEFAULT_TICKS = {"BTCUSDT": 0.1, "ETHUSDT": 0.01, "SOLUSDT": 0.01, "BNBUSDT": 0.01,
                 "XRPUSDT": 0.0001, "DOGEUSDT": 0.00001}


async def fetch_ticks(rest_base: str, symbols: tuple[str, ...]) -> dict[str, float]:
    ticks = {s: DEFAULT_TICKS.get(s, 0.01) for s in symbols}
    try:
        import httpx
        async with httpx.AsyncClient(timeout=6) as c:
            r = await c.get(f"{rest_base}/fapi/v1/exchangeInfo")
            r.raise_for_status()
            for s in r.json().get("symbols", []):
                if s["symbol"] in ticks:
                    for f in s.get("filters", []):
                        if f.get("filterType") == "PRICE_FILTER":
                            ticks[s["symbol"]] = float(f["tickSize"])
    except Exception as e:  # network blocked, geo-restricted, etc.
        log.warning("exchangeInfo unavailable (%s); using default tick sizes", e)
    return ticks


class Recorder:
    def __init__(self, path: str):
        self.f = open(path, "a", buffering=1)

    def write(self, raw: str, now_ms: int) -> None:
        self.f.write(json.dumps({"r": now_ms, "m": raw}) + "\n")


class BinanceFeed:
    kind = "live"

    def __init__(self, ws_base: str, symbols: tuple[str, ...], recorder: Recorder | None = None):
        streams = "/".join(f"{s.lower()}@bookTicker/{s.lower()}@aggTrade" for s in symbols)
        self.url = f"{ws_base}/stream?streams={streams}"
        self.recorder = recorder
        self.status = "connecting"
        self.error: str | None = None

    async def run(self, handle: Handler) -> None:
        backoff = 1
        while True:
            try:
                async with websockets.connect(self.url, ping_interval=20, max_size=2**22) as ws:
                    self.status, self.error, backoff = "live", None, 1
                    log.info("connected %s", self.url)
                    async for raw in ws:
                        now = int(time.time() * 1000)
                        if self.recorder:
                            self.recorder.write(raw, now)
                        handle(json.loads(raw), now)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.status, self.error = "reconnecting", f"{type(e).__name__}: {e}"
                log.warning("feed error %s; retry in %ss", self.error, backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)


class ReplayFeed:
    kind = "replay"

    def __init__(self, path: str, speed: float = 1.0):
        self.path, self.speed = path, max(speed, 0.01)
        self.status, self.error = "replaying", None

    async def run(self, handle: Handler) -> None:
        while True:
            first_r = start = None
            with open(self.path) as f:
                for line in f:
                    rec = json.loads(line)
                    if first_r is None:
                        first_r, start = rec["r"], time.monotonic()
                    due = (rec["r"] - first_r) / 1000 / self.speed
                    lag = due - (time.monotonic() - start)
                    if lag > 0:
                        await asyncio.sleep(lag)
                    handle(json.loads(rec["m"]), rec["r"])
            self.status = "replay finished, looping"


class _SimBook:
    """Toy queue model: price moves when a best queue is exhausted."""

    def __init__(self, symbol: str, tick: float, price: float, depth: float, rng: random.Random):
        self.s, self.tick, self.rng = symbol, tick, rng
        self.pb = round(price / tick)
        self.pa = self.pb + 1
        self.base = depth
        self.qb = self.qa = depth
        self.phase = rng.random() * 6.283
        self.bias = 0.0

    def liquidity(self, t: float) -> float:   # slow depth regimes, so beta varies with depth
        return self.base * (1 + 0.65 * math.sin(t / 180 + self.phase))

    def step(self, now_ms: int) -> list[dict]:
        r, L = self.rng, self.liquidity(now_ms / 1000)
        self.bias = 0.97 * self.bias + r.gauss(0, 0.04)
        size = r.expovariate(1 / (0.12 * L))
        out = []
        u = r.random()
        tilt = self.bias * 0.15                     # persistent buy/sell pressure
        self.qb += 0.01 * (L - self.qb)             # queues revert toward current liquidity
        self.qa += 0.01 * (L - self.qa)
        if u < 0.22 + tilt:
            self.qb += size
        elif u < 0.44:
            self.qa += size
        elif u < 0.56:
            self.qb -= size
        elif u < 0.68:
            self.qa -= size
        elif u < 0.84 - tilt:
            self.qb -= size
            out.append(self._trade(self.pb, size, True, now_ms))
        else:
            self.qa -= size
            out.append(self._trade(self.pa, size, False, now_ms))
        if self.qb <= 0:
            self.pb -= 1; self.pa -= 1
            self.qb, self.qa = L * r.uniform(0.4, 1.1), L * r.uniform(0.05, 0.3)
        elif self.qa <= 0:
            self.pa += 1; self.pb += 1
            self.qa, self.qb = L * r.uniform(0.4, 1.1), L * r.uniform(0.05, 0.3)
        d = self.tick_decimals()
        out.append({"stream": f"{self.s.lower()}@bookTicker", "data": {
            "e": "bookTicker", "E": now_ms, "T": now_ms, "s": self.s,
            "b": f"{self.pb * self.tick:.{d}f}", "B": f"{self.qb:.4f}",
            "a": f"{self.pa * self.tick:.{d}f}", "A": f"{self.qa:.4f}"}})
        return out

    def tick_decimals(self) -> int:
        return max(0, -int(math.floor(math.log10(self.tick))))

    def _trade(self, ptick, qty, buyer_is_maker, now_ms) -> dict:
        return {"stream": f"{self.s.lower()}@aggTrade", "data": {
            "e": "aggTrade", "E": now_ms, "T": now_ms, "s": self.s,
            "p": f"{ptick * self.tick:.{self.tick_decimals()}f}", "q": f"{qty:.4f}", "m": buyer_is_maker}}


class SimFeed:
    kind = "sim"
    SEEDS = {"BTCUSDT": (100000.0, 6.0), "ETHUSDT": (3500.0, 90.0), "SOLUSDT": (180.0, 900.0)}

    def __init__(self, symbols: tuple[str, ...], ticks: dict[str, float], seed: int = 7):
        rng = random.Random(seed)
        self.books = [_SimBook(s, ticks[s], *self.SEEDS.get(s, (50.0, 500.0)), rng) for s in symbols]
        self.rng = rng
        self.status, self.error = "simulated", None

    def burst(self, now_ms: int) -> list[dict]:
        msgs = []
        for b in self.books:
            for _ in range(self.rng.randint(0, 3)):
                msgs.extend(b.step(now_ms))
        return msgs

    def warmup(self, handle: Handler, seconds: int) -> None:
        """Back-fill history so the offline demo shows full charts immediately."""
        t = int(time.time() * 1000) - seconds * 1000
        for _ in range(seconds * 50):
            t += 20
            for m in self.burst(t):
                handle(m, t)

    async def run(self, handle: Handler) -> None:
        while True:
            now = int(time.time() * 1000)
            for m in self.burst(now):
                handle(m, now)
            await asyncio.sleep(0.02)
