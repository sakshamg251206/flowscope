from __future__ import annotations

import asyncio
import json
import logging
import time
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles

from . import __version__
from .config import load
from .engine import SymbolEngine, dispatch
from .feeds import DEFAULT_TICKS, BinanceFeed, Recorder, ReplayFeed, SimFeed, fetch_ticks

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("flowscope")

BUCKETS = (1, 5, 10, 30)
WINDOWS = (300, 900, 1800)


class Hub:
    def __init__(self):
        self.settings = load()
        self.engines: dict[str, SymbolEngine] = {}
        self.feed = None
        self.task: asyncio.Task | None = None
        self.started = time.time()
        self.messages = 0

    async def start(self):
        s = self.settings
        if s.feed == "binance":
            ticks = await fetch_ticks(s.rest_base, s.symbols)
            rec = Recorder(s.record_path) if s.record_path else None
            self.feed = BinanceFeed(s.ws_base, s.symbols, rec)
        elif s.feed == "replay":
            if not s.replay_path:
                raise RuntimeError("FEED=replay needs REPLAY_PATH")
            ticks = await fetch_ticks(s.rest_base, s.symbols)
            self.feed = ReplayFeed(s.replay_path, s.replay_speed)
        else:
            ticks = {x: DEFAULT_TICKS.get(x, 0.01) for x in s.symbols}
            self.feed = SimFeed(s.symbols, ticks)
        self.engines = {x: SymbolEngine(x, ticks[x], s.history_sec) for x in s.symbols}
        if isinstance(self.feed, SimFeed) and s.sim_warmup > 0:
            self.feed.warmup(self.handle, s.sim_warmup)
        self.task = asyncio.create_task(self.feed.run(self.handle))
        log.info("feed=%s symbols=%s ticks=%s", s.feed, s.symbols, ticks)

    def handle(self, msg: dict, now_ms: int):
        self.messages += 1
        try:
            dispatch(self.engines, msg, now_ms)
        except (KeyError, ValueError, TypeError) as e:
            log.debug("bad message %s: %s", e, msg)

    def meta(self) -> dict:
        return {"version": __version__, "feed": self.settings.feed,
                "status": getattr(self.feed, "status", "starting"),
                "error": getattr(self.feed, "error", None),
                "symbols": list(self.settings.symbols), "messages": self.messages,
                "uptime_s": round(time.time() - self.started), "buckets": BUCKETS, "windows": WINDOWS}

    def payload(self, k: int, w: int) -> dict:
        return {"meta": self.meta(), "data": {s: e.snapshot(k, w) for s, e in self.engines.items()}}


hub = Hub()


@asynccontextmanager
async def lifespan(_: FastAPI):
    await hub.start()
    yield
    if hub.task:
        hub.task.cancel()


app = FastAPI(title="Flowscope", version=__version__, lifespan=lifespan)


def _clamp(k: int, w: int) -> tuple[int, int]:
    return (k if k in BUCKETS else 5), (w if w in WINDOWS else 900)


@app.get("/api/health")
def health():
    return hub.meta()


@app.get("/api/snapshot")
def snapshot(symbol: str = Query("BTCUSDT"), bucket: int = 5, window: int = 900):
    eng = hub.engines.get(symbol.upper())
    if not eng:
        raise HTTPException(404, f"unknown symbol {symbol}; tracking {list(hub.engines)}")
    k, w = _clamp(bucket, window)
    return {"meta": hub.meta(), "data": eng.snapshot(k, w)}


@app.websocket("/ws")
async def ws_stream(ws: WebSocket):
    await ws.accept()
    k, w = 5, 900
    try:
        while True:
            await ws.send_text(json.dumps(hub.payload(k, w)))
            try:
                msg = await asyncio.wait_for(ws.receive_text(), timeout=1.0)
                p = json.loads(msg)
                k, w = _clamp(int(p.get("bucket", k)), int(p.get("window", w)))
            except asyncio.TimeoutError:
                pass
    except (WebSocketDisconnect, RuntimeError):
        return


app.mount("/", StaticFiles(directory=Path(__file__).parent / "static", html=True), name="static")
