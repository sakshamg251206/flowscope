"""Order-flow imbalance engine.

Implements Cont, Kukanov & Stoikov (2014): every change of the best quotes
contributes e_n, summed into time buckets, then regressed against mid changes.
Prices are held as integer ticks so comparisons are exact.
"""
from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass

MAX_GAP_FILL = 60  # seconds; longer gaps reset state instead of being filled


def ofi_increment(pb0: int, qb0: float, pa0: int, qa0: float,
                  pb1: int, qb1: float, pa1: int, qa1: float) -> float:
    """e_n for one top-of-book update (prices in ticks)."""
    e = 0.0
    if pb1 >= pb0:
        e += qb1
    if pb1 <= pb0:
        e -= qb0
    if pa1 <= pa0:
        e -= qa1
    if pa1 >= pa0:
        e += qa0
    return e


def ols(x: list[float], y: list[float]) -> dict | None:
    """Simple OLS y = a + b x with R^2 and the slope's t-stat."""
    n = len(x)
    if n < 5:
        return None
    mx, my = sum(x) / n, sum(y) / n
    sxx = sum((a - mx) ** 2 for a in x)
    syy = sum((b - my) ** 2 for b in y)
    sxy = sum((a - mx) * (b - my) for a, b in zip(x, y))
    if sxx <= 0:
        return None
    beta = sxy / sxx
    alpha = my - beta * mx
    r2 = (sxy * sxy) / (sxx * syy) if syy > 0 else 0.0
    s2 = max(syy - beta * sxy, 0.0) / (n - 2)
    se = math.sqrt(s2 / sxx) if s2 > 0 else 0.0
    return {"alpha": alpha, "beta": beta, "r2": r2, "se": se,
            "t": beta / se if se > 0 else None, "n": n}


@dataclass
class Bucket:
    t: int                       # bucket start, epoch seconds (exchange clock)
    ofi: float = 0.0
    tflow: float = 0.0           # signed taker volume, + = buyer-initiated
    mid: float | None = None     # last mid in ticks
    depth_sum: float = 0.0
    n_q: int = 0
    n_t: int = 0


class SymbolEngine:
    def __init__(self, symbol: str, tick: float, history_sec: int = 7200):
        self.symbol = symbol
        self.tick = tick
        self.buckets: deque[Bucket] = deque(maxlen=history_sec)
        self.cur: Bucket | None = None
        self.last: tuple[int, float, int, float] | None = None
        self.best: tuple[float, float, float, float] | None = None
        self.crossed = 0
        self.n_quotes = 0
        self.n_trades = 0
        self.last_ts_ms: int | None = None

    # ---- ingestion -------------------------------------------------------
    def _bucket(self, ts_ms: int) -> Bucket:
        t = ts_ms // 1000
        if self.cur is None:
            self.cur = Bucket(t)
        elif t > self.cur.t:
            prev = self.cur
            self.buckets.append(prev)
            gap = t - prev.t - 1
            if gap > MAX_GAP_FILL:
                self.last = None          # don't let a stale quote create a huge e_n
                self.cur = Bucket(t)
            else:
                for g in range(prev.t + 1, t):
                    self.buckets.append(Bucket(g, mid=prev.mid))
                self.cur = Bucket(t, mid=prev.mid)
        return self.cur                    # late messages fold into current bucket

    def on_quote(self, ts_ms: int, bid: float, bq: float, ask: float, aq: float) -> None:
        pb, pa = round(bid / self.tick), round(ask / self.tick)
        if pa <= pb:
            self.crossed += 1
            return
        b = self._bucket(ts_ms)
        if self.last is not None:
            b.ofi += ofi_increment(*self.last, pb, bq, pa, aq)
        self.last = (pb, bq, pa, aq)
        self.best = (bid, bq, ask, aq)
        b.mid = (pb + pa) / 2
        b.depth_sum += (bq + aq) / 2
        b.n_q += 1
        self.n_quotes += 1
        self.last_ts_ms = ts_ms

    def on_trade(self, ts_ms: int, price: float, qty: float, buyer_is_maker: bool) -> None:
        b = self._bucket(ts_ms)
        b.tflow += -qty if buyer_is_maker else qty
        b.n_t += 1
        self.n_trades += 1

    # ---- analytics -------------------------------------------------------
    def aggregate(self, k: int) -> list[dict]:
        """Group completed 1s buckets into k-second buckets, with dMid in ticks."""
        rows: list[dict] = []
        group: list[Bucket] = []
        prev_mid: float | None = None

        def flush(g: list[Bucket]):
            nonlocal prev_mid
            mids = [x.mid for x in g if x.mid is not None]
            if not mids:
                return
            mid = mids[-1]
            nq = sum(x.n_q for x in g)
            row = {
                "t": (g[0].t // k) * k,
                "ofi": sum(x.ofi for x in g),
                "tflow": sum(x.tflow for x in g),
                "mid": mid,
                "depth": (sum(x.depth_sum for x in g) / nq) if nq else None,
                "dmid": (mid - prev_mid) if prev_mid is not None else None,
            }
            prev_mid = mid
            if row["dmid"] is not None and row["depth"] is not None:
                rows.append(row)

        for b in self.buckets:
            if group and b.t // k != group[0].t // k:
                flush(group)
                group = []
            group.append(b)
        if group and (group[-1].t + 1) % k == 0:   # only emit complete groups
            flush(group)
        return rows

    def snapshot(self, k: int = 5, window_sec: int = 900) -> dict:
        rows = self.aggregate(k)
        w = max(window_sec // k, 5)
        win = rows[-w:]
        x = [r["ofi"] for r in win]
        y = [r["dmid"] for r in win]
        fit = ols(x, y)
        base = ols([r["tflow"] for r in win], y)
        pred = ols(x[:-1], y[1:]) if len(win) > 6 else None

        # Depth scaling: beta per chunk vs mean depth across all history
        chunk = max(30, min(120, len(rows) // 12 or 30))
        pts = []
        for i in range(0, len(rows) - chunk + 1, chunk):
            c = rows[i:i + chunk]
            f = ols([r["ofi"] for r in c], [r["dmid"] for r in c])
            d = sum(r["depth"] for r in c) / len(c)
            if f and f["beta"] > 0 and d > 0:
                pts.append({"depth": d, "beta": f["beta"]})
        lam = None
        if len(pts) >= 4:
            lf = ols([math.log(p["depth"]) for p in pts], [math.log(p["beta"]) for p in pts])
            if lf:
                lam = {"lambda": -lf["beta"], "se": lf["se"], "n": lf["n"], "c": lf["alpha"]}

        recent = list(self.buckets)[-10:]
        span = max(len(recent), 1)
        best = self.best
        return {
            "symbol": self.symbol,
            "tick": self.tick,
            "bucket": k,
            "window": window_sec,
            "n_rows": len(rows),
            "need_rows": 20,
            "fit": fit,
            "baseline": base,
            "predictive": pred,
            "scatter": [[r["ofi"], r["dmid"]] for r in win],
            "series": [[r["t"], r["mid"] * self.tick, r["ofi"]] for r in rows[-240:]],
            "depth_points": pts,
            "depth_fit": lam,
            "live": {
                "bid": best[0] if best else None, "bid_qty": best[1] if best else None,
                "ask": best[2] if best else None, "ask_qty": best[3] if best else None,
                "spread_ticks": round((best[2] - best[0]) / self.tick) if best else None,
                "depth": (best[1] + best[3]) / 2 if best else None,
                "quotes_per_s": sum(b.n_q for b in recent) / span,
                "trades_per_s": sum(b.n_t for b in recent) / span,
                "crossed": self.crossed,
                "history_s": len(self.buckets),
                "last_ts_ms": self.last_ts_ms,
            },
        }


def dispatch(engines: dict[str, SymbolEngine], msg: dict, now_ms: int) -> None:
    """Route a Binance (combined-stream) message to its engine."""
    data = msg.get("data", msg)
    sym = data.get("s")
    eng = engines.get(sym)
    if eng is None:
        return
    ev = data.get("e")
    if ev == "aggTrade" or ev == "trade":
        eng.on_trade(int(data.get("T") or data.get("E") or now_ms),
                     float(data["p"]), float(data["q"]), bool(data["m"]))
    elif ev == "bookTicker" or ("b" in data and "a" in data and "B" in data):
        eng.on_quote(int(data.get("E") or data.get("T") or now_ms),
                     float(data["b"]), float(data["B"]), float(data["a"]), float(data["A"]))
