"""Fast-forward the simulated feed through the engine: OFI must explain dMid."""
from flowscope.engine import SymbolEngine, dispatch
from flowscope.feeds import DEFAULT_TICKS, SimFeed


def run_sim(seconds=3600):
    syms = ("BTCUSDT",)
    ticks = {s: DEFAULT_TICKS[s] for s in syms}
    feed = SimFeed(syms, ticks, seed=1)
    engines = {s: SymbolEngine(s, ticks[s]) for s in syms}
    t = 1_700_000_000_000
    for _ in range(seconds * 50):
        t += 20
        for m in feed.burst(t):
            dispatch(engines, m, t)
    return engines["BTCUSDT"]


def test_sim_ofi_relationship():
    e = run_sim()
    snap = e.snapshot(10, 1800)
    assert snap["fit"]["beta"] > 0
    assert snap["fit"]["r2"] > 0.3
    assert snap["fit"]["r2"] > snap["baseline"]["r2"]
