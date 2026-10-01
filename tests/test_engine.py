import pytest

from flowscope.engine import SymbolEngine, dispatch, ofi_increment, ols

# Hand-computed e_n cases. Prev book: bid 100 x 5, ask 101 x 7 (ticks).
P = (100, 5.0, 101, 7.0)


@pytest.mark.parametrize("new,expected", [
    ((101, 3.0, 102, 7.0), 3.0 + 7.0),     # bid up 1 tick (new size counts) and ask up (old ask removed = +7)
    ((99, 4.0, 101, 7.0), -5.0),           # bid down: old bid queue wiped out
    ((100, 8.0, 101, 7.0), 3.0),           # bid same, size +3
    ((100, 5.0, 100 + 1, 9.0), -2.0),      # ask same, size +2 -> sell pressure
    ((100, 5.0, 102, 4.0), 7.0),           # ask up: old ask queue consumed
    ((100, 5.0, 100 + 1, 7.0), 0.0),       # no change
])
def test_ofi_increment_cases(new, expected):
    assert ofi_increment(*P, *new) == pytest.approx(expected)


def test_ask_down_counts_new_size():
    # bid unchanged, ask improves to 100.5? use ticks: bid 100, ask 101 -> ask 101 stays; test ask down with wider book
    prev = (100, 5.0, 103, 7.0)
    assert ofi_increment(*prev, 100, 5.0, 102, 2.0) == pytest.approx(-2.0)


def test_ols_recovers_line():
    x = [float(i) for i in range(50)]
    y = [2.0 + 0.5 * v for v in x]
    f = ols(x, y)
    assert f["beta"] == pytest.approx(0.5)
    assert f["alpha"] == pytest.approx(2.0)
    assert f["r2"] == pytest.approx(1.0)


def test_buckets_and_dmid():
    e = SymbolEngine("TEST", tick=0.5)
    t0 = 1_700_000_000_000
    e.on_quote(t0, 100.0, 5, 100.5, 5)          # mid 200.5 ticks
    e.on_quote(t0 + 1000, 100.5, 3, 101.0, 5)   # bid up: +3, ask up: +5 -> 8 ; mid 201.5
    e.on_quote(t0 + 2000, 100.5, 3, 101.0, 5)
    e.on_quote(t0 + 3000, 100.5, 3, 101.0, 5)
    rows = e.aggregate(1)
    assert rows[0]["ofi"] == pytest.approx(8.0)
    assert rows[0]["dmid"] == pytest.approx(1.0)


def test_crossed_quotes_ignored():
    e = SymbolEngine("TEST", tick=0.1)
    e.on_quote(1_000, 10.0, 1, 10.0, 1)
    assert e.crossed == 1 and e.best is None


def test_dispatch_binance_messages():
    e = SymbolEngine("BTCUSDT", tick=0.1)
    engines = {"BTCUSDT": e}
    dispatch(engines, {"stream": "btcusdt@bookTicker", "data": {
        "e": "bookTicker", "E": 1000, "s": "BTCUSDT", "b": "100.0", "B": "1", "a": "100.1", "A": "2"}}, 0)
    dispatch(engines, {"data": {"e": "aggTrade", "T": 1500, "s": "BTCUSDT", "p": "100.1", "q": "0.5", "m": False}}, 0)
    assert e.n_quotes == 1 and e.n_trades == 1
    assert e.cur.tflow == pytest.approx(0.5)
