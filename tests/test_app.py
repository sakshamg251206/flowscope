import os

os.environ["FEED"] = "sim"

from fastapi.testclient import TestClient  # noqa: E402

from flowscope.app import app  # noqa: E402


def test_health_snapshot_ws_and_ui():
    with TestClient(app) as c:
        h = c.get("/api/health").json()
        assert h["feed"] == "sim" and "BTCUSDT" in h["symbols"]
        s = c.get("/api/snapshot", params={"symbol": "ethusdt", "bucket": 1}).json()
        assert s["data"]["symbol"] == "ETHUSDT"
        assert c.get("/api/snapshot", params={"symbol": "NOPE"}).status_code == 404
        assert "Flowscope" in c.get("/").text
        with c.websocket_connect("/ws") as ws:
            first = ws.receive_json()
            assert set(first["data"]) == set(h["symbols"])
            ws.send_json({"bucket": 10, "window": 300})
            nxt = ws.receive_json()
            nxt = ws.receive_json() if nxt["data"]["BTCUSDT"]["bucket"] != 10 else nxt
            assert nxt["data"]["BTCUSDT"]["bucket"] == 10
