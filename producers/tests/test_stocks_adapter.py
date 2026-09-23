import importlib.util
import io
import json
from pathlib import Path
from unittest.mock import patch

from rat_producers.app import App

ADAPTER = Path(__file__).resolve().parents[2] / "plugins" / "stocks" / "adapter.py"
spec = importlib.util.spec_from_file_location("stocks_adapter", ADAPTER)
stocks = importlib.util.module_from_spec(spec)
spec.loader.exec_module(stocks)

SAMPLE_CHART_RESPONSE = {
    "chart": {
        "result": [
            {
                "meta": {
                    "currency": "USD",
                    "symbol": "AAPL",
                    "regularMarketPrice": 250.5,
                    "regularMarketTime": 1725192000,
                }
            }
        ]
    }
}


def fake_urlopen(req, timeout=None):
    return io.BytesIO(json.dumps(SAMPLE_CHART_RESPONSE).encode())


def test_extract_stocks():
    assert stocks.extract_stocks("AAPL is rising") == ["AAPL"]
    assert stocks.extract_stocks("Buy $AAPL and $MSFT now") == ["AAPL", "MSFT"]
    assert stocks.extract_stocks("AAPL $AAPL AAPL") == ["AAPL"]
    assert stocks.extract_stocks("TOOLONGTICKER 123 !@#") == []


def test_register_hooks():
    app = App()
    stocks.register(app)
    assert "stocks" in app.sources
    assert "stocks" in app.extractors
    assert app.extractors["stocks"] == stocks.extract_stocks


def test_poll_maps_to_envelopes():
    with patch.object(stocks.urllib.request, "urlopen", fake_urlopen), \
         patch.object(stocks, "_load_symbols", return_value=["AAPL"]):
        envs = stocks.poll()

    assert len(envs) == 1
    env = envs[0]
    assert env["event_id"] == "stocks:AAPL:1725192000000"
    assert env["source"] == "stocks"
    assert env["entities"] == []  # Core computes entities
    assert env["ts_ms"] == 1725192000000
    assert env["payload"] == {
        "symbol": "AAPL",
        "price": 250.5,
        "currency": "USD",
    }
    assert set(env) == {"event_id", "source", "entities", "ts_ms", "payload"}


def test_poll_filters_since_ms():
    with patch.object(stocks.urllib.request, "urlopen", fake_urlopen), \
         patch.object(stocks, "_load_symbols", return_value=["AAPL"]):
        envs = stocks.poll(since_ms=1725192000000)
    assert envs == []


def test_poll_handles_error_gracefully():
    def failing_urlopen(req, timeout=None):
        if "AAPL" in req.full_url:
            raise OSError("Connection refused")
        return io.BytesIO(json.dumps(SAMPLE_CHART_RESPONSE).encode())

    with patch.object(stocks.urllib.request, "urlopen", failing_urlopen), \
         patch.object(stocks, "_load_symbols", return_value=["AAPL", "MSFT"]):
        envs = stocks.poll()
    assert len(envs) == 1
    assert envs[0]["payload"]["symbol"] == "AAPL"  # from the mocked MSFT response


def test_user_config_override(tmp_path):
    user_conf = tmp_path / "stocks.toml"
    user_conf.write_text('[config]\nsymbols = ["NVDA", "TSLA"]\n')
    with patch("pathlib.Path.home", return_value=tmp_path.parent):
        config_dir = tmp_path.parent / ".config" / "rat"
        config_dir.mkdir(parents=True, exist_ok=True)
        (config_dir / "stocks.toml").write_text('[config]\nsymbols = ["NVDA", "TSLA"]\n')
        symbols = stocks._load_symbols()
        assert symbols == ["NVDA", "TSLA"]
