from decimal import Decimal

import ccxt

from lumibot.example_strategies.binance_futures_testnet_smoke import (
    _amount_from_notional,
    _coerce_symbol,
    _opposite_side,
    _parse_bool,
    _position_side_for_open,
    _set_risk_controls,
    _use_binance_demo_urls,
)


def test_coerce_symbol_accepts_ccxt_symbol():
    markets = {"BTC/USDT:USDT": {"id": "BTCUSDT", "symbol": "BTC/USDT:USDT", "swap": True, "quote": "USDT"}}

    assert _coerce_symbol("BTC/USDT:USDT", markets) == "BTC/USDT:USDT"


def test_coerce_symbol_accepts_binance_market_id():
    markets = {"BTC/USDT:USDT": {"id": "BTCUSDT", "symbol": "BTC/USDT:USDT", "swap": True, "quote": "USDT"}}

    assert _coerce_symbol("BTCUSDT", markets) == "BTC/USDT:USDT"


def test_coerce_symbol_ignores_non_usdt_swaps():
    markets = {
        "BTC/USDT": {"id": "BTCUSDT", "symbol": "BTC/USDT", "swap": False, "quote": "USDT"},
        "BTC/USDT:USDT": {"id": "BTCUSDT", "symbol": "BTC/USDT:USDT", "swap": True, "quote": "USDT"},
    }

    assert _coerce_symbol("BTCUSDT", markets) == "BTC/USDT:USDT"


def test_side_helpers():
    assert _opposite_side("buy") == "sell"
    assert _opposite_side("sell") == "buy"
    assert _position_side_for_open("buy") == "LONG"
    assert _position_side_for_open("sell") == "SHORT"


def test_parse_bool_accepts_binance_response_strings():
    assert _parse_bool(True) is True
    assert _parse_bool("true") is True
    assert _parse_bool("false") is False


def test_use_binance_demo_urls_switches_api_urls():
    class FakeExchange:
        id = "binanceusdm"
        urls = {
            "api": {
                "sapi": "https://api.binance.com/sapi/v1",
                "fapiPublic": "https://fapi.binance.com/fapi/v1",
            },
            "demo": {"fapiPublic": "https://demo-fapi.binance.com/fapi/v1"},
        }

    exchange = FakeExchange()

    _use_binance_demo_urls(exchange)

    assert exchange.urls["api"]["fapiPublic"] == "https://demo-fapi.binance.com/fapi/v1"
    assert exchange.urls["api"]["sapi"] == "https://api.binance.com/sapi/v1"


def test_amount_from_notional_rounds_up_above_binance_min_notional():
    class FakeExchange:
        markets = {
            "BTC/USDT:USDT": {
                "precision": {"amount": 3},
                "limits": {"cost": {"min": 50}},
                "info": {
                    "filters": [
                        {"filterType": "MARKET_LOT_SIZE", "stepSize": "0.001"},
                        {"filterType": "MIN_NOTIONAL", "notional": "50"},
                    ]
                },
            }
        }

        def amount_to_precision(self, _symbol, amount):
            return f"{amount:.3f}"

    amount, estimated_notional = _amount_from_notional(
        FakeExchange(),
        "BTC/USDT:USDT",
        Decimal("50"),
        Decimal("64015.5"),
    )

    assert amount == "0.001"
    assert estimated_notional >= Decimal("50")


def test_set_risk_controls_ignores_open_order_margin_mode_error():
    class FakeExchange:
        def __init__(self):
            self.leverage_set = False

        def set_margin_mode(self, _margin_mode, _symbol):
            raise ccxt.ExchangeError('binanceusdm {"code":-4047,"msg":"Margin type cannot be changed if there exists open orders."}')

        def set_leverage(self, leverage, symbol):
            assert leverage == 5
            assert symbol == "NEAR/USDT:USDT"
            self.leverage_set = True

    exchange = FakeExchange()

    _set_risk_controls(exchange, "NEAR/USDT:USDT", 5, "isolated")

    assert exchange.leverage_set is True
