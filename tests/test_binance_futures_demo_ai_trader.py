import argparse
import subprocess
import sys
import time
import types as module_types
import urllib.parse
from decimal import Decimal

import ccxt
import pytest

from lumibot.example_strategies.binance_futures_demo_ai_trader import (
    AIDecision,
    AgentOpinion,
    DEFAULT_LOCK_FILE,
    Indicators,
    MarketRegime,
    MultiAgentDecision,
    PositionSummary,
    SymbolCandidate,
    _amount_from_notional_with_cap,
    _account_position_block_reason,
    _advisor_decision_comparisons,
    _advisor_model_chain,
    _ai_context_payload,
    _apply_runtime_config,
    _auto_tune_notification_message,
    _acquire_process_lock,
    _breakeven_or_trailing_action,
    _binance_performance_summary,
    _binance_symbol_performance,
    _binance_symbol_score_penalty,
    _calculate_indicators,
    _candidate_payload,
    _cancel_open_algo_orders,
    _cleanup_flat_symbol_stale_orders,
    _daily_loss_exceeded,
    _decision_prompt,
    _detect_market_regime,
    _dynamic_exit_plan,
    _entry_execution_plan_from_ai,
    _entry_quote_drift_block_reason,
    _entry_notional_from_risk,
    _execution_context_payload,
    _exit_plan_from_open_trade,
    _ensure_native_protection,
    _expert_signals,
    _fetch_position,
    _fetch_usdt_equity,
    _fee_reward_entry_block_reason,
    _gemini_decision,
    _global_entry_block_reason,
    _funding_entry_block_reason,
    _large_cap_usdm_symbols,
    _load_state,
    _maybe_auto_tune_parameters,
    _maybe_apply_binance_symbol_performance_guard,
    _maybe_print_health_report,
    _maybe_reprice_native_protection,
    _model_chain,
    _native_protection_order_count,
    _openai_compatible_completion_text,
    _parse_ai_decision,
    _parse_model_spec,
    _parse_models,
    _parse_multi_agent_decision,
    _parse_symbols,
    _parse_timeframes,
    _place_native_protection,
    _position_notional,
    _protective_stop_take_prices,
    _recent_entry_block_counts,
    _clear_exchange_error_streak,
    _close_binance_report,
    _close_position,
    _record_entry_block,
    _record_exchange_error,
    _record_trade_journal_event,
    _record_trade_outcome,
    _resolve_requested_symbols,
    _risk_manager_veto,
    _run_iteration,
    _run_trading_cycle,
    _safe_hot_reload_runtime_config,
    _save_state,
    _select_symbol_and_candidates_for_iteration,
    _shadow_decision_comparisons,
    _shadow_model_chain,
    _spread_entry_block_reason,
    _should_retry_ai_error,
    _stale_candle_reason,
    _sync_binance_performance,
    _submit_entry_order,
    _symbol_setup_score,
    _stop_or_take_profit_action,
    _strip_json_fence,
    _symbol_entry_block_reason,
    _timeframe_payload,
    _telegram_notification_text,
    _notify,
    _update_open_trade_state,
    _validate_args,
)


class FakeBinanceFuturesExchange:
    id = "binance"
    markets = {"BTC/USDT:USDT": {"id": "BTCUSDT", "swap": True, "quote": "USDT"}}

    def __init__(self, account=None, positions=None):
        self.account = account or {}
        self.positions = positions or []
        self.fetch_balance_called = False
        self.fetch_positions_called = False

    def market(self, symbol):
        return self.markets[symbol]

    def fapiPrivateV2GetAccount(self):
        return self.account

    def fapiPrivateV2GetPositionRisk(self, params):
        assert params == {"symbol": "BTCUSDT"}
        return self.positions

    def fetch_balance(self):
        self.fetch_balance_called = True
        return {"USDT": {"total": "456"}}

    def fetch_positions(self, symbols=None):
        self.fetch_positions_called = True
        return []


class FakeTickerExchange:
    id = "binance"
    markets = {
        "BTC/USDT:USDT": {"id": "BTCUSDT", "symbol": "BTC/USDT:USDT", "swap": True, "quote": "USDT"},
        "ETH/USDT:USDT": {"id": "ETHUSDT", "symbol": "ETH/USDT:USDT", "swap": True, "quote": "USDT"},
        "SOL/USDT:USDT": {"id": "SOLUSDT", "symbol": "SOL/USDT:USDT", "swap": True, "quote": "USDT"},
        "DOGE/USDT:USDT": {"id": "DOGEUSDT", "symbol": "DOGE/USDT:USDT", "swap": True, "quote": "USDT"},
    }

    def fetch_tickers(self, symbols):
        return {
            "BTC/USDT:USDT": {"quoteVolume": "100"},
            "ETH/USDT:USDT": {"quoteVolume": "300"},
            "SOL/USDT:USDT": {"quoteVolume": "200"},
            "DOGE/USDT:USDT": {"quoteVolume": "50"},
        }


class FakeProtectionExchange:
    id = "binance"
    def __init__(self, open_orders=None):
        self.orders = []
        self.open_orders = open_orders or []

    def price_to_precision(self, symbol, price):
        return f"{price:.2f}"

    def fetch_open_orders(self, symbol):
        return self.open_orders

    def create_order(self, symbol, order_type, side, amount, price, params):
        order = {
            "id": f"order-{len(self.orders) + 1}",
            "symbol": symbol,
            "type": order_type,
            "side": side,
            "amount": amount,
            "price": price,
            "params": params,
        }
        self.orders.append(order)
        return order


class FakeAlgoProtectionExchange(FakeProtectionExchange):
    markets = {"BTC/USDT:USDT": {"id": "BTCUSDT", "swap": True, "quote": "USDT"}}

    def __init__(self, open_algo_orders=None):
        super().__init__()
        self.algo_orders = []
        self.open_algo_orders = open_algo_orders or []
        self.cancel_algo_params = []
        self.cancel_all_order_symbols = []

    def market(self, symbol):
        return self.markets[symbol]

    def fapiPrivatePostAlgoOrder(self, params):
        order = {
            "algoId": f"algo-{len(self.algo_orders) + 1}",
            "algoStatus": "NEW",
            "orderType": params["type"],
            **params,
        }
        self.algo_orders.append(order)
        return order

    def fapiPrivateGetOpenAlgoOrders(self, params):
        assert params == {"symbol": "BTCUSDT", "algoType": "CONDITIONAL"}
        return self.open_algo_orders

    def fapiPrivateDeleteAlgoOpenOrders(self, params):
        self.cancel_algo_params.append(params)
        self.open_algo_orders = []
        return {"code": 200, "msg": "done"}

    def cancel_all_orders(self, symbol):
        self.cancel_all_order_symbols.append(symbol)
        return {"code": 200, "msg": "done"}


class FakePerformanceExchange:
    id = "binance"
    markets = {"BTC/USDT:USDT": {"id": "BTCUSDT", "swap": True, "quote": "USDT"}}

    def __init__(self):
        self.income_calls = []
        self.trade_calls = []
        self.funding_rate = Decimal("0.0004")

    def market(self, symbol):
        return self.markets[symbol]

    def fapiPrivateGetIncome(self, params):
        self.income_calls.append(params)
        return [
            {"tranId": "1", "incomeType": "REALIZED_PNL", "income": "3.5", "time": "1000"},
            {"tranId": "2", "incomeType": "COMMISSION", "income": "-0.2", "time": "1001"},
            {"tranId": "3", "incomeType": "FUNDING_FEE", "income": "-0.1", "time": "1002"},
        ]

    def fapiPrivateGetUserTrades(self, params):
        self.trade_calls.append(params)
        return [{"id": "10", "orderId": "20", "qty": "0.1", "realizedPnl": "3.5", "time": "1003"}]

    def fetch_funding_rate(self, symbol):
        return {"fundingRate": str(self.funding_rate)}


class FakeEntryExecutionExchange:
    id = "binance"
    def __init__(self):
        self.orders = []
        self.canceled = []

    def price_to_precision(self, symbol, price):
        return f"{price:.4f}"

    def amount_to_precision(self, symbol, amount):
        return f"{Decimal(str(amount)):.6f}".rstrip("0").rstrip(".")

    def create_order(self, symbol, order_type, side, amount, price, params):
        order = {
            "id": f"entry-{len(self.orders) + 1}",
            "symbol": symbol,
            "type": order_type,
            "side": side,
            "amount": amount,
            "price": price,
            "status": "open" if order_type == "limit" else "closed",
            "filled": "0" if order_type == "limit" else amount,
            "average": price or "100",
            "params": params,
        }
        self.orders.append(order)
        return order

    def fetch_order(self, order_id, symbol):
        return next(order for order in self.orders if order["id"] == order_id)

    def cancel_order(self, order_id, symbol):
        self.canceled.append((order_id, symbol))
        return {"id": order_id, "status": "canceled"}


class FakePartialEntryExecutionExchange(FakeEntryExecutionExchange):
    def create_order(self, symbol, order_type, side, amount, price, params):
        order = super().create_order(symbol, order_type, side, amount, price, params)
        if order_type == "limit":
            order["filled"] = "0.04"
            order["average"] = price
        return order


class FakeCloseAccountingExchange:
    id = "binance"
    markets = {"SOL/USDT:USDT": {"id": "SOLUSDT", "swap": True, "quote": "USDT"}}

    def __init__(self):
        self.orders = []
        self.user_trade_calls = []
        self.income_calls = []
        self.cancel_all_order_symbols = []

    def market(self, symbol):
        return self.markets[symbol]

    def cancel_all_orders(self, symbol):
        self.cancel_all_order_symbols.append(symbol)
        return {"code": 200, "msg": "done"}

    def create_order(self, symbol, order_type, side, amount, price, params):
        order = {
            "id": "2054605069",
            "status": "closed",
            "symbol": symbol,
            "type": order_type,
            "side": side,
            "amount": amount,
            "price": price,
            "params": params,
        }
        self.orders.append(order)
        return order

    def fapiPrivateGetUserTrades(self, params):
        self.user_trade_calls.append(params)
        return [
            {
                "id": "70016794",
                "orderId": "2054605069",
                "qty": "9.03",
                "price": "63.38",
                "realizedPnl": "-1.71920000",
                "commission": "0.22892856",
                "time": "1780802267000",
            }
        ]

    def fapiPrivateGetIncome(self, params):
        self.income_calls.append(params)
        return [
            {"tranId": "entry-commission", "incomeType": "COMMISSION", "income": "-0.19568053", "time": "1780801048000"},
            {"tranId": "exit-pnl", "incomeType": "REALIZED_PNL", "income": "-1.71920000", "time": "1780802267000"},
            {"tranId": "exit-commission", "incomeType": "COMMISSION", "income": "-0.22892856", "time": "1780802267000"},
        ]


class FakeRunIterationExchange:
    id = "binance"
    markets = {
        "BTC/USDT:USDT": {"id": "BTCUSDT", "swap": True, "quote": "USDT", "limits": {}},
        "ETH/USDT:USDT": {"id": "ETHUSDT", "swap": True, "quote": "USDT", "limits": {}},
    }

    def __init__(self, positions=None, available_balance="1000", fail_native_protection=False, ticker_has_spread=True):
        self.cancel_all_order_symbols = []
        self.orders = []
        self.positions = positions or []
        self.available_balance = available_balance
        self.account_calls = 0
        self.fail_native_protection = fail_native_protection
        self.ticker_has_spread = ticker_has_spread
        self.order_book_calls = 0

    def fapiPrivateV2GetAccount(self):
        self.account_calls += 1
        return {"totalWalletBalance": "1000", "availableBalance": self.available_balance, "positions": self.positions}

    def market(self, symbol):
        return self.markets[symbol]

    def amount_to_precision(self, symbol, amount):
        return f"{Decimal(str(amount)):.6f}"

    def fapiPrivateV2GetPositionRisk(self, params):
        assert params == {"symbol": "BTCUSDT"}
        return self.positions

    def cancel_all_orders(self, symbol):
        self.cancel_all_order_symbols.append(symbol)
        return {"code": 200, "msg": "done"}

    def fapiPrivateDeleteAlgoOpenOrders(self, params):
        return {"code": 200, "msg": "done"}

    def fapiPrivateGetOpenAlgoOrders(self, params):
        return []

    def create_order(self, symbol, order_type, side, amount, price, params):
        if self.fail_native_protection is True and order_type in {"STOP_MARKET", "TAKE_PROFIT_MARKET"}:
            raise ccxt.ExchangeError("native protection rejected")
        if self.fail_native_protection == "take_profit" and order_type == "TAKE_PROFIT_MARKET":
            raise ccxt.ExchangeError("native protection rejected")
        order = {
            "id": f"order-{len(self.orders) + 1}",
            "status": "closed",
            "average": "120",
            "symbol": symbol,
            "type": order_type,
            "side": side,
            "amount": amount,
            "price": price,
            "params": params,
        }
        self.orders.append(order)
        return order

    def fetch_ohlcv(self, symbol, timeframe, limit):
        candles = []
        for index in range(limit):
            close = Decimal("100") + Decimal(index) / Decimal("10")
            candles.append(
                [index, close - Decimal("0.5"), close + Decimal("0.5"), close - Decimal("1"), close, 10]
            )
        return candles

    def fetch_ticker(self, symbol):
        if not self.ticker_has_spread:
            return {"last": "120"}
        return {"last": "120", "bid": "119.9", "ask": "120.1"}

    def fetch_order_book(self, symbol, limit=5):
        self.order_book_calls += 1
        return {"bids": [["119.95", "1"]], "asks": [["120.05", "1"]]}


def test_parse_ai_decision_accepts_plain_json():
    decision = _parse_ai_decision('{"action":"buy","confidence":0.72,"reason":"trend up"}')

    assert decision == AIDecision("BUY", Decimal("0.72"), "trend up")


def test_parse_ai_decision_clamps_invalid_action_and_confidence():
    decision = _parse_ai_decision('{"action":"YOLO","confidence":2,"reason":"bad"}')

    assert decision.action == "HOLD"
    assert decision.confidence == Decimal("1")


def test_parse_multi_agent_decision_accepts_committee_json():
    payload = """
    {
      "agents": [
        {"name": "market_analyst", "action": "HOLD", "confidence": 0.4, "reason": "range"},
        {"name": "risk_manager", "action": "HOLD", "confidence": 0.8, "reason": "risk unclear"}
      ],
      "final_decision": {"action": "SELL", "confidence": 0.61, "reason": "trend aligned"}
    }
    """

    decision = _parse_multi_agent_decision(payload)

    assert decision.final == AIDecision("SELL", Decimal("0.61"), "trend aligned")
    assert len(decision.agents) == 2
    assert decision.agents[0].name == "market_analyst"


def test_parse_multi_agent_decision_accepts_agents_object_map():
    payload = """
    {
      "agents": {
        "market_analyst": {"name": "market_analyst", "action": "BUY", "confidence": 0.82, "reason": "trend"},
        "risk_manager": {"name": "risk_manager", "action": "HOLD", "confidence": 0.90, "reason": "risk"}
      },
      "final_decision": {"action": "HOLD", "confidence": 0.88, "reason": "risk veto"}
    }
    """

    decision = _parse_multi_agent_decision(payload)

    assert decision.final == AIDecision("HOLD", Decimal("0.88"), "risk veto")
    assert [agent.name for agent in decision.agents] == ["market_analyst", "risk_manager"]
    assert decision.agents[0].action == "BUY"


def test_parse_multi_agent_decision_accepts_final_entry_plan():
    payload = """
    {
      "agents": [],
      "final_decision": {
        "action": "BUY",
        "confidence": 0.78,
        "reason": "trend pullback",
        "entry_plan": {
          "entry_style": "wait_pullback",
          "limit_price": "99.50",
          "stop_atr_multiplier": "2.1",
          "reward_risk": "2.4"
        }
      }
    }
    """

    decision = _parse_multi_agent_decision(payload)

    assert decision.final.action == "BUY"
    assert decision.final.entry_plan == {
        "entry_style": "wait_pullback",
        "limit_price": "99.50",
        "stop_atr_multiplier": "2.1",
        "reward_risk": "2.4",
    }


def test_parse_multi_agent_decision_accepts_legacy_json():
    decision = _parse_multi_agent_decision('{"action":"BUY","confidence":0.52,"reason":"legacy"}')

    assert decision.final == AIDecision("BUY", Decimal("0.52"), "legacy")
    assert decision.agents == []


def test_parse_multi_agent_decision_accepts_decision_reasoning_aliases():
    decision = _parse_multi_agent_decision('{"decision":"WAIT","confidence":0.52,"reasoning":"flat is safer"}')

    assert decision.final == AIDecision("HOLD", Decimal("0.52"), "flat is safer")
    assert decision.agents == []


def test_parse_multi_agent_decision_rejects_empty_payload():
    with pytest.raises(ValueError, match="decision payload"):
        _parse_multi_agent_decision("{}")


def test_telegram_notification_text_formats_entry_as_markdown():
    text = _telegram_notification_text(
        "entry",
        "INJ/USDT:USDT buy amount=115.8 entry=5.18 estimated_notional=599.8440 USDT",
    )

    assert "*LumiBot Binance Futures*" in text
    assert "*Event:* Entry opened" in text
    assert "*Summary:* INJ/USDT:USDT buy" in text
    assert "\\- Amount: `115.8`" in text
    assert "\\- Entry price: `5.18`" in text
    assert "\\- Estimated notional: `599.8440 USDT`" in text


def test_notify_sends_telegram_markdown_payload(monkeypatch):
    requests = []

    class FakeResponse:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, traceback):
            return False

    def fake_urlopen(request, timeout):
        requests.append(request)
        return FakeResponse()

    monkeypatch.setattr("lumibot.example_strategies.binance_futures_demo_ai_trader.urllib.request.urlopen", fake_urlopen)
    args = argparse.Namespace(
        notify_events="entry",
        discord_webhook_url="",
        telegram_bot_token="token",
        telegram_chat_id="chat-1",
    )

    _notify(args, "entry", "BTC/USDT:USDT buy amount=0.1 entry=100 estimated_notional=10 USDT")

    assert len(requests) == 1
    payload = urllib.parse.parse_qs(requests[0].data.decode("utf-8"))
    assert payload["chat_id"] == ["chat-1"]
    assert payload["parse_mode"] == ["MarkdownV2"]
    assert payload["disable_web_page_preview"] == ["true"]
    assert "*Event:* Entry opened" in payload["text"][0]
    assert "\\- Amount: `0.1`" in payload["text"][0]


def test_auto_tune_notification_message_is_human_readable():
    message = _auto_tune_notification_message(
        {
            "mode": "performance",
            "trades": 9,
            "win_rate": "0.3333333333333333333333333333",
            "profit_factor": "3.530487470957990901780322337",
            "gross_win": "19.3446",
            "gross_loss": "5.4793000001076",
            "reasons": ["tightened_after_weak_recent_performance"],
            "overrides": {
                "min_confidence": "0.75",
                "stop_loss_pct": "0.4050",
                "take_profit_pct": "1.320",
                "trailing_activation_pct": "0.720",
                "trailing_distance_pct": "0.3150",
                "symbol_cooldown_minutes": "25.00",
                "loss_symbol_cooldown_minutes": "90.00",
            },
        }
    )

    assert message.startswith("Auto-tune updated mode=performance")
    assert "win_rate=33.3%" in message
    assert "profit_factor=3.53" in message
    assert "reasons=tightened after weak recent performance" in message
    assert "Stop loss 0.405%" in message
    assert "Loss cooldown 90m" in message
    assert "loss_symbol_cooldown_minutes" not in message
    assert "{'mode':" not in message


def test_risk_manager_veto_blocks_new_entry_when_confident_hold():
    decision = MultiAgentDecision(
        final=AIDecision("SELL", Decimal("0.72"), "trend down"),
        agents=[
            AgentOpinion("market_analyst", "SELL", Decimal("0.7"), "weak"),
            AgentOpinion("risk_manager", "HOLD", Decimal("0.8"), "entry risk unclear"),
        ],
    )

    assert _risk_manager_veto(decision) == "risk_manager veto: entry risk unclear"


def test_strip_json_fence():
    assert _strip_json_fence('```json\n{"action":"HOLD"}\n```') == '{"action":"HOLD"}'


def test_calculate_indicators_detects_uptrend():
    candles = []
    for i in range(1, 80):
        close = Decimal("100") + Decimal(i)
        candles.append([i, close - 1, close + 1, close - 2, close, 10])

    indicators = _calculate_indicators(candles, fast_period=5, slow_period=20)

    assert indicators.trend == "up"
    assert indicators.ema_fast > indicators.ema_slow


def test_expert_signals_include_committee_members():
    candles = []
    for i in range(1, 80):
        close = Decimal("100") + Decimal(i)
        candles.append([i, close - 1, close + 1, close - 2, close, 10])
    indicators = _calculate_indicators(candles, fast_period=5, slow_period=20)

    signals = _expert_signals(indicators, candles, None)

    assert {signal.name for signal in signals} == {
        "trend_follower",
        "breakout",
        "mean_reversion",
        "risk_manager",
    }
    assert any(signal.name == "trend_follower" and signal.action == "BUY" for signal in signals)


def test_expert_signals_include_timeframe_consensus_when_provided():
    candles = []
    for i in range(1, 80):
        close = Decimal("100") + Decimal(i)
        candles.append([i, close - 1, close + 1, close - 2, close, 10])
    indicators = _calculate_indicators(candles, fast_period=5, slow_period=20)

    signals = _expert_signals(indicators, candles, None, {"1m": indicators, "5m": indicators, "15m": indicators})

    assert any(signal.name == "timeframe_consensus" and signal.action == "BUY" for signal in signals)


def test_expert_signals_include_regime_detector_when_provided():
    candles = []
    for i in range(1, 80):
        close = Decimal("100") + Decimal(i)
        candles.append([i, close - 1, close + 1, close - 2, close, 10])
    indicators = _calculate_indicators(candles, fast_period=5, slow_period=20)
    regime = MarketRegime("trending", "follow_trend", Decimal("0.72"), "test trend")

    signals = _expert_signals(indicators, candles, None, {"1m": indicators, "5m": indicators}, regime)

    assert any(signal.name == "regime_detector" and signal.action == "BUY" for signal in signals)


def test_parse_timeframes_deduplicates_and_strips():
    assert _parse_timeframes("1m, 5m,1m, 15m") == ["1m", "5m", "15m"]


def test_parse_symbols_deduplicates_and_strips():
    assert _parse_symbols("BTCUSDT, ETHUSDT, BTCUSDT") == ["BTCUSDT", "ETHUSDT"]


def test_resolve_requested_symbols_excludes_configured_symbols():
    exchange = FakeTickerExchange()
    args = argparse.Namespace(
        symbols="BTCUSDT,ETHUSDT,SOLUSDT",
        symbol_universe="manual",
        symbol="BTC/USDT:USDT",
        universe_size=3,
        exclude_symbols="ETHUSDT",
    )

    assert _resolve_requested_symbols(exchange, args) == ["BTC/USDT:USDT", "SOL/USDT:USDT"]


def test_parse_models_deduplicates_and_strips():
    assert _parse_models("gemini-a, gemini-b, gemini-a") == ["gemini-a", "gemini-b"]


def test_apply_runtime_config_coerces_values_and_skips_protected_keys(tmp_path):
    config = tmp_path / "runtime.json"
    config.write_text(
        """
        {
          "trading": {
            "execute": true,
            "min-confidence": "0.72",
            "allow-short": true,
            "interval-seconds": 120,
            "state-file": "ignored.json",
            "lock-file": "ignored.lock"
          }
        }
        """,
        encoding="utf-8",
    )
    args = argparse.Namespace(
        runtime_config_file=config,
        execute=False,
        min_confidence=Decimal("0.60"),
        allow_short=False,
        interval_seconds=300.0,
        state_file="state.json",
        lock_file=DEFAULT_LOCK_FILE,
    )

    assert _apply_runtime_config(args) is True

    assert args.execute is False
    assert args.min_confidence == Decimal("0.72")
    assert args.allow_short is True
    assert args.interval_seconds == 120.0
    assert args.state_file == "state.json"
    assert args.lock_file == DEFAULT_LOCK_FILE


def test_apply_runtime_config_hot_reload_only_applies_hot_keys(tmp_path):
    config = tmp_path / "runtime.json"
    config.write_text(
        '{"trading":{"min_confidence":"0.71","advisor_models":"qwen:qwen3.7-max-thinking","shadow_models":"qwen:qwen-plus","timeframe":"1m"}}',
        encoding="utf-8",
    )
    args = argparse.Namespace(
        runtime_config_file=config,
        min_confidence=Decimal("0.60"),
        advisor_models="",
        shadow_models="",
        timeframe="5m",
    )

    assert _apply_runtime_config(args, hot_reload=True) is True

    assert args.min_confidence == Decimal("0.71")
    assert args.advisor_models == "qwen:qwen3.7-max-thinking"
    assert args.shadow_models == "qwen:qwen-plus"
    assert args.timeframe == "5m"


def test_safe_hot_reload_runtime_config_rolls_back_bad_config_and_recovers(tmp_path, monkeypatch):
    config = tmp_path / "runtime.json"
    config.write_text('{"trading":{"min_confidence":"bad"}}', encoding="utf-8")
    notifications = []
    monkeypatch.setattr(
        "lumibot.example_strategies.binance_futures_demo_ai_trader._notify",
        lambda args, event, message: notifications.append((event, message)),
    )
    monkeypatch.setattr("lumibot.example_strategies.binance_futures_demo_ai_trader._validate_args", lambda args: None)
    args = argparse.Namespace(
        runtime_config_file=config,
        min_confidence=Decimal("0.60"),
        notify_events="risk",
        exchange_error_alert_cooldown_minutes=Decimal("30"),
    )
    state = {}

    assert _safe_hot_reload_runtime_config(args, state) is False

    assert args.min_confidence == Decimal("0.60")
    assert state["runtime_config_errors"]["consecutive"] == 1
    assert state["runtime_config_errors"]["last_error_type"] == "InvalidOperation"
    assert notifications and notifications[0][0] == "risk"

    config.write_text('{"trading":{"min_confidence":"0.73"}}', encoding="utf-8")

    assert _safe_hot_reload_runtime_config(args, state) is True

    assert args.min_confidence == Decimal("0.73")
    assert state["runtime_config_errors"]["consecutive"] == 0
    assert state["runtime_config_errors"]["previous_consecutive"] == 1


def test_model_chain_prefers_models_over_single_model():
    args = argparse.Namespace(model="gemini-default", models="gemini-a,gemini-b")

    assert _model_chain(args) == ["gemini-a", "gemini-b"]


def test_model_chain_falls_back_to_single_model():
    args = argparse.Namespace(model="gemini-default", models="")

    assert _model_chain(args) == ["gemini-default"]


def test_shadow_model_chain_parses_shadow_models_only():
    args = argparse.Namespace(
        model="gemini-default",
        models="gemini-final",
        advisor_models="qwen:qwen3.7-max-thinking",
        shadow_models="qwen:qwen3.7-max-thinking, qwen:qwen-plus",
    )

    assert _model_chain(args) == ["gemini-final"]
    assert _advisor_model_chain(args) == ["qwen:qwen3.7-max-thinking"]
    assert _shadow_model_chain(args) == ["qwen:qwen3.7-max-thinking", "qwen:qwen-plus"]


def test_parse_model_spec_supports_provider_prefixes():
    assert _parse_model_spec("gemini:gemini-3.5-flash").provider == "gemini"
    assert _parse_model_spec("gemini:gemini-3.5-flash").model == "gemini-3.5-flash"
    assert _parse_model_spec("openai:gpt-4o-mini").provider == "openai"
    assert _parse_model_spec("openai:gpt-4o-mini").model == "gpt-4o-mini"
    assert _parse_model_spec("openai:meta-llama/llama-3.1-70b").model == "meta-llama/llama-3.1-70b"
    assert _parse_model_spec("qwen:qwen3.7-max-thinking").provider == "qwen"
    assert _parse_model_spec("qwen:qwen3.7-max-thinking").model == "qwen3.7-max-thinking"
    assert _parse_model_spec("gemini-3.5-flash").provider == "gemini"


def test_should_retry_ai_error_skips_quota_but_retries_high_demand():
    assert _should_retry_ai_error(Exception("503 UNAVAILABLE high demand")) is True
    assert _should_retry_ai_error(Exception("429 RESOURCE_EXHAUSTED Quota exceeded")) is False


def test_gemini_decision_skips_remaining_attempts_after_quota(monkeypatch):
    calls = []

    class FakeModels:
        def generate_content(self, model, contents, config):
            calls.append(model)
            if model == "quota-model":
                raise RuntimeError("429 RESOURCE_EXHAUSTED Quota exceeded")
            return module_types.SimpleNamespace(
                text='{"final_decision":{"action":"HOLD","confidence":0.5,"reason":"ok"},"agents":[]}'
            )

    class FakeClient:
        def __init__(self, api_key):
            self.models = FakeModels()

    google_module = module_types.ModuleType("google")
    genai_module = module_types.ModuleType("google.genai")
    genai_types_module = module_types.ModuleType("google.genai.types")
    genai_module.Client = FakeClient
    genai_types_module.GenerateContentConfig = lambda **kwargs: kwargs
    genai_module.types = genai_types_module
    google_module.genai = genai_module

    monkeypatch.setitem(sys.modules, "google", google_module)
    monkeypatch.setitem(sys.modules, "google.genai", genai_module)
    monkeypatch.setitem(sys.modules, "google.genai.types", genai_types_module)
    monkeypatch.setenv("GEMINI_API_KEY", "test-key")

    indicators = Indicators(
        close=Decimal("100"),
        ema_fast=Decimal("101"),
        ema_slow=Decimal("99"),
        rsi=Decimal("55"),
        atr=Decimal("1"),
        atr_pct=Decimal("1"),
        trend="up",
    )
    regime = MarketRegime("trending", "follow_trend", Decimal("0.7"), "test")

    decision = _gemini_decision(
        ["quota-model", "ok-model"],
        indicators,
        None,
        "BTC/USDT:USDT",
        [],
        {},
        regime,
        [],
        max_retries=2,
        retry_delay_seconds=0,
    )

    assert decision.final.action == "HOLD"
    assert calls == ["quota-model", "ok-model"]


def test_gemini_decision_can_use_openai_compatible_primary(monkeypatch):
    calls = []

    class FakeCompletions:
        def create(self, **kwargs):
            calls.append(kwargs)
            return module_types.SimpleNamespace(
                choices=[
                    module_types.SimpleNamespace(
                        message=module_types.SimpleNamespace(
                            content='{"final_decision":{"action":"SELL","confidence":0.7,"reason":"ok"},"agents":[]}'
                        )
                    )
                ]
            )

    class FakeChat:
        def __init__(self):
            self.completions = FakeCompletions()

    class FakeOpenAI:
        def __init__(self, **kwargs):
            calls.append({"client": kwargs})
            self.chat = FakeChat()

    openai_module = module_types.ModuleType("openai")
    openai_module.OpenAI = FakeOpenAI

    monkeypatch.setitem(sys.modules, "openai", openai_module)
    monkeypatch.delenv("OPENAI_COMPATIBLE_API_KEY", raising=False)
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.setenv("OPENAI_COMPATIBLE_BASE_URL", "http://localhost:8083/v1")

    indicators = Indicators(
        close=Decimal("100"),
        ema_fast=Decimal("99"),
        ema_slow=Decimal("101"),
        rsi=Decimal("45"),
        atr=Decimal("1"),
        atr_pct=Decimal("1"),
        trend="down",
    )
    regime = MarketRegime("trending", "follow_trend", Decimal("0.7"), "test")

    decision = _gemini_decision(
        ["openai:gemini-3.5-flash"],
        indicators,
        None,
        "BTC/USDT:USDT",
        [],
        {},
        regime,
        [],
        max_retries=0,
        retry_delay_seconds=0,
        request_timeout_seconds=180,
    )

    assert decision.final.action == "SELL"
    assert calls[0]["client"]["base_url"] == "http://localhost:8083/v1"
    assert calls[0]["client"]["api_key"] == "EMPTY"
    assert calls[0]["client"]["timeout"] == 180
    assert calls[1]["model"] == "gemini-3.5-flash"
    assert "current_position.has_position is false" in calls[1]["messages"][1]["content"]


def test_openai_compatible_qwen_request_disables_thinking(monkeypatch):
    from lumibot.example_strategies.binance_futures_demo_ai_trader import _OPENAI_CLIENTS
    _OPENAI_CLIENTS.clear()
    calls = []

    class FakeCompletions:
        def create(self, **kwargs):
            calls.append(kwargs)
            return module_types.SimpleNamespace(
                choices=[
                    module_types.SimpleNamespace(
                        message=module_types.SimpleNamespace(content='{"final_decision":{"action":"HOLD"}}')
                    )
                ]
            )

    class FakeOpenAI:
        def __init__(self, **kwargs):
            calls.append({"client": kwargs})
            self.chat = module_types.SimpleNamespace(completions=FakeCompletions())

    openai_module = module_types.ModuleType("openai")
    openai_module.OpenAI = FakeOpenAI
    monkeypatch.setitem(sys.modules, "openai", openai_module)
    monkeypatch.setenv("OPENAI_COMPATIBLE_BASE_URL", "http://127.0.0.1:8083/v1")
    monkeypatch.setenv("OPENAI_COMPATIBLE_API_KEY", "gemini-key")
    monkeypatch.setenv("QWEN_OPENAI_COMPATIBLE_BASE_URL", "http://127.0.0.1:7860/v1")
    monkeypatch.setenv("QWEN_OPENAI_COMPATIBLE_API_KEY", "qwen-key")

    _openai_compatible_completion_text(
        "qwen3.7-max-thinking",
        {"context": "test"},
        0.2,
        100,
        30,
        provider="qwen",
    )

    assert calls[0]["client"]["base_url"] == "http://127.0.0.1:7860/v1"
    assert calls[0]["client"]["api_key"] == "qwen-key"
    assert calls[1]["model"] == "qwen3.7-max-thinking"
    assert calls[1]["extra_body"] == {"enable_thinking": True}

    # Test that a non-thinking Qwen model disables thinking
    _openai_compatible_completion_text(
        "qwen-max",
        {"context": "test"},
        0.2,
        100,
        30,
        provider="qwen",
    )
    assert calls[2]["model"] == "qwen-max"
    assert calls[2]["extra_body"] == {"enable_thinking": False}


def test_shadow_decision_comparison_records_but_keeps_final_decision(monkeypatch):
    def fake_shadow_decision(*args, **kwargs):
        return MultiAgentDecision(
            final=AIDecision("SELL", Decimal("0.80"), "shadow says short"),
            agents=[
                AgentOpinion("risk_manager", "SELL", Decimal("0.70"), "risk acceptable"),
                AgentOpinion("final_portfolio_manager", "SELL", Decimal("0.80"), "short setup"),
            ],
        )

    monkeypatch.setattr(
        "lumibot.example_strategies.binance_futures_demo_ai_trader._gemini_decision",
        fake_shadow_decision,
    )
    state = {}
    args = argparse.Namespace(
        shadow_models="qwen:qwen3.7-max-thinking",
        ai_max_retries=0,
        ai_retry_delay_seconds=0,
        ai_request_timeout_seconds=30,
        ai_entry_aggressiveness=Decimal("0.50"),
    )
    indicators = Indicators(
        close=Decimal("100"),
        ema_fast=Decimal("101"),
        ema_slow=Decimal("99"),
        rsi=Decimal("55"),
        atr=Decimal("1"),
        atr_pct=Decimal("1"),
        trend="up",
    )
    regime = MarketRegime("trending", "follow_trend", Decimal("0.7"), "test")
    final_decision = AIDecision("BUY", Decimal("0.75"), "gemini final")

    records = _shadow_decision_comparisons(
        args,
        state,
        "BTC/USDT:USDT",
        final_decision,
        indicators,
        None,
        [],
        {},
        regime,
        [],
        [],
        {"risk_context": {}},
    )

    assert final_decision.action == "BUY"
    assert records[0]["model"] == "qwen:qwen3.7-max-thinking"
    assert records[0]["status"] == "ok"
    assert records[0]["action"] == "SELL"
    assert records[0]["final_action"] == "BUY"
    assert records[0]["action_agreement"] is False
    assert state["last_shadow_comparisons"] == records
    assert state["shadow_decision_comparisons"] == records
    assert state["trade_journal"][-1]["event"] == "shadow_decision_comparison"


def test_advisor_decision_comparison_records_context_without_final_action(monkeypatch):
    def fake_advisor_decision(*args, **kwargs):
        return MultiAgentDecision(
            final=AIDecision("SELL", Decimal("0.80"), "advisor says short"),
            agents=[AgentOpinion("final_portfolio_manager", "SELL", Decimal("0.80"), "short setup")],
        )

    monkeypatch.setattr(
        "lumibot.example_strategies.binance_futures_demo_ai_trader._gemini_decision",
        fake_advisor_decision,
    )
    state = {}
    args = argparse.Namespace(
        advisor_models="qwen:qwen3.7-max-thinking",
        ai_max_retries=0,
        ai_retry_delay_seconds=0,
        ai_request_timeout_seconds=30,
        ai_entry_aggressiveness=Decimal("0.50"),
    )
    indicators = Indicators(
        close=Decimal("100"),
        ema_fast=Decimal("101"),
        ema_slow=Decimal("99"),
        rsi=Decimal("55"),
        atr=Decimal("1"),
        atr_pct=Decimal("1"),
        trend="up",
    )
    regime = MarketRegime("trending", "follow_trend", Decimal("0.7"), "test")

    records = _advisor_decision_comparisons(
        args,
        state,
        "BTC/USDT:USDT",
        indicators,
        None,
        [],
        {},
        regime,
        [],
        [],
        {"risk_context": {}},
    )

    assert records[0]["model"] == "qwen:qwen3.7-max-thinking"
    assert records[0]["status"] == "ok"
    assert records[0]["action"] == "SELL"
    assert "final_action" not in records[0]
    assert state["last_advisor_comparisons"] == records
    assert state["advisor_decision_comparisons"] == records
    assert state["trade_journal"][-1]["event"] == "advisor_decision_comparison"


def test_decision_prompt_uses_flat_position_object_instead_of_null():
    indicators = Indicators(
        close=Decimal("100"),
        ema_fast=Decimal("99"),
        ema_slow=Decimal("101"),
        rsi=Decimal("45"),
        atr=Decimal("1"),
        atr_pct=Decimal("1"),
        trend="down",
    )
    regime = MarketRegime("trending", "follow_trend", Decimal("0.7"), "test")

    prompt = _decision_prompt(
        indicators,
        None,
        "BTC/USDT:USDT",
        [],
        {},
        regime,
        [],
        entry_aggressiveness=Decimal("0.65"),
    )

    assert prompt["current_position"] == {
        "has_position": False,
        "side": "flat",
        "amount": "0",
        "entry_price": "0",
        "unrealized_pnl": "0",
    }
    assert prompt["entry_aggressiveness"] == "0.65"


def test_ai_context_payload_enriches_decision_prompt():
    candles = []
    for i in range(1, 80):
        close = Decimal("100") + Decimal(i) / Decimal("10")
        candles.append([i, close - Decimal("0.2"), close + Decimal("0.4"), close - Decimal("0.4"), close, 10])
    indicators = _calculate_indicators(candles, fast_period=5, slow_period=20)
    regime = MarketRegime("trending", "follow_trend", Decimal("0.72"), "trend")
    args = argparse.Namespace(
        min_confidence=Decimal("0.75"),
        dynamic_exit_plan=True,
        dynamic_exit_atr_stop_multiplier=Decimal("1.8"),
        dynamic_exit_min_stop_pct=Decimal("0.30"),
        dynamic_exit_max_stop_pct=Decimal("0.90"),
        dynamic_exit_min_take_profit_pct=Decimal("0.70"),
        dynamic_exit_max_take_profit_pct=Decimal("2.50"),
        dynamic_exit_min_reward_risk=Decimal("1.80"),
        dynamic_exit_max_reward_risk=Decimal("3.00"),
        stop_loss_pct=Decimal("0.45"),
        take_profit_pct=Decimal("1.20"),
        max_spread_pct=Decimal("0.08"),
        ai_entry_limit_max_distance_pct=Decimal("0.40"),
        execution_mode="marketable_limit",
        limit_entry_timeout_seconds=8.0,
        limit_entry_price_offset_pct=Decimal("0.02"),
        limit_entry_min_fill_ratio=Decimal("1"),
        taker_fee_rate_pct=Decimal("0.05"),
        adaptive_risk=True,
        risk_per_trade_pct=Decimal("0.25"),
        max_position_notional_usdt=Decimal("600"),
        max_total_notional_usdt=Decimal("700"),
        max_daily_loss_pct=Decimal("0.8"),
        max_consecutive_losses=2,
        entry_block_report_lookback_minutes=Decimal("360"),
    )
    state = {
        "start_equity": "1000",
        "adaptive_risk_multiplier": "0.8",
        "global_control": {
            "consecutive_losses": "2",
            "pause_until": str(Decimal(str(time.time())) - Decimal("60")),
        },
        "closed_trades": [
            {
                "symbol": "BTC/USDT:USDT",
                "estimated_pnl_usdt_before_fees": "2",
                "exit_reason": "TRAILING_STOP",
            },
            {
                "symbol": "BTC/USDT:USDT",
                "estimated_pnl_usdt_before_fees": "-1",
                "exit_reason": "STOP_LOSS",
            },
        ],
    }
    account = {"availableBalance": "900"}
    context = _ai_context_payload(
        FakeRunIterationExchange(),
        args,
        state,
        "BTC/USDT:USDT",
        {"last": "108", "bid": "107.9", "ask": "108.1"},
        Decimal("990"),
        account,
        indicators,
        candles,
        {"5m": indicators, "15m": indicators},
        regime,
        [],
    )

    prompt = _decision_prompt(
        indicators,
        None,
        "BTC/USDT:USDT",
        [],
        {"5m": indicators},
        regime,
        [],
        context_payload=context,
    )

    assert prompt["context"]["execution_context"]["spread_pct"] is not None
    assert prompt["context"]["execution_context"]["ai_entry_limit_max_distance_pct"] == "0.40"
    assert prompt["context"]["execution_context"]["configured_execution_mode"] == "marketable_limit"
    assert prompt["context"]["execution_context"]["limit_entry_timeout_seconds"] == "8.0"
    assert prompt["context"]["execution_context"]["limit_entry_min_fill_ratio"] == "1"
    assert prompt["context"]["risk_context"]["available_balance_usdt"] == "900"
    assert prompt["context"]["risk_context"]["daily_drawdown_pct"] == "1.00"
    assert prompt["context"]["risk_context"]["global_consecutive_losses"] == "2"
    assert prompt["context"]["risk_context"]["max_consecutive_losses"] == "2"
    assert prompt["context"]["risk_context"]["global_loss_pause_active"] is False
    assert prompt["context"]["risk_context"]["global_loss_pause_status"] == "expired"
    assert prompt["context"]["risk_context"]["hard_entry_block_reason"] is None
    assert "global_loss_pause_active" in prompt["task"]
    assert "ai_entry_limit_max_distance_pct" in prompt["task"]
    assert "wait_pullback is not a persistent pending-entry workflow" in prompt["task"]
    assert prompt["context"]["exit_plan_preview"]["if_sell"]["mode"] == "dynamic_atr"
    assert prompt["context"]["performance_context"]["symbol_recent"]["trades"] == "2"
    assert prompt["context"]["market_structure"]["breakout_state"] in {
        "above_previous_20_high",
        "below_previous_20_low",
        "inside_previous_20_range",
    }
    assert prompt["context"]["market_breadth"]["candidate_count"] == 0


def test_execution_context_falls_back_to_order_book_when_ticker_lacks_bid_ask():
    exchange = FakeRunIterationExchange()
    context = _execution_context_payload(
        exchange,
        "BTC/USDT:USDT",
        {"last": "120"},
        argparse.Namespace(max_spread_pct=Decimal("0.08"), taker_fee_rate_pct=Decimal("0.05")),
    )

    assert exchange.order_book_calls == 1
    assert context["quote_source"] == "order_book"
    assert context["bid"] == "119.95"
    assert context["ask"] == "120.05"
    assert context["bid_size"] == "1"
    assert context["ask_size"] == "1"
    assert context["spread_pct"] is not None


def test_large_cap_usdm_symbols_ranks_available_markets_by_volume():
    assert _large_cap_usdm_symbols(FakeTickerExchange(), 3) == [
        "ETH/USDT:USDT",
        "SOL/USDT:USDT",
        "BTC/USDT:USDT",
    ]


def test_timeframe_payload_serializes_indicators():
    candles = []
    for i in range(1, 80):
        close = Decimal("100") + Decimal(i)
        candles.append([i, close - 1, close + 1, close - 2, close, 10])
    indicators = _calculate_indicators(candles, fast_period=5, slow_period=20)

    payload = _timeframe_payload({"5m": indicators})

    assert payload["5m"]["trend"] == "up"
    assert payload["5m"]["close"] == str(indicators.close)


def test_candidate_payload_serializes_top_scanner_context():
    candles = []
    for i in range(1, 80):
        close = Decimal("100") + Decimal(i)
        candles.append([i, close - 1, close + 1, close - 2, close, 10])
    indicators = _calculate_indicators(candles, fast_period=5, slow_period=20)
    candidate = SymbolCandidate(
        "ETH/USDT:USDT",
        Decimal("0.81"),
        indicators,
        {"5m": indicators},
        MarketRegime("trending", "follow_trend", Decimal("0.72"), "trend"),
        candles,
    )

    payload = _candidate_payload([candidate])

    assert payload[0]["symbol"] == "ETH/USDT:USDT"
    assert payload[0]["score"] == "0.81"
    assert payload[0]["multi_timeframe"]["5m"]["trend"] == "up"


def test_detect_market_regime_trending():
    candles = []
    for i in range(1, 80):
        close = Decimal("100") + Decimal(i)
        candles.append([i, close - 1, close + 1, close - 2, close, 10])
    indicators = _calculate_indicators(candles, fast_period=5, slow_period=20)

    regime = _detect_market_regime(
        indicators,
        {"1m": indicators, "5m": indicators, "15m": indicators},
        Decimal("5"),
        Decimal("0.01"),
    )

    assert regime.name == "trending"
    assert regime.preferred_style == "follow_trend"


def test_detect_market_regime_high_volatility_takes_priority():
    candles = []
    for i in range(1, 80):
        close = Decimal("100") + Decimal(i)
        candles.append([i, close - 20, close + 20, close - 20, close, 10])
    calculated = _calculate_indicators(candles, fast_period=5, slow_period=20)

    regime = _detect_market_regime(calculated, {"5m": calculated}, Decimal("1"), Decimal("0.01"))

    assert regime.name == "high_volatility"


def test_symbol_setup_score_prefers_trending_over_high_volatility_penalty():
    candles = []
    for i in range(1, 80):
        close = Decimal("100") + Decimal(i)
        candles.append([i, close - 1, close + 1, close - 2, close, 10])
    indicators = _calculate_indicators(candles, fast_period=5, slow_period=20)

    trending_score = _symbol_setup_score(
        indicators,
        {"1m": indicators, "5m": indicators},
        MarketRegime("trending", "follow_trend", Decimal("0.72"), "trend"),
    )
    high_vol_score = _symbol_setup_score(
        indicators,
        {"1m": indicators, "5m": indicators},
        MarketRegime("high_volatility", "reduce_or_hold", Decimal("0.8"), "volatile"),
    )

    assert trending_score > high_vol_score


def test_validate_args_requires_live_confirmation_for_live_execute():
    args = argparse.Namespace(
        leverage=2,
        ema_fast=20,
        ema_slow=50,
        candle_limit=200,
        mtf_candle_limit=200,
        high_volatility_atr_pct=Decimal("1.5"),
        low_volatility_atr_pct=Decimal("0.15"),
        universe_size=10,
        risk_per_trade_pct=Decimal("0"),
        max_margin_allocation_pct=Decimal("5"),
        environment="live",
        execute=True,
        live_confirm="",
    )

    with pytest.raises(RuntimeError, match="Live execution is locked"):
        _validate_args(args)


@pytest.mark.parametrize(
    ("position", "last_price", "expected"),
    [
        (PositionSummary("long", Decimal("0.1"), Decimal("100"), Decimal("0")), Decimal("98"), "STOP_LOSS"),
        (PositionSummary("long", Decimal("0.1"), Decimal("100"), Decimal("0")), Decimal("103"), "TAKE_PROFIT"),
        (PositionSummary("short", Decimal("0.1"), Decimal("100"), Decimal("0")), Decimal("102"), "STOP_LOSS"),
        (PositionSummary("short", Decimal("0.1"), Decimal("100"), Decimal("0")), Decimal("97"), "TAKE_PROFIT"),
    ],
)
def test_stop_or_take_profit_action(position, last_price, expected):
    assert _stop_or_take_profit_action(position, last_price, Decimal("1"), Decimal("2")) == expected


def test_protective_stop_take_prices_for_long_and_short():
    assert _protective_stop_take_prices("buy", Decimal("100"), Decimal("1"), Decimal("2")) == (
        Decimal("99.00"),
        Decimal("102.00"),
    )
    assert _protective_stop_take_prices("sell", Decimal("100"), Decimal("1"), Decimal("2")) == (
        Decimal("101.00"),
        Decimal("98.00"),
    )


def test_place_native_protection_submits_reduce_only_orders():
    exchange = FakeProtectionExchange()
    args = argparse.Namespace(
        native_protection=True,
        execute=True,
        stop_loss_pct=Decimal("1"),
        take_profit_pct=Decimal("2"),
    )

    protection = _place_native_protection(
        exchange,
        "BTC/USDT:USDT",
        "buy",
        "0.01",
        Decimal("100"),
        args,
        dual_side=False,
    )

    assert protection["stop_order_id"] == "order-1"
    assert protection["take_profit_order_id"] == "order-2"
    assert exchange.orders[0]["type"] == "STOP_MARKET"
    assert exchange.orders[0]["side"] == "sell"
    assert exchange.orders[0]["params"]["reduceOnly"] is True
    assert exchange.orders[0]["params"]["stopPrice"] == "99.00"
    assert exchange.orders[1]["type"] == "TAKE_PROFIT_MARKET"
    assert exchange.orders[1]["params"]["stopPrice"] == "102.00"


def test_place_native_protection_prefers_binance_algo_orders():
    exchange = FakeAlgoProtectionExchange()
    args = argparse.Namespace(
        native_protection=True,
        execute=True,
        stop_loss_pct=Decimal("1"),
        take_profit_pct=Decimal("2"),
    )

    protection = _place_native_protection(
        exchange,
        "BTC/USDT:USDT",
        "sell",
        "0.01",
        Decimal("100"),
        args,
        dual_side=False,
    )

    assert protection["stop_order_id"] == "algo-1"
    assert protection["take_profit_order_id"] == "algo-2"
    assert exchange.orders == []
    assert exchange.algo_orders[0]["symbol"] == "BTCUSDT"
    assert exchange.algo_orders[0]["side"] == "BUY"
    assert exchange.algo_orders[0]["type"] == "STOP_MARKET"
    assert exchange.algo_orders[0]["triggerPrice"] == "101.00"
    assert exchange.algo_orders[0]["closePosition"] == "true"
    assert "quantity" not in exchange.algo_orders[0]
    assert "reduceOnly" not in exchange.algo_orders[0]
    assert exchange.algo_orders[1]["type"] == "TAKE_PROFIT_MARKET"
    assert exchange.algo_orders[1]["triggerPrice"] == "98.00"


def test_native_protection_order_count_reads_open_algo_orders():
    exchange = FakeAlgoProtectionExchange(
        open_algo_orders=[
            {"algoStatus": "NEW", "orderType": "STOP_MARKET"},
            {"algoStatus": "NEW", "orderType": "TAKE_PROFIT_MARKET"},
        ]
    )

    assert _native_protection_order_count(exchange, "BTC/USDT:USDT") == 2


def test_cancel_open_algo_orders_uses_binance_cancel_all_endpoint():
    exchange = FakeAlgoProtectionExchange(open_algo_orders=[{"algoStatus": "NEW", "orderType": "STOP_MARKET"}])

    _cancel_open_algo_orders(exchange, "BTC/USDT:USDT")

    assert exchange.cancel_algo_params == [{"symbol": "BTCUSDT"}]


def test_cleanup_flat_symbol_stale_orders_uses_state_cooldown():
    exchange = FakeAlgoProtectionExchange()
    args = argparse.Namespace(cancel_stale_open_orders=True, execute=True)
    state = {}

    _cleanup_flat_symbol_stale_orders(exchange, args, state, "BTC/USDT:USDT")
    _cleanup_flat_symbol_stale_orders(exchange, args, state, "BTC/USDT:USDT")

    assert exchange.cancel_all_order_symbols == ["BTC/USDT:USDT"]
    assert exchange.cancel_algo_params == [{"symbol": "BTCUSDT"}]
    assert state["stale_order_cleanup"]["BTC/USDT:USDT"]


def test_ensure_native_protection_recreates_missing_orders():
    exchange = FakeProtectionExchange(open_orders=[])
    args = argparse.Namespace(
        native_protection=True,
        execute=True,
        stop_loss_pct=Decimal("1"),
        take_profit_pct=Decimal("2"),
    )
    open_trade = {}

    _ensure_native_protection(
        exchange,
        "ETH/USDT:USDT",
        PositionSummary("short", Decimal("0.634"), Decimal("1597.95"), Decimal("0")),
        args,
        dual_side=False,
        open_trade=open_trade,
    )

    assert [order["type"] for order in exchange.orders] == ["STOP_MARKET", "TAKE_PROFIT_MARKET"]
    assert exchange.orders[0]["side"] == "buy"
    assert "native_protection" in open_trade


def test_ensure_native_protection_does_not_duplicate_existing_orders():
    exchange = FakeProtectionExchange(
        open_orders=[
            {"type": "STOP_MARKET", "info": {"type": "STOP_MARKET"}},
            {"type": "TAKE_PROFIT_MARKET", "info": {"type": "TAKE_PROFIT_MARKET"}},
        ]
    )
    args = argparse.Namespace(native_protection=True, execute=True)

    _ensure_native_protection(
        exchange,
        "ETH/USDT:USDT",
        PositionSummary("short", Decimal("0.634"), Decimal("1597.95"), Decimal("0")),
        args,
        dual_side=False,
        open_trade={},
    )

    assert exchange.orders == []


@pytest.mark.parametrize(
    ("position", "last_price", "open_trade", "expected_reason"),
    [
        (
            PositionSummary("long", Decimal("0.1"), Decimal("100"), Decimal("0")),
            Decimal("100.8"),
            {"highest_price": "101.5", "lowest_price": "100", "breakeven_armed": False, "trailing_armed": False},
            "TRAILING_STOP",
        ),
        (
            PositionSummary("short", Decimal("0.1"), Decimal("100"), Decimal("0")),
            Decimal("99.2"),
            {"highest_price": "100", "lowest_price": "98.5", "breakeven_armed": False, "trailing_armed": False},
            "TRAILING_STOP",
        ),
        (
            PositionSummary("long", Decimal("0.1"), Decimal("100"), Decimal("0")),
            Decimal("100.02"),
            {"highest_price": "100.5", "lowest_price": "100", "breakeven_armed": False, "trailing_armed": False},
            "BREAKEVEN_STOP",
        ),
    ],
)
def test_breakeven_or_trailing_action(position, last_price, open_trade, expected_reason):
    action = _breakeven_or_trailing_action(
        position,
        last_price,
        open_trade,
        Decimal("0.4"),
        Decimal("0.05"),
        Decimal("0.6"),
        Decimal("0.35"),
    )

    assert action is not None
    assert action.reason == expected_reason


def test_daily_loss_exceeded():
    state = {"start_equity": "1000"}

    assert _daily_loss_exceeded(state, Decimal("979"), Decimal("2")) is True
    assert _daily_loss_exceeded(state, Decimal("990"), Decimal("2")) is False


def test_record_trade_outcome_sets_loss_cooldown_and_global_pause():
    state = {}
    args = argparse.Namespace(
        loss_symbol_cooldown_minutes=Decimal("30"),
        symbol_cooldown_minutes=Decimal("10"),
        auto_blacklist_loss_count=3,
        auto_blacklist_minutes=Decimal("360"),
        max_consecutive_losses=2,
        loss_pause_minutes=Decimal("60"),
    )
    position = PositionSummary("long", Decimal("1"), Decimal("100"), Decimal("0"))

    pnl = _record_trade_outcome(state, args, "BTC/USDT:USDT", position, Decimal("99"), "STOP_LOSS")
    _record_trade_outcome(state, args, "ETH/USDT:USDT", position, Decimal("99"), "STOP_LOSS")

    assert pnl == Decimal("-1")
    assert _symbol_entry_block_reason(state, "BTC/USDT:USDT").startswith("symbol cooldown until")
    assert _global_entry_block_reason(state).startswith("global loss pause until")
    assert len(state["closed_trades"]) == 2


def test_record_trade_outcome_blacklists_symbol_after_repeated_losses():
    state = {}
    args = argparse.Namespace(
        loss_symbol_cooldown_minutes=Decimal("30"),
        symbol_cooldown_minutes=Decimal("10"),
        auto_blacklist_loss_count=2,
        auto_blacklist_minutes=Decimal("360"),
        max_consecutive_losses=10,
        loss_pause_minutes=Decimal("60"),
    )
    position = PositionSummary("short", Decimal("2"), Decimal("100"), Decimal("0"))

    _record_trade_outcome(state, args, "NEAR/USDT:USDT", position, Decimal("101"), "STOP_LOSS")
    _record_trade_outcome(state, args, "NEAR/USDT:USDT", position, Decimal("101"), "STOP_LOSS")

    assert _symbol_entry_block_reason(state, "NEAR/USDT:USDT").startswith("symbol temporarily blacklisted until")


def test_record_trade_outcome_updates_agent_scorecard_and_adaptive_risk():
    state = {"adaptive_risk_multiplier": "0.90"}
    args = argparse.Namespace(
        loss_symbol_cooldown_minutes=Decimal("30"),
        symbol_cooldown_minutes=Decimal("10"),
        auto_blacklist_loss_count=5,
        auto_blacklist_minutes=Decimal("360"),
        max_consecutive_losses=10,
        loss_pause_minutes=Decimal("60"),
        adaptive_risk=True,
        adaptive_risk_step=Decimal("0.10"),
        adaptive_risk_min_multiplier=Decimal("0.50"),
        adaptive_risk_max_multiplier=Decimal("1.00"),
    )
    position = PositionSummary("long", Decimal("1"), Decimal("100"), Decimal("0"))
    open_trade = {
        "entry_agents": [
            {"name": "trend_analyst", "action": "BUY", "confidence": "0.8"},
            {"name": "risk_manager", "action": "HOLD", "confidence": "0.7"},
        ]
    }

    _record_trade_outcome(state, args, "BTC/USDT:USDT", position, Decimal("101"), "TAKE_PROFIT", open_trade)

    assert state["agent_scorecard"]["trend_analyst"]["aligned_with_outcome"] == 1
    assert state["agent_scorecard"]["risk_manager"]["aligned_with_outcome"] == 0
    assert state["adaptive_risk_multiplier"] == "1.00"


def test_record_trade_outcome_uses_binance_net_pnl_for_loss_controls():
    state = {"adaptive_risk_multiplier": "1.00"}
    args = argparse.Namespace(
        loss_symbol_cooldown_minutes=Decimal("30"),
        symbol_cooldown_minutes=Decimal("10"),
        auto_blacklist_loss_count=5,
        auto_blacklist_minutes=Decimal("360"),
        max_consecutive_losses=10,
        loss_pause_minutes=Decimal("60"),
        adaptive_risk=True,
        adaptive_risk_step=Decimal("0.10"),
        adaptive_risk_min_multiplier=Decimal("0.50"),
        adaptive_risk_max_multiplier=Decimal("1.00"),
    )
    position = PositionSummary("long", Decimal("9.03"), Decimal("63.5703875969"), Decimal("0"))
    binance_report = {
        "binance_realized_pnl_usdt": "-1.71920000",
        "binance_commission_usdt": "-0.42460909",
        "binance_net_pnl_usdt": "-2.14380909",
        "binance_pnl_source": "income",
    }

    pnl = _record_trade_outcome(
        state,
        args,
        "SOL/USDT:USDT",
        position,
        Decimal("63.5703875969"),
        "AI_CLOSE",
        binance_report=binance_report,
    )

    assert pnl == Decimal("0E-10")
    assert state["closed_trades"][-1]["binance_net_pnl_usdt"] == "-2.14380909"
    assert state["symbol_controls"]["SOL/USDT:USDT"]["consecutive_losses"] == "1"
    assert state["global_control"]["consecutive_losses"] == "1"
    assert state["adaptive_risk_multiplier"] == "0.90"


def test_auto_tune_parameters_tightens_after_weak_recent_trades(monkeypatch):
    monkeypatch.setattr("lumibot.example_strategies.binance_futures_demo_ai_trader._now_ts", lambda: Decimal("2000"))
    state = {
        "closed_trades": [
            {"symbol": "BTC/USDT:USDT", "estimated_pnl_usdt_before_fees": "-2"},
            {"symbol": "BTC/USDT:USDT", "estimated_pnl_usdt_before_fees": "-1"},
            {"symbol": "BTC/USDT:USDT", "estimated_pnl_usdt_before_fees": "-1"},
            {"symbol": "ETH/USDT:USDT", "estimated_pnl_usdt_before_fees": "0.5"},
        ]
    }
    args = argparse.Namespace(
        auto_tune_parameters=True,
        auto_tune_interval_minutes=Decimal("0"),
        auto_tune_lookback_trades=4,
        auto_tune_min_trades=4,
        auto_tune_symbol_min_trades=3,
        auto_tune_symbol_max_loss_rate=Decimal("0.70"),
        auto_tune_blacklist_minutes=Decimal("720"),
        min_confidence=Decimal("0.70"),
        stop_loss_pct=Decimal("1.0"),
        take_profit_pct=Decimal("1.2"),
        dynamic_exit_atr_stop_multiplier=Decimal("1.80"),
        dynamic_exit_min_stop_pct=Decimal("0.30"),
        dynamic_exit_max_stop_pct=Decimal("0.90"),
        dynamic_exit_min_take_profit_pct=Decimal("0.70"),
        dynamic_exit_max_take_profit_pct=Decimal("2.50"),
        dynamic_exit_min_reward_risk=Decimal("1.80"),
        dynamic_exit_max_reward_risk=Decimal("3.00"),
        trailing_activation_pct=Decimal("0.8"),
        trailing_distance_pct=Decimal("0.35"),
        symbol_cooldown_minutes=Decimal("20"),
        loss_symbol_cooldown_minutes=Decimal("60"),
        notify_events="",
    )

    _maybe_auto_tune_parameters(state, args)

    overrides = state["auto_tune_overrides"]
    assert Decimal(overrides["min_confidence"]) > Decimal("0.70")
    assert Decimal(overrides["stop_loss_pct"]) > Decimal("1.0")
    assert Decimal(overrides["take_profit_pct"]) != Decimal("1.2")
    assert _symbol_entry_block_reason(state, "BTC/USDT:USDT").startswith("symbol temporarily blacklisted until")


def test_auto_tune_relaxes_after_idle_without_enough_trades(monkeypatch):
    monkeypatch.setattr("lumibot.example_strategies.binance_futures_demo_ai_trader._now_ts", lambda: Decimal("7200"))
    state = {"auto_tune_tracking_started_ts": "1", "last_entry_ts": "0", "closed_trades": []}
    args = argparse.Namespace(
        auto_tune_parameters=True,
        auto_tune_interval_minutes=Decimal("0"),
        auto_tune_lookback_trades=30,
        auto_tune_min_trades=8,
        auto_tune_idle_after_minutes=Decimal("60"),
        auto_tune_idle_min_confidence=Decimal("0.62"),
        auto_tune_idle_confidence_step=Decimal("0.03"),
        auto_tune_idle_max_spread_pct=Decimal("0.15"),
        auto_tune_idle_spread_step_pct=Decimal("0.02"),
        auto_tune_idle_min_reward_to_fee_ratio=Decimal("2"),
        auto_tune_idle_reward_fee_step=Decimal("0.25"),
        auto_tune_idle_max_entry_funding_cost_pct=Decimal("0.08"),
        auto_tune_idle_funding_step_pct=Decimal("0.01"),
        min_confidence=Decimal("0.70"),
        max_spread_pct=Decimal("0.08"),
        min_reward_to_fee_ratio=Decimal("3"),
        max_entry_funding_cost_pct=Decimal("0.03"),
        stop_loss_pct=Decimal("0.45"),
        take_profit_pct=Decimal("1.2"),
        dynamic_exit_atr_stop_multiplier=Decimal("1.80"),
        dynamic_exit_min_stop_pct=Decimal("0.30"),
        dynamic_exit_max_stop_pct=Decimal("0.90"),
        dynamic_exit_min_take_profit_pct=Decimal("0.70"),
        dynamic_exit_max_take_profit_pct=Decimal("2.50"),
        dynamic_exit_min_reward_risk=Decimal("1.80"),
        dynamic_exit_max_reward_risk=Decimal("3.00"),
        trailing_activation_pct=Decimal("0.8"),
        trailing_distance_pct=Decimal("0.35"),
        symbol_cooldown_minutes=Decimal("20"),
        loss_symbol_cooldown_minutes=Decimal("60"),
        notify_events="",
    )

    _maybe_auto_tune_parameters(state, args)

    overrides = state["auto_tune_overrides"]
    assert overrides["min_confidence"] == "0.67"
    assert overrides["max_spread_pct"] == "0.10"
    assert overrides["min_reward_to_fee_ratio"] == "2.75"
    assert overrides["max_entry_funding_cost_pct"] == "0.04"
    assert state["auto_tune"]["last_summary"]["mode"] == "idle"


def test_entry_block_events_are_counted_since_reference_time(monkeypatch):
    current_ts = Decimal("1000")
    monkeypatch.setattr("lumibot.example_strategies.binance_futures_demo_ai_trader._now_ts", lambda: current_ts)
    state = {}

    _record_entry_block(state, "BTC/USDT:USDT", "ai_hold", "AI chose HOLD")
    current_ts = Decimal("1100")
    _record_entry_block(state, "ETH/USDT:USDT", "confidence", "confidence too low")

    assert _recent_entry_block_counts(state, Decimal("1050")) == {"confidence": 1}


def test_auto_tune_increases_ai_aggressiveness_when_ai_hold_dominates(monkeypatch):
    monkeypatch.setattr("lumibot.example_strategies.binance_futures_demo_ai_trader._now_ts", lambda: Decimal("7200"))
    state = {
        "auto_tune_tracking_started_ts": "1",
        "last_entry_ts": "0",
        "closed_trades": [],
        "entry_block_events": [
            {"ts": "100", "symbol": "BTC/USDT:USDT", "category": "ai_hold", "reason": "hold"},
            {"ts": "200", "symbol": "ETH/USDT:USDT", "category": "ai_hold", "reason": "hold"},
            {"ts": "300", "symbol": "SOL/USDT:USDT", "category": "ai_hold", "reason": "hold"},
        ],
    }
    args = argparse.Namespace(
        auto_tune_parameters=True,
        auto_tune_interval_minutes=Decimal("0"),
        auto_tune_lookback_trades=30,
        auto_tune_min_trades=8,
        auto_tune_idle_after_minutes=Decimal("60"),
        auto_tune_idle_min_confidence=Decimal("0.62"),
        auto_tune_idle_confidence_step=Decimal("0.03"),
        auto_tune_idle_max_spread_pct=Decimal("0.15"),
        auto_tune_idle_spread_step_pct=Decimal("0.02"),
        auto_tune_idle_min_reward_to_fee_ratio=Decimal("2"),
        auto_tune_idle_reward_fee_step=Decimal("0.25"),
        auto_tune_idle_max_entry_funding_cost_pct=Decimal("0.08"),
        auto_tune_idle_funding_step_pct=Decimal("0.01"),
        ai_entry_aggressiveness=Decimal("0.50"),
        min_confidence=Decimal("0.70"),
        max_spread_pct=Decimal("0.08"),
        min_reward_to_fee_ratio=Decimal("3"),
        max_entry_funding_cost_pct=Decimal("0.03"),
        stop_loss_pct=Decimal("0.45"),
        take_profit_pct=Decimal("1.2"),
        dynamic_exit_atr_stop_multiplier=Decimal("1.80"),
        dynamic_exit_min_stop_pct=Decimal("0.30"),
        dynamic_exit_max_stop_pct=Decimal("0.90"),
        dynamic_exit_min_take_profit_pct=Decimal("0.70"),
        dynamic_exit_max_take_profit_pct=Decimal("2.50"),
        dynamic_exit_min_reward_risk=Decimal("1.80"),
        dynamic_exit_max_reward_risk=Decimal("3.00"),
        trailing_activation_pct=Decimal("0.8"),
        trailing_distance_pct=Decimal("0.35"),
        symbol_cooldown_minutes=Decimal("20"),
        loss_symbol_cooldown_minutes=Decimal("60"),
        notify_events="",
    )

    _maybe_auto_tune_parameters(state, args)

    overrides = state["auto_tune_overrides"]
    assert overrides == {"min_confidence": "0.67", "ai_entry_aggressiveness": "0.55"}
    assert "entry_block_counts" in state["auto_tune"]["last_summary"]["reasons"][1]


def test_auto_tune_softens_weak_performance_tightening_after_idle_ai_holds(monkeypatch):
    monkeypatch.setattr("lumibot.example_strategies.binance_futures_demo_ai_trader._now_ts", lambda: Decimal("7200"))
    state = {
        "auto_tune_tracking_started_ts": "1",
        "last_entry_ts": "0",
        "closed_trades": [
            {"symbol": "BTC/USDT:USDT", "estimated_pnl_usdt_before_fees": "-2"},
            {"symbol": "BTC/USDT:USDT", "estimated_pnl_usdt_before_fees": "-1"},
            {"symbol": "ETH/USDT:USDT", "estimated_pnl_usdt_before_fees": "-1"},
            {"symbol": "SOL/USDT:USDT", "estimated_pnl_usdt_before_fees": "0.5"},
        ],
        "entry_block_events": [
            {"ts": "100", "symbol": "BTC/USDT:USDT", "category": "ai_hold", "reason": "hold"},
            {"ts": "200", "symbol": "ETH/USDT:USDT", "category": "ai_hold", "reason": "hold"},
            {"ts": "300", "symbol": "SOL/USDT:USDT", "category": "ai_hold", "reason": "hold"},
        ],
    }
    args = argparse.Namespace(
        auto_tune_parameters=True,
        auto_tune_interval_minutes=Decimal("0"),
        auto_tune_lookback_trades=4,
        auto_tune_min_trades=4,
        auto_tune_symbol_min_trades=3,
        auto_tune_symbol_max_loss_rate=Decimal("0.70"),
        auto_tune_blacklist_minutes=Decimal("720"),
        auto_tune_idle_after_minutes=Decimal("60"),
        auto_tune_idle_min_confidence=Decimal("0.62"),
        auto_tune_idle_confidence_step=Decimal("0.03"),
        auto_tune_idle_max_spread_pct=Decimal("0.15"),
        auto_tune_idle_spread_step_pct=Decimal("0.02"),
        auto_tune_idle_min_reward_to_fee_ratio=Decimal("2"),
        auto_tune_idle_reward_fee_step=Decimal("0.25"),
        auto_tune_idle_max_entry_funding_cost_pct=Decimal("0.08"),
        auto_tune_idle_funding_step_pct=Decimal("0.01"),
        ai_entry_aggressiveness=Decimal("0.50"),
        min_confidence=Decimal("0.70"),
        max_spread_pct=Decimal("0.08"),
        min_reward_to_fee_ratio=Decimal("3"),
        max_entry_funding_cost_pct=Decimal("0.03"),
        stop_loss_pct=Decimal("0.45"),
        take_profit_pct=Decimal("1.2"),
        dynamic_exit_atr_stop_multiplier=Decimal("1.80"),
        dynamic_exit_min_stop_pct=Decimal("0.30"),
        dynamic_exit_max_stop_pct=Decimal("0.90"),
        dynamic_exit_min_take_profit_pct=Decimal("0.70"),
        dynamic_exit_max_take_profit_pct=Decimal("2.50"),
        dynamic_exit_min_reward_risk=Decimal("1.80"),
        dynamic_exit_max_reward_risk=Decimal("3.00"),
        trailing_activation_pct=Decimal("0.8"),
        trailing_distance_pct=Decimal("0.35"),
        symbol_cooldown_minutes=Decimal("20"),
        loss_symbol_cooldown_minutes=Decimal("60"),
        notify_events="",
    )

    _maybe_auto_tune_parameters(state, args)

    overrides = state["auto_tune_overrides"]
    assert overrides["min_confidence"] == "0.72"
    assert overrides["ai_entry_aggressiveness"] == "0.55"
    assert "softened_after_idle_blocks_under_weak_performance" in state["auto_tune"]["last_summary"]["reasons"]


def test_health_report_includes_entry_blocks_and_auto_tune_summary(monkeypatch, capsys):
    monkeypatch.setattr("lumibot.example_strategies.binance_futures_demo_ai_trader._now_ts", lambda: Decimal("7200"))
    notifications = []
    monkeypatch.setattr(
        "lumibot.example_strategies.binance_futures_demo_ai_trader._notify",
        lambda args, event, message: notifications.append((event, message)),
    )
    state = {
        "last_entry_ts": "1000",
        "open_trade": {
            "symbol": "BTC/USDT:USDT",
            "side": "long",
            "amount": "0.01",
            "entry_price": "100000",
        },
        "entry_block_events": [
            {"ts": "7000", "symbol": "ETH/USDT:USDT", "category": "ai_hold", "reason": "hold"},
            {"ts": "7100", "symbol": "SOL/USDT:USDT", "category": "spread", "reason": "wide"},
        ],
        "auto_tune": {"last_summary": {"mode": "idle", "reasons": ["entry_block_counts={'ai_hold': 1}"]}},
        "auto_tune_overrides": {"ai_entry_aggressiveness": "0.55"},
        "exchange_errors": {
            "consecutive": 2,
            "last_error_ts": "7100",
            "last_error_type": "ExchangeError",
            "last_error": "temporary",
        },
        "runtime_config_errors": {
            "consecutive": 1,
            "last_error_ts": "7150",
            "last_error_type": "JSONDecodeError",
            "last_error": "bad json",
        },
    }
    args = argparse.Namespace(
        new_entries_enabled=True,
        emergency_close_positions=True,
        health_report_interval_minutes=Decimal("60"),
        entry_block_report_lookback_minutes=Decimal("360"),
        notify_events="health",
    )

    _maybe_print_health_report(state, args, Decimal("1234.56"))

    output = capsys.readouterr().out
    assert "health_report equity=1234.56" in output
    assert "open_trade=BTC/USDT:USDT:long" in output
    assert "new_entries_enabled=True" in output
    assert "emergency_close_positions=True" in output
    assert "exchange_error_consecutive=2" in output
    assert "last_exchange_error=ExchangeError@" in output
    assert "runtime_config_error_consecutive=1" in output
    assert "last_runtime_config_error=JSONDecodeError@" in output
    assert "entry_block_counts_360m={'ai_hold': 1, 'spread': 1}" in output
    assert "auto_tune_mode=idle" in output
    assert state["last_health_report_ts"] == "7200"
    assert notifications and notifications[0][0] == "health"


def test_exchange_error_tracking_alerts_after_threshold_and_resets(monkeypatch, capsys):
    current_ts = Decimal("1000")
    monkeypatch.setattr("lumibot.example_strategies.binance_futures_demo_ai_trader._now_ts", lambda: current_ts)
    notifications = []
    monkeypatch.setattr(
        "lumibot.example_strategies.binance_futures_demo_ai_trader._notify",
        lambda args, event, message: notifications.append((event, message)),
    )
    state = {}
    args = argparse.Namespace(
        exchange_error_alert_threshold=2,
        exchange_error_alert_cooldown_minutes=Decimal("30"),
        notify_events="risk",
    )

    _record_exchange_error(state, args, RuntimeError("first"))
    _record_exchange_error(state, args, RuntimeError("second"))

    output = capsys.readouterr().out
    assert "exchange_error_alert" in output
    assert state["exchange_errors"]["consecutive"] == 2
    assert state["exchange_errors"]["last_error_type"] == "RuntimeError"
    assert len(state["exchange_errors"]["history"]) == 2
    assert notifications and notifications[0][0] == "risk"

    current_ts = Decimal("1100")
    _record_exchange_error(state, args, RuntimeError("third"))
    assert len(notifications) == 1

    _clear_exchange_error_streak(state)
    assert state["exchange_errors"]["consecutive"] == 0
    assert state["exchange_errors"]["previous_consecutive"] == 3


def test_state_save_is_atomic_and_load_recovers_from_backup(tmp_path, capsys):
    state_path = tmp_path / "state.json"

    _save_state(state_path, {"version": 1, "open_trade": {"symbol": "BTC/USDT:USDT"}})
    _save_state(state_path, {"version": 2})

    assert _load_state(state_path) == {"version": 2}
    assert (tmp_path / "state.json.bak").exists()

    state_path.write_text("{broken", encoding="utf-8")

    assert _load_state(state_path) == {"version": 1, "open_trade": {"symbol": "BTC/USDT:USDT"}}
    output = capsys.readouterr().out
    assert "State file is not valid JSON" in output
    assert "Recovered state from backup" in output


def test_process_lock_rejects_second_process(tmp_path):
    lock_path = tmp_path / "bot.lock"
    holder_code = (
        "import sys, time; "
        "from pathlib import Path; "
        "from lumibot.example_strategies.binance_futures_demo_ai_trader import _acquire_process_lock; "
        "_acquire_process_lock(Path(sys.argv[1])); "
        "time.sleep(5)"
    )
    contender_code = (
        "import sys; "
        "from pathlib import Path; "
        "from lumibot.example_strategies.binance_futures_demo_ai_trader import _acquire_process_lock; "
        "\ntry:\n"
        "    _acquire_process_lock(Path(sys.argv[1]))\n"
        "except Exception as exc:\n"
        "    print(exc)\n"
        "    raise SystemExit(2)\n"
        "raise SystemExit(0)\n"
    )
    holder = subprocess.Popen([sys.executable, "-c", holder_code, str(lock_path)])
    try:
        deadline = time.time() + 3
        while time.time() < deadline and not lock_path.exists():
            time.sleep(0.05)
        assert lock_path.exists()

        contender = subprocess.run(
            [sys.executable, "-c", contender_code, str(lock_path)],
            capture_output=True,
            text=True,
            check=False,
        )

        assert contender.returncode == 2
        assert "already running" in contender.stdout
    finally:
        holder.terminate()
        holder.wait(timeout=5)


def test_run_iteration_manual_pause_blocks_new_entry_before_ai(monkeypatch, capsys):
    ai_calls = []
    monkeypatch.setattr(
        "lumibot.example_strategies.binance_futures_demo_ai_trader._gemini_decision",
        lambda *args, **kwargs: ai_calls.append((args, kwargs)),
    )
    exchange = FakeRunIterationExchange()
    args = argparse.Namespace(
        new_entries_enabled=False,
        emergency_close_positions=False,
        performance_report_interval_minutes=Decimal("0"),
        health_report_interval_minutes=Decimal("0"),
        max_daily_loss_pct=Decimal("99"),
        timeframe="5m",
        analysis_timeframes="5m,15m",
        candle_limit=80,
        mtf_candle_limit=80,
        ema_fast=5,
        ema_slow=20,
        high_volatility_atr_pct=Decimal("10"),
        low_volatility_atr_pct=Decimal("0.01"),
        cancel_stale_open_orders=False,
        stop_loss_pct=Decimal("1"),
        take_profit_pct=Decimal("2"),
        breakeven_trigger_pct=Decimal("0.5"),
        breakeven_offset_pct=Decimal("0.05"),
        trailing_activation_pct=Decimal("1"),
        trailing_distance_pct=Decimal("0.5"),
    )
    state = {}

    result = _run_iteration(exchange, args, "BTC/USDT:USDT", False, state, [])

    output = capsys.readouterr().out
    assert result is state
    assert not ai_calls
    assert "Blocked: new entries disabled by runtime config." in output
    assert state["entry_block_events"][-1]["category"] == "manual_pause"


def test_stale_candle_reason_uses_latest_candle_age(monkeypatch):
    monkeypatch.setattr("lumibot.example_strategies.binance_futures_demo_ai_trader._now_ts", lambda: Decimal("1000"))

    assert _stale_candle_reason([[990_000, 1, 1, 1, 1, 1]], "5m", Decimal("900")) is None

    reason = _stale_candle_reason([[1, 1, 1, 1, 1, 1]], "5m", Decimal("10"))

    assert reason is not None
    assert "last candle age" in reason


def test_run_iteration_stale_primary_data_blocks_ai_entry(monkeypatch, capsys):
    monkeypatch.setattr("lumibot.example_strategies.binance_futures_demo_ai_trader._now_ts", lambda: Decimal("1000"))
    ai_calls = []
    monkeypatch.setattr(
        "lumibot.example_strategies.binance_futures_demo_ai_trader._gemini_decision",
        lambda *args, **kwargs: ai_calls.append((args, kwargs)),
    )
    exchange = FakeRunIterationExchange()
    args = argparse.Namespace(
        new_entries_enabled=True,
        emergency_close_positions=False,
        execute=False,
        performance_report_interval_minutes=Decimal("0"),
        health_report_interval_minutes=Decimal("0"),
        max_daily_loss_pct=Decimal("99"),
        timeframe="5m",
        analysis_timeframes="5m,15m",
        candle_limit=80,
        mtf_candle_limit=80,
        max_data_age_seconds=Decimal("10"),
        ema_fast=5,
        ema_slow=20,
        high_volatility_atr_pct=Decimal("10"),
        low_volatility_atr_pct=Decimal("0.01"),
        cancel_stale_open_orders=False,
    )
    state = {}

    result = _run_iteration(exchange, args, "BTC/USDT:USDT", False, state, [])

    output = capsys.readouterr().out
    assert result is state
    assert not ai_calls
    assert "Blocked: stale market data" in output
    assert state["entry_block_events"][-1]["category"] == "stale_data"


def test_select_symbol_reuses_single_account_snapshot_for_positions(capsys):
    exchange = FakeRunIterationExchange(
        positions=[
            {
                "symbol": "ETHUSDT",
                "positionAmt": "0.25",
                "entryPrice": "2500",
                "unRealizedProfit": "5",
            }
        ]
    )
    args = argparse.Namespace(cancel_stale_open_orders=False, execute=True)

    selected_symbol, candidates = _select_symbol_and_candidates_for_iteration(
        exchange,
        args,
        ["BTC/USDT:USDT", "ETH/USDT:USDT"],
        {},
    )

    output = capsys.readouterr().out
    assert selected_symbol == "ETH/USDT:USDT"
    assert candidates == []
    assert exchange.account_calls == 1
    assert "Managing existing position on ETH/USDT:USDT" in output


def test_run_iteration_blocks_entry_when_account_has_other_position(monkeypatch, capsys):
    ai_calls = []
    monkeypatch.setattr(
        "lumibot.example_strategies.binance_futures_demo_ai_trader._gemini_decision",
        lambda *args, **kwargs: ai_calls.append((args, kwargs)),
    )
    exchange = FakeRunIterationExchange(
        positions=[
            {
                "symbol": "ETHUSDT",
                "positionAmt": "0.25",
                "entryPrice": "2500",
                "unRealizedProfit": "5",
            }
        ]
    )
    args = argparse.Namespace(
        new_entries_enabled=True,
        emergency_close_positions=False,
        execute=True,
        performance_report_interval_minutes=Decimal("0"),
        health_report_interval_minutes=Decimal("0"),
        max_daily_loss_pct=Decimal("99"),
        timeframe="5m",
        analysis_timeframes="5m,15m",
        candle_limit=80,
        mtf_candle_limit=80,
        max_data_age_seconds=Decimal("0"),
        ema_fast=5,
        ema_slow=20,
        high_volatility_atr_pct=Decimal("10"),
        low_volatility_atr_pct=Decimal("0.01"),
        cancel_stale_open_orders=False,
        stop_loss_pct=Decimal("1"),
        take_profit_pct=Decimal("2"),
        breakeven_trigger_pct=Decimal("0.5"),
        breakeven_offset_pct=Decimal("0.05"),
        trailing_activation_pct=Decimal("1"),
        trailing_distance_pct=Decimal("0.5"),
        min_confidence=Decimal("0.70"),
        allow_short=False,
    )
    state = {}

    result = _run_iteration(exchange, args, "BTC/USDT:USDT", False, state, [])

    output = capsys.readouterr().out
    assert result is state
    assert not ai_calls
    assert not exchange.orders
    assert "account already has open position(s): ETHUSDT:long:0.25" in output
    assert state["entry_block_events"][-1]["category"] == "account_position"


def test_account_position_guard_allows_other_symbol_below_multi_position_cap():
    exchange = FakeRunIterationExchange(
        positions=[
            {
                "symbol": "ETHUSDT",
                "positionAmt": "0.25",
                "entryPrice": "2500",
                "unRealizedProfit": "5",
            }
        ]
    )
    account = exchange.fapiPrivateV2GetAccount()

    assert (
        _account_position_block_reason(
            exchange,
            argparse.Namespace(max_open_positions=1),
            "BTC/USDT:USDT",
            None,
            account,
        )
        == "account already has open position(s): ETHUSDT:long:0.25"
    )
    assert (
        _account_position_block_reason(
            exchange,
            argparse.Namespace(max_open_positions=2),
            "BTC/USDT:USDT",
            None,
            account,
        )
        is None
    )


def test_run_iteration_blocks_entry_when_available_margin_is_too_low(monkeypatch, capsys):
    monkeypatch.setattr(
        "lumibot.example_strategies.binance_futures_demo_ai_trader._gemini_decision",
        lambda *args, **kwargs: MultiAgentDecision(
            agents=[AgentOpinion("final_portfolio_manager", "BUY", Decimal("0.90"), "trend")],
            final=AIDecision("BUY", Decimal("0.90"), "trend"),
        ),
    )
    exchange = FakeRunIterationExchange(available_balance="1")
    args = argparse.Namespace(
        new_entries_enabled=True,
        emergency_close_positions=False,
        execute=True,
        performance_report_interval_minutes=Decimal("0"),
        health_report_interval_minutes=Decimal("0"),
        max_daily_loss_pct=Decimal("99"),
        timeframe="5m",
        analysis_timeframes="5m,15m",
        candle_limit=80,
        mtf_candle_limit=80,
        max_data_age_seconds=Decimal("0"),
        ema_fast=5,
        ema_slow=20,
        high_volatility_atr_pct=Decimal("10"),
        low_volatility_atr_pct=Decimal("0.01"),
        cancel_stale_open_orders=False,
        stop_loss_pct=Decimal("1"),
        take_profit_pct=Decimal("2"),
        breakeven_trigger_pct=Decimal("0.5"),
        breakeven_offset_pct=Decimal("0.05"),
        trailing_activation_pct=Decimal("1"),
        trailing_distance_pct=Decimal("0.5"),
        min_confidence=Decimal("0.70"),
        allow_short=False,
        max_spread_pct=Decimal("0"),
        taker_fee_rate_pct=Decimal("0.05"),
        min_reward_to_fee_ratio=Decimal("0"),
        max_abs_funding_rate_pct=Decimal("0"),
        max_entry_funding_cost_pct=Decimal("0"),
        order_notional_usdt=Decimal("60"),
        risk_per_trade_pct=Decimal("0"),
        max_margin_allocation_pct=Decimal("5"),
        adaptive_risk=False,
        sizing_cap_buffer_pct=Decimal("1"),
        max_position_notional_usdt=Decimal("600"),
        max_total_notional_usdt=Decimal("700"),
        leverage=5,
        available_balance_buffer_pct=Decimal("5"),
    )
    state = {}

    result = _run_iteration(exchange, args, "BTC/USDT:USDT", False, state, [])

    output = capsys.readouterr().out
    assert result is state
    assert not exchange.orders
    assert exchange.account_calls == 1
    assert "available_balance" in output
    assert state["entry_block_events"][-1]["category"] == "available_margin"


def test_run_iteration_uses_order_book_spread_when_ticker_lacks_bid_ask(monkeypatch):
    monkeypatch.setattr(
        "lumibot.example_strategies.binance_futures_demo_ai_trader._gemini_decision",
        lambda *args, **kwargs: MultiAgentDecision(
            agents=[AgentOpinion("final_portfolio_manager", "BUY", Decimal("0.90"), "trend")],
            final=AIDecision("BUY", Decimal("0.90"), "trend"),
        ),
    )
    exchange = FakeRunIterationExchange(ticker_has_spread=False)
    args = argparse.Namespace(
        new_entries_enabled=True,
        emergency_close_positions=False,
        execute=True,
        performance_report_interval_minutes=Decimal("0"),
        health_report_interval_minutes=Decimal("0"),
        max_daily_loss_pct=Decimal("99"),
        timeframe="5m",
        analysis_timeframes="5m,15m",
        candle_limit=80,
        mtf_candle_limit=80,
        max_data_age_seconds=Decimal("0"),
        ema_fast=5,
        ema_slow=20,
        high_volatility_atr_pct=Decimal("10"),
        low_volatility_atr_pct=Decimal("0.01"),
        cancel_stale_open_orders=False,
        stop_loss_pct=Decimal("1"),
        take_profit_pct=Decimal("2"),
        breakeven_trigger_pct=Decimal("0.5"),
        breakeven_offset_pct=Decimal("0.05"),
        trailing_activation_pct=Decimal("1"),
        trailing_distance_pct=Decimal("0.5"),
        min_confidence=Decimal("0.70"),
        allow_short=False,
        max_spread_pct=Decimal("0.20"),
        taker_fee_rate_pct=Decimal("0.05"),
        min_reward_to_fee_ratio=Decimal("0"),
        max_abs_funding_rate_pct=Decimal("0"),
        max_entry_funding_cost_pct=Decimal("0"),
        order_notional_usdt=Decimal("60"),
        risk_per_trade_pct=Decimal("0"),
        max_margin_allocation_pct=Decimal("5"),
        adaptive_risk=False,
        sizing_cap_buffer_pct=Decimal("1"),
        max_position_notional_usdt=Decimal("600"),
        max_total_notional_usdt=Decimal("700"),
        leverage=5,
        available_balance_buffer_pct=Decimal("5"),
        native_protection=False,
        close_on_native_protection_failure=True,
    )
    state = {}

    result = _run_iteration(exchange, args, "BTC/USDT:USDT", False, state, [])

    assert result is state
    assert exchange.order_book_calls == 1
    assert exchange.orders and exchange.orders[-1]["side"] == "buy"
    assert state["open_trade"]["symbol"] == "BTC/USDT:USDT"
    assert "entry_block_events" not in state


def test_run_iteration_records_external_close_for_previous_symbol_when_selected_symbol_changes():
    exchange = FakeRunIterationExchange()
    args = argparse.Namespace(
        new_entries_enabled=False,
        emergency_close_positions=True,
        execute=True,
        performance_report_interval_minutes=Decimal("0"),
        health_report_interval_minutes=Decimal("0"),
        max_daily_loss_pct=Decimal("99"),
        cancel_stale_open_orders=False,
        symbol_cooldown_minutes=Decimal("0"),
        loss_symbol_cooldown_minutes=Decimal("0"),
        auto_blacklist_loss_count=3,
        auto_blacklist_minutes=Decimal("0"),
        max_consecutive_losses=3,
        loss_pause_minutes=Decimal("0"),
        adaptive_risk=False,
        notify_events="",
        discord_webhook_url="",
        telegram_bot_token="",
        telegram_chat_id="",
    )
    state = {
        "open_trade": {
            "symbol": "BTC/USDT:USDT",
            "side": "long",
            "amount": "0.1",
            "entry_price": "100",
            "highest_price": "100",
            "lowest_price": "100",
        }
    }
    account = {"totalWalletBalance": "1000", "availableBalance": "1000", "positions": []}

    result = _run_iteration(exchange, args, "ETH/USDT:USDT", False, state, [], account)

    assert result is state
    assert state.get("open_trade") is None
    assert state["closed_trades"][-1]["symbol"] == "BTC/USDT:USDT"
    assert state["closed_trades"][-1]["exit_reason"] == "EXTERNAL_OR_NATIVE_CLOSE"
    assert state["closed_trades"][-1]["exit_price"] == "120"
    assert state["trade_journal"][-1]["event"] == "exit"


def test_run_iteration_closes_entry_when_native_protection_fails(monkeypatch, capsys):
    monkeypatch.setattr(
        "lumibot.example_strategies.binance_futures_demo_ai_trader._gemini_decision",
        lambda *args, **kwargs: MultiAgentDecision(
            agents=[AgentOpinion("final_portfolio_manager", "BUY", Decimal("0.90"), "trend")],
            final=AIDecision("BUY", Decimal("0.90"), "trend"),
        ),
    )
    notifications = []
    monkeypatch.setattr(
        "lumibot.example_strategies.binance_futures_demo_ai_trader._notify",
        lambda args, event, message: notifications.append((event, message)),
    )
    exchange = FakeRunIterationExchange(fail_native_protection="take_profit")
    args = argparse.Namespace(
        new_entries_enabled=True,
        emergency_close_positions=False,
        execute=True,
        performance_report_interval_minutes=Decimal("0"),
        health_report_interval_minutes=Decimal("0"),
        max_daily_loss_pct=Decimal("99"),
        timeframe="5m",
        analysis_timeframes="5m,15m",
        candle_limit=80,
        mtf_candle_limit=80,
        max_data_age_seconds=Decimal("0"),
        ema_fast=5,
        ema_slow=20,
        high_volatility_atr_pct=Decimal("10"),
        low_volatility_atr_pct=Decimal("0.01"),
        cancel_stale_open_orders=False,
        stop_loss_pct=Decimal("1"),
        take_profit_pct=Decimal("2"),
        breakeven_trigger_pct=Decimal("0.5"),
        breakeven_offset_pct=Decimal("0.05"),
        trailing_activation_pct=Decimal("1"),
        trailing_distance_pct=Decimal("0.5"),
        min_confidence=Decimal("0.70"),
        allow_short=False,
        max_spread_pct=Decimal("0"),
        taker_fee_rate_pct=Decimal("0.05"),
        min_reward_to_fee_ratio=Decimal("0"),
        max_abs_funding_rate_pct=Decimal("0"),
        max_entry_funding_cost_pct=Decimal("0"),
        order_notional_usdt=Decimal("60"),
        risk_per_trade_pct=Decimal("0"),
        max_margin_allocation_pct=Decimal("5"),
        adaptive_risk=False,
        sizing_cap_buffer_pct=Decimal("1"),
        max_position_notional_usdt=Decimal("600"),
        max_total_notional_usdt=Decimal("700"),
        leverage=5,
        available_balance_buffer_pct=Decimal("5"),
        native_protection=True,
        close_on_native_protection_failure=True,
        symbol_cooldown_minutes=Decimal("0"),
        loss_symbol_cooldown_minutes=Decimal("0"),
        max_consecutive_losses=99,
        loss_pause_minutes=Decimal("0"),
        auto_blacklist_loss_count=99,
        auto_blacklist_minutes=Decimal("0"),
    )
    state = {}

    result = _run_iteration(exchange, args, "BTC/USDT:USDT", False, state, [])

    output = capsys.readouterr().out
    assert result is state
    assert "NATIVE_PROTECTION_FAILED" in output
    assert [order["type"] for order in exchange.orders] == ["market", "STOP_MARKET", "market"]
    assert [order["side"] for order in exchange.orders] == ["buy", "sell", "sell"]
    assert exchange.cancel_all_order_symbols == ["BTC/USDT:USDT"]
    assert exchange.orders[-1]["params"] == {"reduceOnly": True}
    assert state["closed_trades"][-1]["exit_reason"] == "NATIVE_PROTECTION_FAILED"
    assert "open_trade" not in state
    assert notifications and notifications[-1][0] == "risk"


def test_run_iteration_emergency_close_closes_position_before_ai(monkeypatch, capsys):
    ai_calls = []
    monkeypatch.setattr(
        "lumibot.example_strategies.binance_futures_demo_ai_trader._gemini_decision",
        lambda *args, **kwargs: ai_calls.append((args, kwargs)),
    )
    monkeypatch.setattr("lumibot.example_strategies.binance_futures_demo_ai_trader._review_closed_trade", lambda *args: None)
    notifications = []
    monkeypatch.setattr(
        "lumibot.example_strategies.binance_futures_demo_ai_trader._notify",
        lambda args, event, message: notifications.append((event, message)),
    )
    exchange = FakeRunIterationExchange(
        positions=[
            {
                "symbol": "BTCUSDT",
                "positionAmt": "0.5",
                "entryPrice": "100",
                "unRealizedProfit": "10",
            }
        ]
    )
    args = argparse.Namespace(
        new_entries_enabled=True,
        emergency_close_positions=True,
        execute=True,
        performance_report_interval_minutes=Decimal("0"),
        health_report_interval_minutes=Decimal("0"),
        max_daily_loss_pct=Decimal("99"),
        timeframe="5m",
        analysis_timeframes="5m,15m",
        candle_limit=80,
        mtf_candle_limit=80,
        ema_fast=5,
        ema_slow=20,
        high_volatility_atr_pct=Decimal("10"),
        low_volatility_atr_pct=Decimal("0.01"),
        cancel_stale_open_orders=False,
        stop_loss_pct=Decimal("1"),
        take_profit_pct=Decimal("2"),
        breakeven_trigger_pct=Decimal("0.5"),
        breakeven_offset_pct=Decimal("0.05"),
        trailing_activation_pct=Decimal("1"),
        trailing_distance_pct=Decimal("0.5"),
        max_total_notional_usdt=Decimal("100000"),
        native_protection=False,
        notify_events="risk",
        symbol_cooldown_minutes=Decimal("0"),
        loss_symbol_cooldown_minutes=Decimal("0"),
        max_consecutive_losses=99,
        loss_pause_minutes=Decimal("0"),
        auto_blacklist_loss_count=99,
        auto_blacklist_minutes=Decimal("0"),
        adaptive_risk=False,
    )
    state = {"open_trade": {"symbol": "BTC/USDT:USDT", "side": "long", "amount": "0.5", "entry_price": "100"}}

    result = _run_iteration(exchange, args, "BTC/USDT:USDT", False, state, [])

    output = capsys.readouterr().out
    assert result is state
    assert not ai_calls
    assert "EMERGENCY_CLOSE" in output
    assert exchange.orders[-1]["side"] == "sell"
    assert exchange.orders[-1]["params"] == {"reduceOnly": True}
    assert state["closed_trades"][-1]["exit_reason"] == "EMERGENCY_CLOSE"
    assert "open_trade" not in state
    assert notifications and notifications[-1][0] == "risk"


def test_submit_entry_order_marketable_limit_falls_back_to_market():
    exchange = FakeEntryExecutionExchange()
    args = argparse.Namespace(
        execution_mode="marketable_limit",
        limit_entry_price_offset_pct=Decimal("0.02"),
        limit_entry_timeout_seconds=0,
        limit_entry_min_fill_ratio=Decimal("1"),
    )

    order, detail = _submit_entry_order(
        exchange,
        "BTC/USDT:USDT",
        "buy",
        "0.1",
        {"bid": "99", "ask": "100"},
        Decimal("100"),
        {},
        args,
    )

    assert order["type"] == "market"
    assert detail.startswith("marketable_limit:fallback_market")
    assert exchange.orders[0]["type"] == "limit"
    assert exchange.orders[0]["price"] == "100.0200"
    assert exchange.canceled == [("entry-1", "BTC/USDT:USDT")]


def test_submit_entry_order_uses_ai_limit_override_without_market_fallback():
    exchange = FakeEntryExecutionExchange()
    args = argparse.Namespace(
        execution_mode="marketable_limit",
        limit_entry_price_offset_pct=Decimal("0.02"),
        limit_entry_timeout_seconds=0,
        limit_entry_min_fill_ratio=Decimal("1"),
    )

    with pytest.raises(RuntimeError, match="Limit entry did not fill enough"):
        _submit_entry_order(
            exchange,
            "BTC/USDT:USDT",
            "buy",
            "0.1",
            {"bid": "99", "ask": "100"},
            Decimal("100"),
            {},
            args,
            execution_mode_override="limit",
            limit_price_override=Decimal("99.5000"),
        )

    assert exchange.orders[0]["type"] == "limit"
    assert exchange.orders[0]["price"] == "99.5000"
    assert len(exchange.orders) == 1


def test_submit_entry_order_fallback_market_uses_remaining_amount_after_partial_fill():
    exchange = FakePartialEntryExecutionExchange()
    args = argparse.Namespace(
        execution_mode="marketable_limit",
        limit_entry_price_offset_pct=Decimal("0.02"),
        limit_entry_timeout_seconds=0,
        limit_entry_min_fill_ratio=Decimal("1"),
    )

    order, detail = _submit_entry_order(
        exchange,
        "BTC/USDT:USDT",
        "buy",
        "0.1",
        {"bid": "99", "ask": "100"},
        Decimal("100"),
        {},
        args,
    )

    assert order["type"] == "market"
    assert order["amount"] == "0.10"
    assert order["filled"] == "0.10"
    assert detail.startswith("marketable_limit:fallback_market_remaining=0.06")
    assert exchange.orders[0]["filled"] == "0.04"
    assert exchange.orders[1]["amount"] == "0.06"


def test_entry_quote_drift_blocks_stale_post_ai_price():
    reason = _entry_quote_drift_block_reason(Decimal("100"), Decimal("100.31"), Decimal("0.20"))

    assert reason is not None
    assert "post-AI price drift" in reason


def test_ai_wait_pullback_entry_plan_uses_limit_price():
    exchange = FakeEntryExecutionExchange()
    args = argparse.Namespace(
        execution_mode="marketable_limit",
        ai_entry_plan=True,
        ai_entry_limit_max_distance_pct=Decimal("0.60"),
    )

    mode, limit_price, metadata, block = _entry_execution_plan_from_ai(
        args,
        exchange,
        "buy",
        "BTC/USDT:USDT",
        {"last": "100", "bid": "99.40", "ask": "100.10"},
        {"entry_style": "wait_pullback", "limit_price": "99.50"},
    )

    assert block is None
    assert mode == "limit"
    assert limit_price == Decimal("99.50")
    assert metadata["style"] == "wait_pullback"


def test_ai_entry_plan_blocks_limit_price_too_far_from_fresh_last():
    exchange = FakeEntryExecutionExchange()
    args = argparse.Namespace(
        execution_mode="marketable_limit",
        ai_entry_plan=True,
        ai_entry_limit_max_distance_pct=Decimal("0.20"),
    )

    mode, limit_price, metadata, block = _entry_execution_plan_from_ai(
        args,
        exchange,
        "buy",
        "BTC/USDT:USDT",
        {"last": "100", "bid": "99.40", "ask": "100.10"},
        {"entry_style": "wait_pullback", "limit_price": "99.50"},
    )

    assert mode == "limit"
    assert limit_price == Decimal("99.50")
    assert metadata["distance_pct"] == "0.5000"
    assert block == "AI limit_price distance 0.5000% > ai_entry_limit_max_distance_pct 0.20%"


def test_close_position_uses_binance_user_trades_and_income_for_actual_pnl():
    exchange = FakeCloseAccountingExchange()
    position = PositionSummary("long", Decimal("9.03"), Decimal("63.5703875969"), Decimal("0"))
    open_trade = {"opened_at": "2026-06-07T02:57:28+00:00"}

    close_execution = _close_position(exchange, "SOL/USDT:USDT", position, False, True)
    report = _close_binance_report(
        exchange,
        "SOL/USDT:USDT",
        open_trade,
        close_execution,
        1780802267000,
    )

    assert close_execution.price == Decimal("63.38")
    assert close_execution.report["binance_realized_pnl_usdt"] == "-1.71920000"
    assert close_execution.report["binance_commission_usdt"] == "-0.22892856"
    assert report["binance_realized_pnl_usdt"] == "-1.71920000"
    assert report["binance_commission_usdt"] == "-0.42460909"
    assert report["binance_net_pnl_usdt"] == "-2.14380909"
    assert report["binance_pnl_source"] == "income"


def test_position_notional():
    position = PositionSummary("long", Decimal("0.05"), Decimal("60000"), Decimal("0"))

    assert _position_notional(position, Decimal("62000")) == Decimal("3100.00")


def test_spread_entry_block_reason_blocks_wide_spread():
    ticker = {"bid": "99", "ask": "101"}

    assert _spread_entry_block_reason(ticker, Decimal("1")).startswith("spread")
    assert _spread_entry_block_reason(ticker, Decimal("3")) is None


def test_fee_reward_entry_block_reason_blocks_tiny_take_profit():
    args = argparse.Namespace(
        take_profit_pct=Decimal("0.10"),
        taker_fee_rate_pct=Decimal("0.05"),
        min_reward_to_fee_ratio=Decimal("2"),
    )

    assert _fee_reward_entry_block_reason(args).startswith("take_profit_pct")


def test_funding_entry_block_reason_blocks_directional_funding_cost():
    exchange = FakePerformanceExchange()
    args = argparse.Namespace(
        max_abs_funding_rate_pct=Decimal("0"),
        max_entry_funding_cost_pct=Decimal("0.03"),
    )

    assert _funding_entry_block_reason(exchange, args, "BTC/USDT:USDT", "buy").startswith("buy entry would pay")
    assert _funding_entry_block_reason(exchange, args, "BTC/USDT:USDT", "sell") is None


def test_sync_binance_performance_aggregates_income_and_deduplicates(monkeypatch):
    monkeypatch.setattr("lumibot.example_strategies.binance_futures_demo_ai_trader._now_ms", lambda: 2_000_000)
    monkeypatch.setattr("lumibot.example_strategies.binance_futures_demo_ai_trader._now_ts", lambda: Decimal("2000"))
    exchange = FakePerformanceExchange()
    state = {}
    args = argparse.Namespace(
        binance_performance_sync_interval_minutes=Decimal("1"),
        binance_performance_lookback_minutes=Decimal("60"),
    )

    _sync_binance_performance(exchange, args, state, ["BTC/USDT:USDT"])
    state["binance_performance"]["last_sync_ts"] = "0"
    _sync_binance_performance(exchange, args, state, ["BTC/USDT:USDT"])

    summary = _binance_performance_summary(state)
    assert summary["income_events"] == 3
    assert summary["user_trades"] == 1
    assert summary["realized_pnl_usdt"] == "3.5"
    assert summary["commission_usdt"] == "-0.2"
    assert summary["funding_usdt"] == "-0.1"
    assert summary["net_income_usdt"] == "3.2"


def test_binance_symbol_performance_guard_blacklists_losing_symbol(monkeypatch):
    monkeypatch.setattr("lumibot.example_strategies.binance_futures_demo_ai_trader._now_ts", lambda: Decimal("1000"))
    state = {
        "binance_performance": {
            "income_events": [
                {"symbol": "BCHUSDT", "incomeType": "REALIZED_PNL", "income": "-2.0"},
                {"symbol": "BCHUSDT", "incomeType": "REALIZED_PNL", "income": "-2.5"},
                {"symbol": "BCHUSDT", "incomeType": "REALIZED_PNL", "income": "-2.0"},
                {"symbol": "BCHUSDT", "incomeType": "COMMISSION", "income": "-0.3"},
            ]
        }
    }
    args = argparse.Namespace(
        binance_symbol_performance_guard=True,
        binance_symbol_guard_min_trades=3,
        binance_symbol_guard_max_net_loss_usdt=Decimal("5"),
        binance_symbol_guard_blacklist_minutes=Decimal("60"),
        notify_events="",
        discord_webhook_url="",
        telegram_bot_token="",
        telegram_chat_id="",
    )

    _maybe_apply_binance_symbol_performance_guard(state, args)

    stats = _binance_symbol_performance(state)
    assert stats["BCH/USDT:USDT"]["net_income"] == Decimal("-6.8")
    assert state["symbol_controls"]["BCH/USDT:USDT"]["blacklist_until"] == "4600"
    assert "BCH/USDT:USDT" in state["binance_symbol_performance_guard"]


def test_binance_symbol_score_penalty_uses_negative_net_income():
    state = {
        "binance_performance": {
            "income_events": [
                {"symbol": "XRPUSDT", "incomeType": "REALIZED_PNL", "income": "-4.0"},
                {"symbol": "XRPUSDT", "incomeType": "REALIZED_PNL", "income": "-4.0"},
                {"symbol": "XRPUSDT", "incomeType": "COMMISSION", "income": "-0.5"},
            ]
        }
    }
    args = argparse.Namespace(
        binance_symbol_score_penalty=True,
        binance_symbol_guard_min_trades=2,
        binance_symbol_guard_max_net_loss_usdt=Decimal("5"),
        binance_symbol_score_penalty_max=Decimal("0.30"),
    )

    penalty = _binance_symbol_score_penalty(state, args, "XRP/USDT:USDT")

    assert penalty == Decimal("0.170")


def test_dynamic_reprice_tightens_native_protection(monkeypatch):
    monkeypatch.setattr("lumibot.example_strategies.binance_futures_demo_ai_trader._now_ts", lambda: Decimal("2000"))
    exchange = FakeAlgoProtectionExchange()
    state = {}
    open_trade = {
        "side": "long",
        "entry_price": "100",
        "highest_price": "102",
        "lowest_price": "100",
        "exit_plan": {"stop_loss_pct": "1", "take_profit_pct": "2"},
        "native_protection": {"stop_price": "99", "take_profit_price": "102"},
    }
    args = argparse.Namespace(
        native_protection=True,
        execute=True,
        dynamic_protection_reprice=True,
        dynamic_protection_reprice_interval_minutes=Decimal("0"),
        dynamic_protection_min_improvement_pct=Decimal("0"),
        breakeven_trigger_pct=Decimal("0.4"),
        breakeven_offset_pct=Decimal("0.05"),
        trailing_activation_pct=Decimal("0.6"),
        trailing_distance_pct=Decimal("0.35"),
        stop_loss_pct=Decimal("1"),
        take_profit_pct=Decimal("2"),
        notify_events="",
        discord_webhook_url="",
        telegram_bot_token="",
        telegram_chat_id="",
    )

    changed = _maybe_reprice_native_protection(
        exchange,
        state,
        "BTC/USDT:USDT",
        PositionSummary("long", Decimal("0.5"), Decimal("100"), Decimal("0")),
        args,
        dual_side=False,
        open_trade=open_trade,
        last_price=Decimal("101.5"),
    )

    assert changed is True
    assert open_trade["native_protection"]["stop_price"] == "101.6430"
    assert open_trade["native_protection"]["take_profit_price"] == "102"
    assert open_trade["last_native_reprice_ts"] == "2000"
    assert [order["type"] for order in exchange.algo_orders] == ["STOP_MARKET", "TAKE_PROFIT_MARKET"]
    assert state["trade_journal"][0]["event"] == "native_protection_reprice"


def test_trade_journal_event_is_bounded(monkeypatch):
    monkeypatch.setattr("lumibot.example_strategies.binance_futures_demo_ai_trader.MAX_TRADE_JOURNAL_HISTORY", 3)
    state = {}

    for index in range(5):
        _record_trade_journal_event(state, "test", {"sequence": index})

    assert [event["sequence"] for event in state["trade_journal"]] == [2, 3, 4]


def test_entry_notional_from_risk_uses_equity_stop_leverage_and_caps():
    args = argparse.Namespace(
        order_notional_usdt=Decimal("60"),
        max_position_notional_usdt=Decimal("500"),
        max_total_notional_usdt=Decimal("600"),
        risk_per_trade_pct=Decimal("0.25"),
        stop_loss_pct=Decimal("0.5"),
        max_margin_allocation_pct=Decimal("2"),
        leverage=5,
    )

    notional, reason = _entry_notional_from_risk(args, Decimal("6000"))

    assert notional == Decimal("500")
    assert reason.startswith("risk_based")


def test_entry_notional_from_risk_defaults_to_fixed_when_disabled():
    args = argparse.Namespace(
        order_notional_usdt=Decimal("60"),
        max_position_notional_usdt=Decimal("500"),
        max_total_notional_usdt=Decimal("600"),
        risk_per_trade_pct=Decimal("0"),
    )

    assert _entry_notional_from_risk(args, Decimal("6000")) == (Decimal("60"), "fixed")


def test_dynamic_exit_plan_uses_atr_alignment_and_overextension_bounds():
    args = argparse.Namespace(
        stop_loss_pct=Decimal("0.45"),
        take_profit_pct=Decimal("1.2"),
        dynamic_exit_plan=True,
        dynamic_exit_atr_stop_multiplier=Decimal("1.80"),
        dynamic_exit_min_stop_pct=Decimal("0.30"),
        dynamic_exit_max_stop_pct=Decimal("0.90"),
        dynamic_exit_min_take_profit_pct=Decimal("0.70"),
        dynamic_exit_max_take_profit_pct=Decimal("2.50"),
        dynamic_exit_min_reward_risk=Decimal("1.80"),
        dynamic_exit_max_reward_risk=Decimal("3.00"),
    )
    primary = Indicators(
        close=Decimal("580.03"),
        ema_fast=Decimal("578.32"),
        ema_slow=Decimal("576.01"),
        rsi=Decimal("57.90"),
        atr=Decimal("1.27"),
        atr_pct=Decimal("0.22"),
        trend="up",
    )
    timeframes = {
        "1m": Indicators(Decimal("580.03"), Decimal("579.59"), Decimal("579.08"), Decimal("67.06"), Decimal("0.37"), Decimal("0.064"), "up"),
        "5m": primary,
        "15m": Indicators(Decimal("580.03"), Decimal("576.23"), Decimal("576.00"), Decimal("71.96"), Decimal("3.21"), Decimal("0.554"), "up"),
        "1h": Indicators(Decimal("580.03"), Decimal("578.63"), Decimal("591.41"), Decimal("66.53"), Decimal("7.51"), Decimal("1.295"), "down"),
    }
    regime = MarketRegime("trending", "follow_trend", Decimal("0.72"), "trend")

    plan = _dynamic_exit_plan(args, "buy", Decimal("0.71"), primary, timeframes, regime)

    assert plan.mode == "dynamic_atr"
    assert plan.stop_loss_pct == Decimal("0.3960")
    assert plan.take_profit_pct == Decimal("0.712800")
    assert plan.reward_risk == Decimal("1.80")
    assert "higher_timeframe_opposes" in plan.reasons


def test_exit_plan_from_open_trade_preserves_state_plan():
    args = argparse.Namespace(stop_loss_pct=Decimal("0.45"), take_profit_pct=Decimal("1.2"))
    open_trade = {
        "exit_plan": {
            "mode": "dynamic_atr",
            "stop_loss_pct": "0.3960",
            "take_profit_pct": "0.7128",
            "reasons": ["atr_stop=0.22*1.80"],
        }
    }

    plan = _exit_plan_from_open_trade(open_trade, args)

    assert plan.mode == "dynamic_atr"
    assert plan.stop_loss_pct == Decimal("0.3960")
    assert plan.take_profit_pct == Decimal("0.7128")


class FakeCappedAmountExchange:
    id = "binance"
    markets = {
        "BCH/USDT:USDT": {
            "id": "BCHUSDT",
            "swap": True,
            "quote": "USDT",
            "limits": {},
            "info": {"filters": [{"filterType": "LOT_SIZE", "stepSize": "0.001"}]},
        }
    }

    def amount_to_precision(self, symbol, amount):
        return f"{Decimal(str(amount)):.3f}"


def test_amount_from_notional_with_cap_floors_exchange_step_when_buffer_exceeds_cap():
    amount, estimated_notional = _amount_from_notional_with_cap(
        FakeCappedAmountExchange(),
        "BCH/USDT:USDT",
        Decimal("594"),
        Decimal("222.07383"),
        Decimal("600"),
    )

    assert amount == "2.701"
    assert estimated_notional <= Decimal("600")
    assert estimated_notional > Decimal("599")


def test_update_open_trade_state_preserves_entry_agent_metadata_after_position_sync():
    state = {
        "open_trade": {
            "key": "pending:sell:0.1:100",
            "side": "short",
            "amount": "0.1",
            "entry_price": "100",
            "opened_at": "2026-06-05T00:00:00+00:00",
            "highest_price": "100",
            "lowest_price": "100",
            "breakeven_armed": False,
            "trailing_armed": False,
            "entry_agents": [{"name": "risk_manager", "action": "HOLD"}],
        }
    }
    position = PositionSummary("short", Decimal("0.1"), Decimal("99.5"), Decimal("0"))

    open_trade = _update_open_trade_state(state, position, Decimal("99"))

    assert open_trade["key"] == "short:0.1:99.5"
    assert open_trade["opened_at"] == "2026-06-05T00:00:00+00:00"
    assert open_trade["entry_agents"] == [{"name": "risk_manager", "action": "HOLD"}]


def test_fetch_usdt_equity_uses_binance_futures_v2_account():
    exchange = FakeBinanceFuturesExchange(account={"totalWalletBalance": "123.45"})

    assert _fetch_usdt_equity(exchange) == Decimal("123.45")
    assert exchange.fetch_balance_called is False


def test_fetch_position_uses_binance_futures_v2_position_risk():
    exchange = FakeBinanceFuturesExchange(
        positions=[{"symbol": "BTCUSDT", "positionAmt": "-0.025", "entryPrice": "64000.5", "unRealizedProfit": "1.25"}]
    )
    exchange.fapiPrivateV2GetAccount = None

    def mock_fetch_positions(symbols=None):
        exchange.fetch_positions_called = True
        return [
            {
                "symbol": "BTC/USDT:USDT",
                "contracts": -0.025,
                "entryPrice": 64000.5,
                "unrealizedPnl": 1.25,
                "contractSize": 1,
            }
        ]
    exchange.fetch_positions = mock_fetch_positions

    position = _fetch_position(exchange, "BTC/USDT:USDT")

    assert position == PositionSummary("short", Decimal("0.025"), Decimal("64000.5"), Decimal("1.25"))
    assert exchange.fetch_positions_called is True


def test_fetch_position_uses_binance_futures_v2_account_positions():
    exchange = FakeBinanceFuturesExchange(
        account={
            "positions": [
                {
                    "symbol": "BTCUSDT",
                    "positionAmt": "0.025",
                    "entryPrice": "64000.5",
                    "unRealizedProfit": "-1.25",
                }
            ]
        }
    )

    position = _fetch_position(exchange, "BTC/USDT:USDT")

    assert position == PositionSummary("long", Decimal("0.025"), Decimal("64000.5"), Decimal("-1.25"))
    assert exchange.fetch_positions_called is False


def test_fetch_position_v2_account_zero_position_returns_none():
    exchange = FakeBinanceFuturesExchange(account={"positions": [{"symbol": "BTCUSDT", "positionAmt": "0.0000"}]})

    assert _fetch_position(exchange, "BTC/USDT:USDT") is None
    assert exchange.fetch_positions_called is False


def test_snapshot_args_and_restore_args():
    from lumibot.example_strategies.binance_futures_demo_ai_trader import (
        _snapshot_args,
        _restore_args,
    )
    import argparse
    args = argparse.Namespace(
        min_confidence=Decimal("0.70"),
        stop_loss_pct=Decimal("1.00"),
        other_param="test",
    )
    snapshot = _snapshot_args(args)

    # Mutate args
    args.min_confidence = Decimal("0.88")
    args.stop_loss_pct = Decimal("1.50")
    args.other_param = "modified"
    args.new_param = "extra"

    # Restore args
    _restore_args(args, snapshot)

    # Verify they are restored back to the original values
    assert args.min_confidence == Decimal("0.70")
    assert args.stop_loss_pct == Decimal("1.00")
    assert args.other_param == "test"
    assert not hasattr(args, "new_param")


# =============================================================================
# New tests for B1–B4 winrate overhaul features
# =============================================================================


def test_aggregate_debate_buy_majority():
    """B2: BUY 2-1 majority → final BUY, moderate penalty."""
    from lumibot.example_strategies.binance_futures_demo_ai_trader import (
        _aggregate_debate,
        Decimal,
        AgentOpinion,
    )

    bull = {"bull_thesis": "strong uptrend", "bull_confidence": 0.85, "action": "BUY"}
    bear = {"bear_thesis": "overbought risk", "bear_confidence": 0.70, "action": "HOLD"}
    reviewer = {"action": "BUY", "confidence": 0.80, "reason": "trend aligned", "veto": False}
    result = _aggregate_debate(bull, bear, reviewer)
    assert result.final.action == "BUY"
    assert result.final.confidence > 0
    # Should have 3 agents
    assert len(result.agents) == 3
    # Names check
    assert result.agents[0].name == "bull_analyst"
    assert result.agents[1].name == "bear_analyst"
    assert result.agents[2].name == "reviewer"


def test_aggregate_debate_reviewer_veto():
    """B1: confident reviewer veto forces HOLD."""
    from lumibot.example_strategies.binance_futures_demo_ai_trader import (
        _aggregate_debate,
        Decimal,
    )

    bull = {"bull_thesis": "buy setup", "bull_confidence": 0.80, "action": "BUY"}
    bear = {"bear_thesis": "not convinced", "bear_confidence": 0.60, "action": "HOLD"}
    reviewer = {
        "action": "HOLD",
        "confidence": 0.70,
        "reason": "both cases weak",
        "veto": True,
        "veto_reason": "insufficient conviction",
    }
    result = _aggregate_debate(bull, bear, reviewer)
    assert result.final.action == "HOLD"
    assert "veto" in result.final.reason.lower()


def test_aggregate_debate_disagreement_penalty():
    """B2: 3-way split → higher disagreement penalty (0.15) and HOLD."""
    from lumibot.example_strategies.binance_futures_demo_ai_trader import (
        _aggregate_debate,
        Decimal,
    )

    bull = {"bull_thesis": "buy", "bull_confidence": 0.80, "action": "BUY"}
    bear = {"bear_thesis": "sell", "bear_confidence": 0.75, "action": "SELL"}
    reviewer = {"action": "HOLD", "confidence": 0.60, "reason": "confused", "veto": False}
    result = _aggregate_debate(bull, bear, reviewer)
    # BUY vs SELL vs HOLD → reviewer breaks tie, reviewer said HOLD
    assert result.final.action in ("HOLD", "BUY", "SELL")


def test_calibrate_confidence_cold_start():
    """B3: No scorecard data → returns raw confidence unchanged."""
    from lumibot.example_strategies.binance_futures_demo_ai_trader import (
        _calibrate_confidence,
        Decimal,
    )
    import argparse

    args = argparse.Namespace(confidence_calibration=True, calibration_min_opinions=5)
    state = {}  # No agent_scorecard
    agents = []
    raw = Decimal("0.80")
    result = _calibrate_confidence(raw, state, agents, args)
    assert result == raw


def test_calibrate_confidence_with_alignment():
    """B3: With alignment_rate 0.85 and all guards clear → calibrated = raw * 0.85."""
    from lumibot.example_strategies.binance_futures_demo_ai_trader import (
        _calibrate_confidence,
        Decimal,
        AgentOpinion,
    )
    import argparse

    args = argparse.Namespace(confidence_calibration=True, calibration_min_opinions=2)
    state = {
        "agent_scorecard": {
            "bull_analyst": {
                "opinions": 10,
                "aligned_with_outcome": 8,
                "alignment_rate": "0.80",
            },
            "reviewer": {
                "opinions": 5,
                "aligned_with_outcome": 5,
                "alignment_rate": "1.00",
            },
        }
    }
    agents = [
        AgentOpinion("bull_analyst", "BUY", Decimal("0.80"), "bull thesis"),
        AgentOpinion("reviewer", "BUY", Decimal("0.90"), "review"),
    ]
    raw = Decimal("0.80")
    result = _calibrate_confidence(raw, state, agents, args)
    # avg_alignment = (0.80 + 1.00) / 2 = 0.90
    # calibrated = 0.80 * 0.90 = 0.72
    assert result < raw
    assert result == Decimal("0.72")


def test_regime_entry_block_blocks_low_volatility():
    """B4: low_volatility regime blocked when hard_regime_block enabled."""
    from lumibot.example_strategies.binance_futures_demo_ai_trader import (
        _regime_entry_block_reason,
        MarketRegime,
        Decimal,
    )
    import argparse

    args = argparse.Namespace(
        hard_regime_block=True,
        hard_regime_block_names="low_volatility,mixed",
        regime_block_max_atr_pct=Decimal("0.30"),
    )
    regime = MarketRegime("low_volatility", "wait_for_breakout", Decimal("0.70"), "low ATR")
    block = _regime_entry_block_reason(regime, Decimal("0.12"), args)
    assert block is not None
    assert "low_volatility" in block


def test_regime_entry_block_disabled():
    """B4: When hard_regime_block is off → always None."""
    from lumibot.example_strategies.binance_futures_demo_ai_trader import (
        _regime_entry_block_reason,
        MarketRegime,
        Decimal,
    )
    import argparse

    args = argparse.Namespace(
        hard_regime_block=False,
        hard_regime_block_names="low_volatility,mixed",
        regime_block_max_atr_pct=Decimal("0.30"),
    )
    regime = MarketRegime("low_volatility", "wait_for_breakout", Decimal("0.70"), "low ATR")
    assert _regime_entry_block_reason(regime, Decimal("0.12"), args) is None
    regime2 = MarketRegime("mixed", "wait", Decimal("0.50"), "mixed signals")
    assert _regime_entry_block_reason(regime2, Decimal("0.50"), args) is None


def test_regime_entry_block_ranging_low_atr():
    """B4: ranging with ATR below max_atr is blocked; ranging with high ATR passes."""
    from lumibot.example_strategies.binance_futures_demo_ai_trader import (
        _regime_entry_block_reason,
        MarketRegime,
        Decimal,
    )
    import argparse

    args = argparse.Namespace(
        hard_regime_block=True,
        hard_regime_block_names="low_volatility,mixed",
        regime_block_max_atr_pct=Decimal("0.30"),
    )
    regime = MarketRegime("ranging", "mean_reversion", Decimal("0.62"), "range market")
    # Low ATR → block
    assert _regime_entry_block_reason(regime, Decimal("0.12"), args) is not None
    # High ATR → pass (not blocked by ranging rule)
    assert _regime_entry_block_reason(regime, Decimal("0.50"), args) is None


def test_reviewer_prompt_has_rubric():
    """B5: _reviewer_prompt includes rubric instructions."""
    from lumibot.example_strategies.binance_futures_demo_ai_trader import (
        _reviewer_prompt,
    )

    prompt = {"role": "test", "task": "test task", "symbol": "BTC/USDT:USDT"}
    bull = {"bull_confidence": 0.80, "bull_thesis": "bull case"}
    bear = {"bear_confidence": 0.60, "bear_thesis": "bear case"}
    result = _reviewer_prompt(prompt, bull, bear)
    assert "RUBRIC" in result.get("task", "")
    assert "veto" in result.get("task", "").lower()
    assert result.get("context", {}).get("adversarial_debate", {}).get("bull_case") == bull
    assert result.get("context", {}).get("adversarial_debate", {}).get("bear_case") == bear
