"""Binance USD-M futures demo/live AI trader.

This is the next step after ``binance_futures_testnet_smoke.py`` succeeds. It
defaults to Binance futures demo execution and uses Gemini only for the final
decision layer. Risk checks remain deterministic.

Usage:
    export BINANCE_FUTURES_DEMO_API_KEY="your-demo-key"
    export BINANCE_FUTURES_DEMO_API_SECRET="your-demo-secret"
    export GEMINI_API_KEY="your-gemini-key"

    # One dry-run decision.
    python lumibot/example_strategies/binance_futures_demo_ai_trader.py --once

    # Continuous dry run.
    python lumibot/example_strategies/binance_futures_demo_ai_trader.py --loop

    # Continuous demo trading. This still uses Binance demo URLs, not live URLs.
    python lumibot/example_strategies/binance_futures_demo_ai_trader.py --loop --execute

    # Live trading is locked behind separate live keys and an explicit phrase.
    python lumibot/example_strategies/binance_futures_demo_ai_trader.py \
        --environment live --loop --execute --live-confirm I_UNDERSTAND_LIVE_RISK
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
import sqlite3
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import ROUND_DOWN, Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import ccxt

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover
    load_dotenv = None

from lumibot.example_strategies.binance_futures_testnet_smoke import (
    DEFAULT_LEVERAGE,
    DEFAULT_SYMBOL,
    LIVE_CONFIRMATION,
    _amount_from_notional,
    _build_exchange,
    _coerce_symbol,
    _fetch_dual_side_position,
    _first_env,
    _market_amount_step,
    _market_min_notional,
    _opposite_side,
    _position_side_for_open,
    _positive_decimal,
    _set_risk_controls,
)


DEFAULT_MODEL = "gemini-3.1-flash-lite-preview"
DEFAULT_STATE_FILE = Path("data/binance_futures_demo_ai_trader_state.json")
DEFAULT_LOCK_FILE = Path("data/binance_futures_demo_ai_trader.lock")
STALE_ORDER_CLEANUP_INTERVAL_SECONDS = Decimal("1800")
MAX_CLOSED_TRADE_HISTORY = 200
MAX_BINANCE_EVENT_HISTORY = 500
MAX_ENTRY_BLOCK_EVENT_HISTORY = 300
MAX_EXCHANGE_ERROR_HISTORY = 100
MAX_TRADE_JOURNAL_HISTORY = 500
MAX_SHADOW_COMPARISON_HISTORY = 500
RUNTIME_CONFIG_PROTECTED_KEYS = {
    "runtime_config_file",
    "exchange_id",
    "environment",
    "execute",
    "live_confirm",
    "loop",
    "lock_file",
    "once",
    "state_file",
}
_PROCESS_LOCK_HANDLE: Any | None = None
_ACCOUNT_NOT_PROVIDED: Any = object()
_GEMINI_CLIENTS: dict[str, Any] = {}
_OPENAI_CLIENTS: dict[tuple[str, str | None, float], Any] = {}
RUNTIME_CONFIG_HOT_KEYS = {
    "new_entries_enabled",
    "emergency_close_positions",
    "allow_short",
    "allow_fallback",
    "trend_coherence_guard",
    "trend_coherence_timeframe",
    "adversarial_debate",
    "models",
    "model",
    "advisor_models",
    "shadow_models",
    "ai_candidate_count",
    "ai_entry_aggressiveness",
    "ai_entry_plan",
    "ai_entry_max_price_drift_pct",
    "ai_entry_limit_max_distance_pct",
    "ai_max_retries",
    "ai_retry_delay_seconds",
    "ai_request_timeout_seconds",
    "max_data_age_seconds",
    "min_confidence",
    "max_daily_loss_pct",
    "order_notional_usdt",
    "risk_per_trade_pct",
    "max_margin_allocation_pct",
    "available_balance_buffer_pct",
    "sizing_cap_buffer_pct",
    "max_position_notional_usdt",
    "max_total_notional_usdt",
    "max_open_positions",
    "max_new_entries_per_cycle",
    "max_spread_pct",
    "taker_fee_rate_pct",
    "min_reward_to_fee_ratio",
    "max_abs_funding_rate_pct",
    "max_entry_funding_cost_pct",
    "high_volatility_atr_pct",
    "low_volatility_atr_pct",
    "stop_loss_pct",
    "take_profit_pct",
    "dynamic_exit_plan",
    "dynamic_exit_atr_stop_multiplier",
    "dynamic_exit_min_stop_pct",
    "dynamic_exit_max_stop_pct",
    "dynamic_exit_min_take_profit_pct",
    "dynamic_exit_max_take_profit_pct",
    "dynamic_exit_min_reward_risk",
    "dynamic_exit_max_reward_risk",
    "native_protection",
    "close_on_native_protection_failure",
    "cancel_stale_open_orders",
    "symbol_cooldown_minutes",
    "loss_symbol_cooldown_minutes",
    "max_consecutive_losses",
    "loss_pause_minutes",
    "auto_blacklist_loss_count",
    "auto_blacklist_minutes",
    "performance_report_interval_minutes",
    "health_report_interval_minutes",
    "entry_block_report_lookback_minutes",
    "exchange_error_alert_threshold",
    "exchange_error_alert_cooldown_minutes",
    "binance_performance_sync_interval_minutes",
    "binance_performance_lookback_minutes",
    "binance_symbol_performance_guard",
    "binance_symbol_guard_min_trades",
    "binance_symbol_guard_max_net_loss_usdt",
    "binance_symbol_guard_blacklist_minutes",
    "binance_symbol_score_penalty",
    "binance_symbol_score_penalty_max",
    "adaptive_risk",
    "adaptive_risk_step",
    "adaptive_risk_min_multiplier",
    "adaptive_risk_max_multiplier",
    "breakeven_trigger_pct",
    "breakeven_offset_pct",
    "trailing_activation_pct",
    "trailing_distance_pct",
    "dynamic_protection_reprice",
    "dynamic_protection_reprice_interval_minutes",
    "dynamic_protection_min_improvement_pct",
    "execution_mode",
    "limit_entry_timeout_seconds",
    "limit_entry_price_offset_pct",
    "limit_entry_min_fill_ratio",
    "auto_tune_parameters",
    "auto_tune_interval_minutes",
    "auto_tune_min_trades",
    "auto_tune_lookback_trades",
    "auto_tune_symbol_min_trades",
    "auto_tune_symbol_max_loss_rate",
    "auto_tune_blacklist_minutes",
    "auto_tune_idle_after_minutes",
    "auto_tune_idle_min_confidence",
    "auto_tune_idle_confidence_step",
    "auto_tune_idle_max_spread_pct",
    "auto_tune_idle_spread_step_pct",
    "auto_tune_idle_min_reward_to_fee_ratio",
    "auto_tune_idle_reward_fee_step",
    "auto_tune_idle_max_entry_funding_cost_pct",
    "auto_tune_idle_funding_step_pct",
    "notify_events",
    "discord_webhook_url",
    "telegram_bot_token",
    "telegram_chat_id",
}
LARGE_CAP_USDM_BASES = (
    "BTC",
    "ETH",
    "BNB",
    "SOL",
    "XRP",
    "DOGE",
    "ADA",
    "AVAX",
    "LINK",
    "TRX",
    "DOT",
    "BCH",
    "LTC",
    "UNI",
    "NEAR",
    "APT",
    "OP",
    "ARB",
    "INJ",
    "ETC",
)


@dataclass(frozen=True)
class Indicators:
    close: Decimal
    ema_fast: Decimal
    ema_slow: Decimal
    rsi: Decimal
    atr: Decimal
    atr_pct: Decimal
    trend: str


@dataclass(frozen=True)
class AIDecision:
    action: str
    confidence: Decimal
    reason: str
    entry_plan: dict[str, Any] | None = None


@dataclass(frozen=True)
class AgentOpinion:
    name: str
    action: str
    confidence: Decimal
    reason: str


@dataclass(frozen=True)
class MultiAgentDecision:
    final: AIDecision
    agents: list[AgentOpinion]


@dataclass(frozen=True)
class AIModelSpec:
    provider: str
    model: str
    raw: str


@dataclass(frozen=True)
class EffectiveDecision:
    decision: AIDecision
    veto_reason: str | None = None


@dataclass(frozen=True)
class ExpertSignal:
    name: str
    action: str
    confidence: Decimal
    reason: str


@dataclass(frozen=True)
class MarketRegime:
    name: str
    preferred_style: str
    confidence: Decimal
    reason: str


@dataclass(frozen=True)
class SymbolCandidate:
    symbol: str
    score: Decimal
    indicators: Indicators
    timeframe_indicators: dict[str, Indicators]
    regime: MarketRegime
    candles: list[list[Any]]


@dataclass(frozen=True)
class PositionSummary:
    side: str
    amount: Decimal
    entry_price: Decimal
    unrealized_pnl: Decimal


@dataclass(frozen=True)
class OrderExecutionSummary:
    price: Decimal | None
    report: dict[str, str]


@dataclass(frozen=True)
class ExitAction:
    reason: str
    stop_price: Decimal | None = None


@dataclass(frozen=True)
class ExitPlan:
    mode: str
    stop_loss_pct: Decimal
    take_profit_pct: Decimal
    reward_risk: Decimal
    reasons: list[str]


def _decimal(value: Any, default: str = "0") -> Decimal:
    if value is None or value == "":
        return Decimal(default)
    return Decimal(str(value))


def _ai_decimal(value: Any) -> Decimal:
    try:
        return _decimal(value)
    except (InvalidOperation, ValueError):
        return Decimal("0")


def _nonnegative_decimal(value: str) -> Decimal:
    parsed = _decimal(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be non-negative")
    return parsed


def _ema(values: list[Decimal], period: int) -> Decimal:
    if len(values) < period:
        raise ValueError(f"Need at least {period} values for EMA")
    multiplier = Decimal(2) / Decimal(period + 1)
    ema = sum(values[:period]) / Decimal(period)
    for value in values[period:]:
        ema = (value - ema) * multiplier + ema
    return ema


def _rsi(closes: list[Decimal], period: int = 14) -> Decimal:
    if len(closes) <= period:
        raise ValueError(f"Need more than {period} closes for RSI")
    
    # Calculate all price changes
    changes = [closes[i] - closes[i-1] for i in range(1, len(closes))]
    gains = [max(c, Decimal("0")) for c in changes]
    losses = [max(-c, Decimal("0")) for c in changes]
    
    if len(changes) < period:
        raise ValueError(f"Need more than {period} closes for RSI")
        
    # First average gain and loss (SMA)
    avg_gain = sum(gains[:period]) / Decimal(period)
    avg_loss = sum(losses[:period]) / Decimal(period)
    
    # Wilder's smoothing
    for i in range(period, len(changes)):
        avg_gain = (avg_gain * Decimal(str(period - 1)) + gains[i]) / Decimal(period)
        avg_loss = (avg_loss * Decimal(str(period - 1)) + losses[i]) / Decimal(period)
        
    if avg_gain == 0 and avg_loss == 0:
        return Decimal("50")  # Neutral if flat
    if avg_loss == 0:
        return Decimal("100")
    if avg_gain == 0:
        return Decimal("0")
        
    rs = avg_gain / avg_loss
    return Decimal("100") - (Decimal("100") / (Decimal("1") + rs))


def _atr(candles: list[list[Any]], period: int = 14) -> Decimal:
    if len(candles) <= period:
        raise ValueError(f"Need more than {period} candles for ATR")
        
    # Calculate all true ranges
    true_ranges: list[Decimal] = []
    for i in range(1, len(candles)):
        previous_close = _decimal(candles[i-1][4])
        high = _decimal(candles[i][2])
        low = _decimal(candles[i][3])
        tr = max(high - low, abs(high - previous_close), abs(low - previous_close))
        true_ranges.append(tr)
        
    # First ATR is the simple average of first 'period' true ranges
    atr_val = sum(true_ranges[:period]) / Decimal(period)
    
    # Wilder's smoothing
    for i in range(period, len(true_ranges)):
        atr_val = (atr_val * Decimal(str(period - 1)) + true_ranges[i]) / Decimal(period)
        
    return atr_val


def _calculate_indicators(candles: list[list[Any]], fast_period: int, slow_period: int) -> Indicators:
    min_length = max(slow_period, 15) + 1
    if len(candles) < min_length:
        raise RuntimeError(f"Need at least {min_length} candles, got {len(candles)}")
    closes = [_decimal(candle[4]) for candle in candles]
    close = closes[-1]
    ema_fast = _ema(closes, fast_period)
    ema_slow = _ema(closes, slow_period)
    rsi = _rsi(closes)
    atr = _atr(candles)
    atr_pct = (atr / close * Decimal("100")) if close else Decimal("0")
    trend = "up" if ema_fast > ema_slow else "down" if ema_fast < ema_slow else "flat"
    return Indicators(close=close, ema_fast=ema_fast, ema_slow=ema_slow, rsi=rsi, atr=atr, atr_pct=atr_pct, trend=trend)


def _timeframe_seconds(timeframe: str) -> Decimal:
    value = str(timeframe).strip().lower()
    if not value:
        return Decimal("0")
    unit = value[-1]
    amount = _decimal(value[:-1])
    multipliers = {
        "m": Decimal("60"),
        "h": Decimal("3600"),
        "d": Decimal("86400"),
        "w": Decimal("604800"),
    }
    return amount * multipliers.get(unit, Decimal("0"))


def _candle_timestamp_seconds(candle: list[Any]) -> Decimal:
    if not candle:
        return Decimal("0")
    raw_timestamp = _decimal(candle[0])
    if raw_timestamp <= 0:
        return Decimal("0")
    if raw_timestamp > Decimal("10000000000"):
        return raw_timestamp / Decimal("1000")
    return raw_timestamp


def _stale_candle_reason(
    candles: list[list[Any]],
    timeframe: str,
    max_data_age_seconds: Decimal | int | float | str,
) -> str | None:
    configured_max_age = _decimal(max_data_age_seconds)
    if configured_max_age <= 0:
        return None
    if not candles:
        return "no candles returned"

    last_candle_ts = _candle_timestamp_seconds(candles[-1])
    if last_candle_ts <= 0:
        return "last candle has no valid timestamp"

    timeframe_age_floor = _timeframe_seconds(timeframe) * Decimal("2")
    max_age = max(configured_max_age, timeframe_age_floor)
    age = max(_now_ts() - last_candle_ts, Decimal("0"))
    if age <= max_age:
        return None
    return (
        f"last candle age {age:.0f}s > max_data_age_seconds {max_age:.0f}s "
        f"last_candle_at={_ts_to_iso(last_candle_ts)} timeframe={timeframe}"
    )


def _stale_position_action(
    position: PositionSummary,
    open_trade: dict[str, Any],
    last_price: Decimal,
    args: argparse.Namespace,
) -> str | None:
    stale_limit = _decimal(getattr(args, "stale_position_hold_minutes", Decimal("0")))
    if stale_limit <= 0:
        return None

    opened_at_str = open_trade.get("opened_at")
    if not opened_at_str:
        return None

    try:
        opened_at = datetime.fromisoformat(opened_at_str.replace("Z", "+00:00"))
        hold_seconds = (datetime.now(timezone.utc) - opened_at).total_seconds()
        hold_minutes = Decimal(str(hold_seconds / 60.0))
    except Exception:
        return None

    if hold_minutes < stale_limit:
        return None

    if position.entry_price <= 0:
        return None
    if position.side == "long":
        pnl_pct = (last_price - position.entry_price) / position.entry_price * Decimal("100")
    else:
        pnl_pct = (position.entry_price - last_price) / position.entry_price * Decimal("100")

    min_pnl_pct = _decimal(getattr(args, "stale_position_min_pnl_pct", Decimal("0.25")))
    if min_pnl_pct > 0 and abs(pnl_pct) >= min_pnl_pct:
        return None

    return f"STALE_POSITION: held for {hold_minutes:.1f}m >= limit {stale_limit}m under consolidation (PnL {pnl_pct:.2f}%)"


def _strip_json_fence(text: str) -> str:
    value = text.strip()
    # Remove <think>...</think> tags if present
    if "<think>" in value:
        parts = value.split("</think>", 1)
        if len(parts) > 1:
            value = parts[1].strip()
    if value.startswith("```"):
        value = value.removeprefix("```json").removeprefix("```").strip()
        if value.endswith("```"):
            value = value[:-3].strip()
    return value


def _normalize_ai_decision_payload(payload: dict[str, Any]) -> AIDecision:
    action = str(payload.get("action") or payload.get("decision") or "HOLD").upper()
    if action in {"WAIT", "NO_TRADE", "NO TRADE"}:
        action = "HOLD"
    if action not in {"BUY", "SELL", "HOLD", "CLOSE"}:
        action = "HOLD"

    raw_conf = payload.get("confidence", "0")
    if isinstance(raw_conf, str):
        lowered = raw_conf.lower().strip()
        if lowered in {"high", "strong"}:
            confidence = Decimal("0.85")
        elif lowered in {"medium", "moderate"}:
            confidence = Decimal("0.65")
        elif lowered in {"low", "weak"}:
            confidence = Decimal("0.35")
        else:
            confidence = _ai_decimal(raw_conf)
    else:
        confidence = _ai_decimal(raw_conf)

    if confidence < 0:
        confidence = Decimal("0")
    if confidence > 1:
        confidence = Decimal("1")

    reason = str(payload.get("reason") or payload.get("reasoning") or payload.get("rationale") or "")[:500]
    entry_plan = payload.get("entry_plan")
    if not isinstance(entry_plan, dict):
        entry_plan = None
    return AIDecision(action=action, confidence=confidence, reason=reason, entry_plan=entry_plan)


def _parse_ai_decision(text: str) -> AIDecision:
    try:
        payload = json.loads(_strip_json_fence(text))
    except json.JSONDecodeError as exc:
        raise ValueError(f"AI response was not valid JSON: {text[:300]}") from exc

    return _normalize_ai_decision_payload(payload)


def _parse_multi_agent_decision(text: str) -> MultiAgentDecision:
    try:
        payload = json.loads(_strip_json_fence(text))
    except json.JSONDecodeError as exc:
        raise ValueError(f"AI response was not valid JSON: {text[:300]}") from exc

    final_payload = payload.get("final_decision")
    if not isinstance(final_payload, dict):
        if not any(key in payload for key in ("action", "decision", "confidence", "reason", "reasoning")):
            raise ValueError(f"AI response did not include a decision payload: {text[:300]}")
        # Backward compatible with the older single-decision response shape.
        return MultiAgentDecision(final=_normalize_ai_decision_payload(payload), agents=[])

    agents: list[AgentOpinion] = []
    raw_agents = payload.get("agents")
    if isinstance(raw_agents, dict):
        raw_agents = list(raw_agents.values())
    if isinstance(raw_agents, list):
        for raw_agent in raw_agents:
            if not isinstance(raw_agent, dict):
                continue
            decision = _normalize_ai_decision_payload(raw_agent)
            agents.append(
                AgentOpinion(
                    name=str(raw_agent.get("name", "unknown"))[:80],
                    action=decision.action,
                    confidence=decision.confidence,
                    reason=decision.reason,
                )
            )

    return MultiAgentDecision(final=_normalize_ai_decision_payload(final_payload), agents=agents)


def _hold_decision(reason: str) -> MultiAgentDecision:
    return MultiAgentDecision(final=AIDecision("HOLD", Decimal("0"), reason[:500]), agents=[])


def _multi_agent_fallback(indicators: Indicators) -> MultiAgentDecision:
    return MultiAgentDecision(final=_fallback_decision(indicators), agents=[])


def _fallback_decision(indicators: Indicators) -> AIDecision:
    """Deterministic fallback used only when --allow-fallback is set."""
    if indicators.trend == "up" and indicators.rsi < Decimal("70"):
        return AIDecision("BUY", Decimal("0.55"), "Fallback: EMA trend is up and RSI is below 70.")
    if indicators.trend == "down" and indicators.rsi > Decimal("30"):
        return AIDecision("SELL", Decimal("0.55"), "Fallback: EMA trend is down and RSI is above 30.")
    return AIDecision("HOLD", Decimal("0.5"), "Fallback: no clean trend/risk setup.")


def _expert_signals(
    indicators: Indicators,
    candles: list[list[Any]],
    position: PositionSummary | None,
    timeframe_indicators: dict[str, Indicators] | None = None,
    regime: MarketRegime | None = None,
) -> list[ExpertSignal]:
    highs = [_decimal(candle[2]) for candle in candles]
    lows = [_decimal(candle[3]) for candle in candles]
    close = indicators.close

    trend_action = "BUY" if indicators.ema_fast > indicators.ema_slow else "SELL" if indicators.ema_fast < indicators.ema_slow else "HOLD"
    trend_confidence = min(abs(indicators.ema_fast - indicators.ema_slow) / close * Decimal("20"), Decimal("0.85"))
    trend_confidence = max(trend_confidence, Decimal("0.45")) if trend_action != "HOLD" else Decimal("0.35")

    recent_high = max(highs[-20:-1])
    recent_low = min(lows[-20:-1])
    if close > recent_high:
        breakout = ExpertSignal("breakout", "BUY", Decimal("0.70"), "Close broke above the previous 20-candle high.")
    elif close < recent_low:
        breakout = ExpertSignal("breakout", "SELL", Decimal("0.70"), "Close broke below the previous 20-candle low.")
    else:
        breakout = ExpertSignal("breakout", "HOLD", Decimal("0.40"), "No 20-candle breakout.")

    if indicators.rsi <= Decimal("30"):
        mean_reversion = ExpertSignal("mean_reversion", "BUY", Decimal("0.62"), "RSI is oversold.")
    elif indicators.rsi >= Decimal("70"):
        mean_reversion = ExpertSignal("mean_reversion", "SELL", Decimal("0.62"), "RSI is overbought.")
    else:
        mean_reversion = ExpertSignal("mean_reversion", "HOLD", Decimal("0.45"), "RSI is neutral.")

    if indicators.atr_pct >= Decimal("2.5"):
        risk = ExpertSignal("risk_manager", "HOLD", Decimal("0.80"), "ATR percent is elevated; avoid new risk.")
    elif position and position.unrealized_pnl < 0 and abs(position.unrealized_pnl) > Decimal("5"):
        risk = ExpertSignal("risk_manager", "CLOSE", Decimal("0.65"), "Open position has notable unrealized loss.")
    else:
        risk = ExpertSignal("risk_manager", "HOLD", Decimal("0.45"), "Volatility and open risk are acceptable.")

    trend = ExpertSignal(
        "trend_follower",
        trend_action,
        trend_confidence,
        f"EMA fast {'above' if trend_action == 'BUY' else 'below' if trend_action == 'SELL' else 'near'} EMA slow.",
    )
    signals = [trend, breakout, mean_reversion, risk]

    if timeframe_indicators:
        trends = [value.trend for value in timeframe_indicators.values()]
        up_count = trends.count("up")
        down_count = trends.count("down")
        if up_count > down_count:
            consensus_action = "BUY"
            consensus_confidence = Decimal("0.50") + Decimal("0.08") * Decimal(up_count)
            reason = f"{up_count}/{len(trends)} analyzed timeframes trend up."
        elif down_count > up_count:
            consensus_action = "SELL"
            consensus_confidence = Decimal("0.50") + Decimal("0.08") * Decimal(down_count)
            reason = f"{down_count}/{len(trends)} analyzed timeframes trend down."
        else:
            consensus_action = "HOLD"
            consensus_confidence = Decimal("0.45")
            reason = "Analyzed timeframes are mixed."
        signals.append(
            ExpertSignal(
                "timeframe_consensus",
                consensus_action,
                min(consensus_confidence, Decimal("0.82")),
                reason,
            )
        )

    if regime:
        regime_action = "HOLD"
        if regime.name == "trending" and timeframe_indicators:
            trends = [value.trend for value in timeframe_indicators.values()]
            regime_action = "BUY" if trends.count("up") > trends.count("down") else "SELL"
        signals.append(
            ExpertSignal(
                "regime_detector",
                regime_action,
                regime.confidence,
                f"{regime.name}: {regime.reason} Preferred style: {regime.preferred_style}.",
            )
        )

    return signals


def _signal_payload(signals: list[ExpertSignal]) -> list[dict[str, str]]:
    return [
        {
            "name": signal.name,
            "action": signal.action,
            "confidence": str(signal.confidence),
            "reason": signal.reason,
        }
        for signal in signals
    ]


def _timeframe_payload(timeframe_indicators: dict[str, Indicators]) -> dict[str, dict[str, str]]:
    return {
        timeframe: {
            "close": str(indicators.close),
            "ema_fast": str(indicators.ema_fast),
            "ema_slow": str(indicators.ema_slow),
            "rsi": str(indicators.rsi),
            "atr_pct": str(indicators.atr_pct),
            "trend": indicators.trend,
        }
        for timeframe, indicators in timeframe_indicators.items()
    }


def _percent_distance(value: Decimal, reference: Decimal) -> Decimal:
    if reference <= 0:
        return Decimal("0")
    return (value - reference) / reference * Decimal("100")


def _parse_timeframes(value: str) -> list[str]:
    timeframes: list[str] = []
    for raw_timeframe in value.split(","):
        timeframe = raw_timeframe.strip()
        if timeframe and timeframe not in timeframes:
            timeframes.append(timeframe)
    return timeframes


def _parse_symbols(value: str) -> list[str]:
    symbols: list[str] = []
    for raw_symbol in value.split(","):
        symbol = raw_symbol.strip()
        if symbol and symbol not in symbols:
            symbols.append(symbol)
    return symbols


def _symbol_aliases(symbol: str) -> set[str]:
    upper = symbol.upper()
    return {
        upper,
        upper.replace("/", "").replace(":USDT", "").replace("-", ""),
        upper.replace("/", "").replace(":USDT", "").replace("-", "").replace("USDTUSDT", "USDT"),
    }


def _excluded_symbol_aliases(args: argparse.Namespace) -> set[str]:
    aliases: set[str] = set()
    for symbol in _parse_symbols(getattr(args, "exclude_symbols", "")):
        aliases.update(_symbol_aliases(symbol))
    return aliases


def _is_symbol_excluded(args: argparse.Namespace, symbol: str) -> bool:
    excluded = _excluded_symbol_aliases(args)
    if not excluded:
        return False
    return bool(_symbol_aliases(symbol) & excluded)


def _parse_models(value: str) -> list[str]:
    models: list[str] = []
    for raw_model in value.split(","):
        model = raw_model.strip()
        if model and model not in models:
            models.append(model)
    return models


def _model_chain(args: argparse.Namespace) -> list[str]:
    models = _parse_models(getattr(args, "models", ""))
    if models:
        return models
    return [getattr(args, "model", DEFAULT_MODEL)]


def _parse_model_spec(model: str) -> AIModelSpec:
    raw = model.strip()
    lowered = raw.lower()
    for prefix in ("qwen:", "qwen/"):
        if lowered.startswith(prefix):
            parsed_model = raw[len(prefix) :].strip()
            if not parsed_model:
                raise RuntimeError(f"Qwen OpenAI-compatible model spec {raw!r} is missing a model name")
            return AIModelSpec(provider="qwen", model=parsed_model, raw=raw)
    for prefix in ("openai-compatible:", "openai-compatible/", "openai:", "openai/"):
        if lowered.startswith(prefix):
            parsed_model = raw[len(prefix) :].strip()
            if not parsed_model:
                raise RuntimeError(f"OpenAI-compatible model spec {raw!r} is missing a model name")
            return AIModelSpec(provider="openai", model=parsed_model, raw=raw)
    for prefix in ("gemini:", "gemini/"):
        if lowered.startswith(prefix):
            parsed_model = raw[len(prefix) :].strip()
            if not parsed_model:
                raise RuntimeError(f"Gemini model spec {raw!r} is missing a model name")
            return AIModelSpec(provider="gemini", model=parsed_model, raw=raw)
    return AIModelSpec(provider="gemini", model=raw, raw=raw)


def _model_display_name(spec: AIModelSpec) -> str:
    return spec.raw if spec.raw != spec.model else f"{spec.provider}:{spec.model}"


def _ticker_quote_volume(ticker: dict[str, Any]) -> Decimal:
    info = ticker.get("info") or {}
    return _decimal(
        ticker.get("quoteVolume")
        or ticker.get("baseVolume")
        or info.get("quoteVolume")
        or info.get("volume")
        or "0"
    )


def _large_cap_usdm_symbols(exchange, limit: int) -> list[str]:
    candidates: list[str] = []
    for base in LARGE_CAP_USDM_BASES:
        requested = f"{base}/USDT:USDT"
        try:
            symbol = _coerce_symbol(requested, exchange.markets)
        except RuntimeError:
            continue
        market = exchange.markets.get(symbol, {})
        if market.get("active") is False:
            continue
        if not bool(market.get("swap")) or str(market.get("quote", "")).upper() != "USDT":
            continue
        if symbol not in candidates:
            candidates.append(symbol)

    if not candidates:
        raise RuntimeError("No large-cap USD-M futures markets were found.")

    try:
        tickers = exchange.fetch_tickers(candidates)
    except ccxt.BaseError as exc:
        print(f"Could not rank large-cap universe by volume; using default order: {exc}")
        return candidates[:limit]

    ranked = sorted(candidates, key=lambda symbol: _ticker_quote_volume(tickers.get(symbol) or {}), reverse=True)
    return ranked[:limit]


def _resolve_requested_symbols(exchange, args: argparse.Namespace) -> list[str]:
    if args.symbols:
        requested_symbols = _parse_symbols(args.symbols)
        symbols = [_coerce_symbol(symbol, exchange.markets) for symbol in requested_symbols]
    elif args.symbol_universe == "top-usdm-large-cap":
        symbols = _large_cap_usdm_symbols(exchange, args.universe_size)
    else:
        symbols = [_coerce_symbol(args.symbol, exchange.markets)]

    filtered = [symbol for symbol in symbols if not _is_symbol_excluded(args, symbol)]
    if not filtered:
        raise RuntimeError("All configured symbols were excluded by --exclude-symbols.")
    return filtered


def _fetch_timeframe_indicators(exchange, symbol: str, args: argparse.Namespace, primary: Indicators) -> dict[str, Indicators]:
    timeframe_indicators = {args.timeframe: primary}
    for timeframe in _parse_timeframes(args.analysis_timeframes):
        if timeframe == args.timeframe:
            continue
        try:
            candles = exchange.fetch_ohlcv(symbol, timeframe, limit=args.mtf_candle_limit)
            stale_reason = _stale_candle_reason(candles, timeframe, getattr(args, "max_data_age_seconds", Decimal("0")))
            if stale_reason:
                print(f"Skipping {timeframe} analysis candles because data is stale: {stale_reason}")
                continue
            timeframe_indicators[timeframe] = _calculate_indicators(candles, args.ema_fast, args.ema_slow)
        except ccxt.BaseError as exc:
            print(f"Could not fetch {timeframe} analysis candles; continuing without it: {exc}")
        except RuntimeError as exc:
            print(f"Could not calculate {timeframe} indicators; continuing without it: {exc}")
    return timeframe_indicators


def _detect_market_regime(
    indicators: Indicators,
    timeframe_indicators: dict[str, Indicators],
    high_volatility_atr_pct: Decimal,
    low_volatility_atr_pct: Decimal,
) -> MarketRegime:
    timeframes = list(timeframe_indicators.values()) or [indicators]
    trends = [item.trend for item in timeframes]
    up_count = trends.count("up")
    down_count = trends.count("down")
    neutral_rsi = Decimal("40") <= indicators.rsi <= Decimal("60")

    if indicators.atr_pct >= high_volatility_atr_pct:
        return MarketRegime(
            "high_volatility",
            "reduce_or_hold",
            Decimal("0.80"),
            f"ATR percent {indicators.atr_pct:.3f} is above high-volatility threshold {high_volatility_atr_pct}.",
        )

    if indicators.atr_pct <= low_volatility_atr_pct:
        return MarketRegime(
            "low_volatility",
            "wait_for_breakout",
            Decimal("0.70"),
            f"ATR percent {indicators.atr_pct:.3f} is below low-volatility threshold {low_volatility_atr_pct}.",
        )

    if max(up_count, down_count) >= max(2, len(timeframes) - 1):
        direction = "up" if up_count > down_count else "down"
        return MarketRegime(
            "trending",
            "follow_trend",
            Decimal("0.72"),
            f"{max(up_count, down_count)}/{len(timeframes)} analyzed timeframes trend {direction}.",
        )

    if neutral_rsi and indicators.atr_pct < Decimal("0.75"):
        return MarketRegime(
            "ranging",
            "mean_reversion",
            Decimal("0.62"),
            "RSI is neutral and volatility is moderate, suggesting range behavior.",
        )

    return MarketRegime(
        "mixed",
        "wait",
        Decimal("0.50"),
        "Trend, momentum, and volatility signals are mixed.",
    )


def _regime_payload(regime: MarketRegime) -> dict[str, str]:
    return {
        "name": regime.name,
        "preferred_style": regime.preferred_style,
        "confidence": str(regime.confidence),
        "reason": regime.reason,
    }


def _candidate_payload(candidates: list[SymbolCandidate]) -> list[dict[str, Any]]:
    return [
        {
            "symbol": candidate.symbol,
            "score": str(candidate.score),
            "market_regime": _regime_payload(candidate.regime),
            "primary_timeframe": {
                "close": str(candidate.indicators.close),
                "trend": candidate.indicators.trend,
                "rsi": str(candidate.indicators.rsi),
                "atr_pct": str(candidate.indicators.atr_pct),
            },
            "multi_timeframe": _timeframe_payload(candidate.timeframe_indicators),
        }
        for candidate in candidates
    ]


def _market_breadth_payload(symbol: str, candidates: list[SymbolCandidate] | None) -> dict[str, Any]:
    candidate_list = candidates or []
    if not candidate_list:
        return {
            "candidate_count": 0,
            "selected_rank": None,
            "score_gap_to_next": None,
            "trend_counts": {},
        }
    trend_counts: dict[str, int] = {}
    selected_rank: int | None = None
    for index, candidate in enumerate(candidate_list, start=1):
        trend_counts[candidate.indicators.trend] = trend_counts.get(candidate.indicators.trend, 0) + 1
        if candidate.symbol == symbol and selected_rank is None:
            selected_rank = index
    score_gap_to_next: Decimal | None = None
    if candidate_list and candidate_list[0].symbol == symbol and len(candidate_list) > 1:
        score_gap_to_next = candidate_list[0].score - candidate_list[1].score
    return {
        "candidate_count": len(candidate_list),
        "selected_rank": selected_rank,
        "selected_score": str(candidate_list[selected_rank - 1].score) if selected_rank else None,
        "score_gap_to_next": str(score_gap_to_next) if score_gap_to_next is not None else None,
        "trend_counts": trend_counts,
    }


def _market_structure_payload(
    indicators: Indicators,
    candles: list[list[Any]],
    timeframe_indicators: dict[str, Indicators],
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "ema_fast_distance_pct": str(_percent_distance(indicators.close, indicators.ema_fast)),
        "ema_slow_distance_pct": str(_percent_distance(indicators.close, indicators.ema_slow)),
        "rsi_overbought": indicators.rsi >= Decimal("70"),
        "rsi_oversold": indicators.rsi <= Decimal("30"),
        "timeframe_alignment": {
            "up": sum(1 for item in timeframe_indicators.values() if item.trend == "up"),
            "down": sum(1 for item in timeframe_indicators.values() if item.trend == "down"),
            "flat": sum(1 for item in timeframe_indicators.values() if item.trend == "flat"),
            "total": len(timeframe_indicators),
        },
    }
    if len(candles) >= 21:
        previous = candles[-21:-1]
        recent_high = max(_decimal(candle[2]) for candle in previous)
        recent_low = min(_decimal(candle[3]) for candle in previous)
        payload.update(
            {
                "previous_20_high": str(recent_high),
                "previous_20_low": str(recent_low),
                "distance_to_previous_20_high_pct": str(_percent_distance(indicators.close, recent_high)),
                "distance_to_previous_20_low_pct": str(_percent_distance(indicators.close, recent_low)),
                "breakout_state": (
                    "above_previous_20_high"
                    if indicators.close > recent_high
                    else "below_previous_20_low"
                    if indicators.close < recent_low
                    else "inside_previous_20_range"
                ),
            }
        )
    closes = [_decimal(candle[4]) for candle in candles]
    returns: dict[str, str] = {}
    for lookback in (3, 5, 10):
        if len(closes) > lookback and closes[-lookback - 1] > 0:
            returns[f"{lookback}_candle_return_pct"] = str(_percent_distance(closes[-1], closes[-lookback - 1]))
    if returns:
        payload["recent_returns"] = returns
    if candles:
        last_candle = candles[-1]
        candle_open = _decimal(last_candle[1])
        candle_high = _decimal(last_candle[2])
        candle_low = _decimal(last_candle[3])
        candle_range = candle_high - candle_low
        if candle_range > 0:
            payload["last_candle_body_to_range"] = str(abs(indicators.close - candle_open) / candle_range)
    return payload


def _symbol_setup_score(indicators: Indicators, timeframe_indicators: dict[str, Indicators], regime: MarketRegime) -> Decimal:
    trends = [item.trend for item in timeframe_indicators.values()] or [indicators.trend]
    consensus = Decimal(max(trends.count("up"), trends.count("down"))) / Decimal(len(trends))
    score = consensus * Decimal("0.50") + min(indicators.atr_pct / Decimal("2"), Decimal("1")) * Decimal("0.20")
    if regime.name == "trending":
        score += Decimal("0.25")
    elif regime.name == "ranging":
        score += Decimal("0.12")
    elif regime.name in {"high_volatility", "low_volatility"}:
        score -= Decimal("0.20")
    if indicators.rsi <= Decimal("25") or indicators.rsi >= Decimal("75"):
        score -= Decimal("0.10")
    return max(score, Decimal("0"))


def _risk_manager_veto(decision: MultiAgentDecision) -> str | None:
    if decision.final.action not in {"BUY", "SELL"}:
        return None
    for agent in decision.agents:
        if agent.name.lower() != "risk_manager":
            continue
        if agent.action == "HOLD" and agent.confidence >= Decimal("0.65"):
            return f"risk_manager veto: {agent.reason}"
        if agent.action == "CLOSE" and agent.confidence >= Decimal("0.60"):
            return f"risk_manager requested close instead of new entry: {agent.reason}"
    return None


def _should_retry_ai_error(exc: Exception) -> bool:
    message = str(exc)
    non_retryable_markers = (
        "429",
        "RESOURCE_EXHAUSTED",
        "Quota exceeded",
        "quota exceeded",
        "rate_limit_exceeded",
        "Rate limit",
        "rate limit",
        "Too Many Requests",
        "too many requests",
        "insufficient_quota",
        "INVALID_ARGUMENT",
        "not found for API version",
    )
    return not any(marker in message for marker in non_retryable_markers)


def _decision_prompt(
    indicators: Indicators,
    position: PositionSummary | None,
    symbol: str,
    expert_signals: list[ExpertSignal],
    timeframe_indicators: dict[str, Indicators],
    regime: MarketRegime,
    recent_reviews: list[dict[str, Any]],
    candidate_set: list[SymbolCandidate] | None = None,
    entry_aggressiveness: Decimal = Decimal("0.50"),
    context_payload: dict[str, Any] | None = None,
    is_screener: bool = False,
) -> dict[str, Any]:
    if is_screener:
        position_payload = {
            "has_position": False,
            "side": "flat",
            "amount": "0",
            "entry_price": "0",
            "unrealized_pnl": "0",
        }
        if position:
            position_payload = {
                "has_position": True,
                "side": position.side,
                "amount": str(position.amount),
                "entry_price": str(position.entry_price),
                "unrealized_pnl": str(position.unrealized_pnl),
            }
        prompt = {
            "role": "You are a fast market screener for a trading bot.",
            "task": (
                "Return only JSON with keys action, confidence, and reason. "
                "action must be BUY, SELL, HOLD, or CLOSE. "
                "Use BUY or SELL only if there is a strong trend-aligned signal from indicators and experts. "
                "Use CLOSE if current_position has_position is true and a clear reversal signal is present. "
                "Avoid repeating bad entry setups described in recent_reviews. "
                "Otherwise, return HOLD. "
                "Do not wrap the JSON in markdown. "
                "Example: {\"action\": \"HOLD\", \"confidence\": 0.95, \"reason\": \"...\"}"
            ),
            "symbol": symbol,
            "entry_aggressiveness": str(entry_aggressiveness),
            "indicators": {
                "close": str(indicators.close),
                "ema_fast": str(indicators.ema_fast),
                "ema_slow": str(indicators.ema_slow),
                "rsi": str(indicators.rsi),
                "atr": str(indicators.atr),
                "atr_pct": str(indicators.atr_pct),
                "trend": indicators.trend,
            },
            "market_regime": _regime_payload(regime),
            "expert_signals": _signal_payload(expert_signals),
            "current_position": position_payload,
            "recent_reviews": recent_reviews[-3:],
        }
        return prompt

    position_payload = {
        "has_position": False,
        "side": "flat",
        "amount": "0",
        "entry_price": "0",
        "unrealized_pnl": "0",
    }
    if position:
        position_payload = {
            "has_position": True,
            "side": position.side,
            "amount": str(position.amount),
            "entry_price": str(position.entry_price),
            "unrealized_pnl": str(position.unrealized_pnl),
        }

    prompt = {
        "role": "You are a multi-agent crypto futures trading committee. You do not bypass deterministic risk rules.",
        "task": (
            "Return only JSON with keys agents and final_decision. "
            "agents MUST be a JSON array, not an object/map. "
            "agents must contain exactly six objects with these names: market_analyst, trend_analyst, risk_manager, "
            "execution_manager, trade_reviewer, final_portfolio_manager. "
            "Each agent item must have name, action, confidence, reason. "
            "final_decision must have action, confidence, reason, and may include entry_plan. "
            "Do not wrap the JSON in markdown. Do not add prose before or after the JSON. "
            "Required shape example: "
            "{\"agents\":[{\"name\":\"market_analyst\",\"action\":\"HOLD\",\"confidence\":0.5,\"reason\":\"...\"}],"
            "\"final_decision\":{\"action\":\"HOLD\",\"confidence\":0.5,\"reason\":\"...\"}}. "
            "Actions must be BUY, SELL, HOLD, or CLOSE. Confidence must be 0.0 to 1.0. "
            "CLOSE should ONLY be used when: (1) the position is losing AND trend has clearly reversed on "
            "at least 2 higher timeframes, OR (2) the unrealized loss is significant relative to the stop-loss threshold. "
            "Do NOT close a position that is profitable or at breakeven unless there is a strong reversal signal "
            "on at least 2 higher timeframes. Prefer HOLD over CLOSE when the trend is still aligned with the "
            "position direction. A premature CLOSE at breakeven wastes fees and opportunity. "
            "For BUY/SELL, entry_plan may include entry_style, limit_price, stop_atr_multiplier, reward_risk, "
            "max_wait_minutes, and invalidation. entry_style must be market, marketable_limit, wait_pullback, "
            "wait_breakout, or default. Prefer wait_pullback instead of chasing when decision latency, spread, "
            "or overextension make the current bid/ask stale. Do not choose a price that requires ignoring "
            "deterministic spread, funding, fee, liquidity, or risk limits. "
            "The risk_manager has veto power; if risk is unclear, final_decision should be HOLD. "
            "Treat context.risk_context.global_consecutive_losses as a historical counter. It is a hard "
            "global-loss veto only when context.risk_context.global_loss_pause_active is true or "
            "context.risk_context.hard_entry_block_reason is non-empty. If the global loss pause is expired "
            "or none, do not HOLD solely because global_consecutive_losses is at max_consecutive_losses; "
            "judge the current setup using the remaining active risk and execution fields. "
            "The execution_manager should consider whether a market order is worth paying spread/slippage now. "
            "If final_decision.entry_plan includes limit_price, keep it within "
            "context.execution_context.ai_entry_limit_max_distance_pct of context.execution_context.last_price; "
            "otherwise omit limit_price or use default/marketable_limit. "
            "wait_pullback is not a persistent pending-entry workflow; it submits a short-lived limit order using "
            "context.execution_context.limit_entry_timeout_seconds and must satisfy "
            "context.execution_context.limit_entry_min_fill_ratio. If the trade should enter now, choose "
            "default or marketable_limit instead of a distant pullback limit. "
            "The trade_reviewer should use recent_reviews to avoid repeating weak setups. "
            "The final_portfolio_manager makes the final decision after reading all agents. "
            "candidate_set contains the best scanner candidates for this cycle; only trade the selected symbol "
            "if it is still clearly the best opportunity after risk and execution review. "
            "Use context.execution_context, context.risk_context, context.exit_plan_preview, "
            "context.performance_context, context.market_structure, context.market_breadth, "
            "context.entry_block_context, context.adversarial_debate (if active), and context.exchange_sentiment_context (if available) "
            "to judge whether the setup is worth taking now. "
            "current_position.has_position=false means the account is flat for this symbol; it is valid market data, "
            "not a missing or null input. "
            "entry_aggressiveness is 0.0 to 1.0. Higher values mean the committee should prefer a valid trend-aligned "
            "entry over HOLD when the account is flat, scanner score is strong, and deterministic risk gates are clear. "
            "Follow market_regime behavior: trending=follow trend; ranging=mean reversion; "
            "high_volatility=reduce or hold; low_volatility=wait for breakout only; mixed=hold unless clear."
        ),
        "symbol": symbol,
        "entry_aggressiveness": str(entry_aggressiveness),
        "candidate_set": _candidate_payload(candidate_set or []),
        "indicators": {
            "close": str(indicators.close),
            "ema_fast": str(indicators.ema_fast),
            "ema_slow": str(indicators.ema_slow),
            "rsi": str(indicators.rsi),
            "atr": str(indicators.atr),
            "atr_pct": str(indicators.atr_pct),
            "trend": indicators.trend,
        },
        "multi_timeframe": _timeframe_payload(timeframe_indicators),
        "market_regime": _regime_payload(regime),
        "expert_signals": _signal_payload(expert_signals),
        "current_position": position_payload,
        "recent_reviews": recent_reviews[-5:],
        "context": context_payload or {},
    }
    return prompt


def _gemini_completion_text(model_name: str, prompt: dict[str, Any], temperature: float, max_output_tokens: int) -> str:
    global _GEMINI_CLIENTS
    api_key = os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")
    if not api_key:
        raise RuntimeError("Missing GEMINI_API_KEY or GOOGLE_API_KEY for Gemini model.")

    from google import genai
    from google.genai import types

    if api_key not in _GEMINI_CLIENTS:
        _GEMINI_CLIENTS[api_key] = genai.Client(api_key=api_key)
    client = _GEMINI_CLIENTS[api_key]

    response = client.models.generate_content(
        model=model_name,
        contents=json.dumps(prompt),
        config=types.GenerateContentConfig(
            temperature=temperature,
            response_mime_type="application/json",
            max_output_tokens=max_output_tokens,
        ),
    )
    return getattr(response, "text", "") or ""


def _openai_compatible_completion_text(
    model_name: str,
    prompt: dict[str, Any],
    temperature: float,
    max_output_tokens: int,
    timeout_seconds: float,
    *,
    provider: str = "openai",
) -> str:
    global _OPENAI_CLIENTS
    if provider == "qwen":
        base_url = (
            os.environ.get("QWEN_OPENAI_COMPATIBLE_BASE_URL")
            or os.environ.get("QWEN_OPENAI_BASE_URL")
            or os.environ.get("QWEN_BASE_URL")
        )
        api_key = (
            os.environ.get("QWEN_OPENAI_COMPATIBLE_API_KEY")
            or os.environ.get("QWEN_OPENAI_API_KEY")
            or os.environ.get("QWEN_API_KEY")
        )
        missing_key_error = "Missing QWEN_OPENAI_COMPATIBLE_API_KEY or QWEN_API_KEY for Qwen advisor model."
    else:
        base_url = os.environ.get("OPENAI_COMPATIBLE_BASE_URL") or os.environ.get("OPENAI_BASE_URL")
        api_key = os.environ.get("OPENAI_COMPATIBLE_API_KEY") or os.environ.get("OPENAI_API_KEY")
        missing_key_error = "Missing OPENAI_COMPATIBLE_API_KEY or OPENAI_API_KEY for OpenAI-compatible model."
    if not api_key and base_url and base_url.startswith(("http://localhost", "http://127.0.0.1", "http://[::1]")):
        api_key = "EMPTY"
    if not api_key:
        raise RuntimeError(missing_key_error)

    client_key = (api_key, base_url, timeout_seconds)
    if client_key not in _OPENAI_CLIENTS:
        from openai import OpenAI
        client_args: dict[str, Any] = {"api_key": api_key, "timeout": timeout_seconds}
        if base_url:
            client_args["base_url"] = base_url
        _OPENAI_CLIENTS[client_key] = OpenAI(**client_args)
    client = _OPENAI_CLIENTS[client_key]

    prompt_text = (
        "Analyze this non-null trading payload. "
        "If current_position.has_position is false, the account is flat; do not treat that as missing input. "
        "Return only JSON using the requested schema.\n"
        f"{json.dumps(prompt)}"
    )
    completion_args: dict[str, Any] = {
        "model": model_name,
        "messages": [
            {
                "role": "system",
                "content": (
                    "You are a JSON-only trading committee. Return valid JSON and no markdown. "
                    "Use only BUY, SELL, HOLD, or CLOSE actions. Do not use WAIT."
                ),
            },
            {"role": "user", "content": prompt_text},
        ],
        "temperature": temperature,
        "max_tokens": max_output_tokens,
    }
    if provider == "qwen" or "qwen" in model_name.lower():
        if "thinking" in model_name.lower():
            completion_args["extra_body"] = {"enable_thinking": True}
        else:
            completion_args["extra_body"] = {"enable_thinking": False}

    # Enable stream=True to prevent Gemini/Web proxy connections from getting stuck
    completion_args["stream"] = True

    response = client.chat.completions.create(**completion_args)
    if not hasattr(response, "__iter__"):
        # Fallback for non-iterable response mocks in unit tests
        choices = getattr(response, "choices", None) or []
        if not choices:
            return ""
        message = getattr(choices[0], "message", None)
        if message is None:
            return ""
        return getattr(message, "content", "") or ""

    full_text = ""
    for chunk in response:
        choices = getattr(chunk, "choices", None) or []
        if choices:
            delta = getattr(choices[0], "delta", None)
            content = getattr(delta, "content", "") or ""
            full_text += content
    return full_text.strip()


def _ai_completion_text(
    spec: AIModelSpec,
    prompt: dict[str, Any],
    temperature: float,
    max_output_tokens: int,
    timeout_seconds: float,
) -> str:
    if spec.provider == "gemini":
        return _gemini_completion_text(spec.model, prompt, temperature, max_output_tokens)
    if spec.provider == "openai":
        return _openai_compatible_completion_text(spec.model, prompt, temperature, max_output_tokens, timeout_seconds)
    if spec.provider == "qwen":
        return _openai_compatible_completion_text(
            spec.model,
            prompt,
            temperature,
            max_output_tokens,
            timeout_seconds,
            provider="qwen",
        )
    raise RuntimeError(f"Unsupported AI provider {spec.provider!r}")


def _effective_multi_agent_decision(decision: MultiAgentDecision) -> EffectiveDecision:
    veto_reason = _risk_manager_veto(decision)
    if veto_reason:
        return EffectiveDecision(AIDecision("HOLD", Decimal("0"), veto_reason), veto_reason)
    return EffectiveDecision(decision.final)


def _bull_case_prompt(prompt: dict[str, Any]) -> dict[str, Any]:
    bull_prompt = dict(prompt)
    bull_prompt["role"] = "You are a highly biased crypto Bull Analyst."
    bull_prompt["task"] = (
        "Your sole task is to build the strongest possible BUY (Long) case for the selected symbol based on the provided indicators and timeframe data. "
        "Ignore the bearish signals or explain why they are irrelevant. Highlight support levels, bullish EMA setups, oversold RSI levels, and breakout opportunities. "
        "Return only JSON with keys: bull_thesis (string), bull_confidence (0.0 to 1.0), and key_buy_levels (list of strings). "
        "Do not wrap in markdown or add prose."
    )
    bull_prompt = dict(bull_prompt)
    bull_prompt.pop("recent_reviews", None)
    if "context" in bull_prompt and isinstance(bull_prompt["context"], dict):
        bull_prompt["context"] = {
            "market_structure": bull_prompt["context"].get("market_structure"),
            "exit_plan_preview": bull_prompt["context"].get("exit_plan_preview"),
        }
    return bull_prompt


def _bear_case_prompt(prompt: dict[str, Any]) -> dict[str, Any]:
    bear_prompt = dict(prompt)
    bear_prompt["role"] = "You are a highly biased crypto Bear Analyst."
    bear_prompt["task"] = (
        "Your sole task is to build the strongest possible SELL (Short) or HOLD case for the selected symbol based on the provided indicators and timeframe data. "
        "Ignore the bullish signals or explain why they are irrelevant. Highlight resistance levels, bearish EMA setups, overbought RSI levels, volatility risks, and liquidity issues. "
        "Return only JSON with keys: bear_thesis (string), bear_confidence (0.0 to 1.0), and key_sell_levels (list of strings). "
        "Do not wrap in markdown or add prose."
    )
    bear_prompt = dict(bear_prompt)
    bear_prompt.pop("recent_reviews", None)
    if "context" in bear_prompt and isinstance(bear_prompt["context"], dict):
        bear_prompt["context"] = {
            "market_structure": bear_prompt["context"].get("market_structure"),
            "exit_plan_preview": bear_prompt["context"].get("exit_plan_preview"),
        }
    return bear_prompt


def _gemini_decision(
    models: list[str] | str,
    indicators: Indicators,
    position: PositionSummary | None,
    symbol: str,
    expert_signals: list[ExpertSignal],
    timeframe_indicators: dict[str, Indicators],
    regime: MarketRegime,
    recent_reviews: list[dict[str, Any]],
    candidate_set: list[SymbolCandidate] | None = None,
    max_retries: int = 2,
    retry_delay_seconds: float = 1.0,
    request_timeout_seconds: float = 180.0,
    entry_aggressiveness: Decimal = Decimal("0.50"),
    context_payload: dict[str, Any] | None = None,
    adversarial_debate: bool = False,
    is_screener: bool = False,
) -> MultiAgentDecision:
    model_chain = [models] if isinstance(models, str) else list(models)
    if not model_chain:
        model_chain = [DEFAULT_MODEL]

    prompt = _decision_prompt(
        indicators,
        position,
        symbol,
        expert_signals,
        timeframe_indicators,
        regime,
        recent_reviews,
        candidate_set,
        entry_aggressiveness,
        context_payload,
        is_screener=is_screener,
    )

    if adversarial_debate and position is None:
        try:
            print("Adversarial Debate active: generating Bull and Bear theses...")
            spec = _parse_model_spec(model_chain[0])
            display_name = _model_display_name(spec)
            
            bull_prompt = _bull_case_prompt(prompt)
            bear_prompt = _bear_case_prompt(prompt)
            
            bull_text = _ai_completion_text(spec, bull_prompt, temperature=0.2, max_output_tokens=300, timeout_seconds=request_timeout_seconds)
            bear_text = _ai_completion_text(spec, bear_prompt, temperature=0.2, max_output_tokens=300, timeout_seconds=request_timeout_seconds)
            
            try:
                bull_json = json.loads(_strip_json_fence(bull_text))
            except Exception:
                bull_json = {"bull_thesis": bull_text, "bull_confidence": 0.5}
                
            try:
                bear_json = json.loads(_strip_json_fence(bear_text))
            except Exception:
                bear_json = {"bear_thesis": bear_text, "bear_confidence": 0.5}
                
            if "context" not in prompt:
                prompt["context"] = {}
            prompt["context"]["adversarial_debate"] = {
                "bull_case": bull_json,
                "bear_case": bear_json
            }
            print(f"Adversarial Debate generated. Bull confidence: {bull_json.get('bull_confidence')}, Bear confidence: {bear_json.get('bear_confidence')}")
        except Exception as e:
            print(f"Failed to generate adversarial debate: {e}; continuing with default prompt.")

    attempts = max(1, max_retries + 1)
    failures: list[str] = []
    for model_name in model_chain:
        spec = _parse_model_spec(model_name)
        display_name = _model_display_name(spec)
        for attempt in range(1, attempts + 1):
            try:
                print(f"AI decision request: model={display_name} attempt={attempt}/{attempts}")
                text = _ai_completion_text(
                    spec,
                    prompt,
                    temperature=0.2,
                    max_output_tokens=1800,
                    timeout_seconds=request_timeout_seconds,
                )
                return _parse_multi_agent_decision(text)
            except Exception as exc:
                failures.append(f"{display_name} attempt {attempt}: {exc}")
                if attempt < attempts and _should_retry_ai_error(exc):
                    print(
                        f"AI decision failed: model={display_name} attempt={attempt}/{attempts} "
                        f"error={exc}; retrying."
                    )
                    time.sleep(retry_delay_seconds * attempt)
                    continue
                print(f"AI decision model exhausted: model={display_name} error={exc}")
                break

    raise RuntimeError("AI decision failed for all configured models: " + " | ".join(failures[-5:]))


def _shadow_model_chain(args: argparse.Namespace) -> list[str]:
    return _parse_models(getattr(args, "shadow_models", ""))


def _advisor_model_chain(args: argparse.Namespace) -> list[str]:
    return _parse_models(getattr(args, "advisor_models", ""))


def _decision_state(decision: AIDecision) -> dict[str, Any]:
    return {
        "action": decision.action,
        "confidence": str(decision.confidence),
        "reason": decision.reason,
        "entry_plan": decision.entry_plan,
    }


def _record_model_comparisons(
    state: dict[str, Any],
    records: list[dict[str, Any]],
    *,
    last_key: str,
    history_key: str,
    event: str,
) -> None:
    if not records:
        return
    state[last_key] = records
    history = _bounded_history(state, history_key, MAX_SHADOW_COMPARISON_HISTORY)
    history.extend(records)
    del history[:-MAX_SHADOW_COMPARISON_HISTORY]
    _record_trade_journal_event(state, event, {"comparisons": records})


def _record_shadow_comparisons(state: dict[str, Any], records: list[dict[str, Any]]) -> None:
    _record_model_comparisons(
        state,
        records,
        last_key="last_shadow_comparisons",
        history_key="shadow_decision_comparisons",
        event="shadow_decision_comparison",
    )


def _record_advisor_comparisons(state: dict[str, Any], records: list[dict[str, Any]]) -> None:
    _record_model_comparisons(
        state,
        records,
        last_key="last_advisor_comparisons",
        history_key="advisor_decision_comparisons",
        event="advisor_decision_comparison",
    )


def _shadow_decision_comparisons(
    args: argparse.Namespace,
    state: dict[str, Any],
    symbol: str,
    final_decision: AIDecision,
    indicators: Indicators,
    position: PositionSummary | None,
    expert_signals: list[ExpertSignal],
    timeframe_indicators: dict[str, Indicators],
    regime: MarketRegime,
    recent_reviews: list[dict[str, Any]],
    candidate_set: list[SymbolCandidate] | None,
    ai_context: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    shadow_models = _shadow_model_chain(args)
    if not shadow_models:
        return []

    records: list[dict[str, Any]] = []
    for model_name in shadow_models:
        spec = _parse_model_spec(model_name)
        display_name = _model_display_name(spec)
        started = time.monotonic()
        record: dict[str, Any] = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "symbol": symbol,
            "model": display_name,
            "final_action": final_decision.action,
            "final_confidence": str(final_decision.confidence),
        }
        try:
            shadow_multi_agent_decision = _gemini_decision(
                [model_name],
                indicators,
                position,
                symbol,
                expert_signals,
                timeframe_indicators,
                regime,
                recent_reviews,
                candidate_set,
                getattr(args, "ai_max_retries", 2),
                getattr(args, "ai_retry_delay_seconds", 1.0),
                getattr(args, "ai_request_timeout_seconds", 180.0),
                getattr(args, "ai_entry_aggressiveness", Decimal("0.50")),
                ai_context,
            )
            effective = _effective_multi_agent_decision(shadow_multi_agent_decision)
            shadow_decision = effective.decision
            confidence_gap = shadow_decision.confidence - final_decision.confidence
            record.update(
                {
                    "status": "ok",
                    "latency_seconds": f"{time.monotonic() - started:.3f}",
                    "action": shadow_decision.action,
                    "confidence": str(shadow_decision.confidence),
                    "reason": shadow_decision.reason,
                    "entry_plan": shadow_decision.entry_plan,
                    "risk_veto_reason": effective.veto_reason,
                    "action_agreement": shadow_decision.action == final_decision.action,
                    "confidence_gap": str(confidence_gap),
                    "agents": [
                        {
                            "name": agent.name,
                            "action": agent.action,
                            "confidence": str(agent.confidence),
                            "reason": agent.reason,
                        }
                        for agent in shadow_multi_agent_decision.agents
                    ],
                }
            )
            print(
                f"shadow_ai model={display_name} status=ok action={shadow_decision.action} "
                f"confidence={shadow_decision.confidence} final_action={final_decision.action} "
                f"agreement={record['action_agreement']} latency_seconds={record['latency_seconds']} "
                f"reason={shadow_decision.reason}"
            )
        except Exception as exc:
            record.update(
                {
                    "status": "error",
                    "latency_seconds": f"{time.monotonic() - started:.3f}",
                    "error": str(exc)[:500],
                }
            )
            print(
                f"shadow_ai model={display_name} status=error "
                f"latency_seconds={record['latency_seconds']} error={exc}"
            )
        records.append(record)

    _record_shadow_comparisons(state, records)
    return records


def _advisor_decision_comparisons(
    args: argparse.Namespace,
    state: dict[str, Any],
    symbol: str,
    indicators: Indicators,
    position: PositionSummary | None,
    expert_signals: list[ExpertSignal],
    timeframe_indicators: dict[str, Indicators],
    regime: MarketRegime,
    recent_reviews: list[dict[str, Any]],
    candidate_set: list[SymbolCandidate] | None,
    ai_context: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    advisor_models = _advisor_model_chain(args)
    if not advisor_models:
        return []

    records: list[dict[str, Any]] = []
    for model_name in advisor_models:
        spec = _parse_model_spec(model_name)
        display_name = _model_display_name(spec)
        started = time.monotonic()
        record: dict[str, Any] = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "symbol": symbol,
            "model": display_name,
        }
        try:
            advisor_multi_agent_decision = _gemini_decision(
                [model_name],
                indicators,
                position,
                symbol,
                expert_signals,
                timeframe_indicators,
                regime,
                recent_reviews,
                candidate_set,
                getattr(args, "ai_max_retries", 2),
                getattr(args, "ai_retry_delay_seconds", 1.0),
                getattr(args, "ai_request_timeout_seconds", 180.0),
                getattr(args, "ai_entry_aggressiveness", Decimal("0.50")),
                ai_context,
            )
            effective = _effective_multi_agent_decision(advisor_multi_agent_decision)
            advisor_decision = effective.decision
            record.update(
                {
                    "status": "ok",
                    "latency_seconds": f"{time.monotonic() - started:.3f}",
                    "action": advisor_decision.action,
                    "confidence": str(advisor_decision.confidence),
                    "reason": advisor_decision.reason,
                    "entry_plan": advisor_decision.entry_plan,
                    "risk_veto_reason": effective.veto_reason,
                    "agents": [
                        {
                            "name": agent.name,
                            "action": agent.action,
                            "confidence": str(agent.confidence),
                            "reason": agent.reason,
                        }
                        for agent in advisor_multi_agent_decision.agents
                    ],
                }
            )
            print(
                f"advisor_ai model={display_name} status=ok action={advisor_decision.action} "
                f"confidence={advisor_decision.confidence} latency_seconds={record['latency_seconds']} "
                f"reason={advisor_decision.reason}"
            )
        except Exception as exc:
            record.update(
                {
                    "status": "error",
                    "latency_seconds": f"{time.monotonic() - started:.3f}",
                    "error": str(exc)[:500],
                }
            )
            print(
                f"advisor_ai model={display_name} status=error "
                f"latency_seconds={record['latency_seconds']} error={exc}"
            )
        records.append(record)

    _record_advisor_comparisons(state, records)
    return records


def _gemini_trade_review(
    models: list[str] | str,
    symbol: str,
    position: PositionSummary,
    exit_price: Decimal,
    exit_reason: str,
    open_trade: dict[str, Any],
    indicators: Indicators,
    timeframe_indicators: dict[str, Indicators],
    request_timeout_seconds: float = 180.0,
) -> dict[str, str]:
    if position.side == "long":
        pnl_pct = (exit_price - position.entry_price) / position.entry_price * Decimal("100")
    else:
        pnl_pct = (position.entry_price - exit_price) / position.entry_price * Decimal("100")

    prompt = {
        "role": "You are a strict post-trade reviewer for a crypto futures system.",
        "task": (
            "Return only JSON with keys quality, lesson, next_adjustment. "
            "quality must be good, mixed, or bad. Be concise and practical."
        ),
        "symbol": symbol,
        "exit_reason": exit_reason,
        "side": position.side,
        "entry_price": str(position.entry_price),
        "exit_price": str(exit_price),
        "estimated_pnl_pct_before_fees": str(pnl_pct),
        "open_trade_state": open_trade,
        "exit_indicators": {
            "trend": indicators.trend,
            "rsi": str(indicators.rsi),
            "atr_pct": str(indicators.atr_pct),
        },
        "multi_timeframe": _timeframe_payload(timeframe_indicators),
    }

    model_chain = [models] if isinstance(models, str) else list(models)
    if not model_chain:
        model_chain = [DEFAULT_MODEL]
    failures: list[str] = []
    text = "{}"
    for model_name in model_chain:
        spec = _parse_model_spec(model_name)
        display_name = _model_display_name(spec)
        try:
            print(f"Trade review request: model={display_name}")
            text = _ai_completion_text(
                spec,
                prompt,
                temperature=0.1,
                max_output_tokens=250,
                timeout_seconds=request_timeout_seconds,
            )
            break
        except Exception as exc:
            failures.append(f"{display_name}: {exc}")
            print(f"Trade review model failed: model={display_name} error={exc}")
    else:
        raise RuntimeError("Trade review failed for all configured models: " + " | ".join(failures[-5:]))

    try:
        payload = json.loads(_strip_json_fence(text))
    except json.JSONDecodeError:
        payload = {"quality": "mixed", "lesson": text[:300], "next_adjustment": "Review raw AI output."}
    return {
        "quality": str(payload.get("quality", "mixed"))[:50],
        "lesson": str(payload.get("lesson", ""))[:500],
        "next_adjustment": str(payload.get("next_adjustment", ""))[:500],
    }


def _init_lessons_db(db_path: str = "data/binance_futures_demo_ai_trader_lessons.db") -> None:
    try:
        db_dir = os.path.dirname(db_path)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS trade_lessons (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT,
                exit_reason TEXT,
                side TEXT,
                quality TEXT,
                lesson TEXT,
                next_adjustment TEXT,
                pnl_pct REAL,
                regime TEXT,
                timestamp TEXT
            )
        """)
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Error initializing lessons database: {e}")


def _save_lesson_to_db(review: dict[str, Any], db_path: str = "data/binance_futures_demo_ai_trader_lessons.db") -> None:
    try:
        _init_lessons_db(db_path)
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO trade_lessons (symbol, exit_reason, side, quality, lesson, next_adjustment, pnl_pct, regime, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            review.get("symbol"),
            review.get("exit_reason"),
            review.get("side"),
            review.get("quality"),
            review.get("lesson"),
            review.get("next_adjustment"),
            float(review.get("pnl_pct", 0.0)),
            review.get("regime"),
            datetime.now(timezone.utc).isoformat()
        ))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Error saving lesson to database: {e}")


def _query_relevant_lessons(symbol: str, regime: str, limit: int = 5, db_path: str = "data/binance_futures_demo_ai_trader_lessons.db") -> list[dict[str, Any]]:
    lessons = []
    try:
        if not os.path.exists(db_path):
            return lessons
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT symbol, exit_reason, side, quality, lesson, next_adjustment, pnl_pct, regime, timestamp
            FROM trade_lessons
            WHERE symbol = ? OR regime = ?
            ORDER BY (symbol = ?) DESC, timestamp DESC
            LIMIT ?
        """, (symbol, regime, symbol, limit))
        rows = cursor.fetchall()
        for row in rows:
            lessons.append({
                "symbol": row[0],
                "exit_reason": row[1],
                "side": row[2],
                "quality": row[3],
                "lesson": row[4],
                "next_adjustment": row[5],
                "pnl_pct": row[6],
                "regime": row[7],
                "reviewed_at": row[8]
            })
        conn.close()
    except Exception as e:
        print(f"Error querying lessons database: {e}")
    return lessons


def _load_state(path: Path) -> dict[str, Any]:
    candidates = [path, path.with_suffix(path.suffix + ".bak")]
    for candidate in candidates:
        if not candidate.exists():
            continue
        try:
            payload = json.loads(candidate.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            print(f"State file is not valid JSON: {candidate}; trying backup.")
            continue
        if not isinstance(payload, dict):
            print(f"State file did not contain a JSON object: {candidate}; trying backup.")
            continue
        if candidate != path:
            print(f"Recovered state from backup: {candidate}")
        return payload
    return {}


def _save_state(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(state, indent=2, sort_keys=True)
    backup_path = path.with_suffix(path.suffix + ".bak")
    tmp_path = path.with_suffix(path.suffix + f".tmp.{os.getpid()}")
    if path.exists():
        try:
            existing = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(existing, dict):
                backup_path.write_text(json.dumps(existing, indent=2, sort_keys=True), encoding="utf-8")
        except json.JSONDecodeError:
            pass
    tmp_path.write_text(payload, encoding="utf-8")
    tmp_path.replace(path)


def _acquire_process_lock(path: Path | None) -> None:
    global _PROCESS_LOCK_HANDLE
    if path is None:
        return
    if _PROCESS_LOCK_HANDLE is not None:
        return
    try:
        import fcntl
    except ImportError:  # pragma: no cover - fcntl is expected on Linux VPS deployments.
        print("Process lock unavailable on this platform; continuing without a single-instance guard.")
        return

    path.parent.mkdir(parents=True, exist_ok=True)
    handle = path.open("a+", encoding="utf-8")
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError as exc:
        handle.seek(0)
        existing = handle.read().strip()
        handle.close()
        raise RuntimeError(
            f"Another Binance futures AI trader process is already running. "
            f"lock_file={path} existing={existing or 'unknown'}"
        ) from exc

    handle.seek(0)
    handle.truncate()
    handle.write(f"pid={os.getpid()} acquired_at={datetime.now(timezone.utc).isoformat()}\n")
    handle.flush()
    _PROCESS_LOCK_HANDLE = handle
    print(f"process_lock_acquired path={path}")


def _normalize_runtime_config_key(key: str) -> str:
    return key.strip().lstrip("-").replace("-", "_")


def _coerce_runtime_config_value(current_value: Any, new_value: Any) -> Any:
    if isinstance(current_value, bool):
        if isinstance(new_value, bool):
            return new_value
        if isinstance(new_value, str):
            lowered = new_value.strip().lower()
            if lowered in {"1", "true", "yes", "on"}:
                return True
            if lowered in {"0", "false", "no", "off"}:
                return False
        raise RuntimeError(f"Cannot coerce {new_value!r} to bool")
    if isinstance(current_value, Decimal):
        return _decimal(new_value)
    if isinstance(current_value, int) and not isinstance(current_value, bool):
        return int(new_value)
    if isinstance(current_value, float):
        return float(new_value)
    if isinstance(current_value, Path):
        return Path(str(new_value))
    return str(new_value)


def _read_runtime_config(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError("--runtime-config-file must contain a JSON object")
    config = payload.get("trading", payload)
    if not isinstance(config, dict):
        raise RuntimeError("--runtime-config-file trading key must contain a JSON object")
    return config


def _apply_runtime_config(args: argparse.Namespace, hot_reload: bool = False) -> bool:
    config_file = getattr(args, "runtime_config_file", None)
    if not config_file:
        return False
    path = Path(config_file)
    if not path.exists():
        raise RuntimeError(f"--runtime-config-file does not exist: {path}")

    mtime = path.stat().st_mtime
    if hot_reload and getattr(args, "_runtime_config_mtime", None) == mtime:
        return False

    config = _read_runtime_config(path)
    applied: list[str] = []
    skipped: list[str] = []
    for raw_key, value in config.items():
        key = _normalize_runtime_config_key(str(raw_key))
        if key in RUNTIME_CONFIG_PROTECTED_KEYS:
            skipped.append(key)
            continue
        if hot_reload and key not in RUNTIME_CONFIG_HOT_KEYS:
            skipped.append(key)
            continue
        if not hasattr(args, key):
            skipped.append(key)
            continue
        setattr(args, key, _coerce_runtime_config_value(getattr(args, key), value))
        applied.append(key)

    args._runtime_config_mtime = mtime
    if applied:
        mode = "hot_reload" if hot_reload else "startup"
        print(f"runtime_config_loaded mode={mode} path={path} applied={','.join(sorted(applied))}")
    if skipped and not hot_reload:
        print(f"runtime_config_skipped path={path} skipped={','.join(sorted(set(skipped)))}")
    return bool(applied)


def _snapshot_args(args: argparse.Namespace) -> dict[str, Any]:
    return dict(vars(args))


def _restore_args(args: argparse.Namespace, snapshot: dict[str, Any]) -> None:
    current_keys = set(vars(args))
    snapshot_keys = set(snapshot)
    for key in current_keys - snapshot_keys:
        delattr(args, key)
    for key, value in snapshot.items():
        setattr(args, key, value)


def _runtime_config_error_state(state: dict[str, Any]) -> dict[str, Any]:
    error_state = state.setdefault("runtime_config_errors", {})
    if not isinstance(error_state, dict):
        error_state = {}
        state["runtime_config_errors"] = error_state
    return error_state


def _record_runtime_config_error(state: dict[str, Any], args: argparse.Namespace, exc: Exception) -> None:
    error_state = _runtime_config_error_state(state)
    now = _now_ts()
    consecutive = int(_decimal(error_state.get("consecutive"))) + 1
    error_state["consecutive"] = consecutive
    error_state["last_error_ts"] = str(now)
    error_state["last_error_type"] = type(exc).__name__
    error_state["last_error"] = str(exc)[:500]
    print(f"runtime_config_error consecutive={consecutive} error={exc}")

    cooldown = _seconds(getattr(args, "exchange_error_alert_cooldown_minutes", Decimal("30")))
    last_alert = _decimal(error_state.get("last_alert_ts"))
    if cooldown > 0 and last_alert > 0 and now - last_alert < cooldown:
        return
    _notify(args, "risk", f"Runtime config hot reload failed; keeping previous valid config: {str(exc)[:300]}")
    error_state["last_alert_ts"] = str(now)


def _clear_runtime_config_error_streak(state: dict[str, Any]) -> None:
    error_state = state.get("runtime_config_errors")
    if not isinstance(error_state, dict):
        return
    previous = int(_decimal(error_state.get("consecutive")))
    if previous <= 0:
        return
    error_state["last_recovery_ts"] = str(_now_ts())
    error_state["previous_consecutive"] = previous
    error_state["consecutive"] = 0


def _safe_hot_reload_runtime_config(args: argparse.Namespace, state: dict[str, Any]) -> bool:
    snapshot = _snapshot_args(args)
    try:
        runtime_changed = _apply_runtime_config(args, hot_reload=True)
        if runtime_changed:
            _validate_args(args)
            _clear_runtime_config_error_streak(state)
        return runtime_changed
    except Exception as exc:
        _restore_args(args, snapshot)
        _record_runtime_config_error(state, args, exc)
        return False


def _update_daily_state(state: dict[str, Any], equity: Decimal) -> dict[str, Any]:
    today = datetime.now(timezone.utc).date().isoformat()
    if state.get("day") != today:
        state["day"] = today
        state["start_equity"] = str(equity)
    state["last_equity"] = str(equity)
    return state


def _daily_loss_exceeded(state: dict[str, Any], equity: Decimal, max_daily_loss_pct: Decimal) -> bool:
    start_equity = _decimal(state.get("start_equity"), default=str(equity))
    if start_equity <= 0:
        return False
    drawdown_pct = (start_equity - equity) / start_equity * Decimal("100")
    return drawdown_pct >= max_daily_loss_pct


def _now_ts() -> Decimal:
    return Decimal(str(time.time()))


def _seconds(minutes: Decimal) -> Decimal:
    return minutes * Decimal("60")


def _milliseconds(minutes: Decimal) -> Decimal:
    return _seconds(minutes) * Decimal("1000")


def _now_ms() -> int:
    return int(time.time() * 1000)


def _bounded_history(state: dict[str, Any], key: str, limit: int) -> list[Any]:
    history = state.setdefault(key, [])
    if not isinstance(history, list):
        history = []
        state[key] = history
    del history[:-limit]
    return history


def _record_trade_journal_event(state: dict[str, Any], event: str, payload: dict[str, Any]) -> None:
    journal = _bounded_history(state, "trade_journal", MAX_TRADE_JOURNAL_HISTORY)
    journal.append(
        {
            "ts": str(_now_ts()),
            "event": event,
            **payload,
        }
    )
    del journal[:-MAX_TRADE_JOURNAL_HISTORY]


def _symbol_control(state: dict[str, Any], symbol: str) -> dict[str, Any]:
    controls = state.setdefault("symbol_controls", {})
    if not isinstance(controls, dict):
        controls = {}
        state["symbol_controls"] = controls
    control = controls.setdefault(symbol, {})
    if not isinstance(control, dict):
        control = {}
        controls[symbol] = control
    return control


def _global_control(state: dict[str, Any]) -> dict[str, Any]:
    control = state.setdefault("global_control", {})
    if not isinstance(control, dict):
        control = {}
        state["global_control"] = control
    return control


def _symbol_entry_block_reason(state: dict[str, Any], symbol: str) -> str | None:
    control = _symbol_control(state, symbol)
    now = _now_ts()
    blacklist_until = _decimal(control.get("blacklist_until"))
    if blacklist_until > now:
        return f"symbol temporarily blacklisted until {datetime.fromtimestamp(float(blacklist_until), timezone.utc).isoformat()}"
    cooldown_until = _decimal(control.get("cooldown_until"))
    if cooldown_until > now:
        return f"symbol cooldown until {datetime.fromtimestamp(float(cooldown_until), timezone.utc).isoformat()}"
    return None


def _global_entry_block_reason(state: dict[str, Any]) -> str | None:
    control = _global_control(state)
    pause_until = _decimal(control.get("pause_until"))
    if pause_until > _now_ts():
        return f"global loss pause until {datetime.fromtimestamp(float(pause_until), timezone.utc).isoformat()}"
    return None


def _notification_events(args: argparse.Namespace) -> set[str]:
    return {event.strip().lower() for event in getattr(args, "notify_events", "").split(",") if event.strip()}


_NOTIFICATION_EVENT_LABELS = {
    "entry": "Entry opened",
    "exit": "Position closed",
    "risk": "Risk alert",
    "performance": "Performance update",
    "health": "Health check",
}

_NOTIFICATION_DETAIL_LABELS = {
    "amount": "Amount",
    "entry": "Entry price",
    "entry_price": "Entry price",
    "exit_price": "Exit price",
    "estimated_notional": "Estimated notional",
    "estimated_pnl_before_fees": "Estimated PnL before fees",
    "estimated_pnl_usdt_before_fees": "Estimated PnL before fees",
    "stop_loss_pct": "Stop loss",
    "take_profit_pct": "Take profit",
    "take_profit_price": "Take profit price",
    "reward_risk": "Reward/risk",
    "reason": "Reason",
    "side": "Side",
    "spread_pct": "Spread",
    "funding_rate_pct": "Funding rate",
    "adaptive_risk_multiplier": "Risk multiplier",
    "symbol_recent_pnl": "Symbol recent PnL",
    "binance_net_income": "Binance net income",
    "stop_price": "Stop price",
    "symbol": "Symbol",
    "trades": "Trades",
    "wins": "Wins",
    "losses": "Losses",
    "win_rate": "Win rate",
    "equity": "Equity",
    "open_trade": "Open trade",
    "new_entries_enabled": "New entries",
    "emergency_close_positions": "Emergency close",
    "exchange_error_consecutive": "Exchange errors",
    "runtime_config_error_consecutive": "Runtime config errors",
    "mode": "Mode",
    "min_confidence": "Min confidence",
    "profit_factor": "Profit factor",
    "gross_win": "Gross win",
    "gross_loss": "Gross loss",
    "reasons": "Reasons",
    "overrides": "Overrides",
    "ai_entry_aggressiveness": "AI aggressiveness",
    "symbol_cooldown_minutes": "Symbol cooldown",
    "loss_symbol_cooldown_minutes": "Loss cooldown",
    "trailing_activation_pct": "Trailing activation",
    "trailing_distance_pct": "Trailing distance",
}

_KEY_VALUE_PATTERN = re.compile(r"([A-Za-z_][A-Za-z0-9_]*)=([^=]*?)(?=\s+[A-Za-z_][A-Za-z0-9_]*=|$)")
_MARKDOWN_V2_SPECIALS = r"_*[]()~`>#+-=|{}.!"


def _notification_event_label(event: str) -> str:
    return _NOTIFICATION_EVENT_LABELS.get(event.lower(), event.replace("_", " ").title())


def _notification_detail_label(key: str) -> str:
    return _NOTIFICATION_DETAIL_LABELS.get(key, key.replace("_", " ").title())


def _compact_notification_value(value: Any, limit: int = 260) -> str:
    text = str(value).strip()
    if len(text) <= limit:
        return text
    return f"{text[: limit - 3]}..."


def _format_notification_decimal(value: Any, places: str = "0.01") -> str:
    decimal = _decimal(value)
    if decimal == 0 and str(value).strip() not in {"0", "0.0", "0.00", "0E-12"}:
        return str(value)
    quantized = decimal.quantize(Decimal(places))
    return format(quantized.normalize(), "f")


def _format_notification_percent(value: Any) -> str:
    decimal = _decimal(value)
    if decimal == 0 and str(value).strip() not in {"0", "0.0", "0.00"}:
        return str(value)
    return f"{(decimal * Decimal('100')).quantize(Decimal('0.1'))}%"


def _format_auto_tune_reason(reason: Any) -> str:
    text = str(reason).strip()
    replacements = {
        "tightened_after_weak_recent_performance": "tightened after weak recent performance",
        "relaxed_after_positive_recent_performance": "relaxed after positive recent performance",
        "improved_reward_risk_after_large_average_loss": "improved reward/risk after large average loss",
        "softened_after_idle_blocks_under_weak_performance": "softened after idle blocks under weak performance",
    }
    for needle, replacement in replacements.items():
        text = text.replace(needle, replacement)
    return text.replace("_", " ")


def _format_auto_tune_overrides(overrides: Any) -> str:
    if not isinstance(overrides, dict) or not overrides:
        return "none"
    preferred_keys = (
        "min_confidence",
        "ai_entry_aggressiveness",
        "stop_loss_pct",
        "take_profit_pct",
        "trailing_activation_pct",
        "trailing_distance_pct",
        "symbol_cooldown_minutes",
        "loss_symbol_cooldown_minutes",
        "max_spread_pct",
        "min_reward_to_fee_ratio",
        "max_entry_funding_cost_pct",
    )
    parts = []
    for key in preferred_keys:
        if key not in overrides:
            continue
        label = _notification_detail_label(key)
        value = overrides[key]
        if key.endswith("_pct") or key in {"stop_loss_pct", "take_profit_pct", "max_spread_pct", "max_entry_funding_cost_pct"}:
            value_text = f"{_format_notification_decimal(value, '0.0001')}%"
        elif key.endswith("_minutes"):
            value_text = f"{_format_notification_decimal(value, '0.01')}m"
        else:
            value_text = _format_notification_decimal(value, "0.0001")
        parts.append(f"{label} {value_text}")
    return "; ".join(parts) if parts else "none"


def _auto_tune_notification_message(summary: dict[str, Any]) -> str:
    mode = str(summary.get("mode", "unknown"))
    details = [
        f"mode={mode}",
        f"trades={summary.get('trades', 0)}",
    ]
    if "min_trades" in summary:
        details.append(f"min_trades={summary.get('min_trades')}")
    if "win_rate" in summary:
        details.append(f"win_rate={_format_notification_percent(summary.get('win_rate'))}")
    if "profit_factor" in summary:
        details.append(f"profit_factor={_format_notification_decimal(summary.get('profit_factor'), '0.01')}")
    if "gross_win" in summary:
        details.append(f"gross_win={_format_notification_decimal(summary.get('gross_win'), '0.0001')} USDT")
    if "gross_loss" in summary:
        details.append(f"gross_loss={_format_notification_decimal(summary.get('gross_loss'), '0.0001')} USDT")
    reasons = summary.get("reasons") if isinstance(summary.get("reasons"), list) else []
    if reasons:
        reason_text = "; ".join(_format_auto_tune_reason(reason) for reason in reasons[:4])
        details.append(f"reasons={reason_text}")
    details.append(f"overrides={_format_auto_tune_overrides(summary.get('overrides'))}")
    return "Auto-tune updated " + " ".join(details)


def _parse_notification_message(message: str) -> tuple[str, list[tuple[str, str]]]:
    matches = list(_KEY_VALUE_PATTERN.finditer(message))
    if not matches:
        return message.strip(), []

    summary = message[: matches[0].start()].strip()
    details = []
    for match in matches:
        details.append((match.group(1), _compact_notification_value(match.group(2))))
    return summary, details


def _telegram_markdown_escape(text: Any) -> str:
    value = str(text)
    return "".join(f"\\{char}" if char in _MARKDOWN_V2_SPECIALS else char for char in value)


def _telegram_code_escape(text: Any) -> str:
    return str(text).replace("\\", "\\\\").replace("`", "\\`")


def _telegram_notification_text(event: str, message: str) -> str:
    summary, details = _parse_notification_message(message)
    lines = [
        "*LumiBot Binance Futures*",
        f"*Event:* {_telegram_markdown_escape(_notification_event_label(event))}",
    ]

    if summary:
        lines.extend(["", f"*Summary:* {_telegram_markdown_escape(_compact_notification_value(summary, 500))}"])

    if details:
        lines.extend(["", "*Details:*"])
        for key, value in details[:16]:
            label = _telegram_markdown_escape(_notification_detail_label(key))
            lines.append(f"\\- {label}: `{_telegram_code_escape(value)}`")

    return "\n".join(lines)[:3900]


def _notify(args: argparse.Namespace, event: str, message: str) -> None:
    events = _notification_events(args)
    if not events:
        return
    if events and event.lower() not in events and "all" not in events:
        return

    discord_url = getattr(args, "discord_webhook_url", "") or os.environ.get("DISCORD_WEBHOOK_URL", "")
    telegram_token = getattr(args, "telegram_bot_token", "") or os.environ.get("TELEGRAM_BOT_TOKEN", "")
    telegram_chat_id = getattr(args, "telegram_chat_id", "") or os.environ.get("TELEGRAM_CHAT_ID", "")
    if not discord_url and not (telegram_token and telegram_chat_id):
        return

    text = f"[lumibot-binance-futures:{event}] {message}"[:1800]
    if discord_url:
        try:
            request = urllib.request.Request(
                discord_url,
                data=json.dumps({"content": text}).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(request, timeout=10):
                pass
        except Exception as exc:
            print(f"Notification failed for Discord: {exc}")

    if telegram_token and telegram_chat_id:
        try:
            data = urllib.parse.urlencode(
                {
                    "chat_id": telegram_chat_id,
                    "text": _telegram_notification_text(event, message),
                    "parse_mode": "MarkdownV2",
                    "disable_web_page_preview": "true",
                }
            ).encode("utf-8")
            request = urllib.request.Request(
                f"https://api.telegram.org/bot{telegram_token}/sendMessage",
                data=data,
                method="POST",
            )
            with urllib.request.urlopen(request, timeout=10):
                pass
        except Exception as exc:
            print(f"Notification failed for Telegram: {exc}")


def _record_entry_block(state: dict[str, Any], symbol: str, category: str, reason: str) -> None:
    events = state.setdefault("entry_block_events", [])
    if not isinstance(events, list):
        events = []
        state["entry_block_events"] = events
    now = _now_ts()
    events.append(
        {
            "ts": str(now),
            "symbol": symbol,
            "category": category,
            "reason": reason[:300],
        }
    )
    del events[:-MAX_ENTRY_BLOCK_EVENT_HISTORY]


def _recent_entry_block_counts(state: dict[str, Any], since_ts: Decimal) -> dict[str, int]:
    events = state.get("entry_block_events") if isinstance(state.get("entry_block_events"), list) else []
    counts: dict[str, int] = {}
    for event in events:
        if not isinstance(event, dict):
            continue
        event_ts = _decimal(event.get("ts"))
        if event_ts <= 0 or event_ts < since_ts:
            continue
        category = str(event.get("category") or "unknown")
        counts[category] = counts.get(category, 0) + 1
    return counts


def _trade_pnl(position: PositionSummary, exit_price: Decimal) -> Decimal:
    if position.side == "long":
        return (exit_price - position.entry_price) * position.amount
    return (position.entry_price - exit_price) * position.amount


def _record_trade_outcome(
    state: dict[str, Any],
    args: argparse.Namespace,
    symbol: str,
    position: PositionSummary,
    exit_price: Decimal,
    exit_reason: str,
    open_trade: dict[str, Any] | None = None,
    binance_report: dict[str, str] | None = None,
) -> Decimal:
    pnl = _trade_pnl(position, exit_price)
    now = _now_ts()
    record = {
        "closed_at": datetime.fromtimestamp(float(now), timezone.utc).isoformat(),
        "symbol": symbol,
        "side": position.side,
        "amount": str(position.amount),
        "entry_price": str(position.entry_price),
        "exit_price": str(exit_price),
        "estimated_pnl_usdt_before_fees": str(pnl),
        "exit_reason": exit_reason,
    }
    if binance_report:
        record.update(binance_report)
    if open_trade:
        record["entry_decision"] = open_trade.get("entry_decision")
        record["entry_agents"] = open_trade.get("entry_agents")
        record["entry_timeframes"] = open_trade.get("entry_timeframes")
        record["entry_advisor_comparisons"] = open_trade.get("entry_advisor_comparisons")
        record["entry_shadow_comparisons"] = open_trade.get("entry_shadow_comparisons")
        record["exit_plan"] = open_trade.get("exit_plan")
        record["native_protection"] = open_trade.get("native_protection")
    closed_trades = state.setdefault("closed_trades", [])
    if not isinstance(closed_trades, list):
        closed_trades = []
        state["closed_trades"] = closed_trades
    closed_trades.append(record)
    del closed_trades[:-MAX_CLOSED_TRADE_HISTORY]

    symbol_control = _symbol_control(state, symbol)
    global_control = _global_control(state)
    pnl_for_controls = _decimal(
        record.get("binance_net_pnl_usdt")
        or record.get("binance_realized_pnl_usdt")
        or record.get("estimated_pnl_usdt_before_fees"),
        default=str(pnl),
    )
    _update_agent_scorecard(state, open_trade, position, pnl_for_controls)
    _update_adaptive_risk_multiplier(state, args, pnl_for_controls)

    if pnl_for_controls < 0:
        symbol_losses = int(_decimal(symbol_control.get("consecutive_losses")))
        global_losses = int(_decimal(global_control.get("consecutive_losses")))
        symbol_losses += 1
        global_losses += 1
        symbol_control["consecutive_losses"] = str(symbol_losses)
        global_control["consecutive_losses"] = str(global_losses)
        symbol_control["cooldown_until"] = str(now + _seconds(args.loss_symbol_cooldown_minutes))

        if symbol_losses >= args.auto_blacklist_loss_count:
            symbol_control["blacklist_until"] = str(now + _seconds(args.auto_blacklist_minutes))
            print(
                f"Performance guard: blacklisting {symbol} after {symbol_losses} consecutive losses "
                f"until {datetime.fromtimestamp(float(_decimal(symbol_control['blacklist_until'])), timezone.utc).isoformat()}."
            )
            _notify(args, "risk", f"Blacklisting {symbol} after {symbol_losses} consecutive losses.")
        if global_losses >= args.max_consecutive_losses:
            global_control["pause_until"] = str(now + _seconds(args.loss_pause_minutes))
            print(
                f"Performance guard: pausing new entries after {global_losses} consecutive losses "
                f"until {datetime.fromtimestamp(float(_decimal(global_control['pause_until'])), timezone.utc).isoformat()}."
            )
            _notify(args, "risk", f"Pausing new entries after {global_losses} consecutive losses.")
    else:
        symbol_control["consecutive_losses"] = "0"
        global_control["consecutive_losses"] = "0"
        symbol_control["cooldown_until"] = str(now + _seconds(args.symbol_cooldown_minutes))

    print(
        f"trade_outcome symbol={symbol} side={position.side} exit_reason={exit_reason} "
        f"estimated_pnl_usdt_before_fees={pnl:.4f} pnl_for_controls={pnl_for_controls:.4f}"
    )
    performance = _performance_context_payload(state, symbol)
    _record_trade_journal_event(
        state,
        "exit",
        {
            **record,
            "global_consecutive_losses": global_control.get("consecutive_losses", "0"),
            "symbol_consecutive_losses": symbol_control.get("consecutive_losses", "0"),
            "performance_context": performance,
        },
    )
    _notify(
        args,
        "exit",
        f"{symbol} {position.side} closed reason={exit_reason} estimated_pnl_before_fees={pnl:.4f} USDT "
        f"binance_net_pnl={record.get('binance_net_pnl_usdt', 'n/a')} "
        f"symbol_recent_pnl={performance['symbol_recent']['estimated_pnl_usdt_before_fees']} "
        f"binance_net_income={performance['binance_performance_summary']['net_income_usdt']} "
        f"global_consecutive_losses={global_control.get('consecutive_losses', 0)}",
    )
    return pnl


def _agent_action_aligned_with_trade(agent_action: str, position: PositionSummary, pnl: Decimal) -> bool | None:
    action = agent_action.upper()
    if action not in {"BUY", "SELL", "HOLD", "CLOSE"}:
        return None
    trade_action = "BUY" if position.side == "long" else "SELL"
    if action == trade_action:
        return pnl > 0
    if action in {"HOLD", "CLOSE"}:
        return pnl <= 0
    return pnl <= 0


def _update_agent_scorecard(
    state: dict[str, Any],
    open_trade: dict[str, Any] | None,
    position: PositionSummary,
    pnl: Decimal,
) -> None:
    if not open_trade:
        return
    raw_agents = open_trade.get("entry_agents")
    if not isinstance(raw_agents, list):
        return

    scorecard = state.setdefault("agent_scorecard", {})
    if not isinstance(scorecard, dict):
        scorecard = {}
        state["agent_scorecard"] = scorecard

    for raw_agent in raw_agents:
        if not isinstance(raw_agent, dict):
            continue
        name = str(raw_agent.get("name") or "unknown")[:80]
        aligned = _agent_action_aligned_with_trade(str(raw_agent.get("action") or ""), position, pnl)
        if aligned is None:
            continue
        stats = scorecard.setdefault(
            name,
            {
                "opinions": 0,
                "aligned_with_outcome": 0,
                "winning_trade_opinions": 0,
                "losing_trade_opinions": 0,
                "confidence_sum": "0",
            },
        )
        opinions = int(_decimal(stats.get("opinions"))) + 1
        aligned_count = int(_decimal(stats.get("aligned_with_outcome"))) + (1 if aligned else 0)
        stats["opinions"] = opinions
        stats["aligned_with_outcome"] = aligned_count
        stats["winning_trade_opinions"] = int(_decimal(stats.get("winning_trade_opinions"))) + (1 if pnl > 0 else 0)
        stats["losing_trade_opinions"] = int(_decimal(stats.get("losing_trade_opinions"))) + (1 if pnl < 0 else 0)
        stats["confidence_sum"] = str(_decimal(stats.get("confidence_sum")) + _decimal(raw_agent.get("confidence")))
        stats["alignment_rate"] = str(Decimal(aligned_count) / Decimal(opinions))


def _update_adaptive_risk_multiplier(state: dict[str, Any], args: argparse.Namespace, pnl: Decimal) -> None:
    if not getattr(args, "adaptive_risk", False):
        return
    multiplier = _decimal(state.get("adaptive_risk_multiplier"), default="1")
    step = getattr(args, "adaptive_risk_step", Decimal("0.10"))
    min_multiplier = getattr(args, "adaptive_risk_min_multiplier", Decimal("0.50"))
    max_multiplier = getattr(args, "adaptive_risk_max_multiplier", Decimal("1.00"))
    if pnl < 0:
        multiplier = max(min_multiplier, multiplier - step)
    elif pnl > 0:
        multiplier = min(max_multiplier, multiplier + step)
    state["adaptive_risk_multiplier"] = str(multiplier)


def _auto_tune_baseline(state: dict[str, Any], args: argparse.Namespace) -> dict[str, str]:
    # Always load/refresh baseline from args to allow user configuration edits to take effect
    baseline = {}
    for key in (
        "min_confidence",
        "stop_loss_pct",
        "take_profit_pct",
        "trailing_activation_pct",
        "trailing_distance_pct",
        "symbol_cooldown_minutes",
        "loss_symbol_cooldown_minutes",
        "max_spread_pct",
        "min_reward_to_fee_ratio",
        "max_entry_funding_cost_pct",
        "ai_entry_aggressiveness",
    ):
        if hasattr(args, key):
            baseline[key] = str(getattr(args, key))
    state["auto_tune_baseline"] = baseline
    return baseline


def _apply_auto_tune_overrides(args: argparse.Namespace, state: dict[str, Any]) -> bool:
    if not getattr(args, "auto_tune_parameters", False):
        return False
    overrides = state.get("auto_tune_overrides")
    if not isinstance(overrides, dict):
        return False
    applied: list[str] = []
    for key, value in overrides.items():
        normalized = _normalize_runtime_config_key(str(key))
        if normalized not in RUNTIME_CONFIG_HOT_KEYS or not hasattr(args, normalized):
            continue
        setattr(args, normalized, _coerce_runtime_config_value(getattr(args, normalized), value))
        applied.append(normalized)
    if applied:
        print(f"auto_tune_overrides_applied keys={','.join(sorted(applied))}")
    return bool(applied)


def _apply_lessons_learned_thresholds(args: argparse.Namespace, state: dict[str, Any]) -> None:
    overrides = state.get("lessons_learned_overrides")
    if not isinstance(overrides, dict):
        return
    applied = []
    for key, val in overrides.items():
        if hasattr(args, key):
            cur_val = getattr(args, key)
            if isinstance(cur_val, Decimal):
                setattr(args, key, Decimal(str(val)))
            elif isinstance(cur_val, float):
                setattr(args, key, float(val))
            elif isinstance(cur_val, int):
                setattr(args, key, int(val))
            else:
                setattr(args, key, val)
            applied.append(key)
    if applied:
        print(f"lessons_learned_overrides_applied keys={','.join(sorted(applied))}")


def _closed_trade_pnls(state: dict[str, Any], lookback: int) -> list[tuple[dict[str, Any], Decimal]]:
    trades = state.get("closed_trades") if isinstance(state.get("closed_trades"), list) else []
    result: list[tuple[dict[str, Any], Decimal]] = []
    for trade in trades[-lookback:]:
        if not isinstance(trade, dict):
            continue
        pnl = _decimal(
            trade.get("binance_net_pnl_usdt")
            or trade.get("binance_realized_pnl_usdt")
            or trade.get("estimated_pnl_usdt_before_fees")
        )
        result.append((trade, pnl))
    return result


def _clamp_decimal(value: Decimal, minimum: Decimal, maximum: Decimal) -> Decimal:
    return min(max(value, minimum), maximum)


def _exit_plan_to_state(plan: ExitPlan) -> dict[str, Any]:
    return {
        "mode": plan.mode,
        "stop_loss_pct": str(plan.stop_loss_pct),
        "take_profit_pct": str(plan.take_profit_pct),
        "reward_risk": str(plan.reward_risk),
        "reasons": plan.reasons,
    }


def _fixed_exit_plan(args: argparse.Namespace, reason: str = "fixed_config") -> ExitPlan:
    stop_loss_pct = getattr(args, "stop_loss_pct", Decimal("0.8"))
    take_profit_pct = getattr(args, "take_profit_pct", Decimal("1.2"))
    reward_risk = take_profit_pct / stop_loss_pct if stop_loss_pct > 0 else Decimal("0")
    return ExitPlan(
        mode="fixed",
        stop_loss_pct=stop_loss_pct,
        take_profit_pct=take_profit_pct,
        reward_risk=reward_risk,
        reasons=[reason],
    )


def _timeframe_trend_counts(side: str, timeframe_indicators: dict[str, Indicators]) -> tuple[int, int]:
    wanted = "up" if side == "buy" else "down"
    opposite = "down" if side == "buy" else "up"
    wanted_count = 0
    opposite_count = 0
    for timeframe_indicator in timeframe_indicators.values():
        if timeframe_indicator.trend == wanted:
            wanted_count += 1
        elif timeframe_indicator.trend == opposite:
            opposite_count += 1
    return wanted_count, opposite_count


def _higher_timeframe_opposes(side: str, timeframe_indicators: dict[str, Indicators]) -> bool:
    high_tf = timeframe_indicators.get("1h") or timeframe_indicators.get("4h") or timeframe_indicators.get("1d")
    if high_tf is None:
        return False
    return high_tf.trend == ("down" if side == "buy" else "up")


def _is_overextended_for_side(side: str, indicators: Indicators, timeframe_indicators: dict[str, Indicators]) -> bool:
    values = [indicators, *timeframe_indicators.values()]
    if side == "buy":
        return any(item.rsi >= Decimal("70") for item in values)
    return any(item.rsi <= Decimal("30") for item in values)


def _dynamic_exit_plan(
    args: argparse.Namespace,
    side: str,
    decision_confidence: Decimal,
    indicators: Indicators,
    timeframe_indicators: dict[str, Indicators],
    regime: MarketRegime,
    entry_plan: dict[str, Any] | None = None,
) -> ExitPlan:
    if not getattr(args, "dynamic_exit_plan", True):
        return _fixed_exit_plan(args)

    min_stop = getattr(args, "dynamic_exit_min_stop_pct", Decimal("0.30"))
    max_stop = getattr(args, "dynamic_exit_max_stop_pct", Decimal("0.90"))
    min_tp = getattr(args, "dynamic_exit_min_take_profit_pct", Decimal("0.70"))
    max_tp = getattr(args, "dynamic_exit_max_take_profit_pct", Decimal("2.50"))
    min_rr = getattr(args, "dynamic_exit_min_reward_risk", Decimal("1.80"))
    max_rr = getattr(args, "dynamic_exit_max_reward_risk", Decimal("3.00"))
    atr_multiplier = getattr(args, "dynamic_exit_atr_stop_multiplier", Decimal("1.80"))
    if getattr(args, "ai_entry_plan", True) and isinstance(entry_plan, dict):
        requested_atr_multiplier = _ai_decimal(entry_plan.get("stop_atr_multiplier"))
        if requested_atr_multiplier > 0:
            atr_multiplier = _clamp_decimal(requested_atr_multiplier, Decimal("1.20"), Decimal("2.80"))

    atr_stop = indicators.atr_pct * atr_multiplier
    stop_loss_pct = _clamp_decimal(atr_stop, min_stop, max_stop)
    aligned_count, opposite_count = _timeframe_trend_counts(side, timeframe_indicators)
    reward_risk = min_rr
    reasons = [f"atr_stop={indicators.atr_pct:.4f}*{atr_multiplier}"]

    if aligned_count >= 3:
        reward_risk += Decimal("0.30")
        reasons.append(f"aligned_timeframes={aligned_count}")
    if regime.name == "trending" and regime.confidence >= Decimal("0.65"):
        reward_risk += Decimal("0.20")
        reasons.append(f"regime={regime.name}")
    if decision_confidence >= Decimal("0.76"):
        reward_risk += Decimal("0.20")
        reasons.append(f"confidence={decision_confidence}")
    if _higher_timeframe_opposes(side, timeframe_indicators):
        reward_risk -= Decimal("0.30")
        reasons.append("higher_timeframe_opposes")
    if _is_overextended_for_side(side, indicators, timeframe_indicators):
        reward_risk -= Decimal("0.20")
        reasons.append("rsi_overextended")
    if opposite_count >= 2:
        reward_risk -= Decimal("0.20")
        reasons.append(f"opposite_timeframes={opposite_count}")

    if getattr(args, "ai_entry_plan", True) and isinstance(entry_plan, dict):
        requested_reward_risk = _ai_decimal(entry_plan.get("reward_risk"))
        if requested_reward_risk > 0:
            reward_risk = requested_reward_risk
            reasons.append(f"ai_reward_risk={requested_reward_risk}")

    reward_risk = _clamp_decimal(reward_risk, min_rr, max_rr)
    take_profit_pct = _clamp_decimal(stop_loss_pct * reward_risk, min_tp, max_tp)
    actual_reward_risk = take_profit_pct / stop_loss_pct if stop_loss_pct > 0 else Decimal("0")
    return ExitPlan(
        mode="dynamic_atr",
        stop_loss_pct=stop_loss_pct,
        take_profit_pct=take_profit_pct,
        reward_risk=actual_reward_risk,
        reasons=reasons,
    )


def _exit_plan_from_open_trade(open_trade: dict[str, Any] | None, args: argparse.Namespace) -> ExitPlan:
    if isinstance(open_trade, dict):
        payload = open_trade.get("exit_plan")
        if isinstance(payload, dict):
            stop_loss_pct = _decimal(payload.get("stop_loss_pct"), default=str(getattr(args, "stop_loss_pct", Decimal("0.8"))))
            take_profit_pct = _decimal(payload.get("take_profit_pct"), default=str(getattr(args, "take_profit_pct", Decimal("1.2"))))
            if stop_loss_pct > 0 and take_profit_pct > 0:
                reasons = payload.get("reasons") if isinstance(payload.get("reasons"), list) else []
                return ExitPlan(
                    mode=str(payload.get("mode") or "state"),
                    stop_loss_pct=stop_loss_pct,
                    take_profit_pct=take_profit_pct,
                    reward_risk=take_profit_pct / stop_loss_pct,
                    reasons=[str(item) for item in reasons],
                )
    return _fixed_exit_plan(args, "open_trade_without_exit_plan")


def _exit_plan_preview_payload(
    args: argparse.Namespace,
    indicators: Indicators,
    timeframe_indicators: dict[str, Indicators],
    regime: MarketRegime,
) -> dict[str, Any]:
    assumed_confidence = getattr(args, "min_confidence", Decimal("0.70"))
    buy_plan = _dynamic_exit_plan(args, "buy", assumed_confidence, indicators, timeframe_indicators, regime)
    sell_plan = _dynamic_exit_plan(args, "sell", assumed_confidence, indicators, timeframe_indicators, regime)
    return {
        "assumed_approval_confidence": str(assumed_confidence),
        "if_buy": _exit_plan_to_state(buy_plan),
        "if_sell": _exit_plan_to_state(sell_plan),
        "note": "Final entry plan is recalculated after the AI final_decision using its actual side and confidence.",
    }


def _execution_context_payload(exchange, symbol: str, ticker: dict[str, Any], args: argparse.Namespace) -> dict[str, Any]:
    info = ticker.get("info") if isinstance(ticker.get("info"), dict) else {}
    bid = _decimal(ticker.get("bid") or info.get("bidPrice"))
    ask = _decimal(ticker.get("ask") or info.get("askPrice"))
    bid_size = Decimal("0")
    ask_size = Decimal("0")
    quote_source = "ticker" if bid > 0 and ask > 0 else "missing"
    if (bid <= 0 or ask <= 0) and exchange is not None:
        order_book_ticker = ticker.get("_order_book_ticker") if isinstance(ticker.get("_order_book_ticker"), dict) else None
        if order_book_ticker is None:
            order_book_ticker = _order_book_ticker(exchange, symbol)
            if order_book_ticker is not None:
                ticker["_order_book_ticker"] = order_book_ticker
        if order_book_ticker is not None:
            bid = _decimal(order_book_ticker.get("bid"))
            ask = _decimal(order_book_ticker.get("ask"))
            bid_size = _decimal(order_book_ticker.get("bid_size"))
            ask_size = _decimal(order_book_ticker.get("ask_size"))
            quote_source = "order_book"
    spread_pct = _ticker_spread_pct({"bid": bid, "ask": ask}) if bid > 0 and ask > 0 else None
    funding_rate: Decimal | None = None
    try:
        funding_rate = _fetch_funding_rate(exchange, symbol)
    except Exception as exc:
        print(f"AI context could not fetch funding rate for {symbol}; continuing without it: {exc}")
    round_trip_fee_pct = getattr(args, "taker_fee_rate_pct", Decimal("0")) * Decimal("2")
    return {
        "last_price": str(_decimal(ticker.get("last") or ticker.get("close") or info.get("lastPrice"))),
        "bid": str(bid) if bid > 0 else None,
        "ask": str(ask) if ask > 0 else None,
        "quote_source": quote_source,
        "bid_size": str(bid_size) if bid_size > 0 else None,
        "ask_size": str(ask_size) if ask_size > 0 else None,
        "spread_pct": str(spread_pct) if spread_pct is not None else None,
        "max_spread_pct": str(getattr(args, "max_spread_pct", Decimal("0"))),
        "ai_entry_limit_max_distance_pct": str(getattr(args, "ai_entry_limit_max_distance_pct", Decimal("0.40"))),
        "configured_execution_mode": str(getattr(args, "execution_mode", "market")),
        "limit_entry_timeout_seconds": str(getattr(args, "limit_entry_timeout_seconds", 8.0)),
        "limit_entry_price_offset_pct": str(getattr(args, "limit_entry_price_offset_pct", Decimal("0.02"))),
        "limit_entry_min_fill_ratio": str(getattr(args, "limit_entry_min_fill_ratio", Decimal("1"))),
        "round_trip_taker_fee_pct": str(round_trip_fee_pct),
        "funding_rate": str(funding_rate) if funding_rate is not None else None,
        "funding_rate_pct": str(funding_rate * Decimal("100")) if funding_rate is not None else None,
        "mark_price": str(_decimal(ticker.get("mark") or info.get("markPrice"))) if (ticker.get("mark") or info.get("markPrice")) else None,
        "index_price": str(_decimal(info.get("indexPrice"))) if info.get("indexPrice") else None,
    }


def _ticker_last_price(ticker: dict[str, Any], fallback: Decimal = Decimal("0")) -> Decimal:
    info = ticker.get("info") if isinstance(ticker.get("info"), dict) else {}
    return _decimal(ticker.get("last") or ticker.get("close") or info.get("lastPrice"), default=str(fallback))


def _ensure_ticker_best_quotes(exchange, symbol: str, ticker: dict[str, Any]) -> tuple[Decimal, Decimal, str]:
    info = ticker.get("info") if isinstance(ticker.get("info"), dict) else {}
    bid = _decimal(ticker.get("bid") or info.get("bidPrice"))
    ask = _decimal(ticker.get("ask") or info.get("askPrice"))
    quote_source = "ticker" if bid > 0 and ask > 0 else "missing"
    if bid <= 0 or ask <= 0:
        order_book_ticker = ticker.get("_order_book_ticker") if isinstance(ticker.get("_order_book_ticker"), dict) else None
        if order_book_ticker is None:
            order_book_ticker = _order_book_ticker(exchange, symbol)
            if order_book_ticker is not None:
                ticker["_order_book_ticker"] = order_book_ticker
        if order_book_ticker is not None:
            bid = _decimal(order_book_ticker.get("bid"))
            ask = _decimal(order_book_ticker.get("ask"))
            quote_source = "order_book"
    if bid > 0:
        ticker["bid"] = str(bid)
    if ask > 0:
        ticker["ask"] = str(ask)
    return bid, ask, quote_source


def _entry_quote_drift_block_reason(
    prompt_last_price: Decimal,
    fresh_last_price: Decimal,
    max_drift_pct: Decimal,
) -> str | None:
    if max_drift_pct <= 0 or prompt_last_price <= 0 or fresh_last_price <= 0:
        return None
    drift_pct = abs(fresh_last_price - prompt_last_price) / prompt_last_price * Decimal("100")
    if drift_pct > max_drift_pct:
        return (
            f"post-AI price drift {drift_pct:.4f}% > ai_entry_max_price_drift_pct {max_drift_pct}% "
            f"(prompt_price={prompt_last_price}, fresh_price={fresh_last_price})"
        )
    return None


def _normalise_ai_entry_style(entry_plan: dict[str, Any] | None) -> str:
    if not isinstance(entry_plan, dict):
        return "default"
    style = str(entry_plan.get("entry_style") or "default").strip().lower()
    if style in {"market", "marketable_limit", "wait_pullback", "wait_breakout", "default"}:
        return style
    return "default"


def _entry_execution_plan_from_ai(
    args: argparse.Namespace,
    exchange,
    side: str,
    symbol: str,
    ticker: dict[str, Any],
    entry_plan: dict[str, Any] | None,
) -> tuple[str, Decimal | None, dict[str, str], str | None]:
    configured_mode = getattr(args, "execution_mode", "market")
    if not getattr(args, "ai_entry_plan", True):
        return configured_mode, None, {"style": "disabled", "mode": configured_mode}, None

    style = _normalise_ai_entry_style(entry_plan)
    if style == "default":
        return configured_mode, None, {"style": style, "mode": configured_mode}, None
    if style == "wait_breakout":
        return configured_mode, None, {"style": style, "mode": "hold"}, "AI requested wait_breakout; pending stop-entry orders are not enabled"
    if style == "market":
        return "market", None, {"style": style, "mode": "market"}, None

    bid, ask, quote_source = _ensure_ticker_best_quotes(exchange, symbol, ticker)
    last_price = _ticker_last_price(ticker)
    if bid <= 0 or ask <= 0 or last_price <= 0:
        return configured_mode, None, {"style": style, "mode": configured_mode}, "AI entry plan requires fresh bid/ask/last quotes"

    max_distance_pct = getattr(args, "ai_entry_limit_max_distance_pct", Decimal("0.40"))
    requested_limit = _ai_decimal(entry_plan.get("limit_price")) if isinstance(entry_plan, dict) else Decimal("0")
    if style == "wait_pullback" and requested_limit <= 0:
        requested_limit = bid if side == "buy" else ask
    mode = "limit" if style == "wait_pullback" else "marketable_limit"
    if requested_limit <= 0:
        return mode, None, {"style": style, "mode": mode, "quote_source": quote_source}, None

    distance_pct = abs(requested_limit - last_price) / last_price * Decimal("100")
    if max_distance_pct > 0 and distance_pct > max_distance_pct:
        return mode, requested_limit, {
            "style": style,
            "mode": mode,
            "quote_source": quote_source,
            "limit_price": str(requested_limit),
            "distance_pct": f"{distance_pct:.4f}",
        }, f"AI limit_price distance {distance_pct:.4f}% > ai_entry_limit_max_distance_pct {max_distance_pct}%"

    if style == "wait_pullback":
        if side == "buy" and requested_limit >= ask:
            return mode, requested_limit, {
                "style": style,
                "mode": mode,
                "quote_source": quote_source,
                "limit_price": str(requested_limit),
            }, f"AI wait_pullback buy limit_price {requested_limit} is not below fresh ask {ask}"
        if side == "sell" and requested_limit <= bid:
            return mode, requested_limit, {
                "style": style,
                "mode": mode,
                "quote_source": quote_source,
                "limit_price": str(requested_limit),
            }, f"AI wait_pullback sell limit_price {requested_limit} is not above fresh bid {bid}"

    return mode, requested_limit, {
        "style": style,
        "mode": mode,
        "quote_source": quote_source,
        "limit_price": str(requested_limit),
        "distance_pct": f"{distance_pct:.4f}",
    }, None


def _risk_context_payload(
    state: dict[str, Any],
    args: argparse.Namespace,
    equity: Decimal,
    account: dict[str, Any] | None,
    symbol: str,
) -> dict[str, Any]:
    start_equity = _decimal(state.get("start_equity"), default=str(equity))
    drawdown_pct = max(Decimal("0"), (start_equity - equity) / start_equity * Decimal("100")) if start_equity > 0 else Decimal("0")
    available_balance = _available_balance_from_futures_account(account) if account is not None else None
    global_control = _global_control(state)
    symbol_control = _symbol_control(state, symbol)
    pause_until = _decimal(global_control.get("pause_until"))
    now_ts = _now_ts()
    global_loss_pause_active = pause_until > now_ts
    hard_entry_block_reason = (
        f"global loss pause until {datetime.fromtimestamp(float(pause_until), timezone.utc).isoformat()}"
        if global_loss_pause_active
        else None
    )
    return {
        "equity_usdt": str(equity),
        "start_equity_usdt": str(start_equity),
        "daily_drawdown_pct": str(drawdown_pct),
        "available_balance_usdt": str(available_balance) if available_balance is not None else None,
        "adaptive_risk_enabled": bool(getattr(args, "adaptive_risk", False)),
        "adaptive_risk_multiplier": str(_decimal(state.get("adaptive_risk_multiplier"), default="1")),
        "risk_per_trade_pct": str(getattr(args, "risk_per_trade_pct", Decimal("0"))),
        "max_position_notional_usdt": str(getattr(args, "max_position_notional_usdt", Decimal("0"))),
        "max_total_notional_usdt": str(getattr(args, "max_total_notional_usdt", Decimal("0"))),
        "max_daily_loss_pct": str(getattr(args, "max_daily_loss_pct", Decimal("0"))),
        "global_consecutive_losses": str(_decimal(global_control.get("consecutive_losses"))),
        "max_consecutive_losses": str(getattr(args, "max_consecutive_losses", 0)),
        "global_loss_pause_active": global_loss_pause_active,
        "global_loss_pause_status": "active" if global_loss_pause_active else ("expired" if pause_until > 0 else "none"),
        "global_loss_pause_until": _ts_to_iso(pause_until),
        "global_loss_pause_seconds_remaining": str(max(Decimal("0"), pause_until - now_ts)),
        "hard_entry_block_reason": hard_entry_block_reason,
        "symbol_control": {
            "consecutive_losses": str(_decimal(symbol_control.get("consecutive_losses"))),
            "cooldown_until": _ts_to_iso(symbol_control.get("cooldown_until")),
            "blacklist_until": _ts_to_iso(symbol_control.get("blacklist_until")),
        },
    }


def _performance_context_payload(state: dict[str, Any], symbol: str, lookback: int = 20) -> dict[str, Any]:
    trades = state.get("closed_trades") if isinstance(state.get("closed_trades"), list) else []
    recent = [trade for trade in trades[-lookback:] if isinstance(trade, dict)]
    symbol_recent = [trade for trade in recent if trade.get("symbol") == symbol]

    def summarize(items: list[dict[str, Any]]) -> dict[str, str]:
        pnls = [_decimal(
            item.get("binance_net_pnl_usdt")
            or item.get("binance_realized_pnl_usdt")
            or item.get("estimated_pnl_usdt_before_fees")
        ) for item in items]
        wins = sum(1 for pnl in pnls if pnl > 0)
        losses = sum(1 for pnl in pnls if pnl < 0)
        gross_win = sum((pnl for pnl in pnls if pnl > 0), Decimal("0"))
        gross_loss = abs(sum((pnl for pnl in pnls if pnl < 0), Decimal("0")))
        return {
            "trades": str(len(pnls)),
            "wins": str(wins),
            "losses": str(losses),
            "win_rate": str(Decimal(wins) / Decimal(len(pnls)) if pnls else Decimal("0")),
            "estimated_pnl_usdt_before_fees": str(sum(pnls, Decimal("0"))),
            "gross_win": str(gross_win),
            "gross_loss": str(gross_loss),
            "profit_factor": str(gross_win / gross_loss) if gross_loss > 0 else "inf" if gross_win > 0 else "0",
        }

    return {
        "global_recent": summarize(recent),
        "symbol_recent": summarize(symbol_recent),
        "symbol_recent_exit_reasons": [
            str(item.get("exit_reason") or "") for item in symbol_recent[-5:] if item.get("exit_reason")
        ],
        "binance_performance_summary": _binance_performance_summary(state),
        "auto_tune_overrides": state.get("auto_tune_overrides") if isinstance(state.get("auto_tune_overrides"), dict) else {},
    }


def _entry_block_context_payload(state: dict[str, Any], symbol: str, lookback_minutes: Decimal = Decimal("360")) -> dict[str, Any]:
    since_ts = _now_ts() - _seconds(lookback_minutes)
    events = state.get("entry_block_events") if isinstance(state.get("entry_block_events"), list) else []
    recent_events = [event for event in events if isinstance(event, dict) and _decimal(event.get("ts")) >= since_ts]
    symbol_events = [event for event in recent_events if event.get("symbol") == symbol]
    return {
        "lookback_minutes": str(lookback_minutes),
        "global_counts": _recent_entry_block_counts(state, since_ts),
        "symbol_recent_blocks": [
            {
                "category": str(event.get("category") or ""),
                "reason": str(event.get("reason") or "")[:180],
            }
            for event in symbol_events[-5:]
        ],
    }


def _to_exchange_amount(exchange, symbol: str, amount_base: str | Decimal | float) -> str:
    if exchange.id == "okx":
        market = exchange.markets.get(symbol)
        if market and market.get("contractSize") is not None:
            contract_size = Decimal(str(market["contractSize"]))
            amount_dec = _decimal(amount_base)
            contracts = amount_dec / contract_size
            return str(exchange.amount_to_precision(symbol, float(contracts)))
    return str(amount_base)


def _to_base_amount(exchange, symbol: str, amount_contracts: Decimal) -> Decimal:
    if exchange.id == "okx":
        market = exchange.markets.get(symbol)
        if market and market.get("contractSize") is not None:
            contract_size = Decimal(str(market["contractSize"]))
            return amount_contracts * contract_size
    return amount_contracts


def _normalize_order(exchange, order: dict[str, Any]) -> dict[str, Any]:
    if not order or exchange.id != "okx":
        return order
    symbol = order.get("symbol")
    if not symbol:
        return order
    market = exchange.markets.get(symbol)
    if not market or market.get("contractSize") is None:
        return order
    contract_size = Decimal(str(market["contractSize"]))
    normalized = dict(order)
    for key in ["amount", "filled", "remaining"]:
        if normalized.get(key) is not None:
            try:
                normalized[key] = float(Decimal(str(normalized[key])) * contract_size)
            except Exception:
                pass
    return normalized


def _normalize_trade(exchange, symbol: str, trade: dict[str, Any]) -> dict[str, Any]:
    if not trade or exchange.id != "okx":
        return trade
    market = exchange.markets.get(symbol)
    if not market or market.get("contractSize") is None:
        return trade
    contract_size = Decimal(str(market["contractSize"]))
    normalized = dict(trade)
    if normalized.get("amount") is not None:
        normalized["amount"] = float(Decimal(str(normalized["amount"])) * contract_size)
    return normalized



def _fetch_okx_sentiment_context(exchange, symbol: str) -> dict[str, Any]:
    if exchange.id != "okx":
        return {}

    try:
        ccy = symbol.split("/")[0]
        ls_ratio = "unknown"
        ls_ratio_getter = getattr(exchange, "publicGetRubikStatContractsLongShortAccountRatio", None)
        if callable(ls_ratio_getter):
            res = ls_ratio_getter({"ccy": ccy, "period": "1H"})
            if res and "data" in res and len(res["data"]) > 0:
                ls_ratio = res["data"][0][1]

        top_trader_ratio = "unknown"
        top_trader_ratio_getter = getattr(exchange, "publicGetRubikStatContractsLongShortPositionRatioContractTopTrader", None)
        if callable(top_trader_ratio_getter):
            inst_id = _market_id(exchange, symbol)
            res = top_trader_ratio_getter({"instId": inst_id, "period": "1H"})
            if res and "data" in res and len(res["data"]) > 0:
                top_trader_ratio = res["data"][0][1]

        oi_current = "unknown"
        oi_prev = "unknown"
        oi_getter = getattr(exchange, "publicGetRubikStatContractsOpenInterestVolume", None)
        if callable(oi_getter):
            res = oi_getter({"ccy": ccy, "period": "1H"})
            if res and "data" in res and len(res["data"]) > 0:
                oi_current = res["data"][0][1]
                if len(res["data"]) > 1:
                    oi_prev = res["data"][1][1]

        oi_trend = "flat"
        if oi_current != "unknown" and oi_prev != "unknown":
            try:
                curr = Decimal(str(oi_current))
                prev = Decimal(str(oi_prev))
                if curr > prev * Decimal("1.005"):
                    oi_trend = "rising"
                elif curr < prev * Decimal("0.995"):
                    oi_trend = "falling"
            except Exception:
                pass

        return {
            "long_short_account_ratio": ls_ratio,
            "top_traders_long_short_position_ratio": top_trader_ratio,
            "open_interest_current": oi_current,
            "open_interest_previous": oi_prev,
            "open_interest_trend_1h": oi_trend,
        }
    except Exception as e:
        print(f"Error fetching OKX sentiment context: {e}")
        return {}


def _ai_context_payload(
    exchange,
    args: argparse.Namespace,
    state: dict[str, Any],
    symbol: str,
    ticker: dict[str, Any],
    equity: Decimal,
    account: dict[str, Any] | None,
    indicators: Indicators,
    candles: list[list[Any]],
    timeframe_indicators: dict[str, Indicators],
    regime: MarketRegime,
    candidate_set: list[SymbolCandidate] | None,
) -> dict[str, Any]:
    payload = {
        "execution_context": _execution_context_payload(exchange, symbol, ticker, args),
        "risk_context": _risk_context_payload(state, args, equity, account, symbol),
        "exit_plan_preview": _exit_plan_preview_payload(args, indicators, timeframe_indicators, regime),
        "performance_context": _performance_context_payload(state, symbol),
        "market_structure": _market_structure_payload(indicators, candles, timeframe_indicators),
        "market_breadth": _market_breadth_payload(symbol, candidate_set),
        "entry_block_context": _entry_block_context_payload(
            state,
            symbol,
            getattr(args, "entry_block_report_lookback_minutes", Decimal("360")),
        ),
        "lessons_learned_active_adjustments": {
            k: str(v) for k, v in state.get("lessons_learned_overrides", {}).items()
        },
    }
    if exchange.id == "okx":
        payload["exchange_sentiment_context"] = _fetch_okx_sentiment_context(exchange, symbol)
    return payload


def _idle_auto_tune_overrides(
    state: dict[str, Any],
    args: argparse.Namespace,
    baseline: dict[str, str],
    now: Decimal,
) -> tuple[dict[str, str], list[str]]:
    if isinstance(state.get("open_trade"), dict):
        return {}, []

    idle_after = _seconds(getattr(args, "auto_tune_idle_after_minutes", Decimal("360")))
    if idle_after <= 0:
        return {}, []

    tracking_started = _decimal(state.get("auto_tune_tracking_started_ts"))
    if tracking_started <= 0:
        state["auto_tune_tracking_started_ts"] = str(now)
        tracking_started = now

    reference_ts = max(_decimal(state.get("last_entry_ts")), tracking_started)
    idle_seconds = now - reference_ts
    if idle_seconds < idle_after:
        return {}, []

    steps = min(int(idle_seconds / idle_after), 4)
    block_counts = _recent_entry_block_counts(state, reference_ts)
    total_blocks = sum(block_counts.values())
    dominant_categories = {
        category
        for category, count in block_counts.items()
        if total_blocks > 0 and Decimal(count) / Decimal(total_blocks) >= Decimal("0.30")
    }
    tune_all = not dominant_categories
    min_confidence = _decimal(baseline.get("min_confidence"), default=str(args.min_confidence))
    max_spread_pct = _decimal(baseline.get("max_spread_pct"), default=str(args.max_spread_pct))
    min_reward_to_fee_ratio = _decimal(
        baseline.get("min_reward_to_fee_ratio"),
        default=str(args.min_reward_to_fee_ratio),
    )
    max_entry_funding_cost_pct = _decimal(
        baseline.get("max_entry_funding_cost_pct"),
        default=str(args.max_entry_funding_cost_pct),
    )
    ai_entry_aggressiveness = _decimal(
        baseline.get("ai_entry_aggressiveness"),
        default=str(getattr(args, "ai_entry_aggressiveness", Decimal("0.50"))),
    )

    overrides: dict[str, str] = {}
    if tune_all or "confidence" in dominant_categories or "ai_hold" in dominant_categories:
        overrides["min_confidence"] = str(
            _clamp_decimal(
                min_confidence - getattr(args, "auto_tune_idle_confidence_step", Decimal("0.03")) * Decimal(steps),
                getattr(args, "auto_tune_idle_min_confidence", Decimal("0.62")),
                Decimal("0.88"),
            )
        )
    if tune_all or "spread" in dominant_categories:
        overrides["max_spread_pct"] = str(
            _clamp_decimal(
                max_spread_pct + getattr(args, "auto_tune_idle_spread_step_pct", Decimal("0.02")) * Decimal(steps),
                Decimal("0"),
                getattr(args, "auto_tune_idle_max_spread_pct", Decimal("0.15")),
            )
        )
    if tune_all or "fee_reward" in dominant_categories:
        overrides["min_reward_to_fee_ratio"] = str(
            _clamp_decimal(
                min_reward_to_fee_ratio - getattr(args, "auto_tune_idle_reward_fee_step", Decimal("0.25")) * Decimal(steps),
                getattr(args, "auto_tune_idle_min_reward_to_fee_ratio", Decimal("2")),
                Decimal("10"),
            )
        )
    if tune_all or "funding" in dominant_categories:
        overrides["max_entry_funding_cost_pct"] = str(
            _clamp_decimal(
                max_entry_funding_cost_pct + getattr(args, "auto_tune_idle_funding_step_pct", Decimal("0.01")) * Decimal(steps),
                Decimal("0"),
                getattr(args, "auto_tune_idle_max_entry_funding_cost_pct", Decimal("0.08")),
            )
        )
    if "ai_hold" in dominant_categories:
        overrides["ai_entry_aggressiveness"] = str(
            _clamp_decimal(ai_entry_aggressiveness + Decimal("0.05") * Decimal(steps), Decimal("0.10"), Decimal("0.85"))
        )

    idle_minutes = idle_seconds / Decimal("60")
    if not overrides:
        return {}, []
    reasons = [f"relaxed_after_idle_{idle_minutes:.1f}_minutes_steps_{steps}"]
    if block_counts:
        reasons.append(f"entry_block_counts={block_counts}")
    return overrides, reasons


def _write_auto_tune_summary(
    state: dict[str, Any],
    auto_state: dict[str, Any],
    now: Decimal,
    overrides: dict[str, str],
    summary: dict[str, Any],
    args: argparse.Namespace,
) -> None:
    state["auto_tune_overrides"] = overrides
    auto_state["last_run_ts"] = str(now)
    auto_state["last_summary"] = summary
    print(f"auto_tune_summary {summary}")
    _notify(args, "performance", _auto_tune_notification_message(summary))


def _maybe_auto_tune_parameters(state: dict[str, Any], args: argparse.Namespace) -> None:
    if not getattr(args, "auto_tune_parameters", False):
        return
    interval = _seconds(getattr(args, "auto_tune_interval_minutes", Decimal("60")))
    now = _now_ts()
    auto_state = state.setdefault("auto_tune", {})
    if not isinstance(auto_state, dict):
        auto_state = {}
        state["auto_tune"] = auto_state
    last_run = _decimal(auto_state.get("last_run_ts"))
    if interval > 0 and last_run > 0 and now - last_run < interval:
        return

    baseline = _auto_tune_baseline(state, args)
    idle_overrides, idle_reasons = _idle_auto_tune_overrides(state, args, baseline, now)
    lookback = int(getattr(args, "auto_tune_lookback_trades", 30))
    trades = _closed_trade_pnls(state, lookback)
    min_trades = int(getattr(args, "auto_tune_min_trades", 8))
    if len(trades) < min_trades:
        if idle_overrides:
            _write_auto_tune_summary(
                state,
                auto_state,
                now,
                idle_overrides,
                {
                    "mode": "idle",
                    "trades": len(trades),
                    "min_trades": min_trades,
                    "reasons": idle_reasons,
                    "overrides": idle_overrides,
                },
                args,
            )
        return

    pnl_values = [pnl for _trade, pnl in trades]
    wins = [pnl for pnl in pnl_values if pnl > 0]
    losses = [pnl for pnl in pnl_values if pnl < 0]
    win_rate = Decimal(len(wins)) / Decimal(len(pnl_values))
    gross_win = sum(wins, Decimal("0"))
    gross_loss = abs(sum(losses, Decimal("0")))
    profit_factor = gross_win / gross_loss if gross_loss > 0 else Decimal("99")
    avg_loss = gross_loss / Decimal(len(losses)) if losses else Decimal("0")
    avg_win = gross_win / Decimal(len(wins)) if wins else Decimal("0")

    min_confidence = _decimal(baseline.get("min_confidence"), default=str(args.min_confidence))
    stop_loss_pct = _decimal(baseline.get("stop_loss_pct"), default=str(args.stop_loss_pct))
    take_profit_pct = _decimal(baseline.get("take_profit_pct"), default=str(args.take_profit_pct))
    trailing_activation_pct = _decimal(
        baseline.get("trailing_activation_pct"),
        default=str(args.trailing_activation_pct),
    )
    trailing_distance_pct = _decimal(
        baseline.get("trailing_distance_pct"),
        default=str(args.trailing_distance_pct),
    )
    symbol_cooldown_minutes = _decimal(
        baseline.get("symbol_cooldown_minutes"),
        default=str(args.symbol_cooldown_minutes),
    )
    loss_symbol_cooldown_minutes = _decimal(
        baseline.get("loss_symbol_cooldown_minutes"),
        default=str(args.loss_symbol_cooldown_minutes),
    )

    reasons: list[str] = []
    if win_rate < Decimal("0.40") or profit_factor < Decimal("0.90"):
        min_confidence += Decimal("0.05")
        # Do not tighten stop-loss to avoid the death spiral.
        # Slightly widen the stop-loss and tighten the take profit target.
        stop_loss_pct *= Decimal("1.05")
        take_profit_pct *= Decimal("0.90")
        trailing_activation_pct *= Decimal("0.90")
        trailing_distance_pct *= Decimal("0.90")
        symbol_cooldown_minutes *= Decimal("1.25")
        loss_symbol_cooldown_minutes *= Decimal("1.50")
        reasons.append("tightened_after_weak_recent_performance")
    elif win_rate >= Decimal("0.55") and profit_factor >= Decimal("1.20"):
        min_confidence -= Decimal("0.03")
        stop_loss_pct *= Decimal("1.05")
        take_profit_pct *= Decimal("1.05")
        trailing_activation_pct *= Decimal("1.05")
        trailing_distance_pct *= Decimal("1.05")
        reasons.append("relaxed_after_positive_recent_performance")

    if losses and avg_loss > avg_win and avg_win > 0:
        # Prevent tightening stop-loss on losses to avoid death spiral.
        # Slightly tighten take-profit target to secure wins sooner.
        take_profit_pct *= Decimal("0.95")
        reasons.append("improved_reward_risk_after_large_average_loss")

    overrides = {
        "min_confidence": str(_clamp_decimal(min_confidence, Decimal("0.60"), Decimal("0.88"))),
        "stop_loss_pct": str(_clamp_decimal(stop_loss_pct, Decimal("0.70"), Decimal("1.50"))),
        "take_profit_pct": str(_clamp_decimal(take_profit_pct, Decimal("0.70"), Decimal("2.50"))),
        "trailing_activation_pct": str(_clamp_decimal(trailing_activation_pct, Decimal("0.35"), Decimal("1.50"))),
        "trailing_distance_pct": str(_clamp_decimal(trailing_distance_pct, Decimal("0.20"), Decimal("0.80"))),
        "symbol_cooldown_minutes": str(_clamp_decimal(symbol_cooldown_minutes, Decimal("5"), Decimal("120"))),
        "loss_symbol_cooldown_minutes": str(_clamp_decimal(loss_symbol_cooldown_minutes, Decimal("15"), Decimal("360"))),
    }
    if idle_overrides:
        if "tightened_after_weak_recent_performance" not in reasons:
            overrides.update(idle_overrides)
            reasons.extend(idle_reasons)
        else:
            idle_pressure_applied = False
            if "ai_entry_aggressiveness" in idle_overrides:
                overrides["ai_entry_aggressiveness"] = idle_overrides["ai_entry_aggressiveness"]
                idle_pressure_applied = True
            if "min_confidence" in idle_overrides:
                performance_confidence = _decimal(overrides.get("min_confidence"))
                idle_confidence = _decimal(idle_overrides.get("min_confidence"))
                baseline_confidence = _decimal(baseline.get("min_confidence"), default=str(args.min_confidence))
                confidence_relaxation = max(Decimal("0"), baseline_confidence - idle_confidence)
                if confidence_relaxation > 0 and performance_confidence > baseline_confidence:
                    softened_confidence = _clamp_decimal(
                        performance_confidence - confidence_relaxation,
                        baseline_confidence,
                        performance_confidence,
                    )
                    if softened_confidence < performance_confidence:
                        overrides["min_confidence"] = str(softened_confidence)
                        idle_pressure_applied = True
            if idle_pressure_applied:
                reasons.append("softened_after_idle_blocks_under_weak_performance")
                reasons.extend(idle_reasons)

    symbol_stats: dict[str, dict[str, Decimal]] = {}
    for trade, pnl in trades:
        symbol = str(trade.get("symbol") or "")
        if not symbol:
            continue
        stats = symbol_stats.setdefault(symbol, {"trades": Decimal("0"), "losses": Decimal("0"), "pnl": Decimal("0")})
        stats["trades"] += Decimal("1")
        stats["losses"] += Decimal("1") if pnl < 0 else Decimal("0")
        stats["pnl"] += pnl

    symbol_min_trades = Decimal(int(getattr(args, "auto_tune_symbol_min_trades", 3)))
    max_loss_rate = getattr(args, "auto_tune_symbol_max_loss_rate", Decimal("0.70"))
    for symbol, stats in symbol_stats.items():
        if stats["trades"] < symbol_min_trades:
            continue
        loss_rate = stats["losses"] / stats["trades"]
        if loss_rate >= max_loss_rate and stats["pnl"] < 0:
            control = _symbol_control(state, symbol)
            control["blacklist_until"] = str(now + _seconds(getattr(args, "auto_tune_blacklist_minutes", Decimal("720"))))
            reasons.append(f"auto_blacklisted_{symbol}_loss_rate_{loss_rate:.2f}")
            _notify(args, "risk", f"Auto-tune blacklisted {symbol}: loss_rate={loss_rate:.2f} pnl={stats['pnl']:.4f}")

    _write_auto_tune_summary(
        state,
        auto_state,
        now,
        overrides,
        {
        "mode": "performance",
        "trades": len(pnl_values),
        "win_rate": str(win_rate),
        "profit_factor": str(profit_factor),
        "gross_win": str(gross_win),
        "gross_loss": str(gross_loss),
        "reasons": reasons,
        "overrides": overrides,
        },
        args,
    )


def _position_from_open_trade(open_trade: dict[str, Any]) -> PositionSummary | None:
    side = str(open_trade.get("side", ""))
    amount = _decimal(open_trade.get("amount"))
    entry_price = _decimal(open_trade.get("entry_price"))
    if side not in {"long", "short"} or amount <= 0 or entry_price <= 0:
        return None
    return PositionSummary(side=side, amount=amount, entry_price=entry_price, unrealized_pnl=Decimal("0"))


def _closed_trade_generic_income_payload(
    exchange,
    symbol: str,
    open_trade: dict[str, Any],
) -> dict[str, str]:
    try:
        opened_at_str = open_trade.get("opened_at")
        if not opened_at_str:
            return {}
        opened_at_str = opened_at_str.replace("Z", "+00:00")
        opened_dt = datetime.fromisoformat(opened_at_str)
        since_ms = int(opened_dt.timestamp() * 1000) - 60_000

        trades = exchange.fetch_my_trades(symbol, since=since_ms, limit=100)
        if not trades:
            return {}

        position_side = open_trade.get("side")
        close_side = "sell" if position_side == "long" else "buy"

        close_trades = [
            _normalize_trade(exchange, symbol, t)
            for t in trades
            if str(t.get("side")).lower() == close_side
        ]
        if not close_trades:
            return {}

        close_trades.sort(key=lambda x: x.get("timestamp") or 0, reverse=True)

        target_amount = _decimal(open_trade.get("amount"))
        accumulated_amount = Decimal("0")
        total_cost = Decimal("0")
        total_amount = Decimal("0")
        total_price_volume = Decimal("0")
        total_fee = Decimal("0")

        for t in close_trades:
            trade_amount = _decimal(t.get("amount") or 0)
            trade_price = _decimal(t.get("price") or 0)
            trade_cost = _decimal(t.get("cost") or 0)
            if trade_cost <= 0:
                trade_cost = trade_amount * trade_price

            fee = t.get("fee") or {}
            trade_fee = _decimal(fee.get("cost") or 0)

            total_cost += trade_cost
            total_amount += trade_amount
            total_price_volume += trade_price * trade_amount
            total_fee += trade_fee

            accumulated_amount += trade_amount
            if accumulated_amount >= target_amount * Decimal("0.99"):
                break

        if total_amount <= 0:
            return {}

        avg_price = total_price_volume / total_amount
        entry_price = _decimal(open_trade.get("entry_price"))
        entry_cost = total_amount * entry_price

        if position_side == "long":
            realized_pnl = total_cost - entry_cost
        else:
            realized_pnl = entry_cost - total_cost

        # Sum entry fees as well to get accurate net PnL matching the exchange Web UI
        entry_side = "buy" if position_side == "long" else "sell"
        entry_trades = [
            _normalize_trade(exchange, symbol, t)
            for t in trades
            if str(t.get("side")).lower() == entry_side
        ]
        total_entry_fee = sum(_decimal((t.get("fee") or {}).get("cost") or 0) for t in entry_trades)
        total_fee += total_entry_fee

        net_pnl = realized_pnl - total_fee

        return {
            "exit_price": str(avg_price),
            "binance_realized_pnl_usdt": str(realized_pnl),
            "binance_net_pnl_usdt": str(net_pnl),
            "binance_commission_usdt": str(total_fee),
            "binance_pnl_source": "user_trades_generic",
            "exit_price_source": "user_trades_generic",
        }
    except Exception as exc:
        print(f"Could not fetch generic income for closed trade {symbol}: {exc}")
        return {}


def _maybe_record_external_close(
    exchange,
    state: dict[str, Any],
    args: argparse.Namespace,
    symbol: str,
    previous_open_trade: dict[str, Any] | None,
    last_price: Decimal,
) -> None:
    if not previous_open_trade:
        return
    previous_symbol = previous_open_trade.get("symbol")
    if previous_symbol and previous_symbol != symbol:
        return

    position = _position_from_open_trade(previous_open_trade)
    if position is None:
        return

    binance_income = None
    if exchange.id == "binance" or exchange.id == "binanceusdm":
        binance_income = _closed_trade_binance_income_payload(exchange, symbol, previous_open_trade, _now_ms())
    else:
        generic_income = _closed_trade_generic_income_payload(exchange, symbol, previous_open_trade)
        if generic_income:
            binance_income = generic_income
            if "exit_price" in generic_income:
                last_price = _decimal(generic_income["exit_price"])

    _record_trade_outcome(
        state,
        args,
        symbol,
        position,
        last_price,
        "EXTERNAL_OR_NATIVE_CLOSE",
        previous_open_trade,
        binance_report=binance_income,
    )

    if getattr(args, "review_trades", True):
        try:
            print(f"[External Close Review] Fetching market state to review external close of {symbol}...")
            candles = exchange.fetch_ohlcv(symbol, args.timeframe, limit=args.candle_limit)
            indicators = _calculate_indicators(candles, args.ema_fast, args.ema_slow)
            timeframe_indicators = _fetch_timeframe_indicators(exchange, symbol, args, indicators)
            _review_closed_trade(
                state,
                args,
                symbol,
                position,
                last_price,
                "EXTERNAL_OR_NATIVE_CLOSE",
                previous_open_trade,
                indicators,
                timeframe_indicators,
            )
        except Exception as exc:
            print(f"Failed to generate review for external close: {exc}")


def _maybe_print_performance_report(state: dict[str, Any], args: argparse.Namespace, equity: Decimal) -> None:
    interval = _seconds(args.performance_report_interval_minutes)
    if interval <= 0:
        return
    now = _now_ts()
    last_report = _decimal(state.get("last_performance_report_ts"))
    if last_report > 0 and now - last_report < interval:
        return
    trades = state.get("closed_trades") if isinstance(state.get("closed_trades"), list) else []
    recent = trades[-50:]
    pnl_values = [_decimal(trade.get("estimated_pnl_usdt_before_fees")) for trade in recent if isinstance(trade, dict)]
    total_pnl = sum(pnl_values, Decimal("0"))
    wins = sum(1 for pnl in pnl_values if pnl > 0)
    losses = sum(1 for pnl in pnl_values if pnl < 0)
    by_symbol: dict[str, Decimal] = {}
    for trade in recent:
        if not isinstance(trade, dict):
            continue
        trade_symbol = str(trade.get("symbol", "unknown"))
        by_symbol[trade_symbol] = by_symbol.get(trade_symbol, Decimal("0")) + _decimal(
            trade.get("estimated_pnl_usdt_before_fees")
        )
    worst_symbols = sorted(by_symbol.items(), key=lambda item: item[1])[:3]
    binance_summary = _binance_performance_summary(state)
    report = (
        f"performance_report trades={len(pnl_values)} wins={wins} losses={losses} "
        f"estimated_pnl_before_fees={total_pnl:.4f} equity={equity} "
        f"worst_symbols={[(symbol, str(pnl)) for symbol, pnl in worst_symbols]} "
        f"binance_net_income={binance_summary['net_income_usdt']} "
        f"binance_realized_pnl={binance_summary['realized_pnl_usdt']} "
        f"binance_commission={binance_summary['commission_usdt']} "
        f"binance_funding={binance_summary['funding_usdt']}"
    )
    print(report)
    _notify(args, "performance", report)
    state["last_performance_report_ts"] = str(now)


def _open_trade_summary(state: dict[str, Any]) -> str:
    open_trade = state.get("open_trade")
    if not isinstance(open_trade, dict):
        return "none"
    return (
        f"{open_trade.get('symbol', 'unknown')}:{open_trade.get('side', 'unknown')} "
        f"amount={open_trade.get('amount', '0')} entry={open_trade.get('entry_price', '0')}"
    )


def _ts_to_iso(value: Any) -> str:
    ts = _decimal(value)
    if ts <= 0:
        return "never"
    return datetime.fromtimestamp(float(ts), timezone.utc).isoformat()


def _exchange_error_state(state: dict[str, Any]) -> dict[str, Any]:
    error_state = state.setdefault("exchange_errors", {})
    if not isinstance(error_state, dict):
        error_state = {}
        state["exchange_errors"] = error_state
    return error_state


def _record_exchange_error(state: dict[str, Any], args: argparse.Namespace, exc: Exception) -> None:
    error_state = _exchange_error_state(state)
    now = _now_ts()
    consecutive = int(_decimal(error_state.get("consecutive"))) + 1
    error_state["consecutive"] = consecutive
    error_state["last_error_ts"] = str(now)
    error_state["last_error_type"] = type(exc).__name__
    error_state["last_error"] = str(exc)[:500]
    history = error_state.setdefault("history", [])
    if not isinstance(history, list):
        history = []
        error_state["history"] = history
    history.append(
        {
            "ts": str(now),
            "type": type(exc).__name__,
            "message": str(exc)[:500],
            "consecutive": consecutive,
        }
    )
    del history[:-MAX_EXCHANGE_ERROR_HISTORY]

    threshold = int(getattr(args, "exchange_error_alert_threshold", 0))
    if threshold <= 0 or consecutive < threshold:
        return
    cooldown = _seconds(getattr(args, "exchange_error_alert_cooldown_minutes", Decimal("30")))
    last_alert = _decimal(error_state.get("last_alert_ts"))
    if cooldown > 0 and last_alert > 0 and now - last_alert < cooldown:
        return
    message = (
        f"Exchange errors consecutive={consecutive} last_type={type(exc).__name__} "
        f"last_error={str(exc)[:220]}"
    )
    print(f"exchange_error_alert {message}")
    _notify(args, "risk", message)
    error_state["last_alert_ts"] = str(now)


def _clear_exchange_error_streak(state: dict[str, Any]) -> None:
    error_state = state.get("exchange_errors")
    if not isinstance(error_state, dict):
        return
    previous = int(_decimal(error_state.get("consecutive")))
    if previous <= 0:
        return
    error_state["last_recovery_ts"] = str(_now_ts())
    error_state["previous_consecutive"] = previous
    error_state["consecutive"] = 0


def _maybe_print_health_report(state: dict[str, Any], args: argparse.Namespace, equity: Decimal) -> None:
    interval = _seconds(getattr(args, "health_report_interval_minutes", Decimal("0")))
    if interval <= 0:
        return
    now = _now_ts()
    last_report = _decimal(state.get("last_health_report_ts"))
    if last_report > 0 and now - last_report < interval:
        return

    lookback = _seconds(getattr(args, "entry_block_report_lookback_minutes", Decimal("360")))
    block_counts = _recent_entry_block_counts(state, now - lookback) if lookback > 0 else {}
    auto_tune = state.get("auto_tune") if isinstance(state.get("auto_tune"), dict) else {}
    last_auto_tune = auto_tune.get("last_summary") if isinstance(auto_tune.get("last_summary"), dict) else {}
    auto_mode = last_auto_tune.get("mode", "none")
    auto_reasons = last_auto_tune.get("reasons", [])
    overrides = state.get("auto_tune_overrides") if isinstance(state.get("auto_tune_overrides"), dict) else {}
    exchange_errors = state.get("exchange_errors") if isinstance(state.get("exchange_errors"), dict) else {}
    runtime_config_errors = (
        state.get("runtime_config_errors") if isinstance(state.get("runtime_config_errors"), dict) else {}
    )
    report = (
        f"health_report equity={equity} open_trade={_open_trade_summary(state)} "
        f"new_entries_enabled={getattr(args, 'new_entries_enabled', True)} "
        f"emergency_close_positions={getattr(args, 'emergency_close_positions', False)} "
        f"last_entry={_ts_to_iso(state.get('last_entry_ts'))} "
        f"exchange_error_consecutive={exchange_errors.get('consecutive', 0)} "
        f"last_exchange_error={exchange_errors.get('last_error_type', 'none')}@"
        f"{_ts_to_iso(exchange_errors.get('last_error_ts'))} "
        f"runtime_config_error_consecutive={runtime_config_errors.get('consecutive', 0)} "
        f"last_runtime_config_error={runtime_config_errors.get('last_error_type', 'none')}@"
        f"{_ts_to_iso(runtime_config_errors.get('last_error_ts'))} "
        f"entry_block_counts_{int(lookback / Decimal('60')) if lookback > 0 else 0}m={block_counts} "
        f"auto_tune_mode={auto_mode} auto_tune_reasons={auto_reasons} "
        f"auto_tune_overrides={overrides}"
    )
    print(report)
    _notify(args, "health", report)
    state["last_health_report_ts"] = str(now)


def _market_id(exchange, symbol: str) -> str:
    try:
        market = exchange.market(symbol)
    except Exception:
        market = (getattr(exchange, "markets", {}) or {}).get(symbol, {})
    return str(market.get("id") or symbol.replace("/", "").replace(":USDT", ""))


def _standard_usdm_symbol(symbol: str) -> str:
    raw = str(symbol or "").strip().upper()
    if not raw:
        return ""
    if "/" in raw:
        return raw
    if raw.endswith("USDT"):
        return f"{raw[:-4]}/USDT:USDT"
    return raw


def _equity_from_futures_account(account: dict[str, Any]) -> Decimal:
    for key in ("totalWalletBalance", "totalMarginBalance"):
        value = _decimal(account.get(key))
        if value > 0:
            return value

    for asset in account.get("assets") or []:
        if str(asset.get("asset", "")).upper() != "USDT":
            continue
        for key in ("walletBalance", "marginBalance"):
            value = _decimal(asset.get(key))
            if value > 0:
                return value
    return Decimal("0")


def _available_balance_from_futures_account(account: dict[str, Any]) -> Decimal | None:
    if "availableBalance" in account:
        return _decimal(account.get("availableBalance"))

    for asset in account.get("assets") or []:
        if str(asset.get("asset", "")).upper() != "USDT":
            continue
        if "availableBalance" in asset:
            return _decimal(asset.get("availableBalance"))
    return None


def _is_invalid_account_error(exc: ccxt.BaseError) -> bool:
    return '"code":-1109' in str(exc) or "Invalid account" in str(exc)


def _call_private_with_demo_retry(callable_method, params: dict[str, Any] | None = None):
    attempts = 5
    for attempt in range(1, attempts + 1):
        try:
            if params is None:
                return callable_method()
            return callable_method(params)
        except ccxt.BaseError as exc:
            if not _is_invalid_account_error(exc) or attempt == attempts:
                raise
            time.sleep(0.35 * attempt)
    raise RuntimeError("unreachable")


def _fetch_futures_account(exchange) -> dict[str, Any] | None:
    v2_account = getattr(exchange, "fapiPrivateV2GetAccount", None)
    if not callable(v2_account):
        return None
    return _call_private_with_demo_retry(v2_account)


def _fetch_usdt_equity(exchange, account: dict[str, Any] | None | Any = _ACCOUNT_NOT_PROVIDED) -> Decimal:
    if account is not _ACCOUNT_NOT_PROVIDED:
        equity = _equity_from_futures_account(account or {})
        if equity > 0:
            return equity
    elif callable(getattr(exchange, "fapiPrivateV2GetAccount", None)):
        try:
            account = _fetch_futures_account(exchange)
            equity = _equity_from_futures_account(account or {})
            if equity > 0:
                return equity
        except ccxt.BaseError:
            pass

    balance = exchange.fetch_balance()
    usdt = balance.get("USDT") or {}
    return _decimal(usdt.get("total") or (balance.get("total") or {}).get("USDT"))


def _fetch_usdt_available_balance(exchange, account: dict[str, Any] | None | Any = _ACCOUNT_NOT_PROVIDED) -> Decimal | None:
    if account is not _ACCOUNT_NOT_PROVIDED:
        if account is None:
            return None
        return _available_balance_from_futures_account(account)
    if not callable(getattr(exchange, "fapiPrivateV2GetAccount", None)):
        try:
            balance = exchange.fetch_balance()
            usdt = balance.get("USDT") or {}
            val = usdt.get("free") or (balance.get("free") or {}).get("USDT")
            if val is not None:
                return _decimal(val)
        except Exception:
            pass
        return None
    account = _fetch_futures_account(exchange)
    if account is None:
        return None
    return _available_balance_from_futures_account(account)


def _raw_event_id(event: dict[str, Any], fallback_keys: tuple[str, ...]) -> str:
    for key in fallback_keys:
        value = event.get(key)
        if value not in (None, ""):
            return str(value)
    return json.dumps(event, sort_keys=True, default=str)


def _fetch_binance_income_events(exchange, symbol: str, start_time_ms: int | None = None) -> list[dict[str, Any]]:
    getter = getattr(exchange, "fapiPrivateGetIncome", None)
    if not callable(getter):
        return []
    params: dict[str, Any] = {"symbol": _market_id(exchange, symbol), "limit": 1000}
    if start_time_ms:
        params["startTime"] = start_time_ms
    payload = _call_private_with_demo_retry(getter, params)
    if isinstance(payload, dict):
        return [payload]
    return list(payload or [])


def _fetch_binance_user_trades(exchange, symbol: str, start_time_ms: int | None = None) -> list[dict[str, Any]]:
    getter = getattr(exchange, "fapiPrivateGetUserTrades", None)
    if not callable(getter):
        try:
            limit = 100 if exchange.id == "okx" else 1000
            raw_trades = exchange.fetch_my_trades(symbol, since=start_time_ms, limit=limit)
            trades = [_normalize_trade(exchange, symbol, t) for t in raw_trades or []]
            mapped_trades = []
            for t in trades or []:
                info = t.get("info") or {}
                mapped_trades.append({
                    "id": t.get("id") or info.get("tradeId") or info.get("billId"),
                    "orderId": t.get("order") or info.get("ordId"),
                    "time": t.get("timestamp") or info.get("cTime") or info.get("ts"),
                    "symbol": symbol,
                    "side": t.get("side") or info.get("side"),
                    "price": str(t.get("price") or info.get("fillPx")),
                    "qty": str(t.get("amount") or info.get("fillSz")),
                    "quoteQty": str(t.get("cost") or (Decimal(str(t.get("price") or 0)) * Decimal(str(t.get("amount") or 0)))),
                    "commission": str(t.get("fee", {}).get("cost", 0) or info.get("fee") or 0),
                    "commissionAsset": t.get("fee", {}).get("currency") or info.get("feeCcy") or "USDT",
                    "realizedPnl": str(info.get("pnl") or 0),
                })
            return mapped_trades
        except Exception as e:
            print(f"Error fetching trades on non-Binance exchange {exchange.id}: {e}")
            return []
    params: dict[str, Any] = {"symbol": _market_id(exchange, symbol), "limit": 1000}
    if start_time_ms:
        params["startTime"] = start_time_ms
    payload = _call_private_with_demo_retry(getter, params)
    if isinstance(payload, dict):
        return [payload]
    return list(payload or [])


def _fetch_binance_order_user_trades(exchange, symbol: str, order_id: str | None) -> list[dict[str, Any]]:
    if not order_id:
        return []
    getter = getattr(exchange, "fapiPrivateGetUserTrades", None)
    if not callable(getter):
        try:
            trades = _fetch_binance_user_trades(exchange, symbol)
            return [t for t in trades if str(t.get("orderId")) == str(order_id)]
        except Exception:
            return []
    params: dict[str, Any] = {"symbol": _market_id(exchange, symbol), "orderId": order_id, "limit": 1000}
    payload = _call_private_with_demo_retry(getter, params)
    if isinstance(payload, dict):
        return [payload]
    return list(payload or [])


def _summarize_binance_user_trades(user_trades: list[dict[str, Any]]) -> dict[str, str]:
    amount = Decimal("0")
    quote = Decimal("0")
    realized = Decimal("0")
    commission = Decimal("0")
    trade_count = 0
    for trade in user_trades:
        if not isinstance(trade, dict):
            continue
        qty = _decimal(trade.get("qty") or trade.get("amount"))
        price = _decimal(trade.get("price"))
        if qty > 0 and price > 0:
            amount += qty
            quote += qty * price
        realized += _decimal(trade.get("realizedPnl"))
        fee = _decimal(trade.get("commission"))
        if fee > 0:
            fee = -fee
        commission += fee
        trade_count += 1
    average_price = quote / amount if amount > 0 else Decimal("0")
    net = realized + commission
    return {
        "binance_fill_price": str(average_price),
        "binance_fill_amount": str(amount),
        "binance_realized_pnl_usdt": str(realized),
        "binance_commission_usdt": str(commission),
        "binance_net_pnl_usdt": str(net),
        "binance_user_trade_count": str(trade_count),
    }


def _summarize_binance_income_events(income_events: list[dict[str, Any]]) -> dict[str, str]:
    realized = Decimal("0")
    commission = Decimal("0")
    funding = Decimal("0")
    net = Decimal("0")
    count = 0
    for event in income_events:
        if not isinstance(event, dict):
            continue
        amount = _decimal(event.get("income"))
        income_type = str(event.get("incomeType") or "").upper()
        net += amount
        if income_type == "REALIZED_PNL":
            realized += amount
        elif income_type == "COMMISSION":
            commission += amount
        elif income_type == "FUNDING_FEE":
            funding += amount
        count += 1
    return {
        "binance_realized_pnl_usdt": str(realized),
        "binance_commission_usdt": str(commission),
        "binance_funding_usdt": str(funding),
        "binance_net_pnl_usdt": str(net),
        "binance_income_event_count": str(count),
    }


def _opened_at_ms(open_trade: dict[str, Any] | None) -> int | None:
    if not open_trade:
        return None
    opened_at = open_trade.get("opened_at")
    if not opened_at:
        return None
    try:
        opened = datetime.fromisoformat(str(opened_at).replace("Z", "+00:00"))
    except ValueError:
        return None
    if opened.tzinfo is None:
        opened = opened.replace(tzinfo=timezone.utc)
    return int(opened.timestamp() * 1000)


def _closed_trade_binance_income_payload(
    exchange,
    symbol: str,
    open_trade: dict[str, Any] | None,
    close_time_ms: int,
) -> dict[str, str]:
    opened_ms = _opened_at_ms(open_trade)
    if not opened_ms:
        return {}
    start_time_ms = max(opened_ms - 60_000, 0)
    end_time_ms = close_time_ms + 30_000
    try:
        income_events = _fetch_binance_income_events(exchange, symbol, start_time_ms)
    except ccxt.BaseError as exc:
        print(f"Could not fetch Binance income for closed trade {symbol}: {exc}")
        return {}
    filtered = [
        event
        for event in income_events
        if isinstance(event, dict)
        and start_time_ms <= int(_decimal(event.get("time"))) <= end_time_ms
    ]
    if not filtered:
        return {}
    payload = _summarize_binance_income_events(filtered)
    payload["binance_pnl_source"] = "income"
    return payload


def _sync_binance_performance(exchange, args: argparse.Namespace, state: dict[str, Any], symbols: list[str]) -> None:
    interval = _seconds(getattr(args, "binance_performance_sync_interval_minutes", Decimal("0")))
    if interval <= 0:
        return

    sync_state = state.setdefault("binance_performance", {})
    if not isinstance(sync_state, dict):
        sync_state = {}
        state["binance_performance"] = sync_state

    now = _now_ts()
    last_sync_ts = _decimal(sync_state.get("last_sync_ts"))
    if last_sync_ts > 0 and now - last_sync_ts < interval:
        return

    lookback_ms = int(_milliseconds(getattr(args, "binance_performance_lookback_minutes", Decimal("1440"))))
    previous_sync_ms = int(_decimal(sync_state.get("last_sync_ms")))
    start_time_ms = max(previous_sync_ms - 60_000, _now_ms() - lookback_ms) if previous_sync_ms else _now_ms() - lookback_ms

    income_history = _bounded_history(sync_state, "income_events", MAX_BINANCE_EVENT_HISTORY)
    trade_history = _bounded_history(sync_state, "user_trades", MAX_BINANCE_EVENT_HISTORY)
    seen_income = {str(item.get("event_key")) for item in income_history if isinstance(item, dict)}
    seen_trades = {str(item.get("event_key")) for item in trade_history if isinstance(item, dict)}
    new_income_count = 0
    new_trade_count = 0
    max_event_ms = previous_sync_ms

    for symbol in symbols:
        try:
            income_events = _fetch_binance_income_events(exchange, symbol, start_time_ms)
            user_trades = _fetch_binance_user_trades(exchange, symbol, start_time_ms)
        except ccxt.BaseError as exc:
            print(f"Binance performance sync skipped for {symbol}: {exc}")
            continue

        for event in income_events:
            if not isinstance(event, dict):
                continue
            event_key = f"income:{symbol}:{_raw_event_id(event, ('tranId', 'tradeId', 'time'))}:{event.get('incomeType')}"
            if event_key in seen_income:
                continue
            record = {"event_key": event_key, "symbol": symbol, **event}
            income_history.append(record)
            seen_income.add(event_key)
            new_income_count += 1
            max_event_ms = max(max_event_ms, int(_decimal(event.get("time"))))

        for trade in user_trades:
            if not isinstance(trade, dict):
                continue
            event_key = f"trade:{symbol}:{_raw_event_id(trade, ('id', 'orderId', 'time'))}"
            if event_key in seen_trades:
                continue
            record = {"event_key": event_key, "symbol": symbol, **trade}
            trade_history.append(record)
            seen_trades.add(event_key)
            new_trade_count += 1
            max_event_ms = max(max_event_ms, int(_decimal(trade.get("time"))))

    del income_history[:-MAX_BINANCE_EVENT_HISTORY]
    del trade_history[:-MAX_BINANCE_EVENT_HISTORY]
    sync_state["last_sync_ts"] = str(now)
    sync_state["last_sync_ms"] = str(max(max_event_ms, _now_ms() - 60_000))
    summary = _binance_performance_summary(state)
    print(
        f"binance_performance_sync new_income_events={new_income_count} new_user_trades={new_trade_count} "
        f"realized_pnl={summary['realized_pnl_usdt']} commission={summary['commission_usdt']} "
        f"funding={summary['funding_usdt']} net_income={summary['net_income_usdt']}"
    )


def _binance_performance_summary(state: dict[str, Any]) -> dict[str, Any]:
    sync_state = state.get("binance_performance")
    if not isinstance(sync_state, dict):
        return {
            "income_events": 0,
            "user_trades": 0,
            "realized_pnl_usdt": "0",
            "commission_usdt": "0",
            "funding_usdt": "0",
            "net_income_usdt": "0",
            "by_symbol": {},
        }

    income_events = sync_state.get("income_events") if isinstance(sync_state.get("income_events"), list) else []
    user_trades = sync_state.get("user_trades") if isinstance(sync_state.get("user_trades"), list) else []
    realized = Decimal("0")
    commission = Decimal("0")
    funding = Decimal("0")
    net = Decimal("0")
    by_symbol: dict[str, Decimal] = {}
    for event in income_events:
        if not isinstance(event, dict):
            continue
        amount = _decimal(event.get("income"))
        income_type = str(event.get("incomeType") or "").upper()
        symbol = str(event.get("symbol") or "unknown")
        net += amount
        by_symbol[symbol] = by_symbol.get(symbol, Decimal("0")) + amount
        if income_type == "REALIZED_PNL":
            realized += amount
        elif income_type == "COMMISSION":
            commission += amount
        elif income_type == "FUNDING_FEE":
            funding += amount

    return {
        "income_events": len(income_events),
        "user_trades": len(user_trades),
        "realized_pnl_usdt": str(realized),
        "commission_usdt": str(commission),
        "funding_usdt": str(funding),
        "net_income_usdt": str(net),
        "by_symbol": {symbol: str(value) for symbol, value in sorted(by_symbol.items())},
    }


def _binance_symbol_performance(state: dict[str, Any]) -> dict[str, dict[str, Decimal]]:
    sync_state = state.get("binance_performance")
    if not isinstance(sync_state, dict):
        return {}
    income_events = sync_state.get("income_events") if isinstance(sync_state.get("income_events"), list) else []
    stats: dict[str, dict[str, Decimal]] = {}
    for event in income_events:
        if not isinstance(event, dict):
            continue
        symbol = _standard_usdm_symbol(str(event.get("symbol") or ""))
        if not symbol:
            continue
        symbol_stats = stats.setdefault(
            symbol,
            {
                "net_income": Decimal("0"),
                "realized_pnl": Decimal("0"),
                "commission": Decimal("0"),
                "funding": Decimal("0"),
                "realized_events": Decimal("0"),
            },
        )
        amount = _decimal(event.get("income"))
        income_type = str(event.get("incomeType") or "").upper()
        symbol_stats["net_income"] += amount
        if income_type == "REALIZED_PNL":
            symbol_stats["realized_pnl"] += amount
            symbol_stats["realized_events"] += Decimal("1")
        elif income_type == "COMMISSION":
            symbol_stats["commission"] += amount
        elif income_type == "FUNDING_FEE":
            symbol_stats["funding"] += amount
    return stats


def _binance_symbol_score_penalty(state: dict[str, Any], args: argparse.Namespace, symbol: str) -> Decimal:
    if not getattr(args, "binance_symbol_score_penalty", True):
        return Decimal("0")
    stats = _binance_symbol_performance(state).get(symbol)
    if not stats:
        return Decimal("0")
    min_trades = Decimal(str(getattr(args, "binance_symbol_guard_min_trades", 3)))
    if stats["realized_events"] < min_trades:
        return Decimal("0")
    max_loss = getattr(args, "binance_symbol_guard_max_net_loss_usdt", Decimal("5"))
    if max_loss <= 0 or stats["net_income"] >= 0:
        return Decimal("0")
    max_penalty = getattr(args, "binance_symbol_score_penalty_max", Decimal("0.30"))
    scaled = abs(stats["net_income"]) / max_loss * Decimal("0.10")
    return _clamp_decimal(scaled, Decimal("0"), max_penalty)


def _maybe_apply_binance_symbol_performance_guard(
    state: dict[str, Any],
    args: argparse.Namespace,
) -> None:
    if not getattr(args, "binance_symbol_performance_guard", False):
        return
    min_trades = Decimal(str(getattr(args, "binance_symbol_guard_min_trades", 3)))
    max_loss = getattr(args, "binance_symbol_guard_max_net_loss_usdt", Decimal("5"))
    if min_trades <= 0 or max_loss <= 0:
        return
    now = _now_ts()
    blacklist_seconds = _seconds(getattr(args, "binance_symbol_guard_blacklist_minutes", Decimal("720")))
    guard_state = state.setdefault("binance_symbol_performance_guard", {})
    if not isinstance(guard_state, dict):
        guard_state = {}
        state["binance_symbol_performance_guard"] = guard_state
    for symbol, stats in _binance_symbol_performance(state).items():
        if stats["realized_events"] < min_trades or stats["net_income"] > -max_loss:
            continue
        control = _symbol_control(state, symbol)
        blacklist_until = _decimal(control.get("blacklist_until"))
        if blacklist_until > now:
            continue
        until = now + blacklist_seconds
        control["blacklist_until"] = str(until)
        reason = (
            f"binance_net_income={stats['net_income']:.4f} "
            f"realized_events={int(stats['realized_events'])} threshold=-{max_loss}"
        )
        guard_state[symbol] = {
            "last_blacklist_ts": str(now),
            "blacklist_until": str(until),
            "reason": reason,
        }
        print(f"Binance performance guard: blacklisting {symbol} {reason}")
        _notify(args, "risk", f"Binance performance guard blacklisted {symbol} reason={reason}")


def _position_from_futures_risk(payload: dict[str, Any], symbol: str) -> PositionSummary | None:
    raw_amount = _decimal(payload.get("positionAmt"))
    if raw_amount == 0:
        return None
    side = "long" if raw_amount > 0 else "short"
    return PositionSummary(
        side=side,
        amount=abs(raw_amount),
        entry_price=_decimal(payload.get("entryPrice") or payload.get("breakEvenPrice")),
        unrealized_pnl=_decimal(payload.get("unRealizedProfit")),
    )


def _fetch_position(exchange, symbol: str, account: dict[str, Any] | None | Any = _ACCOUNT_NOT_PROVIDED) -> PositionSummary | None:
    try:
        positions = _fetch_account_positions(exchange, account)
        return positions.get(_market_id(exchange, symbol))
    except Exception as exc:
        print(f"Error fetching position for {symbol}: {exc}")
        return None


def _fetch_account_positions(exchange, account: dict[str, Any] | None | Any = _ACCOUNT_NOT_PROVIDED) -> dict[str, PositionSummary]:
    if account is _ACCOUNT_NOT_PROVIDED:
        try:
            account = _fetch_futures_account(exchange)
        except ccxt.BaseError as exc:
            print(f"Could not fetch futures account positions; skipping this cycle to avoid duplicate entries: {exc}")
            raise

    if account is not None:
        positions: dict[str, PositionSummary] = {}
        for raw_position in account.get("positions") or []:
            market_id = str(raw_position.get("symbol", ""))
            if not market_id:
                continue
            position = _position_from_futures_risk(raw_position, market_id)
            if position is not None:
                positions[market_id] = position
        return positions

    # Non-Binance / OKX fallback
    try:
        raw_positions = exchange.fetch_positions()
    except ccxt.BaseError as exc:
        print(f"Could not fetch active positions; skipping this cycle to avoid duplicate entries: {exc}")
        raise

    positions: dict[str, PositionSummary] = {}
    for pos in raw_positions:
        info = pos.get("info") or {}
        contracts = _decimal(pos.get("contracts") or info.get("pos") or info.get("positionAmt") or 0)
        if contracts == 0:
            continue
        symbol = pos.get("symbol")
        if not symbol:
            continue
        market_id = _market_id(exchange, symbol)
        contract_size = _decimal(pos.get("contractSize") or (exchange.markets.get(symbol) or {}).get("contractSize") or 1)
        amount = abs(contracts * contract_size)
        side = pos.get("side")
        if not side:
            side = "long" if contracts > 0 else "short"
        else:
            side = side.lower()
        positions[market_id] = PositionSummary(
            side=side,
            amount=amount,
            entry_price=_decimal(pos.get("entryPrice") or info.get("entryPrice")),
            unrealized_pnl=_decimal(pos.get("unrealizedPnl") or info.get("unRealizedProfit")),
        )
    return positions


def _open_position_symbols(
    exchange,
    symbols: list[str],
    account: dict[str, Any] | None | Any = _ACCOUNT_NOT_PROVIDED,
) -> list[str]:
    account_positions = _fetch_account_positions(exchange, account)
    return [symbol for symbol in symbols if _market_id(exchange, symbol) in account_positions]


def _open_trades_by_symbol(state: dict[str, Any]) -> dict[str, Any]:
    open_trades = state.setdefault("open_trades", {})
    if not isinstance(open_trades, dict):
        open_trades = {}
        state["open_trades"] = open_trades
    legacy_open_trade = state.get("open_trade")
    if isinstance(legacy_open_trade, dict):
        symbol = legacy_open_trade.get("symbol")
        if symbol and symbol not in open_trades:
            open_trades[str(symbol)] = legacy_open_trade
    return open_trades


def _activate_symbol_open_trade(state: dict[str, Any], symbol: str) -> None:
    open_trades = _open_trades_by_symbol(state)
    open_trade = open_trades.get(symbol)
    if isinstance(open_trade, dict):
        state["open_trade"] = open_trade
    else:
        current = state.get("open_trade")
        if isinstance(current, dict) and current.get("symbol") == symbol:
            open_trades[symbol] = current
        else:
            state.pop("open_trade", None)


def _store_active_symbol_open_trade(state: dict[str, Any], symbol: str) -> None:
    open_trades = _open_trades_by_symbol(state)
    open_trade = state.get("open_trade")
    if isinstance(open_trade, dict) and open_trade.get("symbol") == symbol:
        open_trades[symbol] = open_trade
    else:
        open_trades.pop(symbol, None)


def _restore_primary_open_trade(state: dict[str, Any], symbols: list[str]) -> None:
    open_trades = _open_trades_by_symbol(state)
    for symbol in symbols:
        open_trade = open_trades.get(symbol)
        if isinstance(open_trade, dict):
            state["open_trade"] = open_trade
            return
    state.pop("open_trade", None)


def _account_position_block_reason(
    exchange,
    args: argparse.Namespace,
    current_symbol: str,
    current_position: PositionSummary | None,
    account: dict[str, Any] | None | Any = _ACCOUNT_NOT_PROVIDED,
) -> str | None:
    account_positions = _fetch_account_positions(exchange, account)
    if not account_positions:
        return None

    current_market_id = _market_id(exchange, current_symbol)
    other_positions = {
        market_id: position
        for market_id, position in account_positions.items()
        if market_id != current_market_id
    }
    max_open_positions = int(getattr(args, "max_open_positions", 1))
    if max_open_positions <= 1 and other_positions:
        summary = ", ".join(
            f"{market_id}:{position.side}:{position.amount}" for market_id, position in sorted(other_positions.items())
        )
        return f"account already has open position(s): {summary}"

    if current_position is None and current_market_id in account_positions:
        position = account_positions[current_market_id]
        return f"account already has open position on {current_market_id}: {position.side}:{position.amount}"
    if current_position is None and len(account_positions) >= max_open_positions:
        return f"account already has {len(account_positions)} open position(s), max_open_positions={max_open_positions}"
    return None


def _cancel_open_orders(exchange, symbol: str, execute: bool) -> None:
    print(f"Cancel open orders for {symbol} execute={execute}")
    if not execute:
        return
    try:
        exchange.cancel_all_orders(symbol)
    except ccxt.BaseError as exc:
        try:
            open_orders = exchange.fetch_open_orders(symbol)
            for order in open_orders:
                exchange.cancel_order(order['id'], symbol)
                print(f"Cancelled open order {order['id']} for {symbol}")
        except ccxt.BaseError as exc2:
            print(f"Could not cancel open orders for {symbol}; continuing with close: {exc2}")
    _cancel_open_algo_orders(exchange, symbol)


def _cancel_open_algo_orders(exchange, symbol: str) -> None:
    market_id = _market_id(exchange, symbol)
    cancel_all_algo = getattr(exchange, "fapiPrivateDeleteAlgoOpenOrders", None)
    if not callable(cancel_all_algo):
        cancel_all_algo = getattr(exchange, "fapiPrivateDeleteAllOpenAlgoOrders", None)
    if callable(cancel_all_algo):
        try:
            cancel_all_algo({"symbol": market_id})
            print(f"Cancel open algo orders for {symbol}: done")
            return
        except ccxt.BaseError as exc:
            print(f"Could not cancel all open algo orders for {symbol}; trying per-order cancel: {exc}")

    if exchange.id == "okx":
        cancel_algo = getattr(exchange, "privatePostTradeCancelAlgos", None)
        if callable(cancel_algo):
            open_algo_orders = _fetch_open_algo_orders(exchange, symbol)
            if open_algo_orders:
                payload = []
                for order in open_algo_orders:
                    algo_id = order.get("algoId")
                    if algo_id:
                        payload.append({"algoId": algo_id, "instId": market_id})
                if payload:
                    try:
                        cancel_algo(payload)
                        print(f"Cancel open algo orders for {symbol}: {payload}")
                        return
                    except ccxt.BaseError as exc:
                        print(f"Could not cancel algo orders for {symbol} on OKX: {exc}")
        return

    open_algo_orders = _fetch_open_algo_orders(exchange, symbol)
    cancel_algo = getattr(exchange, "fapiPrivateDeleteAlgoOrder", None)
    if not callable(cancel_algo):
        return
    for order in open_algo_orders:
        algo_id = order.get("algoId") or (order.get("info") or {}).get("algoId")
        client_algo_id = order.get("clientAlgoId") or (order.get("info") or {}).get("clientAlgoId")
        params = {"algoId": algo_id} if algo_id else {"clientAlgoId": client_algo_id}
        if not params.get("algoId") and not params.get("clientAlgoId"):
            continue
        try:
            cancel_algo(params)
            print(f"Cancel open algo order for {symbol}: {params}")
        except ccxt.BaseError as exc:
            print(f"Could not cancel algo order for {symbol} {params}: {exc}")


def _close_position(
    exchange,
    symbol: str,
    position: PositionSummary,
    dual_side: bool,
    execute: bool,
    close_amount: Decimal | None = None,
) -> OrderExecutionSummary | None:
    side = "sell" if position.side == "long" else "buy"
    params: dict[str, Any] = {}
    if dual_side:
        if exchange.id == "okx":
            params["posSide"] = "long" if position.side == "long" else "short"
        else:
            params["positionSide"] = "LONG" if position.side == "long" else "SHORT"
    else:
        params["reduceOnly"] = True

    amt = close_amount if close_amount is not None else position.amount
    print(f"Close position: side={side} amount={amt} execute={execute}")
    if execute:
        if close_amount is None:
            _cancel_open_orders(exchange, symbol, execute)
        order = _normalize_order(exchange, exchange.create_order(
            symbol,
            "market",
            side,
            _to_exchange_amount(exchange, symbol, amt),
            None,
            params,
        ))
        print(f"Close order: id={order.get('id')} status={order.get('status')}")
        price = _order_average_price(order, Decimal("0"))
        if price <= 0 and order.get("id"):
            try:
                time.sleep(0.5)
                fetched_order = _normalize_order(exchange, exchange.fetch_order(order["id"], symbol))
                price = _order_average_price(fetched_order, Decimal("0"))
            except Exception as e:
                print(f"Could not fetch closed order average price for {symbol}: {e}")

        if price <= 0 and order.get("id"):
            try:
                since_ms = int(time.time() * 1000) - 30000
                raw_trades = exchange.fetch_my_trades(symbol, since=since_ms, limit=10)
                trades = [_normalize_trade(exchange, symbol, t) for t in raw_trades or []]
                order_trades = [t for t in trades if t.get("order") == order["id"]]
                if order_trades:
                    total_price_volume = sum(_decimal(t.get("price") or 0) * _decimal(t.get("amount") or 0) for t in order_trades)
                    total_amount = sum(_decimal(t.get("amount") or 0) for t in order_trades)
                    if total_amount > 0:
                        price = total_price_volume / total_amount
            except Exception as e:
                print(f"Could not fetch trades for close order {order.get('id')}: {e}")

        report = _order_execution_report(exchange, symbol, order)
        binance_fill_price = _decimal(report.get("binance_fill_price"))
        if binance_fill_price > 0:
            price = binance_fill_price
        if price <= 0:
            price = position.entry_price
        if report:
            report["binance_pnl_source"] = "user_trades"
            report["exit_price_source"] = "binance_user_trades" if binance_fill_price > 0 else "order_or_fallback"
        return OrderExecutionSummary(price=price, report=report)
    return None


def _close_execution_price(close_execution: OrderExecutionSummary | None, fallback: Decimal) -> Decimal:
    if close_execution and close_execution.price and close_execution.price > 0:
        return close_execution.price
    return fallback


def _close_binance_report(
    exchange,
    symbol: str,
    open_trade: dict[str, Any] | None,
    close_execution: OrderExecutionSummary | None,
    close_time_ms: int,
) -> dict[str, str]:
    report = dict(close_execution.report) if close_execution and close_execution.report else {}
    income_report = _closed_trade_binance_income_payload(exchange, symbol, open_trade, close_time_ms)
    if income_report:
        report.update(income_report)
    return report


def _order_average_price(order: dict[str, Any], fallback: Decimal) -> Decimal:
    info = order.get("info") or {}
    for value in (
        order.get("average"),
        order.get("avgPrice"),
        order.get("price"),
        info.get("avgPrice"),
        info.get("price"),
    ):
        parsed = _decimal(value)
        if parsed > 0:
            return parsed
    return fallback


def _order_id(order: dict[str, Any]) -> str | None:
    info = order.get("info") or {}
    for value in (order.get("id"), order.get("orderId"), info.get("orderId"), info.get("id")):
        if value not in (None, ""):
            return str(value)
    return None


def _order_embedded_trades(order: dict[str, Any]) -> list[dict[str, Any]]:
    trades = order.get("trades")
    if isinstance(trades, list):
        return [trade for trade in trades if isinstance(trade, dict)]
    info = order.get("info") or {}
    info_trades = info.get("trades")
    if isinstance(info_trades, list):
        return [trade for trade in info_trades if isinstance(trade, dict)]
    return []


def _order_execution_report(exchange, symbol: str, order: dict[str, Any]) -> dict[str, str]:
    order_id = _order_id(order)
    user_trades = _order_embedded_trades(order)
    if not user_trades:
        try:
            user_trades = _fetch_binance_order_user_trades(exchange, symbol, order_id)
        except ccxt.BaseError as exc:
            print(f"Could not fetch Binance user trades for order {order_id}: {exc}")
            user_trades = []
    if not user_trades:
        return {"binance_order_id": order_id or ""}
    report = _summarize_binance_user_trades(user_trades)
    report["binance_order_id"] = order_id or ""
    return report


def _order_filled_amount(order: dict[str, Any]) -> Decimal:
    info = order.get("info") or {}
    for value in (
        order.get("filled"),
        order.get("filledQty"),
        order.get("executedQty"),
        info.get("executedQty"),
        info.get("cumQty"),
    ):
        parsed = _decimal(value)
        if parsed > 0:
            return parsed
    return Decimal("0")


def _entry_limit_price(ticker: dict[str, Any], side: str, fallback: Decimal, offset_pct: Decimal) -> Decimal:
    bid = _decimal(ticker.get("bid"))
    ask = _decimal(ticker.get("ask"))
    offset = offset_pct / Decimal("100")
    if side == "buy":
        base = ask if ask > 0 else fallback
        return base * (Decimal("1") + offset)
    base = bid if bid > 0 else fallback
    return base * (Decimal("1") - offset)


def _wait_for_order_fill(exchange, symbol: str, order: dict[str, Any], amount: Decimal, timeout_seconds: float) -> dict[str, Any]:
    order_id = order.get("id")
    deadline = time.time() + max(timeout_seconds, 0)
    current = order
    while time.time() < deadline:
        filled = _order_filled_amount(current)
        status = str(current.get("status") or "").lower()
        if filled >= amount or status in {"closed", "filled"}:
            return current
        if order_id and callable(getattr(exchange, "fetch_order", None)):
            try:
                current = _normalize_order(exchange, exchange.fetch_order(order_id, symbol))
            except ccxt.BaseError as exc:
                print(f"Could not fetch entry limit order status; continuing to wait: {exc}")
        time.sleep(0.5)
    return current


def _submit_entry_order(
    exchange,
    symbol: str,
    side: str,
    amount: str,
    ticker: dict[str, Any],
    last_price: Decimal,
    params: dict[str, Any],
    args: argparse.Namespace,
    execution_mode_override: str | None = None,
    limit_price_override: Decimal | None = None,
) -> tuple[dict[str, Any], str]:
    mode = execution_mode_override or getattr(args, "execution_mode", "market")
    if mode == "market":
        order = _normalize_order(exchange, exchange.create_order(
            symbol,
            "market",
            side,
            _to_exchange_amount(exchange, symbol, amount),
            None,
            params,
        ))
        return order, "market"

    amount_decimal = _decimal(amount)
    limit_price = limit_price_override or _entry_limit_price(
        ticker,
        side,
        last_price,
        getattr(args, "limit_entry_price_offset_pct", Decimal("0.02")),
    )
    limit_price_text = _price_to_precision(exchange, symbol, limit_price)
    limit_params = dict(params)
    if mode == "marketable_limit":
        limit_params["timeInForce"] = "GTC"
    order = _normalize_order(exchange, exchange.create_order(
        symbol,
        "limit",
        side,
        _to_exchange_amount(exchange, symbol, amount),
        limit_price_text,
        limit_params,
    ))
    print(f"Limit entry submitted: id={order.get('id')} price={limit_price_text} mode={mode}")
    checked_order = _wait_for_order_fill(
        exchange,
        symbol,
        order,
        amount_decimal,
        getattr(args, "limit_entry_timeout_seconds", 8.0),
    )
    fill_ratio = Decimal("0")
    if amount_decimal > 0:
        fill_ratio = _order_filled_amount(checked_order) / amount_decimal
    if fill_ratio >= getattr(args, "limit_entry_min_fill_ratio", Decimal("1")):
        return checked_order, f"{mode}:filled_ratio={fill_ratio:.4f}"

    filled_amount = _order_filled_amount(checked_order)
    print(f"Limit entry not filled enough: fill_ratio={fill_ratio:.4f}; canceling and falling back.")
    try:
        if checked_order.get("id"):
            exchange.cancel_order(checked_order.get("id"), symbol)
    except ccxt.BaseError as exc:
        print(f"Could not cancel unfilled entry limit order; continuing carefully: {exc}")

    if mode == "limit":
        raise RuntimeError(f"Limit entry did not fill enough; fill_ratio={fill_ratio:.4f}")

    remaining_amount = amount_decimal - filled_amount
    if remaining_amount <= 0:
        return checked_order, f"{mode}:partial_fill_completed_after_cancel fill_ratio={fill_ratio:.4f}"
    remaining_amount_text = exchange.amount_to_precision(symbol, float(remaining_amount))
    market_order = _normalize_order(exchange, exchange.create_order(
        symbol,
        "market",
        side,
        _to_exchange_amount(exchange, symbol, remaining_amount_text),
        None,
        params,
    ))
    market_filled = _order_filled_amount(market_order)
    if market_filled <= 0:
        market_filled = _decimal(remaining_amount_text)
    limit_price = _order_average_price(checked_order, limit_price)
    market_price = _order_average_price(market_order, last_price)
    combined_filled = filled_amount + market_filled
    if combined_filled > 0 and limit_price > 0 and market_price > 0:
        combined_average = ((filled_amount * limit_price) + (market_filled * market_price)) / combined_filled
        combined_order = dict(market_order)
        combined_order["average"] = str(combined_average)
        combined_order["filled"] = str(combined_filled)
        combined_order["amount"] = str(combined_filled)
        return combined_order, (
            f"{mode}:fallback_market_remaining={remaining_amount_text} "
            f"after_fill_ratio={fill_ratio:.4f}"
        )
    return market_order, f"{mode}:fallback_market_remaining={remaining_amount_text} after_fill_ratio={fill_ratio:.4f}"


def _protective_stop_take_prices(
    side: str,
    entry_price: Decimal,
    stop_loss_pct: Decimal,
    take_profit_pct: Decimal,
) -> tuple[Decimal, Decimal]:
    stop_fraction = stop_loss_pct / Decimal("100")
    take_profit_fraction = take_profit_pct / Decimal("100")
    if side == "buy":
        return (
            entry_price * (Decimal("1") - stop_fraction),
            entry_price * (Decimal("1") + take_profit_fraction),
        )
    return (
        entry_price * (Decimal("1") + stop_fraction),
        entry_price * (Decimal("1") - take_profit_fraction),
    )


def _price_to_precision(exchange, symbol: str, price: Decimal) -> str:
    try:
        return str(exchange.price_to_precision(symbol, price))
    except Exception:
        return str(price)


def _amount_from_notional_with_cap(
    exchange,
    symbol: str,
    notional_usdt: Decimal,
    last_price: Decimal,
    max_notional_usdt: Decimal,
) -> tuple[str, Decimal]:
    amount, estimated_notional = _amount_from_notional(exchange, symbol, notional_usdt, last_price)
    if estimated_notional <= max_notional_usdt:
        return amount, estimated_notional

    market = exchange.markets[symbol]
    max_amount_base = max_notional_usdt / last_price
    step_base = _market_amount_step(market)
    if step_base is not None and step_base > 0:
        max_amount_base = (max_amount_base / step_base).to_integral_value(rounding=ROUND_DOWN) * step_base

    if exchange.id == "okx":
        contract_size = Decimal(str(market.get("contractSize") or 1))
        contracts = max_amount_base / contract_size
        capped_contracts = exchange.amount_to_precision(symbol, float(contracts))
        capped_amount_base = Decimal(capped_contracts) * contract_size
        capped_amount = str(capped_amount_base)
    else:
        capped_amount = exchange.amount_to_precision(symbol, float(max_amount_base))
        capped_amount_base = Decimal(capped_amount)

    capped_notional = capped_amount_base * last_price
    while step_base is not None and step_base > 0 and capped_notional > max_notional_usdt and capped_amount_base > step_base:
        max_amount_base = capped_amount_base - step_base
        if exchange.id == "okx":
            contract_size = Decimal(str(market.get("contractSize") or 1))
            contracts = max_amount_base / contract_size
            capped_contracts = exchange.amount_to_precision(symbol, float(contracts))
            capped_amount_base = Decimal(capped_contracts) * contract_size
            capped_amount = str(capped_amount_base)
        else:
            capped_amount = exchange.amount_to_precision(symbol, float(max_amount_base))
            capped_amount_base = Decimal(capped_amount)
        capped_notional = capped_amount_base * last_price

    if capped_amount_base <= 0:
        raise RuntimeError(f"Calculated capped amount is not positive: {capped_amount}")
    return capped_amount, capped_notional


def _place_native_protection(
    exchange,
    symbol: str,
    side: str,
    amount: str,
    entry_price: Decimal,
    args: argparse.Namespace,
    dual_side: bool,
    stop_loss_pct: Decimal | None = None,
    take_profit_pct: Decimal | None = None,
) -> dict[str, str]:
    if not getattr(args, "native_protection", True):
        return {}

    stop_price, take_profit_price = _protective_stop_take_prices(
        side,
        entry_price,
        stop_loss_pct if stop_loss_pct is not None else args.stop_loss_pct,
        take_profit_pct if take_profit_pct is not None else args.take_profit_pct,
    )
    close_side = "sell" if side == "buy" else "buy"
    return _place_native_protection_prices(
        exchange,
        symbol,
        side,
        close_side,
        amount,
        stop_price,
        take_profit_price,
        args,
        dual_side,
    )


def _place_native_protection_prices(
    exchange,
    symbol: str,
    entry_side: str,
    close_side: str,
    amount: str,
    stop_price: Decimal,
    take_profit_price: Decimal,
    args: argparse.Namespace,
    dual_side: bool,
) -> dict[str, str]:
    if not getattr(args, "native_protection", True):
        return {}

    base_params: dict[str, Any] = {"workingType": "MARK_PRICE"}
    if dual_side:
        if exchange.id == "okx":
            base_params["posSide"] = "long" if entry_side == "buy" else "short"
        else:
            base_params["positionSide"] = _position_side_for_open(entry_side)
    else:
        base_params["reduceOnly"] = True

    print(
        f"Native protection: stop_price={stop_price:.8f} "
        f"take_profit_price={take_profit_price:.8f} execute={args.execute}"
    )
    if not args.execute:
        return {}

    try:
        stop_order, take_profit_order = _create_native_protection_orders(
            exchange,
            symbol,
            close_side,
            amount,
            _price_to_precision(exchange, symbol, stop_price),
            _price_to_precision(exchange, symbol, take_profit_price),
            base_params,
        )
    except ccxt.BaseError as exc:
        print(f"Native protection failed; software exits remain active: {exc}")
        return {}

    print(
        f"Native protection orders: stop_id={_protection_order_id(stop_order)} "
        f"take_profit_id={_protection_order_id(take_profit_order)}"
    )
    return {
        "stop_order_id": _protection_order_id(stop_order),
        "take_profit_order_id": _protection_order_id(take_profit_order),
        "stop_price": str(stop_price),
        "take_profit_price": str(take_profit_price),
    }


def _create_native_protection_orders(
    exchange,
    symbol: str,
    close_side: str,
    amount: str,
    stop_price: str,
    take_profit_price: str,
    base_params: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    post_algo_order = getattr(exchange, "fapiPrivatePostAlgoOrder", None)
    if callable(post_algo_order):
        market_id = _market_id(exchange, symbol)
        algo_base_params: dict[str, Any] = {
            "algoType": "CONDITIONAL",
            "symbol": market_id,
            "side": close_side.upper(),
            "workingType": str(base_params.get("workingType", "MARK_PRICE")),
            "closePosition": "true",
        }
        if "positionSide" in base_params:
            algo_base_params["positionSide"] = base_params["positionSide"]
        if "posSide" in base_params:
            algo_base_params["posSide"] = base_params["posSide"]
        stop_order = post_algo_order(
            {
                **algo_base_params,
                "type": "STOP_MARKET",
                "triggerPrice": stop_price,
            }
        )
        take_profit_order = post_algo_order(
            {
                **algo_base_params,
                "type": "TAKE_PROFIT_MARKET",
                "triggerPrice": take_profit_price,
            }
        )
        return stop_order, take_profit_order

    if exchange.id == "okx":
        okx_params = {k: v for k, v in base_params.items() if k != "reduceOnly"}
        okx_params["cxlOnPosClose"] = True
        okx_amount = _to_exchange_amount(exchange, symbol, amount)
        stop_order = _normalize_order(exchange, exchange.create_order(
            symbol,
            "market",
            close_side,
            okx_amount,
            None,
            {
                **okx_params,
                "triggerPrice": stop_price,
            },
        ))
        take_profit_order = _normalize_order(exchange, exchange.create_order(
            symbol,
            "market",
            close_side,
            okx_amount,
            None,
            {
                **okx_params,
                "triggerPrice": take_profit_price,
            },
        ))
        return stop_order, take_profit_order

    stop_order = exchange.create_order(
        symbol,
        "STOP_MARKET",
        close_side,
        amount,
        None,
        {
            **base_params,
            "stopPrice": stop_price,
        },
    )
    take_profit_order = exchange.create_order(
        symbol,
        "TAKE_PROFIT_MARKET",
        close_side,
        amount,
        None,
        {
            **base_params,
            "stopPrice": take_profit_price,
        },
    )
    return stop_order, take_profit_order


def _protection_order_id(order: dict[str, Any]) -> str:
    return str(order.get("algoId") or order.get("id") or (order.get("info") or {}).get("algoId") or "")


def _fetch_open_algo_orders(exchange, symbol: str, position: PositionSummary | None = None) -> list[dict[str, Any]]:
    getter = getattr(exchange, "fapiPrivateGetOpenAlgoOrders", None)
    if getter is None and exchange.id == "okx":
        getter = getattr(exchange, "privateGetTradeOrdersAlgoPending", None)
    if not callable(getter):
        return []
    try:
        if exchange.id == "okx":
            market_id = _market_id(exchange, symbol)
            mapped_orders = []
            for ord_type in ["trigger", "conditional"]:
                response = getter({"instId": market_id, "ordType": ord_type, "instType": "SWAP"})
                if response and "data" in response:
                    for o in response["data"]:
                        trigger_px = float(o.get("triggerPx") or 0)
                        if position:
                            is_long = (position.side == "long")
                            orderType = "STOP_MARKET" if (trigger_px < float(position.entry_price) if is_long else trigger_px > float(position.entry_price)) else "TAKE_PROFIT_MARKET"
                        else:
                            try:
                                ticker = exchange.fetch_ticker(symbol)
                                last_px = float(ticker.get("last") or 0)
                            except Exception:
                                last_px = 0.0
                            orderType = "STOP_MARKET" if (trigger_px < last_px and last_px > 0) else "TAKE_PROFIT_MARKET"
                        mapped_orders.append({
                            "algoId": o.get("algoId"),
                            "symbol": symbol,
                            "orderType": orderType,
                            "algoStatus": "NEW" if o.get("state") == "live" else o.get("state"),
                        })
            return mapped_orders
        else:
            orders = getter({"symbol": _market_id(exchange, symbol), "algoType": "CONDITIONAL"})
    except ccxt.BaseError as exc:
        print(f"Could not inspect open algo protection orders for {symbol}: {exc}")
        return []
    if isinstance(orders, dict):
        return [orders]
    return list(orders or [])


def _native_protection_order_types(exchange, symbol: str, position: PositionSummary | None = None) -> list[str]:
    algo_orders = _fetch_open_algo_orders(exchange, symbol, position)
    if algo_orders:
        return [
            str(order.get("orderType") or order.get("type") or (order.get("info") or {}).get("orderType") or "").upper()
            for order in algo_orders
            if str(order.get("algoStatus") or order.get("status") or "NEW").upper() == "NEW"
        ]

    try:
        orders = exchange.fetch_open_orders(symbol)
    except ccxt.BaseError as exc:
        print(f"Could not inspect open protection orders for {symbol}: {exc}")
        return ["UNKNOWN"]

    return [
        str(order.get("type") or (order.get("info") or {}).get("type") or "").upper()
        for order in orders
    ]


def _native_protection_order_count(exchange, symbol: str, position: PositionSummary | None = None) -> int:
    return sum(1 for order_type in _native_protection_order_types(exchange, symbol, position) if order_type in {"STOP_MARKET", "TAKE_PROFIT_MARKET"})


def _native_protection_prices_from_orders(exchange, symbol: str, position: PositionSummary | None = None) -> tuple[Decimal | None, Decimal | None]:
    stop_price: Decimal | None = None
    take_profit_price: Decimal | None = None
    for order in _fetch_open_algo_orders(exchange, symbol, position):
        if str(order.get("algoStatus") or order.get("status") or "NEW").upper() != "NEW":
            continue
        order_type = str(order.get("orderType") or order.get("type") or (order.get("info") or {}).get("orderType") or "").upper()
        raw_price = (
            order.get("triggerPrice")
            or order.get("stopPrice")
            or (order.get("info") or {}).get("triggerPrice")
            or (order.get("info") or {}).get("stopPrice")
        )
        price = _decimal(raw_price)
        if price <= 0:
            continue
        if order_type == "STOP_MARKET":
            stop_price = price
        elif order_type == "TAKE_PROFIT_MARKET":
            take_profit_price = price
    return stop_price, take_profit_price


def _more_protective_stop(position: PositionSummary, current_stop: Decimal, candidate_stop: Decimal) -> bool:
    if current_stop <= 0:
        return candidate_stop > 0
    if position.side == "long":
        return candidate_stop > current_stop
    return candidate_stop < current_stop


def _closer_take_profit(position: PositionSummary, current_take_profit: Decimal, candidate_take_profit: Decimal, last_price: Decimal) -> bool:
    if candidate_take_profit <= 0:
        return False
    if position.side == "long":
        if candidate_take_profit <= last_price:
            return False
        return current_take_profit <= 0 or candidate_take_profit < current_take_profit
    if candidate_take_profit >= last_price:
        return False
    return current_take_profit <= 0 or candidate_take_profit > current_take_profit


def _reprice_candidate_prices(
    position: PositionSummary,
    open_trade: dict[str, Any],
    last_price: Decimal,
    args: argparse.Namespace,
) -> tuple[Decimal, Decimal]:
    exit_plan = _exit_plan_from_open_trade(open_trade, args)
    side = "buy" if position.side == "long" else "sell"
    base_stop, base_take_profit = _protective_stop_take_prices(
        side,
        position.entry_price,
        exit_plan.stop_loss_pct,
        exit_plan.take_profit_pct,
    )
    desired_stop = base_stop
    desired_take_profit = base_take_profit
    favorable_pct = _favorable_move_pct(position, open_trade)
    if favorable_pct >= getattr(args, "breakeven_trigger_pct", Decimal("0")):
        offset = getattr(args, "breakeven_offset_pct", Decimal("0")) / Decimal("100")
        breakeven_stop = (
            position.entry_price * (Decimal("1") + offset)
            if position.side == "long"
            else position.entry_price * (Decimal("1") - offset)
        )
        if _more_protective_stop(position, desired_stop, breakeven_stop):
            desired_stop = breakeven_stop
    if favorable_pct >= getattr(args, "trailing_activation_pct", Decimal("0")):
        distance = getattr(args, "trailing_distance_pct", Decimal("0")) / Decimal("100")
        if position.side == "long":
            highest_price = _decimal(open_trade.get("highest_price"), default=str(last_price))
            trailing_stop = highest_price * (Decimal("1") - distance)
        else:
            lowest_price = _decimal(open_trade.get("lowest_price"), default=str(last_price))
            trailing_stop = lowest_price * (Decimal("1") + distance)
        if _more_protective_stop(position, desired_stop, trailing_stop):
            desired_stop = trailing_stop

    current_take_profit = _decimal((open_trade.get("native_protection") or {}).get("take_profit_price"))
    if not _closer_take_profit(position, current_take_profit, desired_take_profit, last_price):
        desired_take_profit = current_take_profit if current_take_profit > 0 else base_take_profit
    return desired_stop, desired_take_profit


def _maybe_reprice_native_protection(
    exchange,
    state: dict[str, Any],
    symbol: str,
    position: PositionSummary,
    args: argparse.Namespace,
    dual_side: bool,
    open_trade: dict[str, Any],
    last_price: Decimal,
) -> bool:
    if not getattr(args, "dynamic_protection_reprice", False):
        return False
    if not getattr(args, "native_protection", True) or not args.execute:
        return False
    now = _now_ts()
    interval = _seconds(getattr(args, "dynamic_protection_reprice_interval_minutes", Decimal("10")))
    last_reprice = _decimal(open_trade.get("last_native_reprice_ts"))
    if interval > 0 and last_reprice > 0 and now - last_reprice < interval:
        return False
    desired_stop, desired_take_profit = _reprice_candidate_prices(position, open_trade, last_price, args)
    order_stop, order_take_profit = _native_protection_prices_from_orders(exchange, symbol, position)
    protection_state = open_trade.get("native_protection") if isinstance(open_trade.get("native_protection"), dict) else {}
    current_stop = order_stop if order_stop is not None else _decimal(protection_state.get("stop_price"))
    current_take_profit = order_take_profit if order_take_profit is not None else _decimal(protection_state.get("take_profit_price"))
    min_improvement_pct = getattr(args, "dynamic_protection_min_improvement_pct", Decimal("0.05"))
    min_improvement = position.entry_price * min_improvement_pct / Decimal("100")
    stop_improves = _more_protective_stop(position, current_stop, desired_stop) and abs(desired_stop - current_stop) >= min_improvement
    tp_improves = _closer_take_profit(position, current_take_profit, desired_take_profit, last_price) and abs(desired_take_profit - current_take_profit) >= min_improvement
    if not stop_improves and not tp_improves:
        return False

    entry_side = "buy" if position.side == "long" else "sell"
    close_side = "sell" if position.side == "long" else "buy"
    print(
        f"Native protection reprice for {symbol}: stop {current_stop}->{desired_stop} "
        f"take_profit {current_take_profit}->{desired_take_profit}"
    )
    _cancel_open_algo_orders(exchange, symbol)
    protection = _place_native_protection_prices(
        exchange,
        symbol,
        entry_side,
        close_side,
        str(position.amount),
        desired_stop,
        desired_take_profit,
        args,
        dual_side,
    )
    if not protection:
        print(f"Native protection reprice failed for {symbol}; software exits remain active.")
        return False
    open_trade["native_protection"] = protection
    open_trade["last_native_reprice_ts"] = str(now)
    open_trade["last_native_reprice_reason"] = "tighten_stop_or_take_profit"
    _record_trade_journal_event(
        state,
        "native_protection_reprice",
        {
            "symbol": symbol,
            "side": position.side,
            "amount": str(position.amount),
            "entry_price": str(position.entry_price),
            "last_price": str(last_price),
            "old_stop_price": str(current_stop),
            "new_stop_price": str(desired_stop),
            "old_take_profit_price": str(current_take_profit),
            "new_take_profit_price": str(desired_take_profit),
        },
    )
    _notify(
        args,
        "risk",
        f"Native protection repriced symbol={symbol} side={position.side} "
        f"stop_price={desired_stop} take_profit_price={desired_take_profit} reason=tighten_stop_or_take_profit",
    )
    return True


def _ensure_native_protection(
    exchange,
    symbol: str,
    position: PositionSummary,
    args: argparse.Namespace,
    dual_side: bool,
    open_trade: dict[str, Any],
) -> None:
    if not getattr(args, "native_protection", True):
        return
    if not args.execute:
        return
    order_types = _native_protection_order_types(exchange, symbol, position)
    if order_types.count("STOP_MARKET") == 1 and order_types.count("TAKE_PROFIT_MARKET") == 1:
        return

    entry_side = "buy" if position.side == "long" else "sell"
    if any(order_type in {"STOP_MARKET", "TAKE_PROFIT_MARKET"} for order_type in order_types):
        print(f"Native protection stale/incomplete for {symbol}; canceling existing algo orders before recreating.")
        _cancel_open_algo_orders(exchange, symbol)
    print(f"Native protection missing/incomplete for {symbol}; recreating close-position stop and take-profit algo orders.")
    exit_plan = _exit_plan_from_open_trade(open_trade, args)
    protection = _place_native_protection(
        exchange,
        symbol,
        entry_side,
        str(position.amount),
        position.entry_price,
        args,
        dual_side,
        exit_plan.stop_loss_pct,
        exit_plan.take_profit_pct,
    )
    if protection:
        open_trade["native_protection"] = protection


def _stop_or_take_profit_action(
    position: PositionSummary,
    last_price: Decimal,
    stop_loss_pct: Decimal,
    take_profit_pct: Decimal,
) -> str | None:
    if position.entry_price <= 0:
        return None
    stop_fraction = stop_loss_pct / Decimal("100")
    take_profit_fraction = take_profit_pct / Decimal("100")
    if position.side == "long":
        if last_price <= position.entry_price * (Decimal("1") - stop_fraction):
            return "STOP_LOSS"
        if last_price >= position.entry_price * (Decimal("1") + take_profit_fraction):
            return "TAKE_PROFIT"
    else:
        if last_price >= position.entry_price * (Decimal("1") + stop_fraction):
            return "STOP_LOSS"
        if last_price <= position.entry_price * (Decimal("1") - take_profit_fraction):
            return "TAKE_PROFIT"
    return None


def _position_key(position: PositionSummary) -> str:
    return f"{position.side}:{position.amount}:{position.entry_price}"


def _position_notional(position: PositionSummary, last_price: Decimal) -> Decimal:
    return position.amount * last_price


def _ticker_spread_pct(ticker: dict[str, Any]) -> Decimal | None:
    bid = _decimal(ticker.get("bid"))
    ask = _decimal(ticker.get("ask"))
    if bid <= 0 or ask <= 0 or ask < bid:
        return None
    mid = (bid + ask) / Decimal("2")
    if mid <= 0:
        return None
    return (ask - bid) / mid * Decimal("100")


def _spread_entry_block_reason(ticker: dict[str, Any], max_spread_pct: Decimal) -> str | None:
    if max_spread_pct <= 0:
        return None
    spread_pct = _ticker_spread_pct(ticker)
    if spread_pct is None:
        return "spread filter could not read bid/ask"
    if spread_pct > max_spread_pct:
        return f"spread {spread_pct:.4f}% > max_spread_pct {max_spread_pct}%"
    return None


def _order_book_ticker(exchange, symbol: str) -> dict[str, Any] | None:
    try:
        order_book = exchange.fetch_order_book(symbol, limit=5)
    except ccxt.BaseError as exc:
        print(f"Spread filter could not fetch order book for {symbol}: {exc}")
        return None
    bids = order_book.get("bids") or []
    asks = order_book.get("asks") or []
    if not bids or not asks:
        return None
    return {
        "bid": bids[0][0],
        "ask": asks[0][0],
        "bid_size": bids[0][1] if len(bids[0]) > 1 else None,
        "ask_size": asks[0][1] if len(asks[0]) > 1 else None,
    }


def _entry_spread_block_reason(exchange, symbol: str, ticker: dict[str, Any], max_spread_pct: Decimal) -> str | None:
    reason = _spread_entry_block_reason(ticker, max_spread_pct)
    if reason != "spread filter could not read bid/ask":
        return reason
    order_book_ticker = ticker.get("_order_book_ticker") if isinstance(ticker.get("_order_book_ticker"), dict) else None
    if order_book_ticker is None:
        order_book_ticker = _order_book_ticker(exchange, symbol)
        if order_book_ticker is not None:
            ticker["_order_book_ticker"] = order_book_ticker
    if order_book_ticker is None:
        return reason
    return _spread_entry_block_reason(order_book_ticker, max_spread_pct)


def _fee_reward_entry_block_reason(args: argparse.Namespace, take_profit_pct: Decimal | None = None) -> str | None:
    min_ratio = getattr(args, "min_reward_to_fee_ratio", Decimal("0"))
    if min_ratio <= 0:
        return None
    round_trip_fee_pct = getattr(args, "taker_fee_rate_pct", Decimal("0")) * Decimal("2")
    if round_trip_fee_pct <= 0:
        return None
    effective_take_profit_pct = take_profit_pct if take_profit_pct is not None else args.take_profit_pct
    reward_to_fee = effective_take_profit_pct / round_trip_fee_pct
    if reward_to_fee < min_ratio:
        return (
            f"take_profit_pct {effective_take_profit_pct}% has reward_to_fee {reward_to_fee:.2f} "
            f"< min_reward_to_fee_ratio {min_ratio}"
        )
    return None


def _fetch_funding_rate(exchange, symbol: str) -> Decimal | None:
    fetch_funding_rate = getattr(exchange, "fetch_funding_rate", None)
    if callable(fetch_funding_rate):
        payload = fetch_funding_rate(symbol)
        rate = _decimal(payload.get("fundingRate") or (payload.get("info") or {}).get("lastFundingRate"))
        return rate

    getter = getattr(exchange, "fapiPublicGetPremiumIndex", None)
    if callable(getter):
        payload = getter({"symbol": _market_id(exchange, symbol)})
        return _decimal(payload.get("lastFundingRate"))

    return None


def _funding_entry_block_reason(exchange, args: argparse.Namespace, symbol: str, side: str) -> str | None:
    max_abs_funding_rate_pct = getattr(args, "max_abs_funding_rate_pct", Decimal("0"))
    max_entry_funding_cost_pct = getattr(args, "max_entry_funding_cost_pct", Decimal("0"))
    if max_abs_funding_rate_pct <= 0 and max_entry_funding_cost_pct <= 0:
        return None

    try:
        funding_rate = _fetch_funding_rate(exchange, symbol)
    except ccxt.BaseError as exc:
        print(f"Funding filter could not fetch funding rate for {symbol}; continuing: {exc}")
        return None
    if funding_rate is None:
        print(f"Funding filter unavailable for {symbol}; continuing.")
        return None

    funding_rate_pct = funding_rate * Decimal("100")
    if max_abs_funding_rate_pct > 0 and abs(funding_rate_pct) > max_abs_funding_rate_pct:
        return f"abs funding rate {funding_rate_pct:.4f}% > max_abs_funding_rate_pct {max_abs_funding_rate_pct}%"

    pays_funding = (side == "buy" and funding_rate > 0) or (side == "sell" and funding_rate < 0)
    if pays_funding and max_entry_funding_cost_pct > 0 and abs(funding_rate_pct) > max_entry_funding_cost_pct:
        return (
            f"{side} entry would pay funding {funding_rate_pct:.4f}% "
            f"> max_entry_funding_cost_pct {max_entry_funding_cost_pct}%"
        )
    return None


def _entry_notional_from_risk(
    args: argparse.Namespace,
    equity: Decimal,
    stop_loss_pct: Decimal | None = None,
) -> tuple[Decimal, str]:
    fixed_notional = min(args.order_notional_usdt, args.max_position_notional_usdt, args.max_total_notional_usdt)
    if args.risk_per_trade_pct <= 0:
        return fixed_notional, "fixed"

    risk_amount = equity * args.risk_per_trade_pct / Decimal("100")
    effective_stop_loss_pct = stop_loss_pct if stop_loss_pct is not None else args.stop_loss_pct
    stop_fraction = effective_stop_loss_pct / Decimal("100")
    risk_sized_notional = risk_amount / stop_fraction
    margin_cap_notional = equity * args.max_margin_allocation_pct / Decimal("100") * Decimal(args.leverage)
    target_notional = min(
        risk_sized_notional,
        margin_cap_notional,
        args.max_position_notional_usdt,
        args.max_total_notional_usdt,
    )
    reason = (
        f"risk_based risk_amount={risk_amount:.4f} risk_sized_notional={risk_sized_notional:.4f} "
        f"margin_cap_notional={margin_cap_notional:.4f} stop_loss_pct={effective_stop_loss_pct}"
    )
    return target_notional, reason


def _available_margin_entry_block_reason(
    exchange,
    args: argparse.Namespace,
    estimated_notional: Decimal,
    account: dict[str, Any] | None | Any = _ACCOUNT_NOT_PROVIDED,
) -> str | None:
    available_balance = _fetch_usdt_available_balance(exchange, account)
    if available_balance is None:
        return None

    required_margin = estimated_notional / Decimal(args.leverage)
    buffer_pct = getattr(args, "available_balance_buffer_pct", Decimal("0"))
    required_with_buffer = required_margin * (Decimal("1") + buffer_pct / Decimal("100"))
    if required_with_buffer <= available_balance:
        return None
    return (
        f"estimated_margin {required_margin:.4f} USDT plus available_balance_buffer_pct {buffer_pct}% "
        f"> available_balance {available_balance:.4f} USDT"
    )


def _portfolio_notional_entry_block_reason(
    exchange,
    args: argparse.Namespace,
    symbol: str,
    estimated_entry_notional: Decimal,
    account: dict[str, Any] | None | Any = _ACCOUNT_NOT_PROVIDED,
) -> str | None:
    max_total_notional = getattr(args, "max_total_notional_usdt", Decimal("0"))
    if max_total_notional <= 0:
        return None
    positions = _fetch_account_positions(exchange, account)
    current_market_id = _market_id(exchange, symbol)
    existing_notional = Decimal("0")
    for market_id, position in positions.items():
        if market_id == current_market_id:
            continue
        existing_notional += position.amount * position.entry_price
    projected_total = existing_notional + estimated_entry_notional
    if projected_total <= max_total_notional:
        return None
    return (
        f"projected_total_notional {projected_total:.4f} USDT "
        f"> max_total_notional {max_total_notional} "
        f"(existing={existing_notional:.4f}, new={estimated_entry_notional:.4f})"
    )


def _update_open_trade_state(
    state: dict[str, Any],
    position: PositionSummary | None,
    last_price: Decimal,
    symbol: str | None = None,
) -> dict[str, Any]:
    if position is None:
        state.pop("open_trade", None)
        return {}

    key = _position_key(position)
    open_trade = state.get("open_trade")
    if not isinstance(open_trade, dict) or open_trade.get("key") != key:
        previous_open_trade = open_trade if isinstance(open_trade, dict) else {}
        open_trade = {
            "key": key,
            "symbol": symbol or previous_open_trade.get("symbol"),
            "side": position.side,
            "amount": str(position.amount),
            "entry_price": str(position.entry_price),
            "opened_at": previous_open_trade.get("opened_at") or datetime.now(timezone.utc).isoformat(),
            "highest_price": str(last_price),
            "lowest_price": str(last_price),
            "breakeven_armed": False,
            "trailing_armed": False,
        }
        for metadata_key in (
            "entry_decision",
            "entry_agents",
            "entry_timeframes",
            "entry_advisor_comparisons",
            "entry_shadow_comparisons",
            "exit_plan",
            "native_protection",
            "partial_tp_executed",
        ):
            if metadata_key in previous_open_trade:
                open_trade[metadata_key] = previous_open_trade[metadata_key]

    if symbol and not open_trade.get("symbol"):
        open_trade["symbol"] = symbol
    highest_price = max(_decimal(open_trade.get("highest_price")), last_price)
    lowest_price = min(_decimal(open_trade.get("lowest_price"), default=str(last_price)), last_price)
    open_trade["highest_price"] = str(highest_price)
    open_trade["lowest_price"] = str(lowest_price)
    state["open_trade"] = open_trade
    return open_trade


def _favorable_move_pct(position: PositionSummary, open_trade: dict[str, Any]) -> Decimal:
    if position.entry_price <= 0:
        return Decimal("0")
    if position.side == "long":
        highest_price = _decimal(open_trade.get("highest_price"), default=str(position.entry_price))
        return (highest_price - position.entry_price) / position.entry_price * Decimal("100")
    lowest_price = _decimal(open_trade.get("lowest_price"), default=str(position.entry_price))
    return (position.entry_price - lowest_price) / position.entry_price * Decimal("100")


def _breakeven_or_trailing_action(
    position: PositionSummary,
    last_price: Decimal,
    open_trade: dict[str, Any],
    breakeven_trigger_pct: Decimal,
    breakeven_offset_pct: Decimal,
    trailing_activation_pct: Decimal,
    trailing_distance_pct: Decimal,
) -> ExitAction | None:
    if position.entry_price <= 0:
        return None

    favorable_pct = _favorable_move_pct(position, open_trade)
    if favorable_pct >= breakeven_trigger_pct:
        open_trade["breakeven_armed"] = True
    if favorable_pct >= trailing_activation_pct:
        open_trade["trailing_armed"] = True

    if position.side == "long":
        breakeven_stop = position.entry_price * (Decimal("1") + breakeven_offset_pct / Decimal("100"))
        highest_price = _decimal(open_trade.get("highest_price"), default=str(last_price))
        trailing_stop = highest_price * (Decimal("1") - trailing_distance_pct / Decimal("100"))
        if open_trade.get("trailing_armed") and last_price <= trailing_stop:
            return ExitAction("TRAILING_STOP", trailing_stop)
        if open_trade.get("breakeven_armed") and last_price <= breakeven_stop:
            return ExitAction("BREAKEVEN_STOP", breakeven_stop)
    else:
        breakeven_stop = position.entry_price * (Decimal("1") - breakeven_offset_pct / Decimal("100"))
        lowest_price = _decimal(open_trade.get("lowest_price"), default=str(last_price))
        trailing_stop = lowest_price * (Decimal("1") + trailing_distance_pct / Decimal("100"))
        if open_trade.get("trailing_armed") and last_price >= trailing_stop:
            return ExitAction("TRAILING_STOP", trailing_stop)
        if open_trade.get("breakeven_armed") and last_price >= breakeven_stop:
            return ExitAction("BREAKEVEN_STOP", breakeven_stop)
    return None


def _record_trade_review(state: dict[str, Any], review: dict[str, str]) -> None:
    reviews = state.setdefault("trade_reviews", [])
    if not isinstance(reviews, list):
        reviews = []
        state["trade_reviews"] = reviews
    review = {"reviewed_at": datetime.now(timezone.utc).isoformat(), **review}
    reviews.append(review)
    del reviews[:-20]


def _review_closed_trade(
    state: dict[str, Any],
    args: argparse.Namespace,
    symbol: str,
    position: PositionSummary,
    exit_price: Decimal,
    exit_reason: str,
    open_trade: dict[str, Any],
    indicators: Indicators,
    timeframe_indicators: dict[str, Indicators],
) -> None:
    if not args.review_trades:
        return
    try:
        review = _gemini_trade_review(
            _model_chain(args),
            symbol,
            position,
            exit_price,
            exit_reason,
            open_trade,
            indicators,
            timeframe_indicators,
            getattr(args, "ai_request_timeout_seconds", 180.0),
        )
    except Exception as exc:
        print(f"Trade review failed: {exc}")
        return
    _record_trade_review(state, {"symbol": symbol, "exit_reason": exit_reason, **review})
    print(
        f"Trade review: quality={review['quality']} lesson={review['lesson']} "
        f"next_adjustment={review['next_adjustment']}"
    )

    # Threshold Evolution: Evolve parameters on loss or bad quality review
    if position.side == "long":
        pnl_pct = (exit_price - position.entry_price) / position.entry_price * Decimal("100")
    else:
        pnl_pct = (position.entry_price - exit_price) / position.entry_price * Decimal("100")

    # Save to SQLite lessons database
    db_review = {
        "symbol": symbol,
        "exit_reason": exit_reason,
        "side": position.side,
        "quality": review.get("quality", "mixed"),
        "lesson": review.get("lesson", ""),
        "next_adjustment": review.get("next_adjustment", ""),
        "pnl_pct": float(pnl_pct),
        "regime": open_trade.get("entry_regime", "unknown")
    }
    _save_lesson_to_db(db_review, getattr(args, "lessons_db_path", "data/binance_futures_demo_ai_trader_lessons.db"))

    if pnl_pct < 0 or review.get("quality") == "bad":
        overrides = state.setdefault("lessons_learned_overrides", {})

        min_conf = Decimal(str(overrides.get("min_confidence", args.min_confidence)))
        min_conf = min(min_conf + Decimal("0.02"), Decimal("0.85"))
        overrides["min_confidence"] = str(min_conf)

        agg = Decimal(str(overrides.get("ai_entry_aggressiveness", args.ai_entry_aggressiveness)))
        agg = max(agg - Decimal("0.05"), Decimal("0.20"))
        overrides["ai_entry_aggressiveness"] = str(agg)

        # NOTE: Stop-loss tightening after losses removed to prevent death spiral.
        # Tighter stops cause more stop-outs, leading to more losses, leading to even tighter stops.
        # Auto-tune handles stop-loss adjustments based on aggregate performance instead.
        sl = Decimal(str(overrides.get("stop_loss_pct", args.stop_loss_pct)))

        print(
            f"[Threshold Evolution] Loss in {symbol} ({pnl_pct:.2f}%). "
            f"Tightened overrides: min_confidence={min_conf}, "
            f"ai_entry_aggressiveness={agg}, stop_loss_pct={sl}"
        )


def _scan_symbol_candidates(
    exchange,
    args: argparse.Namespace,
    symbols: list[str],
    state: dict[str, Any] | None = None,
) -> list[SymbolCandidate]:
    candidates: list[SymbolCandidate] = []
    for symbol in symbols:
        try:
            candles = exchange.fetch_ohlcv(symbol, args.timeframe, limit=args.candle_limit)
            stale_reason = _stale_candle_reason(candles, args.timeframe, getattr(args, "max_data_age_seconds", Decimal("0")))
            if stale_reason:
                print(f"symbol_scan={symbol} skipped stale_data={stale_reason}")
                continue
            indicators = _calculate_indicators(candles, args.ema_fast, args.ema_slow)
            timeframe_indicators = _fetch_timeframe_indicators(exchange, symbol, args, indicators)
            regime = _detect_market_regime(
                indicators,
                timeframe_indicators,
                args.high_volatility_atr_pct,
                args.low_volatility_atr_pct,
            )
            score = _symbol_setup_score(indicators, timeframe_indicators, regime)
            penalty = _binance_symbol_score_penalty(state, args, symbol) if state is not None else Decimal("0")
            if penalty > 0:
                score = max(score - penalty, Decimal("0"))
        except ccxt.BaseError as exc:
            print(f"symbol_scan={symbol} skipped exchange_error={exc}")
            continue
        except RuntimeError as exc:
            print(f"symbol_scan={symbol} skipped data_error={exc}")
            continue
        print(
            f"symbol_scan={symbol} score={score:.3f} regime={regime.name} "
            f"style={regime.preferred_style} trend={indicators.trend} rsi={indicators.rsi:.2f} "
            f"atr_pct={indicators.atr_pct:.3f}"
            f"{f' binance_score_penalty={penalty:.3f}' if penalty > 0 else ''}"
        )
        candidates.append(
            SymbolCandidate(
                symbol=symbol,
                score=score,
                indicators=indicators,
                timeframe_indicators=timeframe_indicators,
                regime=regime,
                candles=candles,
            )
        )

    candidates.sort(key=lambda candidate: candidate.score, reverse=True)
    return candidates


def _select_symbol_for_iteration(
    exchange,
    args: argparse.Namespace,
    symbols: list[str],
) -> str:
    selected_symbol, _candidate_set = _select_symbol_and_candidates_for_iteration(exchange, args, symbols)
    return selected_symbol


def _cleanup_flat_symbol_stale_orders(
    exchange,
    args: argparse.Namespace,
    state: dict[str, Any] | None,
    symbol: str,
) -> None:
    if not getattr(args, "cancel_stale_open_orders", True) or not args.execute or state is None:
        return

    cleanup_state = state.setdefault("stale_order_cleanup", {})
    if not isinstance(cleanup_state, dict):
        cleanup_state = {}
        state["stale_order_cleanup"] = cleanup_state

    now = Decimal(str(time.time()))
    last_cleanup = _decimal(cleanup_state.get(symbol))
    if last_cleanup > 0 and now - last_cleanup < STALE_ORDER_CLEANUP_INTERVAL_SECONDS:
        return

    _cancel_open_orders(exchange, symbol, args.execute)
    cleanup_state[symbol] = str(now)


def _has_recent_ai_hold(state: dict[str, Any], symbol: str, cooldown_minutes: Decimal) -> bool:
    now = _now_ts()
    cooldown_seconds = _seconds(cooldown_minutes)
    events = state.get("entry_block_events", [])
    if isinstance(events, list):
        for event in reversed(events):
            if isinstance(event, dict) and event.get("symbol") == symbol and event.get("category") == "ai_hold":
                event_ts = _decimal(event.get("ts"))
                if now - event_ts < cooldown_seconds:
                    return True
    return False


def _select_symbol_and_candidates_for_iteration(
    exchange,
    args: argparse.Namespace,
    symbols: list[str],
    state: dict[str, Any] | None = None,
    account: dict[str, Any] | None | Any = _ACCOUNT_NOT_PROVIDED,
    manage_existing_positions: bool = True,
) -> tuple[str, list[SymbolCandidate]]:
    tradeable_symbols: list[str] = []
    if account is _ACCOUNT_NOT_PROVIDED:
        account = _fetch_futures_account(exchange)
    account_positions = _fetch_account_positions(exchange, account) if account is not None else {}
    for symbol in symbols:
        position = account_positions.get(_market_id(exchange, symbol)) if account is not None else _fetch_position(exchange, symbol)
        if position:
            if manage_existing_positions:
                print(f"Managing existing position on {symbol}: {position}")
                return symbol, []
            print(f"Skipping entry scan for {symbol}: existing position {position}")
            continue
        _cleanup_flat_symbol_stale_orders(exchange, args, state, symbol)
        block_reason = _symbol_entry_block_reason(state, symbol) if state is not None else None
        if block_reason:
            print(f"symbol_blocked={symbol} reason={block_reason}")
            continue
        
        # Filter out symbols under recent AI decision cooldown
        cooldown_mins = Decimal(str(getattr(args, "ai_decision_cooldown_minutes", "15")))
        if state is not None and _has_recent_ai_hold(state, symbol, cooldown_mins):
            print(f"Skipping selection for {symbol}: under AI decision cooldown.")
            continue
            
        tradeable_symbols.append(symbol)

    if not tradeable_symbols:
        if not manage_existing_positions:
            print("No tradeable flat symbols after position/cooldown/blacklist filtering; skipping new entry scan.")
            return "", []
        print(f"No tradeable symbols after cooldown/blacklist filtering; falling back to configured symbol {symbols[0]}.")
        return symbols[0], []

    candidates = _scan_symbol_candidates(exchange, args, tradeable_symbols, state)
    if not candidates:
        print(f"No valid scanner candidates; falling back to configured symbol {symbols[0]}.")
        return symbols[0], []

    top_candidates = candidates[: getattr(args, "ai_candidate_count", 1)]
    best = top_candidates[0]
    print(f"Selected symbol for this cycle: {best.symbol} score={best.score:.3f}")
    return best.symbol, top_candidates


def _run_symbol_iteration(
    exchange,
    args: argparse.Namespace,
    symbol: str,
    dual_side: bool,
    state: dict[str, Any],
    candidate_set: list[SymbolCandidate] | None,
    account: dict[str, Any] | None | Any,
) -> dict[str, Any]:
    _activate_symbol_open_trade(state, symbol)
    state = _run_iteration(exchange, args, symbol, dual_side, state, candidate_set, account)
    _store_active_symbol_open_trade(state, symbol)
    return state


def _run_trading_cycle(
    exchange,
    args: argparse.Namespace,
    symbols: list[str],
    dual_side: bool,
    state: dict[str, Any],
    account: dict[str, Any] | None | Any = _ACCOUNT_NOT_PROVIDED,
) -> dict[str, Any]:
    _apply_lessons_learned_thresholds(args, state)
    if account is _ACCOUNT_NOT_PROVIDED:
        account = _fetch_futures_account(exchange)

    open_symbols = _open_position_symbols(exchange, symbols, account)
    for symbol in open_symbols:
        print(f"Managing existing position before entry scan: {symbol}")
        state = _run_symbol_iteration(exchange, args, symbol, dual_side, state, [], account)

    account = _fetch_futures_account(exchange)
    open_symbols = _open_position_symbols(exchange, symbols, account)
    max_open_positions = int(getattr(args, "max_open_positions", 1))
    if len(open_symbols) >= max_open_positions:
        print(
            f"Open position cap reached: open_positions={len(open_symbols)} "
            f"max_open_positions={max_open_positions}; skipping new entry scan."
        )
        _restore_primary_open_trade(state, symbols)
        return state

    max_new_entries = int(getattr(args, "max_new_entries_per_cycle", 1))
    for _entry_index in range(max_new_entries):
        selected_symbol, candidate_set = _select_symbol_and_candidates_for_iteration(
            exchange,
            args,
            symbols,
            state,
            account,
            manage_existing_positions=False,
        )
        if not selected_symbol:
            break
        state = _run_symbol_iteration(exchange, args, selected_symbol, dual_side, state, candidate_set, account)
        account = _fetch_futures_account(exchange)
        open_symbols = _open_position_symbols(exchange, symbols, account)
        if len(open_symbols) >= max_open_positions:
            break
    _restore_primary_open_trade(state, symbols)
    return state


def _run_iteration(
    exchange,
    args: argparse.Namespace,
    symbol: str,
    dual_side: bool,
    state: dict[str, Any],
    candidate_set: list[SymbolCandidate] | None = None,
    account: dict[str, Any] | None | Any = _ACCOUNT_NOT_PROVIDED,
) -> dict[str, Any]:
    _apply_lessons_learned_thresholds(args, state)
    if account is _ACCOUNT_NOT_PROVIDED:
        account = _fetch_futures_account(exchange)
    equity = _fetch_usdt_equity(exchange, account)
    state = _update_daily_state(state, equity)
    _maybe_print_performance_report(state, args, equity)
    _maybe_print_health_report(state, args, equity)
    if _daily_loss_exceeded(state, equity, args.max_daily_loss_pct):
        print(f"Daily loss limit reached. equity={equity}; blocking new trades.")
        position = _fetch_position(exchange, symbol, account)
        if position:
            print("Daily loss limit reached with an open position; closing position.")
            _close_position(exchange, symbol, position, dual_side, args.execute)
        return state

    previous_open_trade = state.get("open_trade") if isinstance(state.get("open_trade"), dict) else None
    previous_symbol = str((previous_open_trade or {}).get("symbol") or "")

    position = _fetch_position(exchange, symbol, account)
    if position is None and previous_open_trade and previous_symbol and previous_symbol != symbol:
        previous_position = _fetch_position(exchange, previous_symbol, account)
        previous_ticker = exchange.fetch_ticker(previous_symbol)
        previous_last_price = _decimal(
            previous_ticker.get("last") or (previous_position.entry_price if previous_position else "0")
        )
        if previous_position is not None:
            symbol = previous_symbol
            position = previous_position
            ticker = previous_ticker
            last_price = previous_last_price
        else:
            _maybe_record_external_close(exchange, state, args, previous_symbol, previous_open_trade, previous_last_price)
            previous_open_trade = None
            ticker = exchange.fetch_ticker(symbol)
            last_price = _decimal(ticker.get("last") or "0")
    else:
        ticker = exchange.fetch_ticker(symbol)
        last_price = _decimal(ticker.get("last") or (position.entry_price if position else "0"))

    if position is None and getattr(args, "cancel_stale_open_orders", True):
        _cleanup_flat_symbol_stale_orders(exchange, args, state, symbol)
    if position is None:
        _maybe_record_external_close(exchange, state, args, symbol, previous_open_trade, last_price)
    open_trade = _update_open_trade_state(state, position, last_price, symbol)

    if getattr(args, "emergency_close_positions", False):
        if position:
            print("EMERGENCY_CLOSE: closing position because emergency_close_positions is enabled.")
            close_execution = _close_position(exchange, symbol, position, dual_side, args.execute)
            close_time_ms = _now_ms()
            close_price = _close_execution_price(close_execution, last_price)
            binance_report = _close_binance_report(exchange, symbol, open_trade, close_execution, close_time_ms)
            _record_trade_outcome(
                state,
                args,
                symbol,
                position,
                close_price,
                "EMERGENCY_CLOSE",
                open_trade,
                binance_report=binance_report,
            )
            _notify(args, "risk", f"Emergency close submitted for {symbol} side={position.side} amount={position.amount}")
            state.pop("open_trade", None)
            return state
        reason = "emergency_close_positions enabled; blocking new entries"
        print(f"Blocked: {reason}.")
        _record_entry_block(state, symbol, "emergency_pause", reason)
        return state

    candles = exchange.fetch_ohlcv(symbol, args.timeframe, limit=args.candle_limit)
    stale_reason = _stale_candle_reason(candles, args.timeframe, getattr(args, "max_data_age_seconds", Decimal("0")))
    if stale_reason:
        reason = f"stale market data: {stale_reason}"
        print(f"Blocked: {reason}.")
        if position:
            _ensure_native_protection(exchange, symbol, position, args, dual_side, open_trade)
            _maybe_reprice_native_protection(exchange, state, symbol, position, args, dual_side, open_trade, last_price)
            position_notional = _position_notional(position, last_price)
            if position_notional > args.max_total_notional_usdt:
                print(
                    f"TOTAL_NOTIONAL_LIMIT: closing position notional={position_notional:.4f} "
                    f"> max_total_notional={args.max_total_notional_usdt}."
                )
                close_execution = _close_position(exchange, symbol, position, dual_side, args.execute)
                close_time_ms = _now_ms()
                close_price = _close_execution_price(close_execution, last_price)
                binance_report = _close_binance_report(exchange, symbol, open_trade, close_execution, close_time_ms)
                _record_trade_outcome(
                    state,
                    args,
                    symbol,
                    position,
                    close_price,
                    "TOTAL_NOTIONAL_LIMIT",
                    open_trade,
                    binance_report=binance_report,
                )
                state.pop("open_trade", None)
                return state

            exit_plan = _exit_plan_from_open_trade(open_trade, args)
            exit_reason = _stop_or_take_profit_action(
                position,
                last_price,
                exit_plan.stop_loss_pct,
                exit_plan.take_profit_pct,
            )

            # Check for Partial Take Profit (stale path)
            if exit_reason is None and not open_trade.get("partial_tp_executed"):
                partial_ratio = Decimal("0.5")
                partial_tp_fraction = (exit_plan.take_profit_pct * partial_ratio) / Decimal("100")
                is_partial_tp_hit = False
                if position.side == "long":
                    if last_price >= position.entry_price * (Decimal("1") + partial_tp_fraction):
                        is_partial_tp_hit = True
                else:
                    if last_price <= position.entry_price * (Decimal("1") - partial_tp_fraction):
                        is_partial_tp_hit = True

                if is_partial_tp_hit:
                    close_amount = _market_amount_step(exchange, symbol, position.amount / Decimal("2"))
                    if close_amount > 0:
                        print(f"PARTIAL_TAKE_PROFIT: executing partial close of {close_amount} shares (50% of {position.amount}).")
                        close_execution = _close_position(exchange, symbol, position, dual_side, args.execute, close_amount=close_amount)
                        open_trade["partial_tp_executed"] = True
                        if "exit_plan" in open_trade and isinstance(open_trade["exit_plan"], dict):
                            open_trade["exit_plan"]["stop_loss_pct"] = "0.0"
                            print("[Threshold Evolution] Tightened stop_loss_pct of remaining position to BREAKEVEN (0.0%).")
                        _save_state(args.state_file, state)
                        _notify(args, "exit", f"Partial TP executed for {symbol} amount={close_amount} price={last_price}")
                        return state

            # Check for stale position exit (stale path)
            stale_exit_reason = _stale_position_action(position, open_trade, last_price, args)
            if stale_exit_reason:
                print(f"{stale_exit_reason}; closing position.")
                close_execution = _close_position(exchange, symbol, position, dual_side, args.execute)
                close_time_ms = _now_ms()
                close_price = _close_execution_price(close_execution, last_price)
                binance_report = _close_binance_report(exchange, symbol, open_trade, close_execution, close_time_ms)
                _record_trade_outcome(
                    state,
                    args,
                    symbol,
                    position,
                    close_price,
                    "STALE_POSITION",
                    open_trade,
                    binance_report=binance_report,
                )
                state.pop("open_trade", None)
                return state

            if exit_reason:
                print(f"{exit_reason}: closing position.")
                close_execution = _close_position(exchange, symbol, position, dual_side, args.execute)
                close_time_ms = _now_ms()
                close_price = _close_execution_price(close_execution, last_price)
                binance_report = _close_binance_report(exchange, symbol, open_trade, close_execution, close_time_ms)
                _record_trade_outcome(
                    state,
                    args,
                    symbol,
                    position,
                    close_price,
                    exit_reason,
                    open_trade,
                    binance_report=binance_report,
                )
                state.pop("open_trade", None)
                return state

            trailing_exit = _breakeven_or_trailing_action(
                position,
                last_price,
                open_trade,
                args.breakeven_trigger_pct,
                args.breakeven_offset_pct,
                args.trailing_activation_pct,
                args.trailing_distance_pct,
            )
            if trailing_exit:
                print(f"{trailing_exit.reason}: closing position at price={last_price} stop_price={trailing_exit.stop_price}.")
                close_execution = _close_position(exchange, symbol, position, dual_side, args.execute)
                close_time_ms = _now_ms()
                close_price = _close_execution_price(close_execution, last_price)
                binance_report = _close_binance_report(exchange, symbol, open_trade, close_execution, close_time_ms)
                _record_trade_outcome(
                    state,
                    args,
                    symbol,
                    position,
                    close_price,
                    trailing_exit.reason,
                    open_trade,
                    binance_report=binance_report,
                )
                state.pop("open_trade", None)
                return state

            print("Skipping AI position review because candle data is stale; deterministic protection remains active.")
        else:
            _record_entry_block(state, symbol, "stale_data", reason)
        return state

    indicators = _calculate_indicators(candles, args.ema_fast, args.ema_slow)
    if last_price <= 0:
        last_price = indicators.close
    timeframe_indicators = _fetch_timeframe_indicators(exchange, symbol, args, indicators)
    regime = _detect_market_regime(
        indicators,
        timeframe_indicators,
        args.high_volatility_atr_pct,
        args.low_volatility_atr_pct,
    )
    expert_signals = _expert_signals(indicators, candles, position, timeframe_indicators, regime)

    print(
        f"{datetime.now(timezone.utc).isoformat()} symbol={symbol} price={last_price} "
        f"trend={indicators.trend} rsi={indicators.rsi:.2f} atr_pct={indicators.atr_pct:.3f} "
        f"position={position}"
    )
    print(
        f"market_regime={regime.name} style={regime.preferred_style} "
        f"confidence={regime.confidence} reason={regime.reason}"
    )
    for timeframe, timeframe_indicator in timeframe_indicators.items():
        print(
            f"timeframe={timeframe} trend={timeframe_indicator.trend} "
            f"rsi={timeframe_indicator.rsi:.2f} atr_pct={timeframe_indicator.atr_pct:.3f}"
        )
    for signal in expert_signals:
        print(
            f"expert={signal.name} action={signal.action} confidence={signal.confidence} "
            f"reason={signal.reason}"
        )
    ai_context = _ai_context_payload(
        exchange,
        args,
        state,
        symbol,
        ticker,
        equity,
        account if isinstance(account, dict) else None,
        indicators,
        candles,
        timeframe_indicators,
        regime,
        candidate_set,
    )

    if position:
        _ensure_native_protection(exchange, symbol, position, args, dual_side, open_trade)
        _maybe_reprice_native_protection(exchange, state, symbol, position, args, dual_side, open_trade, last_price)

        position_notional = _position_notional(position, last_price)
        if position_notional > args.max_total_notional_usdt:
            print(
                f"TOTAL_NOTIONAL_LIMIT: closing position notional={position_notional:.4f} "
                f"> max_total_notional={args.max_total_notional_usdt}."
            )
            close_execution = _close_position(exchange, symbol, position, dual_side, args.execute)
            close_time_ms = _now_ms()
            close_price = _close_execution_price(close_execution, last_price)
            binance_report = _close_binance_report(exchange, symbol, open_trade, close_execution, close_time_ms)
            _record_trade_outcome(
                state,
                args,
                symbol,
                position,
                close_price,
                "TOTAL_NOTIONAL_LIMIT",
                open_trade,
                binance_report=binance_report,
            )
            _review_closed_trade(
                state,
                args,
                symbol,
                position,
                close_price,
                "TOTAL_NOTIONAL_LIMIT",
                open_trade,
                indicators,
                timeframe_indicators,
            )
            state.pop("open_trade", None)
            return state

        exit_plan = _exit_plan_from_open_trade(open_trade, args)
        exit_reason = _stop_or_take_profit_action(
            position,
            last_price,
            exit_plan.stop_loss_pct,
            exit_plan.take_profit_pct,
        )

        # Check for Partial Take Profit (fresh path)
        if exit_reason is None and not open_trade.get("partial_tp_executed"):
            partial_ratio = Decimal("0.5")
            partial_tp_fraction = (exit_plan.take_profit_pct * partial_ratio) / Decimal("100")
            is_partial_tp_hit = False
            if position.side == "long":
                if last_price >= position.entry_price * (Decimal("1") + partial_tp_fraction):
                    is_partial_tp_hit = True
            else:
                if last_price <= position.entry_price * (Decimal("1") - partial_tp_fraction):
                    is_partial_tp_hit = True

            if is_partial_tp_hit:
                close_amount = _market_amount_step(exchange, symbol, position.amount / Decimal("2"))
                if close_amount > 0:
                    print(f"PARTIAL_TAKE_PROFIT: executing partial close of {close_amount} shares (50% of {position.amount}).")
                    close_execution = _close_position(exchange, symbol, position, dual_side, args.execute, close_amount=close_amount)
                    open_trade["partial_tp_executed"] = True
                    if "exit_plan" in open_trade and isinstance(open_trade["exit_plan"], dict):
                        open_trade["exit_plan"]["stop_loss_pct"] = "0.0"
                        print("[Threshold Evolution] Tightened stop_loss_pct of remaining position to BREAKEVEN (0.0%).")
                    _save_state(args.state_file, state)
                    _notify(args, "exit", f"Partial TP executed for {symbol} amount={close_amount} price={last_price}")
                    return state

        # Check for stale position exit (fresh path)
        stale_exit_reason = _stale_position_action(position, open_trade, last_price, args)
        if stale_exit_reason:
            print(f"{stale_exit_reason}: closing position.")
            close_execution = _close_position(exchange, symbol, position, dual_side, args.execute)
            close_time_ms = _now_ms()
            close_price = _close_execution_price(close_execution, last_price)
            binance_report = _close_binance_report(exchange, symbol, open_trade, close_execution, close_time_ms)
            _record_trade_outcome(
                state,
                args,
                symbol,
                position,
                close_price,
                "STALE_POSITION",
                open_trade,
                binance_report=binance_report,
            )
            _review_closed_trade(
                state,
                args,
                symbol,
                position,
                close_price,
                "STALE_POSITION",
                open_trade,
                indicators,
                timeframe_indicators,
            )
            state.pop("open_trade", None)
            return state

        if exit_reason:
            print(f"{exit_reason}: closing position.")
            close_execution = _close_position(exchange, symbol, position, dual_side, args.execute)
            close_time_ms = _now_ms()
            close_price = _close_execution_price(close_execution, last_price)
            binance_report = _close_binance_report(exchange, symbol, open_trade, close_execution, close_time_ms)
            _record_trade_outcome(
                state,
                args,
                symbol,
                position,
                close_price,
                exit_reason,
                open_trade,
                binance_report=binance_report,
            )
            _review_closed_trade(
                state,
                args,
                symbol,
                position,
                close_price,
                exit_reason,
                open_trade,
                indicators,
                timeframe_indicators,
            )
            state.pop("open_trade", None)
            return state

        trailing_exit = _breakeven_or_trailing_action(
            position,
            last_price,
            open_trade,
            args.breakeven_trigger_pct,
            args.breakeven_offset_pct,
            args.trailing_activation_pct,
            args.trailing_distance_pct,
        )
        if trailing_exit:
            print(f"{trailing_exit.reason}: closing position at price={last_price} stop_price={trailing_exit.stop_price}.")
            close_execution = _close_position(exchange, symbol, position, dual_side, args.execute)
            close_time_ms = _now_ms()
            close_price = _close_execution_price(close_execution, last_price)
            binance_report = _close_binance_report(exchange, symbol, open_trade, close_execution, close_time_ms)
            _record_trade_outcome(
                state,
                args,
                symbol,
                position,
                close_price,
                trailing_exit.reason,
                open_trade,
                binance_report=binance_report,
            )
            _review_closed_trade(
                state,
                args,
                symbol,
                position,
                close_price,
                trailing_exit.reason,
                open_trade,
                indicators,
                timeframe_indicators,
            )
            state.pop("open_trade", None)
            return state

    if position is None and not getattr(args, "new_entries_enabled", True):
        reason = "new entries disabled by runtime config"
        print(f"Blocked: {reason}.")
        _record_entry_block(state, symbol, "manual_pause", reason)
        return state

    if position is None:
        account_position_block = _account_position_block_reason(exchange, args, symbol, position, account)
        if account_position_block:
            print(f"Blocked: {account_position_block}.")
            _record_entry_block(state, symbol, "account_position", account_position_block)
            return state

        # Trend Coherence Guard
        if getattr(args, "trend_coherence_guard", False) and regime.name == "trending":
            coherence_tf = getattr(args, "trend_coherence_timeframe", "1h")
            if coherence_tf in timeframe_indicators:
                tf_ind = timeframe_indicators[coherence_tf]
                if indicators.trend != "flat" and tf_ind.trend != "flat" and indicators.trend != tf_ind.trend:
                    reason = f"trend coherence mismatch in trending market: primary({args.timeframe})={indicators.trend} vs {coherence_tf}={tf_ind.trend}"
                    print(f"Blocked: {reason}.")
                    _record_entry_block(state, symbol, "trend_coherence", reason)
                    return state

    recent_reviews = state.get("trade_reviews", []) if isinstance(state.get("trade_reviews"), list) else []
    db_path = getattr(args, "lessons_db_path", "data/binance_futures_demo_ai_trader_lessons.db")
    sqlite_lessons = _query_relevant_lessons(symbol, regime.name, limit=5, db_path=db_path)
    combined_reviews = list(recent_reviews)
    for lesson in sqlite_lessons:
        if not any(r.get("reviewed_at") == lesson.get("reviewed_at") for r in combined_reviews):
            combined_reviews.append(lesson)
    recent_reviews = combined_reviews

    # Decision Log Cooldown Check
    if position is None:
        now = _now_ts()
        cooldown_seconds = _seconds(getattr(args, "ai_decision_cooldown_minutes", Decimal("15")))
        recent_hold = None
        events = state.get("entry_block_events", [])
        if isinstance(events, list):
            for event in reversed(events):
                if isinstance(event, dict) and event.get("symbol") == symbol and event.get("category") == "ai_hold":
                    event_ts = _decimal(event.get("ts"))
                    if now - event_ts < cooldown_seconds:
                        recent_hold = event
                        break
        if recent_hold:
            print(
                f"[Decision Log Skip] Skipping AI decision for {symbol} due to recent HOLD decision "
                f"at {datetime.fromtimestamp(float(recent_hold.get('ts')), timezone.utc).isoformat()} "
                f"(reason: {recent_hold.get('reason')})."
            )
            return state

    # Screener-Reasoner (Gatekeeper) Hierarchy:
    # Step 1: Run fast screener (Gemini)
    screener_models = _model_chain(args)
    advisor_models = _advisor_model_chain(args)
    
    if advisor_models:
        print(f"[Gatekeeper] Running fast screener using {','.join(screener_models)}...")
        try:
            screener_multi = _gemini_decision(
                screener_models,
                indicators,
                position,
                symbol,
                expert_signals,
                timeframe_indicators,
                regime,
                recent_reviews,
                candidate_set,
                getattr(args, "ai_max_retries", 2),
                getattr(args, "ai_retry_delay_seconds", 1.0),
                getattr(args, "ai_request_timeout_seconds", 180.0),
                getattr(args, "ai_entry_aggressiveness", Decimal("0.50")),
                ai_context,
                adversarial_debate=False,  # Keep screener fast and cheap
                is_screener=True,
            )
            screener_eff = _effective_multi_agent_decision(screener_multi)
            screener_action = screener_eff.decision.action
        except Exception as exc:
            print(f"[Gatekeeper] Screener failed: {exc}. Falling back to deep reasoner directly.")
            screener_action = "DEBATE"
            screener_multi = None

        if screener_action == "HOLD":
            print(f"[Gatekeeper] Screener returned HOLD. Skipping deep reasoning to save tokens.")
            multi_agent_decision = screener_multi
        else:
            print(f"[Gatekeeper] Screener returned {screener_action}. Invoking deep reasoner {','.join(advisor_models)}...")
            if screener_multi:
                screener_eff = _effective_multi_agent_decision(screener_multi)
                ai_context["screener_proposal"] = {
                    "action": screener_eff.decision.action,
                    "confidence": str(screener_eff.decision.confidence),
                    "reason": screener_eff.decision.reason,
                }
            
            try:
                multi_agent_decision = _gemini_decision(
                    advisor_models,  # Advisor models (Qwen) acts as final reasoner/judge
                    indicators,
                    position,
                    symbol,
                    expert_signals,
                    timeframe_indicators,
                    regime,
                    recent_reviews,
                    candidate_set,
                    getattr(args, "ai_max_retries", 2),
                    getattr(args, "ai_retry_delay_seconds", 1.0),
                    getattr(args, "ai_request_timeout_seconds", 180.0),
                    getattr(args, "ai_entry_aggressiveness", Decimal("0.50")),
                    ai_context,
                    adversarial_debate=getattr(args, "adversarial_debate", False),
                )
            except Exception as exc:
                if args.allow_fallback:
                    print(f"Deep reasoning failed, using deterministic fallback: {exc}")
                    multi_agent_decision = _multi_agent_fallback(indicators)
                else:
                    print(f"Deep reasoning failed after retries; failing closed with HOLD: {exc}")
                    multi_agent_decision = _hold_decision("Deep reasoning unavailable; fail-closed HOLD.")
    else:
        # No advisor models configured, run standard execution flow using primary model
        try:
            multi_agent_decision = _gemini_decision(
                screener_models,
                indicators,
                position,
                symbol,
                expert_signals,
                timeframe_indicators,
                regime,
                recent_reviews,
                candidate_set,
                getattr(args, "ai_max_retries", 2),
                getattr(args, "ai_retry_delay_seconds", 1.0),
                getattr(args, "ai_request_timeout_seconds", 180.0),
                getattr(args, "ai_entry_aggressiveness", Decimal("0.50")),
                ai_context,
                adversarial_debate=getattr(args, "adversarial_debate", False),
            )
        except Exception as exc:
            if args.allow_fallback:
                print(f"AI decision failed, using deterministic fallback: {exc}")
                multi_agent_decision = _multi_agent_fallback(indicators)
            else:
                print(f"AI decision failed after retries; failing closed with HOLD: {exc}")
                multi_agent_decision = _hold_decision("AI unavailable; fail-closed HOLD.")

    for agent in multi_agent_decision.agents:
        print(
            f"ai_agent={agent.name} action={agent.action} confidence={agent.confidence} "
            f"reason={agent.reason}"
        )
    effective_decision = _effective_multi_agent_decision(multi_agent_decision)
    decision = effective_decision.decision
    print(f"AI decision: action={decision.action} confidence={decision.confidence} reason={decision.reason}")
    shadow_comparisons = _shadow_decision_comparisons(
        args,
        state,
        symbol,
        decision,
        indicators,
        position,
        expert_signals,
        timeframe_indicators,
        regime,
        state.get("trade_reviews", []) if isinstance(state.get("trade_reviews"), list) else [],
        candidate_set,
        ai_context,
    )
    advisor_comparisons = []

    if decision.action == "CLOSE" and position:
        min_close_confidence = Decimal("0.65")
        if decision.confidence < min_close_confidence:
            print(
                f"AI CLOSE blocked: confidence {decision.confidence} < min_close_confidence {min_close_confidence}. "
                f"Treating as HOLD."
            )
            _record_entry_block(state, symbol, "ai_close_low_confidence", f"confidence {decision.confidence} < {min_close_confidence}")
            return state
        close_execution = _close_position(exchange, symbol, position, dual_side, args.execute)
        close_time_ms = _now_ms()
        close_price = _close_execution_price(close_execution, last_price)
        binance_report = _close_binance_report(exchange, symbol, open_trade, close_execution, close_time_ms)
        _record_trade_outcome(
            state,
            args,
            symbol,
            position,
            close_price,
            "AI_CLOSE",
            open_trade,
            binance_report=binance_report,
        )
        _review_closed_trade(
            state,
            args,
            symbol,
            position,
            close_price,
            "AI_CLOSE",
            open_trade,
            indicators,
            timeframe_indicators,
        )
        state.pop("open_trade", None)
        return state

    if decision.action not in {"BUY", "SELL"}:
        print("No entry action.")
        if position is None:
            _record_entry_block(state, symbol, "ai_hold", decision.reason)
        return state

    global_block = _global_entry_block_reason(state)
    if global_block:
        print(f"Blocked: {global_block}.")
        _record_entry_block(state, symbol, "global_control", global_block)
        return state

    symbol_block = _symbol_entry_block_reason(state, symbol)
    if symbol_block:
        print(f"Blocked: {symbol} {symbol_block}.")
        _record_entry_block(state, symbol, "symbol_control", symbol_block)
        return state

    if decision.confidence < args.min_confidence:
        reason = f"confidence {decision.confidence} < min_confidence {args.min_confidence}"
        print(f"Blocked: {reason}.")
        _record_entry_block(state, symbol, "confidence", reason)
        return state

    if decision.action == "SELL" and not args.allow_short:
        print("Blocked: short entries are disabled. Add --allow-short to permit SELL entries.")
        _record_entry_block(state, symbol, "short_disabled", "short entries are disabled")
        return state

    if position:
        print("Blocked: existing position is open. Close it before opening a new one.")
        _record_entry_block(state, symbol, "existing_position", "existing position is open")
        return state

    side = "buy" if decision.action == "BUY" else "sell"
    prompt_last_price = last_price
    prompt_ticker = ticker
    try:
        ticker = exchange.fetch_ticker(symbol)
    except ccxt.BaseError as exc:
        reason = f"could not refresh ticker after AI decision: {exc}"
        print(f"Blocked: {reason}.")
        _record_entry_block(state, symbol, "quote_refresh", reason)
        return state
    if (
        isinstance(prompt_ticker.get("_order_book_ticker"), dict)
        and _decimal(ticker.get("bid")) <= 0
        and _decimal(ticker.get("ask")) <= 0
    ):
        ticker["_order_book_ticker"] = prompt_ticker["_order_book_ticker"]
    last_price = _ticker_last_price(ticker, prompt_last_price)
    drift_block = _entry_quote_drift_block_reason(
        prompt_last_price,
        last_price,
        getattr(args, "ai_entry_max_price_drift_pct", Decimal("0.20")),
    )
    if drift_block:
        print(f"Blocked: {drift_block}.")
        _record_entry_block(state, symbol, "quote_drift", drift_block)
        return state
    ai_context["post_ai_quote_refresh"] = {
        "prompt_last_price": str(prompt_last_price),
        "fresh_last_price": str(last_price),
        "max_price_drift_pct": str(getattr(args, "ai_entry_max_price_drift_pct", Decimal("0.20"))),
    }
    ai_context["execution_context"] = _execution_context_payload(exchange, symbol, ticker, args)

    execution_mode, limit_price_override, entry_plan_state, entry_plan_block = _entry_execution_plan_from_ai(
        args,
        exchange,
        side,
        symbol,
        ticker,
        decision.entry_plan,
    )
    if entry_plan_block:
        print(f"Blocked: {entry_plan_block}.")
        _record_entry_block(state, symbol, "entry_plan", entry_plan_block)
        return state

    exit_plan = _dynamic_exit_plan(
        args,
        side,
        decision.confidence,
        indicators,
        timeframe_indicators,
        regime,
        decision.entry_plan,
    )
    print(
        f"exit_plan mode={exit_plan.mode} stop_loss_pct={exit_plan.stop_loss_pct:.4f} "
        f"take_profit_pct={exit_plan.take_profit_pct:.4f} reward_risk={exit_plan.reward_risk:.2f} "
        f"reasons={exit_plan.reasons}"
    )
    for category, block_reason in (
        ("spread", _entry_spread_block_reason(exchange, symbol, ticker, args.max_spread_pct)),
        ("fee_reward", _fee_reward_entry_block_reason(args, exit_plan.take_profit_pct)),
        ("funding", _funding_entry_block_reason(exchange, args, symbol, side)),
    ):
        if block_reason:
            print(f"Blocked: {block_reason}.")
            _record_entry_block(state, symbol, category, block_reason)
            return state

    target_notional, sizing_reason = _entry_notional_from_risk(args, equity, exit_plan.stop_loss_pct)
    if getattr(args, "adaptive_risk", False):
        adaptive_multiplier = _decimal(state.get("adaptive_risk_multiplier"), default="1")
        adaptive_multiplier = min(max(adaptive_multiplier, args.adaptive_risk_min_multiplier), args.adaptive_risk_max_multiplier)
        target_notional *= adaptive_multiplier
        sizing_reason = f"{sizing_reason} adaptive_risk_multiplier={adaptive_multiplier}"
    rounding_buffer = min(getattr(args, "sizing_cap_buffer_pct", Decimal("1")), Decimal("20")) / Decimal("100")
    cap_with_buffer = (
        min(args.max_position_notional_usdt, args.max_total_notional_usdt)
        * (Decimal("1") - rounding_buffer)
    )
    target_notional = min(target_notional, cap_with_buffer)
    market = exchange.markets[symbol]
    min_notional = _market_min_notional(market)
    if min_notional is not None:
        target_notional = max(target_notional, min_notional)

    max_entry_notional = min(args.max_position_notional_usdt, args.max_total_notional_usdt)
    amount, estimated_notional = _amount_from_notional_with_cap(
        exchange,
        symbol,
        target_notional,
        last_price,
        max_entry_notional,
    )
    if estimated_notional > args.max_position_notional_usdt:
        reason = (
            f"Blocked: estimated_notional {estimated_notional:.4f} > "
            f"max_position_notional {args.max_position_notional_usdt}"
        )
        print(f"{reason}.")
        _record_entry_block(state, symbol, "notional_cap", reason)
        return state
    if estimated_notional > args.max_total_notional_usdt:
        reason = (
            f"Blocked: estimated_notional {estimated_notional:.4f} > "
            f"max_total_notional {args.max_total_notional_usdt}"
        )
        print(f"{reason}.")
        _record_entry_block(state, symbol, "notional_cap", reason)
        return state

    portfolio_notional_block = _portfolio_notional_entry_block_reason(
        exchange,
        args,
        symbol,
        estimated_notional,
        account,
    )
    if portfolio_notional_block:
        print(f"Blocked: {portfolio_notional_block}.")
        _record_entry_block(state, symbol, "portfolio_notional", portfolio_notional_block)
        return state

    available_margin_block = _available_margin_entry_block_reason(exchange, args, estimated_notional, account)
    if available_margin_block:
        print(f"Blocked: {available_margin_block}.")
        _record_entry_block(state, symbol, "available_margin", available_margin_block)
        return state

    params: dict[str, Any] = {}
    if dual_side:
        if exchange.id == "okx":
            params["posSide"] = "long" if side == "buy" else "short"
        else:
            params["positionSide"] = _position_side_for_open(side)

    print(
        f"Entry order: side={side} amount={amount} estimated_notional_usdt={estimated_notional:.4f} "
        f"estimated_margin_usdt={(estimated_notional / Decimal(args.leverage)):.4f} "
        f"sizing={sizing_reason} execute={args.execute}"
    )
    if args.execute:
        try:
            order, execution_detail = _submit_entry_order(
                exchange,
                symbol,
                side,
                amount,
                ticker,
                last_price,
                params,
                args,
                execution_mode_override=execution_mode,
                limit_price_override=limit_price_override,
            )
        except RuntimeError as exc:
            print(f"Entry execution blocked: {exc}")
            _record_entry_block(state, symbol, "execution", str(exc))
            return state
        print(f"Open order: id={order.get('id')} status={order.get('status')} execution={execution_detail}")
        entry_price = _order_average_price(order, last_price)
        protection = _place_native_protection(
            exchange,
            symbol,
            side,
            amount,
            entry_price,
            args,
            dual_side,
            exit_plan.stop_loss_pct,
            exit_plan.take_profit_pct,
        )
        entry_record = {
            "key": f"pending:{side}:{amount}:{entry_price}",
            "symbol": symbol,
            "side": "long" if side == "buy" else "short",
            "amount": str(amount),
            "entry_price": str(entry_price),
            "opened_at": datetime.now(timezone.utc).isoformat(),
            "highest_price": str(entry_price),
            "lowest_price": str(entry_price),
            "breakeven_armed": False,
            "trailing_armed": False,
            "exit_plan": _exit_plan_to_state(exit_plan),
            "native_protection": protection,
            "entry_regime": regime.name,
            "entry_decision": {
                "action": decision.action,
                "confidence": str(decision.confidence),
                "reason": decision.reason,
                "entry_plan": decision.entry_plan,
            },
            "entry_execution_plan": entry_plan_state,
            "entry_agents": [
                {
                    "name": agent.name,
                    "action": agent.action,
                    "confidence": str(agent.confidence),
                    "reason": agent.reason,
                }
                for agent in multi_agent_decision.agents
            ],
            "entry_timeframes": _timeframe_payload(timeframe_indicators),
            "entry_context": ai_context,
        }
        if advisor_comparisons:
            entry_record["entry_advisor_comparisons"] = advisor_comparisons
        if shadow_comparisons:
            entry_record["entry_shadow_comparisons"] = shadow_comparisons
        if (
            getattr(args, "native_protection", True)
            and getattr(args, "close_on_native_protection_failure", True)
            and not protection
        ):
            print("NATIVE_PROTECTION_FAILED: closing just-opened position because native TP/SL protection was not placed.")
            entry_position = PositionSummary(entry_record["side"], _decimal(amount), entry_price, Decimal("0"))
            close_execution = _close_position(exchange, symbol, entry_position, dual_side, args.execute)
            close_time_ms = _now_ms()
            close_price = _close_execution_price(close_execution, entry_price)
            binance_report = _close_binance_report(exchange, symbol, entry_record, close_execution, close_time_ms)
            _record_trade_outcome(
                state,
                args,
                symbol,
                entry_position,
                close_price,
                "NATIVE_PROTECTION_FAILED",
                entry_record,
                binance_report=binance_report,
            )
            _notify(args, "risk", f"{symbol} closed after native protection placement failed.")
            state.pop("open_trade", None)
            return state
        entry_performance = _performance_context_payload(state, symbol)
        execution_context = ai_context.get("execution_context", {}) if isinstance(ai_context, dict) else {}
        risk_context = ai_context.get("risk_context", {}) if isinstance(ai_context, dict) else {}
        _record_trade_journal_event(
            state,
            "entry",
            {
                "symbol": symbol,
                "side": "long" if side == "buy" else "short",
                "amount": str(amount),
                "entry_price": str(entry_price),
                "estimated_notional_usdt": str(estimated_notional),
                "execution_detail": execution_detail,
                "entry_decision": entry_record["entry_decision"],
                "entry_agents": entry_record["entry_agents"],
                "entry_timeframes": entry_record["entry_timeframes"],
                "exit_plan": entry_record["exit_plan"],
                "native_protection": protection,
                "entry_execution_plan": entry_record["entry_execution_plan"],
                "entry_context": ai_context,
                "entry_advisor_comparisons": advisor_comparisons,
                "entry_shadow_comparisons": shadow_comparisons,
            },
        )
        _notify(
            args,
            "entry",
            f"{symbol} {side} amount={amount} entry={entry_price} "
            f"estimated_notional={estimated_notional:.4f} USDT "
            f"entry_style={entry_plan_state.get('style', 'default')} execution_mode={execution_mode} "
            f"stop_loss_pct={exit_plan.stop_loss_pct:.4f} take_profit_pct={exit_plan.take_profit_pct:.4f} "
            f"reward_risk={exit_plan.reward_risk:.2f} "
            f"stop_price={(protection or {}).get('stop_price', 'software')} "
            f"take_profit_price={(protection or {}).get('take_profit_price', 'software')} "
            f"spread_pct={execution_context.get('spread_pct', 'unknown')} "
            f"funding_rate_pct={execution_context.get('funding_rate_pct', 'unknown')} "
            f"adaptive_risk_multiplier={risk_context.get('adaptive_risk_multiplier', '1')} "
            f"symbol_recent_pnl={entry_performance['symbol_recent']['estimated_pnl_usdt_before_fees']} "
            f"binance_net_income={entry_performance['binance_performance_summary']['net_income_usdt']}",
        )
        state["last_entry_ts"] = str(_now_ts())
        state["open_trade"] = entry_record
    return state


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a Binance USD-M futures demo AI trader.")
    parser.add_argument("--exchange-id", default="binanceusdm")
    parser.add_argument("--environment", choices=["demo", "live"], default="demo")
    parser.add_argument("--symbol", default=DEFAULT_SYMBOL)
    parser.add_argument("--symbols", default="", help="Comma-separated symbols to scan. Overrides --symbol when set.")
    parser.add_argument("--exclude-symbols", default="", help="Comma-separated symbols to exclude from new entries.")
    parser.add_argument(
        "--symbol-universe",
        choices=["manual", "top-usdm-large-cap"],
        default="manual",
        help="Use a dynamic scanner universe when --symbols is not set.",
    )
    parser.add_argument("--universe-size", type=int, default=10, help="Number of symbols to keep from a scanner universe.")
    parser.add_argument(
        "--ai-candidate-count",
        type=int,
        default=3,
        help="Number of top scanner candidates to include in the AI committee prompt.",
    )
    parser.add_argument(
        "--ai-entry-aggressiveness",
        type=_nonnegative_decimal,
        default=Decimal("0.50"),
        help="0.0-1.0 bias for AI to prefer valid flat-account entries over HOLD when scanner/risk context is strong.",
    )
    parser.add_argument("--timeframe", default="5m")
    parser.add_argument("--analysis-timeframes", default="1m,5m,15m,1h")
    parser.add_argument(
        "--trend-coherence-guard",
        action="store_true",
        help="Block entries when primary trend mismatches the longer coherence timeframe trend."
    )
    parser.add_argument("--trend-coherence-timeframe", default="1h", help="Timeframe for trend coherence checks (default 1h).")
    parser.add_argument(
        "--adversarial-debate",
        action="store_true",
        help="Enable adversarial debate mode where separate Bull and Bear analyst models run before the final model."
    )
    parser.add_argument("--candle-limit", type=int, default=200)
    parser.add_argument("--mtf-candle-limit", type=int, default=200)
    parser.add_argument(
        "--max-data-age-seconds",
        type=_nonnegative_decimal,
        default=Decimal("900"),
        help="Block candle-based AI decisions when the latest primary candle is older than this. 0 disables.",
    )
    parser.add_argument("--ema-fast", type=int, default=20)
    parser.add_argument("--ema-slow", type=int, default=50)
    parser.add_argument(
        "--model",
        default=DEFAULT_MODEL,
        help="Single AI model to use when --models is not set. Plain names use Gemini; prefix openai: for OpenAI-compatible APIs.",
    )
    parser.add_argument(
        "--models",
        default="",
        help=(
            "Comma-separated AI fallback chain. First item is primary. Plain names use Gemini; "
            "use gemini:<model> or openai:<model> to choose a provider explicitly."
        ),
    )
    parser.add_argument(
        "--advisor-models",
        default="",
        help=(
            "Comma-separated advisor comparison models. These models run before the final model and "
            "their opinions are added to final-model context, but they never directly drive orders."
        ),
    )
    parser.add_argument(
        "--shadow-models",
        default="",
        help=(
            "Comma-separated shadow comparison models. These models receive the same decision prompt "
            "after the final model, but their output is logged only and never drives orders."
        ),
    )
    parser.add_argument("--order-notional-usdt", type=_positive_decimal, default=Decimal("60"))
    parser.add_argument("--max-position-notional-usdt", type=_positive_decimal, default=Decimal("100"))
    parser.add_argument("--max-total-notional-usdt", type=_positive_decimal, default=Decimal("150"))
    parser.add_argument(
        "--max-open-positions",
        type=int,
        default=1,
        help="Maximum number of simultaneous symbols with open positions. Pyramiding on the same symbol is still blocked.",
    )
    parser.add_argument(
        "--max-new-entries-per-cycle",
        type=int,
        default=1,
        help="Maximum new entries to submit after existing positions are managed in one cycle.",
    )
    parser.add_argument(
        "--risk-per-trade-pct",
        type=_nonnegative_decimal,
        default=Decimal("0"),
        help="If >0, size entries from account equity and stop distance instead of fixed --order-notional-usdt.",
    )
    parser.add_argument(
        "--max-margin-allocation-pct",
        type=_positive_decimal,
        default=Decimal("5"),
        help="Max account equity percentage that risk-based sizing may allocate as initial margin.",
    )
    parser.add_argument(
        "--available-balance-buffer-pct",
        type=_nonnegative_decimal,
        default=Decimal("5"),
        help="Extra available-balance buffer required before submitting a futures entry. 0 disables the buffer.",
    )
    parser.add_argument(
        "--sizing-cap-buffer-pct",
        type=_nonnegative_decimal,
        default=Decimal("1"),
        help="Reduce target notional below max caps before quantity rounding to avoid post-rounding cap blocks.",
    )
    parser.add_argument("--leverage", type=int, default=DEFAULT_LEVERAGE)
    parser.add_argument("--margin-mode", choices=["isolated", "cross", "none"], default="isolated")
    parser.add_argument("--min-confidence", type=_positive_decimal, default=Decimal("0.60"))
    parser.add_argument("--max-daily-loss-pct", type=_positive_decimal, default=Decimal("2.0"))
    parser.add_argument("--stop-loss-pct", type=_positive_decimal, default=Decimal("0.8"))
    parser.add_argument("--take-profit-pct", type=_positive_decimal, default=Decimal("1.2"))
    parser.add_argument(
        "--dynamic-exit-plan",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Derive per-entry TP/SL from ATR, trend alignment, RSI, and confidence within configured bounds.",
    )
    parser.add_argument("--dynamic-exit-atr-stop-multiplier", type=_positive_decimal, default=Decimal("1.80"))
    parser.add_argument("--dynamic-exit-min-stop-pct", type=_positive_decimal, default=Decimal("0.30"))
    parser.add_argument("--dynamic-exit-max-stop-pct", type=_positive_decimal, default=Decimal("0.90"))
    parser.add_argument("--dynamic-exit-min-take-profit-pct", type=_positive_decimal, default=Decimal("0.70"))
    parser.add_argument("--dynamic-exit-max-take-profit-pct", type=_positive_decimal, default=Decimal("2.50"))
    parser.add_argument("--dynamic-exit-min-reward-risk", type=_positive_decimal, default=Decimal("1.80"))
    parser.add_argument("--dynamic-exit-max-reward-risk", type=_positive_decimal, default=Decimal("3.00"))
    parser.add_argument("--high-volatility-atr-pct", type=_positive_decimal, default=Decimal("1.5"))
    parser.add_argument("--low-volatility-atr-pct", type=_positive_decimal, default=Decimal("0.15"))
    parser.add_argument(
        "--max-spread-pct",
        type=_nonnegative_decimal,
        default=Decimal("0"),
        help="Block new entries when ticker bid/ask spread exceeds this percentage. 0 disables the filter.",
    )
    parser.add_argument(
        "--taker-fee-rate-pct",
        type=_nonnegative_decimal,
        default=Decimal("0.05"),
        help="Estimated taker fee percentage per side for reward-vs-fee checks.",
    )
    parser.add_argument(
        "--min-reward-to-fee-ratio",
        type=_nonnegative_decimal,
        default=Decimal("3"),
        help="Block entries when take-profit percentage is too small versus estimated round-trip taker fees. 0 disables.",
    )
    parser.add_argument(
        "--max-abs-funding-rate-pct",
        type=_nonnegative_decimal,
        default=Decimal("0"),
        help="Block entries when absolute funding rate exceeds this percentage. 0 disables the filter.",
    )
    parser.add_argument(
        "--max-entry-funding-cost-pct",
        type=_nonnegative_decimal,
        default=Decimal("0"),
        help="Block entries that would pay funding above this percentage. 0 disables the directional funding-cost filter.",
    )
    parser.add_argument("--breakeven-trigger-pct", type=_positive_decimal, default=Decimal("0.4"))
    parser.add_argument("--breakeven-offset-pct", type=_positive_decimal, default=Decimal("0.05"))
    parser.add_argument("--trailing-activation-pct", type=_positive_decimal, default=Decimal("0.6"))
    parser.add_argument("--trailing-distance-pct", type=_positive_decimal, default=Decimal("0.35"))
    parser.add_argument(
        "--dynamic-protection-reprice",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Periodically cancel/recreate native Binance TP/SL protection when software exit logic can tighten risk.",
    )
    parser.add_argument("--dynamic-protection-reprice-interval-minutes", type=_nonnegative_decimal, default=Decimal("10"))
    parser.add_argument("--dynamic-protection-min-improvement-pct", type=_nonnegative_decimal, default=Decimal("0.05"))
    parser.add_argument(
        "--ai-entry-plan",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Allow the AI final_decision to propose entry style, limit price, ATR stop multiplier, and reward/risk within deterministic clamps.",
    )
    parser.add_argument(
        "--ai-entry-max-price-drift-pct",
        type=_nonnegative_decimal,
        default=Decimal("0.20"),
        help="Block entry when the fresh post-AI last price moved this far from the price seen by the AI. 0 disables.",
    )
    parser.add_argument(
        "--ai-entry-limit-max-distance-pct",
        type=_nonnegative_decimal,
        default=Decimal("0.40"),
        help="Max allowed distance between an AI-proposed limit entry and fresh last price. 0 disables.",
    )
    parser.add_argument(
        "--execution-mode",
        choices=["market", "limit", "marketable_limit"],
        default="market",
        help="Entry execution mode. marketable_limit tries a limit first, then falls back to market if not filled enough.",
    )
    parser.add_argument("--limit-entry-timeout-seconds", type=float, default=8.0)
    parser.add_argument("--limit-entry-price-offset-pct", type=_nonnegative_decimal, default=Decimal("0.02"))
    parser.add_argument("--limit-entry-min-fill-ratio", type=_positive_decimal, default=Decimal("1"))
    parser.add_argument("--native-protection", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument(
        "--close-on-native-protection-failure",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Close a just-opened position if native Binance TP/SL protection cannot be placed.",
    )
    parser.add_argument("--cancel-stale-open-orders", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--symbol-cooldown-minutes", type=_nonnegative_decimal, default=Decimal("10"))
    parser.add_argument("--loss-symbol-cooldown-minutes", type=_nonnegative_decimal, default=Decimal("30"))
    parser.add_argument("--max-consecutive-losses", type=int, default=3)
    parser.add_argument("--loss-pause-minutes", type=_nonnegative_decimal, default=Decimal("60"))
    parser.add_argument("--auto-blacklist-loss-count", type=int, default=3)
    parser.add_argument("--auto-blacklist-minutes", type=_nonnegative_decimal, default=Decimal("360"))
    parser.add_argument("--performance-report-interval-minutes", type=_nonnegative_decimal, default=Decimal("60"))
    parser.add_argument("--health-report-interval-minutes", type=_nonnegative_decimal, default=Decimal("0"))
    parser.add_argument("--entry-block-report-lookback-minutes", type=_nonnegative_decimal, default=Decimal("360"))
    parser.add_argument("--exchange-error-alert-threshold", type=int, default=3)
    parser.add_argument("--exchange-error-alert-cooldown-minutes", type=_nonnegative_decimal, default=Decimal("30"))
    parser.add_argument("--binance-performance-sync-interval-minutes", type=_nonnegative_decimal, default=Decimal("30"))
    parser.add_argument("--binance-performance-lookback-minutes", type=_nonnegative_decimal, default=Decimal("1440"))
    parser.add_argument("--binance-symbol-performance-guard", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--binance-symbol-guard-min-trades", type=int, default=3)
    parser.add_argument("--binance-symbol-guard-max-net-loss-usdt", type=_positive_decimal, default=Decimal("5"))
    parser.add_argument("--binance-symbol-guard-blacklist-minutes", type=_nonnegative_decimal, default=Decimal("720"))
    parser.add_argument("--binance-symbol-score-penalty", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--binance-symbol-score-penalty-max", type=_nonnegative_decimal, default=Decimal("0.30"))
    parser.add_argument("--adaptive-risk", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--adaptive-risk-step", type=_nonnegative_decimal, default=Decimal("0.10"))
    parser.add_argument("--adaptive-risk-min-multiplier", type=_positive_decimal, default=Decimal("0.50"))
    parser.add_argument("--adaptive-risk-max-multiplier", type=_positive_decimal, default=Decimal("1.00"))
    parser.add_argument("--auto-tune-parameters", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--auto-tune-interval-minutes", type=_nonnegative_decimal, default=Decimal("60"))
    parser.add_argument("--auto-tune-min-trades", type=int, default=8)
    parser.add_argument("--auto-tune-lookback-trades", type=int, default=30)
    parser.add_argument("--auto-tune-symbol-min-trades", type=int, default=3)
    parser.add_argument("--auto-tune-symbol-max-loss-rate", type=_positive_decimal, default=Decimal("0.70"))
    parser.add_argument("--auto-tune-blacklist-minutes", type=_nonnegative_decimal, default=Decimal("720"))
    parser.add_argument("--auto-tune-idle-after-minutes", type=_nonnegative_decimal, default=Decimal("360"))
    parser.add_argument("--auto-tune-idle-min-confidence", type=_positive_decimal, default=Decimal("0.62"))
    parser.add_argument("--auto-tune-idle-confidence-step", type=_nonnegative_decimal, default=Decimal("0.03"))
    parser.add_argument("--auto-tune-idle-max-spread-pct", type=_nonnegative_decimal, default=Decimal("0.15"))
    parser.add_argument("--auto-tune-idle-spread-step-pct", type=_nonnegative_decimal, default=Decimal("0.02"))
    parser.add_argument("--auto-tune-idle-min-reward-to-fee-ratio", type=_positive_decimal, default=Decimal("2"))
    parser.add_argument("--auto-tune-idle-reward-fee-step", type=_nonnegative_decimal, default=Decimal("0.25"))
    parser.add_argument("--auto-tune-idle-max-entry-funding-cost-pct", type=_nonnegative_decimal, default=Decimal("0.08"))
    parser.add_argument("--auto-tune-idle-funding-step-pct", type=_nonnegative_decimal, default=Decimal("0.01"))
    parser.add_argument(
        "--notify-events",
        default="",
        help="Comma-separated notification events to send: entry,exit,risk,performance,all. Empty disables notifications.",
    )
    parser.add_argument("--discord-webhook-url", default=os.environ.get("DISCORD_WEBHOOK_URL", ""))
    parser.add_argument("--telegram-bot-token", default=os.environ.get("TELEGRAM_BOT_TOKEN", ""))
    parser.add_argument("--telegram-chat-id", default=os.environ.get("TELEGRAM_CHAT_ID", ""))
    parser.add_argument(
        "--ai-decision-cooldown-minutes",
        type=_nonnegative_decimal,
        default=Decimal("15"),
        help="Skip calling the AI decision API for a symbol if the last decision was HOLD within this many minutes.",
    )
    parser.add_argument(
        "--stale-position-hold-minutes",
        type=_nonnegative_decimal,
        default=Decimal("720"),
        help="Close open positions if they remain open longer than this many minutes and are consolidating. 0 disables.",
    )
    parser.add_argument(
        "--stale-position-min-pnl-pct",
        type=_nonnegative_decimal,
        default=Decimal("0.25"),
        help="Only close stale positions if their absolute unrealized PnL is less than this percentage (consolidation). 0 disables consolidation check.",
    )
    parser.add_argument("--interval-seconds", type=float, default=300)
    parser.add_argument("--state-file", type=Path, default=DEFAULT_STATE_FILE)
    parser.add_argument("--lessons-db-path", default="data/binance_futures_demo_ai_trader_lessons.db", help="Path to SQLite lessons database.")
    parser.add_argument(
        "--lock-file",
        type=Path,
        default=DEFAULT_LOCK_FILE,
        help="Single-instance lock file. Prevents accidental duplicate bot processes on a VPS.",
    )
    parser.add_argument(
        "--runtime-config-file",
        type=Path,
        default=None,
        help="Optional JSON file for trading parameters. Hot-reloadable keys are re-read each loop.",
    )
    parser.add_argument(
        "--new-entries-enabled",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Hot-reloadable kill switch for new entries. Open positions are still managed.",
    )
    parser.add_argument(
        "--emergency-close-positions",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Hot-reloadable emergency switch that closes open positions and blocks new entries while enabled.",
    )
    parser.add_argument("--execute", action="store_true")
    parser.add_argument(
        "--live-confirm",
        default="",
        help=f"Required with --environment live --execute. Must equal {LIVE_CONFIRMATION!r}.",
    )
    parser.add_argument("--loop", action="store_true")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--allow-short", action="store_true")
    parser.add_argument("--allow-fallback", action="store_true")
    parser.add_argument(
        "--ai-max-retries",
        type=int,
        default=2,
        help="Retry AI decision calls before fail-closed handling.",
    )
    parser.add_argument(
        "--ai-retry-delay-seconds",
        type=float,
        default=1.0,
        help="Base delay for AI decision retry backoff.",
    )
    parser.add_argument(
        "--ai-request-timeout-seconds",
        type=float,
        default=180.0,
        help="Per-model AI request timeout. Use a larger value for slower thinking models.",
    )
    parser.add_argument("--review-trades", action=argparse.BooleanOptionalAction, default=True)
    return parser.parse_args()


def _validate_args(args: argparse.Namespace) -> None:
    if args.leverage < 1 or args.leverage > 125:
        raise RuntimeError("--leverage must be between 1 and 125")
    if args.universe_size < 1:
        raise RuntimeError("--universe-size must be at least 1")
    if getattr(args, "ai_candidate_count", 1) < 1:
        raise RuntimeError("--ai-candidate-count must be at least 1")
    if getattr(args, "ai_entry_aggressiveness", Decimal("0")) > 1:
        raise RuntimeError("--ai-entry-aggressiveness must be <= 1")
    if getattr(args, "ai_entry_max_price_drift_pct", Decimal("0")) > Decimal("2"):
        raise RuntimeError("--ai-entry-max-price-drift-pct must be <= 2")
    if getattr(args, "ai_entry_limit_max_distance_pct", Decimal("0")) > Decimal("5"):
        raise RuntimeError("--ai-entry-limit-max-distance-pct must be <= 5")
    if getattr(args, "ai_max_retries", 0) < 0:
        raise RuntimeError("--ai-max-retries must be >= 0")
    if getattr(args, "ai_retry_delay_seconds", 0) < 0:
        raise RuntimeError("--ai-retry-delay-seconds must be >= 0")
    if getattr(args, "ai_request_timeout_seconds", 180.0) <= 0:
        raise RuntimeError("--ai-request-timeout-seconds must be > 0")
    if getattr(args, "sizing_cap_buffer_pct", Decimal("0")) > 20:
        raise RuntimeError("--sizing-cap-buffer-pct must be <= 20")
    if getattr(args, "adaptive_risk_min_multiplier", Decimal("0")) > getattr(args, "adaptive_risk_max_multiplier", Decimal("1")):
        raise RuntimeError("--adaptive-risk-min-multiplier must be <= --adaptive-risk-max-multiplier")
    if getattr(args, "limit_entry_timeout_seconds", 0) < 0:
        raise RuntimeError("--limit-entry-timeout-seconds must be >= 0")
    if getattr(args, "limit_entry_min_fill_ratio", Decimal("1")) > 1:
        raise RuntimeError("--limit-entry-min-fill-ratio must be <= 1")
    if getattr(args, "binance_performance_lookback_minutes", Decimal("1440")) <= 0:
        raise RuntimeError("--binance-performance-lookback-minutes must be > 0")
    if getattr(args, "binance_symbol_guard_min_trades", 1) < 1:
        raise RuntimeError("--binance-symbol-guard-min-trades must be at least 1")
    if getattr(args, "binance_symbol_score_penalty_max", Decimal("0")) > 1:
        raise RuntimeError("--binance-symbol-score-penalty-max must be <= 1")
    if getattr(args, "auto_tune_min_trades", 1) < 1:
        raise RuntimeError("--auto-tune-min-trades must be at least 1")
    if getattr(args, "auto_tune_lookback_trades", 1) < getattr(args, "auto_tune_min_trades", 1):
        raise RuntimeError("--auto-tune-lookback-trades must be >= --auto-tune-min-trades")
    if getattr(args, "auto_tune_symbol_min_trades", 1) < 1:
        raise RuntimeError("--auto-tune-symbol-min-trades must be at least 1")
    if getattr(args, "auto_tune_symbol_max_loss_rate", Decimal("0")) > 1:
        raise RuntimeError("--auto-tune-symbol-max-loss-rate must be <= 1")
    if getattr(args, "auto_tune_idle_min_confidence", Decimal("0")) > Decimal("0.88"):
        raise RuntimeError("--auto-tune-idle-min-confidence must be <= 0.88")
    if getattr(args, "max_consecutive_losses", 1) < 1:
        raise RuntimeError("--max-consecutive-losses must be at least 1")
    if getattr(args, "auto_blacklist_loss_count", 1) < 1:
        raise RuntimeError("--auto-blacklist-loss-count must be at least 1")
    if getattr(args, "exchange_error_alert_threshold", 0) < 0:
        raise RuntimeError("--exchange-error-alert-threshold must be >= 0")
    if getattr(args, "max_open_positions", 1) < 1:
        raise RuntimeError("--max-open-positions must be at least 1")
    if getattr(args, "max_new_entries_per_cycle", 1) < 1:
        raise RuntimeError("--max-new-entries-per-cycle must be at least 1")
    if not _model_chain(args):
        raise RuntimeError("At least one AI model must be configured")
    if args.risk_per_trade_pct > 100:
        raise RuntimeError("--risk-per-trade-pct must be <= 100")
    if args.max_margin_allocation_pct > 100:
        raise RuntimeError("--max-margin-allocation-pct must be <= 100")
    if getattr(args, "available_balance_buffer_pct", Decimal("0")) > 100:
        raise RuntimeError("--available-balance-buffer-pct must be <= 100")
    if getattr(args, "dynamic_exit_min_stop_pct", Decimal("0")) > getattr(args, "dynamic_exit_max_stop_pct", Decimal("1")):
        raise RuntimeError("--dynamic-exit-min-stop-pct must be <= --dynamic-exit-max-stop-pct")
    if getattr(args, "dynamic_exit_min_take_profit_pct", Decimal("0")) > getattr(args, "dynamic_exit_max_take_profit_pct", Decimal("1")):
        raise RuntimeError("--dynamic-exit-min-take-profit-pct must be <= --dynamic-exit-max-take-profit-pct")
    if getattr(args, "dynamic_exit_min_reward_risk", Decimal("0")) > getattr(args, "dynamic_exit_max_reward_risk", Decimal("1")):
        raise RuntimeError("--dynamic-exit-min-reward-risk must be <= --dynamic-exit-max-reward-risk")
    if args.ema_fast >= args.ema_slow:
        raise RuntimeError("--ema-fast must be lower than --ema-slow")
    if args.candle_limit < max(args.ema_slow, 15) + 1:
        raise RuntimeError("--candle-limit is too low for the configured indicators")
    if args.mtf_candle_limit < max(args.ema_slow, 15) + 1:
        raise RuntimeError("--mtf-candle-limit is too low for the configured indicators")
    if getattr(args, "max_data_age_seconds", Decimal("0")) < 0:
        raise RuntimeError("--max-data-age-seconds must be >= 0")
    if args.high_volatility_atr_pct <= args.low_volatility_atr_pct:
        raise RuntimeError("--high-volatility-atr-pct must be greater than --low-volatility-atr-pct")
    if args.environment == "live" and args.execute and args.live_confirm != LIVE_CONFIRMATION:
        raise RuntimeError(
            f"Live execution is locked. Re-run with --live-confirm {LIVE_CONFIRMATION!r} only after demo validation."
        )


def main() -> int:
    if load_dotenv is not None:
        load_dotenv()

    args = parse_args()
    _apply_runtime_config(args, hot_reload=False)
    if not args.loop and not args.once:
        args.once = True
    _validate_args(args)
    _acquire_process_lock(args.lock_file)

    if args.environment == "live":
        _key_name, api_key = _first_env("BINANCE_FUTURES_LIVE_API_KEY")
        _secret_name, api_secret = _first_env("BINANCE_FUTURES_LIVE_API_SECRET")
    else:
        _key_name, api_key = _first_env("BINANCE_FUTURES_DEMO_API_KEY", "BINANCE_FUTURES_TESTNET_API_KEY")
        _secret_name, api_secret = _first_env("BINANCE_FUTURES_DEMO_API_SECRET", "BINANCE_FUTURES_TESTNET_API_SECRET")

    exchange = _build_exchange(args.exchange_id, api_key, api_secret, args.environment)
    print(f"Exchange: {exchange.id} environment={args.environment} execute={args.execute}")
    exchange.load_markets()
    symbols = _resolve_requested_symbols(exchange, args)
    args.resolved_symbols = symbols
    print(f"Markets: {', '.join(symbols)}")
    for symbol in symbols:
        print(f"Configuring market: {symbol}")
        _set_risk_controls(exchange, symbol, args.leverage, args.margin_mode)
    dual_side = _fetch_dual_side_position(exchange)
    print(f"Position mode: {'hedge' if dual_side else 'one-way'}")

    state = _load_state(args.state_file)
    baseline_snapshot = _snapshot_args(args)
    while True:
        try:
            _restore_args(args, baseline_snapshot)
            runtime_changed = _safe_hot_reload_runtime_config(args, state)
            if runtime_changed:
                state.pop("auto_tune_overrides", None)
                state.pop("lessons_learned_overrides", None)
                baseline_snapshot = _snapshot_args(args)
            auto_override_changed = _apply_auto_tune_overrides(args, state)
            _apply_lessons_learned_thresholds(args, state)
            _maybe_auto_tune_parameters(state, args)
            auto_override_changed = _apply_auto_tune_overrides(args, state) or auto_override_changed
            if auto_override_changed:
                _validate_args(args)
            _sync_binance_performance(exchange, args, state, symbols)
            _maybe_apply_binance_symbol_performance_guard(state, args)
            account = _fetch_futures_account(exchange)
            state = _run_trading_cycle(exchange, args, symbols, dual_side, state, account)
            _clear_exchange_error_streak(state)
            _save_state(args.state_file, state)
        except ccxt.BaseError as exc:
            # Binance demo occasionally returns transient account errors even
            # when the next request succeeds. Do not let one bad demo response
            # turn into a restart loop.
            _record_exchange_error(state, args, exc)
            _save_state(args.state_file, state)
            print(f"Exchange error during iteration; skipping this cycle: {exc}")
        if args.once:
            break
        time.sleep(args.interval_seconds)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("Stopped.")
        raise SystemExit(130)
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)
