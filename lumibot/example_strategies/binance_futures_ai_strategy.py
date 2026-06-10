"""LumiBot Strategy wrapper for the Binance USD-M futures AI trader.

This is the bridge from the standalone CCXT runner toward a full LumiBot
Strategy. It uses LumiBot lifecycle methods while keeping Binance futures
execution in the tested direct-CCXT engine from
``binance_futures_demo_ai_trader.py``.

The shared LumiBot CCXT broker path is currently spot-oriented for this use
case, so this wrapper intentionally does not submit Binance futures orders
through ``self.submit_order`` yet.
"""

from __future__ import annotations

import sys
import time
from argparse import Namespace

import ccxt

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover
    load_dotenv = None

from lumibot.brokers.example_broker import ExampleBroker
from lumibot.entities import Asset, Order, Position
from lumibot.example_strategies.binance_futures_demo_ai_trader import (
    _acquire_process_lock,
    _apply_auto_tune_overrides,
    _apply_lessons_learned_thresholds,
    _apply_runtime_config,
    _build_exchange,
    _clear_exchange_error_streak,
    _fetch_dual_side_position,
    _fetch_futures_account,
    _first_env,
    _load_state,
    _maybe_apply_binance_symbol_performance_guard,
    _record_exchange_error,
    _resolve_requested_symbols,
    _run_trading_cycle,
    _safe_hot_reload_runtime_config,
    _save_state,
    _set_risk_controls,
    _maybe_auto_tune_parameters,
    _sync_binance_performance,
    _validate_args,
    parse_args,
)
from lumibot.strategies.strategy import Strategy


class BinanceFuturesLifecycleBroker(ExampleBroker):
    """Quiet broker adapter for using LumiBot lifecycle around direct futures execution."""

    NAME = "BinanceFuturesLifecycleBroker"

    def _get_balances_at_broker(self, quote_asset: Asset, strategy) -> tuple:
        return 0.0, 0.0, 0.0

    def _get_stream_object(self):
        return None

    def _parse_broker_order(self, response: dict, strategy_name: str, strategy_object: Strategy | None = None) -> Order:
        return None

    def _pull_broker_all_orders(self) -> list:
        return []

    def _pull_broker_order(self, identifier: str) -> dict | None:
        return None

    def _pull_position(self, strategy: Strategy, asset: Asset) -> Position | None:
        return None

    def _pull_positions(self, strategy: Strategy) -> list[Position]:
        return []

    def _register_stream_events(self):
        return None

    def _run_stream(self):
        return None

    def get_historical_account_value(self) -> dict:
        return {"hourly": None, "daily": None}


class BinanceFuturesAIStrategy(Strategy):
    """LumiBot lifecycle wrapper around the Binance futures AI engine."""

    IS_BACKTESTABLE = False

    def initialize(self, runtime_args: Namespace | None = None) -> None:
        if runtime_args is None:
            raise ValueError("runtime_args is required. Use parse_args() from the runner.")

        self.args = runtime_args
        self.set_market("24/7")
        self.sleeptime = f"{int(max(self.args.interval_seconds, 1))}S"

        if self.args.exchange_id == "okx":
            if self.args.environment == "live":
                _key_name, api_key = _first_env("OKX_API_KEY")
                _secret_name, api_secret = _first_env("OKX_SECRET_KEY")
            else:
                _key_name, api_key = _first_env("OKX_DEMO_API_KEY", "OKX_API_KEY", "BINANCE_FUTURES_DEMO_API_KEY")
                _secret_name, api_secret = _first_env("OKX_DEMO_SECRET_KEY", "OKX_SECRET_KEY", "BINANCE_FUTURES_DEMO_API_SECRET")
        else:
            if self.args.environment == "live":
                _key_name, api_key = _first_env("BINANCE_FUTURES_LIVE_API_KEY")
                _secret_name, api_secret = _first_env("BINANCE_FUTURES_LIVE_API_SECRET")
            else:
                _key_name, api_key = _first_env("BINANCE_FUTURES_DEMO_API_KEY", "BINANCE_FUTURES_TESTNET_API_KEY")
                _secret_name, api_secret = _first_env(
                    "BINANCE_FUTURES_DEMO_API_SECRET",
                    "BINANCE_FUTURES_TESTNET_API_SECRET",
                )

        self.exchange = _build_exchange(self.args.exchange_id, api_key, api_secret, self.args.environment)
        self.log_message(
            f"Exchange: {self.exchange.id} environment={self.args.environment} execute={self.args.execute}"
        )
        self.exchange.load_markets()

        self.symbols = _resolve_requested_symbols(self.exchange, self.args)
        self.args.resolved_symbols = self.symbols
        self.log_message(f"Markets: {', '.join(self.symbols)}")

        for symbol in self.symbols:
            self.log_message(f"Configuring market: {symbol}")
            _set_risk_controls(self.exchange, symbol, self.args.leverage, self.args.margin_mode)

        self.dual_side = _fetch_dual_side_position(self.exchange)
        self.log_message(f"Position mode: {'hedge' if self.dual_side else 'one-way'}")
        self.state = _load_state(self.args.state_file)

    def on_trading_iteration(self) -> None:
        try:
            runtime_changed = _safe_hot_reload_runtime_config(self.args, self.state)
            if runtime_changed:
                self.state.pop("auto_tune_overrides", None)
                self.state.pop("lessons_learned_overrides", None)
            auto_override_changed = _apply_auto_tune_overrides(self.args, self.state)
            _apply_lessons_learned_thresholds(self.args, self.state)
            _maybe_auto_tune_parameters(self.state, self.args)
            auto_override_changed = _apply_auto_tune_overrides(self.args, self.state) or auto_override_changed
            if auto_override_changed:
                _validate_args(self.args)
            _sync_binance_performance(self.exchange, self.args, self.state, self.symbols)
            _maybe_apply_binance_symbol_performance_guard(self.state, self.args)
            account = _fetch_futures_account(self.exchange)
            self.state = _run_trading_cycle(
                self.exchange,
                self.args,
                self.symbols,
                self.dual_side,
                self.state,
                account,
            )
            _clear_exchange_error_streak(self.state)
            _save_state(self.args.state_file, self.state)
        except ccxt.BaseError as exc:
            _record_exchange_error(self.state, self.args, exc)
            _save_state(self.args.state_file, self.state)
            self.log_message(f"Exchange error during iteration; skipping this cycle: {exc}")

    def on_abrupt_closing(self) -> None:
        _save_state(self.args.state_file, self.state)

    def on_bot_crash(self, error: Exception | None = None) -> None:
        _save_state(self.args.state_file, self.state)
        if error is not None:
            self.log_message(f"Bot crashed after saving state: {error}")


def build_strategy(args: Namespace) -> BinanceFuturesAIStrategy:
    """Build the Strategy wrapper with a minimal broker adapter."""
    strategy = BinanceFuturesAIStrategy(
        broker=BinanceFuturesLifecycleBroker(),
        benchmark_asset=None,
        should_send_summary_to_discord=False,
        should_backup_variables_to_database=False,
    )
    strategy.initialize(runtime_args=args)
    return strategy


def run_strategy_loop(strategy: BinanceFuturesAIStrategy, args: Namespace) -> int:
    """Run the lifecycle wrapper without using the spot-oriented CCXT broker."""
    while True:
        strategy.on_trading_iteration()
        if args.once:
            break
        time.sleep(args.interval_seconds)
    return 0


def main() -> int:
    if load_dotenv is not None:
        load_dotenv()

    args = parse_args()
    _apply_runtime_config(args, hot_reload=False)
    if not args.loop and not args.once:
        args.once = True
    _validate_args(args)
    _acquire_process_lock(args.lock_file)

    strategy = build_strategy(args)
    return run_strategy_loop(strategy, args)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("Stopped.")
        raise SystemExit(130)
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)


StrategyClass = BinanceFuturesAIStrategy
