"""Binance USD-M futures demo/testnet smoke test.

This script intentionally uses CCXT directly instead of LumiBot's shared CCXT
broker. The current LumiBot CCXT broker path is spot-oriented; this smoke test
validates Binance futures-specific behavior before you wire it into a strategy.

Usage:
    # Either export the variables in your shell or put them in a local .env file.
    export BINANCE_FUTURES_DEMO_API_KEY="your-demo-key"
    export BINANCE_FUTURES_DEMO_API_SECRET="your-demo-secret"

    python lumibot/example_strategies/binance_futures_testnet_smoke.py
    python lumibot/example_strategies/binance_futures_testnet_smoke.py --execute

By default the script is a dry run. Add --execute to open a tiny market position
on Binance Futures demo trading, wait briefly, then close it.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from decimal import ROUND_UP, Decimal, InvalidOperation
from typing import Any

import ccxt

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - python-dotenv is a LumiBot dependency, but keep this script standalone.
    load_dotenv = None


DEFAULT_SYMBOL = "BTC/USDT:USDT"
DEFAULT_NOTIONAL_USDT = Decimal("25")
DEFAULT_LEVERAGE = 2
DEFAULT_ENVIRONMENT = "demo"
LIVE_CONFIRMATION = "I_UNDERSTAND_LIVE_RISK"


def _parse_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _first_env(*names: str) -> tuple[str, str]:
    for name in names:
        value = os.environ.get(name, "").strip()
        if value:
            return name, value
    raise RuntimeError(f"Missing one of: {', '.join(names)}.")


def _decimal_arg(value: str, name: str) -> Decimal:
    try:
        parsed = Decimal(str(value))
    except InvalidOperation as exc:
        raise argparse.ArgumentTypeError(f"{name} must be a number") from exc
    if parsed <= 0:
        raise argparse.ArgumentTypeError(f"{name} must be positive")
    return parsed


def _positive_decimal(value: str) -> Decimal:
    return _decimal_arg(value, "value")


def _opposite_side(side: str) -> str:
    normalized = side.lower()
    if normalized == "buy":
        return "sell"
    if normalized == "sell":
        return "buy"
    raise ValueError("side must be 'buy' or 'sell'")


def _position_side_for_open(side: str) -> str:
    normalized = side.lower()
    if normalized == "buy":
        return "LONG"
    if normalized == "sell":
        return "SHORT"
    raise ValueError("side must be 'buy' or 'sell'")


def _coerce_symbol(requested_symbol: str, markets: dict[str, dict[str, Any]]) -> str:
    """Accept Binance ids like BTCUSDT or CCXT futures symbols like BTC/USDT:USDT."""
    if requested_symbol in markets:
        return requested_symbol

    normalized = requested_symbol.upper().replace("/", "").replace(":USDT", "").replace("-", "")
    for symbol, market in markets.items():
        market_id = str(market.get("id", "")).upper()
        market_symbol = str(market.get("symbol", "")).upper()
        is_usdt_swap = bool(market.get("swap")) and str(market.get("quote", "")).upper() == "USDT"
        if is_usdt_swap and (market_id == normalized or market_symbol.replace("/", "").replace(":USDT", "") == normalized):
            return symbol

    raise RuntimeError(
        f"Could not find a USD-M futures market for {requested_symbol!r}. "
        f"Try a CCXT symbol like {DEFAULT_SYMBOL!r}."
    )


def _use_binance_demo_urls(exchange) -> None:
    demo_urls = exchange.urls.get("demo")
    if not demo_urls:
        raise RuntimeError(f"{exchange.id} does not expose Binance demo URLs in this CCXT version.")
    api_urls = dict(exchange.urls.get("api") or {})
    api_urls.update(demo_urls)
    exchange.urls["api"] = api_urls


def _build_exchange(exchange_id: str, api_key: str, api_secret: str, environment: str):
    try:
        exchange_class = getattr(ccxt, exchange_id)
    except AttributeError as exc:
        raise RuntimeError(f"CCXT exchange id {exchange_id!r} is not available.") from exc

    config = {
        "apiKey": api_key,
        "secret": api_secret,
        "enableRateLimit": True,
        "options": {
            "defaultType": "future",
            "adjustForTimeDifference": True,
            "enableDemoTrading": environment == "demo",
            "fetchCurrencies": environment != "demo",
        },
    }

    # If it is OKX, read the passphrase
    password = os.environ.get("OKX_PASSPHRASE") or os.environ.get("BINANCE_FUTURES_DEMO_API_PASSWORD") or os.environ.get("OKX_DEMO_PASSPHRASE")
    if password:
        config["password"] = password

    exchange = exchange_class(config)

    if environment == "demo":
        if exchange_id == "binance" or exchange_id == "binanceusdm":
            # CCXT deprecated Binance USD-M futures sandbox/testnet mode. The current
            # Binance simulation path is demo trading, exposed through demo-fapi.
            _use_binance_demo_urls(exchange)
        else:
            exchange.set_sandbox_mode(True)
    elif environment == "live":
        pass
    else:
        raise RuntimeError(f"Unsupported environment {environment!r}. Expected 'demo' or 'live'.")
    return exchange


def _fetch_dual_side_position(exchange) -> bool:
    if exchange.id == "okx":
        try:
            config = exchange.privateGetAccountConfig()
            if config and "data" in config and len(config["data"]) > 0:
                pos_mode = config["data"][0].get("posMode")
                return pos_mode == "long_short_mode"
        except Exception as e:
            print(f"Failed to fetch position mode on OKX: {e}")
        return False

    getter = getattr(exchange, "fapiPrivateGetPositionSideDual", None)
    if getter is None:
        print("Position mode check unavailable in this CCXT build; assuming one-way mode.")
        return False

    response = getter({})
    return _parse_bool(response.get("dualSidePosition"))


def _print_balance(exchange) -> None:
    balance = exchange.fetch_balance()
    usdt = balance.get("USDT") or {}
    free = usdt.get("free") or balance.get("free", {}).get("USDT")
    total = usdt.get("total") or balance.get("total", {}).get("USDT")
    print(f"USDT balance: free={free} total={total}")


def _set_risk_controls(exchange, symbol: str, leverage: int, margin_mode: str) -> None:
    if margin_mode != "none":
        try:
            params = {}
            if exchange.id == "okx":
                params = {"lever": leverage}
            exchange.set_margin_mode(margin_mode.upper(), symbol, params)
            print(f"Margin mode set: {margin_mode}")
        except ccxt.BaseError as exc:
            message = str(exc)
            if (
                "No need to change margin type" in message
                or "not modified" in message.lower()
                or "Margin type cannot be changed if there exists open orders" in message
            ):
                print(f"Margin mode unchanged/already {margin_mode}: {message}")
            else:
                print(f"WARNING: Could not set margin mode to {margin_mode}: {message}")

    try:
        exchange.set_leverage(leverage, symbol)
        print(f"Leverage set: {leverage}x")
    except ccxt.BaseError as exc:
        message = str(exc)
        if "not modified" in message.lower() or "cannot be changed if there exists open orders" in message:
            print(f"Leverage unchanged/already {leverage}x: {message}")
        else:
            print(f"WARNING: Could not set leverage to {leverage}x: {message}")


def _market_min_notional(market: dict[str, Any]) -> Decimal | None:
    cost_min = ((market.get("limits") or {}).get("cost") or {}).get("min")
    if cost_min is not None:
        return Decimal(str(cost_min))

    info = market.get("info") or {}
    for market_filter in info.get("filters") or []:
        if market_filter.get("filterType") == "MIN_NOTIONAL":
            notional = market_filter.get("notional")
            if notional is not None:
                return Decimal(str(notional))
    return None


def _market_amount_step(market: dict[str, Any]) -> Decimal | None:
    step = None
    info = market.get("info") or {}
    for filter_type in ("MARKET_LOT_SIZE", "LOT_SIZE"):
        for market_filter in info.get("filters") or []:
            if market_filter.get("filterType") == filter_type and market_filter.get("stepSize") is not None:
                step = Decimal(str(market_filter["stepSize"]))
                break
        if step is not None:
            break

    if step is None:
        precision_amount = (market.get("precision") or {}).get("amount")
        if precision_amount is not None:
            precision_decimal = Decimal(str(precision_amount))
            if precision_decimal > 0:
                if precision_decimal >= 1:
                    step = Decimal(1).scaleb(-int(precision_decimal))
                else:
                    step = precision_decimal

    if step is not None:
        contract_size = market.get("contractSize")
        if contract_size is not None:
            step = step * Decimal(str(contract_size))
        return step
    return None


def _round_amount_up_to_step(raw_amount: Decimal, step: Decimal | None) -> Decimal:
    if step is None or step <= 0:
        return raw_amount
    return (raw_amount / step).to_integral_value(rounding=ROUND_UP) * step


def _amount_from_notional(exchange, symbol: str, notional_usdt: Decimal, last_price: Decimal) -> tuple[str, Decimal]:
    market = exchange.markets[symbol]
    min_notional = _market_min_notional(market)
    target_notional = max(notional_usdt, min_notional or Decimal("0"))
    # A small buffer keeps the market order above Binance's minimum if price ticks down
    # between sizing and submission.
    target_notional *= Decimal("1.01")

    raw_amount_base = target_notional / last_price
    step_base = _market_amount_step(market)
    raw_amount_base = _round_amount_up_to_step(raw_amount_base, step_base)

    if exchange.id == "okx":
        contract_size = Decimal(str(market.get("contractSize") or 1))
        contracts = raw_amount_base / contract_size
        contracts_str = exchange.amount_to_precision(symbol, float(contracts))
        amount_base = Decimal(contracts_str) * contract_size
        amount = str(amount_base)
    else:
        amount = exchange.amount_to_precision(symbol, float(raw_amount_base))
        amount_base = Decimal(amount)

    if amount_base <= 0:
        raise RuntimeError(f"Calculated amount is not positive: {amount}")
    return amount, amount_base * last_price


def _fetch_position_summary(exchange, symbol: str) -> None:
    try:
        positions = exchange.fetch_positions([symbol])
    except ccxt.BaseError as exc:
        print(f"Could not fetch positions: {exc}")
        return

    for position in positions:
        if position.get("symbol") != symbol:
            continue
        info = position.get("info") or {}
        contracts = position.get("contracts") or info.get("positionAmt")
        entry = position.get("entryPrice") or info.get("entryPrice")
        unrealized = position.get("unrealizedPnl") or info.get("unRealizedProfit")
        side = position.get("side") or info.get("positionSide")
        print(f"Position: symbol={symbol} side={side} contracts={contracts} entry={entry} unrealized={unrealized}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Open and close a tiny Binance USD-M futures demo position.")
    parser.add_argument("--exchange-id", default="binanceusdm", help="CCXT exchange id. Default: binanceusdm")
    parser.add_argument(
        "--environment",
        choices=["demo"],
        default=DEFAULT_ENVIRONMENT,
        help="Binance futures environment. Default: demo",
    )
    parser.add_argument("--symbol", default=DEFAULT_SYMBOL, help=f"Futures symbol or id. Default: {DEFAULT_SYMBOL}")
    parser.add_argument("--side", choices=["buy", "sell"], default="buy", help="Opening side. Default: buy")
    parser.add_argument(
        "--notional-usdt",
        type=_positive_decimal,
        default=DEFAULT_NOTIONAL_USDT,
        help=f"Position notional in USDT, not margin. Default: {DEFAULT_NOTIONAL_USDT}",
    )
    parser.add_argument(
        "--leverage",
        type=int,
        default=DEFAULT_LEVERAGE,
        help=f"Initial leverage to set on the symbol. Default: {DEFAULT_LEVERAGE}",
    )
    parser.add_argument(
        "--margin-mode",
        choices=["isolated", "cross", "none"],
        default="isolated",
        help="Margin mode to set before the order. Use 'none' to skip. Default: isolated",
    )
    parser.add_argument("--hold-seconds", type=float, default=3.0, help="Seconds to wait before closing. Default: 3")
    parser.add_argument("--execute", action="store_true", help="Actually place and close the testnet order.")
    parser.add_argument("--leave-open", action="store_true", help="Do not close the opened testnet position.")
    return parser.parse_args()


def main() -> int:
    if load_dotenv is not None:
        load_dotenv()

    args = parse_args()
    if args.leverage < 1 or args.leverage > 125:
        raise RuntimeError("--leverage must be between 1 and 125")
    if args.hold_seconds < 0:
        raise RuntimeError("--hold-seconds must be >= 0")

    api_key_name, api_key = _first_env("BINANCE_FUTURES_DEMO_API_KEY", "BINANCE_FUTURES_TESTNET_API_KEY")
    api_secret_name, api_secret = _first_env(
        "BINANCE_FUTURES_DEMO_API_SECRET",
        "BINANCE_FUTURES_TESTNET_API_SECRET",
    )
    if api_key_name.startswith("BINANCE_FUTURES_TESTNET") or api_secret_name.startswith("BINANCE_FUTURES_TESTNET"):
        print("Using legacy TESTNET env var names against Binance Futures demo URLs.")

    exchange = _build_exchange(args.exchange_id, api_key, api_secret, args.environment)
    print(f"Exchange: {exchange.id} environment={args.environment}")
    markets = exchange.load_markets()
    symbol = _coerce_symbol(args.symbol, markets)
    market = markets[symbol]
    print(f"Market: symbol={symbol} id={market.get('id')} type={market.get('type')} swap={market.get('swap')}")

    _print_balance(exchange)
    dual_side = _fetch_dual_side_position(exchange)
    print(f"Position mode: {'hedge' if dual_side else 'one-way'}")
    _set_risk_controls(exchange, symbol, args.leverage, args.margin_mode)

    ticker = exchange.fetch_ticker(symbol)
    last_price = Decimal(str(ticker["last"]))
    amount, estimated_notional = _amount_from_notional(exchange, symbol, args.notional_usdt, last_price)
    print(f"Last price: {last_price}")
    print(
        f"Planned order: side={args.side} amount={amount} "
        f"requested_notional_usdt={args.notional_usdt} estimated_notional_usdt={estimated_notional:.4f}"
    )

    if not args.execute:
        print("Dry run complete. Re-run with --execute to place the testnet order.")
        return 0

    open_params: dict[str, Any] = {}
    close_params: dict[str, Any] = {}
    if dual_side:
        position_side = _position_side_for_open(args.side)
        open_params["positionSide"] = position_side
        close_params["positionSide"] = position_side
    else:
        close_params["reduceOnly"] = True

    print("Submitting opening market order...")
    open_order = exchange.create_order(symbol, "market", args.side, amount, None, open_params)
    print(f"Open order: id={open_order.get('id')} status={open_order.get('status')}")
    _fetch_position_summary(exchange, symbol)

    if args.leave_open:
        print("Leaving testnet position open because --leave-open was set.")
        return 0

    if args.hold_seconds:
        time.sleep(args.hold_seconds)

    close_side = _opposite_side(args.side)
    print("Submitting closing market order...")
    close_order = exchange.create_order(symbol, "market", close_side, amount, None, close_params)
    print(f"Close order: id={close_order.get('id')} status={close_order.get('status')}")
    _fetch_position_summary(exchange, symbol)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)
