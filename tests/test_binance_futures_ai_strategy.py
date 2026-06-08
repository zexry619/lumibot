from argparse import Namespace

from lumibot.example_strategies import binance_futures_ai_strategy as strategy_module


def test_strategy_alias_points_to_strategy_class():
    assert strategy_module.StrategyClass is strategy_module.BinanceFuturesAIStrategy


def test_run_strategy_loop_runs_once():
    class FakeStrategy:
        def __init__(self):
            self.calls = 0

        def on_trading_iteration(self):
            self.calls += 1

    strategy = FakeStrategy()

    result = strategy_module.run_strategy_loop(strategy, Namespace(once=True, interval_seconds=0))

    assert result == 0
    assert strategy.calls == 1


def test_build_strategy_calls_initialize(monkeypatch):
    calls = {}

    class FakeStrategy:
        def __init__(self, **_kwargs):
            pass

        def initialize(self, runtime_args=None):
            calls["runtime_args"] = runtime_args

    monkeypatch.setattr(strategy_module, "BinanceFuturesAIStrategy", FakeStrategy)
    monkeypatch.setattr(strategy_module, "BinanceFuturesLifecycleBroker", lambda: object())
    args = Namespace()

    strategy = strategy_module.build_strategy(args)

    assert isinstance(strategy, FakeStrategy)
    assert calls["runtime_args"] is args


def test_main_acquires_process_lock_before_building_strategy(monkeypatch):
    calls = []
    args = Namespace(loop=False, once=True, lock_file="bot.lock")
    monkeypatch.setattr(strategy_module, "parse_args", lambda: args)
    monkeypatch.setattr(strategy_module, "_apply_runtime_config", lambda parsed_args, hot_reload=False: calls.append("config"))
    monkeypatch.setattr(strategy_module, "_validate_args", lambda parsed_args: calls.append("validate"))
    monkeypatch.setattr(strategy_module, "_acquire_process_lock", lambda lock_file: calls.append(("lock", lock_file)))
    monkeypatch.setattr(strategy_module, "build_strategy", lambda parsed_args: "strategy")
    monkeypatch.setattr(strategy_module, "run_strategy_loop", lambda strategy, parsed_args: calls.append(("run", strategy)) or 0)

    assert strategy_module.main() == 0

    assert calls == ["config", "validate", ("lock", "bot.lock"), ("run", "strategy")]


def test_strategy_iteration_uses_safe_hot_reload_and_clears_exchange_errors(monkeypatch):
    calls = []
    strategy = object.__new__(strategy_module.BinanceFuturesAIStrategy)
    strategy.args = Namespace(state_file="state.json")
    strategy.state = {}
    strategy.exchange = object()
    strategy.symbols = ["BTC/USDT:USDT"]
    strategy.dual_side = False
    account = {"totalWalletBalance": "1000", "positions": []}

    monkeypatch.setattr(
        strategy_module,
        "_safe_hot_reload_runtime_config",
        lambda args, state: calls.append("safe_hot_reload") or True,
    )
    monkeypatch.setattr(strategy_module, "_apply_auto_tune_overrides", lambda args, state: False)
    monkeypatch.setattr(strategy_module, "_maybe_auto_tune_parameters", lambda state, args: None)
    monkeypatch.setattr(strategy_module, "_sync_binance_performance", lambda exchange, args, state, symbols: calls.append("sync"))
    monkeypatch.setattr(
        strategy_module,
        "_maybe_apply_binance_symbol_performance_guard",
        lambda state, args: calls.append("symbol_guard"),
    )
    monkeypatch.setattr(strategy_module, "_fetch_futures_account", lambda exchange: account)
    monkeypatch.setattr(
        strategy_module,
        "_run_trading_cycle",
        lambda exchange, args, symbols, dual_side, state, account_snapshot: (
            calls.append(("select_account", account_snapshot is account)) or
            calls.append(("run_account", account_snapshot is account)) or
            state
        ),
    )
    monkeypatch.setattr(strategy_module, "_clear_exchange_error_streak", lambda state: calls.append("clear_errors"))
    monkeypatch.setattr(strategy_module, "_save_state", lambda path, state: calls.append("save"))

    strategy.on_trading_iteration()

    assert calls == [
        "safe_hot_reload",
        "sync",
        "symbol_guard",
        ("select_account", True),
        ("run_account", True),
        "clear_errors",
        "save",
    ]
