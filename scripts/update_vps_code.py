import json

# 1. Update Strategy Python Code
py_path = "/home/ubuntu/lumibot/lumibot/example_strategies/binance_futures_demo_ai_trader.py"
with open(py_path, "r", encoding="utf-8") as f:
    code = f.read()

# Replace _auto_tune_baseline
old_baseline = """def _auto_tune_baseline(state: dict[str, Any], args: argparse.Namespace) -> dict[str, str]:
    baseline = state.setdefault("auto_tune_baseline", {})
    if not isinstance(baseline, dict):
        baseline = {}
        state["auto_tune_baseline"] = baseline
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
        if key not in baseline and hasattr(args, key):
            baseline[key] = str(getattr(args, key))
    return baseline"""

new_baseline = """def _auto_tune_baseline(state: dict[str, Any], args: argparse.Namespace) -> dict[str, str]:
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
    return baseline"""

# Replace stop_loss_pct clamp
old_clamp = '"stop_loss_pct": str(_clamp_decimal(stop_loss_pct, Decimal("0.25"), Decimal("1.20"))),'
new_clamp = '"stop_loss_pct": str(_clamp_decimal(stop_loss_pct, Decimal("0.70"), Decimal("1.50"))),'

if old_baseline in code:
    code = code.replace(old_baseline, new_baseline)
    print("Baseline function replaced successfully.")
else:
    print("WARNING: old_baseline signature not found in code or already replaced.")

if old_clamp in code:
    code = code.replace(old_clamp, new_clamp)
    print("Stop loss clamp replaced successfully.")
else:
    print("WARNING: old_clamp not found in code or already replaced.")

with open(py_path, "w", encoding="utf-8") as f:
    f.write(code)

# 2. Update Configuration JSON
json_path = "/home/ubuntu/lumibot/config/binance_futures_ai_trader.demo.json"
with open(json_path, "r", encoding="utf-8") as f:
    config = json.load(f)

config["trading"]["stop_loss_pct"] = "0.80"
config["trading"]["dynamic_exit_min_stop_pct"] = "0.70"
config["trading"]["dynamic_exit_max_stop_pct"] = "1.50"

with open(json_path, "w", encoding="utf-8") as f:
    json.dump(config, f, indent=2)
print("Configuration file updated successfully.")
