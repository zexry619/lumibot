#!/usr/bin/env python3
"""Backup and selective state reset for the Binance futures AI trader (D).

Usage:
    python scripts/reset_state_binance_futures.py [--path data/binance_futures_demo_ai_trader_state.json]

What it does:
    1. Backs up the state file to {path}.bak.{timestamp}
    2. Resets adaptive_risk_multiplier to 1.0
    3. Clears global_control and symbol_controls
    4. Removes auto_tune_overrides and lessons overrides
    5. Preserves closed_trades, agent_scorecard, and binance_performance

Run this once before deploying the new trader version.
"""

import argparse
import json
import shutil
import sys
from datetime import datetime
from pathlib import Path


def reset_state(state_path: str) -> None:
    path = Path(state_path)
    if not path.exists():
        print(f"State file not found: {path} — nothing to reset.")
        return

    state = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(state, dict):
        print(f"State file is not a JSON object: {path}")
        sys.exit(1)

    backup_path = path.with_suffix(f".bak.{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
    shutil.copy2(str(path), str(backup_path))
    print(f"Backup saved: {backup_path}")

    # Track what changed
    changes: list[str] = []

    # Reset adaptive risk multiplier
    old_arm = state.get("adaptive_risk_multiplier")
    state["adaptive_risk_multiplier"] = "1.0"
    if old_arm != "1.0":
        changes.append(f"adaptive_risk_multiplier: {old_arm} → 1.0")
    else:
        changes.append("adaptive_risk_multiplier: already 1.0 (no change)")

    # Clear global control
    old_gc = state.pop("global_control", None)
    changes.append(f"global_control: cleared (was {old_gc})")

    # Clear symbol controls
    old_sc = state.pop("symbol_controls", None)
    changes.append(f"symbol_controls: cleared (was {old_sc})")

    # Remove auto_tune overrides
    old_ato = state.pop("auto_tune_overrides", None)
    changes.append(f"auto_tune_overrides: removed (was {old_ato})")

    # Remove lessons overrides/state
    old_lessons = state.pop("lessons_override", None)
    changes.append(f"lessons_override: removed (was {old_lessons})")
    old_lessons_state = state.pop("lessons_applied", None)
    if old_lessons_state is not None:
        changes.append("lessons_applied: removed")

    # Preserve closed_trades, agent_scorecard, binance_performance
    preserved = []
    for key in ("closed_trades", "agent_scorecard", "binance_performance"):
        if key in state:
            preserved.append(f"{key}: {len(state[key]) if isinstance(state[key], list) else 'preserved'} entries" if isinstance(state[key], (list, dict)) else f"{key}: preserved")
        else:
            preserved.append(f"{key}: not present")

    path.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")
    print(f"\nState reset complete for: {path}")
    print("Changes:")
    for c in changes:
        print(f"  • {c}")
    print("Preserved:")
    for p in preserved:
        print(f"  • {p}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Reset Binance futures trader state")
    parser.add_argument("--path", default="data/binance_futures_demo_ai_trader_state.json", help="Path to the state file")
    args = parser.parse_args()
    reset_state(args.path)


if __name__ == "__main__":
    main()
