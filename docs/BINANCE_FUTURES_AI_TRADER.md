# Binance Futures AI Trader

One-line description: Runbook for the guarded Binance USD-M Futures demo/live AI trader.

Last Updated: 2026-06-07

Status: Draft

Audience: LumiBot users and contributors deploying the Binance futures AI trader to a VPS.

## Overview

The Binance futures AI trader is a standalone example in `lumibot/example_strategies/binance_futures_demo_ai_trader.py`. It is not a guaranteed-profit strategy and it does not let the model trade without limits. The design is:

- Deterministic expert signals: trend follower, breakout trader, mean reversion trader, and risk manager.
- AI model fallback chain as a multi-agent committee: market analyst, trend analyst, risk manager, execution manager, trade reviewer, and final portfolio manager. Plain model names use Gemini; `openai:<model>` uses an OpenAI-compatible Chat Completions endpoint.
- Multi-timeframe context: the model sees 1m, 5m, 15m, and 1h indicator state by default.
- Rich AI context: each decision prompt includes execution context, risk/account context, proposed dynamic exit plans, recent performance, market structure, market breadth, and recent entry-block reasons so the committee can judge whether a setup is worth taking now instead of looking only at trend direction. Execution context uses ticker bid/ask when available and falls back to the order book best bid/ask when ticker quotes are missing.
- Regime detector: each cycle is classified as trending, ranging, high volatility, low volatility, or mixed.
- Multi-symbol scanner: the bot can scan a large-cap Binance USD-M universe, rank candidates, choose the best candidate, and include the top scanner candidates in the AI committee prompt while keeping one active account position and a total notional cap.
- Deterministic risk controls: leverage, margin mode, max daily loss, available-margin buffer, native Binance USD-M algo stop/take-profit orders, native-protection failure auto-close, software stop loss, software take profit, breakeven stop, trailing stop, max notional, account-level one-position-at-a-time, and reduce-only closes.
- Final decision: the final portfolio manager returns `BUY`, `SELL`, `HOLD`, or `CLOSE`; deterministic risk rules still decide whether the order is allowed. A confident `risk_manager=HOLD` or `risk_manager=CLOSE` agent response vetoes new entries in code.
- Trade review: after a close, Gemini can review whether the trade was good, mixed, or bad and store the lesson in state for the next committee decision.
- AI outage handling: AI decision calls retry with backoff and can try a comma-separated `--models` fallback chain. The first item is primary, later items are fallback. If every model remains unavailable, the bot fails closed with `HOLD` by default instead of crashing. Add `--allow-fallback` only if you intentionally want the deterministic EMA/RSI fallback to trade while AI access is unavailable.
- Performance guard: after closed trades, the bot records estimated PnL before fees in the state file, prints periodic performance reports, cools down symbols after exits, applies longer cooldowns after losses, temporarily blacklists symbols after repeated losses, and pauses all new entries after a configurable losing streak. When a close has Binance-derived net PnL, that net PnL is used for loss controls instead of the local estimate.
- Binance performance sync: the bot can periodically pull Binance USD-M income and user-trade history to summarize realized PnL, commission, funding fees, net income, fill count, and symbol-level income. This is more accurate than the state-file estimated PnL, and close events also try to attach order/user-trade and income-derived PnL so a flat-looking local estimate does not hide spread or fee losses.
- Binance symbol performance guard: synced Binance net income can penalize weak symbols in the scanner and temporarily blacklist symbols after enough losing realized income events.
- Dynamic native protection repricing: open positions can periodically cancel/recreate Binance native TP/SL orders when breakeven/trailing logic can tighten the stop or safely pull take profit closer. The repricer only tightens risk; it does not widen stops or move take profit behind the current price.
- Execution filters: optional spread, funding-rate, and reward-vs-fee guards can block entries after AI approval but before a market order is submitted. The spread guard also falls back to order book best bid/ask when ticker quotes are missing.
- Trade journal: entry, exit, and native-protection-reprice events are stored in `state["trade_journal"]`, with Telegram entry/exit notifications including richer readable context such as TP/SL, spread, funding, adaptive risk, recent symbol PnL, Binance net income, and per-close Binance net PnL when available.
- Data freshness guard: `--max-data-age-seconds` blocks candle-based AI decisions and new entries when the latest primary candle is stale. Existing positions still keep deterministic protection checks active.
- AI decision cooldown: `--ai-decision-cooldown-minutes` skips calling the AI decision API for a symbol if the last decision was HOLD within the cooldown period (defaults to 15 minutes), saving API tokens.
- Agent scorecard: after a close, entry agent opinions are scored against the trade outcome so later reviews can see which agents aligned with profitable or losing trades.
- Optional adaptive risk: `--adaptive-risk` reduces the risk multiplier after losing closes and restores it after winning closes within configured min/max limits.
- Auto-tuning: `--auto-tune-parameters` reviews recent closed trades, writes auditable parameter overrides into the state file, tightens or relaxes confidence/TP/SL/trailing/cooldown settings, and can temporarily blacklist weak symbols.
- Idle auto-tuning: if there are not enough closed trades yet and no entries occur for `--auto-tune-idle-after-minutes`, the bot records recent entry-block reasons and can gradually relax only the relevant entry gate: minimum confidence, spread cap, reward-to-fee ratio, funding-cost cap, or AI entry aggressiveness.
- Smarter execution: `--execution-mode marketable_limit` tries a marketable limit entry first and falls back to market if the limit does not fill enough within the configured timeout. If the limit partially fills, the market fallback is only submitted for the remaining quantity.
- Optional notifications: `--notify-events` can send entry, exit, risk, performance, and health messages to Discord or Telegram.
- Runtime config: `--runtime-config-file` can load trading parameters from JSON. Safe hot-reload keys are re-read each loop so VPS tuning does not require editing systemd or running `daemon-reload`.
- Safe hot reload: if a runtime JSON edit is invalid during hot reload, the bot rolls back to the last valid in-memory config, records `runtime_config_errors`, and keeps trading/position management running.
- Manual pause: `new_entries_enabled=false` in the runtime JSON stops new entries without stopping the service. Open positions are still managed.
- Emergency flatten: `emergency_close_positions=true` in the runtime JSON closes open positions and blocks new entries while enabled. This runs before candle analysis so stale market data cannot block a manual flatten.
- Exchange error tracking: repeated CCXT/Binance cycle errors are stored in state and can trigger a risk notification after `--exchange-error-alert-threshold` consecutive errors.
- Resilient state file: state writes are atomic and keep a `.bak` copy of the previous valid state so the bot can recover after a crash or forced reboot during a write.
- Single-instance guard: `--lock-file` prevents accidentally running a second bot process against the same account/state.
- Demo-first execution: `--environment demo` is the default and uses Binance USD-M Futures demo URLs.
- Locked live execution: `--environment live --execute` requires separate live keys and `--live-confirm I_UNDERSTAND_LIVE_RISK`.

`lumibot/example_strategies/binance_futures_ai_strategy.py` provides a LumiBot
`Strategy` lifecycle wrapper around the same engine. It moves setup into
`initialize()` and one cycle into `on_trading_iteration()`. The wrapper still
uses direct CCXT futures execution because the shared LumiBot CCXT broker path
is spot-oriented for this Binance USD-M futures demo/live workflow today.

## Required Secrets

Use VPS environment variables or a private untracked env file. Do not commit real keys.

```bash
BINANCE_FUTURES_DEMO_API_KEY=your_demo_key
BINANCE_FUTURES_DEMO_API_SECRET=your_demo_secret
GEMINI_API_KEY=your_gemini_key
OPENAI_COMPATIBLE_BASE_URL=http://localhost:8083/v1
# Optional for local no-auth endpoints; required for hosted providers.
OPENAI_COMPATIBLE_API_KEY=your_openai_compatible_key
# Optional comparison endpoint for OpenAI-compatible models such as Qwen.
# Keep --models pointed at the execution model if advisor/shadow models are enabled.
QWEN_OPENAI_COMPATIBLE_BASE_URL=http://127.0.0.1:7860/v1
QWEN_OPENAI_COMPATIBLE_API_KEY=your_qwen_key
# Optional notifications.
DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/...
TELEGRAM_BOT_TOKEN=your_telegram_bot_token
TELEGRAM_CHAT_ID=your_telegram_chat_id
```

For live trading, use separate credentials:

```bash
BINANCE_FUTURES_LIVE_API_KEY=your_live_key
BINANCE_FUTURES_LIVE_API_SECRET=your_live_secret
GEMINI_API_KEY=your_gemini_key
OPENAI_COMPATIBLE_BASE_URL=http://localhost:8083/v1
OPENAI_COMPATIBLE_API_KEY=your_openai_compatible_key
```

Prefer Binance API keys with futures permission only, withdrawal disabled, and VPS IP restrictions enabled. Rotate any key that has been pasted into chat or logs.

## VPS Setup

Use a Python virtual environment so the VPS does not mix LumiBot dependencies with system packages:

```bash
cd /opt/lumibot
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e .
```

Create a private env file readable only by the service user:

```bash
sudo install -m 600 -o "$USER" -g "$USER" /dev/null /etc/lumibot-binance-futures.env
```

Add only the secrets and non-secret runtime defaults you need:

```bash
BINANCE_FUTURES_DEMO_API_KEY=your_demo_key
BINANCE_FUTURES_DEMO_API_SECRET=your_demo_secret
GEMINI_API_KEY=your_gemini_key
OPENAI_COMPATIBLE_BASE_URL=http://localhost:8083/v1
# OPENAI_COMPATIBLE_API_KEY=your_openai_compatible_key
```

## Validation Order

Run these in order. Do not skip directly to live execution.

```bash
source .venv/bin/activate
set -a
source /etc/lumibot-binance-futures.env
set +a

# 1. Connect, inspect market, and size the order without placing it.
python lumibot/example_strategies/binance_futures_testnet_smoke.py

# 2. Open and close one tiny demo futures position.
python lumibot/example_strategies/binance_futures_testnet_smoke.py --execute --notional-usdt 50 --leverage 2

# 3. Ask the expert committee + Gemini for one dry-run decision.
python lumibot/example_strategies/binance_futures_demo_ai_trader.py --once

# 3b. Run the LumiBot Strategy lifecycle wrapper for one dry-run decision.
python lumibot/example_strategies/binance_futures_ai_strategy.py --once

# 4. Run the full loop in dry-run mode.
python lumibot/example_strategies/binance_futures_demo_ai_trader.py --loop

# 5. Execute the loop on Binance Futures demo.
python lumibot/example_strategies/binance_futures_demo_ai_trader.py --loop --execute
```

Useful conservative defaults for first VPS runs:

```bash
python lumibot/example_strategies/binance_futures_demo_ai_trader.py \
  --loop \
  --execute \
  --order-notional-usdt 60 \
  --max-position-notional-usdt 100 \
  --leverage 2 \
  --min-confidence 0.65 \
  --max-daily-loss-pct 1 \
  --stop-loss-pct 0.6 \
  --take-profit-pct 1.0
```

For a more active demo-only validation profile, lower the confidence gate and
permit shorts:

```bash
python lumibot/example_strategies/binance_futures_demo_ai_trader.py \
  --loop \
  --execute \
  --runtime-config-file config/binance_futures_ai_trader.demo.json
```

The config file keeps trading parameters out of systemd. Startup-only values
such as `symbol_universe`, `universe_size`, and `leverage` are applied when the
bot starts. Hot-reload values such as `models`, `ai_entry_aggressiveness`,
`ai_entry_plan`, `ai_entry_max_price_drift_pct`,
`ai_entry_limit_max_distance_pct`, `min_confidence`,
`risk_per_trade_pct`, notional caps, spread/funding filters, TP/SL,
cooldowns, adaptive-risk settings, Binance symbol performance guards,
trailing/breakeven settings, dynamic protection repricing, and notification
settings are re-read each loop. `environment`, `execute`, `live_confirm`,
`state_file`, and `runtime_config_file` are intentionally protected and must
stay in the launch command or service definition.

To pause new entries on a VPS without stopping position management, set this in
the runtime config:

```json
"new_entries_enabled": false
```

Set it back to `true` to resume new entries. The change is hot-reloaded on the
next loop. While paused, the bot still reports health/performance and manages
any open position with deterministic exits and protection.

To close open positions and keep the bot from re-entering, set:

```json
"emergency_close_positions": true
```

The bot will submit reduce-only market closes for open positions it sees,
record the close as `EMERGENCY_CLOSE`, send a `risk` notification, and block new
entries while this flag remains `true`. Set it back to `false` after the account
is flat and you are ready to resume normal management.

For hot reload, invalid JSON or invalid values do not replace the active config.
The bot keeps the last valid in-memory config, records the failure under
`runtime_config_errors`, and sends a `risk` notification subject to the same
cooldown used by exchange-error alerts. Startup config loading is still strict:
if the service starts with an invalid config file, it fails fast rather than
trading with unknown settings.

When `--symbol-universe top-usdm-large-cap` is enabled, the scanner builds a
large-cap USD-M futures universe, ranks available markets by Binance 24h
volume, selects the highest-scoring setup, and sends the top
`--ai-candidate-count` candidates to the AI committee for context. When
`--risk-per-trade-pct` is greater than zero, the entry notional is sized from
account equity, stop distance, leverage, and the max notional/margin caps
instead of blindly using the fixed `--order-notional-usdt` value.
`--sizing-cap-buffer-pct` keeps the target notional slightly below max caps
before exchange quantity rounding. If Binance precision still rounds the order
slightly above the cap, the trader floors the amount to the next lower exchange
step so the submitted notional stays within the configured cap instead of
blocking a valid setup because of dust-level rounding.

### AI entry planning and stale-price protection

AI decisions can take long enough for the quote used in the prompt to become
stale. The trader therefore refreshes Binance ticker data after the AI returns
BUY/SELL and before it calculates sizing, TP/SL, and the actual entry order.
If the fresh last price has moved more than
`ai_entry_max_price_drift_pct` from the prompt price, the entry is blocked and
the bot waits for the next cycle.

With `ai_entry_plan` enabled, the AI may include an `entry_plan` inside
`final_decision`. Supported fields are `entry_style`, `limit_price`,
`stop_atr_multiplier`, `reward_risk`, `max_wait_minutes`, and `invalidation`.
The bot treats this as a proposal, not authority:

- `market` uses a market order.
- `marketable_limit` uses the normal marketable limit flow.
- `wait_pullback` submits a non-marketable limit order and does not fall back to
  market if it fails to fill.
- `wait_breakout` is held until a future cycle confirms the setup; pending
  stop-entry orders are not enabled.

AI limit prices are rejected when they are too far from the fresh last price
(`ai_entry_limit_max_distance_pct`) or when a pullback limit would cross the
fresh bid/ask. AI-suggested ATR stop multipliers and reward/risk values are
still clamped by the deterministic dynamic exit plan min/max settings.
When Binance performance sync is active, `--binance-symbol-score-penalty`
reduces a losing symbol's scanner score based on recent Binance net income, and
`--binance-symbol-performance-guard` can temporarily blacklist symbols after at
least `--binance-symbol-guard-min-trades` realized income events and net income
below `--binance-symbol-guard-max-net-loss-usdt`. Binance raw symbols such as
`BCHUSDT` are normalized to the scanner format (`BCH/USDT:USDT`) before these
controls are applied.
`--dynamic-exit-plan` is enabled by default and derives each new trade's
stop-loss and take-profit percentages from primary-timeframe ATR, multi-timeframe
trend alignment, market regime, decision confidence, higher-timeframe opposition,
and RSI overextension. The result is bounded by
`--dynamic-exit-min-stop-pct` / `--dynamic-exit-max-stop-pct`,
`--dynamic-exit-min-take-profit-pct` / `--dynamic-exit-max-take-profit-pct`,
and `--dynamic-exit-min-reward-risk` / `--dynamic-exit-max-reward-risk`. The
entry notional uses that effective stop distance when `--risk-per-trade-pct` is
enabled, and the chosen exit plan is stored on `open_trade` for auditability.
Before the AI committee approves a trade, the prompt also includes a preview of
the dynamic BUY and SELL exit plans using the current approval confidence
threshold. The final exit plan is still recalculated after the committee returns
its actual side and confidence.
The AI prompt is intentionally structured rather than raw-candle heavy. It
includes bid/ask spread, quote source (`ticker` or `order_book`), top-of-book
bid/ask size, fee and funding context, account equity and available balance,
adaptive risk state, loss streaks, symbol cooldown/blacklist status, recent
global and per-symbol PnL summaries, Binance performance sync summary, distance
to EMAs, 20-candle breakout/range context, short-window returns, candidate
rank/score gap, and recent entry-block reasons.
`--native-protection` is enabled by default and attempts to place Binance
reduce-only `STOP_MARKET` and `TAKE_PROFIT_MARKET` orders immediately after a
market entry. `--close-on-native-protection-failure` is also enabled by
default; if Binance rejects those protective orders after an entry fills, the
bot submits a reduce-only close, records `NATIVE_PROTECTION_FAILED`, and sends a
`risk` notification instead of leaving a fresh position without native TP/SL.
`--dynamic-protection-reprice` extends this behavior for already-open positions:
each loop can compare the current native orders against the stored exit plan,
breakeven trigger, and trailing trigger, then cancel/recreate Binance native
protection only when the new stop or take profit improves by at least
`--dynamic-protection-min-improvement-pct`. The interval is controlled by
`--dynamic-protection-reprice-interval-minutes`.
Use `--no-native-protection` only for debugging. `--cancel-stale-open-orders` is also enabled by default; when
the bot sees no position on the selected symbol, it cancels leftover open
orders before a new entry is allowed.
The trader can run more than one active symbol when `--max-open-positions` is
greater than `1`, but it still blocks pyramiding on the same symbol. Each cycle
manages all open positions first, then opens at most
`--max-new-entries-per-cycle` new position if portfolio-level notional remains
below `--max-total-notional-usdt`.

Use `--models` to choose the primary AI provider and fallback order. Plain
model names are treated as Gemini for backward compatibility. Prefixes are
explicit:

- `gemini:<model>` for native Gemini, using `GEMINI_API_KEY` or `GOOGLE_API_KEY`.
- `openai:<model>` or `openai-compatible:<model>` for an OpenAI-compatible
  Chat Completions API, using `OPENAI_COMPATIBLE_BASE_URL` or `OPENAI_BASE_URL`
  and `OPENAI_COMPATIBLE_API_KEY` or `OPENAI_API_KEY`.
- `qwen:<model>` for a separate Qwen OpenAI-compatible endpoint, using
  `QWEN_OPENAI_COMPATIBLE_BASE_URL` and `QWEN_OPENAI_COMPATIBLE_API_KEY`.

Use `--advisor-models` for active side-by-side model comparison. Advisor models
receive the same market prompt before the final model; their output is written
into `model_comparison_context` so the final Gemini model can weigh the second
opinion. Advisor output still never drives entries, exits, or risk checks
directly. Use `--shadow-models` only for passive audit models that should run
after the final decision and not influence the final prompt.

For a local no-auth OpenAI-compatible server, this is enough:

```bash
export OPENAI_COMPATIBLE_BASE_URL=http://localhost:8083/v1
export QWEN_OPENAI_COMPATIBLE_BASE_URL=http://127.0.0.1:7860/v1
python lumibot/example_strategies/binance_futures_demo_ai_trader.py \
  --loop \
  --execute \
  --models openai:gemini-3.5-flash \
  --advisor-models qwen:qwen3.7-max-thinking \
  --ai-request-timeout-seconds 180
```

For Qwen-style OpenAI-compatible endpoints, the trader sends
`extra_body={"enable_thinking": false}` automatically when the model name
contains `qwen`, matching local `qwen2API` deployments that expose
`qwen3.7-max-thinking` through `/v1/chat/completions`. The thinking model can
take more than 100 seconds on full trading prompts, so the demo runtime config
uses `ai_request_timeout_seconds=360` when Qwen is enabled as an advisor.

If the primary AI provider is overloaded or returns a transient error, the
trader retries each model `--ai-max-retries` times using
`--ai-retry-delay-seconds` as the base backoff, then moves to the next model in
`--models`. Quota and invalid-model errors are treated as non-retryable for
that model so the bot moves to the next fallback quickly. `--ai-request-timeout-seconds`
defaults to 180 seconds so slower thinking models have time to finish. Without
`--allow-fallback`, failed AI access across every configured model becomes a
safe `HOLD` for that cycle. With `--allow-fallback`, the deterministic EMA/RSI
fallback may issue `BUY` or `SELL`, so keep that option demo-only until you
have reviewed its behavior. If a configured model name is not available to the
selected provider, the bot logs the failure and tries the next model.

### Execution Filters and Performance Sync

The final AI decision is still checked by deterministic execution filters:

- `--max-spread-pct`: blocks entries when current bid/ask spread is wider than the threshold. If the ticker response does not include bid/ask, the trader checks the best order-book bid/ask before blocking. The same top-of-book fallback is sent to the AI execution context with `quote_source=order_book`. The default `0` disables this filter.
- `--taker-fee-rate-pct` and `--min-reward-to-fee-ratio`: block entries when the effective take-profit target, including the dynamic exit plan when enabled, is too small relative to estimated round-trip taker fees.
- `--max-abs-funding-rate-pct`: blocks entries when absolute funding rate is too large, regardless of direction. The default `0` disables this filter.
- `--max-entry-funding-cost-pct`: blocks entries that would pay funding above the threshold. Longs pay when funding is positive; shorts pay when funding is negative. The default `0` disables this filter.

`--binance-performance-sync-interval-minutes` periodically reads Binance
USD-M income and user-trade history. The performance report then includes
Binance-derived `net_income`, `realized_pnl`, `commission`, and `funding`
alongside the state-file estimated PnL before fees. This endpoint-based report
is the one to use when evaluating whether the bot is actually profitable.
For individual closes, the trade journal prefers Binance user-trade fill prices
and income-derived net PnL when those endpoints are available; this keeps
spread and commission losses visible even when a close order response lacks an
average price.

`--adaptive-risk` is disabled by default. When enabled, losing closes reduce
`adaptive_risk_multiplier` by `--adaptive-risk-step` down to
`--adaptive-risk-min-multiplier`; winning closes restore it up to
`--adaptive-risk-max-multiplier`.

`--auto-tune-parameters` is disabled by default unless enabled in the runtime
config. It uses recent state-file `closed_trades`, not future data. When enough
trades exist, it stores `auto_tune_overrides` in the state file and applies
them on each cycle after the runtime config is loaded. Weak recent performance
tightens `min_confidence`, stop loss, trailing, and cooldowns while increasing
the take-profit target. Better recent performance relaxes those settings within
hard-coded bounds. Symbols with enough recent trades, high loss rate, and
negative PnL are temporarily blacklisted.

Idle auto-tuning can still react to an over-conservative bot. After
`--auto-tune-idle-after-minutes` without a real entry, it reads
`entry_block_events` from the state file. If confidence blocks dominate it can
lower `min_confidence`; if spread, fee/reward, or funding blocks dominate it
relaxes only that matching gate; if AI HOLD decisions dominate it can raise
`ai_entry_aggressiveness` and slightly lower `min_confidence` so the committee
prefers a valid flat-account entry when scanner and risk context are strong.
When performance mode is already tightening because recent trades were weak,
idle pressure can only soften confidence back toward the baseline; it does not
override the protective TP/SL, trailing, cooldown, symbol blacklist, leverage,
notional cap, or risk-exit rules.

`--health-report-interval-minutes` prints and optionally notifies a compact
runtime health report. It includes equity, open trade summary, last entry time,
recent `entry_block_events` grouped by category over
`--entry-block-report-lookback-minutes`, the last auto-tune mode/reasons, and
active `auto_tune_overrides`. It also includes the current consecutive exchange
error count and last exchange error type/time. Enable Telegram/Discord delivery
by including `health` in `--notify-events`.

`--exchange-error-alert-threshold` controls alerting for repeated exchange
errors, including intermittent Binance demo account errors. The bot records the
error history in `exchange_errors`, sends a `risk` notification when the
threshold is reached, then respects `--exchange-error-alert-cooldown-minutes`
before alerting again. A successful iteration resets the consecutive error
streak while preserving history.

The state file is written atomically through a temporary file and keeps
`<state-file>.bak` as the previous valid JSON object. If the primary state file
is corrupt after a VPS crash or forced reboot, startup falls back to the backup
and prints a recovery message.

`--lock-file` defaults to `data/binance_futures_demo_ai_trader.lock`. The bot
acquires an exclusive process lock before connecting to Binance. If systemd is
already running the bot and another manual command starts the same trader, the
second process exits before scanning or placing orders. Keep one lock file per
account/strategy instance.

`--execution-mode market` submits entries as normal market orders. `limit`
submits a limit and skips the entry if it does not fill enough.
`marketable_limit` submits a limit near the current bid/ask using
`--limit-entry-price-offset-pct`, waits `--limit-entry-timeout-seconds`, and
falls back to market if `--limit-entry-min-fill-ratio` is not reached. Partial
limit fills are counted, so the market fallback only submits the unfilled
remaining amount.

`--max-data-age-seconds` is enabled in the demo runtime config at `900`
seconds. If the latest primary timeframe candle is older than the configured
threshold, the bot records a `stale_data` entry block and skips AI entry/close
decisions for that cycle. When a position is already open, deterministic native
protection, total-notional cap, stop loss, take profit, breakeven, and trailing
checks still run from ticker/position data.

Before submitting any new entry, the bot re-checks Binance account positions.
If the account already has another nonzero USD-M position, it records an
`account_position` entry block and skips the order. This protects the intended
one-position-at-a-time design even when a manual position, another service, or
an out-of-universe symbol exists on the same futures account.

The entry path also checks Binance futures `availableBalance` when the account
endpoint provides it. If the estimated initial margin plus
`--available-balance-buffer-pct` would exceed available balance, the bot records
an `available_margin` entry block instead of submitting an order that Binance is
likely to reject for insufficient margin.

Enable notifications explicitly with `--notify-events`, for example:

```bash
python lumibot/example_strategies/binance_futures_demo_ai_trader.py \
  --loop \
  --execute \
  --notify-events entry,exit,risk,performance,health
```

Discord receives a compact plain-text line. Telegram messages use Telegram
MarkdownV2 with a title, event label, summary, and key/value details so entry,
exit, risk, performance, and health alerts are readable on mobile. Auto-tune
performance alerts are formatted as rounded key/value fields with readable
reason labels and compact override summaries instead of raw Python dictionaries.

Keep live trading more conservative until the demo journal has enough fills to
evaluate entry quality, slippage, exits, and drawdown.

## Live Trading

Live trading requires replacing demo keys with live env vars and adding the confirmation phrase:

```bash
python lumibot/example_strategies/binance_futures_demo_ai_trader.py \
  --environment live \
  --loop \
  --execute \
  --live-confirm I_UNDERSTAND_LIVE_RISK \
  --order-notional-usdt 60 \
  --max-position-notional-usdt 100 \
  --leverage 2 \
  --min-confidence 0.70 \
  --max-daily-loss-pct 1 \
  --stop-loss-pct 0.6 \
  --take-profit-pct 1.0
```

Start live with small notional and low leverage. Increase only after reviewing logs, fills, slippage, and close behavior.

## Systemd Example

Create `/etc/systemd/system/lumibot-binance-futures-demo.service`:

```ini
[Unit]
Description=LumiBot Binance Futures Demo AI Trader
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=/opt/lumibot
EnvironmentFile=/etc/lumibot-binance-futures.env
ExecStart=/opt/lumibot/.venv/bin/python lumibot/example_strategies/binance_futures_ai_strategy.py --loop --execute --runtime-config-file config/binance_futures_ai_trader.demo.json
Restart=on-failure
RestartSec=20

[Install]
WantedBy=multi-user.target
```

Then:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now lumibot-binance-futures-demo.service
sudo journalctl -u lumibot-binance-futures-demo.service -f
```

Use a separate service file for live trading so demo and live commands cannot be confused.

## Troubleshooting

Binance Futures demo private endpoints can intermittently return `{"code":-1109,"msg":"Invalid account."}` even when the same key succeeds on the next request. The demo/live AI trader retries those demo private calls a few times and then fails closed for that cycle if Binance continues to reject account or position reads. Do not bypass this guard; opening a new position without a reliable position snapshot can duplicate exposure.

Binance USD-M Futures TP/SL and trailing-stop orders are conditional algo orders. The trader creates native protection through `POST /fapi/v1/algoOrder`, checks it with `GET /fapi/v1/openAlgoOrders`, and clears stale protection with `DELETE /fapi/v1/algoOpenOrders`. Do not use normal open-order checks alone for TP/SL visibility; Binance can show those orders in the UI while `fetch_open_orders()` returns no regular orders.

If a symbol starts producing repeated losses, exclude it directly with `--exclude-symbols BTCUSDT,NEARUSDT` or let the performance guard block it temporarily with `--auto-blacklist-loss-count` and `--auto-blacklist-minutes`. The guard is intentionally state-file based and estimates PnL before fees; use Binance account history for final accounting.
