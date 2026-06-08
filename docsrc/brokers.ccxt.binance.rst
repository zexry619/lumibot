Binance CCXT Configuration
==========================

Binance has selected Lumibot/CCXT handling, but it is not currently one of the
global auto-detected credential paths. Use an explicit ``Ccxt`` broker config.

Status
------

* **Live trading path:** explicit ``Ccxt`` config
* **Credential style:** API key and secret
* **Backtesting:** documented CCXT backtesting exchange id: ``binance``
* **Asset class:** crypto

Manual Config
-------------

.. code-block:: python

   from lumibot.brokers import Ccxt

   BINANCE_CONFIG = {
       "exchange_id": "binance",
       "apiKey": "your_api_key",
       "secret": "your_api_secret",
       "margin": False,
       "sandbox": False,
   }

   broker = Ccxt(BINANCE_CONFIG)

Backtesting
-----------

.. code-block:: bash

   export BACKTESTING_DATA_SOURCE=binance

Binance availability and product access vary by jurisdiction. Validate account
permissions, symbols, quote assets, order types, and fee assumptions before
using live funds.

Binance Futures Demo Smoke Test
-------------------------------

Binance USD-M futures require futures-specific behavior such as leverage,
margin mode, position mode, and reduce-only close orders. Before using live
funds, validate that behavior on Binance Futures demo trading with the standalone
smoke test:

.. code-block:: shell

   export BINANCE_FUTURES_DEMO_API_KEY=your_demo_key
   export BINANCE_FUTURES_DEMO_API_SECRET=your_demo_secret

   # You may also put those variables in a local .env file.
   # Do not commit real keys.

   # Dry run: connects to testnet, loads markets, checks balance, and prints the planned order.
   python lumibot/example_strategies/binance_futures_testnet_smoke.py

   # Execute: opens a tiny BTCUSDT demo position, waits briefly, then closes it.
   python lumibot/example_strategies/binance_futures_testnet_smoke.py --execute

The smoke test uses CCXT directly and routes Binance USD-M futures calls to the
``demo-fapi`` URLs. CCXT no longer supports the old Binance USD-M futures
sandbox/testnet mode. The script is intentionally separate from the shared
LumiBot CCXT broker because the shared broker path is spot-oriented today.
Treat a successful smoke test as the first validation step, not as proof that a
full AI futures strategy is production-ready.

Binance Futures Demo/Live AI Trader
-----------------------------------

After the smoke test opens and closes a demo position successfully, you can run
the guarded AI trader. It defaults to Binance Futures demo trading, uses
deterministic expert signals for trend, breakout, mean-reversion, and risk
management, adds multi-timeframe context, then asks Gemini to run a
multi-agent committee. The committee includes a market analyst, trend analyst,
risk manager, execution manager, trade reviewer, and final portfolio manager.
The decision prompt also includes structured execution context, risk/account
context, proposed dynamic exit plans, recent performance, market structure,
market breadth, and recent entry-block reasons so the committee can evaluate
whether the setup is worth taking immediately instead of only reading the trend
labels. Execution context uses ticker bid/ask when available and falls back to
the order book best bid/ask when ticker quotes are missing, including the quote
source and top-of-book size in the prompt.
The deterministic scanner can evaluate a large-cap Binance USD-M universe,
select the strongest candidate, and send the top scanner candidates to the
committee as context while keeping one active account position and a total notional cap. A regime detector
tells the committee whether the current market is trending, ranging, high
volatility, low volatility, or mixed.
Order sizing, leverage, available-margin checks, native Binance USD-M algo stop/take-profit orders,
native-protection failure auto-close, software stop loss, software take profit,
breakeven stop, trailing stop, daily loss limits, and reduce-only closes remain deterministic code. A confident
risk-manager veto from the committee blocks new entries in code even if the
final portfolio manager asks to trade. AI decision calls retry with backoff and
can try a comma-separated ``--models`` fallback chain. Plain model names use
Gemini for backward compatibility; ``openai:<model>`` uses an OpenAI-compatible
Chat Completions endpoint. The first entry in ``--models`` is the primary
model. The trader fails closed to ``HOLD`` by default if every configured model
remains unavailable; ``--allow-fallback`` intentionally permits the
deterministic EMA/RSI fallback to trade while AI access is down. After a
position is closed, the optional trade reviewer asks the same model chain to
summarize whether the trade was good, mixed, or bad and stores the lesson in
the state file. Binance USD-M Futures TP/SL and trailing-stop orders are
conditional algo orders; native protection is placed with
``/fapi/v1/algoOrder`` and checked with ``/fapi/v1/openAlgoOrders`` instead of
regular open-order queries. The state-file performance guard records estimated
PnL before fees, prints periodic reports, applies per-symbol cooldowns,
temporarily blacklists symbols after repeated losses, and pauses all new
entries after a configurable losing streak. For more accurate live/demo
evaluation, the trader can also sync Binance USD-M income and user-trade
history to summarize realized PnL, commission, funding fees, net income, and
fill count. Individual close events also try to attach Binance user-trade fill
prices and income-derived net PnL so spread and commission losses are visible
even when an exchange order response omits the average fill price. The synced
Binance net income can also penalize weak symbols in the scanner and
temporarily blacklist symbols after enough losing realized income events.
When ``--max-open-positions`` is greater than ``1``, the trader manages every
open symbol first, then scans only flat symbols for at most
``--max-new-entries-per-cycle`` new entry. Same-symbol pyramiding remains
blocked, and projected portfolio notional must stay below
``--max-total-notional-usdt``.
Telegram performance alerts for auto-tune updates are sent as compact rounded
key/value fields with readable reason labels instead of raw Python dictionaries.
Optional execution filters can block entries after AI approval
when the bid/ask spread is too wide, expected reward is too small versus
estimated taker fees, or funding is too expensive. The spread guard uses the
same order book best bid/ask fallback when ticker quotes are missing. The optional agent scorecard
records which entry agents aligned with profitable or losing outcomes, and
``--adaptive-risk`` can reduce sizing after losses and restore it after wins.
``--auto-tune-parameters`` can review recent closed trades, write auditable
state-file overrides, tighten or relax confidence/TP/SL/trailing/cooldown
settings, and temporarily blacklist weak symbols. ``--execution-mode
marketable_limit`` tries a marketable limit entry first and falls back to
market if the limit does not fill enough within the configured timeout. If the
limit partially fills, the fallback market order is submitted only for the
remaining quantity.
State writes are atomic and keep a ``.bak`` copy of the previous valid state so
the trader can recover if the VPS is rebooted during a write.
``--lock-file`` defaults to ``data/binance_futures_demo_ai_trader.lock`` and
prevents accidental duplicate bot processes from trading the same account/state.
``--dynamic-protection-reprice`` can periodically cancel/recreate native
Binance TP/SL orders for an open position when breakeven or trailing logic can
tighten the stop or safely pull take profit closer. It only tightens risk; it
does not widen stops or move take profit behind the current price.
Idle auto-tuning can relax entry gates after
``--auto-tune-idle-after-minutes`` without a real entry. The bot records recent
``entry_block_events`` and relaxes the matching gate when one dominates:
confidence, spread, fee/reward, funding, or AI entry aggressiveness plus a
small confidence softening when AI HOLD decisions dominate. If performance mode
is already tightening after weak recent trades, idle pressure can only soften
confidence back toward the baseline and raise AI entry aggressiveness; it does
not override protective exits, cooldowns, symbol blacklists, leverage, or
notional caps.
Use ``--runtime-config-file`` to load trading parameters from JSON; safe
hot-reload keys are re-read each loop so VPS tuning does not require editing
systemd or running ``daemon-reload``.
Invalid hot-reload edits are rolled back to the previous valid in-memory config,
recorded under ``runtime_config_errors``, and reported through a ``risk``
notification instead of stopping the running bot.
Set ``new_entries_enabled=false`` in that runtime config to pause new entries
without stopping open-position management.
Set ``emergency_close_positions=true`` to submit reduce-only market closes for
open positions and block new entries while the flag remains enabled. This runs
before candle analysis so stale market data cannot block a manual flatten.
``--health-report-interval-minutes`` can print and optionally notify a compact
health report with equity, open-trade summary, last entry time, recent
entry-block categories, the latest auto-tune overrides, and the current
consecutive exchange error count.

.. code-block:: shell

   export BINANCE_FUTURES_DEMO_API_KEY=your_demo_key
   export BINANCE_FUTURES_DEMO_API_SECRET=your_demo_secret
   export GEMINI_API_KEY=your_gemini_key
   export OPENAI_COMPATIBLE_BASE_URL=http://localhost:8083/v1

   # One dry-run AI decision.
   python lumibot/example_strategies/binance_futures_demo_ai_trader.py --once

   # Continuous dry run.
   python lumibot/example_strategies/binance_futures_demo_ai_trader.py --loop

   # Continuous demo trading. Still demo-only, not live.
   python lumibot/example_strategies/binance_futures_demo_ai_trader.py \
      --loop \
      --execute \
      --runtime-config-file config/binance_futures_ai_trader.demo.json

The config file keeps trading parameters out of systemd. Startup-only values
such as ``symbol_universe``, ``universe_size``, and ``leverage`` are applied
when the bot starts. Hot-reload values such as ``models``, ``advisor_models``, ``shadow_models``,
``ai_entry_aggressiveness``, ``ai_entry_plan``,
``ai_entry_max_price_drift_pct``, ``ai_entry_limit_max_distance_pct``,
``min_confidence``,
``risk_per_trade_pct``, notional caps, spread/funding filters, TP/SL,
data freshness, cooldowns, adaptive-risk settings, trailing/breakeven settings,
Binance symbol performance guard settings, dynamic protection repricing
settings, and notification settings are re-read each loop. ``environment``, ``execute``, ``live_confirm``,
``state_file``, and ``runtime_config_file`` are intentionally protected and must
stay in the launch command or service definition.
Set ``new_entries_enabled`` to ``false`` in the JSON file to pause new entries
while keeping health/performance reports and deterministic position exits active;
set it back to ``true`` to resume.

Useful optional guards:

- ``--max-spread-pct`` blocks entries when the current bid/ask spread is wider
  than the threshold. If the ticker response does not include bid/ask, the
  trader checks the best order-book bid/ask before blocking and sends that
  fallback to the AI execution context as ``quote_source=order_book``. ``0``
  disables the spread filter.
- ``--taker-fee-rate-pct`` and ``--min-reward-to-fee-ratio`` block trades where
  the effective take-profit target, including the dynamic exit plan when
  enabled, is too small relative to estimated round-trip taker fees.
- ``--ai-entry-plan`` lets the AI propose an entry style, limit price, ATR stop
  multiplier, and reward/risk target. The bot refreshes Binance ticker data
  after the AI decision, blocks entries when the post-AI price drift is above
  ``--ai-entry-max-price-drift-pct``, and rejects AI limit prices outside
  ``--ai-entry-limit-max-distance-pct`` or across the fresh bid/ask. AI values
  remain bounded by deterministic spread, fee, funding, sizing, and dynamic
  TP/SL clamps.
- ``--max-abs-funding-rate-pct`` blocks entries during extreme funding
  regardless of side.
- ``--max-entry-funding-cost-pct`` blocks entries that would pay funding above
  the threshold.
- ``--max-data-age-seconds`` blocks candle-based AI decisions and new entries
  when the latest primary timeframe candle is stale. ``0`` disables the guard.
  Existing positions still keep deterministic native protection, notional cap,
  stop loss, take profit, breakeven, and trailing checks active from
  ticker/position data.
- Before any new entry, the trader re-checks Binance account positions. If the
  account already has another nonzero USD-M position, it records an
  ``account_position`` entry block and skips the order so the one-position
  design still holds when a manual or out-of-universe position exists.
- ``--available-balance-buffer-pct`` requires Binance futures
  ``availableBalance`` to cover estimated initial margin plus a buffer before
  submitting an entry. If available balance is too low, the trader records an
  ``available_margin`` entry block instead of sending an order likely to fail
  for insufficient margin.
- ``--binance-performance-sync-interval-minutes`` enables endpoint-based
  realized PnL, commission, funding, and net-income reporting. Close journals
  prefer Binance user-trade fill prices and income-derived net PnL when those
  endpoints are available.
- ``--binance-symbol-score-penalty`` reduces a symbol's scanner score when
  Binance synced net income is negative after enough realized income events.
  ``--binance-symbol-performance-guard`` can temporarily blacklist the symbol
  when net income falls below ``--binance-symbol-guard-max-net-loss-usdt``.
- ``--notify-events entry,exit,risk,performance,health`` sends optional Discord or
  Telegram notifications when matching webhook/token environment variables are
  configured. Discord receives compact plain text; Telegram uses Telegram
  MarkdownV2 with a title, event label, summary, and key/value details for
  mobile-readable entry, exit, risk, performance, and health alerts.
- ``--auto-tune-parameters`` stores ``auto_tune_overrides`` in the state file
  after enough closed trades are available. The overrides are applied after the
  runtime config loads, so they are auditable and can be disabled by turning the
  flag off in the JSON config.
- ``--auto-tune-idle-after-minutes`` handles the over-conservative case where
  the bot does not get entries for hours or days. It tracks recent entry-block
  reasons and relaxes only the matching entry gate. AI HOLD dominance can raise
  entry aggressiveness and gently soften confidence, but not leverage, notional
  caps, or protective exits.
- ``--execution-mode marketable_limit`` attempts a limit order near the current
  bid/ask before falling back to market for only the unfilled remaining amount.
  Use plain ``market`` for maximum fill certainty or ``limit`` to skip entries
  that do not fill.
- ``--exchange-error-alert-threshold`` records repeated CCXT/Binance cycle
  errors in state and sends a ``risk`` notification after the configured number
  of consecutive failures. ``--exchange-error-alert-cooldown-minutes`` prevents
  repeated alert spam while an upstream exchange issue is ongoing.

The trader defaults to long-only. Add ``--allow-short`` only after validating
short entries and reduce-only closing behavior in demo trading.
With ``--symbol-universe top-usdm-large-cap``, the scanner builds a large-cap
USD-M futures universe, ranks available symbols by Binance 24h volume, selects
the best-scoring setup, and sends the top ``--ai-candidate-count`` candidates
to the AI committee for context. With ``--risk-per-trade-pct`` above zero,
position notional is sized from equity, stop distance, leverage, and the
configured margin/notional caps. ``--sizing-cap-buffer-pct`` keeps target
notional below caps before exchange quantity rounding. If Binance precision
still rounds the order slightly above the cap, the trader floors the amount to
the next lower exchange step so the submitted notional stays within the
configured cap. ``--dynamic-exit-plan`` is enabled by default and derives each
new trade's stop-loss and take-profit percentages from ATR, multi-timeframe
trend alignment, market regime, decision confidence, higher-timeframe
opposition, and RSI overextension while staying inside configured min/max stop,
take-profit, and reward/risk bounds. The chosen exit plan is stored on
``open_trade`` and risk-based sizing uses its effective stop distance.
``--native-protection`` is enabled by default and attempts to place
Binance reduce-only ``STOP_MARKET`` and ``TAKE_PROFIT_MARKET`` orders
immediately after a market entry. ``--close-on-native-protection-failure`` is
also enabled by default; if Binance rejects those protective orders after an
entry fills, the trader submits a reduce-only close, records
``NATIVE_PROTECTION_FAILED``, and sends a ``risk`` notification instead of
leaving a fresh position without native TP/SL. Software exits remain active as a fallback.
With ``--dynamic-protection-reprice``, open positions are revisited on a
configurable interval. The trader compares existing native protection against
the stored exit plan plus breakeven/trailing state, then recreates native
orders only when the new stop or take-profit price improves by at least
``--dynamic-protection-min-improvement-pct``.
``--cancel-stale-open-orders`` is enabled by default; when the trader sees no
position on the selected symbol, it cancels leftover open orders before a new
entry is allowed.
For hosted OpenAI-compatible providers, also set
``OPENAI_COMPATIBLE_API_KEY``. Local no-auth endpoints on ``localhost`` can
omit it. If a model is overloaded, quota-limited, or not available to the
selected provider, the trader logs the failure and tries the next model in
``--models`` before fail-closed handling. ``--ai-request-timeout-seconds``
defaults to 180 seconds so slower thinking models have time to finish.
Use ``--advisor-models`` to compare another provider such as
``qwen:qwen3.7-max-thinking`` against the execution model before the final
decision. Advisor models see the same prompt and their opinions are added to
the final Gemini context, but entries, exits, and risk checks still use the
final ``--models`` decision. Use ``--shadow-models`` for passive after-the-fact
audit models. The ``qwen:`` prefix uses
``QWEN_OPENAI_COMPATIBLE_BASE_URL`` and ``QWEN_OPENAI_COMPATIBLE_API_KEY`` so
it can run beside a separate Gemini OpenAI-compatible bridge configured through
``OPENAI_COMPATIBLE_BASE_URL``. Qwen requests automatically send
``enable_thinking=false`` in the provider-specific request body.
Thinking-model advisor calls can take more than 100 seconds on full trading
prompts, so use a larger ``--ai-request-timeout-seconds`` value such as ``360``
when enabling ``qwen:qwen3.7-max-thinking``.

The same engine is also available through a LumiBot ``Strategy`` lifecycle
wrapper:

.. code-block:: shell

   python lumibot/example_strategies/binance_futures_ai_strategy.py --once

The wrapper moves setup into ``initialize()`` and one cycle into
``on_trading_iteration()`` while keeping direct CCXT futures execution. The
shared LumiBot CCXT broker path is still spot-oriented for this Binance USD-M
futures workflow.

Live execution uses separate environment variables and is locked behind an
explicit confirmation phrase:

.. code-block:: shell

   export BINANCE_FUTURES_LIVE_API_KEY=your_live_key
   export BINANCE_FUTURES_LIVE_API_SECRET=your_live_secret
   export GEMINI_API_KEY=your_gemini_key

   python lumibot/example_strategies/binance_futures_demo_ai_trader.py \
      --environment live --loop --execute --live-confirm I_UNDERSTAND_LIVE_RISK

Validate dry-run and demo execution before enabling live trading. Prefer
IP-restricted Binance API keys on a VPS.

Binance Futures demo private endpoints may intermittently return
``{"code":-1109,"msg":"Invalid account."}`` even when the same credentials
succeed on retry. The guarded demo/live AI trader retries these account and
position reads briefly, then fails closed for that cycle if Binance continues
to reject the request.
