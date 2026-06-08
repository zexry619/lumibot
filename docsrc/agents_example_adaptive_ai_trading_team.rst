Adaptive AI Trading Team Strategy
=================================

This strategy demonstrates how to implement three advanced AI agent features inspired by the Meridian Solana agent:

1. **Structured Decision Logging**: Logs recent decisions (BUY, SKIP) to a local JSON file. Before running any expensive LLM calls, the strategy checks the log. If a ticker was recently skipped, the strategy skips it again instantly, reducing API token usage.
2. **Lessons Learned & Threshold Evolution**: Evaluates closed trade performance. If a trade results in a loss, a "lesson" is recorded, and parameters (e.g., minimum volume requirements or RSI thresholds) are dynamically tightened for subsequent iterations.
3. **Intent-Based Action Gating**: Employs a multi-agent structure where research and risk evaluation are read-only phases, and trading is gated behind an explicit confirmation from the risk manager. Only the final ``trader`` agent has permissions to submit orders.

How the team works
------------------

* ``researcher`` analyzes market data and technical indicators.
* ``risk_manager`` evaluates the researcher's output against active technical limits and past lessons learned.
* ``trader`` executes trades only for tickers approved by the Risk Manager.

Run it with a broker
--------------------

The file defaults to broker-connected execution. With Alpaca, it runs in paper mode unless you set ``ALPACA_IS_PAPER=false``.

.. code-block:: bash

   export GEMINI_API_KEY='your-key-here'
   export ALPACA_API_KEY='your-alpaca-key'
   export ALPACA_API_SECRET='your-alpaca-secret'
   export ALPACA_IS_PAPER=true
   python examples/adaptive_ai_trading_team_example.py

Backtest it
-----------

Use the same strategy class and change ``IS_BACKTESTING = False`` to ``IS_BACKTESTING = True`` in the runner:

.. code-block:: bash

   export GEMINI_API_KEY='your-key-here'
   python examples/adaptive_ai_trading_team_example.py

Example code
------------

.. literalinclude:: ../examples/adaptive_ai_trading_team_example.py
   :language: python
