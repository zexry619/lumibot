import os
import json
import logging
from datetime import datetime
from lumibot.strategies.strategy import Strategy

class AdaptiveAITradingTeamStrategy(Strategy):
    """
    Strategi AI Trading Team adaptif untuk Lumibot.
    
    Mengimplementasikan 3 pilar pintar terinspirasi Meridian:
    1. Structured Decision Logging: Mengurangi biaya token LLM dengan melewatkan ticker yang baru saja di-skip.
    2. Lessons Learned & Threshold Evolution: Menyesuaikan batasan teknis (misal minimum volume/RSI) secara dinamis jika perdagangan berakhir rugi.
    3. Intent-Based Gating: Memisahkan tahap read-only (researcher, risk manager) dan write (trader) untuk keamanan transaksi.
    """
    parameters = {
        "universe": ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"],
        "base_min_volume": 1000000,
        "base_rsi_lower_limit": 30,
        "base_rsi_upper_limit": 70,
    }

    def initialize(self):
        self.sleeptime = "1D"
        
        # File persistensi lokal
        self.decision_log_file = "decision_log.json"
        self.lessons_log_file = "lessons_log.json"
        self.trades_history_file = "trades_history.json"
        
        # Inisialisasi parameter dinamis
        self.active_thresholds = {
            "min_volume": self.parameters["base_min_volume"],
            "rsi_lower_limit": self.parameters["base_rsi_lower_limit"],
            "rsi_upper_limit": self.parameters["base_rsi_upper_limit"]
        }
        self.load_state_from_files()

        model = os.environ.get("AI_TRADING_TEAM_MODEL", "gemini-3.1-flash-lite")
        
        # 1. Researcher Agent (Read-only)
        self.agents.create(
            name="researcher",
            model=model,
            allow_trading=False,
            system_prompt=(
                "You are a quantitative researcher. Your job is to analyze market indicators "
                "and provide a structured evidence summary. Keep your analysis objective, "
                "mentioning indicators like RSI, moving averages, and trading volume."
            )
        )

        # 2. Risk Manager Agent (Read-only - Gatekeeper)
        self.agents.create(
            name="risk_manager",
            model=model,
            allow_trading=False,
            system_prompt=(
                "You are a strict risk manager. Review the researcher's output against our active technical "
                "limits and the past trading lessons. You must decide if a trade has too much downside risk. "
                "Respond with either 'APPROVED' or 'REJECTED: <reason>'."
            )
        )

        # 3. Trader Agent (Write - Trader)
        self.agents.create(
            name="trader",
            model=model,
            allow_trading=True,
            system_prompt=(
                "You are an execution trader. You execute trades only for tickers approved by the Risk Manager. "
                "Use the available cash to place buy orders, or submit sell orders to exit underperforming positions."
            )
        )

    def load_state_from_files(self):
        # Membuat file kosong jika belum ada
        for filepath, default_val in [
            (self.decision_log_file, []),
            (self.lessons_log_file, []),
            (self.trades_history_file, {})
        ]:
            if not os.path.exists(filepath):
                with open(filepath, "w") as f:
                    json.dump(default_val, f, indent=2)

        # Threshold Evolution: Memperketat batasan trading berdasarkan Lessons Learned historis
        try:
            with open(self.lessons_log_file, "r") as f:
                lessons = json.load(f)
            
            for lesson in lessons:
                if lesson.get("outcome") == "LOSS":
                    # Naikkan batas volume min sebesar 10% agar lebih selektif
                    self.active_thresholds["min_volume"] = int(self.active_thresholds["min_volume"] * 1.10)
                    # Naikkan batas bawah RSI masuk sebesar 1.0 poin
                    self.active_thresholds["rsi_lower_limit"] += 1.0
                    self.log_message(
                        f"Threshold diperketat otomatis akibat LOSS di {lesson.get('ticker')}: "
                        f"min_volume={self.active_thresholds['min_volume']}, "
                        f"rsi_lower_limit={self.active_thresholds['rsi_lower_limit']}"
                    )
        except Exception as e:
            self.log_message(f"Error memuat/menerapkan Threshold Evolution: {e}")

    def log_decision(self, ticker, action, reason):
        try:
            with open(self.decision_log_file, "r") as f:
                decisions = json.load(f)
            
            decisions.append({
                "timestamp": datetime.now().isoformat(),
                "ticker": ticker,
                "action": action,
                "reason": reason
            })
            
            decisions = decisions[-50:]  # Batasi 50 log terakhir
            with open(self.decision_log_file, "w") as f:
                json.dump(decisions, f, indent=2)
        except Exception as e:
            self.log_message(f"Error menulis log keputusan: {e}")

    def check_recent_decision_skip(self, ticker):
        """Mengecek log keputusan terakhir untuk menghindari LLM spamming."""
        try:
            with open(self.decision_log_file, "r") as f:
                decisions = json.load(f)
            
            recent_skips = [d for d in decisions[-15:] if d["ticker"] == ticker]
            if recent_skips:
                last_decision = recent_skips[-1]
                if last_decision["action"] == "SKIP":
                    return True, last_decision["reason"]
        except Exception:
            pass
        return False, ""

    def evaluate_closed_trades(self):
        """Memantau trade yang baru saja ditutup untuk memicu proses Lessons Learned."""
        try:
            with open(self.trades_history_file, "r") as f:
                tracked_trades = json.load(f)
            
            active_positions = self.get_positions()
            active_tickers = [p.symbol for p in active_positions]
            
            closed_tickers = []
            for ticker, trade_info in list(tracked_trades.items()):
                if trade_info.get("status") == "OPEN" and ticker not in active_tickers:
                    # Posisi telah ditutup, hitung PnL
                    close_price = self.get_last_price(ticker)
                    entry_price = trade_info["entry_price"]
                    pnl_pct = ((close_price - entry_price) / entry_price) * 100
                    
                    trade_info["status"] = "CLOSED"
                    trade_info["close_price"] = close_price
                    trade_info["pnl_pct"] = pnl_pct
                    trade_info["closed_at"] = datetime.now().isoformat()
                    closed_tickers.append((ticker, trade_info))
            
            if closed_tickers:
                with open(self.trades_history_file, "w") as f:
                    json.dump(tracked_trades, f, indent=2)
                
                with open(self.lessons_log_file, "r") as f:
                    lessons = json.load(f)
                
                for ticker, info in closed_tickers:
                    outcome = "WIN" if info["pnl_pct"] >= 0 else "LOSS"
                    lesson = {
                        "ticker": ticker,
                        "pnl_pct": info["pnl_pct"],
                        "outcome": outcome,
                        "entry_volume": info.get("entry_volume"),
                        "entry_rsi": info.get("entry_rsi"),
                        "closed_at": info["closed_at"],
                        "summary": f"Trade {ticker} ditutup {outcome} dengan PnL {info['pnl_pct']:.2f}%."
                    }
                    
                    if outcome == "LOSS":
                        # Evolusi parameter langsung
                        self.active_thresholds["min_volume"] = int(self.active_thresholds["min_volume"] * 1.10)
                        self.active_thresholds["rsi_lower_limit"] += 1.0
                    
                    lessons.append(lesson)
                    self.log_message(f"Pelajaran baru dicatat untuk {ticker}. Hasil: {outcome}. PnL: {info['pnl_pct']:.2f}%")
                
                with open(self.lessons_log_file, "w") as f:
                    json.dump(lessons, f, indent=2)
                    
        except Exception as e:
            self.log_message(f"Error memproses evaluasi trade ditutup: {e}")

    def on_trading_iteration(self):
        # 1. Evaluasi trade yang sudah ditutup & perbarui thresholds
        self.evaluate_closed_trades()

        try:
            with open(self.lessons_log_file, "r") as f:
                lessons = json.load(f)
            recent_lessons_str = json.dumps(lessons[-3:], indent=2)
        except Exception:
            recent_lessons_str = "Belum ada catatan pelajaran."

        # 2. Proses keputusan untuk setiap ticker di universe
        for ticker in self.parameters["universe"]:
            # Cek Decision Log untuk menghindari pemanggilan LLM yang redundan
            should_skip, skip_reason = self.check_recent_decision_skip(ticker)
            if should_skip:
                self.log_message(f"[Decision Log Skip] Melewati {ticker} karena keputusan baru-baru ini: '{skip_reason}'")
                continue

            try:
                price = self.get_last_price(ticker)
                
                # Mock indikator teknis untuk demonstrasi (dapat diganti indikator real)
                mock_rsi = 35.0  
                mock_volume = 1200000  
                
                context = {
                    "ticker": ticker,
                    "price": price,
                    "rsi": mock_rsi,
                    "volume": mock_volume,
                    "active_thresholds": self.active_thresholds,
                    "recent_lessons": recent_lessons_str
                }

                # 3. Fase Riset (Researcher - Read-only)
                research = self.agents["researcher"].run(
                    task_prompt=f"Perform market research on {ticker}.",
                    context=context
                )

                # 4. Fase Evaluasi Risiko (Risk Manager - Read-only Gatekeeper)
                risk_context = {
                    **context,
                    "research_summary": research.summary,
                    "rule_min_volume": self.active_thresholds["min_volume"],
                    "rule_rsi_lower_limit": self.active_thresholds["rsi_lower_limit"]
                }
                
                risk_result = self.agents["risk_manager"].run(
                    task_prompt=(
                        f"Evaluate {ticker}. Check if volume {mock_volume} exceeds "
                        f"rule_min_volume {self.active_thresholds['min_volume']} and "
                        f"rsi {mock_rsi} is above rule_rsi_lower_limit {self.active_thresholds['rsi_lower_limit']}. "
                        "Determine if we should APPROVE or REJECT the trade."
                    ),
                    context=risk_context
                )

                risk_decision = risk_result.summary.strip() if risk_result.summary else ""
                
                # 5. Gated Execution (Trader hanya dipanggil jika APPROVED)
                if "APPROVED" in risk_decision:
                    trader_result = self.agents["trader"].run(
                        task_prompt=f"Buy 10 shares of {ticker} at market price.",
                        context={**risk_context, "risk_decision": risk_decision}
                    )
                    
                    self.log_decision(ticker, "BUY", "Approved by Risk Manager: " + risk_decision)
                    self.track_trade_entry(ticker, price, mock_volume, mock_rsi)
                else:
                    self.log_decision(ticker, "SKIP", f"Rejected by Risk Manager: {risk_decision}")
                    self.log_message(f"Risk Manager melewatkan {ticker}: {risk_decision}")

            except Exception as e:
                self.log_message(f"Error memproses ticker {ticker}: {e}")

    def track_trade_entry(self, ticker, price, volume, rsi):
        try:
            with open(self.trades_history_file, "r") as f:
                trades = json.load(f)
            
            trades[ticker] = {
                "status": "OPEN",
                "entry_price": price,
                "entry_volume": volume,
                "entry_rsi": rsi,
                "entered_at": datetime.now().isoformat()
            }
            
            with open(self.trades_history_file, "w") as f:
                json.dump(trades, f, indent=2)
        except Exception as e:
            self.log_message(f"Error mencatat entry trading: {e}")

if __name__ == "__main__":
    IS_BACKTESTING = True

    if IS_BACKTESTING:
        from lumibot.backtesting import YahooDataBacktesting

        AdaptiveAITradingTeamStrategy.backtest(
            YahooDataBacktesting,
            datetime(2026, 4, 7),
            datetime(2026, 5, 22),
        )
    else:
        from lumibot.brokers import Alpaca
        from lumibot.traders import Trader

        ALPACA_CONFIG = {
            "API_KEY": os.environ.get("ALPACA_API_KEY", ""),
            "API_SECRET": os.environ.get("ALPACA_API_SECRET", ""),
            "PAPER": os.environ.get("ALPACA_IS_PAPER", "true").lower() != "false",
        }

        broker = Alpaca(ALPACA_CONFIG)
        strategy = AdaptiveAITradingTeamStrategy(broker=broker)

        trader = Trader()
        trader.add_strategy(strategy)
        trader.run_all()
