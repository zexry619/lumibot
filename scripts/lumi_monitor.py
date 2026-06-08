import os
import time
import threading
import logging
from collections import deque
from datetime import datetime
from flask import Flask, jsonify, render_template_string

# Create a custom logging handler to capture strategy logs
class LumiLogHandler(logging.Handler):
    def __init__(self, capacity=200):
        super().__init__()
        self.log_history = deque(maxlen=capacity)

    def emit(self, record):
        try:
            msg = self.format(record)
            self.log_history.append({
                "time": datetime.fromtimestamp(record.created).strftime("%Y-%m-%d %H:%M:%S"),
                "level": record.levelname,
                "logger": record.name.replace("lumibot.", ""),
                "message": msg
            })
        except Exception:
            self.handleError(record)


class LumiWebMonitor:
    """
    Sleek, real-time web monitoring server for LumiBot strategies.
    Starts a background Flask thread that exposes a REST API and a premium dark-mode dashboard.
    """
    def __init__(self, strategy, host="127.0.0.1", port=8000):
        self.strategy = strategy
        self.host = host
        self.port = port
        self.start_time = datetime.now()
        
        # Setup log interception
        self.log_handler = LumiLogHandler()
        formatter = logging.Formatter('%(message)s')
        self.log_handler.setFormatter(formatter)
        
        # Attach to the main lumibot logger
        lumibot_logger = logging.getLogger("lumibot")
        lumibot_logger.addHandler(self.log_handler)
        
        # Keep history of equity (portfolio value) for the charts
        self.equity_history = []
        self.equity_timestamps = []
        
        # Flask app configuration
        self.app = Flask("LumiBotMonitor")
        self.setup_routes()
        self.server_thread = None
        self._is_running = False

    def setup_routes(self):
        @self.app.route("/")
        def index():
            return render_template_string(HTML_TEMPLATE)

        @self.app.route("/api/status")
        def status():
            # Gather strategy info
            try:
                portfolio_value = self.strategy.get_portfolio_value()
                cash = self.strategy.get_cash()
            except Exception:
                portfolio_value = 0.0
                cash = 0.0

            # Calculate PnL
            initial_val = getattr(self.strategy, "initial_portfolio_value", portfolio_value) or portfolio_value
            pnl = portfolio_value - initial_val
            pnl_percent = (pnl / initial_val * 100) if initial_val > 0 else 0.0

            # Get positions
            positions_data = []
            try:
                positions = self.strategy.get_tracked_positions()
                for pos in positions:
                    positions_data.append({
                        "symbol": pos.symbol,
                        "quantity": pos.quantity,
                        "avg_price": float(pos.avg_fill_price) if pos.avg_fill_price else 0.0,
                        "current_price": float(pos.current_price) if pos.current_price else 0.0,
                        "market_value": float(pos.market_value) if pos.market_value else 0.0,
                        "pnl": float(pos.pnl) if pos.pnl else 0.0,
                        "pnl_percent": float(pos.pnl_percent) if pos.pnl_percent else 0.0,
                        "side": str(pos.side)
                    })
            except Exception as e:
                # Fallback if no positions or error
                pass

            # Get orders
            orders_data = []
            try:
                orders = self.strategy.get_tracked_orders()
                for order in orders:
                    orders_data.append({
                        "id": order.identifier,
                        "symbol": order.asset.symbol if order.asset else "N/A",
                        "quantity": order.quantity,
                        "side": str(order.side),
                        "type": str(order.order_type) if order.order_type else "SIMPLE",
                        "limit_price": float(order.limit_price) if order.limit_price else None,
                        "status": str(order.status),
                        "created": order.date_created.strftime("%Y-%m-%d %H:%M:%S") if order.date_created else "N/A"
                    })
            except Exception:
                pass

            # Track equity for chart
            now_str = datetime.now().strftime("%H:%M:%S")
            if not self.equity_timestamps or self.equity_timestamps[-1] != now_str:
                self.equity_history.append(portfolio_value)
                self.equity_timestamps.append(now_str)
                # Keep last 50 data points
                if len(self.equity_history) > 50:
                    self.equity_history.pop(0)
                    self.equity_timestamps.pop(0)

            # Get parameters
            parameters = {}
            try:
                parameters = self.strategy.get_parameters()
            except Exception:
                # Fallback parameters from attributes
                for key, val in self.strategy.__dict__.items():
                    if isinstance(val, (int, float, str, bool)) and not key.startswith("_"):
                        parameters[key] = val

            uptime = str(datetime.now() - self.start_time).split(".")[0]

            return jsonify({
                "strategy_name": self.strategy.__class__.__name__,
                "status": "Running" if self.strategy.sleeptime > 0 else "Paused",
                "uptime": uptime,
                "cash": cash,
                "portfolio_value": portfolio_value,
                "initial_value": initial_val,
                "pnl": pnl,
                "pnl_percent": pnl_percent,
                "positions": positions_data,
                "orders": orders_data,
                "parameters": parameters,
                "logs": list(self.log_handler.log_history),
                "chart": {
                    "labels": self.equity_timestamps,
                    "data": self.equity_history
                }
            })

    def start(self):
        """Starts the Flask server in a background daemon thread."""
        if self._is_running:
            return
        
        self._is_running = True
        # Save initial portfolio value to track PnL
        try:
            self.strategy.initial_portfolio_value = self.strategy.get_portfolio_value()
        except Exception:
            self.strategy.initial_portfolio_value = 0.0

        self.server_thread = threading.Thread(
            target=self._run_server, 
            name="LumiWebMonitorServer", 
            daemon=True
        )
        self.server_thread.start()
        logging.getLogger("lumibot").info(
            f"🌐 [LumiWebMonitor] Dashboard active at http://{self.host}:{self.port}"
        )

    def _run_server(self):
        # Disable Flask default console logging to prevent spamming strategy terminal
        log = logging.getLogger('werkzeug')
        log.setLevel(logging.ERROR)
        
        try:
            self.app.run(host=self.host, port=self.port, debug=False, use_reloader=False)
        except Exception as e:
            logging.getLogger("lumibot").error(f"Error running Web Monitor: {e}")

    def stop(self):
        """Clean up resources."""
        # Flask doesn't have an easy shutdown API in newer versions without hitting an endpoint,
        # but running it as a daemon thread ensures it terminates when the main Python process exits.
        self._is_running = False
        lumibot_logger = logging.getLogger("lumibot")
        lumibot_logger.removeHandler(self.log_handler)


# Premium glassmorphic dark-theme HTML Dashboard
HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>LumiBot - Strategy Live Monitor</title>
    
    <!-- Google Fonts -->
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
    
    <!-- Font Awesome Icons -->
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
    
    <!-- Chart.js CDN -->
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>

    <style>
        :root {
            --bg-base: #080b11;
            --bg-surface: #0f1624;
            --bg-card: rgba(22, 31, 51, 0.65);
            --border-color: rgba(255, 255, 255, 0.07);
            --border-hover: rgba(99, 102, 241, 0.3);
            
            --text-primary: #f8fafc;
            --text-secondary: #94a3b8;
            --text-muted: #64748b;
            
            --accent-primary: #6366f1; /* Electric Indigo */
            --accent-primary-glow: rgba(99, 102, 241, 0.15);
            --accent-success: #10b981; /* Emerald */
            --accent-success-glow: rgba(16, 185, 129, 0.15);
            --accent-danger: #f43f5e; /* Rose */
            --accent-danger-glow: rgba(244, 63, 94, 0.15);
            --accent-warning: #f59e0b; /* Amber */
            
            --font-sans: 'Plus Jakarta Sans', sans-serif;
            --font-mono: 'JetBrains Mono', monospace;
            
            --card-shadow: 0 10px 30px -10px rgba(0, 0, 0, 0.5);
            --glass-blur: blur(12px);
        }

        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
            user-select: none;
        }

        body {
            background-color: var(--bg-base);
            color: var(--text-primary);
            font-family: var(--font-sans);
            min-height: 100vh;
            overflow-x: hidden;
            background-image: 
                radial-gradient(circle at 10% 20%, rgba(99, 102, 241, 0.05) 0%, transparent 40%),
                radial-gradient(circle at 90% 80%, rgba(16, 185, 129, 0.03) 0%, transparent 40%);
        }

        /* Custom Scrollbar */
        ::-webkit-scrollbar {
            width: 8px;
            height: 8px;
        }
        ::-webkit-scrollbar-track {
            background: rgba(255, 255, 255, 0.01);
        }
        ::-webkit-scrollbar-thumb {
            background: rgba(255, 255, 255, 0.1);
            border-radius: 4px;
        }
        ::-webkit-scrollbar-thumb:hover {
            background: rgba(255, 255, 255, 0.2);
        }

        /* Layout Container */
        .app-container {
            display: flex;
            flex-direction: column;
            width: 100%;
            max-width: 1600px;
            margin: 0 auto;
            padding: 24px;
            gap: 24px;
        }

        /* Header Component */
        header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            background: var(--bg-card);
            border: 1px solid var(--border-color);
            backdrop-filter: var(--glass-blur);
            border-radius: 20px;
            padding: 20px 32px;
            box-shadow: var(--card-shadow);
            animation: slideDown 0.6s cubic-bezier(0.16, 1, 0.3, 1);
        }

        .logo-group {
            display: flex;
            align-items: center;
            gap: 16px;
        }

        .logo-icon {
            font-size: 28px;
            background: linear-gradient(135deg, var(--accent-primary) 0%, #a855f7 100%);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            filter: drop-shadow(0 2px 10px rgba(99, 102, 241, 0.3));
        }

        .logo-text h1 {
            font-size: 20px;
            font-weight: 700;
            letter-spacing: -0.5px;
            background: linear-gradient(to right, #ffffff, #e2e8f0);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
        }

        .logo-text span {
            font-size: 12px;
            color: var(--text-muted);
            font-weight: 500;
        }

        .status-badge {
            display: flex;
            align-items: center;
            gap: 8px;
            background: rgba(16, 185, 129, 0.08);
            border: 1px solid rgba(16, 185, 129, 0.2);
            padding: 8px 16px;
            border-radius: 12px;
            font-size: 13px;
            font-weight: 600;
            color: var(--accent-success);
        }

        .status-dot {
            width: 8px;
            height: 8px;
            background-color: var(--accent-success);
            border-radius: 50%;
            position: relative;
        }

        .status-dot::after {
            content: '';
            position: absolute;
            top: -4px;
            left: -4px;
            width: 16px;
            height: 16px;
            border: 2px solid var(--accent-success);
            border-radius: 50%;
            animation: pulse 1.8s infinite;
        }

        /* Top Grid - Stats */
        .stats-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
            gap: 20px;
            animation: fadeIn 0.8s cubic-bezier(0.16, 1, 0.3, 1);
        }

        .card {
            background: var(--bg-card);
            border: 1px solid var(--border-color);
            backdrop-filter: var(--glass-blur);
            border-radius: 20px;
            padding: 24px;
            box-shadow: var(--card-shadow);
            transition: all 0.3s cubic-bezier(0.16, 1, 0.3, 1);
            position: relative;
            overflow: hidden;
        }

        .card::before {
            content: '';
            position: absolute;
            top: 0;
            left: 0;
            width: 100%;
            height: 100%;
            background: linear-gradient(180deg, rgba(255, 255, 255, 0.01) 0%, transparent 100%);
            pointer-events: none;
        }

        .card:hover {
            transform: translateY(-4px);
            border-color: var(--border-hover);
            box-shadow: 0 15px 35px -10px rgba(99, 102, 241, 0.15);
        }

        .card-header-small {
            display: flex;
            justify-content: space-between;
            align-items: center;
            color: var(--text-secondary);
            font-size: 13px;
            font-weight: 500;
            margin-bottom: 12px;
        }

        .card-icon {
            width: 36px;
            height: 36px;
            border-radius: 10px;
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 16px;
        }

        .icon-blue {
            background: var(--accent-primary-glow);
            color: var(--accent-primary);
        }

        .icon-green {
            background: var(--accent-success-glow);
            color: var(--accent-success);
        }

        .card-value {
            font-size: 28px;
            font-weight: 700;
            letter-spacing: -1px;
            margin-bottom: 6px;
        }

        .card-subtext {
            font-size: 12px;
            font-weight: 500;
            color: var(--text-muted);
            display: flex;
            align-items: center;
            gap: 4px;
        }

        .trend-up {
            color: var(--accent-success);
        }

        .trend-down {
            color: var(--accent-danger);
        }

        /* Dashboard Mid Grid (Chart + Side Panel) */
        .dashboard-mid-grid {
            display: grid;
            grid-template-columns: 2fr 1fr;
            gap: 20px;
            min-height: 380px;
        }

        @media (max-width: 1024px) {
            .dashboard-mid-grid {
                grid-template-columns: 1fr;
            }
        }

        /* Table & Lists Viewport */
        .dashboard-lower-grid {
            display: grid;
            grid-template-columns: 1fr;
            gap: 20px;
        }

        .card-title {
            font-size: 16px;
            font-weight: 600;
            display: flex;
            align-items: center;
            gap: 10px;
            margin-bottom: 20px;
        }

        .card-title i {
            color: var(--accent-primary);
        }

        /* Tables */
        .table-wrapper {
            width: 100%;
            overflow-x: auto;
        }

        table {
            width: 100%;
            border-collapse: collapse;
            text-align: left;
        }

        th {
            color: var(--text-muted);
            font-size: 12px;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            padding: 12px 16px;
            border-bottom: 1px solid var(--border-color);
        }

        td {
            padding: 16px;
            font-size: 14px;
            border-bottom: 1px solid rgba(255, 255, 255, 0.03);
            font-weight: 500;
        }

        tr:last-child td {
            border-bottom: none;
        }

        tr {
            transition: background 0.2s;
        }

        tr:hover td {
            background-color: rgba(255, 255, 255, 0.015);
        }

        .badge-buy {
            background: var(--accent-success-glow);
            color: var(--accent-success);
            padding: 4px 8px;
            border-radius: 6px;
            font-size: 11px;
            font-weight: 700;
            text-transform: uppercase;
        }

        .badge-sell {
            background: var(--accent-danger-glow);
            color: var(--accent-danger);
            padding: 4px 8px;
            border-radius: 6px;
            font-size: 11px;
            font-weight: 700;
            text-transform: uppercase;
        }

        .tag {
            padding: 2px 6px;
            border-radius: 4px;
            font-size: 11px;
            font-weight: 600;
        }

        .tag-filled { background: rgba(16, 185, 129, 0.1); color: var(--accent-success); }
        .tag-active { background: rgba(99, 102, 241, 0.1); color: var(--accent-primary); }
        .tag-canceled { background: rgba(148, 163, 184, 0.1); color: var(--text-secondary); }

        /* Logger / Terminal Component */
        .terminal-container {
            background-color: rgba(10, 15, 26, 0.9);
            border: 1px solid var(--border-color);
            border-radius: 16px;
            padding: 16px;
            font-family: var(--font-mono);
            font-size: 12px;
            height: 350px;
            overflow-y: auto;
            display: flex;
            flex-direction: column;
            gap: 8px;
            box-shadow: inset 0 2px 10px rgba(0,0,0,0.8);
        }

        .log-line {
            display: flex;
            gap: 12px;
            line-height: 1.6;
            animation: logFadeIn 0.3s ease;
        }

        .log-time {
            color: var(--text-muted);
            min-width: 135px;
        }

        .log-level {
            font-weight: bold;
            min-width: 65px;
        }

        .log-level-INFO { color: var(--accent-primary); }
        .log-level-WARNING { color: var(--accent-warning); }
        .log-level-ERROR { color: var(--accent-danger); }
        .log-level-CRITICAL { color: #ffffff; background: var(--accent-danger); padding: 0 4px; border-radius: 2px; }

        .log-msg {
            color: #e2e8f0;
            white-space: pre-wrap;
            word-break: break-all;
        }

        .log-controls {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 12px;
        }

        .log-search {
            background: rgba(255, 255, 255, 0.05);
            border: 1px solid var(--border-color);
            border-radius: 8px;
            padding: 6px 12px;
            color: #fff;
            font-family: var(--font-sans);
            font-size: 12px;
            outline: none;
            width: 200px;
            transition: all 0.3s;
        }

        .log-search:focus {
            border-color: var(--accent-primary);
            box-shadow: 0 0 0 2px var(--accent-primary-glow);
        }

        /* Config list */
        .config-list {
            display: flex;
            flex-direction: column;
            gap: 12px;
            max-height: 300px;
            overflow-y: auto;
        }

        .config-item {
            display: flex;
            justify-content: space-between;
            align-items: center;
            background: rgba(255, 255, 255, 0.02);
            border: 1px solid var(--border-color);
            padding: 10px 14px;
            border-radius: 10px;
            font-size: 13px;
        }

        .config-label {
            color: var(--text-secondary);
            font-weight: 500;
        }

        .config-val {
            font-family: var(--font-mono);
            color: #fff;
            background: rgba(255,255,255,0.05);
            padding: 2px 6px;
            border-radius: 4px;
            font-size: 11px;
        }

        /* Animations */
        @keyframes pulse {
            0% { transform: scale(0.9); opacity: 1; }
            50% { transform: scale(1.6); opacity: 0; }
            100% { transform: scale(0.9); opacity: 0; }
        }

        @keyframes slideDown {
            from { transform: translateY(-20px); opacity: 0; }
            to { transform: translateY(0); opacity: 1; }
        }

        @keyframes fadeIn {
            from { opacity: 0; transform: translateY(10px); }
            to { opacity: 1; transform: translateY(0); }
        }

        @keyframes logFadeIn {
            from { opacity: 0; transform: translateX(-5px); }
            to { opacity: 1; transform: translateX(0); }
        }
    </style>
</head>
<body>
    <div class="app-container">
        <!-- Top Navigation / Header -->
        <header>
            <div class="logo-group">
                <i class="fa-solid fa-cube logo-icon"></i>
                <div class="logo-text">
                    <h1 id="strategy-name">LumiBot Live Monitor</h1>
                    <span>ALGORITHMIC TRADING CORE</span>
                </div>
            </div>
            
            <div class="status-badge">
                <div class="status-dot"></div>
                <span id="uptime-text">Uptime: 00:00:00</span>
            </div>
        </header>

        <!-- Metrics Cards Grid -->
        <div class="stats-grid">
            <!-- Card 1: Portfolio Value -->
            <div class="card">
                <div class="card-header-small">
                    <span>PORTFOLIO EQUITY</span>
                    <div class="card-icon icon-blue">
                        <i class="fa-solid fa-wallet"></i>
                    </div>
                </div>
                <div class="card-value" id="portfolio-value">$0.00</div>
                <div class="card-subtext">
                    Total current balance & holdings
                </div>
            </div>

            <!-- Card 2: Cash -->
            <div class="card">
                <div class="card-header-small">
                    <span>AVAILABLE CASH</span>
                    <div class="card-icon icon-blue">
                        <i class="fa-solid fa-coins"></i>
                    </div>
                </div>
                <div class="card-value" id="cash-value">$0.00</div>
                <div class="card-subtext">
                    Liquid buying power
                </div>
            </div>

            <!-- Card 3: Unrealized PnL -->
            <div class="card">
                <div class="card-header-small">
                    <span>UNREALIZED P&L</span>
                    <div class="card-icon icon-green" id="pnl-icon">
                        <i class="fa-solid fa-chart-line"></i>
                    </div>
                </div>
                <div class="card-value" id="pnl-value">$0.00</div>
                <div class="card-subtext" id="pnl-subtext">
                    <span class="trend-up"><i class="fa-solid fa-caret-up"></i> 0.00%</span> since start
                </div>
            </div>
        </div>

        <!-- Middle Grid (Chart & Configuration) -->
        <div class="dashboard-mid-grid">
            <!-- Chart Card -->
            <div class="card">
                <div class="card-title">
                    <i class="fa-solid fa-wave-square"></i> Live Equity Curve
                </div>
                <div style="position: relative; height: 300px; width: 100%;">
                    <canvas id="equityChart"></canvas>
                </div>
            </div>

            <!-- Config Card -->
            <div class="card">
                <div class="card-title">
                    <i class="fa-solid fa-sliders"></i> Strategy Parameters
                </div>
                <div class="config-list" id="parameters-container">
                    <!-- Config items populated dynamically -->
                    <div class="config-item"><span class="config-label">N/A</span><span class="config-val">N/A</span></div>
                </div>
            </div>
        </div>

        <!-- Lower Grid (Positions & Orders) -->
        <div class="dashboard-lower-grid">
            <div class="card">
                <div class="card-title">
                    <i class="fa-solid fa-briefcase"></i> Active Portfolio Positions
                </div>
                <div class="table-wrapper">
                    <table>
                        <thead>
                            <tr>
                                <th>Symbol</th>
                                <th>Side</th>
                                <th>Quantity</th>
                                <th>Avg Cost</th>
                                <th>Current Price</th>
                                <th>Market Value</th>
                                <th>PnL (Unrealized)</th>
                            </tr>
                        </thead>
                        <tbody id="positions-body">
                            <tr>
                                <td colspan="7" style="text-align: center; color: var(--text-muted);">No open positions.</td>
                            </tr>
                        </tbody>
                    </table>
                </div>
            </div>
        </div>

        <!-- Orders & Logs Split Grid -->
        <div class="dashboard-mid-grid">
            <!-- Recent Orders -->
            <div class="card">
                <div class="card-title">
                    <i class="fa-solid fa-clipboard-list"></i> Order History
                </div>
                <div class="table-wrapper">
                    <table>
                        <thead>
                            <tr>
                                <th>ID</th>
                                <th>Symbol</th>
                                <th>Side</th>
                                <th>Qty</th>
                                <th>Type</th>
                                <th>Limit Price</th>
                                <th>Status</th>
                            </tr>
                        </thead>
                        <tbody id="orders-body">
                            <tr>
                                <td colspan="7" style="text-align: center; color: var(--text-muted);">No orders submitted.</td>
                            </tr>
                        </tbody>
                    </table>
                </div>
            </div>

            <!-- Live Logs Terminal -->
            <div class="card">
                <div class="log-controls">
                    <div class="card-title" style="margin-bottom: 0;">
                        <i class="fa-solid fa-terminal"></i> Bot Engine Logs
                    </div>
                    <input type="text" class="log-search" id="log-search-input" placeholder="Filter logs...">
                </div>
                <div class="terminal-container" id="logs-container">
                    <!-- Logs populated dynamically -->
                </div>
            </div>
        </div>
    </div>

    <script>
        // Format Money
        const formatMoney = (val) => {
            return new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD' }).format(val);
        };

        // Initialize Equity Chart
        const ctx = document.getElementById('equityChart').getContext('2d');
        const equityChart = new Chart(ctx, {
            type: 'line',
            data: {
                labels: [],
                datasets: [{
                    label: 'Portfolio Value ($)',
                    data: [],
                    borderColor: '#6366f1',
                    borderWidth: 2.5,
                    backgroundColor: 'rgba(99, 102, 241, 0.08)',
                    fill: true,
                    tension: 0.35,
                    pointRadius: 2,
                    pointHoverRadius: 5,
                    pointBackgroundColor: '#6366f1'
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: { display: false }
                },
                scales: {
                    x: {
                        grid: { display: false },
                        ticks: { color: '#64748b', font: { family: 'Plus Jakarta Sans', size: 10 } }
                    },
                    y: {
                        grid: { color: 'rgba(255, 255, 255, 0.03)' },
                        ticks: { color: '#64748b', font: { family: 'Plus Jakarta Sans', size: 10 } }
                    }
                }
            }
        });

        // Store log cache to enable local filtering
        let logCache = [];

        // Log search filter listener
        document.getElementById('log-search-input').addEventListener('input', (e) => {
            filterLogs(e.target.value.toLowerCase());
        });

        const filterLogs = (query) => {
            const container = document.getElementById('logs-container');
            container.innerHTML = '';
            
            logCache.forEach(log => {
                if (log.message.toLowerCase().includes(query) || log.level.toLowerCase().includes(query)) {
                    appendLogToDOM(log);
                }
            });
        };

        const appendLogToDOM = (log) => {
            const container = document.getElementById('logs-container');
            const line = document.createElement('div');
            line.className = 'log-line';
            
            line.innerHTML = `
                <span class="log-time">${log.time}</span>
                <span class="log-level log-level-${log.level}">[${log.level}]</span>
                <span class="log-msg">${escapeHtml(log.message)}</span>
            `;
            container.appendChild(line);
        };

        const escapeHtml = (text) => {
            const map = {
                '&': '&amp;',
                '<': '&lt;',
                '>': '&gt;',
                '"': '&quot;',
                "'": '&#039;'
            };
            return text.replace(/[&<>"']/g, function(m) { return map[m]; });
        };

        // Poll Strategy API
        const pollStatus = async () => {
            try {
                const response = await fetch('/api/status');
                if (!response.ok) return;
                const data = await response.json();

                // Update Header
                document.getElementById('strategy-name').textContent = `${data.strategy_name} Monitor`;
                document.getElementById('uptime-text').textContent = `Uptime: ${data.uptime}`;

                // Update Metrics
                document.getElementById('portfolio-value').textContent = formatMoney(data.portfolio_value);
                document.getElementById('cash-value').textContent = formatMoney(data.cash);

                // Update PnL
                const pnlVal = document.getElementById('pnl-value');
                const pnlSubtext = document.getElementById('pnl-subtext');
                const pnlIcon = document.getElementById('pnl-icon');

                pnlVal.textContent = (data.pnl >= 0 ? '+' : '') + formatMoney(data.pnl);
                
                if (data.pnl >= 0) {
                    pnlVal.style.color = 'var(--accent-success)';
                    pnlSubtext.innerHTML = `<span class="trend-up"><i class="fa-solid fa-caret-up"></i> ${data.pnl_percent.toFixed(2)}%</span> since start`;
                    pnlIcon.className = 'card-icon icon-green';
                    pnlIcon.style.background = 'var(--accent-success-glow)';
                    pnlIcon.style.color = 'var(--accent-success)';
                } else {
                    pnlVal.style.color = 'var(--accent-danger)';
                    pnlSubtext.innerHTML = `<span class="trend-down"><i class="fa-solid fa-caret-down"></i> ${data.pnl_percent.toFixed(2)}%</span> since start`;
                    pnlIcon.className = 'card-icon';
                    pnlIcon.style.background = 'var(--accent-danger-glow)';
                    pnlIcon.style.color = 'var(--accent-danger)';
                }

                // Update Parameters panel
                const paramsContainer = document.getElementById('parameters-container');
                paramsContainer.innerHTML = '';
                if (Object.keys(data.parameters).length === 0) {
                    paramsContainer.innerHTML = '<div class="config-item"><span class="config-label">No params detected</span></div>';
                } else {
                    for (const [key, value] of Object.entries(data.parameters)) {
                        paramsContainer.innerHTML += `
                            <div class="config-item">
                                <span class="config-label">${key}</span>
                                <span class="config-val">${typeof value === 'object' ? JSON.stringify(value) : value}</span>
                            </div>
                        `;
                    }
                }

                // Update Chart
                if (data.chart.labels.length > 0) {
                    equityChart.data.labels = data.chart.labels;
                    equityChart.data.datasets[0].data = data.chart.data;
                    equityChart.update('none'); // Update without animation for performance
                }

                // Update Positions Table
                const posBody = document.getElementById('positions-body');
                posBody.innerHTML = '';
                if (data.positions.length === 0) {
                    posBody.innerHTML = '<tr><td colspan="7" style="text-align: center; color: var(--text-muted);">No open positions.</td></tr>';
                } else {
                    data.positions.forEach(pos => {
                        const isProfit = pos.pnl >= 0;
                        posBody.innerHTML += `
                            <tr>
                                <td><strong>${pos.symbol}</strong></td>
                                <td><span class="${pos.side.toLowerCase().includes('short') ? 'badge-sell' : 'badge-buy'}">${pos.side}</span></td>
                                <td>${pos.quantity}</td>
                                <td>${formatMoney(pos.avg_price)}</td>
                                <td>${formatMoney(pos.current_price)}</td>
                                <td>${formatMoney(pos.market_value)}</td>
                                <td style="color: ${isProfit ? 'var(--accent-success)' : 'var(--accent-danger)'}; font-weight: bold;">
                                    ${isProfit ? '+' : ''}${formatMoney(pos.pnl)} (${isProfit ? '+' : ''}${pos.pnl_percent.toFixed(2)}%)
                                </td>
                            </tr>
                        `;
                    });
                }

                // Update Orders Table
                const ordBody = document.getElementById('orders-body');
                ordBody.innerHTML = '';
                if (data.orders.length === 0) {
                    ordBody.innerHTML = '<tr><td colspan="7" style="text-align: center; color: var(--text-muted);">No orders submitted.</td></tr>';
                } else {
                    // Show last 6 orders
                    data.orders.slice(-6).reverse().forEach(ord => {
                        let statusTag = 'tag-active';
                        if (ord.status === 'filled') statusTag = 'tag-filled';
                        if (ord.status === 'canceled' || ord.status === 'rejected') statusTag = 'tag-canceled';

                        ordBody.innerHTML += `
                            <tr>
                                <td style="font-family: var(--font-mono); font-size: 11px; color: var(--text-muted);">${ord.id.slice(0, 8)}...</td>
                                <td><strong>${ord.symbol}</strong></td>
                                <td><span class="${ord.side.toLowerCase() === 'sell' ? 'badge-sell' : 'badge-buy'}">${ord.side.toUpperCase()}</span></td>
                                <td>${ord.quantity}</td>
                                <td>${ord.type}</td>
                                <td>${ord.limit_price ? formatMoney(ord.limit_price) : 'MKT'}</td>
                                <td><span class="tag ${statusTag}">${ord.status.toUpperCase()}</span></td>
                            </tr>
                        `;
                    });
                }

                // Update Logs
                const searchQuery = document.getElementById('log-search-input').value.toLowerCase();
                const container = document.getElementById('logs-container');
                const wasAtBottom = container.scrollHeight - container.clientHeight <= container.scrollTop + 50;
                
                logCache = data.logs;
                
                if (!searchQuery) {
                    container.innerHTML = '';
                    data.logs.forEach(log => appendLogToDOM(log));
                } else {
                    filterLogs(searchQuery);
                }

                // Scroll to bottom of logs if user was already near bottom
                if (wasAtBottom) {
                    container.scrollTop = container.scrollHeight;
                }

            } catch (err) {
                console.error("Failed to fetch status from strategy:", err);
            }
        };

        // Poll every 2 seconds
        setInterval(pollStatus, 2000);
        pollStatus(); // initial load
    </script>
</body>
</html>
"""
