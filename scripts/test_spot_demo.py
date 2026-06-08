import os
import ccxt
from dotenv import load_dotenv

load_dotenv('/home/ubuntu/lumibot/.env')
api_key = os.getenv('BINANCE_FUTURES_DEMO_API_KEY')
api_secret = os.getenv('BINANCE_FUTURES_DEMO_API_SECRET')

print("Testing API Key on SPOT Demo...")
spot_exchange = ccxt.binance({
    'apiKey': api_key,
    'secret': api_secret,
    'enableRateLimit': True,
    'options': {
        'enableDemoTrading': True,
        'adjustForTimeDifference': True,
    },
})

try:
    balance = spot_exchange.fetch_balance()
    # Check if there are any balances with free funds > 0
    balances = {k: v for k, v in balance.get('total', {}).items() if float(v) > 0}
    print("SPOT Demo SUCCESS! Balances:", balances)
except Exception as e:
    print("SPOT Demo FAILED:", type(e).__name__, str(e))
