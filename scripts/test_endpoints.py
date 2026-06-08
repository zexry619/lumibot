import os
import ccxt
from dotenv import load_dotenv

load_dotenv('/home/ubuntu/lumibot/.env')
api_key = os.getenv('BINANCE_FUTURES_DEMO_API_KEY')
api_secret = os.getenv('BINANCE_FUTURES_DEMO_API_SECRET')

exchange = ccxt.binanceusdm({
    'apiKey': api_key,
    'secret': api_secret,
    'enableRateLimit': True,
    'options': {
        'defaultType': 'future',
        'adjustForTimeDifference': True,
        'enableDemoTrading': True,
        'fetchCurrencies': False,
    },
})

# Mimic _use_binance_demo_urls
demo_urls = exchange.urls.get('demo')
if demo_urls:
    api_urls = dict(exchange.urls.get('api') or {})
    api_urls.update(demo_urls)
    exchange.urls['api'] = api_urls

def test_endpoint(name, func, *args):
    try:
        res = func(*args)
        print(f"SUCCESS: {name} returned response type {type(res).__name__}")
    except Exception as e:
        print(f"FAILED: {name} raised {type(e).__name__}: {e}")

test_endpoint("fapiPrivateV2GetAccount", exchange.fapiPrivateV2GetAccount, {})
test_endpoint("fapiPrivateGetPositionSideDual", exchange.fapiPrivateGetPositionSideDual, {})
test_endpoint("fapiPrivateGetIncome", exchange.fapiPrivateGetIncome, {})
test_endpoint("fapiPrivateGetUserTrades", exchange.fapiPrivateGetUserTrades, {"symbol": "BTCUSDT"})
