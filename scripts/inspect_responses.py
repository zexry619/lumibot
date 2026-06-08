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

print("API Key prefix:", api_key[:10])

try:
    res = exchange.fapiPrivateGetPositionSideDual({})
    print("fapiPrivateGetPositionSideDual SUCCESS:", res)
except Exception as e:
    print("fapiPrivateGetPositionSideDual FAILED:", type(e).__name__, str(e))

try:
    res = exchange.fapiPrivateV2GetAccount({})
    print("fapiPrivateV2GetAccount SUCCESS:", res)
except Exception as e:
    print("fapiPrivateV2GetAccount FAILED:", type(e).__name__, str(e))
