import os
import ccxt
from dotenv import load_dotenv

load_dotenv()
api_key = os.getenv('BINANCE_FUTURES_DEMO_API_KEY')
api_secret = os.getenv('BINANCE_FUTURES_DEMO_API_SECRET')

print(f"Testing key {api_key[:10]}... on testnet.binancefuture.com endpoints")

exchange = ccxt.binanceusdm({
    'apiKey': api_key,
    'secret': api_secret,
    'enableRateLimit': True,
    'options': {
        'defaultType': 'future',
        'adjustForTimeDifference': True,
        'enableDemoTrading': False,
        'fetchCurrencies': False,
    },
})

# Manually override to testnet.binancefuture.com
exchange.urls['api'] = {
    'fapiPublic': 'https://testnet.binancefuture.com/fapi/v1',
    'fapiPublicV2': 'https://testnet.binancefuture.com/fapi/v2',
    'fapiPublicV3': 'https://testnet.binancefuture.com/fapi/v3',
    'fapiPrivate': 'https://testnet.binancefuture.com/fapi/v1',
    'fapiPrivateV2': 'https://testnet.binancefuture.com/fapi/v2',
    'fapiPrivateV3': 'https://testnet.binancefuture.com/fapi/v3',
}

def test_endpoint(name, func, *args, **kwargs):
    try:
        if callable(func):
            res = func(*args, **kwargs)
            print(f"SUCCESS: {name} -> returns type {type(res).__name__}")
        else:
            print(f"NOT CALLABLE: {name}")
    except Exception as e:
        print(f"FAILED: {name} -> {type(e).__name__}: {e}")

test_endpoint("fapiPrivateV2GetAccount", getattr(exchange, "fapiPrivateV2GetAccount", None), {})
test_endpoint("fapiPrivateGetUserTrades", getattr(exchange, "fapiPrivateGetUserTrades", None), {"symbol": "BTCUSDT"})
