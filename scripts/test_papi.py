import os
import ccxt
from dotenv import load_dotenv

load_dotenv()
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

demo_urls = exchange.urls.get('demo')
if demo_urls:
    api_urls = dict(exchange.urls.get('api') or {})
    api_urls.update(demo_urls)
    exchange.urls['api'] = api_urls

def test_endpoint(name, func, *args, **kwargs):
    try:
        if callable(func):
            res = func(*args, **kwargs)
            print(f"SUCCESS: {name} -> returns type {type(res).__name__}")
        else:
            print(f"NOT CALLABLE: {name}")
    except Exception as e:
        print(f"FAILED: {name} -> {type(e).__name__}: {e}")

test_endpoint("papiGetUmAccount", getattr(exchange, "papiGetUmAccount", None), {})
test_endpoint("papiV2GetUmAccount", getattr(exchange, "papiV2GetUmAccount", None), {})
test_endpoint("papi_get_account", getattr(exchange, "papi_get_account", None), {})
test_endpoint("papi_get_balance", getattr(exchange, "papi_get_balance", None), {})
test_endpoint("sapiGetPortfolioAccount", getattr(exchange, "sapiGetPortfolioAccount", None), {})
