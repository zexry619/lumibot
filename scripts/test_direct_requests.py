import os
import time
import hmac
import hashlib
import requests
from dotenv import load_dotenv

load_dotenv()
api_key = os.getenv('BINANCE_FUTURES_DEMO_API_KEY')
api_secret = os.getenv('BINANCE_FUTURES_DEMO_API_SECRET')

def sign_query(query_string, secret):
    return hmac.new(secret.encode('utf-8'), query_string.encode('utf-8'), hashlib.sha256).hexdigest()

def make_request(base_url, path, method="GET", params=None):
    if params is None:
        params = {}
    params['timestamp'] = int(time.time() * 1000)
    
    # Sort and encode parameters
    query_string = '&'.join([f"{k}={v}" for k, v in sorted(params.items())])
    signature = sign_query(query_string, api_secret)
    full_url = f"{base_url}{path}?{query_string}&signature={signature}"
    
    headers = {
        'X-MBX-APIKEY': api_key
    }
    
    try:
        if method == "GET":
            res = requests.get(full_url, headers=headers, timeout=10)
        else:
            res = requests.post(full_url, headers=headers, timeout=10)
        print(f"Request: {method} {path} -> Status Code: {res.status_code}")
        print(f"Response Headers: {dict(res.headers)}")
        print(f"Response Body: {res.text}")
    except Exception as e:
        print(f"Error: {e}")

print("--- Testing on demo-fapi.binance.com ---")
make_request("https://demo-fapi.binance.com", "/fapi/v2/account")
make_request("https://demo-fapi.binance.com", "/fapi/v1/userTrades", params={"symbol": "BTCUSDT", "limit": 1})

print("\n--- Testing on testnet.binancefuture.com ---")
make_request("https://testnet.binancefuture.com", "/fapi/v2/account")
make_request("https://testnet.binancefuture.com", "/fapi/v1/userTrades", params={"symbol": "BTCUSDT", "limit": 1})
