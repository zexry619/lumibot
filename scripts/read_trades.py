import json

try:
    with open('/home/ubuntu/lumibot/data/binance_futures_demo_ai_trader_state.json') as f:
        data = json.load(f)
except Exception as e:
    print('Failed to load state file:', e)
    exit(1)

reviews = data.get('trade_reviews', [])
print(f'Total trade reviews: {len(reviews)}')
print('Last 5 trade reviews:')
for r in reviews[-5:]:
    print(f"  Symbol: {r.get('symbol')}, Side: {r.get('side')}, Exit: {r.get('exit_reason')}")
    print(f"  Review: {r.get('review')}")
    print(f"  Lesson: {r.get('lesson')}")
    print("-" * 50)
