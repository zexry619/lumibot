import ccxt
exchange = ccxt.binanceusdm()
print('API URLs:', exchange.urls.get('api'))
print('Demo URLs:', exchange.urls.get('demo'))
