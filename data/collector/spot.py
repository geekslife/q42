import json

import requests


def get_symbols():
    url='https://api1.binance.com/api/v3/exchangeInfo'
    symbols=json.loads(requests.get(url).text)['symbols']
    data=filter(lambda it: (it['symbol'].endswith('USDT') and it['status']=='TRADING'), symbols)
    return [it['symbol'] for it in data]
