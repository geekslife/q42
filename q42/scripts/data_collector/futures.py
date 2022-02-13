import fire
import requests
import json
import sys

from pathlib import Path

CUR_DIR = Path(__file__).resolve().parent
sys.path.append(str(CUR_DIR.parent))


def get_symbols():
    url = 'https://fapi.binance.com/fapi/v1/exchangeInfo'
    symbols = json.loads(requests.get(url).text)['symbols']
    symbols = list(filter(lambda x: x['status'] == 'TRADING' and x['contractType'] == 'PERPETUAL', symbols))
    return [it['symbol'] for it in symbols]


class Main:
    @staticmethod
    def tickers():
        url = 'https://fapi.binance.com/fapi/v1/exchangeInfo'
        symbols = json.loads(requests.get(url).text)['symbols']
        symbols = list(filter(lambda x: x['status'] == 'TRADING' and x['contractType'] == 'PERPETUAL', symbols))
        return [it['symbol'] for it in symbols]


if __name__ == '__main__':
    fire.Fire(Main)
