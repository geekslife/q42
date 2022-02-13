from datetime import datetime, timezone
from time import sleep

import calendar
import json
import pandas as pd
import requests


def ts2dt(ts):
    return datetime.fromtimestamp(ts / 1000, timezone.utc)


def dt2ts(date):
    return int(calendar.timegm(date.timetuple()) * 1000 + date.microsecond / 1000)


def get_interval(interval):
    for iv in ['1m', '1h', '4h', '6h', '1d']:
        if pd.Timedelta(interval) == pd.Timedelta(iv):
            return iv
    raise ValueError("unknown interval")


def fetch_klines(symbol, startTime: datetime, endTime: datetime = None, interval='1d', market='SPOT', debug=True,
                 limit=None):
    if market == 'SPOT':
        endpoint = 'https://api.binance.com/api/v3/klines'
        limit = 1000 if limit is None else limit
    elif market == 'UMFUTURES':
        endpoint = 'https://fapi.binance.com/fapi/v1/klines'
        limit = 1500 if limit is None else limit

    endTime = datetime.today().astimezone(timezone.utc) if endTime is None else endTime

    params = {
        "symbol": symbol,
        "interval": interval if interval != '1min' else '1m',
        "startTime": dt2ts(startTime),
        "endTime": dt2ts(endTime),
        "limit": limit
    }

    next = startTime

    while next.timestamp() <= endTime.timestamp():
        resp = requests.get(endpoint, params=params)
        result = json.loads(resp.text)
        if debug:
            print(
                f"fetch_klines({ts2dt(params['startTime'])}-{ts2dt(params['endTime']) if 'endTime' in params else ''}):[{market}]{params} ")
        if "code" in result:
            print("retry...", result)
            sleep(10)
        else:
            for it in result:
                current = ts2dt(it[0])
                if current.timestamp() > endTime.timestamp():
                    break
                yield it
            next = current + pd.Timedelta(interval)
            params['startTime'] = dt2ts(next)


def spot_klines(symbol, interval='1d', startTime: datetime = None, endTime: datetime = None, limit=500, debug=True):
    return fetch_klines(symbol, startTime, endTime, interval, 'SPOT', debug, limit)


def futures_klines(symbol, interval='1d', startTime: datetime = None, endTime: datetime = None, limit=1500, debug=True):
    return fetch_klines(symbol, startTime, endTime, interval, 'UMFUTURES', debug, limit)
