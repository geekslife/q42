import fire
import sys
import pandas as pd

from loguru import logger
from abc import ABC, ABCMeta
from pathlib import Path

CUR_DIR = Path(__file__).resolve().parent
sys.path.append(str(CUR_DIR.parent.parent))

from data_collector.futures import get_symbols
from helper import futures_klines, ts2dt
from data_collector.base import BaseCollector, BaseNormalize, BaseRun


class BinanceCollector(BaseCollector):
    def __init__(
            self,
            save_dir: [str, Path],
            start=None,
            end=None,
            interval="1d",
            max_workers=1,
            max_collector_count=2,
            delay=1,  # delay need to be one
            check_data_length: int = None,
            limit_nums: int = None,
    ):
        """

        Parameters
        ----------
        save_dir: str
            crypto save dir
        max_workers: int
            workers, default 4
        max_collector_count: int
            default 2
        delay: float
            time.sleep(delay), default 0
        interval: str
            freq, value from [1min, 1d], default 1min
        start: str
            start datetime, default None
        end: str
            end datetime, default None
        check_data_length: int
            check data length, if not None and greater than 0, each symbol will be considered complete if its data length is greater than or equal to this value, otherwise it will be fetched again, the maximum number of fetches being (max_collector_count). By default None.
        limit_nums: int
            using for debug, by default None
        """
        super(BinanceCollector, self).__init__(
            save_dir=save_dir,
            start=start,
            end=end,
            interval=interval,
            max_workers=max_workers,
            max_collector_count=max_collector_count,
            delay=delay,
            check_data_length=check_data_length,
            limit_nums=limit_nums,
        )

    def get_instrument_list(self):
        logger.info("get crypto symbols......")
        symbols = get_symbols()
        logger.info(f"get {len(symbols)} symbols.")
        return symbols

    @staticmethod
    def get_data_from_remote(symbol, interval, start, end):
        error_msg = f"{symbol}-{interval}-{start}-{end}"
        names = ['date', 'open', 'high', 'low', 'close', 'volume']
        datefmt = '%Y-%m-%d' if interval == '1d' else '%Y-%m-%d %H:%M:%S'
        try:
            data = list(futures_klines(symbol, startTime=start, endTime=end, interval=interval))
            df = pd.DataFrame(data, dtype=str)
            df = df.drop(df.columns[len(names):], axis=1)
            df.columns = names
            df['date'] = df['date'].map(lambda x: ts2dt(int(x)).strftime(datefmt))
            return df
        except Exception as e:
            logger.warning(f"{error_msg}:{e}")

    def get_data(
            self, symbol: str, interval: str, start_datetime: pd.Timestamp, end_datetime: pd.Timestamp
    ) -> [pd.DataFrame]:
        def _get_simple(start_, end_):
            self.sleep()
            _remote_interval = interval
            return self.get_data_from_remote(
                symbol,
                interval=_remote_interval,
                start=start_,
                end=end_,
            )

        if interval in (self.INTERVAL_1d, self.INTERVAL_1h, self.INTERVAL_1min):
            _result = _get_simple(start_datetime, end_datetime)
        else:
            raise ValueError(f"cannot support {interval}")
        return _result


class BinanceCollector1d(BinanceCollector, ABC):

    def normalize_symbol(self, symbol):
        return symbol


class BinanceCollector1h(BinanceCollector, ABC):

    def normalize_symbol(self, symbol):
        return symbol


class BinanceCollector1min(BinanceCollector, ABC):

    def normalize_symbol(self, symbol):
        return symbol


class BinanceNormalize(BaseNormalize, metaclass=ABCMeta):
    @staticmethod
    def normalize_crypto(
            df: pd.DataFrame,
            calendar_list: list = None,
            date_field_name: str = "date",
            symbol_field_name: str = "symbol",
    ):
        if df.empty:
            return df
        df = df.copy()
        df.set_index(date_field_name, inplace=True)
        df.index = pd.to_datetime(df.index)
        df = df[~df.index.duplicated(keep="first")]
        if calendar_list is not None:
            df = df.reindex(
                pd.DataFrame(index=calendar_list)
                    .loc[
                pd.Timestamp(df.index.min()).date(): pd.Timestamp(df.index.max()).date()
                                                     + pd.Timedelta(hours=23, minutes=59)
                ]
                    .index
            )
        df.sort_index(inplace=True)

        df.index.names = [date_field_name]
        return df.reset_index()

    def normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        df = self.normalize_crypto(df, self._calendar_list, self._date_field_name, self._symbol_field_name)
        return df


class BinanceNormalize1d(BinanceNormalize, metaclass=ABCMeta):
    def _get_calendar_list(self):
        return None


class BinanceNormalize1h(BinanceNormalize, metaclass=ABCMeta):
    def _get_calendar_list(self):
        return None


class BinanceNormalize1min(BinanceNormalize, metaclass=ABCMeta):
    def _get_calendar_list(self):
        return None


class Run(BaseRun):
    def __init__(self, source_dir=None, normalize_dir=None, max_workers=1, interval="1d"):
        super().__init__(source_dir, normalize_dir, max_workers, interval)

    @property
    def collector_class_name(self):
        return f"BinanceCollector{self.interval}"

    @property
    def default_base_dir(self) -> [Path, str]:
        return CUR_DIR

    @property
    def normalize_class_name(self):
        return f"BinanceNormalize{self.interval}"

    def download_data(
            self,
            max_collector_count=2,
            delay=0,
            start=None,
            end=None,
            check_data_length: int = None,
            limit_nums=None,
    ):
        """download data from Internet

        Parameters
        ----------
        max_collector_count: int
            default 2
        delay: float
            time.sleep(delay), default 0
        interval: str
            freq, value from [1min, 1d], default 1d, currently only supprot 1d
        start: str
            start datetime, default "2000-01-01"
        end: str
            end datetime, default ``pd.Timestamp(datetime.datetime.now() + pd.Timedelta(days=1))``
        check_data_length: int # if this param useful?
            check data length, if not None and greater than 0, each symbol will be considered complete if its data length is greater than or equal to this value, otherwise it will be fetched again, the maximum number of fetches being (max_collector_count). By default None.
        limit_nums: int
            using for debug, by default None

        Examples
        ---------
            # get daily data
            $ python collector.py download_data --source_dir ~/.qlib/crypto_data/source/1d --start 2015-01-01 --end 2021-11-30 --delay 1 --interval 1d
        """

        start += ' 00:00:00'
        end += ' 23:59:59'
        super(Run, self).download_data(max_collector_count, delay, start, end, self.interval, check_data_length,
                                       limit_nums)

    def normalize_data(self, date_field_name: str = "date", symbol_field_name: str = "symbol"):
        """normalize data

        Parameters
        ----------
        date_field_name: str
            date field name, default date
        symbol_field_name: str
            symbol field name, default symbol

        Examples
        ---------
            $ python collector.py normalize_data --source_dir ~/.qlib/crypto_data/source/1d --normalize_dir ~/.qlib/crypto_data/source/1d_nor --interval 1d --date_field_name date
        """
        super(Run, self).normalize_data(date_field_name, symbol_field_name)


if __name__ == "__main__":
    fire.Fire(Run)
