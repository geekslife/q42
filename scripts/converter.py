import os
import fire
import pandas as pd
import glob

from datetime import timedelta
from os.path import expanduser


def pad_dataframe(df, sdt, edt):
    _df = df.copy()

    head = list(pd.date_range(sdt, _df.iloc[0]['date'], closed='left'))
    for dt in head:
        #         print('head:',dt)
        _df = _df.append({'date': dt}, ignore_index=True)
    _df = _df.sort_values(by='date', ignore_index=True).bfill()

    tail = list(pd.date_range(_df.iloc[-1]['date'], edt, closed='right'))
    for dt in tail:
        #         print('tail:',dt)
        _df = _df.append({'date': dt}, ignore_index=True)
    _df = _df.sort_values(by='date', ignore_index=True).ffill()

    return _df



class Main:
    @staticmethod
    def split_csv_update(source_dir, target_dir, out_dir=None):
        """
        target_dir/MMDD/*.csv 파일의 last_date 를 읽어와서,
        source_dir/*.csv 파일에서 date>last_date+1 인 데이터를 추출해서
        target_dir/MMDD/*.csv 파일에 append한다.
        """
        out_dir = target_dir if out_dir is None else out_dir

        for i, path in enumerate(glob.glob(expanduser(f'{target_dir}/*/*.csv'))):
            filename = path.split('/')[-1]
            hour=int(path.split('/')[-2][:2])
            minute=int(path.split('/')[-2][2:])

            target_df = pd.read_csv(path, index_col=['date'], parse_dates=True)
            #print(f'source_df path: {source_dir}/{filename}, target_df path: {path}')

            source_df = pd.read_csv(f'{source_dir}/{filename}', index_col=['date'], parse_dates=True, usecols=['date','open','high','low','close','volume'])
            source_df['close'] = source_df.close.shift(-1439)
            source_df['high'] = source_df.high.rolling(1440).max().shift(-1439)
            source_df['low'] = source_df.low.rolling(1440).min().shift(-1439)
            source_df['volume'] = source_df.volume.rolling(1440).sum().shift(-1439)
            source_df.dropna(inplace=True)
            source_df = source_df[(source_df.index.minute == minute)&(source_df.index.hour==hour)]
            source_df = source_df[source_df.index.date>target_df.index.date.max()]

            if source_df.empty:
                print(f'nothing to update: {path}')
                continue

            if source_df.index.date[0]>target_df.index.date[-1]+timedelta(days=1):
                print(f'skip: cannot find data {target_df.index.date[-1]+timedelta(days=1)}~{source_df.index.date[0]-timedelta(days=1)}')
                continue

            _out_dir = f'{out_dir}/{hour:02}{minute:02}'
            os.makedirs(_out_dir, exist_ok=True)
            merged = pd.concat([target_df, source_df]).reset_index()
            #print("merged:\n",merged.tail(20))
            merged.to_csv(f'{_out_dir}/{filename}', index=False, date_format='%Y-%m-%d')

    @staticmethod
    def split_csv_all(source_dir, target_dir):
        gl = glob.glob(expanduser(f'{source_dir}/*.csv'))
        dates_by_filename = {}

        for i, path in enumerate(gl):
            filename = path.split('/')[-1]
            print(f'processing {filename} ({i + 1}/{len(gl)})')

            df = pd.read_csv(path, index_col=['date'], parse_dates=True)
            df['close'] = df.close.shift(-1439)
            df['high'] = df.high.rolling(1440).max().shift(-1439)
            df['low'] = df.low.rolling(1440).min().shift(-1439)
            df['volume'] = df.volume.rolling(1440).sum().shift(-1439)
            hour = df.index.hour
            minute = df.index.minute
            for h in range(24):
                for m in range(60):
                    os.makedirs(f'{target_dir}/{h:02}{m:02}', exist_ok=True)
                    path = expanduser(f'{target_dir}/{h:02}{m:02}/{filename}')
                    if os.path.exists(path):
                        continue
                    sub_df = df.iloc[(hour == h) & (minute == m)].reset_index()
                    sub_df.dropna().to_csv(
                        path,
                        index=False, \
                        date_format='%Y-%m-%d',
                        columns=['date', 'open', 'high', 'low', 'close', 'volume']
                    )

    @staticmethod
    def min2day_update(source_dir, qlib_dir):
        for h in range(24):
            for m in range(60):
                _qlib_dir = f'{qlib_dir}/{h:02}{m:02}'
                if os.path.exists(_qlib_dir):
                    continue
                os.makedirs(_qlib_dir, exist_ok=True)
                cmd = f'python dump_bin.py dump_update --csv_path {source_dir}{h:02}{m:02} --qlib_dir {_qlib_dir} --freq day --date_field_name date --include_fields open,high,low,close,volume'
                os.system(cmd)

    @staticmethod
    def min2day_all(source_dir, qlib_dir):
        for h in range(24):
            for m in range(60):
                _qlib_dir = f'{qlib_dir}/{h:02}{m:02}'
                if os.path.exists(_qlib_dir):
                    continue
                os.makedirs(_qlib_dir, exist_ok=True)
                cmd = f'python dump_bin.py dump_all --csv_path {source_dir}{h:02}{m:02} --qlib_dir {_qlib_dir} --freq day --date_field_name date --include_fields open,high,low,close,volume'
                os.system(cmd)

    @staticmethod
    def normalize_dir(qlib_dir):
        for h in range(24):
            for m in range(60):
                _qlib_dir = expanduser(f'{qlib_dir}/{h:02}{m:02}')
                if not os.path.exists(_qlib_dir):
                    print(f'not exists: {_qlib_dir}')
                    continue
                cmd = f'python collector.py normalize_data --source_dir {_qlib_dir} --normalize_dir {_qlib_dir}'
                print(cmd)
                os.system(cmd)

if __name__ == '__main__':
    fire.Fire(Main)
