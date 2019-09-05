import pandas as pd
import datetime as dt
from ..data_manager.db import *
from ..data_manager.local import H5Loader
import numpy as np


class FactorBase:
    """
    因子的基类
    """
    def __init__(self, frequency, db_name='cta_write'):
        self.factor_name = '{0}_{1}'.format(self.__class__.__name__, frequency)
        self.frequency = frequency
        self.raw_value = None
        self.contracts = []
        self.api = QueryApi(db_name)
        self.limit_contracts()

    def __add__(self, other):
        return self.raw_value + other.raw_value

    def __sub__(self, other):
        return self.raw_value - other.raw_value

    def __mul__(self, other):
        return self.raw_value * other.raw_value

    def __truediv__(self, other):
        res = self.raw_value / other.raw_value
        res.replace([np.inf, -np.inf], np.nan, inplace=True)
        return res

    def compute(self, start_date=None, end_date=None):
        raise NotImplementedError

    def update(self):
        table_name = self.factor_name
        factor_latest_day = self.api.latest_day(table_name)
        if factor_latest_day is None:
            factor_latest_day = dt.datetime(2004, 12, 31).date()
        current_day = dt.datetime.now().date()
        if current_day > factor_latest_day:
            start_date = factor_latest_day + dt.timedelta(1)
            self.compute(str(start_date), str(current_day))
            if self.raw_value is not None:
                if len(self.raw_value) > 0:
                    temp = pd.DataFrame(self.raw_value.stack())
                    temp.index.names = ['datetime', 'codes']
                    temp.columns = [self.factor_name]
                    self.api.copy_data_frame_to_pg(table_name, temp)
            else:
                print('No data to update!')

    def get_raw_value(self, start_date=None, end_date=None):
        if start_date is None:
            start_date = str(self.api.earliest_day(self.factor_name))
        if end_date is None:
            end_date = str(self.api.latest_day(self.factor_name))
        res_df = self.api.factor_data(self.factor_name, start_date, end_date, key_pivot=True)
        if res_df is not None:
            res_df.dropna(how='all', inplace=True)
            self.raw_value = res_df
        else:
            print('No data! Trying to update factor!')
            self.update()
        return self.raw_value

    def get_cached_factor(self, path):
        h5 = H5Loader(path)
        df = h5.load_cache(self.factor_name)
        if df is None:
            self.cache_factor(path)
            df = h5.load_cache(self.factor_name)
        return df

    def cache_factor(self, path):
        h5 = H5Loader(path)
        if self.raw_value is None:
            self.get_raw_value()
        h5.cache(self.raw_value, self.factor_name)

    def factor_start(self):
        return self.api.earliest_day(self.factor_name)

    def factor_end(self):
        return self.api.latest_day(self.factor_name)

    def limit_contracts(self):
        # contracts = all_contracts()
        # filter_contracts = ['ZZ500', 'HS300', 'SZ50', 'T', 'TF', 'IC', 'IF', 'IH']
        # contracts = [i.lower() for i in contracts if i not in filter_contracts]
        self.contracts = ['a', 'ag', 'al', 'ap', 'au', 'b', 'bu',
                          'bb', 'c', 'cs', 'cf', 'cu', 'er',
                          'fb', 'fg', 'fu', 'hc', 'i', 'j', 'jd',
                          'jm', 'jr', 'm', 'rb', 'l', 'lr', 'ma',
                          'me', 'ni', 'p', 'pb', 'pp', 'ro', 'rs',
                          'ru', 'sf', 'sm', 'sn', 'sr', 'ta',
                          'v', 'ws', 'y', 'zc', 'zn']

    def set_universe(self, contract_list):
        temp = [i for i in contract_list if i not in self.contracts]
        if len(temp) > 0:
            print('Contracts not in factor data: ', temp)
            return 0
        else:
            return self.raw_value[contract_list]

    def set_dynamic_universe(self, domain_factor, category):
        return self.raw_value[domain_factor == category].dropna(how='all')

    def filter_extreme_nd_standarize(self):
        """
        中位数去极值
        Args:

        Returns:

        """
        median = self.raw_value.median(axis=1)
        mad = abs(self.raw_value.fac.sub(median, axis=0)).median(axis=1)
        data = self.raw_value.fac.clip(median - 5.2 * mad, median + 5.2 * mad, axis=0)
        return data.sub(data.mean(axis=1), 0).div(data.std(1), 0).dropna(how='all')

    def ma(self, n):
        if self.raw_value is None:
            raise Exception("Load data first!!")
        return self.raw_value.rolling(n, min_periods=n).mean()


class PgFactor(FactorBase):
    def __init__(self, db_name, factor_name):
        super(PgFactor, self).__init__(db_name)
        self.factor_name = factor_name

    def compute(self, start_date=None, end_date=None):
        print('Fetch from pg No need to compute!')

    def update(self):
        print('Fetch from pg No need to update!')
