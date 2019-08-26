import pandas as pd
from ...FactorBase import FactorBase
from ..basic import TREND_CLOSE
import datetime as dt


class T_SKEWNESS(FactorBase):
    def __init__(self, frequency, trend, n):
        super(T_SKEWNESS, self).__init__(frequency)
        self.N = n
        self.trend = trend
        self.factor_name = "{}_t{}_{}".format(self.factor_name, trend, n)

    def compute(self, start_date=None, end_date=None):
        close = TREND_CLOSE(self.frequency, self.trend)
        if start_date is None:
            start_date = close.factor_start()
        if end_date is None:
            end_date = close.factor_end()
        if type(start_date) == str:
            rely_start_date = (dt.datetime.strptime(start_date, '%Y-%m-%d').date() - dt.timedelta(self.N * 2)).strftime('%Y-%m-%d')
        else:
            rely_start_date = (start_date - dt.timedelta(self.N * 2)).strftime('%Y-%m-%d')
        relay_lastest_day = str(close.factor_end())
        if relay_lastest_day >= start_date:
            c = close.get_raw_value(rely_start_date, end_date)
            returns = c.pct_change(fill_method=None).dropna(how='all')
            skewness = returns.rolling(self.N, min_periods=self.N // 2).skew()
            skewness = skewness[(skewness.index >= start_date) & (skewness.index <= end_date)]
            if len(skewness) > 0:
                self.raw_value = skewness
            else:
                print('No new data!')
