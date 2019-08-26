import pandas as pd
from ...FactorBase import FactorBase
from ..basic import ORIGINAL_CLOSE
import datetime as dt


class O_KURTOSIS(FactorBase):
    def __init__(self, frequency, n):
        super(O_KURTOSIS, self).__init__(frequency)
        self.N = n
        self.factor_name = "{}_{}".format(self.factor_name, n)

    def compute(self, start_date=None, end_date=None):
        close = ORIGINAL_CLOSE(self.frequency)
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
            kurtosis = returns.rolling(self.N, min_periods=self.N // 2).kurt()
            kurtosis = kurtosis[(kurtosis.index >= start_date) & (kurtosis.index <= end_date)]
            if len(kurtosis) > 0:
                self.raw_value = kurtosis
            else:
                print('No new data!')
