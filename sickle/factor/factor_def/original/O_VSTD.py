import pandas as pd
from ...FactorBase import FactorBase
from ..basic import ORIGINAL_VOLUME
import datetime as dt


class O_VSTD(FactorBase):
    def __init__(self, frequency, n):
        super(O_VSTD, self).__init__(frequency)
        self.N = n
        self.factor_name = "{}_{}".format(self.factor_name, n)

    def compute(self, start_date=None, end_date=None):
        vol = ORIGINAL_VOLUME(self.frequency)
        if start_date is None:
            start_date = vol.factor_start()
        if end_date is None:
            end_date = vol.factor_end()
        if type(start_date) == str:
            rely_start_date = (dt.datetime.strptime(start_date, '%Y-%m-%d').date() - dt.timedelta(self.N * 2)).strftime('%Y-%m-%d')
        else:
            rely_start_date = (start_date - dt.timedelta(self.N * 2)).strftime('%Y-%m-%d')
        relay_lastest_day = str(vol.factor_end())
        if relay_lastest_day >= str(start_date):
            v = vol.get_raw_value(rely_start_date, end_date)
            res = v.rolling(self.N, min_periods=self.N).apply(lambda x: x.std() / x.mean())
            res = res.dropna(how='all')
            res = res[(res.index >= start_date) & (res.index <= end_date)]
            if len(res) > 0:
                self.raw_value = res
            else:
                print('No new data!')
