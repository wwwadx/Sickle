"""
非流动性因子， illiq = 过去n天的（收益率绝对值/成交金额）的均值， illiq值越大表示流动性越差
"""
import pandas as pd
from ...FactorBase import FactorBase
from ..basic import ORIGINAL_CLOSE, ORIGINAL_AMT
import datetime as dt


class O_ILLIQ(FactorBase):
    def __init__(self, frequency, n):
        super(O_ILLIQ, self).__init__(frequency)
        self.N = n
        self.factor_name = "{}_{}".format(self.factor_name, n)

    def compute(self, start_date=None, end_date=None):
        close = ORIGINAL_CLOSE(self.frequency)
        amt = ORIGINAL_AMT(self.frequency)
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
            amount = amt.get_raw_value(rely_start_date, end_date)
            returns = c.pct_change(fill_method=None).dropna(how='all')
            temp = abs(returns) / amount
            illiq = temp.rolling(self.N, min_periods=self.N // 2).mean()
            illiq = illiq[(illiq.index >= start_date) & (illiq.index <= end_date)]
            if len(illiq) > 0:
                self.raw_value = illiq
            else:
                print('No new data!')
