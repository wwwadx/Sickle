import pandas as pd
from ...FactorBase import FactorBase
from .ORIGINAL_CLOSE import ORIGINAL_CLOSE
import datetime as dt


class TREND_CLOSE(FactorBase):
    def __init__(self, frequency, n):
        super(TREND_CLOSE, self).__init__(frequency)
        self.frequency = frequency
        self.N = n
        self.factor_name = self.factor_name + '_' + str(n)

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
        if relay_lastest_day >= str(start_date):
            close_current = close.get_raw_value(rely_start_date, end_date)
            c_ma_current = close_current.rolling(self.N, min_periods=self.N).mean()
            c_ma_yesterday = c_ma_current.shift(1)
            close_yesterday = close_current.shift(1)
            cond_1 = (close_yesterday < c_ma_yesterday) & (close_current > c_ma_current)
            cond_2 = (close_yesterday > c_ma_yesterday) & (close_current < c_ma_current)
            cond_3 = (close_yesterday > c_ma_yesterday) & (close_current > c_ma_current) & (close_current > close_yesterday)
            cond_4 = (close_yesterday > c_ma_yesterday) & (close_current > c_ma_current) & (close_current < close_yesterday)
            cond_5 = (close_yesterday < c_ma_yesterday) & (close_current < c_ma_current) & (close_current > close_yesterday)
            cond_6 = (close_yesterday < c_ma_yesterday) & (close_current < c_ma_current) & (close_current < close_yesterday)
            cond = cond_1 * 1 - cond_2 * 1 + cond_3 * 1 + cond_4 * 0 + cond_5 * 0 - cond_6 * 1
            cond = cond.dropna(how='all')
            cond = cond[(cond.index >= start_date) & (cond.index <= end_date)]
            cond = cond.cumsum()
            if len(cond) > 0:
                self.raw_value = cond
            else:
                print('No new data!')
