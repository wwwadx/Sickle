import pandas as pd
from ...FactorBase import FactorBase
from ..basic.TREND_CLOSE import TREND_CLOSE


class T_MOMENTUM(FactorBase):
    def __init__(self, frequency, trend, n):
        super(T_MOMENTUM, self).__init__(frequency)
        self.N = n
        self.trend = trend
        self.factor_name = "{}_t{}_{}".format(self.factor_name, trend, n)

    def compute(self, start_date=None, end_date=None):
        close = TREND_CLOSE(self.frequency, self.trend)
        close_df = close.get_raw_value(start_date, end_date)
        change_ratio = close_df.pct_change(self.N, fill_method=None)
        if len(change_ratio) > 0:
            self.raw_value = change_ratio
        else:
            print('No new data!')
