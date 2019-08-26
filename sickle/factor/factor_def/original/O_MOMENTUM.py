import pandas as pd
from ...FactorBase import FactorBase
from ..basic.ORIGINAL_CLOSE import ORIGINAL_CLOSE


class O_MOMENTUM(FactorBase):
    def __init__(self, frequency, n):
        super(O_MOMENTUM, self).__init__(frequency)
        self.N = n
        self.factor_name = self.factor_name + '_' + str(n)

    def compute(self, start_date=None, end_date=None):
        close = ORIGINAL_CLOSE(self.frequency)
        close_df = close.get_raw_value(start_date, end_date)
        change_ratio = close_df.pct_change(self.N, fill_method=None)
        if len(change_ratio) > 0:
            self.raw_value = change_ratio
        else:
            print('No new data!')
