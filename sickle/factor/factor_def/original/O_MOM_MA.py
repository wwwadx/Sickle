import pandas as pd
from ...FactorBase import FactorBase
from ..basic.ORIGINAL_CLOSE import ORIGINAL_CLOSE


class O_MOM_MA(FactorBase):
    def __init__(self, frequency, n):
        super(O_MOM_MA, self).__init__(frequency)
        self.N = n
        self.factor_name = self.factor_name + '_' + str(n)

    def compute(self, start_date=None, end_date=None):
        close = ORIGINAL_CLOSE(self.frequency)
        close_df = close.get_raw_value(start_date, end_date)
        close_ma_df = close.ma(self.N).dropna(how='all')
        res = (close_df - close_ma_df) / close_ma_df
        res = res.dropna(how='all')
        if len(res) > 0:
            self.raw_value = res
        else:
            print('No new data!')
