import pandas as pd
from ...FactorBase import FactorBase
from ..basic import TREND_CLOSE


class T_MA_MOM(FactorBase):
    def __init__(self, frequency, trend, n):
        super(T_MA_MOM, self).__init__(frequency)
        self.N = n
        self.trend = trend
        self.factor_name = "{}_t{}_{}".format(self.factor_name, trend, n)

    def compute(self, start_date=None, end_date=None):
        close = TREND_CLOSE(self.frequency, self.trend)
        close.get_raw_value(start_date, end_date)
        n_list = [i+1 for i in range(self.N)]
        res_df = pd.DataFrame()
        for n in n_list[:-1]:
            ma_1 = close.ma(n)
            ma_2 = close.ma(n+1)
            sig = (ma_1 - ma_2) / abs(ma_1 - ma_2)
            if len(res_df) == 0:
                res_df = sig
            else:
                res_df = res_df + sig
        if len(res_df) > 0:
            self.raw_value = res_df
        else:
            print('No new data!')
