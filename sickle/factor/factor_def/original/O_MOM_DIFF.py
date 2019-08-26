import pandas as pd
from ...FactorBase import FactorBase
from .O_MOMENTUM import O_MOMENTUM


class O_MOM_DIFF(FactorBase):
    def __init__(self, frequency, n):
        super(O_MOM_DIFF, self).__init__(frequency)
        self.N = n
        self.factor_name = self.factor_name + '_' + str(n)

    def compute(self, start_date=None, end_date=None):
        mom_s = O_MOMENTUM(self.frequency, int(self.N / 2))
        mom_l = O_MOMENTUM(self.frequency, self.N)
        s_df = mom_s.get_raw_value(start_date, end_date)
        l_df = mom_l.get_raw_value(start_date, end_date)
        res_df = s_df - l_df
        res_df = res_df.dropna(how='all')
        if len(res_df) > 0:
            self.raw_value = res_df
        else:
            print('No new data!')
