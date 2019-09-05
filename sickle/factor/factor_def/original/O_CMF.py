import pandas as pd
from ...FactorBase import FactorBase
from ..basic import ORIGINAL_CLOSE, ORIGINAL_LOW, ORIGINAL_HIGH, ORIGINAL_VOLUME


class O_CMF(FactorBase):
    def __init__(self, frequency, n):
        super(O_CMF, self).__init__(frequency)
        self.N = n
        self.factor_name = "{}_{}".format(self.factor_name, n)

    def compute(self, start_date=None, end_date=None):
        close = ORIGINAL_CLOSE(self.frequency)
        low = ORIGINAL_LOW(self.frequency)
        high = ORIGINAL_HIGH(self.frequency)
        vol = ORIGINAL_VOLUME(self.frequency)
        close_df = close.get_raw_value(start_date, end_date)
        low_df = low.get_raw_value(start_date, end_date)
        high_df = high.get_raw_value(start_date, end_date)
        vol_df = vol.get_raw_value(start_date, end_date)
        mfv = (((close_df - low_df) - (high_df - close_df)) / (high_df - low_df)) * vol_df
        cmf = mfv.rolling(self.N, min_periods=self.N).sum() / vol_df.rolling(self.N, min_periods=self.N).sum()
        cmf = cmf.dropna(how='all')
        if len(cmf) > 0:
            self.raw_value = cmf
        else:
            print('No new data!')
