import pandas as pd
from ...FactorBase import FactorBase
from ..basic import ORIGINAL_CLOSE, ORIGINAL_LOW, ORIGINAL_HIGH, ORIGINAL_VOLUME


class O_AD(FactorBase):
    def __init__(self, frequency):
        super(O_AD, self).__init__(frequency)

    def compute(self, start_date=None, end_date=None):
        close = ORIGINAL_CLOSE(self.frequency)
        low = ORIGINAL_LOW(self.frequency)
        high = ORIGINAL_HIGH(self.frequency)
        vol = ORIGINAL_VOLUME(self.frequency)
        close_df = close.get_raw_value(start_date, end_date)
        low_df = low.get_raw_value(start_date, end_date)
        high_df = high.get_raw_value(start_date, end_date)
        vol_df = vol.get_raw_value(start_date, end_date)
        res = (((close_df - low_df) - (high_df - close_df)) / (high_df - low_df)) * vol_df
        if len(res) > 0:
            self.raw_value = res
        else:
            print('No new data!')
