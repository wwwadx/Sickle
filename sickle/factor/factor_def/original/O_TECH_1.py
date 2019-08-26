import pandas as pd
from ...FactorBase import FactorBase
from ..basic import ORIGINAL_CLOSE, ORIGINAL_OPEN, ORIGINAL_HIGH, ORIGINAL_LOW, ORIGINAL_VOLUME


class O_TECH_1(FactorBase):
    def __init__(self, frequency):
        super(O_TECH_1, self).__init__(frequency)

    def compute(self, start_date=None, end_date=None):
        close = ORIGINAL_CLOSE(self.frequency)
        c = close.get_raw_value(start_date, end_date)
        open_p = ORIGINAL_OPEN(self.frequency)
        o = open_p.get_raw_value(start_date, end_date)
        high = ORIGINAL_HIGH(self.frequency)
        h = high.get_raw_value(start_date, end_date)
        low = ORIGINAL_LOW(self.frequency)
        l = low.get_raw_value(start_date, end_date)
        vol = ORIGINAL_VOLUME(self.frequency)
        v = vol.get_raw_value(start_date, end_date)
        up = c - o
        up[up > 0] = 1
        up[up <= 0] = -1
        down = c - o
        down[down < 0] = -1
        down[down >= 0] = 1
        m_up = up * (c - l)
        m_down = down * (h - c)
        p_1 = m_up - m_down
        if len(p_1) > 0:
            self.raw_value = p_1
        else:
            print('No new data!')
