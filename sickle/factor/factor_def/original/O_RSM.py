"""
收益信号动量， 过去R个交易日每日涨跌方向构建一个新型的动量因子，过去R个交易日上涨概率高于阈值做多，低于做空
这里定义因子为过去N个交易日日内涨幅大于0的概率
"""
import pandas as pd
from ...FactorBase import FactorBase
from ..basic import ORIGINAL_CLOSE, ORIGINAL_OPEN


class O_RSM(FactorBase):
    def __init__(self, frequency, n):
        super(O_RSM, self).__init__(frequency)
        self.N = n
        self.factor_name = self.factor_name + '_' + str(n)

    def compute(self, start_date=None, end_date=None):
        close = ORIGINAL_CLOSE(self.frequency)
        open = ORIGINAL_OPEN(self.frequency)
        close_df = close.get_raw_value(start_date, end_date)
        open_df = open.get_raw_value(start_date, end_date)
        df = close_df - open_df
        df[df > 0] = 1
        df[df <= 0] = 0
        res_df = df.rolling(self.N, min_periods=self.N).sum() / self.N
        if len(res_df) > 0:
            self.raw_value = res_df
        else:
            print('No new data!')
