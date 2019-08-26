"""
收益信号动量， 过去R个交易日每日涨跌方向构建一个新型的动量因子，过去R个交易日上涨概率高于阈值做多，低于做空
这里定义因子为过去N个交易日日内涨幅大于0的概率
"""
import pandas as pd
from ...FactorBase import FactorBase
from ..basic import TREND_CLOSE


class T_RSM(FactorBase):
    def __init__(self, frequency, trend, n):
        super(T_RSM, self).__init__(frequency)
        self.N = n
        self.trend = trend
        self.factor_name = "{}_t{}_{}".format(self.factor_name, trend, n)

    def compute(self, start_date=None, end_date=None):
        close = TREND_CLOSE(self.frequency, self.trend)
        close_df = close.get_raw_value(start_date, end_date)
        close_before = close_df.shift()
        df = close_df - close_before
        df[df > 0] = 1
        df[df <= 0] = 0
        res_df = df.rolling(self.N, min_periods=self.N).sum() / self.N
        if len(res_df) > 0:
            self.raw_value = res_df
        else:
            print('No new data!')
