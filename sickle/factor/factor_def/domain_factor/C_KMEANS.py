import pandas as pd
from ...FactorBase import FactorBase
from ..basic import ORIGINAL_CLOSE
from sklearn.cluster import KMeans
import empyrical
from numpy.lib.stride_tricks import as_strided as stride


class C_KMEANS(FactorBase):
    def __init__(self, frequency, n):
        super(C_KMEANS, self).__init__(frequency)
        self.N = n
        self.factor_name = self.factor_name + '_' + str(n)

    def compute(self, start_date=None, end_date=None):
        close = ORIGINAL_CLOSE(self.frequency)
        close_df = close.get_raw_value(start_date, end_date)
        barly_returns = close_df.pct_change(fill_method=None).dropna(how='all')
        res_df = pd.DataFrame()
        for i in range(self.N, len(barly_returns) + 1):
            temp_df = barly_returns.iloc[i-self.N:i, :].dropna(axis=1)
            net_value = empyrical.cum_returns(temp_df, starting_value=1)
            res = KMeans(n_clusters=3, random_state=9).fit_predict(net_value.T)
            df = pd.DataFrame(data=res, index=temp_df.columns, columns=[temp_df.index[-1]]).T
            res_df = pd.concat([res_df, df], axis=0)
        if len(res_df) > 0:
            self.raw_value = res_df
        else:
            print('No new data!')
