import pandas as pd
from .FactorAnalyser import FactorAnalyser
from .PortfolioPerformace import PortfolioPerformance
from .CalIdicator import CalIdicator
from tqdm import tqdm
import multiprocessing


class OneParamOptimizer:
    def __init__(self, factor, frequency, param_list):
        self.param_type_checker(frequency, param_list)
        self.frequency_list = frequency
        self.factor = factor
        self.param_list = param_list
        self.ana = FactorAnalyser()

    def param_type_checker(self, frequency, param):
        assert type(frequency) == list, "frequency is not list"
        assert type(param) == list, "frequency is not list"

    def cal_factor_and_check(self, side=1, start_date='2013-01-01', end_date='2018-05-01'):
        res_df = pd.DataFrame()
        pool = multiprocessing.Pool(processes=multiprocessing.cpu_count())
        for frequence in self.frequency_list:
            perf = PortfolioPerformance(frequence)
            for param in tqdm(self.param_list):
                factor = self.factor(frequence, param)
                fac_df = factor.get_raw_value(start_date=start_date, end_date=end_date)
                # 计算因子均值
                for count in [2, 4, 6, 8, 10]:
                    # 正常因子值算收益
                    name = "{}@{}@{}".format(factor.factor_name, side, count)
                    ls_pos = self.ana.factor_to_portfolio_ls(fac=fac_df, side=side, count=count, period=1)
                    net_value, turnover = perf.long_and_short_perf_optimize(ls_pos, 0.0001)
                    cal = CalIdicator(net_value, turnover)
                    perf_df = cal.profit_distribution()
                    perf_df = pd.DataFrame(perf_df.loc['annual'])
                    perf_df.columns = [name]
                    # 算平均后因子值收益
                    for ma_len in [3, 5, 10, 20, 40]:
                        fac_ma = factor.ma(ma_len)
                        name_ma = "{}@ma{}@{}@{}".format(factor.factor_name, ma_len, side, count)
                        ls_pos_ma = self.ana.factor_to_portfolio_ls(fac=fac_ma, side=side, count=count, period=1)
                        net_value_ma, turnover_ma = perf.long_and_short_perf_optimize(ls_pos_ma, 0.0001)
                        cal_ma = CalIdicator(net_value_ma, turnover_ma)
                        perf_df_ma = cal_ma.profit_distribution()
                        perf_df_ma = pd.DataFrame(perf_df_ma.loc['annual'])
                        perf_df_ma.columns = [name_ma]
                        res_df = pd.concat([res_df, perf_df.T, perf_df_ma.T], axis=0)
        res_df.to_csv("{}.csv".format(self.factor.__dict__['__module__'].split('.')[-1]))
        return res_df


class TwoParamOptimizer:
    def __init__(self, factor, frequency, param_list_1, param_list_2):
        self.param_type_checker(frequency, param_list_1)
        self.param_type_checker(frequency, param_list_2)
        self.frequency_list = frequency
        self.factor = factor
        self.param_list_1 = param_list_1
        self.param_list_2 = param_list_2
        self.ana = FactorAnalyser()

    def param_type_checker(self, frequency, param):
        assert type(frequency) == list, "frequency is not list"
        assert type(param) == list, "frequency is not list"

    def cal_factor_and_check(self, side=1, start_date='2013-01-01', end_date='2018-05-01'):
        res_df = pd.DataFrame()
        for frequence in self.frequency_list:
            perf = PortfolioPerformance(frequence)
            for param_1 in tqdm(self.param_list_1):
                for param_2 in self.param_list_2:
                    factor = self.factor(frequence, param_1, param_2)
                    fac_df = factor.get_raw_value(start_date=start_date, end_date=end_date)
                    for count in [2, 4, 6, 8, 10]:
                        name = "{}@{}@{}".format(factor.factor_name, side, count)
                        ls_pos = self.ana.factor_to_portfolio_ls(fac=fac_df, side=side, count=count, period=1)
                        if len(ls_pos) > 0:
                            net_value, turnover = perf.long_and_short_perf_optimize(ls_pos, 0.0001)
                            cal = CalIdicator(net_value, turnover)
                            perf_df = cal.profit_distribution()
                            perf_df = pd.DataFrame(perf_df.loc['annual'])
                            perf_df.columns = [name]
                            # 算平均后因子值收益
                            for ma_len in [3, 5, 10, 20, 40]:
                                fac_ma = factor.ma(ma_len)
                                name_ma = "{}@ma{}@{}@{}".format(factor.factor_name, ma_len, side, count)
                                ls_pos_ma = self.ana.factor_to_portfolio_ls(fac=fac_ma, side=side, count=count, period=1)
                                if len(ls_pos_ma) > 0:
                                    net_value_ma, turnover_ma = perf.long_and_short_perf_optimize(ls_pos_ma, 0.0001)
                                    cal_ma = CalIdicator(net_value_ma, turnover_ma)
                                    perf_df_ma = cal_ma.profit_distribution()
                                    perf_df_ma = pd.DataFrame(perf_df_ma.loc['annual'])
                                    perf_df_ma.columns = [name_ma]
                                    res_df = pd.concat([res_df, perf_df.T, perf_df_ma.T], axis=0)
        res_df.to_csv("{}.csv".format(self.factor.__dict__['__module__'].split('.')[-1]))
        return res_df
