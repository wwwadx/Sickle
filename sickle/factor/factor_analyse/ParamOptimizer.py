import pandas as pd
from .FactorAnalyser import FactorAnalyser
from .PortfolioPerformace import PortfolioPerformance
from .CalIdicator import CalIdicator
from tqdm import tqdm
import xlwt
import os
from ..domians.normal_domian_1 import domain_dict


class OneParamOptimizer:
    def __init__(self, factor, frequency, param_list, domain=domain_dict, holding_period_list=None, holding_num_list=None, cost=0.0001, netvalue_in_res=False):
        """
        因子本身参数只有一个的参数优化
        :param factor: 因子类名
        :param frequency: list， e.g. ['5min']
        :param param_list: list, e.g. [5, 10, 15, 20, 40, 60]
        :param holding_num_list: 默认是[2, 4, 6, 8, 10]
        :param ma_list: 默认是 [3, 5, 10, 20, 40]
        :param cost: 默认是 0.0001
        """
        self.param_type_checker(frequency, param_list)
        self.frequency_list = frequency
        self.factor = factor
        self.param_list = param_list
        self.domain = domain
        self.netvalue_in_res = netvalue_in_res
        if holding_num_list is not None:
            self.holding_num_list = holding_num_list
        else:
            self.holding_num_list = [2, 4, 6, 8, 10]
        if holding_period_list is not None:
            self.holding_period_list = holding_period_list
        else:
            self.holding_period_list = [1, 5, 10, 20]
        self.cost = cost
        self.ana = FactorAnalyser()

    def param_type_checker(self, frequency, param):
        assert type(frequency) == list, "frequency is not list"
        assert type(param) == list, "frequency is not list"

    def cal_factor_res(self, name, fac_df, side, count, perf, period):
        ls_pos = self.ana.factor_to_portfolio_ls(fac=fac_df, side=side, count=count, period=period)
        net_value, turnover = perf.long_and_short_perf_optimize_with_numba(ls_pos, self.cost)
        cal = CalIdicator(net_value, turnover)
        perf_df = cal.profit_distribution()
        perf_df = pd.DataFrame(perf_df.loc['annual'])
        perf_df.columns = [name]
        net_value = pd.DataFrame(net_value)
        net_value.columns = [name]
        return perf_df, net_value

    def cal_factor_and_check(self, side=1, start_date='2013-01-01', end_date='2018-05-01'):
        domain_category = list(self.domain.keys())
        domain_category.append('all')
        for single_domain in domain_category:
            done = []
            res_df = pd.DataFrame()
            net_value_df = pd.DataFrame()
            for frequence in self.frequency_list:
                perf = PortfolioPerformance(frequence)
                for param in self.param_list:
                    factor = self.factor(frequence, param)
                    fac_df = factor.get_raw_value(start_date=start_date, end_date=end_date)
                    # 限定分域品种
                    if single_domain != 'all':
                        fac_df = factor.set_universe(self.domain[single_domain])
                    for count in self.holding_num_list:
                        for period in self.holding_period_list:
                            # 正常因子值算收益
                            name = "{}@{}@{}@{}@{}".format(factor.factor_name, single_domain, side, count, period)
                            if name not in done:
                                perf_df, net_value = self.cal_factor_res(name, fac_df, side, count, perf, period)
                                res_df = pd.concat([res_df, perf_df.T], axis=0)
                                if self.netvalue_in_res:
                                    net_value_df = pd.concat([net_value_df, net_value], axis=1)
                                done.append(name)
                                print(name)
            if not os.path.exists("{}@side{}.xlsx".format(self.factor.__dict__['__module__'].split('.')[-1], side)):
                with pd.ExcelWriter("{}@side{}.xlsx".format(self.factor.__dict__['__module__'].split('.')[-1], side),
                                    mode='w') as writer:
                    res_df.to_excel(writer, sheet_name=single_domain)
                    if self.netvalue_in_res:
                        net_value_df.to_excel(writer, sheet_name='{}_net_value'.format(single_domain))
            else:
                with pd.ExcelWriter("{}@side{}.xlsx".format(self.factor.__dict__['__module__'].split('.')[-1], side), mode='a') as writer:
                    res_df.to_excel(writer, sheet_name=single_domain)
                    if self.netvalue_in_res:
                        net_value_df.to_excel(writer, sheet_name='{}_net_value'.format(single_domain))


class TwoParamOptimizer:
    def __init__(self, factor, frequency, param_list_1, param_list_2, domain=domain_dict, holding_period_list=None, holding_num_list=None, cost=0.0001):
        """
        因子本身参数有两个的参数优化
        :param factor: 因子类名
        :param frequency: list， e.g. ['5min']
        :param param_list_1: list
        :param param_list_2: list, e.g. [5, 10, 15, 20, 40, 60]
        :param holding_num_list: 默认是[2, 4, 6, 8, 10]
        :param ma_list: 默认是 [3, 5, 10, 20, 40]
        :param cost: 默认是 0.0001
        """
        self.param_type_checker(frequency, param_list_1)
        self.param_type_checker(frequency, param_list_2)
        self.frequency_list = frequency
        self.factor = factor
        self.domain = domain
        if holding_num_list is not None:
            self.holding_num_list = holding_num_list
        else:
            self.holding_num_list = [2, 4, 6, 8, 10]
        if holding_period_list is not None:
            self.holding_period_list = holding_period_list
        else:
            self.holding_period_list = [1, 5, 10, 20]
        self.param_list_1 = param_list_1
        self.param_list_2 = param_list_2
        self.cost = cost
        self.ana = FactorAnalyser()

    def param_type_checker(self, frequency, param):
        assert type(frequency) == list, "frequency is not list"
        assert type(param) == list, "frequency is not list"

    def cal_factor_res(self, name, fac_df, side, count, perf, period):
        ls_pos = self.ana.factor_to_portfolio_ls(fac=fac_df, side=side, count=count, period=period)
        if len(ls_pos) > 0:
            net_value, turnover = perf.long_and_short_perf_optimize_with_numba(ls_pos, self.cost)
            cal = CalIdicator(net_value, turnover)
            perf_df = cal.profit_distribution()
            perf_df = pd.DataFrame(perf_df.loc['annual'])
            perf_df.columns = [name]
            net_value = pd.DataFrame(net_value)
            net_value.columns = [name]
        else:
            perf_df = pd.DataFrame()
            net_value = pd.DataFrame()
        return perf_df, net_value

    def cal_factor_and_check(self, side=1, start_date='2013-01-01', end_date='2018-05-01'):
        domain_category = list(self.domain.keys())
        domain_category.append('all')
        for single_domain in domain_category:
            res_df = pd.DataFrame()
            net_value_df = pd.DataFrame()
            done = []
            for frequence in self.frequency_list:
                perf = PortfolioPerformance(frequence)
                for param_1 in tqdm(self.param_list_1):
                    for param_2 in self.param_list_2:
                        factor = self.factor(frequence, param_1, param_2)
                        fac_df = factor.get_raw_value(start_date=start_date, end_date=end_date)
                        # 限定分域品种
                        if single_domain != 'all':
                            fac_df = factor.set_universe(self.domain[single_domain])
                        for count in self.holding_num_list:
                            for period in self.holding_period_list:
                                name = "{}@{}@{}@{}@{}".format(factor.factor_name, single_domain, side, count, period)
                                if name not in done:
                                    perf_df, net_value = self.cal_factor_res(name, fac_df, side, count, perf, period)
                                    res_df = pd.concat([res_df, perf_df.T], axis=0)
                                    net_value_df = pd.concat([net_value_df, net_value], axis=0)
                                    done.append(name)
            if not os.path.exists("{}@side{}.xlsx".format(self.factor.__dict__['__module__'].split('.')[-1], side)):
                with pd.ExcelWriter("{}@side{}.xlsx".format(self.factor.__dict__['__module__'].split('.')[-1], side),
                                    mode='w') as writer:
                    res_df.to_excel(writer, sheet_name=single_domain)
                    net_value_df.to_excel(writer, sheet_name='{}_net_value'.format(single_domain))
            else:
                with pd.ExcelWriter("{}@side{}.xlsx".format(self.factor.__dict__['__module__'].split('.')[-1], side),
                                    mode='a') as writer:
                    res_df.to_excel(writer, sheet_name=single_domain)
                    net_value_df.to_excel(writer, sheet_name='{}_net_value'.format(single_domain))
