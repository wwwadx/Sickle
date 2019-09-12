import pandas as pd
import numpy as np
from ..factor_def.basic import ORIGINAL_CLOSE
import os
import time
user_home = os.path.expanduser('~')


class FactorAnalyser:
    def __init__(self):
        pass

    def trading_kline(self, frequency):
        close = ORIGINAL_CLOSE(frequency)
        close_df = close.get_cached_factor(os.path.join(user_home, 'factor_cache'))
        trading_kline = close_df.index
        return trading_kline

    def single_target_portfolio(self, name_list, frequence, period):
        """
        构建指定品种的组合，方便测试
        Args:
            name_list: 品种列表
            frequence: 品种k线周期
            period: 调仓周期，是因子频率的period倍

        Returns:
            字典形式存储的结果

        """
        trading_kline = self.trading_kline(frequence)
        temp_df = pd.DataFrame(trading_kline)
        w = 1.0 / len(name_list)
        for name in name_list:
            temp_df[name] = w
        temp_df = temp_df.set_index('datetime')
        temp_df.columns.name = 'codes'
        # 每隔period时间调仓,选出每次调仓结果组成dataframe
        select_rebanlance = [i for i in range(1, len(temp_df), period)]
        select_df = temp_df.iloc[select_rebanlance, :]
        select_df = select_df.div(select_df.sum(1), 0)
        select_df = select_df.fillna(0)
        return select_df

    def ls_portfolio(self, long_list, short_list, frequence, period):
        """
        构建指定品种的多空组合
        Args:
            long_list: 做多品种列表
            short_list: 做空品种列表
            frequence: 品种k线周期
            period: 调仓周期，是因子频率的period倍

        Returns:
            字典形式存储的结果

        """
        trading_kline = self.trading_kline(frequence)
        temp_df = pd.DataFrame(trading_kline)
        long_weight = 1 / len(long_list)
        short_weight = 1 / len(short_list)
        for name in long_list:
            temp_df[name] = long_weight
        for name in short_list:
            temp_df[name] = -short_weight
        temp_df = temp_df.set_index('datetime')
        temp_df.columns.name = 'codes'
        # 每隔period时间调仓,选出每次调仓结果组成dataframe
        select_rebanlance = [i for i in range(1, len(temp_df), period)]
        select_df = temp_df.iloc[select_rebanlance, :]
        select_df = select_df.fillna(0)
        return select_df

    def factor_to_portfolio(self, fac, side, count, period, ls):
        """
        因子构建多头组合 或 空头组合
        Args:
            fac: 矩阵形式存储的因子DataFrame
            side: 因子方向, int, 1或-1
            count: 做多多少只股票，如果小于1则按照quantile做
            period: 调仓周期，是因子频率的period倍
            ls: 1多头，-1空头

        Returns:
            字典形式存储的结果

        """
        fac.replace(np.inf, np.nan, inplace=True)
        fac.replace(-np.inf, np.nan, inplace=True)
        fac = fac.dropna(how='all', axis=0)
        if count < 1:
            stock_chosen = fac.copy()
            if side * ls == 1:
                fac = fac.sub(fac.quantile(1 - count, axis=1), axis=0)
                stock_chosen[fac > 0] = 1
                stock_chosen[fac <= 0] = 0
            elif side * ls == -1:
                fac = fac.sub(fac.quantile(count, axis=1), axis=0)
                stock_chosen[fac < 0] = 1
                stock_chosen[fac >= 0] = 0
            else:
                print('Wrong side! ')
            chosen_num = stock_chosen.sum(1)
            result_df = stock_chosen.div(chosen_num, 0)
        if count >= 1:
            stock_chosen = fac.copy()
            if side * ls == 1:
                fac = fac.rank(axis=1, method='first')
                fac = fac.sub(fac.max(1) - count, 0)
                stock_chosen[fac > 0] = 1
                stock_chosen[fac <= 0] = 0
                stock_chosen = stock_chosen.fillna(0)
            elif side * ls == -1:
                fac = fac.rank(axis=1)
                fac = fac.sub(fac.min(1) + count, 0)
                stock_chosen[fac < 0] = 1
                stock_chosen[fac >= 0] = 0
                stock_chosen = stock_chosen.fillna(0)
            chosen_num = stock_chosen.sum(1)
            result_df = stock_chosen.div(chosen_num, 0)
        # 每隔period时间调仓,选出每次调仓结果组成dataframe
        select_rebanlance = [i for i in range(1, len(result_df), period)]
        select_df = result_df.iloc[select_rebanlance, :]
        select_df = select_df.div(select_df.sum(1), 0)
        select_df = select_df.fillna(0)
        return select_df

    def factor_to_portfolio_ls(self, fac, side, count, period):
        """
        因子构建多空组合, 品种等权
        Args:
            fac: 矩阵形式存储的因子DataFrame
            side: 因子方向, int, 1或-1
            count: 做多多少只股票，如果小于1则按照quantile做
            period: 调仓周期，是因子频率的period倍

        Returns:
            字典形式存储的结果

        """
        fac_df = fac.copy(deep=True)
        fac_df.replace(np.inf, np.nan, inplace=True)
        fac_df.replace(-np.inf, np.nan, inplace=True)
        fac_df = fac_df.dropna(how='all', axis=0)
        # 对因子进行过滤，从开始剔除截面上少于2个品种的数据（没意义）
        temp_count = fac_df.count(1)
        fac_df = fac_df[fac_df.index >= temp_count[temp_count > 1].index[0]]
        if count < 1:
            long_chosen = fac_df.copy()
            short_chosen = fac_df.copy()
            temp_l = fac_df.sub(fac_df.quantile(1 - count, axis=1), axis=0)

            long_chosen[temp_l >= 0] = 1
            long_chosen[temp_l < 0] = 0
            temp_s = fac_df.sub(fac_df.quantile(count, axis=1), axis=0)
            short_chosen[temp_s <= 0] = 1
            short_chosen[temp_s > 0] = 0
            if side == -1:
                long_chosen, short_chosen = short_chosen, long_chosen
            long_chosen = long_chosen.fillna(0)
            short_chosen = short_chosen.fillna(0)
            chosen_num_l = long_chosen.sum(1)
            result_df_l = long_chosen.div(chosen_num_l, 0)
            chosen_num_s = short_chosen.sum(1)
            result_df_s = -short_chosen.div(chosen_num_s, 0)
            result_df = result_df_l + result_df_s
            # 如果截面上全部数据一致时，会出现持仓为0的情况，这种时候修正持仓，让其等于上一个bar的持仓，相当于持仓不变
            zero_point = result_df[(abs(result_df).sum(1) == 0)].index
            for time_index in zero_point:
                result_df.loc[time_index] = result_df.iloc[np.searchsorted(result_df.index, time_index) - 1, :]
            # 归一化
            result_df[result_df > 0] = result_df[result_df > 0].div(result_df[result_df > 0].sum(1), 0)
            result_df[result_df < 0] = -result_df[result_df < 0].div(result_df[result_df < 0].sum(1), 0)
        if count >= 1:
            long_chosen = fac_df.copy()
            short_chosen = fac_df.copy()
            fac_df = fac_df.rank(axis=1, method='first')
            fac_df = fac_df[fac_df.max(1) >= count * 2]

            temp_l = fac_df.sub(fac_df.max(1) - count, 0)
            long_chosen = long_chosen.loc[temp_l.index]
            long_chosen[temp_l > 0] = 1
            long_chosen[temp_l <= 0] = 0
            temp_s = fac_df.sub(fac_df.min(1) + count, 0)
            short_chosen = short_chosen.loc[temp_s.index]
            short_chosen[temp_s < 0] = 1
            short_chosen[temp_s >= 0] = 0
            if side == -1:
                long_chosen, short_chosen = short_chosen, long_chosen
            long_chosen = long_chosen.fillna(0)
            short_chosen = short_chosen.fillna(0)
            chosen_num_l = long_chosen.sum(1)
            result_df_l = long_chosen.div(chosen_num_l, 0)
            chosen_num_s = short_chosen.sum(1)
            result_df_s = -short_chosen.div(chosen_num_s, 0)
            result_df = result_df_l + result_df_s
            zero_point = result_df[(abs(result_df).sum(1) == 0)].index
            for time_index in zero_point:
                result_df.loc[time_index] = result_df.iloc[np.searchsorted(result_df.index, time_index) - 1, :]
            # 归一化
            result_df[result_df > 0] = result_df[result_df > 0].div(result_df[result_df > 0].sum(1), 0)
            result_df[result_df < 0] = -result_df[result_df < 0].div(result_df[result_df < 0].sum(1), 0)
        # 每隔period时间调仓,选出每次调仓结果组成dataframe
        select_rebanlance = [i for i in range(1, len(result_df), period)]
        select_df = result_df.iloc[select_rebanlance, :]
        select_df = select_df.fillna(0)
        return select_df

    @staticmethod
    def filter_extreme_nd_standarize(fac):
        """
        中位数去极值
        Args:
            fac: 矩阵形式存储的因子DataFrame

        Returns:

        """
        median = fac.median(axis=1)
        mad = abs(fac.sub(median, axis=0)).median(axis=1)
        data = fac.clip(median - 5.2 * mad, median + 5.2 * mad, axis=0)
        return data.sub(data.mean(axis=1), 0).div(data.std(1), 0).dropna(how='all')

    @staticmethod
    def filter_extreme(fac):
        """
        中位数去极值
        Args:
            fac: 矩阵形式存储的因子DataFrame

        Returns:

        """
        median = fac.median(axis=1)
        mad = abs(fac.sub(median, axis=0)).median(axis=1)
        data = fac.clip(median - 5.2 * mad, median + 5.2 * mad, axis=0)
        return data

    def factor_to_portfolio_ls_weight(self, fac_df, side, period):
        """
        因子构建多空组合, 品种按因子值加权
        Args:
            fac_df: 矩阵形式存储的因子DataFrame
            side: 因子方向, int, 1或-1
            period: 调仓周期，是因子频率的period倍

        Returns:
            字典形式存储的结果

        """
        fac = fac_df.copy(deep=True)
        fac.replace(np.inf, np.nan, inplace=True)
        fac.replace(-np.inf, np.nan, inplace=True)
        fac = fac.dropna(how='all', axis=0)
        # 对因子进行过滤，从开始剔除截面上少于2个品种的数据（没意义）
        temp_count = fac.count(1)
        fac = fac[fac.index >= temp_count[temp_count > 1].index[0]]
        # 对因子覆盖度进行过滤， 平均覆盖度小于截面信息一半的因子剔除（没意义）
        if len(fac) > 1 and fac.count(1).mean() > fac.shape[1] // 2:
            fac = self.filter_extreme(fac)
            long_chosen = fac.copy(deep=True)
            short_chosen = fac.copy(deep=True)
            sub_median = fac.sub(fac.median(1), axis=0)
            long_chosen[sub_median <= 0] = 0
            short_chosen[sub_median > 0] = 0
            if side == -1:
                long_chosen, short_chosen = short_chosen, long_chosen
            long_chosen = long_chosen.fillna(0)
            short_chosen = short_chosen.fillna(0)
            chosen_num_l = long_chosen.sum(1)
            result_df_l = long_chosen.div(chosen_num_l, 0)
            chosen_num_s = short_chosen.sum(1)
            result_df_s = -short_chosen.div(chosen_num_s, 0)
            result_df = result_df_l + result_df_s
            temp_df = result_df[(abs(result_df).sum(1) > 0)]
            if len(temp_df) > 0:
                result_df = result_df[result_df.index > temp_df.index[0]]
            if len(result_df) > 1:
                # 如果截面上全部数据一致时，会出现持仓为0的情况，这种时候修正持仓，让其等于上一个bar的持仓，相当于持仓不变
                zero_point = result_df[(abs(result_df).sum(1) == 0)].index
                if len(zero_point) / len(result_df) < 0.5:
                    for time_index in zero_point:
                        result_df.loc[time_index] = result_df.iloc[np.searchsorted(result_df.index, time_index) - 1, :]
                    # 归一化
                    result_df[result_df > 0] = result_df[result_df > 0].div(result_df[result_df > 0].sum(1), 0)
                    result_df[result_df < 0] = -result_df[result_df < 0].div(result_df[result_df < 0].sum(1), 0)
                    # 每隔period时间调仓,选出每次调仓结果组成dataframe
                    select_rebanlance = [i for i in range(1, len(result_df), period)]
                    select_df = result_df.iloc[select_rebanlance, :]
                    select_df = select_df.fillna(0)
                else:
                    select_df = pd.DataFrame()
                    print("截面因子值相同大于50%")
            else:
                select_df = pd.DataFrame()
                print("因子截面全部相同，无意义")
        else:
            select_df = pd.DataFrame()
            print("因子覆盖度不足50%")
        return select_df

    def factor_to_portfolio_ls_basedon_value(self, fac, value, period):
        """
        因子构建多空组合, 根据给定值构
        Args:
            fac: 矩阵形式存储的因子DataFrame
            value: 因子值大于value做多，小于value做空
            period: 调仓周期，是因子频率的period倍

        Returns:
            字典形式存储的结果

        """
        fac.replace(np.inf, np.nan, inplace=True)
        fac.replace(-np.inf, np.nan, inplace=True)
        fac = fac.dropna(how='all', axis=0)
        long_chosen = fac.copy()
        short_chosen = fac.copy()
        long_chosen[fac > value] = 1
        long_chosen[fac <= value] = 0
        short_chosen[fac < value] = 1
        short_chosen[fac >= value] = 0
        long_chosen = long_chosen.fillna(0)
        short_chosen = short_chosen.fillna(0)
        chosen_num_l = long_chosen.sum(1)
        result_df_l = long_chosen.div(chosen_num_l, 0)
        chosen_num_s = short_chosen.sum(1)
        result_df_s = -short_chosen.div(chosen_num_s, 0)
        result_df = result_df_l + result_df_s
        # 每隔period时间调仓,选出每次调仓结果组成dataframe
        select_rebanlance = [i for i in range(1, len(result_df), period)]
        select_df = result_df.iloc[select_rebanlance, :]
        select_df = select_df.fillna(0)
        return select_df

