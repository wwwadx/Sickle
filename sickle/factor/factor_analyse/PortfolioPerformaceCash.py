import pandas as pd
from ..factor_def.basic import ORIGINAL_CLOSE, ORIGINAL_OPEN, ORIGINAL_ASK1, ORIGINAL_BID1
import os
from tqdm import tqdm
import numpy as np
import datetime as dt
from tzlocal import get_localzone
user_home = os.path.expanduser('~')
pd.set_option('max_columns', 100)


class PortfolioPerformanceCash:
    def __init__(self, frequence):
        """
        Args:
            path: 各种因子缓存的路径
        """
        self.open = None
        self.close = None
        self.trade_times = None
        self.frequence = frequence
        self.init()

    def init(self):
        open_price = ORIGINAL_OPEN(frequency=self.frequence)
        close_price = ORIGINAL_CLOSE(frequency=self.frequence)
        self.open = open_price.get_cached_factor(os.path.join(user_home, 'factor_cache')).sort_index()
        self.close = close_price.get_cached_factor(os.path.join(user_home, 'factor_cache')).sort_index()
        self.open = self.open.ffill()
        self.close = self.close.ffill()
        self.trade_times = self.close.index

    def correct_position(self, pos, tday_series):
        """
        持仓后移，相当于每根bar的持仓是前一根的，避免未来数据
        :param pos:
        :return:
        """
        position_df = pd.merge(pd.DataFrame(tday_series.shift(-1)), pos, left_index=True, right_index=True).dropna(
            how='all')
        position_df = position_df.set_index('dates').reset_index().rename(columns={'dates': 'datetime'})
        position_df = position_df.drop_duplicates()
        position_df = position_df.set_index('datetime')
        position_df = position_df[position_df.index.notnull()]
        return position_df

    # @do_profile('./portfolio_efficient.prof')
    def cal_portfolio_perf(self, position_df, cost_ratio, init_cap):
        """
        计算组合收益， 需要指定cost_ratio
        Args:
            position_df: 没有索引的三列, DATETIME, CODES, POSITION, 其中相同日期POSITION和为100
            cost_ratio: 单边手续费
        Returns: 净值，日收益和基准净值的DataFrame
        """
        pos = position_df.sort_index()
        start_time = min(pos.index)
        end_time = max(pos.index)
        # 交易日序列
        tday_series = pd.Series(self.trade_times, index=self.trade_times)
        tday_series = tday_series.loc[start_time:]
        tday_series.name = 'dates'
        position_df = self.correct_position(pos, tday_series)
        # 换仓日序列
        change_days = position_df.index.unique()
        # 交易日序列
        tday_series = tday_series[(tday_series.index >= change_days[0]) & (tday_series.index <= end_time)]
        # 计算净值曲线表格
        cal_df = pd.DataFrame(columns=['是否换仓', '多头头寸', '空头头寸', '多头收益率', '本期多头盈亏',
                                       '空头收益率', '本期空头盈亏', '现金余额', '净值', '净值曲线', '手续费'])

        cal_df.sort_index(inplace=True)
        # 初始开仓计算
        init_cap = init_cap
        i = 0
        if tday_series.isin(change_days)[i]:
            tday = tday_series[i]
        else:
            a = pd.Series(change_days, index=change_days)
            tday = a.asof(tday_series[i])
            tday_series = tday_series[(tday_series >= tday) & (tday_series <= end_time)]

        cal_df.at[tday, '是否换仓'] = True
        cal_df.at[tday, '净值'] = init_cap
        cal_df.at[tday, '净值曲线'] = 1.0
        temp_pos = position_df[position_df.index == tday].T
        temp_pos_l = temp_pos[temp_pos > 0].fillna(0) / 2.0
        temp_pos_l.columns = ['pos_long']
        temp_pos_s = -temp_pos[temp_pos < 0].fillna(0) / 2.0
        temp_pos_s.columns = ['pos_short']
        temp_open = pd.DataFrame(self.open.loc[tday])
        temp_open.columns = ['open']
        temp_close = pd.DataFrame(self.close.loc[tday])
        temp_close.columns = ['close']
        num_temp = pd.concat([temp_pos_l, temp_pos_s], axis=1, join='inner')
        num_temp = pd.concat([num_temp, temp_open], axis=1, join='inner')
        num_temp = pd.concat([num_temp, temp_close], axis=1, join='inner')
        num_temp['多头手数'] = (num_temp['pos_long'] * cal_df.at[tday, '净值'] / (num_temp['open'])).apply(
            lambda x: round(x, 0))
        num_temp['空头手数'] = (num_temp['pos_short'] * cal_df.at[tday, '净值'] / (num_temp['open'])).apply(
            lambda x: round(x, 0))
        change_amount = (num_temp['多头手数'] * num_temp['open'] + num_temp['空头手数'] * num_temp['open']).sum()
        costs = change_amount * cost_ratio
        cal_df.at[tday, '多头头寸'] = (num_temp['多头手数'] * num_temp['close']).sum()
        cal_df.at[tday, '空头头寸'] = (num_temp['空头手数'] * num_temp['close']).sum()
        cal_df.at[tday, '空头收益率'] = -(cal_df.at[tday, '空头头寸'] / (num_temp['空头手数'] * num_temp['open']).sum() - 1)
        cal_df.at[tday, '多头收益率'] = cal_df.at[tday, '多头头寸'] / (num_temp['多头手数'] * num_temp['open']).sum() - 1
        cal_df.at[tday, '本期多头盈亏'] = cal_df.at[tday, '多头头寸'] - (num_temp['多头手数'] * num_temp['open']).sum()
        cal_df.at[tday, '本期空头盈亏'] = (num_temp['空头手数'] * num_temp['open']).sum() - cal_df.at[tday, '空头头寸']
        cal_df.at[tday, '现金余额'] = init_cap - change_amount - costs
        cal_df.at[tday, '手续费'] = costs
        cal_df.at[tday, '净值'] = cal_df.at[tday, '本期多头盈亏'] + cal_df.at[tday, '本期空头盈亏'] + init_cap - costs
        cal_df.at[tday, '净值曲线'] = cal_df.at[tday, '净值'] / init_cap
        cal_df.at[tday, '手续费占比'] = costs / cal_df.at[tday, '净值']
        num_temp.drop(['open', 'close'], axis=1, inplace=True)
        num_temp = num_temp.fillna(0)
        # 后续循环
        for i, tday in tqdm(enumerate(tday_series[1:])):
            previous_day = tday_series[i]
            # 非换仓日处理
            if not (tday_series[1:].isin(change_days)[i]):
                cal_df.at[tday, '是否换仓'] = False  # 是否换仓
                temp_close = pd.DataFrame(self.close.loc[tday])
                temp_close.columns = ['close']
                # 多头计算
                num_temp = pd.concat([num_temp, temp_close], axis=1, join='inner')
                cal_df.at[tday, '多头头寸'] = (num_temp['多头手数'] * num_temp['close']).sum()
                cal_df.at[tday, '多头收益率'] = (cal_df.at[tday, '多头头寸'] / cal_df.at[previous_day, '多头头寸']) - 1
                cal_df.at[tday, '空头头寸'] = (num_temp['空头手数'] * num_temp['close']).sum()
                cal_df.at[tday, '空头收益率'] = (cal_df.at[tday, '空头头寸'] / cal_df.at[previous_day, '空头头寸']) - 1
                cal_df.at[tday, '本期多头盈亏'] = cal_df.at[previous_day, '多头头寸'] * cal_df.at[tday, '多头收益率']
                cal_df.at[tday, '本期空头盈亏'] = cal_df.at[previous_day, '空头头寸'] * cal_df.at[tday, '空头收益率']
                cal_df.at[tday, '多头头寸'] = cal_df.at[previous_day, '多头头寸'] + cal_df.at[tday, '本期多头盈亏']
                cal_df.at[tday, '空头头寸'] = cal_df.at[previous_day, '空头头寸'] + cal_df.at[tday, '本期空头盈亏']
                cal_df.at[tday, '现金余额'] = cal_df.at[previous_day, '现金余额']
                cal_df.at[tday, '净值'] = cal_df.at[tday, '本期多头盈亏'] + cal_df.at[tday, '本期空头盈亏'] + cal_df.at[previous_day, '净值']
                cal_df.at[tday, '净值曲线'] = cal_df.at[tday, '净值'] / cal_df.iloc[0]['净值']
                num_temp.drop('close', axis=1, inplace=True)
            else:
                cal_df.at[tday, u'是否换仓'] = True  # 是否换仓
                # 换仓前计算
                temp_open = pd.DataFrame(self.open.loc[tday])
                temp_open.columns = ['open']
                temp_close = pd.DataFrame(self.close.loc[tday])
                temp_close.columns = ['close']
                num_temp = pd.concat([num_temp, temp_open], axis=1, join='inner')

                cal_df.at[tday, '多头收益率'] = ((num_temp['多头手数'] * num_temp['open']).sum()) / cal_df.at[
                    previous_day, '多头头寸'] - 1
                cal_df.at[tday, '空头收益率'] = -((num_temp['空头手数'] * num_temp['open']).sum() / cal_df.at[previous_day, '空头头寸'] - 1)
                cal_df.at[tday, '本期多头盈亏'] = cal_df.at[previous_day, '多头头寸'] * cal_df.at[tday, '多头收益率']
                cal_df.at[tday, '本期空头盈亏'] = cal_df.at[previous_day, '空头头寸'] * cal_df.at[tday, '空头收益率']
                cal_df.at[tday, '净值'] = cal_df.at[tday, '本期多头盈亏'] + cal_df.at[tday, '本期空头盈亏'] + cal_df.at[previous_day, '净值']
                cal_df.at[tday, '净值曲线'] = cal_df.at[tday, '净值'] / cal_df.iloc[0]['净值']
                position_temp_before = num_temp.rename(columns={'pos_long': '换仓前多头权重',
                                                                'pos_short': '换仓前空头权重',
                                                                '多头手数': '换仓前多头手数',
                                                                '空头手数': '换仓前空头手数'})
                # 换仓后计算
                temp_pos = position_df[position_df.index == tday].T
                temp_pos_l = temp_pos[temp_pos > 0].fillna(0) / 2.0
                temp_pos_l.columns = ['pos_long']
                temp_pos_s = -temp_pos[temp_pos < 0].fillna(0) / 2.0
                temp_pos_s.columns = ['pos_short']
                num_temp = pd.concat([temp_pos_l, temp_pos_s], axis=1, join='inner')
                num_temp = pd.concat([num_temp, temp_open], axis=1, join='inner')
                num_temp = pd.concat([num_temp, temp_close], axis=1, join='inner')

                num_temp['多头手数'] = (num_temp['pos_long'] * cal_df.at[tday, '净值'] / (num_temp['open'])).apply(
                    lambda x: round(x, 0))
                num_temp['空头手数'] = (num_temp['pos_short'] * cal_df.at[tday, '净值'] / (num_temp['open'])).apply(
                    lambda x: round(x, 0))
                cal_df.at[tday, '多头头寸'] = (num_temp['多头手数'] * num_temp['close']).sum()
                cal_df.at[tday, '空头头寸'] = (num_temp['空头手数'] * num_temp['close']).sum()
                cal_df.at[tday, '空头收益率'] = -(cal_df.at[tday, '空头头寸'] / cal_df.at[previous_day, '空头头寸'] - 1)
                cal_df.at[tday, '多头收益率'] = cal_df.at[tday, '多头头寸'] / cal_df.at[previous_day, '多头头寸'] - 1
                cal_df.at[tday, '现金余额'] += cal_df.at[previous_day, '多头头寸'] * cal_df.at[tday, '多头收益率'] + cal_df.at[previous_day, '空头头寸'] * cal_df.at[tday, '空头收益率']

                # 计算仓位变动表
                num_temp = num_temp.reset_index().rename(columns={'index': 'codes'})
                position_temp_before = position_temp_before.reset_index().rename(columns={'index': 'codes'}).drop('open', axis=1)
                position_temp = pd.merge(num_temp, position_temp_before, on='codes', how='outer')
                position_temp.fillna(0.0, inplace=True)
                change_long = (abs(position_temp['多头手数'] - position_temp['换仓前多头手数']) * position_temp['open']).sum()
                change_short = (abs(position_temp['空头手数'] - position_temp['换仓前空头手数']) * position_temp['open']).sum()
                costs = (change_long + change_short) * cost_ratio
                cal_df.at[tday, '手续费'] = costs

                # 数据更正计算
                cal_df.at[tday, '本期多头盈亏'] += cal_df.at[tday, '多头头寸'] - (num_temp['多头手数'] * num_temp['open']).sum()
                cal_df.at[tday, '本期空头盈亏'] += (num_temp['空头手数'] * num_temp['open']).sum() - cal_df.at[tday, '空头头寸']
                cal_df.at[tday, '现金余额'] -= costs
                cal_df.at[tday, '净值'] = cal_df.at[tday, '本期多头盈亏'] + cal_df.at[tday, '本期空头盈亏'] + cal_df.at[previous_day, '净值'] - costs
                cal_df.at[tday, '净值曲线'] = cal_df.at[tday, '净值'] / cal_df.iloc[0]['净值']
                cal_df.at[tday, '手续费占比'] = costs / cal_df.at[tday, '净值']

                num_temp.drop(['close', 'open'], axis=1, inplace=True)
                num_temp = num_temp.set_index('codes')

        # 补全第一天的相关数据
        cal_df.at[start_time, '净值曲线'] = 1.0
        cal_df.at[start_time, '净值'] = init_cap
        cal_df.at[start_time, '现金余额'] = init_cap
        cal_df.at[start_time, '是否换仓'] = False
        cal_df.fillna(0, inplace=True)

        # 净值序列数据
        result_df = pd.DataFrame()
        cal_df.sort_index(inplace=True)
        result_df['net_value'] = cal_df['净值曲线']
        result_df.sort_index(inplace=True)
        result_df['portfolio_return'] = result_df['net_value'].pct_change()
        result_df.fillna(0, inplace=True)
        result_df.index.name = 'DATETIME'
        return result_df
