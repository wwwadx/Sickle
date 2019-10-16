import pandas as pd
from ..factor_def.basic import ORIGINAL_CLOSE, ORIGINAL_OPEN, ORIGINAL_ASK1, ORIGINAL_BID1
import os
from tqdm import tqdm
import numpy as np
import datetime as dt
from tzlocal import get_localzone
from ...utils.profiling import do_profile
import numba
user_home = os.path.expanduser('~')


@numba.jit
def cal_net(pos_long_array, pos_short_array, rebalance_time, returns_times,
            tday_series, close_price_array, open_price_array, cost, init_cap):
    account_value_list = np.zeros(tday_series.shape[0])
    net_value_list = np.zeros(tday_series.shape[0])
    turnover_list = np.zeros(tday_series.shape[0])
    long_value_list = np.zeros(tday_series.shape[0])
    short_value_list = np.zeros(tday_series.shape[0])
    long_volume_list = np.zeros((tday_series.shape[0], pos_long_array.shape[1]))
    short_volume_list = np.zeros((tday_series.shape[0], pos_short_array.shape[1]))
    # save result for portfolio analyse
    cash_list = np.zeros(tday_series.shape[0])
    # 建仓
    long_volume = np.floor(pos_long_array[0] * init_cap / open_price_array[0])
    short_volume = np.floor(pos_short_array[0] * init_cap / open_price_array[0])
    long_volume_list[0] = long_volume
    short_volume_list[0] = short_volume
    change_amount = np.sum(long_volume * open_price_array[0]) + np.sum(short_volume * open_price_array[0])
    cost_money = change_amount * cost
    cash_list[0] = init_cap - change_amount - cost_money
    long_earning = np.sum(long_volume * close_price_array[0]) - np.sum(long_volume * open_price_array[0])
    short_earning = np.sum(short_volume * open_price_array[0]) - np.sum(short_volume * close_price_array[0])
    long_value_list[0] = np.sum(long_volume * close_price_array[0])
    short_value_list[0] = np.sum(short_volume * close_price_array[0])
    account_value_list[0] = long_earning + short_earning + init_cap - cost_money
    net_value_list[0] = account_value_list[0] / init_cap
    turnover_list[0] = change_amount / init_cap
    for i in np.arange(tday_series.shape[0] - 1):
        # 换仓
        i = i + 1
        trade_time = tday_series[i]
        if np.any(rebalance_time == trade_time):
            re_time_index = np.searchsorted(rebalance_time, trade_time)
            return_time_index = np.searchsorted(returns_times, trade_time)
            # 开盘时的盈亏计算
            long_earning = np.sum(long_volume_list[i-1] * open_price_array[return_time_index]) - long_value_list[i-1]
            short_earning = short_value_list[i-1] - np.sum(short_volume_list[i-1] * open_price_array[return_time_index])
            account_value_list[i] = account_value_list[i-1] + long_earning + short_earning
            # 开盘换仓（开盘时的权重和乘以目标权重）
            # 目标仓位
            target_long = np.floor(pos_long_array[re_time_index] * account_value_list[i] / open_price_array[return_time_index])
            target_short = np.floor(pos_short_array[re_time_index] * account_value_list[i] / open_price_array[return_time_index])
            long_volume_list[i] = target_long
            short_volume_list[i] = target_short
            # 换仓的变化金额，手续费
            change_amount = np.sum(np.abs(target_long - long_volume_list[i-1]) * open_price_array[return_time_index]) + \
                            np.sum(np.abs(target_short - short_volume_list[i-1]) * open_price_array[return_time_index])
            cost_money = change_amount * cost
            turnover_list[i] = change_amount / account_value_list[i-1]
            # bar结束，多空市值
            long_value_list[i] = np.sum(target_long * close_price_array[return_time_index])
            short_value_list[i] = np.sum(target_short * close_price_array[return_time_index])

            # 多空盈亏（相对当bar开盘）和开盘相对前收盘的加一起
            long_earning += long_value_list[i] - np.sum(target_long * open_price_array[return_time_index])
            short_earning += np.sum(target_short * open_price_array[return_time_index]) - short_value_list[i]

            # 账户价值变动
            account_value_list[i] = long_earning + short_earning + account_value_list[i-1] - cost_money
            net_value_list[i] = account_value_list[i] / account_value_list[0]
            cash_list[i] = account_value_list[i] - long_value_list[i] - short_value_list[i]
        # 非换仓
        else:
            return_time_index = np.searchsorted(returns_times, trade_time)
            long_volume_list[i] = long_volume_list[i-1]
            short_volume_list[i] = short_volume_list[i-1]
            turnover_list[i] = 0
            long_value_list[i] = np.sum(long_volume_list[i] * close_price_array[return_time_index])
            short_value_list[i] = np.sum(short_volume_list[i] * close_price_array[return_time_index])
            cash_list[i] = cash_list[i-1]
            # 盈亏就是市值变化
            long_earning = long_value_list[i] - long_value_list[i-1]
            short_earning = short_value_list[i-1] - short_value_list[i]

            account_value_list[i] = account_value_list[i-1] + long_earning + short_earning
            net_value_list[i] = account_value_list[i] / account_value_list[0]
    return net_value_list, turnover_list, long_volume_list, short_volume_list, cash_list


class PortfolioPerformance:
    def __init__(self, frequence):
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

    # @do_profile("./pos_cal.prof")
    def long_and_short_perf_optimize_with_numba(self, pos, cost, init_cap, analyse=False):
        """
        同时多空的收益
        每次rebalance的时候要rebalance多空的市值
        :param pos:
        :param cost:
        :param init_cap: 初始资金
        :return:
        """
        if len(pos) > 0:
            pos = pos.sort_index()
            start_time = min(pos.index)
            end_time = max(pos.index)
            tday_series = pd.Series(self.trade_times, index=self.trade_times)
            tday_series = tday_series.loc[start_time:]
            tday_series.name = 'dates'
            position_df = pd.merge(pd.DataFrame(tday_series.shift(-1)), pos, left_index=True, right_index=True).dropna(how='all')
            position_df = position_df.set_index('dates').reset_index().rename(columns={'dates': 'datetime'})
            position_df = position_df.drop_duplicates()
            position_df = position_df.set_index('datetime')
            position_df = position_df[position_df.index.notnull()]
            rebalance_time = position_df.index
            position_df_long = position_df[position_df > 0].fillna(0)
            position_df_short = -position_df[position_df < 0].fillna(0)
            close_price = self.close.reindex(columns=position_df.columns).ffill()
            close_price = close_price[close_price.index >= rebalance_time[0]]
            close_price = close_price.fillna(1)
            open_price = self.open.reindex(columns=position_df.columns).ffill()
            open_price = open_price[open_price.index >= rebalance_time[0]]
            open_price = open_price.fillna(1)
            # change dataframe into numpy array
            pos_long_array = position_df_long.values / 2.0
            pos_short_array = position_df_short.values / 2.0
            close_price_array = close_price.values
            open_price_array = open_price.values
            returns_times = np.array([i.timestamp() for i in close_price.index])
            tday_series = tday_series[(tday_series.index >= rebalance_time[0]) & (tday_series.index <= end_time)]
            tday_series = tday_series.apply(lambda x: x.timestamp()).values
            rebalance_time = np.array([i.timestamp() for i in rebalance_time])
            net_value_list, turnover_list, long_volume_list, short_volume_list, cash_list = cal_net(pos_long_array, pos_short_array, rebalance_time, returns_times,
                    tday_series, close_price_array, open_price_array, cost, init_cap)
            time_list = [dt.datetime.fromtimestamp(i) for i in tday_series]
            net_df = pd.DataFrame(data=net_value_list, index=time_list, columns=['net_value'])
            turnover_df = pd.DataFrame(data=turnover_list, index=time_list, columns=['turnover'])
            tz_zone = get_localzone()
            net_df.index = net_df.index.tz_localize(tz_zone)
            turnover_df.index = turnover_df.index.tz_localize(tz_zone)
            # table for portfolio analyse
            if analyse:
                long_position_volume = pd.DataFrame(data=long_volume_list, index=time_list, columns=position_df.columns)
                short_position_volume = pd.DataFrame(data=short_volume_list, index=time_list, columns=position_df.columns)
                position_volume = long_position_volume - short_position_volume
                cash_df = pd.DataFrame(data=cash_list, index=time_list, columns=['cash'])
                position_volume.index = position_volume.index.tz_localize(tz_zone)
                cash_df.index = cash_df.index.tz_localize(tz_zone)
                return net_df, position_volume, cash_df
        else:
            net_df = pd.DataFrame()
            turnover_df = pd.DataFrame()
        return net_df, turnover_df
