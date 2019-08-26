import pandas as pd
from ..factor_def.basic.ORIGINAL_CLOSE import ORIGINAL_CLOSE
from ..factor_def.basic.ORIGINAL_OPEN import ORIGINAL_OPEN
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
            tday_series, close_open_array, open_close_array, close_close_array, cost):
    net_value_list = np.zeros(tday_series.shape[0])
    turnover_list = np.zeros(tday_series.shape[0])
    # 建仓
    temp_long = pos_long_array[0] * (1 - cost + close_open_array[0]) * 0.5
    temp_short = pos_short_array[0] * (1 - cost - close_open_array[0]) * 0.5
    daily_weight_long = temp_long
    daily_weight_short = temp_short

    net_value_list[0] = np.sum(temp_long) + np.sum(temp_short)
    turnover_list[0] = 1

    last_cost = np.array([cost])
    for i in np.arange(tday_series.shape[0]):
        # 换仓
        trade_time = tday_series[i]
        if np.any(rebalance_time == trade_time):
            re_time_index = np.searchsorted(rebalance_time, trade_time)
            return_time_index = np.searchsorted(returns_times, trade_time)
            temp_long_open = daily_weight_long * (1 + open_close_array[return_time_index])
            temp_short_open = daily_weight_short * (1 - open_close_array[return_time_index])
            temp_long = pos_long_array[re_time_index] * (
                    np.sum(temp_long_open) + np.sum(temp_short_open) - last_cost) * 0.5
            temp_short = pos_short_array[re_time_index] * (
                    np.sum(temp_long_open) + np.sum(temp_short_open) - last_cost) * 0.5
            cost_long = np.abs(temp_long - daily_weight_long) * cost
            cost_short = np.abs(temp_short - daily_weight_short) * cost
            cost_percent = cost_long + cost_short
            turnover = np.sum(np.abs(temp_long - daily_weight_long)) + np.sum(
                np.abs(temp_short - daily_weight_short))
            temp_long_close = temp_long * (1 - cost_long + close_open_array[return_time_index])
            temp_short_close = temp_short * (1 - cost_short - close_open_array[return_time_index])
            daily_weight_long = temp_long_close
            daily_weight_short = temp_short_close
            net_value_list[i] = np.sum(temp_long_close) + np.sum(temp_short_close)
            turnover_list[i] = turnover
            last_cost = cost_percent
        # 非换仓
        else:
            return_time_index = np.searchsorted(returns_times, trade_time)
            temp_long = daily_weight_long * (1 + close_close_array[return_time_index])
            temp_short = daily_weight_short * (1 - close_close_array[return_time_index])
            daily_weight_long = temp_long
            daily_weight_short = temp_short
            net_value_list[i] = np.sum(temp_short) + np.sum(temp_long)
            turnover_list[i] = 0

    return net_value_list, turnover_list


class PortfolioPerformance:
    def __init__(self, frequence):
        self.open = None
        self.close = None
        self.trade_times = None
        # 当天收盘和当天开盘
        self.close_open_returns = None
        # 当天收盘和前一天收盘
        self.close_close_returns = None
        # 当天开盘和前一天收盘
        self.open_close_return = None
        self.frequence = frequence
        self.init()

    def init(self):
        open_price = ORIGINAL_OPEN(frequency=self.frequence)
        close_price = ORIGINAL_CLOSE(frequency=self.frequence)
        self.open = open_price.get_cached_factor(os.path.join(user_home, 'factor_cache')).sort_index()
        self.close = close_price.get_cached_factor(os.path.join(user_home, 'factor_cache')).sort_index()
        self.close_close_returns = self.close.pct_change(fill_method=None)
        self.close_close_returns = self.close_close_returns.fillna(0)
        self.close_open_returns = self.close / self.open - 1
        self.close_open_returns = self.close_open_returns.fillna(0)
        self.open_close_return = self.open / self.close.shift(1) - 1
        self.open_close_return = self.open_close_return.fillna(0)
        self.trade_times = self.close.index

    def long_or_short_perf(self, pos, cost, ls):
        """
        做多或做空的收益
        :param pos:
        :param cost:
        :param ls: 1做多， -1做空
        :return:
        """
        start_time = min(pos.index)
        end_time = max(pos.index)
        tday_series = pd.Series(self.trade_times, index=self.trade_times)
        tday_series = tday_series.loc[start_time:]
        tday_series.name = 'dates'
        position_df = pd.merge(pd.DataFrame(tday_series.shift(-1)), pos, left_index=True, right_index=True)
        position_df = position_df.set_index('dates').reset_index().rename(columns={'dates': 'datetime'})
        position_df = position_df.set_index('datetime')
        rebalance_time = pd.DatetimeIndex(position_df.index.drop_duplicates())
        daily_weight = pd.DataFrame()
        net_value_list = []
        trade_time_list = []
        cost_list = []
        # 建仓
        temp = position_df.loc[rebalance_time[0]] * (1 - cost + ls * self.close_open_returns.loc[rebalance_time[0]])
        temp = temp.fillna(0)
        daily_weight = daily_weight.append(temp)
        net_value_list.append(temp.sum())
        trade_time_list.append(rebalance_time[0])
        cost_list.append(cost)
        tday_series = tday_series[
            tday_series.index.isin([i for i in tday_series.index if end_time >= i > rebalance_time[0]])]
        for trade_time in tqdm(tday_series.index):
            # 换仓
            if trade_time in rebalance_time and len(daily_weight) > 0:
                # 换仓按照开盘价换， 先算前收盘持仓到今开盘持仓的权重变化， 在这个基础上进行换仓
                temp_1 = daily_weight.iloc[-1, :] * (1 + ls * self.open_close_return.loc[trade_time])
                temp_1 = temp_1.fillna(0)
                # 换完仓的权重
                temp_2 = position_df.loc[trade_time] * temp_1.sum()
                # 双边成本
                cost_percent = abs(temp_2 - temp_1).sum() * cost
                # 换完仓到收盘的权重
                temp_3 = temp_2 * (1 - cost_percent + ls * self.close_open_returns.loc[trade_time])
                temp_3.name = trade_time
                temp_3 = temp_3.fillna(0)
                daily_weight = daily_weight.append(temp_3)
                net_value_list.append(temp_3.sum())
                trade_time_list.append(trade_time)
                cost_list.append(cost_percent)
            # 非换仓
            if trade_time not in rebalance_time and len(daily_weight) > 0:
                temp = daily_weight.iloc[-1, :] * (1 + ls * self.close_close_returns.loc[trade_time])
                temp = temp.fillna(0)
                temp.name = trade_time
                daily_weight = daily_weight.append(temp)
                net_value_list.append(temp.sum())
                trade_time_list.append(trade_time)
        net_df = pd.DataFrame().from_records(dict(zip(trade_time_list, net_value_list)), index=['net_value']).T
        return net_df

    def long_and_short_perf(self, pos, cost):
        """
        同时多空的收益
        每次rebalance的时候要rebalance多空的市值
        :param pos:
        :param cost:
        :return:
        """
        start_time = min(pos.index)
        end_time = max(pos.index)
        tday_series = pd.Series(self.trade_times, index=self.trade_times)
        tday_series = tday_series.loc[start_time:]
        tday_series.name = 'dates'
        position_df = pd.merge(pd.DataFrame(tday_series.shift(-1)), pos, left_index=True, right_index=True).dropna()
        position_df = position_df.set_index('dates').reset_index().rename(columns={'dates': 'datetime'})
        position_df = position_df.set_index('datetime')
        rebalance_time = pd.DatetimeIndex(position_df.index)
        position_df_long = position_df[position_df > 0].fillna(0)
        position_df_short = -position_df[position_df < 0].fillna(0)
        daily_weight_long = pd.DataFrame()
        daily_weight_short = pd.DataFrame()
        net_value_list = []
        time_list = []
        cost_list = []
        # 建仓
        temp_long = position_df_long.loc[rebalance_time[0]] * (1 - cost + self.close_open_returns.loc[rebalance_time[0]]) * 0.5
        temp_long = temp_long.fillna(0)
        temp_short = position_df_short.loc[rebalance_time[0]] * (1 - cost - self.close_open_returns.loc[rebalance_time[0]]) * 0.5
        temp_short = temp_short.fillna(0)
        daily_weight_long = daily_weight_long.append(temp_long)
        daily_weight_short = daily_weight_short.append(temp_short)
        net_value_list.append(temp_long.sum() + temp_short.sum())
        time_list.append(rebalance_time[0])
        cost_list.append(cost)
        tday_series = tday_series[tday_series.index.isin([i for i in tday_series.index if end_time >= i > rebalance_time[0]])]
        for trade_time in tqdm(tday_series.index):
            # 换仓
            if trade_time in rebalance_time and len(daily_weight_long) > 0 and len(daily_weight_short) > 0:
                # 先计算开盘换仓的权重变化
                temp_long_open = daily_weight_long.iloc[-1, :] * (1 + self.open_close_return.loc[trade_time])
                temp_long_open = temp_long_open.fillna(0)
                temp_short_open = daily_weight_short.iloc[-1, :] * (1 - self.open_close_return.loc[trade_time])
                temp_short_open = temp_short_open.fillna(0)
                # 换完仓的权重, 这里相当于对市值进行了rebalance
                temp_long = position_df_long.loc[trade_time] * (temp_long_open.sum() + temp_short_open.sum() - cost_list[-1]) * 0.5
                temp_short = position_df_short.loc[trade_time] * (temp_long_open.sum() + temp_short_open.sum() - cost_list[-1]) * 0.5
                # 双边成本
                cost_long = abs(temp_long - daily_weight_long.iloc[-1, :]) * cost
                cost_short = abs(temp_short - daily_weight_short.iloc[-1, :]) * cost
                cost_percent = cost_long + cost_short
                # 持仓到收盘的权重变化
                temp_long_close = temp_long * (1 - cost_long + self.close_open_returns.loc[trade_time])
                temp_long_close.name = trade_time
                temp_long_close = temp_long_close.fillna(0)
                temp_short_close = temp_short * (1 - cost_short - self.close_open_returns.loc[trade_time])
                temp_short_close.name = trade_time
                temp_short_close = temp_short_close.fillna(0)
                daily_weight_long = daily_weight_long.append(temp_long_close)
                daily_weight_short = daily_weight_short.append(temp_short_close)
                net_value_list.append(temp_long_close.sum() + temp_short_close.sum())
                cost_list.append(cost_percent)
                time_list.append(trade_time)
            # 非换仓
            if trade_time not in rebalance_time and len(daily_weight_long) > 0 and len(daily_weight_short) > 0:
                temp_long = daily_weight_long.iloc[-1, :] * (1 + self.close_close_returns.loc[trade_time])
                temp_long = temp_long.fillna(0)
                temp_long.name = trade_time
                temp_short = daily_weight_short.iloc[-1, :] * (1 - self.close_close_returns.loc[trade_time])
                temp_short = temp_short.fillna(0)
                temp_short.name = trade_time
                daily_weight_long = daily_weight_long.append(temp_long)
                daily_weight_short = daily_weight_short.append(temp_short)
                net_value_list.append(temp_short.sum() + temp_long.sum())
                time_list.append(trade_time)
        net_df = pd.DataFrame().from_records(dict(zip(time_list, net_value_list)), index=['net_value']).T
        return net_df

    @do_profile("./pos_cal.prof")
    def long_and_short_perf_optimize(self, pos, cost):
        """
        同时多空的收益
        每次rebalance的时候要rebalance多空的市值
        :param pos:
        :param cost:
        :return:
        """
        pos = pos.sort_index()
        start_time = min(pos.index)
        end_time = max(pos.index)
        tday_series = pd.Series(self.trade_times, index=self.trade_times)
        tday_series = tday_series.loc[start_time:]
        tday_series.name = 'dates'
        position_df = pd.merge(pd.DataFrame(tday_series.shift(-1)), pos, left_index=True, right_index=True).dropna(how='all')
        position_df = position_df.set_index('dates').reset_index().rename(columns={'dates': 'datetime'})
        position_df = position_df.set_index('datetime')
        position_df = position_df.drop_duplicates()
        rebalance_time = position_df.index
        position_df_long = position_df[position_df > 0].fillna(0)
        position_df_short = -position_df[position_df < 0].fillna(0)
        close_open_returns = self.close_open_returns.reindex(columns=position_df.columns).fillna(0)
        close_open_returns = close_open_returns[close_open_returns.index >= rebalance_time[0]]
        close_close_returns = self.close_close_returns.reindex(columns=position_df.columns).fillna(0)
        close_close_returns = close_close_returns[close_close_returns.index >= rebalance_time[0]]
        open_close_returns = self.open_close_return.reindex(columns=position_df.columns).fillna(0)
        open_close_returns = open_close_returns[open_close_returns.index >= rebalance_time[0]]
        # change dataframe into numpy array
        pos_long_array = position_df_long.values
        pos_short_array = position_df_short.values
        close_open_array = close_open_returns.values
        close_close_array = close_close_returns.values
        open_close_array = open_close_returns.values
        returns_times = close_close_returns.index
        net_value_list = []
        time_list = []
        turnover_list = []
        # 建仓
        temp_long = pos_long_array[0] * (1 - cost + close_open_array[0]) * 0.5
        temp_short = pos_short_array[0] * (1 - cost - close_open_array[0]) * 0.5
        daily_weight_long = temp_long
        daily_weight_short = temp_short
        net_value_list.append(temp_long.sum() + temp_short.sum())
        time_list.append(rebalance_time[0])
        turnover_list.append(1)
        last_cost = cost
        tday_series = tday_series[(tday_series.index > rebalance_time[0]) & (tday_series.index <= end_time)]
        for trade_time in tday_series.index:
            # 换仓
            if trade_time in rebalance_time:
                re_time_index = np.searchsorted(rebalance_time, trade_time)
                return_time_index = np.searchsorted(returns_times, trade_time)
                # 先计算开盘换仓的权重变化
                temp_long_open = daily_weight_long * (1 + open_close_array[return_time_index])
                temp_short_open = daily_weight_short * (1 - open_close_array[return_time_index])
                # 换完仓的权重, 这里相当于对市值进行了rebalance
                temp_long = pos_long_array[re_time_index] * (temp_long_open.sum() + temp_short_open.sum() - last_cost) * 0.5
                temp_short = pos_short_array[re_time_index] * (temp_long_open.sum() + temp_short_open.sum() - last_cost) * 0.5
                # 双边成本
                cost_long = abs(temp_long - daily_weight_long) * cost
                cost_short = abs(temp_short - daily_weight_short) * cost
                cost_percent = cost_long + cost_short
                turnover = abs(temp_long - daily_weight_long).sum() + abs(temp_short - daily_weight_short).sum()
                turnover_list.append(turnover)
                # 持仓到收盘的权重变化
                temp_long_close = temp_long * (1 - cost_long + close_open_array[return_time_index])
                temp_short_close = temp_short * (1 - cost_short - close_open_array[return_time_index])
                daily_weight_long = temp_long_close
                daily_weight_short = temp_short_close
                net_value_list.append(temp_long_close.sum() + temp_short_close.sum())
                last_cost = cost_percent
                time_list.append(trade_time)
            # 非换仓
            if trade_time not in rebalance_time:
                return_time_index = np.searchsorted(returns_times, trade_time)
                temp_long = daily_weight_long * (1 + close_close_array[return_time_index])
                temp_short = daily_weight_short * (1 - close_close_array[return_time_index])
                daily_weight_long = temp_long
                daily_weight_short = temp_short
                net_value_list.append(temp_short.sum() + temp_long.sum())
                time_list.append(trade_time)
                turnover_list.append(0)
        net_df = pd.DataFrame().from_records(dict(zip(time_list, net_value_list)), index=['net_value']).T
        turnover_df = pd.DataFrame().from_records(dict(zip(time_list, turnover_list)), index=['turnover']).T
        return net_df, turnover_df

    def long_and_short_perf_optimize_with_numba(self, pos, cost):
        """
        同时多空的收益
        每次rebalance的时候要rebalance多空的市值
        :param pos:
        :param cost:
        :return:
        """
        pos = pos.sort_index()
        start_time = min(pos.index)
        end_time = max(pos.index)
        tday_series = pd.Series(self.trade_times, index=self.trade_times)
        tday_series = tday_series.loc[start_time:]
        tday_series.name = 'dates'
        position_df = pd.merge(pd.DataFrame(tday_series.shift(-1)), pos, left_index=True, right_index=True).dropna(how='all')
        position_df = position_df.set_index('dates').reset_index().rename(columns={'dates': 'datetime'})
        position_df = position_df.set_index('datetime')
        position_df = position_df.drop_duplicates()
        rebalance_time = position_df.index
        position_df_long = position_df[position_df > 0].fillna(0)
        position_df_short = -position_df[position_df < 0].fillna(0)
        close_open_returns = self.close_open_returns.reindex(columns=position_df.columns).fillna(0)
        close_open_returns = close_open_returns[close_open_returns.index >= rebalance_time[0]]
        close_close_returns = self.close_close_returns.reindex(columns=position_df.columns).fillna(0)
        close_close_returns = close_close_returns[close_close_returns.index >= rebalance_time[0]]
        open_close_returns = self.open_close_return.reindex(columns=position_df.columns).fillna(0)
        open_close_returns = open_close_returns[open_close_returns.index >= rebalance_time[0]]
        # change dataframe into numpy array
        pos_long_array = position_df_long.values
        pos_short_array = position_df_short.values
        close_open_array = close_open_returns.values
        close_close_array = close_close_returns.values
        open_close_array = open_close_returns.values
        returns_times = np.array([i.timestamp() for i in close_close_returns.index])
        tday_series = tday_series[(tday_series.index > rebalance_time[0]) & (tday_series.index <= end_time)]
        tday_series = tday_series.apply(lambda x: x.timestamp()).values
        rebalance_time = np.array([i.timestamp() for i in rebalance_time])
        net_value_list, turnover_list = cal_net(pos_long_array, pos_short_array, rebalance_time, returns_times,
                tday_series, close_open_array, open_close_array, close_close_array, cost)
        time_list = [dt.datetime.fromtimestamp(i) for i in tday_series]
        net_df = pd.DataFrame().from_records(dict(zip(time_list, net_value_list)), index=['net_value']).T
        turnover_df = pd.DataFrame().from_records(dict(zip(time_list, turnover_list)), index=['turnover']).T
        tz_zone = get_localzone()
        net_df.index = net_df.index.tz_localize(tz_zone)
        turnover_df.index = turnover_df.index.tz_localize(tz_zone)
        return net_df, turnover_df
