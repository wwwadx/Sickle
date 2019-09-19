# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import math
import empyrical


class CalIdicator:
    def __init__(self, net_value, turnover=None):
        self.net_value = pd.Series(data=net_value.values.flatten(), index=net_value.index)
        self.bar_return = self.net_value.pct_change()
        self.bar_return.iloc[0] = self.net_value.iloc[0] - 1
        self.net_value_day = self.net_value.resample('B').last().dropna()
        self.daily_return = self.net_value_day.pct_change()
        self.daily_return.iloc[0] = self.net_value_day.iloc[0] - 1
        if turnover is not None:
            turnover = turnover.resample('B').sum().dropna()
            self.turn_over = pd.Series(data=turnover.values.flatten(), index=turnover.index)
        else:
            self.turn_over = turnover

    # 计算最长创新高时间
    def longest_new_high_time(self):
        temp = pd.DataFrame(self.net_value)
        newHighTS = pd.DataFrame()
        newHighTS = newHighTS.append(temp[0:1])
        i = 1
        while (i <= len(temp)):
            if temp[i - 1:i].values[0][0] > newHighTS[-1:].values[0][0]:  # 判断取出创新高的数据
                newHighTS = newHighTS.append(temp[i - 1:i])
            i += 1
        diff = newHighTS.index[1:].to_pydatetime() - newHighTS.index[:-1].to_pydatetime()
        longestNewHigh = max((temp.index[:-1][-1] - newHighTS.index[1:].max()).days,
                             diff.max().days)  # 将最长创新高时间与历史新高到结尾日期间隔相比，求出最长创新高时间
        return longestNewHigh

    def profit_distribution(self):
        year_return = self.net_value_day.groupby(
            [self.net_value_day.index.year]
        ).apply(
            lambda x: x.iloc[-1] - x.iloc[0]
        )
        year_mdrawdown = self.daily_return.groupby(
            [self.daily_return.index.year]
        ).apply(
            lambda x: empyrical.max_drawdown(x)
        )
        sharpe_ratio = self.daily_return.groupby(
            [self.daily_return.index.year]
        ).apply(
            lambda x: empyrical.sharpe_ratio(x)
        )
        profit_factor = self.daily_return.groupby(
            [self.daily_return.index.year]
        ).apply(
            lambda x: x[x > 0].dropna().sum() / abs(x[x < 0].dropna().sum())
        )
        win_bar = self.bar_return.groupby(
            [self.bar_return.index.year]
        ).apply(
            lambda x: len(x[x > 0].dropna()) / len(x)
        )
        win_day = self.daily_return.groupby(
            [self.daily_return.index.year]
        ).apply(
            lambda x: len(x[x > 0].dropna()) / len(x)
        )
        week_netvalue = self.net_value.resample('W').last().dropna()
        week_return = week_netvalue.pct_change()
        week_return.iloc[0] = week_netvalue.iloc[0] - 1
        win_week = week_return.groupby(
            [week_return.index.year]
        ).apply(
            lambda x: len(x[x > 0].dropna()) / len(x)
        )
        month_netvalue = self.net_value.resample('M').last().dropna()
        month_return = month_netvalue.pct_change()
        month_return.iloc[0] = month_netvalue.iloc[0] - 1
        win_month = month_return.groupby(
            [month_return.index.year]
        ).apply(
            lambda x: len(x[x > 0].dropna()) / len(x)
        )

        # 盈利分布
        def f(x):
            temp = self.win_loss_distribution(x.values, x.index)
            max_win = max(temp['win_or_loss'])
            max_loss = min(temp['win_or_loss'])
            max_win_pnl = max(temp['pnl'])
            max_loss_pnl = min(temp['pnl'])
            # [max_win, max_loss, max_win_pnl, max_loss_pnl]
            return {'max_win': max_win, 'max_loss': max_loss, 'max_win_pnl': max_win_pnl, 'max_loss_pnl': max_loss_pnl}

        win_loss_dis_bar = self.bar_return.groupby(
            [self.bar_return.index.year]
        ).apply(
            lambda x: f(x)
        )
        win_loss_dis_bar = win_loss_dis_bar.unstack()
        win_loss_dis_bar = win_loss_dis_bar.rename(
            columns={'max_win': '最大连赢bars', 'max_loss': '最大连亏bars', 'max_win_pnl': '最大连赢幅度bars',
                     'max_loss_pnl': '最大连亏幅度bars'})
        win_loss_dis_day = self.daily_return.groupby(
            [self.daily_return.index.year]
        ).apply(
            lambda x: f(x)
        )
        win_loss_dis_day = win_loss_dis_day.unstack()
        win_loss_dis_day = win_loss_dis_day.rename(
            columns={'max_win': '最大连赢天数', 'max_loss': '最大连亏天数', 'max_win_pnl': '最大连赢幅度',
                     'max_loss_pnl': '最大连亏幅度'})
        if self.turn_over is not None:
            year_turnover = self.turn_over.groupby(
                [self.turn_over.index.year]
            ).mean()
            total_turnover = self.turn_over.mean()
        else:
            year_turnover = None
            total_turnover = None
        year_per = pd.DataFrame(
            {
                '收益': year_return,
                '最大回撤': year_mdrawdown,
                '夏普': sharpe_ratio,
                'bar胜率': win_bar,
                '日胜率': win_day,
                '周胜率': win_week,
                '月胜率': win_month,
                '盈利因子': profit_factor,
                '换手': year_turnover
            }
        )
        year_per = pd.concat([year_per, win_loss_dis_day, win_loss_dis_bar], axis=1)
        win_loss_dis_tot_bar = self.win_loss_distribution(self.bar_return.values, self.bar_return.index)
        win_loss_dis_tot_day = self.win_loss_distribution(self.daily_return.values, self.daily_return.index)
        year_per = year_per.append(
            pd.Series(
                {
                    '收益': empyrical.annual_return(self.daily_return),
                    '最大回撤': empyrical.max_drawdown(self.daily_return),
                    '夏普': empyrical.sharpe_ratio(self.daily_return),
                    'bar胜率': len(self.bar_return[self.bar_return > 0].dropna()) / len(self.bar_return),
                    '日胜率': len(self.daily_return[self.daily_return > 0].dropna()) / len(self.daily_return),
                    '周胜率': len(week_return[week_return > 0].dropna()) / len(week_return),
                    '月胜率': len(month_return[month_return > 0].dropna()) / len(month_return),
                    '盈利因子': self.daily_return[self.daily_return > 0].dropna().sum() / abs(self.daily_return[self.daily_return < 0].dropna().sum()),
                    '换手': total_turnover,
                    '最大连赢天数': max(win_loss_dis_tot_day['win_or_loss']),
                    '最大连亏天数': min(win_loss_dis_tot_day['win_or_loss']),
                    '最大连赢幅度': max(win_loss_dis_tot_day['pnl']),
                    '最大连亏幅度': min(win_loss_dis_tot_day['pnl']),
                    '最大连赢bars': max(win_loss_dis_tot_bar['win_or_loss']),
                    '最大连亏bars': min(win_loss_dis_tot_bar['win_or_loss']),
                    '最大连赢幅度bars': max(win_loss_dis_tot_bar['pnl']),
                    '最大连亏幅度bars': min(win_loss_dis_tot_bar['pnl'])

                }, name='annual'
            )
        )
        return round(year_per, 4)

    def profit_distribution_total(self):
        year_per = pd.DataFrame()
        year_per = year_per.append(
            pd.Series(
                {
                    '收益': empyrical.annual_return(self.daily_return),
                    '最大回撤': empyrical.max_drawdown(self.daily_return),
                    '夏普': empyrical.sharpe_ratio(self.daily_return),
                }, name='annual'
            )
        )
        return year_per

    def profit_distribution_for_mining(self):
        sharpe_ratio = self.daily_return.groupby(
            [self.daily_return.index.year]
        ).apply(
            lambda x: empyrical.sharpe_ratio(x)
        )
        year_turnover = self.turn_over.groupby(
            [self.turn_over.index.year]
        ).mean()
        total_turnover = self.turn_over.mean()
        year_per = pd.DataFrame(
            {
                '夏普': sharpe_ratio,
                '换手': year_turnover
            }
        )

        year_per = year_per.append(
            pd.Series(
                {
                    '夏普': empyrical.sharpe_ratio(self.daily_return),
                    '换手': total_turnover
                }, name='annual'
            )
        )
        return round(year_per, 4)

    def win_loss_distribution(self, pnl_array, datetime_array, res_type='c'):
        """
        连赢连输分布
        :param pnl_array:
        :param datetime_array:
        :param res_type: 返回结果类型，'c': 复杂结果，Dataframe， 's'：简单结果，list
        :param freq:
        :return:
        """
        dis = pnl_array.copy()
        dis[dis > 0] = 1
        dis[dis < 0] = -1
        dis = dis.astype(int)
        # label: current dis value
        label = 0
        result_pnl_list = []
        result_dis_list = []
        result_date_list = []
        for i in range(len(dis)):
            pnl_value = pnl_array[i]
            dis_value = dis[i]
            date_value = datetime_array[i]  # .date()
            # the beginning of the algo. append value to result list
            if label == 0:
                result_pnl_list.append(pnl_value)
                result_dis_list.append(dis_value)
                result_date_list.append([date_value])
                label = dis_value
            else:
                # when label equals dis, means the win or loss is continuous
                if label == dis_value:
                    result_pnl_list[-1] += pnl_value
                    result_dis_list[-1] += dis_value
                    result_date_list[-1].append(date_value)
                else:
                    result_dis_list.append(dis_value)
                    result_pnl_list.append(pnl_value)
                    result_date_list.append([date_value])
                    label = dis_value
        if res_type == 'c':
            result = pd.DataFrame(
                {
                    'datetime': [i[-1] for i in result_date_list],
                    'pnl': result_pnl_list,
                    'win_or_loss': result_dis_list
                }
            )
        elif res_type == 's':
            result = np.array(result_dis_list)
        else:
            raise Exception('Wrong res_type!Check parameters.')
        return result
