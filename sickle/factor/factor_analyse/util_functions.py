import pandas as pd
from sickle.factor.factor_analyse.PortfolioPerformace import PortfolioPerformance


def performance_daily(ls_pos, frequency, cost):
    perf = PortfolioPerformance(frequency)
    barly_return = pd.DataFrame()
    barly_turn = pd.DataFrame()

    def f(x):
        global barly_return
        global barly_turn
        net, turn = perf.long_and_short_perf_optimize_with_numba(x, cost)
        bar_return = net.pct_change()
        bar_return.iloc[0] = net.iloc[0] - 1
        barly_return = barly_return.append(bar_return)
        barly_turn = barly_turn.append(turn)

    ls_pos.groupby([ls_pos.index.year, ls_pos.index.month, ls_pos.index.day]).apply(f)

    net_value = (1 + barly_return).cumprod()
    return net_value, barly_turn


def performance_monthly(ls_pos, frequency, cost):
    perf = PortfolioPerformance(frequency)
    barly_return = pd.DataFrame()
    barly_turn = pd.DataFrame()

    def f(x):
        global barly_return
        global barly_turn
        net, turn = perf.long_and_short_perf_optimize_with_numba(x, cost)
        bar_return = net.pct_change()
        bar_return.iloc[0] = net.iloc[0] - 1
        barly_return = barly_return.append(bar_return)
        barly_turn = barly_turn.append(turn)

    ls_pos.groupby([ls_pos.index.year, ls_pos.index.month]).apply(f)

    net_value = (1 + barly_return).cumprod()
    return net_value, barly_turn


def performance_yearly(ls_pos, frequency, cost):
    perf = PortfolioPerformance(frequency)
    barly_return = pd.DataFrame()
    barly_turn = pd.DataFrame()

    def f(x):
        global barly_return
        global barly_turn
        net, turn = perf.long_and_short_perf_optimize_with_numba(x, cost)
        bar_return = net.pct_change()
        bar_return.iloc[0] = net.iloc[0] - 1
        barly_return = barly_return.append(bar_return)
        barly_turn = barly_turn.append(turn)

    ls_pos.groupby([ls_pos.index.year]).apply(f)

    net_value = (1 + barly_return).cumprod()
    return net_value, barly_turn