import pyfolio
import os
import pandas as pd
from tzlocal import get_localzone
from sickle.factor.factor_def import ORIGINAL_CLOSE
from sickle.factor.domians.normal_domian_1 import domain_dict
from pandas.plotting import register_matplotlib_converters
register_matplotlib_converters()
user_home = os.path.expanduser('~')


def portfolio_analyser(net_df, position_df, cash_df, frequency):
    # returns
    barly_returns = net_df.pct_change()
    barly_returns.iloc[0] = net_df.iloc[0] - 1
    barly_returns = barly_returns['net_value']
    daily_returns = barly_returns.resample('B').sum()
    # market data
    close_price = ORIGINAL_CLOSE(frequency=frequency)
    close = close_price.get_cached_factor(os.path.join(user_home, 'factor_cache')).sort_index()
    close = close.ffill()
    # positions
    position_amount = (position_df * close).dropna(how='all')
    position_amount['cash'] = cash_df
    tz_zone = get_localzone()
    position_amount.index = position_amount.index.tz_convert(tz_zone)
    position_amount = position_amount.fillna(0)
    position_amount_daily = position_amount.resample('B').last()
    # transactions
    trade = position_df - position_df.shift()
    trade.iloc[0] = position_df.iloc[0]
    trade = pd.DataFrame(trade.stack())
    trade.index.names = ['datetime', 'codes']
    trade.columns = ['amount']
    close_price = pd.DataFrame(close.stack())
    close_price.columns = ['price']
    transactions = pd.concat([trade, close_price], axis=1, join='inner')
    transactions = transactions[transactions['amount'] != 0]
    transactions.index.names = ['index', 'symbol']
    transactions = transactions.reset_index().set_index('index')
    # sector
    sector = domain_dict
    sector_mappings = {}
    for key, value in sector.items():
        for v in value:
            sector_mappings[v] = key
    pyfolio.tears.create_returns_tear_sheet(barly_returns, position_amount, transactions)
    pyfolio.tears.create_interesting_times_tear_sheet(barly_returns)
    pyfolio.tears.create_position_tear_sheet(barly_returns, position_amount, transactions=transactions,
                                             sector_mappings=sector_mappings)
    pyfolio.tears.create_txn_tear_sheet(daily_returns, position_amount_daily, transactions=transactions)
    pyfolio.tears.create_round_trip_tear_sheet(daily_returns, position_amount_daily, transactions=transactions,
                                               sector_mappings=sector_mappings)

# from sickle.factor.FactorBase import FactorBase
# from sickle.factor.factor_def.basic import ORIGINAL_CLOSE, ORIGINAL_OPEN, ORIGINAL_LOW, ORIGINAL_VOLUME, ORIGINAL_HIGH
# from sickle.factor.factor_analyse.FactorAnalyser import FactorAnalyser
# from sickle.factor.factor_analyse.PortfolioPerformace import PortfolioPerformance
# from sickle.factor.factor_analyse.CalIdicator import CalIdicator
# import numpy as np
# from factor_generator.Functions import *
# from factor_generator.Data.BaseData import BaseLoader
# from factor_generator.Functions.express_to_factor import expression_to_df
# perf = PortfolioPerformance('half_day')
# ana = FactorAnalyser()
# loader = BaseLoader('2014-01-01', '2019-07-31', 'half_day', '/home/ddd/factor_data')
# Data_matrix = loader.load_xtrain()
# df = expression_to_df('GdMaRelated2_44,YnGdmax,x_C_LdivH_L', Data_matrix)
# ls_pos = ana.factor_to_portfolio_ls_weight(fac_df=df.dropna(how='all'), side=1, period=14)
# net_df, position_volume, cash_df = perf.long_and_short_perf_optimize_with_numba(ls_pos, 0.0007, 100000000, True)
# portfolio_analyser(net_df, position_volume, cash_df, 'half_day')
