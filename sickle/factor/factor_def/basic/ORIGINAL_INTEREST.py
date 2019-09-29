import pandas as pd
from ...FactorBase import FactorBase


class ORIGINAL_INTEREST(FactorBase):
    def __init__(self, frequency):
        super(ORIGINAL_INTEREST, self).__init__(frequency)

    def compute(self, start_date=None, end_date=None):
        res_df = pd.DataFrame()
        for contract in self.contracts:
            table_name = 'cc_pre_{0}_add_interest_{1}'.format(self.frequency, contract)
            if start_date is None:
                start_date = str(self.api.earliest_day(table_name))
            if end_date is None:
                end_date = str(self.api.latest_day(table_name))
            temp = self.api.market_data(table_name, 'datetime,open_interest', start_date, end_date)
            if len(temp) > 0:
                temp = temp.rename(columns={'open_interest': contract})
                res_df = pd.concat([res_df, temp], axis=1)
        if len(res_df) > 0:
            self.raw_value = res_df
        else:
            print('No new data!')
