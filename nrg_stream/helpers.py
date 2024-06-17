"""
"""
import pandas as pd
import datetime
from tz_converter import TZConverter

class Helpers:
    """
    """

    def __init__(self):
        """
        """
        self.market_aliases = {'ERCOT': ['ERC', 'ERCOT'],
                          'CAISO': ['CA', 'CAISO'],
                          'MISO': ['MISO'],
                          'AESO': ['AEISO', 'AB']}
    
    def get_market_common_name(self, market_alias):
        """
        """
        for (market, aliases) in self.market_aliases.items():
            if market_alias in aliases:
                return market
        return market_alias
    
    def get_freq(self, df, date_col):
        """get the frequency of a timeseries
        """
        return pd.infer_freq(df[date_col])
    
    def convert_tz(self, df, source_tz, target_tz, date_col):
        """
        """
        tz_converter = TZConverter(df, source_tz, target_tz, date_col)
        return tz_converter.run()
