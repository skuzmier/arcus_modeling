"""
"""

import pandas as pd
import datetime
import pytz


class TZConverter:
    """convert time zones
    """
    default_time_zones = pytz.all_timezones
    custom_time_zones = {'MPT': 'US/Mountain', 'CPT': 'US/Central', 'EPT': 'US/Eastern', 'PPT': 'US/Pacific'} 

    def __init__(self, df, source_tz, target_tz, date_col):
        """
            Args:
            -----
                df: pandas.DataFrame
                source_tz: str
                target_tz: str
        """
        self.df = df
        self.source_tz = source_tz
        self.target_tz = target_tz
        self.date_col = date_col
        self._validate_inputs()
        self._get_common_tz_name()
    
    def _validate_inputs(self):
        """
        """
        if not isinstance(self.df, pd.DataFrame):
            raise TypeError('df must be a pandas.DataFrame')
        if not isinstance(self.source_tz, str):
            raise TypeError('source_tz must be a str')
        self.source_tz = self.source_tz.upper()
        if not isinstance(self.target_tz, str):
            raise TypeError('target_tz must be a str')
        self.target_tz = self.target_tz.upper()
        if self.source_tz not in self.default_time_zones and self.source_tz not in self.custom_time_zones:
            raise ValueError(f'source_tz: {self.source_tz} is not a valid time zone')
        if self.target_tz not in self.default_time_zones and self.target_tz not in self.custom_time_zones:
            raise ValueError(f'target_tz: {self.source_tz} is not a valid time zone')
        if self.source_tz == self.target_tz:
            raise ValueError('source_tz and target_tz must be different')
    
    def _get_common_tz_name(self):
        """if the tz is a custom tz, get the common name
        """
        if self.source_tz in self.custom_time_zones:
            self.source_tz = self.custom_time_zones[self.source_tz]
        if self.target_tz in self.custom_time_zones:
            self.target_tz = self.custom_time_zones[self.target_tz]
    
    def run(self):
        """
        """
        self.df[self.date_col] = pd.to_datetime(self.df[self.date_col])
        try:
            self.df[self.date_col] = self.df[self.date_col].dt.tz_localize(self.source_tz, ambiguous='infer', nonexistent='NaT')
            self.df = self.df.loc[self.df[self.date_col] != 'NaT']
        except Exception as e:
            try:
                self.df[self.date_col] = self.df[self.date_col].dt.tz_localize(self.source_tz, ambiguous='NaT', nonexistent='NaT')
                self.df = self.df.loc[self.df[self.date_col] != 'NaT']
                self.df = self.df.dropna(subset=[self.date_col])
            except Exception as e:
                raise ValueError(f'error localizing time zone: {e}')
        self.df[self.date_col] = self.df[self.date_col].dt.tz_convert(self.target_tz)
        return self.df

    
