"""
"""
import os
import sys
import pandas as pd
import numpy as np

sys.path.append(os.path.expanduser("~/ercot_virts/utils"))
from utils import Utils


class ArcusDerivedFeatures:
    """generate derived features for Arcus data
    """
    def __init__(self, df, **kwargs):
        """
        """
        self.u = Utils()
        self.df = df
        self.kwargs = kwargs
        self.feature_records = []
        self.solar_weights = {'North_HZ': 0.0945, 'South_HZ': 0.0790, 'West_HZ': 0.8772, 'Houston_HZ': 0.01}
        self.thermal_weights = {'North_HZ': 0.029, 'South_HZ': 0.015, 'West_HZ': 0.030, 'Houston_HZ': 0.011}
        self.lag_mappings = {"1D": ['DAM_North_HZ_price', 'DAM_South_HZ_price', 'DAM_West_HZ_price', 
                        'DAM_Houston_HZ_price', 'DAM_PAN_HZ_price', 'DAM_North_LZ_price',
                        'DAM_South_LZ_price', 'DAM_West_LZ_price', 'DAM_Houston_LZ_price',
                        'DAM_AEN_LZ_price', 'DAM_CPS_LZ_price', 'DAM_LCRA_LZ_price',
                        'DAM_RAYB_LZ_price']}
        self.market_run = kwargs.get("market_run", None)
        self.debug = True
        self._setup()

    def _setup(self):
        """
        """
        # Check df is not empty
        if len(self.df) == 0:
            raise ValueError("Dataframe is empty")
        # check df has a datetime index
        if not isinstance(self.df.index, pd.DatetimeIndex):
            raise ValueError("Dataframe must have a datetime index")
        # check df has a frequency
        if self.df.index.freq is None:
            raise ValueError("Dataframe must have a frequency")

    def _apply_forecast_weights(self, base_col, weights, prefix):
        """
        """
        for k, v in weights.items():
            self.df[f"{prefix}_{k}"] = self.df[base_col] * v

    def lag_feature(self, col, timedelta_str, periods=None):
        """
            Args:
            -----
            col: str
                column to lag
            timedelta_str: str
                string representation of the timedelta to lag by, see pandas.to_timedelta for more info
            periods: int, optional
                number of periods to lag by
        """
        # check if both timedelta_str or periods are provided not both
        if timedelta_str is not None and periods is not None:
            raise ValueError("Provide either timedelta_str or periods, not both")
        # confirm timedelta_str is valid
        if timedelta_str is not None:
            try:
                lag_interval = pd.to_timedelta(timedelta_str)
            except:
                raise ValueError(f"Invalid timedelta string: {timedelta_str}")
        # confirm timedelta_str is greater than or equal to df index freq
        if lag_interval < pd.to_timedelta(self.df.index.freq):
            raise ValueError("Lag interval must be greater than or equal to the dataframe index frequency")
        # determine the number of periods to lag by
        if periods is None:
            periods = int(lag_interval / pd.to_timedelta(self.df.index.freq))
        if timedelta_str is not None:
            self.df[f"{col}_{timedelta_str}_back"] = self.df[col].shift(periods)
        else:
            self.df[f"{col}_{periods}periods_back"] = self.df[col].shift(periods)

    def _get_cyclical_features(self):
        """get cyclical encoding of datetime, allowing the model to understand the cyclical nature of time
        """
        dt_val = self.df.index
        self.df['day_sin'] = np.sin(2 * np.pi * dt_val.hour / 24)
        self.df['day_cos'] = np.cos(2 * np.pi * dt_val.hour / 24)
        self.df['week_sin'] = np.sin(2 * np.pi * dt_val.dayofweek / 7)
        self.df['week_cos'] = np.cos(2 * np.pi * dt_val.dayofweek / 7)
        self.df['hour_sin'] = np.sin(2 * np.pi * dt_val.minute / 60)
        self.df['hour_cos'] = np.cos(2 * np.pi * dt_val.minute / 60)
        self.df['year_sin'] = np.sin(2 * np.pi * dt_val.dayofyear / 365)
        self.df['year_cos'] = np.cos(2 * np.pi * dt_val.dayofyear / 365)

    def _calc_dda_cols(self):
        """NOTE: I do not understand the purpose of this code. Verify with Arcus team
        """
        self.df["DDapprox_For_North_HZ"] = (
            self.df["LoadForecast_West_HZ"]
            - self.df["ThermalForecast_West_HZ"]
            - (self.df["SolarForecast_West_HZ"] + self.df["WindForecast_West_HZ"])
        )
        self.df["DDapprox_For_South_HZ"] = (
            self.df["LoadForecast_Houston_HZ"]
            - self.df["North_to_Houston_DAM_Lim"]
            - self.df["ThermalForecast_Houston_HZ"]
            - (self.df["SolarForecast_South_HZ"] + self.df["WindForecast_Houston_HZ"])
        )
        self.df["DDapprox_For_West_HZ"] = (
            self.df["LoadForecast_West_HZ"]
            - self.df["ThermalForecast_West_HZ"]
            - (self.df["SolarForecast_West_HZ"] + self.df["WindForecast_West_HZ"])
        )
        self.df["DDapprox_For_Houston_HZ"] = (
            self.df["LoadForecast_Houston_HZ"]
            - self.df["North_to_Houston_DAM_Lim"]
            - self.df["ThermalForecast_Houston_HZ"]
            - (self.df["SolarForecast_Houston_HZ"] + self.df["WindForecast_Houston_HZ"])
        )

    def _calculate_combined_features(self):
        """I am not cleary why this is happening. Verify with Arcus team
        """
        self.df['WindActual_West_HZ'] = self.df['WindActual_West_HZ_aux'] + self.df['WindActual_North_HZ']
        self.df['LoadActual_West_HZ'] = self.df['LoadActual_West_HZ_aux'] + self.df['LoadActual_North_HZ'] # Load Actual North HZ 
        self.df['WindForecast_West_HZ'] = self.df['WindForecast_North_HZ'] + self.df['WindForecast_West_HZ_aux']
        self.df['LoadForecast_West_HZ'] = self.df['LoadForecast_North_HZ'] + self.df['LoadForecast_West_HZ_aux']

    def get_weighted_cols(self):
        """ apply weights to the solar and thermal forecasts
        """
        self._apply_forecast_weights("SolarForecast_ERCOT", self.solar_weights, "SolarForecast")
        self._apply_forecast_weights("ThermalForecast_ERCOT", self.thermal_weights, "ThermalForecast")

    def lag_features(self):
        """ create lag features for the columns in self.lag_mappings
        """
        for k, v in self.lag_mappings.items():
            for col in v:
                self.lag_feature(col, timedelta_str=k)

    def run(self, market_run=None):
        """
        """
        if market_run is not None:
            self.market_run = market_run
        if self.market_run == "DAM":
            self._run_dam()
        elif self.market_run == "RTM":
            self._run_rtm()
        else:
            raise ValueError("Invalid market run")

    def _set_default_values(self):
        """set default mappings for missing data, if ffill and bfill limit 10 not sufficent"""
        defaults = {
            "North_to_Houston_DAM_Lim": 5500,
            "East_to_Texas_DAM_Lim": 9999,
            "Williamson_to_Burnet_DAM_Lim": 9999,
            "Culberson_DAM_Lim": 9999,
            "McCamey_DAM_Lim": 2999,
            "WestTexas_DAM_Lim": 10000,
            "Nelson_Sharpe_DAM_Lim": 700,
            "Nedin_to_Lobo_DAM_Lim": 980,
            "Raymondville_to_Riohondo_DAM_Lim": 9999,
            "Redtap_DAM_Lim": 9999,
            "Valley_Export_DAM_Lim": 10100,
            "Zapata_to_Starr_DAM_Lim": 9999,
        }
        for k, v in defaults.items():
            self.df[k] = self.df[k].ffill(limit=10).bfill(limit=10).fillna(v)

    def _run_dam(self):
        """setup data for a dam training run
        """
        self.get_weighted_cols()
        self._calculate_combined_features()
        self._calc_dda_cols()
        self.lag_features()
        self._get_cyclical_features()
        self.df.ffill(inplace=True)
        self.df.bfill(inplace=True)
    
    def _run_rtm(self):
        """setup data for a rtm training run
        """
        self.get_weighted_cols()
        self._calculate_combined_features()
        self._calc_dda_cols()
        self.lag_features()
        self._get_cyclical_features()
        self.df.ffill(inplace=True)
        self.df.bfill(inplace=True)

# Good dataset id 663445186c70aa4b12d52b98
