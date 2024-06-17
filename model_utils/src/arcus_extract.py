"""Build extracts for Arcus data
"""
import os
import sys

sys.path.append(os.path.expanduser("~/ercot_virts/utils"))
sys.path.append(os.path.expanduser("~/ercot_virts"))
sys.path.append(os.path.expanduser("~/ercot_virts/time_series"))
sys.path.append(os.path.expanduser("~/ercot_virts/model_utils"))


from utils import Utils
from feature_ts import FeatureTS
import datetime
from arcus_derived_features import ArcusDerivedFeatures
from arcus_streams import dam_run_streams, rtm_run_streams, five_min_streams


class ArcusErcotExtractBuilder:
    """
    """
    def __init__(
        self,
        s_date,
        e_date,
        market_run,
        stream_mappings=None,
        **kwargs
    ):
        """ """
        self.u = Utils()
        self.s_date = s_date
        self.e_date = e_date
        self.market_run = market_run
        self.stream_mappings = stream_mappings
        self.kwargs = kwargs
        self.feature_ts = None
        self.feature_ts_records = []
        self.df = None
        self._setup()

    def _setup(self) -> None:
        """ """
        # confirm dates are valid
        self._validate_dates()
        # get stream mapping
        self._get_stream_mapping()
        # initialize feature_ts
        self.feature_ts = FeatureTS(
            s_date=self.s_date,
            e_date=self.e_date,
            feature_mappings=self.stream_mappings,
        )

    def _get_stream_mapping(self) -> None:
        """lookup default stream mappings based on market run if not provided
        """
        # do not overwrite if stream_mappings is provided
        if self.stream_mappings is not None:
            return None
        if self.market_run == 'DAM':
            self.stream_mappings = dam_run_streams
        elif self.market_run == 'RTM':
            self.stream_mappings = rtm_run_streams
        elif self.market_run == '5MIN':
            self.stream_mappings = five_min_streams
        else:
            raise ValueError("Invalid market run")

    def _validate_dates(self) -> None:
        """ """
        self.s_date = self.u.time_converter(self.s_date, target='dt')
        self.e_date = self.u.time_converter(self.e_date, target='dt')
        if self.s_date >= self.e_date:
            raise ValueError("Start date must be before end date")

    def _get_arcus_derived_features(self):
        """ """
        self.df = self.feature_ts.run()
        adf = ArcusDerivedFeatures(df=self.df, market_run=self.market_run)
        adf.run()
        self.df = adf.df

    def run(self) -> None:
        """ """
        self._get_arcus_derived_features()
        # some stuff will go here later
