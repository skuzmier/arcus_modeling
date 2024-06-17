"""
"""
from psycopg2 import sql
import sys
import numpy as np
import os

sys.path.append(os.path.expanduser('~/ercot_virts/utils'))
sys.path.append(os.path.expanduser('~/ercot_virts/nrg_stream'))
sys.path.append(os.path.expanduser('~/ercot_virts'))

from utils import Utils
from stream_record import StreamRecord
from nrg_stream import NRGStream
from helpers import Helpers


class BaseScraper:
    """
    """
    def __init__(self, stream_id, s_date, e_date, **kwargs):
        """
        """
        self.u = Utils()
        self.stream_id = stream_id
        self.s_date = s_date
        self.e_date = e_date
        self.kwargs = kwargs
        self.stream_record = None
        self.hlp = Helpers()
        self.df = None
        self._setup()

    def _setup(self):
        """
        """
        self.stream_record = StreamRecord(stream_id=self.stream_id)
        if self.stream_record.id is None:
            raise ValueError(f'no stream record with id {self.stream_id}')
        if self.stream_record.db_info.table not in ['dam_lmp', 'rtm_lmp', 'rtpd_lmp', 'fmm_lmp']:
            raise ValueError(f'stream record with id {self.stream_id} is not a price stream')
        self._validate_dates()
        self.stream_record = StreamRecord(stream_id=self.stream_id)
    
    def _validate_dates(self):
        """
        """
        self.s_date = self.u.time_converter(self.s_date, target='dt')
        self.e_date = self.u.time_converter(self.e_date, target='dt')
        if self.s_date >= self.e_date:
            raise ValueError('start date must be before end date')
    
    def run(self):
        """
        """
        self._get_data()
        self._get_columns()
        self._filter_existing_data()
        self._load_data()
    
    def _get_data(self):
        """
        """
        api = NRGStream(username='spencer', password='nrg4spencer')
        api.get_stream(self.stream_id, self.s_date, self.e_date, data_format='json')
        self.df = api.data
    
    def _get_columns(self, api):
        """get column names, add energy, congestion, and loss if needed
        """
        columns = []
        for col in api.meta['columns']:
            columns.append(col['name'].lower())
        self.df.columns = columns
        if self.stream_record.iso == 'ERCOT':
            self._add_columns_ercot()
        else:
            self._add_columns()
        self.df['iso_id'] = self.stream_record.iso_id
        self.df['node_id'] = self.stream_record.location_id
        self.df == self.df[['iso_id', 'node_id', 'dt', 'lmp', 'energy', 'congestion', 'loss']]
    
    def _add_columns_ercot(self):
        """ercot doesn't have energy, congestion, and loss columns add them
        """
        self.df.rename({'date/time': 'dt', 'price': 'lmp'}, inplace=True)
        self.df['energy'] = np.nan
        self.df['congestion'] = np.nan
        self.df['loss'] = np.nan
    
    def _add_columns(self):
        """fix column names and add energy col for markets which break out prices
        """
        self.df.rename({'date/time': 'dt', 'price': 'lmp'}, inplace=True)
        self.df['energy'] = self.df['lmp'] - self.df['congestion'] - self.df['loss']
    
    def _filter_existing_data(self):
        """remove data that may have already been loaded
        """
        if self.stream_record.first_date is not None:
            self.df = self.df[self.df['dt'] < self.stream_record.first_date]
        if self.stream_record.first_date is not None:
            self.df = self.df[self.df['dt'] > self.stream_record.last_date]
    
    # def _load_data(self):
    #     """
    #     """
    #     try:
    #         self.u.load_df(self.df, self.stream_record.db_info.table)
    #         self.

        
