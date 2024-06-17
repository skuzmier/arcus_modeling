"""
"""
import os
import sys
import pandas as pd
from psycopg2 import sql

sys.path.append(os.path.expanduser('~/ercot_virts/utils'))
from utils import Utils

class Prices:
    def __init__(self, s_date, e_date, nodes=[], **kwargs):
        self.u = Utils()
        self.s_date = s_date
        self.e_date = e_date
        self.nodes = nodes
        self.kwargs = kwargs
        self.market_id = None
        self.da_prices = None
        self.rt_prices = None
        self._setup()

    def _setup(self):
        """
        """
        self._validate_dates()
        self._validate_nodes()
        for key, value in self.kwargs.items():
            setattr(self, key, value)
    
    def run(self):
        """
        """
        self._get_da_prices()
        self._get_rt_prices()
        if hasattr(self, 'localize') and self.localize:
            self.localize_tz()
    
    def _validate_dates(self):
        """
        """
        self.s_date = self.u.time_converter(self.s_date, target='dt')
        self.e_date = self.u.time_converter(self.e_date, target='dt')
        if self.s_date >= self.e_date:
            raise ValueError('start date must be before end date')
    
    def _validate_nodes(self):
        """
        """
        if len(self.nodes) == 0:
            raise ValueError('must provide at least one node')
        if self.nodes[0] == 'ALL':
            raise NotImplementedError('ALL nodes not yet implemented')
        if isinstance(self.nodes[0], str):
            self._get_node_ids()
    
    def _get_node_ids(self):
        """lookup node ids from node names
        """
        node_ids = []
        query = sql.SQL(""" SELECT id FROM location WHERE name IN ({}) """).format(sql.SQL(',').join(map(sql.Literal, self.nodes)))
        res = self.u.execute_query(query, dict_cursor=False, fetch='all')
        for row in res:
            node_ids.append(row[0])
        self.nodes = node_ids
    
    def _get_da_prices(self):
        """
        """
        query = sql.SQL(""" SELECT dt, location.name as node, lmp FROM da_lmp 
                        INNER JOIN location ON da_lmp.node_id = location.id
                        WHERE dt >= {s_date} 
                        AND dt <= {e_date} 
                        AND node_id IN ({nodes})
                         """).format(s_date=sql.Literal(self.s_date),
                                                             e_date=sql.Literal(self.e_date),
                                                             nodes=sql.SQL(',').join(map(sql.Literal, self.nodes)))
        res = self.u.execute_query(query, dict_cursor=True, fetch='all')
        self.da_prices = pd.DataFrame(res)
    
    def _get_rt_prices(self):
        """
        """
        query = sql.SQL(""" SELECT distinct on (node_id, dt) 
                        dt, location.name as node,
                        lmp FROM rtm_lmp 
                        INNER JOIN location ON rtm_lmp.node_id = location.id
                        WHERE dt >= {s_date} 
                        AND dt <= {e_date} 
                        AND node_id IN ({nodes})
                        ORDER BY node_id, dt """).format(s_date=sql.Literal(self.s_date),
                                                             e_date=sql.Literal(self.e_date),
                                                             nodes=sql.SQL(',').join(map(sql.Literal, self.nodes)))
        res = self.u.execute_query(query, dict_cursor=True, fetch='all')
        self.rt_prices = pd.DataFrame(res)
    
    def localize_tz(self):
        """convert UTC to US/Central
        """
        self.da_prices['dt'] = self.da_prices['dt'].dt.tz_convert('US/Central')
        self.da_prices.rename(columns={'dt': 'ldt'}, inplace=True)
        self.rt_prices['dt'] = self.rt_prices['dt'].dt.tz_convert('US/Central')
        self.rt_prices.rename(columns={'dt': 'ldt'}, inplace=True)
    

    

