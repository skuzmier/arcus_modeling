"""parse excel file for results from Arcus
"""
import os
import sys
import datetime
import pandas as pd

sys.path.append(os.path.expanduser("~/ercot_virts/utils"))
from utils import Utils


class ParseExcelFile:
    """ """

    def __init__(self, file, **kwargs):
        """ """
        self.u = Utils()
        self.file = file
        self.market_run = kwargs.get("market_run", None)
        self.sheets = None
        self.df = pd.DataFrame()
        self.valid_node_lookup = {
            "HB": ["HOUSTON", "NORTH", "PAN", "SOUTH", "WEST"],
            "LZ": ["AEN", "CPS", "HOUSTON", "LCRA", "NORTH", "RAYBN", "SOUTH", "WEST"],
        }
        self._get_sheets()
        self._get_market_run()

    def _get_market_run(self):
        """attempt to get market run from sheet name"""
        if self.market_run is not None:
            return
        if "DAM" in self.sheets[0]:
            self.market_run = "DAM"
        elif "RTM" in self.sheets[0]:
            self.market_run = "RTM"
        else:
            raise ValueError(
                f"could not determine market run from sheet name {self.sheets[0]}"
            )

    def _get_node(self, sheet):
        """get node name from sheet name"""
        # get node type either HB or LZ
        if "HB" in sheet:
            node_type = "HB"
        elif "LZ" in sheet:
            node_type = "LZ"
        else:
            raise ValueError(f"could not determine node type from sheet name {sheet}")
        # get node name
        node_name = None
        for node in self.valid_node_lookup[node_type]:
            if node in sheet.upper():
                node_name = node_type + "_" + node
                return node_name
        if node_name is None:
            raise ValueError(f"could not determine node name from sheet name {sheet}")

    def run(self):
        """ """
        for sheet in self.sheets:
            self._get_sheet_data(sheet=sheet)

    def _get_sheets(self):
        """get all sheets in the workbook"""
        xlsx = pd.ExcelFile(self.file)
        self.sheets = xlsx.sheet_names
        
    def _get_sheet_data(self, sheet):
        """get data for a given sheet"""
        df = pd.read_excel(self.file, sheet_name=sheet)
        df = df.iloc[:, :3]
        columns = ["dt_local"]
        if self.market_run == "DAM":
            columns.append("da_actual")
            columns.append("da_pred")
        elif self.market_run == "RTM":
            columns.append("rt_actual")
            columns.append("rt_pred")
        else:
            raise ValueError(f"invalid market run {self.market_run}")
        df["node"] = self._get_node(sheet=sheet)
        columns.append("node")
        df.columns = columns
        self.df = pd.concat([self.df, df], axis=0)
