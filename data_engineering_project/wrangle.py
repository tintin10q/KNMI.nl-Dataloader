"""
So we get a file path
"""
import glob
from pathlib import Path
from typing import Union, List

import netCDF4 as nc
import numpy as np

from data_engineering_project.db_connection import get_connection

import toml

nc_keys_to_save = (
    "station", "time", "wsi", "stationname", "lat", "lon", "height", "D1H", "dd", "dn", "dr", "dsd", "dx", "ff", "ffs",
    "fsd", "fx", "fxs", "gff", "gffs", "h", "h1", "h2", "h3", "hc", "hc1", "hc2", "hc3", "n", "n1", "n2", "n3",
    "nc", "nc1", "nc2", "nc3", "p0", "pp", "pg", "pr", "ps", "pwc", "Q1H", "Q24H", "qg", "qgn", "qgx", "qnh", "R12H",
    "R1H", "R24H", "R6H", "rg", "rh", "rh10", "Sav1H", "Sax1H", "Sax3H", "Sax6H", "sq", "ss", "Sx1H", "Sx3H", "Sx6H",
    "t10", "ta", "tb", "tb1", "Tb1n6", "Tb1x6", "tb2", "Tb2n6", "Tb2x6", "tb3", "tb4", "tb5", "td", "td10", "tg",
    "tgn", "Tgn12", "Tgn14", "Tgn6", "tn", "Tn12", "Tn14", "Tn6", "tsd", "tx", "Tx12", "Tx24", "Tx6", "vv", "W10",
    "W10-10", "ww", "ww-10", "zm")



Measurements = List[List[str | int | float]]


def process_nc_file(filename: str | Path) -> Measurements:
    """
       Process a single NetCDF file and return a list of lists with values in the order of nc_keys_to_save.

       :param filename: Path to the NetCDF file to be processed.
       :type filename: str | Path
       :return: A list of lists containing data for each station in the order specified by nc_keys_to_save.
       :rtype: List[List[Union[str,float,int]]]
    """

    # In a width statement it sadly does not seem to know the type of the value
    db = nc.Dataset(filename)
    try:
        time = db.variables['time'][:].item()

        to_insert = []  # List of data to insert into the database in the end.
        # For each station iterate each key in the nc file
        for index, station in enumerate(db.variables['station']):
            to_insert.append([])
            for key in nc_keys_to_save:

                if key == "time":
                    to_insert[index].append(time)
                    continue

                station_reading = db.variables[key][index]

                is_string = isinstance(station_reading, str)
                if is_string:
                    to_insert[index].append(station_reading)
                    continue

                # Else check for a masked array
                is_masked = False
                if isinstance(station_reading, np.ma.core.MaskedArray) and station_reading.mask:
                    is_masked = station_reading.mask[0]

                if is_masked:
                    to_insert[index].append(None)
                    continue

                value = station_reading.item()  # Otherwise just get the item
                to_insert[index].append(value)

    finally:
        db.close()

    return to_insert


column_names = ', '.join(nc_keys_to_save)
placeholders = ', '.join(['?'] * len(nc_keys_to_save))
insert_statement = f"INSERT INTO Measurement ({column_names}) VALUES ({placeholders})"
# Here we fix the issue that some keys have a - in them. Terrible but I think this is the best way:
import re

insert_statement = re.sub(r"([A-Z0-9a-z]+-[A-Z0-9a-z]+)", r'"\1"', insert_statement)

def insert_nc_filerows(result: Measurements):
    con = get_connection()
    con.executemany(insert_statement, result)


if __name__ == '__main__':
    from tqdm import tqdm

    indexes = set()
    for filename in tqdm(glob.glob("nc_files/*.nc")):
        result = process_nc_file(filename)
        insert_nc_filerows(result)
