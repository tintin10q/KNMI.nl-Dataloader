"""
So we get a file path
"""
import glob
from pathlib import Path
from typing import Union, List

import netCDF4 as nc
import numpy as np

from data_engineering_project.db_connection import get_connection
from data_engineering_project.to_save import ALL_KNMI_VARIABLES

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
    db = nc.Dataset(filename) # Step 2
    try:
        time = db.variables['time'][:].item()  # step 3

        to_insert = []  # List of data to insert into the database in the end.
        # For each station iterate each key in the nc file
        for index, station in enumerate(db.variables['station']):
            to_insert.append([])  # Step 4
            for key in ALL_KNMI_VARIABLES: # Step 5

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


column_names = ', '.join(ALL_KNMI_VARIABLES)
placeholders = ', '.join(['?'] * len(ALL_KNMI_VARIABLES))
insert_statement = f"INSERT INTO Measurement ({column_names}) VALUES ({placeholders})"
# Here we fix the issue that some keys have an - in them. Terrible but I think this is the best way:
import re

insert_statement = re.sub(r"([A-Z0-9a-z]+-[A-Z0-9a-z]+)", r'"\1"', insert_statement)

def insert_nc_filerows(result: Measurements): # Step 6
    con = get_connection()
    con.executemany(insert_statement, result)


if __name__ == '__main__':
    from tqdm import tqdm

    indexes = set()
    for filename in tqdm(glob.glob("nc_files/*.nc")):
        result = process_nc_file(filename)
        insert_nc_filerows(result)
