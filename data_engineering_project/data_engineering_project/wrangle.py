







"""
So we get a file path
"""

import netCDF4 as nc

"It would be fancy if we could just send an in memory file object"

def process_nc_file(filename: str):
    with nc.Dataset(filename) as nc_file:
        nc_file