import os
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse
from pyarrow import parquet
from torch.utils.data import IterableDataset
from typing import Literal, Set

# Single api endpoint

## Update both, idk how to make a type out of the type so for now just have 2

ALL_KNMI_VARIABLES = frozenset((
    "station", "time", "wsi", "stationname", "lat", "lon", "height", "D1H", "dd", "dn", "dr", "dsd", "dx", "ff", "ffs",
    "fsd", "fx", "fxs", "gff", "gffs", "h", "h1", "h2", "h3", "hc", "hc1", "hc2", "hc3", "n", "n1", "n2", "n3",
    "nc", "nc1", "nc2", "nc3", "p0", "pp", "pg", "pr", "ps", "pwc", "Q1H", "Q24H", "qg", "qgn", "qgx", "qnh", "R12H",
    "R1H", "R24H", "R6H", "rg", "rh", "rh10", "Sav1H", "Sax1H", "Sax3H", "Sax6H", "sq", "ss", "Sx1H", "Sx3H", "Sx6H",
    "t10", "ta", "tb", "tb1", "Tb1n6", "Tb1x6", "tb2", "Tb2n6", "Tb2x6", "tb3", "tb4", "tb5", "td", "td10", "tg",
    "tgn", "Tgn12", "Tgn14", "Tgn6", "tn", "Tn12", "Tn14", "Tn6", "tsd", "tx", "Tx12", "Tx24", "Tx6", "vv", "W10",
    "W10-10", "ww", "ww-10", "zm"))


KNMI_VARIABLE = Literal[
    "station", "time", "wsi", "stationname", "lat", "lon", "height", "D1H", "dd", "dn", "dr", "dsd", "dx", "ff", "ffs",
    "fsd", "fx", "fxs", "gff", "gffs", "h", "h1", "h2", "h3", "hc", "hc1", "hc2", "hc3", "n", "n1", "n2", "n3",
    "nc", "nc1", "nc2", "nc3", "p0", "pp", "pg", "pr", "ps", "pwc", "Q1H", "Q24H", "qg", "qgn", "qgx", "qnh", "R12H",
    "R1H", "R24H", "R6H", "rg", "rh", "rh10", "Sav1H", "Sax1H", "Sax3H", "Sax6H", "sq", "ss", "Sx1H", "Sx3H", "Sx6H",
    "t10", "ta", "tb", "tb1", "Tb1n6", "Tb1x6", "tb2", "Tb2n6", "Tb2x6", "tb3", "tb4", "tb5", "td", "td10", "tg",
    "tgn", "Tgn12", "Tgn14", "Tgn6", "tn", "Tn12", "Tn14", "Tn6", "tsd", "tx", "Tx12", "Tx24", "Tx6", "vv", "W10",
    "W10-10", "ww", "ww-10", "zm"]

KNMI_NUMERIC_VARIABLE = Literal[
     "time", "lat", "lon", "height", "D1H", "dd", "dn", "dr", "dsd", "dx", "ff", "ffs",
    "fsd", "fx", "fxs", "gff", "gffs", "h", "h1", "h2", "h3", "hc", "hc1", "hc2", "hc3", "n", "n1", "n2", "n3",
    "nc", "nc1", "nc2", "nc3", "p0", "pp", "pg", "pr", "ps", "pwc", "Q1H", "Q24H", "qg", "qgn", "qgx", "qnh", "R12H",
    "R1H", "R24H", "R6H", "rg", "rh", "rh10", "Sav1H", "Sax1H", "Sax3H", "Sax6H", "sq", "ss", "Sx1H", "Sx3H", "Sx6H",
    "t10", "ta", "tb", "tb1", "Tb1n6", "Tb1x6", "tb2", "Tb2n6", "Tb2x6", "tb3", "tb4", "tb5", "td", "td10", "tg",
    "tgn", "Tgn12", "Tgn14", "Tgn6", "tn", "Tn12", "Tn14", "Tn6", "tsd", "tx", "Tx12", "Tx24", "Tx6", "vv", "W10",
    "W10-10", "ww", "ww-10", "zm"]

NON_NUMERIC_KNMI_VARIABLES = frozenset(("station", "wsi", "stationname"))
NUMERIC_KNMI_VARIABLES : Set[KNMI_NUMERIC_VARIABLE] = ALL_KNMI_VARIABLES - NON_NUMERIC_KNMI_VARIABLES

ALL_KNMI_STATIONS = frozenset(("06237", "06258", "06310", "06316", "06331", "06350", "06377", "06251", "06267", "06273", "06279", "06280", "06283", "06313", "06340", "06343", "06235", "06239", "06242", "06285", "06290", "06308", "06315", "06375", "06391", "78873", "78990", "06201", "06204", "06208", "06209", "06211", "06215", "06229", "06236", "06240", "06257", "06260", "06270", "06330", "06348", "06205", "06214", "06225", "06248", "06252", "06317", "06321", "06344", "78871", "06216", "06238", "06269", "06277", "06312", "06324", "06356", "06370", "06203", "06233", "06249", "06275", "06319", "06320", "06323", "06380", "06207", "06278", "06286"))
KNMI_STATION = Literal["06237", "06258", "06310", "06316", "06331", "06350", "06377", "06251", "06267", "06273", "06279", "06280", "06283", "06313", "06340", "06343", "06235", "06239", "06242", "06285", "06290", "06308", "06315", "06375", "06391", "78873", "78990", "06201", "06204", "06208", "06209", "06211", "06215", "06229", "06236", "06240", "06257", "06260", "06270", "06330", "06348", "06205", "06214", "06225", "06248", "06252", "06317", "06321", "06344", "78871", "06216", "06238", "06269", "06277", "06312", "06324", "06356", "06370", "06203", "06233", "06249", "06275", "06319", "06320", "06323", "06380", "06207", "06278", "06286"]


import hashlib


class RemoteKNMIdataset(IterableDataset):
    def __init__(self, train: bool, after: str | datetime = None, until: str | datetime = None, stations: Set[KNMI_STATION] = None,
                 variables: Set[KNMI_NUMERIC_VARIABLE] = None, download_path: Path | str = "data/", download_url: str = "http://0.0.0.0:9999", download: bool = True):
        """
        A dataloader for the KNMI dataset collected from their [notification api](https://developer.dataplatform.knmi.nl/notification-service).
        This dataset is updated around every 10 minutes. Thus, if you do not set limits on time, this dataloader will keep downloading new data.
        Be sure that this is what you want as it will download a lot of bytes by downloading everything again and again.
        For this reason it is advisable to set an 'until' at least.

        :param train: If set to false the dataloader will only yield time values
        :param after: Only data after this date will be downloaded, if None all data will be downloaded until 'until'
        :param until: Only data until this date will be downloaded, if None all data will be downloaded after 'after'
        :param stations: The stations to download data from, if None all stations will be downloaded
        :param variables: The variables to download, if None all variables will be downloaded
        :param download_path: Directory to save data to
        :param download_url: The url of the api
        :param download: If set to False this dataset will not download things from the internet.
        """
        super(RemoteKNMIdataset).__init__()

        if after is None:
            after = datetime(year=1950, month=1, day=1, hour=0, minute=0, second=0).isoformat()
        elif isinstance(after, str):
            after = datetime.fromisoformat(after.replace("Z", "+00:00")).isoformat()  # Check the datetime
        else:
            after = after.isoformat()

        if until is None:
            until = datetime.utcnow().isoformat()
        elif isinstance(until, str):
            until = datetime.fromisoformat(until.replace("Z", "+00:00")).isoformat()
        else:
            until = until.isoformat()
            until = until.split("T")[0]


        if stations is None:
            stations = ALL_KNMI_STATIONS

        if invalid := stations - ALL_KNMI_STATIONS:
            raise ValueError(f'Invalid station name{"s" if len(invalid) != 1 else ""}: {invalid}')

        if variables is None:
            variables = NUMERIC_KNMI_VARIABLES

        if invalid := variables - NUMERIC_KNMI_VARIABLES:
            raise ValueError(f'Invalid or non numeric variable name{"s" if len(invalid) != 1 else ""}: {invalid}')

        self.after = after
        self.until = until
        self.stations: Set[KNMI_STATION] = stations or ALL_KNMI_STATIONS
        self.variables: Set[KNMI_NUMERIC_VARIABLE] = variables or NUMERIC_KNMI_VARIABLES
        self.train = train

        h = str(sorted(stations)).encode()
        h += str(sorted(variables)).encode()
        h = hashlib.sha3_224(h).hexdigest()
        self.filename = f"{after}->{until}_{h}.parquet"

        self.download_path = Path(download_path)
        urlparse(download_url)
        self.download_url = download_url
        self.download = download

        if not self.download_path.exists():
            os.makedirs(self.download_path, exist_ok=True)

        self.filepath = self.download_path / self.filename

        if not self.filepath.exists() and self.download:
            self.download_dataset()

    def download_dataset(self):
        import requests
        try:
            from tqdm import tqdm
        except ImportError:
            tqdm = lambda _: _

        stations_string = ",".join(self.stations)
        variables_string = ",".join(self.variables)
        file = requests.get(self.download_url, params={"after": self.after, "until": self.until, "stations": stations_string, "variables": variables_string}, stream=True)

        with self.filepath.open("wb+") as f:
            for blob in tqdm(file.iter_content(chunk_size=32768)):
                f.write(blob)



    def __iter__(self):
        # Follow the order set by self.variables
        table = parquet.read_table(self.filepath)
        print(table)
        return iter(table)


    def __len__(self) -> int:
        parquet_file = parquet.ParquetFile(self.filepath)
        return parquet_file.metadata.num_rows


class KNMIdataset(IterableDataset):
    def __init__(self, train: bool, after: str = "all", until: str = "all", stations: Set[KNMI_STATION] = None, variables: Set[KNMI_VARIABLE] = None, database_path: str | Path = "knmi.duckdb"):
        super(RemoteKNMIdataset).__init__()
        self.after = after
        self.until = until
        self.stations: Set[KNMI_STATION] = stations or ALL_KNMI_STATIONS
        self.variables: Set[KNMI_NUMERIC_VARIABLE] = variables or NUMERIC_KNMI_VARIABLES
        self.train: bool = train

        h = str(hash(tuple(sorted(stations)))) + str(hash(tuple(sorted(stations))))
        self.filename = f"{after}->{until}_{h}.parquet"
        self.database_path = Path(database_path)

    def __iter__(self):
        return iter(parquet.read_table())

    def __len__(self) -> int:
        pass

def KNMIDataframe(after: str | datetime = None, until: str | datetime = None, stations: Set[KNMI_STATION] = None, variables: Set[KNMI_VARIABLE] = None, download_path: Path | str = "data/", download_url: str = "http://0.0.0.0:9999", download: bool = True):
        """
        A function that gets you a pandas dataframe for the KNMI dataset collected from their [notification api](https://developer.dataplatform.knmi.nl/notification-service).
        This dataset is updated around every 10 minutes. Thus, if you do not set limits on time, this dataloader will keep downloading new data.
        Be sure that this is what you want as it will download a lot of bytes by downloading everything again and again.
        For this reason it is advisable to set an 'until' at least.

        :param after: Only data after this date will be downloaded, if None all data will be downloaded until 'until'
        :param until: Only data until this date will be downloaded, if None all data will be downloaded after 'after'
        :param stations: The stations to download data from, if None all stations will be downloaded
        :param variables: The variables to download, if None all variables will be downloaded
        :param download_path: Directory to save data to
        :param download_url: The url of the api
        :param download: If set to False this dataset will not download things from the internet.
        """
        import pandas

        if after is None:
            after = datetime(year=1950, month=1, day=1, hour=0, minute=0, second=0).isoformat()
        elif isinstance(after, str):
            after = datetime.fromisoformat(after.replace("Z", "+00:00")).isoformat()  # Check the datetime
        else:
            after = after.isoformat()

        if until is None:
            until = datetime.utcnow().isoformat()
        elif isinstance(until, str):
            until = datetime.fromisoformat(until.replace("Z", "+00:00")).isoformat()
        else:
            until = until.isoformat()

        if invalid := stations - ALL_KNMI_STATIONS:
            raise ValueError(f'Invalid station name{"s" if len(invalid) != 1 else ""}: {invalid}')

        if invalid := variables - NUMERIC_KNMI_VARIABLES:
            raise ValueError(f'Invalid or non numeric variable name{"s" if len(invalid) != 1 else ""}: {invalid}')

        print(hash(tuple(sorted(variables))))

        h = str(hash(tuple(sorted(stations)))) + str(hash(tuple(sorted(variables))))
        filename = f"{after}->{until}_{h}.parquet"

        download_path = Path(download_path)

        if not download_path.exists() and not download:
            raise ValueError("Download is false but the data does not exist, so it is not possible to get the data for you, set download to True.")

        if not download_path.exists() and download:
            os.makedirs(download_path, exist_ok=True)

        filepath = download_path / filename

        if not filepath.exists() and not download:
            raise ValueError("Download is false but the data does not exist, so it is not possible to get the data for you, set download to True.")

        if not filepath.exists() and download:
            urlparse(download_url)
            import requests
            try:
                from tqdm import tqdm
            except ImportError:
                tqdm = lambda _: _

            stations_string = ",".join(stations)
            variables_string = ",".join(variables)
            file = requests.get(download_url, params={"after": after, "until": until, "stations": stations_string, "variables": variables_string}, stream=True)

            with filepath.open("wb+") as f:
                for blob in tqdm(file.iter_content(chunk_size=32768)):
                    f.write(blob)

        return pandas.read_parquet(filepath)


if __name__ == '__main__':

    data = RemoteKNMIdataset(train=True, until="2025-12-01")
    print(iter(data))
    print(data)

# Single api endpoint


