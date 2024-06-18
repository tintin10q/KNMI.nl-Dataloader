import os
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse
from pyarrow import parquet

from torch.utils.data import IterableDataset
from typing import List, Set

from to_save import KNMI_STATION, NUMERIC_KNMI_VARIABLES, KNMI_NUMERIC_VARIABLE, ALL_KNMI_STATIONS, KNMI_VARIABLE


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

        if invalid := stations - ALL_KNMI_STATIONS:
            raise ValueError(f'Invalid station name{"s" if len(invalid) != 1 else ""}: {invalid}')

        if invalid := variables - NUMERIC_KNMI_VARIABLES:
            raise ValueError(f'Invalid or non numeric variable name{"s" if len(invalid) != 1 else ""}: {invalid}')

        self.after = after
        self.until = until
        self.stations: Set[KNMI_STATION] = stations or ALL_KNMI_STATIONS
        self.variables: Set[KNMI_NUMERIC_VARIABLE] = variables or NUMERIC_KNMI_VARIABLES
        self.train = train

        h = str(hash(tuple(sorted(self.stations)))) + str(hash(tuple(sorted(self.stations))))
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
        # [time, stations, data]
        pass


    def __len__(self) -> int:
        parquet_file = parquet.ParquetFile(self.filepath)
        return parquet_file.metadata.num_rows


class KNMIdataset(IterableDataset):
    def __init__(self, train: bool, after: str = "all", until: str = "all", stations: Set[KNMI_STATION] = None, variables: Set[KNMI_KEY] = None, database_path: str | Path = "knmi.duckdb"):
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
        pass

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

        h = str(hash(tuple(sorted(stations)))) + str(hash(tuple(sorted(stations))))
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
    data = RemoteKNMIdataset(train=True)
    print(data)

# Single api endpoint


# Single api endpoint
