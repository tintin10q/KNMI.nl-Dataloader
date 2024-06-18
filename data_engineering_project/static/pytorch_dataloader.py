import os
from pathlib import Path
from urllib.parse import urlparse

from torch.utils.data import IterableDataset
from typing import List, Set

from to_save import KNMI_STATION, KNMI_KEY, ALL_KNMI_KEYS, ALL_KNMI_STATIONS

class RemoteKNMIdataset(IterableDataset):
    def __init__(self, train: bool, after: str = "all", until: str = "all", stations: Set[KNMI_STATION] = None,
                 variables: Set[KNMI_KEY] = None, download_path: Path | str = "data/", download_url: str = "http://0.0.0.0:9999"):
        super(RemoteKNMIdataset).__init__()

        if after == "all" and until != "all" or after != "all" and until == "all":
            raise ValueError("Either both 'until' and 'after' are 'all' or none of them.")

        if invalid := stations - ALL_KNMI_STATIONS:
            raise ValueError(f'Invalid station name{"s" if len(invalid) != 1 else ""}: {invalid}')

        if invalid := variables - ALL_KNMI_KEYS:
            raise ValueError(f'Invalid variable name{"s" if len(invalid) != 1 else ""}: {invalid}')

        self.after = after
        self.until = until
        self.stations: Set[KNMI_STATION] = stations or ALL_KNMI_STATIONS
        self.variables: Set[KNMI_KEY] = variables or ALL_KNMI_KEYS
        self.train: bool = train

        h = str(hash(tuple(sorted(self.stations)))) + str(hash(tuple(sorted(self.stations))))
        self.filename = f"{after}->{until}_{h}.parquet"

        self.download_path = Path(download_path)
        self.download_url = urlparse(download_url)

        if not self.download_path.exists():
            os.makedirs(self.download_path, exist_ok=True)

        self.filepath = self.download_path / self.filename

        if not self.filepath.exists():
            import requests
            stations_string = ",".join(list(self.stations))
            keys_string = ",".join(list(self.variables))
            file = requests.get(download_url, params={"after": after, "until": until, "stations": stations_string, "variables":  keys_string}, stream=True)

            with self.filepath.open("wb+") as f:
                for blob in file.iter_content(chunk_size=2024):
                    f.write(blob)

            # Download the dataset


    def __iter__(self):
        pass

    def __len__(self) -> int:
        pass


class KNMIdataset(IterableDataset):
    def __init__(self, train: bool, after: str = "all", until: str = "all", stations: Set[KNMI_STATION] = None, variables: Set[KNMI_KEY] = None, database_path: str | Path = "knmi.duckdb"):
        super(RemoteKNMIdataset).__init__()
        self.after = after
        self.until = until
        self.stations: Set[KNMI_STATION] = stations or ALL_KNMI_STATIONS
        self.variables: Set[KNMI_KEY] = variables or ALL_KNMI_KEYS
        self.train: bool = train

        h = str(hash(tuple(sorted(stations)))) + str(hash(tuple(sorted(stations))))
        self.filename = f"{after}->{until}_{h}.parquet"
        self.database_path = Path(database_path)

    def __iter__(self):
        pass

    def __len__(self) -> int:
        pass



if __name__ == '__main__':
    data = RemoteKNMIdataset(train=True)
    print(data)





# Single api endpoint







# Single api endpoint
