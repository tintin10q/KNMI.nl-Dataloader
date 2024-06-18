#!/usr/bin/env python
# Download all the data of the last week
import requests, datetime

api_host = "http://0.0.0.0:9999"
save_as = "weather.parquet"

now = datetime.datetime.utcnow()
until = now.isoformat()

one_year_ago = now - datetime.timedelta(days=356)
after = one_year_ago.isoformat()

stations = requests.get(f"{api_host}/stations").json()
variables = requests.get(f"{api_host}/variables").json()

stations = ",".join(stations)
variables = ",".join(variables)

parquet = requests.get(f"{api_host}/parquet", stream=True,
                       params={"until": until, "after": after,
                               "stations": stations, "variables": variables})
with open(save_as, "wb") as f:  # Save the file
    for chunk in parquet.iter_content(chunk_size=1024 * 32):
        f.write(chunk)
