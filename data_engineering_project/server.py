import os
from pathlib import Path
from typing import Set
from data_engineering_project.bottle import route, run, request, response, WSGIFileWrapper, static_file, HTTPResponse
import datetime
from . import db_connection
from data_engineering_project.to_save import ALL_KNMI_VARIABLES, ALL_KNMI_STATIONS
import tempfile
import json

ALL_KNMI_STATIONS_str = json.dumps(list(ALL_KNMI_STATIONS))
ALL_KNMI_KEYS_str = json.dumps(list(ALL_KNMI_VARIABLES))

knmi_epoch = datetime.datetime(year=1950, month=1, day=1, hour=0, minute=0, second=0)


@route('/stations')
def stations():
    return ALL_KNMI_STATIONS_str


@route('/variables')
def stations():
    return ALL_KNMI_KEYS_str


@route('/ping')
def stations():
    return '"🏓"'


@route("/parquet")
def download():
    match request.query:
        case {"after": str(after), "until": str(until), "stations": str(stations_string), "variables": str(variables_string)}:
            stations = set(station_list := stations_string.strip().replace(" ", "").replace("\n", "").replace("\r", "").split(","))

            if invalid := stations - ALL_KNMI_STATIONS:
                return HTTPResponse(f'"Could not download data because of {len(invalid)} invalid station name{"s" if len(invalid) != 1 else ""} specified"', status=400)

            variables: Set[str] = set(variables_string.strip().replace(" ", "").replace("\n", "").replace("\r", "").split(","))
            if invalid := variables - ALL_KNMI_VARIABLES:  # If anything remains it's not valid and reject!
                return HTTPResponse(f'"Could not download data because of {len(invalid)} invalid variable name{"s" if len(invalid) != 1 else ""} specified"', status=400)

            try:  # Until
                after_parsable = after.replace("Z", "+00:00")
                parsed_after = datetime.datetime.fromisoformat(after_parsable)
                if parsed_after < knmi_epoch:
                    return HTTPResponse(f'"after has to be more then {knmi_epoch}"', status=400)
            except ValueError:
                return HTTPResponse(f'"after is not a valid iso 8601 date"', status=400)

            after_total_seconds = (parsed_after - knmi_epoch).total_seconds()
            after_seconds = int(after_total_seconds)

            try:  # After
                until_parsable = until.replace("Z", "+00:00")
                parsed_until = datetime.datetime.fromisoformat(until_parsable)
                if parsed_until < knmi_epoch:
                    return HTTPResponse(f'"until has to be more then {knmi_epoch}', status=400)
            except ValueError:
                return HTTPResponse(f'"until is not a valid iso 8601 date"', status=400)


            if parsed_until < parsed_after:
                return HTTPResponse(f'"after has to before until"', status=400)

            until_total_seconds = (parsed_until - knmi_epoch).total_seconds()
            until_seconds = int(until_total_seconds)

            # Fetch the data from the database

            station_questionmarks = ', '.join(['?'] * len(stations))
            requested_variables = ",".join(variables)  # should be safe because we checked the set with an allow list

            outputfile = tempfile.NamedTemporaryFile("r+b")
            outputfilename = outputfile.name

            query = f"select {requested_variables} from Measurement where station in ({station_questionmarks}) and time > ? and time < ? order by time"
            connection = db_connection.get_readonly_connection()

            try:
                connection.sql(query, params=station_list + [after_seconds, until_seconds]).to_parquet(file_name=outputfilename)
            finally:
                connection.close()

            response.set_header('Content-type', 'application/vnd.apache.parquet')
            response.set_header('Content-Disposition', f'attachment; filename="KNMI_after_{parsed_after}_until_{parsed_until}_with_{len(variables)}_variables_{len(stations)}_stations.parquet"');

            def output():
                try:
                    with open(outputfilename, "rb") as f:
                        while chunk := f.read(32768):
                            yield chunk
                finally:
                    outputfile.close()

            return output()
        case _:
            return HTTPResponse('''"Missing query params. You need:
- after :: iso date string;
- until :: iso date string;
- stations :: comma seperated station names ; see /stations for valid station names
- variables :: comma seperated variable names ; see /variables for valid variable names "''', status=400)


static_root = Path(os.path.dirname(__file__)) / 'static'


@route('/', method='GET')
def index():
    """
    Describe the api
    Describe the

    Mention the torch data loader and give a download link.

    Example of how to use the api, with python code
    """
    return static_file("index.html", root=static_root)


@route('/pytorch_dataloader.py', method='GET')
def download_dataloader():
    return static_file('pytorch_dataloader.py', root=static_root)


def start():
    run(host="0.0.0.0", port=9999, debug=False, reloader=True)


if __name__ == "__main__":
    start()
