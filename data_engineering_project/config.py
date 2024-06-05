import os

import toml
from pathlib import Path
from typing import TypedDict


class Auth(TypedDict):
    CLIENT_ID: str
    TOKEN: str
    FILE_DOWNLOAD_TOKEN: str

def load_auth() -> Auth:
    from .colors import RESET, RED, GREEN
    path = Path.cwd() / "auth.toml"

    if not path.exists():
        print(f"{RED}Auth.toml file not found, creating it at {GREEN} {path}{RESET}")
        open(path, "w+").close()


    with open(path, "r+") as authfile:
        auth_config = toml.load(authfile)

    match auth_config:
        case {"CLIENT_ID": str(CLIENT_ID), "TOKEN": str(TOKEN), "FILE_DOWNLOAD_TOKEN": str(FILE_DOWNLOAD_TOKEN), }:
            return auth_config
        case _:
            print(f"{RED}Can not everything from the auth.toml. {RESET}Check that it has at least CLIENT_ID, TOKEN, TOPIC, FILE_DOWNLOAD_TOKEN and BROKER_DOMAIN in {GREEN}{path}{RESET}")
            exit()


class Config(TypedDict):
    BROKER_DOMAIN = str
    TOPIC = str

default_config = """
BROKER_DOMAIN = "mqtt.dataplatform.knmi.nl"
TOPIC = "dataplatform/file/v1/Actuele10mindataKNMIstations/2/updated"
"""

def load_config() -> Config:
    from .colors import RESET, RED, GREEN
    path = Path.cwd() / "config.toml"

    if not path.exists():
        print(f"{RED}config.toml file not found, creating it at {GREEN} {path}{RESET} with defaults.")
        with open(path, "w+") as file:
            file.write(default_config)

    with open(path, "r+") as config_file:
        config = toml.load(config_file)

    match config:
        case {"TOPIC": str(TOPIC), "BROKER_DOMAIN": str(BROKER_DOMAIN)}:
            return config
        case _:
            print(f"{RED}Can not everything from the config.toml. Check that it at least has TOPIC and BROKER_DOMAIN at {GREEN}{path}{RESET}" )
            exit()


