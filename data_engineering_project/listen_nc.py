import json
import logging
import ssl
import time

import paho.mqtt.client as mqtt_client
import paho.mqtt.properties as properties
import paho
import httpx

from pathlib import Path

from data_engineering_project.config import load_auth
from data_engineering_project.wrangle import process_nc_file, insert_nc_filerows
import toml

match load_auth():
        case {"CLIENT_ID": str(CLIENT_ID), "TOKEN": str(TOKEN), "FILE_DOWNLOAD_TOKEN": str(FILE_DOWNLOAD_TOKEN), }:
            pass
        case _:
            print("Can not everything from the auth.toml. Check that it has at least CLIENT_ID, TOKEN, TOPIC, FILE_DOWNLOAD_TOKEN and BROKER_DOMAIN")
            exit()

with open("config.toml") as configfile:
    config = toml.load(configfile)

match config:
    case {"TOPIC": str(TOPIC), "BROKER_DOMAIN": str(BROKER_DOMAIN)}:
        pass
    case _:
        print("Can not everything from the config.toml. Check that it at least has TOPIC and BROKER_DOMAIN")
        exit()

from data_engineering_project.init_wrangle import create_database_if_not_exists

create_database_if_not_exists()

# Version 3.1.1 also supported
PROTOCOL = mqtt_client.MQTTv5


logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel("INFO")


def connect_mqtt() -> mqtt_client:
    def on_connect(c: mqtt_client, userdata, flags, rc, reason_code, props=None):
        logger.info(f"Connected using client ID: {str(c._client_id)}")
        logger.info(f"Session present: {str(flags['session present'])}")
        logger.info(f"Connection result: {str(rc)}")
        # Subscribe here so it is automatically done after disconnect
        subscribe(c, TOPIC)

    client = mqtt_client.Client(paho.mqtt.enums.CallbackAPIVersion.VERSION1, client_id=CLIENT_ID, protocol=PROTOCOL, transport="websockets")
    client.tls_set(tls_version=ssl.PROTOCOL_TLS)
    connect_properties = properties.Properties(properties.PacketTypes.CONNECT)
    # Maximum is 3600
    connect_properties.SessionExpiryInterval = 3600

    # The MQTT username is not used for authentication, only the token
    username = "token"
    client.username_pw_set(username, TOKEN)
    client.on_connect = on_connect

    client.connect(host=BROKER_DOMAIN, port=443, keepalive=60, clean_start=False, properties=connect_properties)

    return client


def subscribe(client: mqtt_client, topic: str):
    def on_message(c: mqtt_client, userdata, message):
        # NOTE: Do NOT do slow processing in this function, as this will interfere with PUBACK messages for QoS=1.
        # A couple of seconds seems fine, a minute is definitely too long.
        logger.info(f"Received message on topic {message.topic}: {message.payload}")
        try:
            payload = json.loads(message.payload)
            match payload:
                case {"data": {"url": str(url), "filename": str(filename)}}:
                    filename = Path(filename)
                    file_path = "nc_files"/filename
                    pass
                case _:
                    raise ValueError("Could not match response", payload, 'with {"data":{"url": str(url)}}')
            response = httpx.get(url, headers={"Authorization": f"Bearer {FILE_DOWNLOAD_TOKEN}"})
            response_json = response.json()
            print("Got response json", response_json)
            match response_json:
                case {"temporaryDownloadUrl": str(url)}:
                    pass
            response = httpx.get(url)
            with open(file_path, "wb+") as f:
                for chunk in response.iter_bytes(2 ** 20 * 2):
                    print(f"Writing chunk of {len(chunk)} bytes to file")
                    f.write(chunk)
            print("Downloaded", file_path)
            start = time.time()
            result = process_nc_file(file_path)
            end_process = time.time()
            insert_nc_filerows(result)
            end_insert = time.time()
            print("Saved rows from", filename, f"into the database. Processing took {round(end_process-start,2)} seconds and saving took {round(end_insert-start,2)} seconds")

        except Exception as e:
            print("Could not download data :(", e)

    def on_subscribe(c: mqtt_client, userdata, mid, granted_qos, *other):
        logger.info(f"Subscribed to topic '{topic}'")

    client.on_subscribe = on_subscribe
    client.on_message = on_message
    # A qos=1 will replay missed events when reconnecting with the same client ID. Use qos=0 to disable
    client.subscribe(topic, qos=1)


def run():
    client = connect_mqtt()
    client.enable_logger(logger=logger)
    client.loop_forever()


if __name__ == '__main__':
    run()
