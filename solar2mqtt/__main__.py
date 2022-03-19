import argparse
import asyncio
import json
import logging

from asyncio_mqtt import Client, MqttError
from typing import List, Dict

DEBUG = True
LINE_LEN = 137


class SolarParser():
    def __init__(self, stream):
        self._sensors = {}

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def process_line(self, line: str):
        if len(line) > LINE_LEN:
            line = line[-LINE_LEN:]
        line = line.strip()

        words = line.split(' ')
        address = int(words[0][0:-1], 0)

        return address, self.buffer_words(address, words[1:])

    def reading(self, word):
        values = word.split(',')
        r = {
            "count":int(values[0]),
            "voltage":float(values[1])/10.,
            "temperature":float(values[2])/10.,
        }
        return r

    def buffer_words(self, address, words: List[str]):
        return self.reading(words[-1])


async def mainloop(input_stream, mqtt_host, mqtt_user, mqtt_pwd):
    async with Client(mqtt_host, username=mqtt_user, password=mqtt_pwd) as client:
            with open(input_stream, "r") as stream:
                with SolarParser(stream) as parser:
                    while True:
                        line = stream.readline()
                        if not line:
                            break
                        try:
                            address, payload = parser.process_line(line)
                            message = json.dumps(payload)
                            await client.publish(f"solar/modules/{address}", payload=message.encode())
                        except (ValueError, IndexError):
                            # skip broken line
                            continue


logger = logging.getLogger('solar2mqtt')
logger.setLevel(logging.DEBUG if DEBUG else logging.INFO)
argp = argparse.ArgumentParser(description='Receive solar module telemetry and submit them via MQTT')
argp.add_argument('-i', '--input-stream', required=True, help="Input stream or file")
argp.add_argument('-H', '--mqtt-host', default="localhost", help="MQTT host")
argp.add_argument('-u', '--mqtt-user', default="", help="MQTT user")
argp.add_argument('-p', '--mqtt-pwd', default="", help="MQTT password")
args = argp.parse_args()

asyncio.run(mainloop(**vars(args)))
