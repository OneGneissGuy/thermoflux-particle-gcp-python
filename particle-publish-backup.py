import ast
import base64
import json
import os
import requests


class Channel(object):
    """ThingSpeak channel object"""

    def __init__(
        self,
        id,
        api_key=None,
        fmt="json",
        timeout=None,
        server_url="https://api.thingspeak.com",
    ):
        self.id = id
        self.api_key = api_key
        self.fmt = ("." + fmt) if fmt in ["json", "xml"] else ""
        self.timeout = timeout
        self.server_url = server_url

    def update(self, data):
        """Update channel feed.

        `update-channel-feed
        <https://mathworks.com/help/thingspeak/update-channel-feed.html>`_
        """
        if self.api_key is not None:
            data["api_key"] = self.api_key
        else:
            raise ValueError("Missing api_key")
        url = "{server_url}/update{fmt}".format(
            server_url=self.server_url, id=self.id, fmt=self.fmt
        )
        r = requests.post(url, params=data, timeout=self.timeout)
        return self._fmt(r)

    def _fmt(self, r):
        r.raise_for_status()
        if self.fmt == "json":
            return r.json()
        else:
            return r.text


def thingspeak_post(myChannel, data):
    myChannel.update(data)


def particle_pubsub_msg(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.

         particle publish payload "{'temperature':39,'netRadiation':302,'battery':12.3,'ancillaryTimeStamp':'2020-10-19T12:50:10Z'}" --private
    """
    print(
        """This Function was triggered by messageId {} published at {}
        """.format(
            context.event_id, context.timestamp
        )
    )

    if "data" in event:
        message = base64.b64decode(event["data"]).decode("utf-8")
        print("Decoded message data: {}".format(message))
        payload = ast.literal_eval(message)  # convert dictionary string to dictionary
        ancillaryTimeStamp = payload.get("ancillaryTimeStamp", None)
        battery = payload.get("battery", None)
        netRadiation = payload.get("netRadiation", None)
        temperature = payload.get("temperature", None)
        device_id = payload.get("device_id", None)

        fluxTimeStamp = payload.get("fluxTimeStamp", None)
        flux = payload.get("flux", None)
        if (fluxTimeStamp is None) and (battery is None):
            device_id = None
        from google.cloud import bigquery

        project_id = os.environ["project_id"]
        table_id = os.environ["table_id"]
        dataset_id = os.environ["dataset_id"]
        table_id = ".".join([project_id, dataset_id, table_id])
        client = bigquery.Client()

        rows_to_insert = [
            {
                u"ancillaryTimeStamp": ancillaryTimeStamp,
                u"temperature": temperature,
                u"battery": battery,
                u"netRadiation": netRadiation,
                u"device_id": device_id,
                u"fluxTimeStamp": fluxTimeStamp,
                u"flux": flux,
            }
        ]

        errors = client.insert_rows_json(
            table_id, rows_to_insert
        )  # Make an API request to insert data into google bigquery table.

        if errors == []:
            print("New rows have been added.")
        else:
            print("Encountered errors while inserting rows: {}".format(errors))

        thingspeak_url = "https://api.thingspeak.com"
        # API channel
        thingspeak_channel_id = os.environ["thingspeak_channel_id"]
        # API channel write key
        thingspeak_write_key = os.environ["thingspeak_write_key"]
        fields = ["field1", "field2", "field3", "field4"]
        values = [temperature, battery, netRadiation, flux]
        data = dict(zip(fields, values))
        myChannel = Channel(
            thingspeak_channel_id,
            api_key=thingspeak_write_key,
            fmt="json",
            timeout=None,
            server_url=thingspeak_url,
        )
        thingspeak_post(myChannel, data)

    else:
        print("Nothing in payload")