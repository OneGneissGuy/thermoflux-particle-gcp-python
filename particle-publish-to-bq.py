import ast
import base64
import json
import os
import requests
from google.cloud import bigquery


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
        sample_count = payload.get("sample_count", None)

        fluxTimeStamp = payload.get("fluxTimeStamp", None)
        flux = payload.get("flux", None)
        if (fluxTimeStamp is None) and (battery is None):
            device_id = None

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
                u"sample_count": sample_count,
            }
        ]

        errors = client.insert_rows_json(
            table_id, rows_to_insert
        )  # Make an API request to insert data into google bigquery table.

        if errors == []:
            print("New rows have been added.")
        else:
            print("Encountered errors while inserting rows: {}".format(errors))

    else:
        print("Nothing in payload")