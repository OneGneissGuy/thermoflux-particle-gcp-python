"""
This script fetches data from a particle cloud device that has a variable enabled and 
uploads the data as csv file to a google cloud platform datastore

More info on particle api here :https://docs.particle.io/reference/device-os/firmware/boron/#particle-variable-
"""
# import dependencies
# built-ins
import json
import os
# external
from google.cloud import storage
import pandas as pd
import requests

# paricle cloud api config
# TODO move config to ENV VARS
with open("../src/tokens.json", "r") as read_file:
    config = json.load(read_file)

#device ID
# config = {"url":'https://api.particle.io/v1/devices',
# "variables":["flux","netRadiation","temperature","fluxTime"]}
# API access token
access_token = config["access_token"]
device_id = config["device_id"] 
variables = config["variables"]
url = config["url"]
# device_id = os.environ["device_id"]
# access_token = os.environ["access_token"]

def build_api_url(url, var, device_id):
    path = f"{url}/{device_id}/{var}"
    return path


def get_particle_variables(event, context):
    list_of_responses = []
    fluxTime_row = 3

    for var in variables:
        req_url = build_api_url(url, var, device_id)
        token_payload = {"access_token": access_token}
        response = requests.get(req_url, params=token_payload)
        json_data = response.json()
        json_data["lastHeard"] = json_data["coreInfo"]["last_heard"]
        json_data["deviceID"] = json_data["coreInfo"]["deviceID"]
        list_of_responses.append(json_data)
    #TODO: ADD TRY/EXCEPT LOOP IF ERROR RETRIEVING DATA (REQUEST TIMEOUT,VAR NOT LISTED)
    # Convert the data into a pandas dataframe
    df = pd.DataFrame.from_dict(list_of_responses)
    # Set the timestamp of all values to the fluxTime
    df["timeStamp"] = df.iloc[fluxTime_row]["result"]

    #df["timeStamp"] = "2020-08-25" #date
    # df["timeStamp"] = "2020-08-25 12:00:00" #datetime

    df.drop(["cmd", "coreInfo"], axis=1, inplace=True)
    df.drop([fluxTime_row], inplace=True)

    df2 = df[["name","result","timeStamp"]]
    #Restructure from long form to short form and set index to timestamp
    df3 = df2.pivot(index="timeStamp",columns="name")
    df3.columns = df3.columns.get_level_values(1)
    df3['deviceID'] = json_data["deviceID"]
    df3['dataTimeStamp'] = df2.iloc[0]["timeStamp"]

    # Save the dataframe to a csv file
    out_filename = "{}_particle_cloud_vars.csv".format(device_id) 

    # Instantiates a client
    storage_client = storage.Client()

    # The name for the new bucket
    bucket_name = "gcp-particle-var"

    # Creates the new bucket and inform user
    bucket = storage_client.get_bucket(bucket_name)
    print("Bucket {} created.".format(bucket.name))

    # Create data bucket blob  
    data = bucket.blob(out_filename)

    # Upload the data to the bucket
    data.upload_from_string(df3.to_csv(index=False), "text/csv")

