""" This
bigquery to gcs csv file
"""
# -*- coding: utf-8 -*-
"""
Created on Wed May 29 13:52:24 2019
script to build a weeek long water quality data
report and send it as a gmail attachment
Read the token built by quickstart.py from credentials.json
Follow tutorial here https://developers.google.com/gmail/api/quickstart/python
to enable Google Gmail API and get credentials.json file
@author: jsaracen
"""
# import necessary packages

import fnmatch
from io import BytesIO, StringIO
import os
import sys
from googleapiclient.errors import HttpError
from googleapiclient.discovery import build
from google.cloud import bigquery, storage
from google.oauth2 import service_account
import numpy as np
import pandas as pd


def format_dataframe(dataframe, index_name, interval="5Min"):
    dataframe.index = dataframe[index_name]
    # convert the index to a pandas datetimeindex
    dataframe.index = pd.to_datetime(dataframe.index)
    dataframe.drop([index_name], axis=1, inplace=True)
    # drop empty rows
    dataframe = dataframe[dataframe.index.notnull()]
    # sort the dataframe into descending order
    dataframe = dataframe.sort_index()
    # slice data to start at deployment date
    # dataframe = dataframe[deployment_date:]
    # convert values to floating points
    dataframe = dataframe.astype(float)
    # round the data time stamp to the nearest defined interval for later merging
    dataframe.index = dataframe.index.round(interval)
    return dataframe.copy()


def fetch_bucket(bucket_name, bucket_filename):
    """Function to fetch a gcs storage
    object and return a pandas dataframe"""
    bucket = storage_client.get_bucket(bucket_name)
    blob = storage.blob.Blob(bucket_filename, bucket)
    content = blob.download_as_string()
    dfbytes = pd.read_csv(
        BytesIO(content),
        na_values=["NAN", "nan", -999],
    )
    return dfbytes


def push_bucket(dataframe, bucket_name, bucket_filename):
    """Function to push a pandas dataframe
    to a gcs storage object as as csv file"""
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(bucket_filename_write)
    # saving a data frame to a buffer (same as with a regular file):
    sio = dataframe.to_csv()
    blob.upload_from_string(data=sio)
    return


def splitSerToArr(ser):
    return [ser.index, ser.values]


if __name__ == "__main__":

    storage_client = storage.Client.from_service_account_json(
        "thermoflux-particle-6cb499f95f01.json"
    )
    # TODO: READ FROM ONE BUCKET, WRITE TO ANOTHER BUCKET
    # validate the service account by listing the project buckets
    list(storage_client.list_buckets())
    bucket_name = "thermoflux-bq-data"
    bucket_filename_read = "demo_table_backup.csv"
    bucket_filename_write = "demo-table-export.csv"

    # FILENAME = "gs://{}/{}".format(bucket_name, bucket_filename)

    # storage_client = storage.Client(project = project_id)

    # set path to filename
    # FILENAME = r"gs://thermoflux-bq-data/demo_table_backup.csv"
    # pull the formatted csv file

    df_full = fetch_bucket(bucket_name, bucket_filename_read)

    # format the dataframe
    df_flux = format_dataframe(
        df_full[["fluxTimeStamp", "flux"]], "fluxTimeStamp", interval="30Min"
    )
    df_ancillary = format_dataframe(
        df_full[["temperature", "netRadiation", "battery", "ancillaryTimeStamp"]],
        "ancillaryTimeStamp",
        interval="5Min",
    )
    df = df_ancillary.merge(df_flux, how="outer", left_index=True, right_index=True)
    # shift the time from UTC to local
    df.index = df.index.tz_convert("US/Pacific")
    # set the index name for plotting and output
    df.index.name = "Datetime (PST)"
    # push the formatted file to a bucket
    push_bucket(df, bucket_name, bucket_filename_write)
