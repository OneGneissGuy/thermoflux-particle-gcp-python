"""
Created on Wed May 29 13:52:24 2019
Script to read in a gcs backup datafile and reformat
to a 30 min average file for analysis
THIS IS FOR A GOOGLE CLOUD FUNCTION
@author: jsaracen
"""
import os
from io import BytesIO

from google.cloud import storage
import pandas as pd


def format_dataframe(
    data_frame,
    index_name,
    interval="5Min",
    drop_null=True,
    round_interval=True,
):
    dataframe = data_frame.copy()
    dataframe.index = dataframe[index_name]
    dataframe.index = pd.to_datetime(dataframe.index)
    # convert the index to a pandas datetimeindex
    dataframe.drop([index_name, "device_id"], axis=1, inplace=True)
    if drop_null:  # drop empty rows
        dataframe = dataframe[dataframe.index.notnull()]
    # sort the dataframe into descending order
    dataframe = dataframe.sort_index()
    # convert values to floating points
    dataframe = dataframe.astype(float)
    if round_interval:
        # round the data time stamp to the nearest defined interval for later merging
        dataframe.index = dataframe.index.round(interval)
    return dataframe.copy()


def fetch_bucket(bucket_name, bucket_filename, storage_client):
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


def push_bucket(dataframe, bucket_name, bucket_filename_write, storage_client):
    """Function to push a pandas dataframe
    to a gcs storage object as as csv file"""
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(bucket_filename_write)
    # saving a data frame to a buffer (same as with a regular file):
    sio = dataframe.to_csv()
    blob.upload_from_string(data=sio)
    return


def merge_dataframes(df1, df2, col_rename_dict):
    df = df1.merge(df2, how="inner", left_index=True, right_index=True)
    # shift the time from UTC to local and drop timezone info
    df.index = df.index.tz_localize(None).shift(-8, "H")
    # set the index name for plotting and output
    df.index.name = "Datetime (PST)"
    # round values
    df = df.round(decimals=2)
    # add units to plot cols
    df.rename(
        columns=col_rename_dict,
        inplace=True,
    )
    return df


def main(event, context):
    bucket_name = os.environ["BUCKET_NAME"]
    bucket_filename_read = os.environ["BUCKET_FILENAME_READ"]
    bucket_filename_write = os.environ["BUCKET_FILENAME_WRITE"]

    storage_client = storage.Client()

    # pull the backup csv file from the gcs (scheduled backed up from bigquery table)
    df_full = fetch_bucket(bucket_name, bucket_filename_read, storage_client)

    # format the dataframe
    df_ancillary = df_full[
        ["temperature", "netRadiation", "battery", "ancillaryTimeStamp", "device_id"]
    ]
    df_ancillary_raw = df_ancillary.drop_duplicates(subset=["ancillaryTimeStamp"])
    df_ancillary_raw_sorted = format_dataframe(
        df_ancillary_raw, "ancillaryTimeStamp", drop_null=True, round_interval=True
    )
    df_ancillary_raw_avg_30_min = df_ancillary_raw_sorted.resample(
        "30min", label="right"
    ).mean()
    # cleanup and format the flux dataframe
    df_flux = df_full[["fluxTimeStamp", "flux", "device_id"]]
    df_flux_raw = df_flux.drop_duplicates(subset=["fluxTimeStamp"])
    df_flux_raw_sorted = format_dataframe(
        df_flux_raw, "fluxTimeStamp", drop_null=False, interval="30Min"
    )
    # merge the two dataframes into one dataframe for plotting analysis
    col_rename_dict = {
        "battery": "Battery voltage (V)",
        "temperature": "Temperature (C)",
        "netRadiation": "Net Radiation (W/m2)",
        "flux": "Sensible heat flux (W/m2)",
    }
    df = merge_dataframes(
        df_ancillary_raw_avg_30_min, df_flux_raw_sorted, col_rename_dict
    )
    push_bucket(df, bucket_name, bucket_filename_write, storage_client)
    print("Done!")
