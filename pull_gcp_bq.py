from google.cloud import bigquery
import json
import google.auth
from google.oauth2 import service_account
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from google.cloud import storage
import os


def format_dataframe(
    data_frame,
    index_name,
    interval="5Min",
    drop_null=True,
    round_interval=True,
):
    dataframe = data_frame.copy()
    dataframe.index = dataframe[index_name]
    # convert the index to a pandas datetimeindex
    dataframe.drop([index_name, "device_id"], axis=1, inplace=True)
    if drop_null:  # drop empty rows
        dataframe = dataframe[dataframe.index.notnull()]
    # sort the dataframe into descending order
    dataframe = dataframe.sort_index()
    # dataframe.index = dataframe.index.shift(tz_offset, freq="H")
    # slice data to start at deployment date
    dataframe = dataframe[deployment_date:]
    # convert values to floating points
    dataframe = dataframe.astype(float)
    if round_interval:
        # round the data time stamp to the nearest defined interval for later merging
        dataframe.index = dataframe.index.round(interval)
    return dataframe.copy()


# os.environ[
#     "GOOGLE_APPLICATION_CREDENTIALS"
# ] = credentials_file

# Biq Query Admin Service account CREDENTIALS
credentials_file = "bq_service_account.json"
# GCP BIQ QUERY FILE INFO
bq_filename = "demo_dataset.demo_table"
ts_index_column = "ancillaryTimeStamp"
deployment_date = "02-26-2020"

service_account_info = json.load(open(credentials_file))
credentials = service_account.Credentials.from_service_account_info(
    service_account_info
)

# Service account must have BigQuery Admin Role
# https://cloud.google.com/docs/authentication/getting-started
# create a client instance
client = bigquery.Client.from_service_account_json(credentials_file)
# create a query for the ancillary data
ancillary_sql = """
 SELECT temperature, netRadiation, battery, ancillaryTimeStamp, device_id FROM `demo_dataset.demo_table`
 WHERE (device_id = "e00fce68e84fc8ca3d4edf94" AND temperature IS NOT NULL);
 """
# query ancillary table data from BiqQuery and store it in a pandas dataframe
df_ancillary_raw = (
    client.query(ancillary_sql)
    .to_dataframe()
    .drop_duplicates(subset=["ancillaryTimeStamp"])
)
df_ancillary_raw_sorted = format_dataframe(
    df_ancillary_raw, "ancillaryTimeStamp", drop_null=False, round_interval=True
)

df_ancillary_raw_avg_30_min = df_ancillary_raw_sorted.resample(
    "30min", label="right"
).mean()

# df_ancillary_raw_sorted.to_csv("demo-table-export-ancillary-raw.csv", index=True)

# cleanup and format the ancillary dataframe
# df_ancillary = format_dataframe(df_ancillary_raw, "ancillaryTimeStamp", interval="5Min")

# create a query for the flux data #ignore old sensor
flux_sql = """
 SELECT flux,fluxTimeStamp,device_id FROM `demo_dataset.demo_table`
 WHERE (device_id = "e00fce68e84fc8ca3d4edf94" AND flux IS NOT NULL);
 """
# query flux table data from BiqQuery and store it in a pandas dataframe
df_flux_raw = (
    client.query(flux_sql).to_dataframe().drop_duplicates(subset=["fluxTimeStamp"])
)

df_flux_raw_sorted = format_dataframe(
    df_flux_raw, "fluxTimeStamp", drop_null=False, round_interval=False
)
# cleanup and format the flux dataframe
df_flux = format_dataframe(
    df_flux_raw, "fluxTimeStamp", drop_null=False, interval="30Min"
)
# filter non-sensecal outliers
# df_flux["flux"][(df_flux["flux"] > 500) | (df_flux["flux"] < -500)] = np.nan
# merge the two dataframes into one dataframe for plotting analysis
df = df_ancillary_raw_avg_30_min.merge(
    df_flux, how="inner", left_index=True, right_index=True
)
# shift the time from UTC to local and drop timezone info
# df.index = df.index.tz_convert("US/Pacific")
df.index = df.index.tz_localize(None).shift(-8, "H")
# set the index name for plotting and output
df.index.name = "Datetime (PST)"
# round values
df = df.round(decimals=2)
# add units to plot cols
df.rename(
    columns={
        "battery": "Battery voltage (V)",
        "temperature": "Temperature (C)",
        "netRadiation": "Net Radiation (W/m2)",
        "flux": "Sensible heat flux (W/m2)",
    },
    inplace=True,
)
# save output to an csv file
df.to_csv("demo-table-export-30min-ave.csv")

# plot the data
plt.figure()
df["Battery voltage (V)"].plot(marker="o")
plt.ylim(12, 16)
# plt.ylabel("Battery voltage (V)")
plt.figure()
df["Temperature (C)"].plot(marker="o")
# plt.ylabel("Temperature (C)")
plt.ylim(-5, 30)
plt.figure()
df["Net Radiation (W/m2)"].plot(marker="o")
plt.ylabel(r"Net radiation, $R_{net}$ $\mathregular{(\frac{W}{m^{2}})}$")
# plt.ylim(-100, 700)
plt.figure()
df["Sensible heat flux (W/m2)"].plot(marker="o")
plt.ylabel(r"Sensible heat flux, $H_{SR}$ $\mathregular{(\frac{W}{m^{2}})}$")
plt.ylim(-200, 400)
# count the number of samples in a day (should be 287-288 for ancillary; 47-48 for unfiltered flux)
df.groupby([df.index.day]).count()
