from google.cloud import bigquery
import json
import google.auth
from google.oauth2 import service_account
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from google.cloud import storage
import os


def format_dataframe(dataframe, index_name, interval="5Min"):
    dataframe.index = dataframe[index_name]
    # convert the index to a pandas datetimeindex
    dataframe.drop([index_name], axis=1, inplace=True)
    # drop empty rows
    dataframe = dataframe[dataframe.index.notnull()]
    # sort the dataframe into descending order
    dataframe = dataframe.sort_index()
    # slice data to start at deployment date
    dataframe = dataframe[deployment_date:]
    # convert values to floating points
    dataframe = dataframe.astype(float)
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
deployment_date = "11-17-2020"

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
 SELECT temperature, netRadiation, battery, ancillaryTimeStamp FROM `demo_dataset.demo_table`
 """
# query ancillary table data from BiqQuery and store it in a pandas dataframe
df_ancillary = client.query(ancillary_sql).to_dataframe()

# cleanup and format the ancillary dataframe
df_ancillary = format_dataframe(df_ancillary, "ancillaryTimeStamp", interval="5Min")

# create a query for the flux data
flux_sql = """
 SELECT flux,fluxTimeStamp FROM `demo_dataset.demo_table`
 """
# query flux table data from BiqQuery and store it in a pandas dataframe
df_flux = client.query(flux_sql).to_dataframe()
# cleanup and format the flux dataframe
df_flux = format_dataframe(df_flux, "fluxTimeStamp", interval="30Min")
# filter non-sensical outliers
df_flux["flux"][(df_flux["flux"] > 500) | (df_flux["flux"] < -500)] = np.nan
# merge the two dataframes into one dataframe for plotting analysis
df = df_ancillary.merge(df_flux, how="outer", left_index=True, right_index=True)

# shift the time from UTC to local
df.index = df.index.tz_convert("US/Pacific")

# set the index name for plotting and output
df.index.name = "Datetime (PST)"

# plot the data
plt.figure()
df.battery.plot(marker="o")
plt.ylim(11, 16)
plt.ylabel("Battery voltage (V)")
plt.figure()
df.temperature.plot(marker="o")
plt.ylabel("Temperature (C)")
plt.ylim(-5, 30)
plt.figure()
df.netRadiation.plot(marker="o")
plt.ylabel(r"Net radiation, $R_{net}$ $\mathregular{(\frac{W}{m^{2}})}$")
# plt.ylim(-100, 700)
plt.figure()
df.flux.plot(marker="o")
plt.ylabel(r"Sensible heat flux, $H_{SR}$ $\mathregular{(\frac{W}{m^{2}})}$")
plt.ylim(-100, 100)
# count the number of samples in a day (should be 287-288 for ancillary; 47-48 for unfiltered flux)
df.groupby([df.index.day]).count()
# save output
df.to_csv("demo-table-export.csv")
