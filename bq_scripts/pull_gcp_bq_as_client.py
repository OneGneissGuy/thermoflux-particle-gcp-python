from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import os

# credentials file must be a service account with BigQuery Admin privilege
credentials_file = "bq_service_account.json"  # admin

# Pass credentials manually or set them as an
# environmental variable to be used as default credential

# os.environ[
#     "GOOGLE_APPLICATION_CREDENTIALS"
# ] = credentials_file

client = bigquery.Client.from_service_account_json(credentials_file)

# GCP BIQ QUERY FILE INFO
# bq_filename = "backup_dataset.backup_table"
# table_id = "backup_dataset.backup_table"
bq_filename = "demo_dataset.backup_table"
table_id = "demo_dataset.backup_table"

ts_index_column = "ancillaryTimeStamp"

# # Download at most 10 rows.
# rows_iter = client.list_rows(table_id, max_results=10)
# rows = list(rows_iter)
# print("Downloaded {} rows from table {}".format(len(rows), table_id))

# sql = """
#  SELECT * FROM `backup_dataset.backup_table`
#  """

ancillary_sql = """
 SELECT temperature, netRadiation, battery, ancillaryTimeStamp FROM `demo_dataset.demo_table`
 """

df_ancillary = client.query(ancillary_sql).to_dataframe()
# df_ancillary
df_ancillary.index = df_ancillary.ancillaryTimeStamp
df_ancillary.drop(["ancillaryTimeStamp"], axis=1, inplace=True)
df_ancillary = df_ancillary[df_ancillary.index.notnull()]

flux_sql = """
 SELECT flux,fluxTimeStamp FROM `demo_dataset.demo_table`
 """

df_flux = client.query(flux_sql).to_dataframe()
# df_flux
df_flux.index = df_flux.fluxTimeStamp
df_flux.drop(["fluxTimeStamp"], axis=1, inplace=True)
df_flux = df_flux[df_flux.index.notnull()]