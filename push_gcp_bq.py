import json
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd

# Biq Query Admin Service account CREDENTIALS
credentials_file = "storage_service_account.json"
# GCP BIQ QUERY FILE INFO
table_id = "thermoflux-particle.demo_dataset.demo_table"
# table_id = "thermoflux-particle.test_dataset.test_table"

service_account_info = json.load(open(credentials_file))
credentials = service_account.Credentials.from_service_account_info(
    service_account_info
)

# Service account must have BigQuery Admin Role
# https://cloud.google.com/docs/authentication/getting-started
# create a client instance
client = bigquery.Client.from_service_account_json(credentials_file)

filename = "e00fce682d99af4881ea8981_BQ_export-11192020-backfill-formatted.csv"
f = open(filename, "rb")

job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.CSV,
    skip_leading_rows=1,
    write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
)

job_config.schema = [
    bigquery.SchemaField("temperature", "NUMERIC"),
    bigquery.SchemaField("ancillaryTimeStamp", "TIMESTAMP"),
    bigquery.SchemaField("netRadiation", "NUMERIC"),
    bigquery.SchemaField("battery", "NUMERIC"),
    bigquery.SchemaField("flux", "NUMERIC"),
    bigquery.SchemaField("fluxTimeStamp", "TIMESTAMP"),
    bigquery.SchemaField("device_id", "STRING"),
]

load_job = client.load_table_from_file(f, table_id, job_config=job_config)
load_job.result()  # Wait for the job to complete.
# client.load_table_from_dataframe(df, table_id, job_config=job_config).result()
f.close()