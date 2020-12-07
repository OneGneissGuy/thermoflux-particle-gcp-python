from google.cloud import bigquery
from google.oauth2 import service_account
import json

# GCP STORAGE AND BIQ QUERY FILE INFO
project = "thermoflux-particle"
bucket_name = "thermoflux-bq-data"
dataset_id = "demo_dataset"
table_id = "demo_table"
bucket_filename = "demo_table_backup.csv"
credentials_file = "storage_service_account.json"

service_account_info = json.load(open(credentials_file))
credentials = service_account.Credentials.from_service_account_info(
    service_account_info
)

client = bigquery.Client.from_service_account_json(credentials_file)

destination_uri = "gs://{}/{}".format(bucket_name, bucket_filename)
dataset_ref = bigquery.DatasetReference(project, dataset_id)
table_ref = dataset_ref.table(table_id)

extract_job = client.extract_table(
    table_ref,
    destination_uri,
    # Location must match that of the source table.
    location="US",
)  # API request
extract_job.result()  # Waits for job to complete.

print("Exported {}:{}.{} to {}".format(project, dataset_id, table_id, destination_uri))
