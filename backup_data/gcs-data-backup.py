import base64
from google.cloud import bigquery
import json

# GCP STORAGE AND BIQ QUERY FILE INFO
project = "thermoflux-particle"
bucket_name = "thermoflux-bq-data"
dataset_id = "demo_dataset"
table_id = "demo_table"
bucket_filename = "demo_table_backup.csv"


def backup_bq_to_gcs(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    pubsub_message = base64.b64decode(event["data"]).decode("utf-8")
    client = bigquery.Client()

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
    print(
        "Exported {}:{}.{} to {}".format(project, dataset_id, table_id, destination_uri)
    )

    print(pubsub_message)
