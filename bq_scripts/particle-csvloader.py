"""
This Google Cloud Function transfers data from Google Cloud Bucket Storage to a Google BigQuery relational database table
    Add this to requirements.txt:    
    # Function dependencies, for example:
    # package>=version
    google.cloud.storage
    google.cloud.bigquery
"""
def csvloader(data,context):
    from google.cloud import bigquery
    device_id = "e00fce682d99af4881ea8981" 
    out_filename = "{}_particle_cloud_vars.csv".format(device_id) 
    dataset_id = "particle_cloud"

    client = bigquery.Client()
    dataset_ref = client.dataset(dataset_id)
    table_ref = client.dataset(dataset_id).table(device_id)

    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    job_config.skip_leading_rows = 1
    job_config.field_delimiter = ","
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.schema = [
    bigquery.SchemaField("flux", "FLOAT"),
    bigquery.SchemaField("netRadiation", "FLOAT"),
    bigquery.SchemaField("temperature", "FLOAT"),
    bigquery.SchemaField("deviceID", "STRING"),
    bigquery.SchemaField("dataTimeStamp", "TIMESTAMP")
    ]
    uri = "gs://gcp-particle-var/{}".format(out_filename)
    load_job = client.load_table_from_uri(uri, table_ref, job_config=job_config)
    load_job.result()
    destination_table = client.get_table(table_ref)