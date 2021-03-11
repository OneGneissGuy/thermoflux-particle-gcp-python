from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import matplotlib.pyplot as plt

# google-cloud-bigquery==2.0.0
# google-cloud-bigquery-storage==2.0.0
# pandas==1.1.3
# pandas-gbq==0.14.0
# pyarrow==1.0.1
# grpcio==1.32.0

sql = """
    SELECT name, SUM(number) as count
    FROM `bigquery-public-data.usa_names.usa_1910_current`
    GROUP BY name
    ORDER BY count DESC
    LIMIT 10
"""

df = client.query(sql).to_dataframe()
# from google.cloud import bigquery
# client = bigquery.Client()
project = "bigquery-public-data"
dataset_id = "samples"

dataset_ref = bigquery.DatasetReference(project, dataset_id)
table_ref = dataset_ref.table("shakespeare")
table = client.get_table(table_ref)

df = client.list_rows(table).to_dataframe()