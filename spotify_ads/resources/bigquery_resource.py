import os
from dagster import resource, Field
from google.cloud import bigquery

@resource(config_schema={"dataset_id": Field(str)})
def bigquery_resource(context):
    project_id = os.environ.get("PROJECT_ID")
    credentials_path = os.environ.get("BIGQUERY_SERVICE_ACCOUNT_CREDENTIALS")
    dataset_id = context.resource_config["BIGQUERY_DATASET_ID"]
    client = bigquery.Client(project=project_id, credentials=credentials_path)
    dataset_ref = client.dataset(dataset_id)
    return client.get_dataset(dataset_ref)
