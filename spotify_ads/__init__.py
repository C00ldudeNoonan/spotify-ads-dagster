from dagster import (
    Definitions,
    EnvVar,
    ScheduleDefinition,
    define_asset_job,
    load_assets_from_package_module,
)
from dagster_gcp_pandas import BigQueryPandasIOManager
from dagster_gcp import BigQueryResource
from .assets import dbt_manifest_path, DBT_PROJECT_DIR
from dagster_dbt import DbtCliResource
from .schedules import daily_refresh
from .jobs import incremental_update, all_assets_job


defs = Definitions(
    assets=load_assets_from_package_module(assets),
    jobs=[incremental_update,all_assets_job],
    resources={
        "io_manager": BigQueryPandasIOManager(
            project=EnvVar("BIGQUERY_PROJECT_ID"),
            dataset=EnvVar("BIGQUERY_DATASET_ID"),
            #gcp_credentials=EnvVar("BIGQUERY_SERVICE_ACCOUNT_CREDENTIALS"),
        ),
        "bigquery":BigQueryResource(
            project=EnvVar("BIGQUERY_PROJECT_ID"),
            #credentials=EnvVar("BIGQUERY_SERVICE_ACCOUNT_CREDENTIALS"),
        ),
        "dbt": DbtCliResource(project_dir=DBT_PROJECT_DIR) ,
    },
    schedules=[daily_refresh],
)

