from dagster_dbt import (
    DagsterDbtTranslator,
    DbtCliResource,
    dbt_assets,
)
from pathlib import Path
from typing import Any, Mapping
from dagster import AssetExecutionContext, AssetKey, load_assets_from_package_module
import os
from . import ingestion
from dagster_fivetran import load_assets_from_fivetran_instance
from .ingestion import fivetran

spotify_ads_assets = load_assets_from_package_module(package_module=ingestion)

fivetran_assets = load_assets_from_fivetran_instance(
    fivetran.fivetran_instance,
    io_manager_key="io_manager",
)


DBT_PROJECT_DIR = Path(__file__).joinpath("..", "..", "..", "dbt_project").resolve()
dbt = DbtCliResource(project_dir=os.fspath(DBT_PROJECT_DIR))

if os.getenv("DAGSTER_DBT_PARSE_PROJECT_ON_LOAD"):
    dbt_manifest_path = (
        dbt.cli(
            ["--quiet", "parse"],
            target_path=Path("target"),
        )
        .wait()
        .target_path.joinpath("manifest.json")
    )
else:
    dbt_manifest_path = DBT_PROJECT_DIR.joinpath("target", "manifest.json")


class CustomDagsterDbtTranslator(DagsterDbtTranslator):
    @classmethod
    def get_asset_key(cls, dbt_resource_props: Mapping[str, Any]) -> AssetKey:
        return super().get_asset_key(dbt_resource_props)


@dbt_assets(
    manifest=dbt_manifest_path,
    dagster_dbt_translator=CustomDagsterDbtTranslator(),
    io_manager_key="io_manager",
    name="dbt_assets",
)
def spotify_ads_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()