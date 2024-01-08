from dagster import AssetSelection, define_asset_job

incremental_update = define_asset_job(
    name="incremental_update",
    selection=["*daily_ad_history", "*spotify_accounts_view", "*spotify_ads", "*spotify_campaigns", "*spotify_analysis"]

)

all_assets_job = define_asset_job(
    name="intial_populate",
    selection=AssetSelection.all()
)

