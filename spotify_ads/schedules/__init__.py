from dagster import AssetSelection, ScheduleDefinition

from ..jobs import incremental_update


daily_refresh = ScheduleDefinition(
    job=incremental_update,
    cron_schedule="0 0 * * *", # every day at midnight
)

# weekly_update_schedule = ScheduleDefinition(
#   job=intial_populate,
#   cron_schedule="0 0 * * 1", # every Monday at midnight
# )