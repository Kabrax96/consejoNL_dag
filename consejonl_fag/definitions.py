# consejonl_fag/definitions.py
import dagster as dg
from .integrations.airbyte_custom import airbyte_sync_consejo_nl
from .dbt_defs import dbt, dbt_only_job, airbyte_to_dbt_sensor

# Job solo para el asset de Airbyte (útil para schedule)
airbyte_only_job = dg.define_asset_job(
    "elt_airbyte_only",
    selection=dg.AssetSelection.keys("airbyte_sync_consejo_nl"),
)

# Schedule diario 06:00 UTC
airbyte_daily_6utc = dg.ScheduleDefinition(
    job=airbyte_only_job,
    cron_schedule="0 6 * * *",
)

defs = dg.Definitions(
    assets=[airbyte_sync_consejo_nl],       # dbt corre como job, no como assets
    resources={"dbt": dbt},
    jobs=[airbyte_only_job, dbt_only_job],
    schedules=[airbyte_daily_6utc],
    sensors=[airbyte_to_dbt_sensor],
)
