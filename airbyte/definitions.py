import dagster as dg
from .airbyte_custom import airbyte_sync_consejo_nl
from .dbt_defs import dbt_models, dbt, dbt_only_job, airbyte_to_dbt_sensor

defs = dg.Definitions(
    assets=[airbyte_sync_consejo_nl, dbt_models],
    resources={"dbt": dbt},
    schedules=[
        dg.ScheduleDefinition(
            job=dg.define_asset_job(
                "elt_airbyte_only",
                selection=dg.AssetSelection.keys("airbyte_sync_consejo_nl"),
            ),
            cron_schedule="0 6 * * *",
        )
    ],
    jobs=[dbt_only_job],
    sensors=[airbyte_to_dbt_sensor],
)
