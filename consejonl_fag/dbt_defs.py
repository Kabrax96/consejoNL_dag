# consejonl_fag/dbt_defs.py
from pathlib import Path
import os
import dagster as dg
from dagster_dbt import DbtCliResource

BASE_DIR = Path(__file__).resolve().parent
DBT_PROJECT_DIR = BASE_DIR / "dbt" / "consejonl"
DBT_PROFILES_DIR = BASE_DIR / "dbt" / "profiles_dagster"

if not DBT_PROJECT_DIR.exists():
    raise RuntimeError(f"DBT project_dir not found: {DBT_PROJECT_DIR}")
if not DBT_PROFILES_DIR.exists():
    raise RuntimeError(f"DBT profiles_dir not found: {DBT_PROFILES_DIR}")

DBT_TARGET = os.getenv("DBT_TARGET", "dev")

dbt = DbtCliResource(
    project_dir=str(DBT_PROJECT_DIR),
    profiles_dir=str(DBT_PROFILES_DIR),
    target=DBT_TARGET,
)

@dg.op(config_schema={"extra_args": [str]}, required_resource_keys={"dbt"})
def run_dbt_build(context: dg.OpExecutionContext):
    extra_args = context.op_config.get("extra_args") or []
    context.resources.dbt.cli(["deps"], context=context).wait()
    context.resources.dbt.cli(["build", "--fail-fast", *extra_args], context=context).wait()

@dg.graph
def _dbt_build_graph():
    run_dbt_build()

dbt_only_job = _dbt_build_graph.to_job(name="dbt_only_job")

@dg.asset_sensor(
    asset_key=dg.AssetKey("airbyte_sync_consejo_nl"),
    job=dbt_only_job,
    minimum_interval_seconds=5,
)
def airbyte_to_dbt_sensor(context, asset_event):
    triggering_run = context.instance.get_run_by_id(asset_event.dagster_run.run_id)
    tags = triggering_run.tags if triggering_run else {}
    is_cp = tags.get("mode") == "cp"

    if is_cp:
        return dg.RunRequest(
            run_key=f"dbt_after_cp_{int(asset_event.timestamp)}",
            tags={"triggered_by": "airbyte_to_dbt_sensor", "mode": "cp"},
            run_config={
                "ops": {
                    "run_dbt_build": {
                        "config": {
                            "extra_args": [
                                "--vars", "{is_cp: true}",
                                "--select", "tag:balance_presupuestario_cp+",
                            ]
                        }
                    }
                }
            },
        )
    return dg.RunRequest(
        run_key=f"dbt_after_{int(asset_event.timestamp)}",
        tags={"triggered_by": "airbyte_to_dbt_sensor", "mode": "normal"},
    )
