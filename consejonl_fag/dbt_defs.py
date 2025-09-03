# consejonl_fag/dbt_defs.py
from pathlib import Path
import os
import dagster as dg
from dagster_dbt import DbtCliResource

# Resolve paths relative to this file, with env var overrides for flexibility
PACKAGE_ROOT = Path(__file__).resolve().parent
PROJECT_ROOT = Path(os.getenv("PROJECT_ROOT", str(PACKAGE_ROOT))).resolve()

# Define the paths for the dbt project and profiles (overridable via env)
DBT_PROJECT_DIR = Path(
    os.getenv("DBT_PROJECT_DIR", str(PROJECT_ROOT / "dbt" / "consejonl"))
).resolve()
DBT_PROFILES_DIR = Path(
    os.getenv("DBT_PROFILES_DIR", str(PROJECT_ROOT / "dbt" / "profiles_dagster"))
).resolve()

# Debugging: Print the resolved paths (useful in Cloud)
print(f"Resolved PROJECT_ROOT: {PROJECT_ROOT}")
print(f"Resolved DBT_PROJECT_DIR: {DBT_PROJECT_DIR}")
print(f"Resolved DBT_PROFILES_DIR: {DBT_PROFILES_DIR}")

# Validate the existence of the directories and files early (fail fast if misconfigured)
if not DBT_PROJECT_DIR.exists():
    raise RuntimeError(f"DBT project_dir not found: {DBT_PROJECT_DIR}")
if not (DBT_PROJECT_DIR / "dbt_project.yml").exists():
    raise RuntimeError(f"dbt_project.yml not found in: {DBT_PROJECT_DIR}")
if not DBT_PROFILES_DIR.exists():
    raise RuntimeError(f"DBT profiles_dir not found: {DBT_PROFILES_DIR}")
if not (DBT_PROFILES_DIR / "profiles.yml").exists():
    raise RuntimeError(f"profiles.yml not found in: {DBT_PROFILES_DIR}")

# Get the dbt target from the environment, default to "dev"
DBT_TARGET = os.getenv("DBT_TARGET", "dev")

# Debugging: Print the dbt target
print(f"DBT_TARGET: {DBT_TARGET}")

# Initialize the DbtCliResource
dbt = DbtCliResource(
    project_dir=str(DBT_PROJECT_DIR),
    profiles_dir=str(DBT_PROFILES_DIR),
    target=DBT_TARGET,
)

# Define a Dagster operation to run dbt build
@dg.op(config_schema={"extra_args": [str]}, required_resource_keys={"dbt"})
def run_dbt_build(context: dg.OpExecutionContext):
    extra_args = context.op_config.get("extra_args") or []
    context.resources.dbt.cli(["deps"], context=context).wait()
    context.resources.dbt.cli(["build", "--fail-fast", *extra_args], context=context).wait()

# Define a Dagster graph for dbt build
@dg.graph
def _dbt_build_graph():
    run_dbt_build()

# Create a Dagster job from the graph
dbt_only_job = _dbt_build_graph.to_job(name="dbt_only_job")

# Define a sensor to trigger the dbt job after an Airbyte asset sync
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
