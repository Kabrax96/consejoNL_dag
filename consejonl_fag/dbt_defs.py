# consejonl_fag/dbt_defs.py
from pathlib import Path
import os
import dagster as dg
from dagster import RunRequest, AssetExecutionContext
from dagster_dbt import DbtProject, DbtCliResource, dbt_assets

BASE_DIR = Path(__file__).resolve().parent
DBT_PROJECT_DIR = BASE_DIR / "dbt" / "consejonl"
DBT_PROFILES_DIR = BASE_DIR / "dbt" / "profiles_dagster"

if not DBT_PROJECT_DIR.exists():
    raise RuntimeError(f"DBT project_dir not found: {DBT_PROJECT_DIR}")
if not DBT_PROFILES_DIR.exists():
    raise RuntimeError(f"DBT profiles_dir not found: {DBT_PROFILES_DIR}")

DBT_TARGET = os.getenv("DBT_TARGET", "prod")

# Proyecto dbt
project = DbtProject(project_dir=DBT_PROJECT_DIR)

# (Opcional) si además pones la env var DAGSTER_DBT_PARSE_PROJECT_ON_LOAD=1,
# esto generará el manifest en load:
project.prepare_if_dev()

# Recurso CLI de dbt
dbt = DbtCliResource(
    project_dir=str(DBT_PROJECT_DIR),
    profiles_dir=str(DBT_PROFILES_DIR),
    target=DBT_TARGET,
)

# Assets de dbt (requieren target/manifest.json)
@dbt_assets(manifest=project.manifest_path)
def dbt_models(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build", "--fail-fast"], context=context).stream()

# Job solo dbt
dbt_only_job = dg.define_asset_job(
    "dbt_only",
    selection=dg.AssetSelection.assets(dbt_models),
)

# (Opcional) op para pipeline secuencial
@dg.op(required_resource_keys={"dbt"})
def run_dbt_build(context):
    deps = context.resources.dbt.cli(["deps"], context=context)
    deps.wait()
    inv = context.resources.dbt.cli(["build", "--fail-fast"], context=context)
    res = inv.wait()
    if not res.success:
        raise Exception("dbt build failed")

# Sensor: al terminar Airbyte dispara dbt
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
        return RunRequest(
            run_key=f"dbt_after_cp_{int(asset_event.timestamp)}",
            tags={"triggered_by": "airbyte_to_dbt_sensor", "mode": "cp"},
            run_config={
                "resources": {
                    "dbt": {
                        "config": {
                            "vars": {"is_cp": True},
                            "args": ["--select", "tag:balance_presupuestario_cp+"],
                        }
                    }
                }
            },
        )
    else:
        return RunRequest(
            run_key=f"dbt_after_{int(asset_event.timestamp)}",
            tags={"triggered_by": "airbyte_to_dbt_sensor", "mode": "normal"},
        )
