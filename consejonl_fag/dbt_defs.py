# consejonl_fag/dbt_defs.py
from pathlib import Path
import os
import dagster as dg
from dagster import RunRequest
from dagster_dbt import DbtCliResource
from dagster_dbt.asset_defs import load_assets_from_dbt_project

# === Rutas relativas y robustas ===
BASE_DIR = Path(__file__).resolve().parent
DBT_PROJECT_DIR = BASE_DIR / "dbt" / "consejonl"          # contiene dbt_project.yml
DBT_PROFILES_DIR = BASE_DIR / "dbt" / "profiles_dagster"  # contiene profiles.yml

if not DBT_PROJECT_DIR.exists():
    raise RuntimeError(f"DBT project_dir not found: {DBT_PROJECT_DIR}")
if not DBT_PROFILES_DIR.exists():
    raise RuntimeError(f"DBT profiles_dir not found: {DBT_PROFILES_DIR}")

DBT_TARGET = os.getenv("DBT_TARGET", "prod")

# Recurso CLI de dbt
dbt = DbtCliResource(
    project_dir=str(DBT_PROJECT_DIR),
    profiles_dir=str(DBT_PROFILES_DIR),
    target=DBT_TARGET,
)

# 👉 Carga assets parseando el proyecto en runtime (no requiere manifest.json previo)
dbt_models = load_assets_from_dbt_project(
    project_dir=str(DBT_PROJECT_DIR),
    profiles_dir=str(DBT_PROFILES_DIR),
    target=DBT_TARGET,
)

# Job que ejecuta únicamente los assets de dbt
dbt_only_job = dg.define_asset_job(
    "dbt_only",
    selection=dg.AssetSelection.assets(*dbt_models),
)

# (Opcional) op para ejecutar dbt build en jobs secuenciales
@dg.op(required_resource_keys={"dbt"})
def run_dbt_build(context):
    deps = context.resources.dbt.cli(["deps"], context=context)
    deps.wait()
    inv = context.resources.dbt.cli(["build", "--fail-fast"], context=context)
    res = inv.wait()
    if not res.success:
        raise Exception("dbt build failed")

# Sensor: cuando termina Airbyte, dispara dbt
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
