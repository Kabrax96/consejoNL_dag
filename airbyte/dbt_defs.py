from pathlib import Path
import os
import dagster as dg
from dagster_dbt import DbtProject, DbtCliResource, dbt_assets

# === Ruta a tu proyecto dbt ===
DBT_DIR = Path("/Users/alexm./Desktop/consejoNL_dbt/consejonl")  # <- tu ruta

project = DbtProject(project_dir=DBT_DIR)
project.prepare_if_dev()  # asegura manifest en dev

# === Recurso dbt CLI ===
dbt = DbtCliResource(
    project_dir=str(DBT_DIR),
    target="dev",
    profiles_dir=os.getenv("DBT_PROFILES_DIR"),
)

# === Op para correr dbt build "ad hoc" (para jobs secuenciales) ===
@dg.op(required_resource_keys={"dbt"})
def run_dbt_build(context):
    inv = context.resources.dbt.cli(["build", "--fail-fast"], context=context)
    res = inv.wait()
    if not res.success:
        raise Exception("dbt build failed")

# === Assets de dbt (para modo assets + jobs basados en selección) ===
# Nota: sin group_name/deps si tu versión de dagster_dbt no lo soporta.
@dbt_assets(manifest=project.manifest_path)
def dbt_models(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    # Ejecuta dbt build (models + seeds + snapshots + tests)
    yield from dbt.cli(["build"], context=context).stream()

# === Job que ejecuta SOLO las assets de dbt ===
dbt_only_job = dg.define_asset_job(
    "dbt_only",
    selection=dg.AssetSelection.assets(dbt_models),
)

# === Sensor: cuando termina la asset de Airbyte, dispara el job de dbt ===
@dg.asset_sensor(
    asset_key=dg.AssetKey("airbyte_sync_consejo_nl"),
    job=dbt_only_job,
    minimum_interval_seconds=5,
)
def airbyte_to_dbt_sensor(context, asset_event):
    return dg.RunRequest(
        run_key=f"dbt_after_{int(asset_event.timestamp)}",
        tags={"triggered_by": "airbyte_to_dbt_sensor"},
    )
