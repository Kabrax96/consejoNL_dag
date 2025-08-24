# consejonl_fag/dbt_defs.py
from pathlib import Path
import os
import dagster as dg
from dagster import RunRequest, AssetExecutionContext
from dagster_dbt import DbtProject, DbtCliResource, dbt_assets

# =========================
# Rutas robustas (relativas)
# =========================
BASE_DIR = Path(__file__).resolve().parent

# Estructura esperada:
# consejonl_fag/
#   dbt/
#     consejonl/              <-- proyecto dbt (dbt_project.yml)
#     profiles_dagster/       <-- perfiles para Cloud (profiles.yml)
DBT_PROJECT_DIR = BASE_DIR / "dbt" / "consejonl"
DBT_PROFILES_DIR = BASE_DIR / "dbt" / "profiles_dagster"

# Validaciones tempranas (ayudan a fallar con mensaje claro si falta algo)
if not DBT_PROJECT_DIR.exists():
    raise RuntimeError(f"DBT project_dir not found: {DBT_PROJECT_DIR}")
if not DBT_PROFILES_DIR.exists():
    raise RuntimeError(f"DBT profiles_dir not found: {DBT_PROFILES_DIR}")

# Target configurable por env var; default "prod" en Cloud
DBT_TARGET = os.getenv("DBT_TARGET", "prod")

# =================================================
# Proyecto dbt + manifest autogenerado si está habilitado
# =================================================
project = DbtProject(project_dir=DBT_PROJECT_DIR)

# Si la variable de entorno DAGSTER_DBT_PARSE_PROJECT_ON_LOAD=1 está seteada,
# esto ejecuta 'dbt parse' al cargar la code location y genera target/manifest.json
project.prepare_if_dev()

# ======================
# Recurso CLI de dbt
# ======================
dbt = DbtCliResource(
    project_dir=str(DBT_PROJECT_DIR),
    profiles_dir=str(DBT_PROFILES_DIR),
    target=DBT_TARGET,
)

# ==========================================================
# Assets de dbt (require manifest.json ya presente en target)
# ==========================================================
@dbt_assets(manifest=project.manifest_path)
def dbt_models(context: AssetExecutionContext, dbt: DbtCliResource):
    # Ejecuta dbt build (models + seeds + snapshots + tests)
    yield from dbt.cli(["build", "--fail-fast"], context=context).stream()

# =======================================
# Job que ejecuta SOLO las assets de dbt
# =======================================
dbt_only_job = dg.define_asset_job(
    "dbt_only",
    selection=dg.AssetSelection.assets(dbt_models),
)

# ===========================================================
# Op opcional para ejecutar dbt build dentro de un job secuencial
# ===========================================================
@dg.op(required_resource_keys={"dbt"})
def run_dbt_build(context):
    deps = context.resources.dbt.cli(["deps"], context=context)
    deps.wait()
    inv = context.resources.dbt.cli(["build", "--fail-fast"], context=context)
    res = inv.wait()
    if not res.success:
        raise Exception("dbt build failed")

# ==================================================================
# Sensor: cuando termina la asset de Airbyte, dispara el job de dbt
# ==================================================================
@dg.asset_sensor(
    asset_key=dg.AssetKey("airbyte_sync_consejo_nl"),
    job=dbt_only_job,
    minimum_interval_seconds=5,
)
def airbyte_to_dbt_sensor(context, asset_event):
    """
    Lógica:
    - Lee los tags del run que ejecutó el asset de Airbyte.
    - Si trae tag mode=cp -> ejecuta dbt_only con vars is_cp=true y selección por tag CP.
    - Si no -> ejecuta dbt_only normal.
    """
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
