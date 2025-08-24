
from pathlib import Path
import os
import dagster as dg
from dagster_dbt import DbtCliResource

# === Rutas del proyecto dbt ===
BASE_DIR = Path(__file__).resolve().parent
DBT_PROJECT_DIR = BASE_DIR / "dbt" / "consejonl"          # contiene dbt_project.yml
DBT_PROFILES_DIR = BASE_DIR / "dbt" / "profiles_dagster"  # contiene profiles.yml

if not DBT_PROJECT_DIR.exists():
    raise RuntimeError(f"DBT project_dir not found: {DBT_PROJECT_DIR}")
if not DBT_PROFILES_DIR.exists():
    raise RuntimeError(f"DBT profiles_dir not found: {DBT_PROFILES_DIR}")

# Target desde env (defecto: dev)
DBT_TARGET = os.getenv("DBT_TARGET", "dev")

# Recurso CLI de dbt
dbt = DbtCliResource(
    project_dir=str(DBT_PROJECT_DIR),
    profiles_dir=str(DBT_PROFILES_DIR),
    target=DBT_TARGET,
)

# --- Op: ejecuta dbt build. Puedes pasar args extra desde run_config ---
@dg.op(config_schema={"extra_args": [str]}, required_resource_keys={"dbt"})
def run_dbt_build(context: dg.OpExecutionContext):
    extra_args = context.op_config.get("extra_args") or []

    context.log.info("Running `dbt deps` …")
    context.resources.dbt.cli(["deps"], context=context).wait()

    cmd = ["build", "--fail-fast", *extra_args]
    context.log.info(f"Running `dbt {' '.join(extra_args)}` …")
    # En dagster-dbt 0.27.x, .wait() levanta excepción si falla; si retorna, fue OK.
    context.resources.dbt.cli(cmd, context=context).wait()

# --- Job simple que llama al op anterior ---
@dg.graph
def _dbt_build_graph():
    run_dbt_build()

dbt_only_job = _dbt_build_graph.to_job(name="dbt_only_job")

# --- Sensor: después del asset de Airbyte, dispara el job de dbt ---
@dg.asset_sensor(
    asset_key=dg.AssetKey("airbyte_sync_consejo_nl"),
    job=dbt_only_job,
    minimum_interval_seconds=5,
)
def airbyte_to_dbt_sensor(context, asset_event):
    """
    Si el run que disparó el asset de Airbyte trae tag mode=cp,
    corremos dbt con selección por tag y una var is_cp=true.
    """
    triggering_run = context.instance.get_run_by_id(asset_event.dagster_run.run_id)
    tags = triggering_run.tags if triggering_run else {}
    is_cp = tags.get("mode") == "cp"

    if is_cp:
        # Pasa flags/vars al op vía config
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

    # modo normal
    return dg.RunRequest(
        run_key=f"dbt_after_{int(asset_event.timestamp)}",
        tags={"triggered_by": "airbyte_to_dbt_sensor", "mode": "normal"},
    )
