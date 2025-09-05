Consejo NL – Dagster + dbt
=================================

Overview
- Orchestration of Consejo NL data pipelines using Dagster (Serverless), with modeling in dbt and extraction via Airbyte.
- Ready to run locally and deploy to Dagster Cloud via GitHub Actions.

Repository Layout
- `consejonl_fag/`: main Python package
  - `definitions.py`: Dagster `defs` (assets, jobs, schedules, sensors)
  - `dbt_defs.py`: `DbtCliResource` config, `dbt_only_job`, and the post‑Airbyte sensor
  - `integrations/`
    - `airbyte_custom.py`: asset that triggers an Airbyte OSS sync via API and waits until completion
  - `dbt/`
    - `consejonl/`: dbt project (macros, models by tiers, ymls)
    - `profiles_dagster/profiles.yml`: dbt profile for Dagster/Dagster Cloud
    - `profiles_local/profiles.yml`: sample local profile (do not use in Cloud)
  - `dagster_cloud.yaml`: Dagster Cloud code location configuration
- `.github/workflows/`: CI/CD to Dagster Cloud (prod and branch deployments)
- `requirements.txt`: dependencies used by CI build (same as `consejonl_fag/requirements.txt`)

Key Versions
- Python: 3.11
- Dagster / Dagster Cloud: 1.11.5
- dagster-dbt: 0.27.5
- dbt-core: 1.10.6, dbt-snowflake: 1.10.0

Environment Configuration
- Snowflake
  - `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`, `SNOWFLAKE_ROLE`, `SNOWFLAKE_WAREHOUSE`, `SNOWFLAKE_DATABASE`
  - `SNOWFLAKE_SCHEMA`: optional; if defined it applies to all targets
  - `SNOWFLAKE_SCHEMA_DEV`, `SNOWFLAKE_SCHEMA_PROD`: used when `SNOWFLAKE_SCHEMA` is not set
- dbt
  - `DBT_TARGET`: `dev` or `prod` (defaults to `dev` locally, set to `prod` in Cloud)
  - `DBT_PROJECT_DIR`, `DBT_PROFILES_DIR`: optional; when pointing to a non‑existent path, the code falls back to `consejonl_fag/dbt/consejonl` and `consejonl_fag/dbt/profiles_dagster`
  - `DAGSTER_DBT_PARSE_PROJECT_ON_LOAD`: optional (1/true to parse at load)
- Airbyte (if used in Cloud)
  - `AIRBYTE_BASE`: Airbyte API base URL (not localhost in Cloud)
  - `AIRBYTE_CONNECTION_ID`: connection id to sync

How dbt Paths Are Resolved
- `consejonl_fag/dbt_defs.py` resolves paths relative to the package and allows env var overrides.
- If `DBT_PROJECT_DIR` or `DBT_PROFILES_DIR` are set to paths that do not exist in the runtime, they are ignored with a warning and defaults are used.
- Strict validation happens inside the `run_dbt_build` op; if files are missing, the job fails with a clear message.

Local Development
1) Requirements
- Python 3.11 and access to Snowflake.

2) Create venv and install deps
- `python3.11 -m venv .venv`
- `. .venv/bin/activate`
- `pip install -r requirements.txt`

3) Example local environment variables
- `export DBT_TARGET=dev`
- `export SNOWFLAKE_ACCOUNT=...`
- `export SNOWFLAKE_USER=...`
- `export SNOWFLAKE_PASSWORD=...`
- `export SNOWFLAKE_ROLE=ACCOUNTADMIN`
- `export SNOWFLAKE_WAREHOUSE=COMPUTE_WH`
- `export SNOWFLAKE_DATABASE=CONSEJO_NL`
- `export SNOWFLAKE_SCHEMA_DEV=STAGING`

4) Run Dagster UI locally
- `dagster dev -m consejonl_fag.definitions`
- UI: http://localhost:3000

5) Run dbt from Dagster
- Job: `dbt_only_job` (runs `dbt deps` and `dbt build` with `--fail-fast`)
- You can pass additional selectors via the op config `run_dbt_build.extra_args` (e.g., `--select tag:balance_presupuestario_cp+`).

Assets, Jobs, Schedules
- Asset: `airbyte_sync_consejo_nl` (calls Airbyte API; requires `AIRBYTE_BASE` and `AIRBYTE_CONNECTION_ID`)
- Job: `elt_airbyte_only` (runs only the Airbyte asset)
- Job: `dbt_only_job` (runs dbt build)
- Schedule: `airbyte_daily_6utc` (runs `elt_airbyte_only` daily at 06:00 UTC)
- Sensor: `airbyte_to_dbt_sensor` (after the Airbyte asset run completes, triggers `dbt_only_job`; if the triggering run has tag `mode=cp`, adds specific dbt selectors)

dbt
- Project: `consejonl_fag/dbt/consejonl`
- Dagster profile: `consejonl_fag/dbt/profiles_dagster/profiles.yml`
- Local reference profile: `consejonl_fag/dbt/profiles_local/profiles.yml` (do not commit real credentials)
- Notable macros: `macros/is_cp.sql`, `macros/period_filter.sql`, `macros/generate_schema_name.sql`
- Models organized under `models/tier_1_staging`, `tier_2_intermediate`, `tier_3_marts`

Deploy to Dagster Cloud
- Code location file: `consejonl_fag/dagster_cloud.yaml`
  - `location_name: consejonl_dag`
  - `build.directory: ".."` (builds from repo root)
  - `module_name: consejonl_fag.definitions`
- GitHub Actions
  - Prod: `.github/workflows/deploy.yml` (push to `main`/`master`)
  - Branch deployments: `.github/workflows/branch_deployments.yml` (PRs)
  - Requires secrets: `DAGSTER_CLOUD_API_TOKEN`, `ORGANIZATION_ID`, Snowflake credentials, and optionally `DBT_TARGET`.
  - The prod workflow includes an optional dbt parse step to validate project/profile presence.

Best Practices
- Avoid absolute paths; rely on env vars or paths relative to the package.
- Never commit credentials; use Secrets/Env Vars in Dagster Cloud.
- Keep `dagster-dbt` and `dbt-core` versions compatible.

Common Issues
- “DBT project_dir not found …”: Ensure `DBT_PROJECT_DIR` is not set to a local path that doesn’t exist in Cloud; leave it unset or use a relative path (e.g., `dbt/consejonl`).
- “Received unexpected config entry env_vars”: In Dagster Serverless, `env_vars` is not allowed in `dagster_cloud.yaml`; configure env vars in the Cloud UI or workspace environment instead.

Useful Commands
- `dagster dev -m consejonl_fag.definitions` (local UI)
- `python -c "import consejonl_fag.dbt_defs as d; print(d.DBT_PROJECT_DIR, d.DBT_PROFILES_DIR)"` (debug paths)

Maintainers
- Track TODOs/issues in PRs. After deploys, check Cloud load logs for dbt/path warnings.
