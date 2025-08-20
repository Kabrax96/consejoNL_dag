# airbyte/airflow_defs.py
import os, json, time, urllib.request
from dagster import op, Field, Out, In

AIRFLOW_API = os.environ["AIRFLOW_API"]      # ej: https://<tu-airflow>/api/v1
AIRFLOW_DAG_ID = os.environ["AIRFLOW_DAG_ID"]  # ej: post_rds_validations
AIRFLOW_TOKEN = os.getenv("AIRFLOW_TOKEN", "") # si usas bearer; si no, quítalo

def _req(path, method="GET", body=None):
    url = f"{AIRFLOW_API}{path}"
    data = None if body is None else json.dumps(body).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    if AIRFLOW_TOKEN:
        headers["Authorization"] = f"Bearer {AIRFLOW_TOKEN}"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read())

@op(out=Out(str), config_schema={"conf": Field(dict)})
def trigger_airflow(context):
    conf = context.op_config["conf"]
    resp = _req(f"/dags/{AIRFLOW_DAG_ID}/dagRuns", method="POST", body={"conf": conf})
    run_id = resp["dag_run_id"]
    context.log.info(f"Airflow DAG {AIRFLOW_DAG_ID} triggered: {run_id}")
    return run_id

@op(ins={"run_id": In(str)}, out=Out(dict))
def wait_for_airflow(context, run_id):
    while True:
        status = _req(f"/dags/{AIRFLOW_DAG_ID}/dagRuns/{run_id}")
        state = status.get("state")
        context.log.info(f"Airflow state={state}")
        if state in ("success", "failed"):
            if state != "success":
                raise Exception(f"Airflow failed: {status}")
            return status
        time.sleep(15)
