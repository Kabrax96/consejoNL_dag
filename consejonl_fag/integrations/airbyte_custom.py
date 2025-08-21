# consejonl_fag/integrations/airbyte_custom.py
import os
import time
from typing import Optional
import requests
import dagster as dg

AIRBYTE_BASE = os.getenv("AIRBYTE_BASE", "http://localhost:8000")
CONNECTION_ID = os.getenv("AIRBYTE_CONNECTION_ID")


def _api(method: str, path: str, **kwargs) -> requests.Response:
    url = f"{AIRBYTE_BASE}{path}"
    resp = requests.request(method, url, timeout=60, **kwargs)
    try:
        resp.raise_for_status()
    except requests.HTTPError as e:
        raise RuntimeError(
            f"Airbyte API {method} {path} failed: {resp.status_code} {resp.text}"
        ) from e
    return resp


def _extract_job_id(data: dict) -> str:
    # Soporta varias formas de respuesta
    job = data.get("job")
    if isinstance(job, dict) and "id" in job:
        return str(job["id"])
    for k in ("id", "jobId", "job_id"):
        if k in data:
            return str(data[k])
    raise RuntimeError(f"Unexpected jobs response, cannot find job id: {data}")


def _extract_status(data: dict) -> Optional[str]:
    # Soporta {"job":{"status":"succeeded"}} o {"status":"succeeded"}
    if isinstance(data.get("job"), dict) and "status" in data["job"]:
        return data["job"]["status"]
    return data.get("status")


@dg.asset(description="Trigger Airbyte sync for Consejo NL and wait until it finishes.")
def airbyte_sync_consejo_nl(context: dg.AssetExecutionContext):
    if not CONNECTION_ID:
        raise dg.Failure("Missing AIRBYTE_CONNECTION_ID")

    # 1) Disparar sync (OSS)
    resp = _api(
        "POST",
        "/api/v1/connections/sync",
        json={"connectionId": CONNECTION_ID},
    )
    data = resp.json()
    job_id = _extract_job_id(data)
    context.log.info(f"Triggered Airbyte job {job_id} (raw: {data})")

    # 2) Polling de estado (OSS)
    while True:
        j = _api("POST", "/api/v1/jobs/get", json={"id": job_id}).json()
        status = _extract_status(j)
        context.log.info(f"Job {job_id} status: {status} (raw: {j})")
        if status in ("succeeded", "failed", "cancelled"):
            if status != "succeeded":
                raise RuntimeError(f"Airbyte job {job_id} ended with status {status}")
            break
        time.sleep(5)
