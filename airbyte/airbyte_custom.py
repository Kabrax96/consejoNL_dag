# airbyte/airbyte_custom.py
import os, time, requests
import dagster as dg

AIRBYTE_BASE = os.getenv("AIRBYTE_BASE", "http://localhost:8000")
CLIENT_ID = os.environ["AIRBYTE_CLIENT_ID"]
CLIENT_SECRET = os.environ["AIRBYTE_CLIENT_SECRET"]
CONNECTION_ID = os.environ["AIRBYTE_CONNECTION_ID"]

def _get_token():
    r = requests.post(
        f"{AIRBYTE_BASE}/api/v1/applications/token",
        json={"client_id": CLIENT_ID, "client_secret": CLIENT_SECRET},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()["access_token"]

def _api(method, path, **kwargs):
    headers = {"Authorization": f"Bearer {_get_token()}"}
    resp = requests.request(method, f"{AIRBYTE_BASE}{path}", headers=headers, timeout=60, **kwargs)
    # Bubble up HTTP errors with body text (easier to debug)
    try:
        resp.raise_for_status()
    except requests.HTTPError as e:
        raise RuntimeError(f"Airbyte API {method} {path} failed: {resp.status_code} {resp.text}") from e
    return resp

def _extract_job_id(data: dict) -> str:
    # Accept multiple shapes: {"job":{"id":123}}, {"id":123}, {"jobId":123}, {"job_id":123}
    job = data.get("job")
    if isinstance(job, dict) and "id" in job:
        return str(job["id"])
    for k in ("id", "jobId", "job_id"):
        if k in data:
            return str(data[k])
    raise RuntimeError(f"Unexpected /jobs response shape, cannot find job id: {data}")

def _extract_status(data: dict) -> str | None:
    # Accept {"job":{"status":"succeeded"}} or {"status":"succeeded"}
    if isinstance(data.get("job"), dict) and "status" in data["job"]:
        return data["job"]["status"]
    return data.get("status")

@dg.asset(description="Trigger Airbyte sync for Consejo NL and wait until it finishes.")
def airbyte_sync_consejo_nl(context: dg.AssetExecutionContext):
    # 1) trigger sync
    resp = _api(
        "POST",
        "/api/public/v1/jobs",
        json={"connectionId": CONNECTION_ID, "jobType": "sync"},
    )
    data = resp.json()
    job_id = _extract_job_id(data)
    context.log.info(f"Triggered Airbyte job {job_id} (raw response: {data})")

    # 2) poll status
    while True:
        j = _api("GET", f"/api/public/v1/jobs/{job_id}").json()
        status = _extract_status(j)
        context.log.info(f"Job {job_id} status: {status} (raw: {j})")
        if status in ("succeeded", "failed", "cancelled"):
            if status != "succeeded":
                raise RuntimeError(f"Airbyte job {job_id} ended with status {status}")
            break
        time.sleep(5)
