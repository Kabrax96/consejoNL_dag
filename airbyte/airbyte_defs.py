import dagster as dg
from dagster_airbyte import AirbyteResource, load_assets_from_airbyte_instance

airbyte = AirbyteResource(
    host=dg.EnvVar("AIRBYTE_HOST"),
    port=dg.EnvVar("AIRBYTE_PORT"),
    # username=dg.EnvVar("AIRBYTE_USERNAME"),
    # password=dg.EnvVar("AIRBYTE_PASSWORD"),
)

airbyte_assets = load_assets_from_airbyte_instance(airbyte=airbyte)
