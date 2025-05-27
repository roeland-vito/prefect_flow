import asyncio

from cams_ncp_client.client import CamsNcpApiClient
from prefect import flow, get_client, State
from prefect.variables import Variable
from vito.sas.air.cams_client import CAMSEuropeClient, Pollutant

from _utils import print_env, was_flow_successful_recently, assert_recent_flow_run


@flow(log_prints=True)
async def download_observations() -> None:
    # Check if station data is fresh enough
    await assert_recent_flow_run("update_station_data")

    print("TODO: Download observations ...")


@flow(log_prints=True)
def main() -> None:
    if Variable.get("debug_python_worker_env", False):
        print_env()

    download_observations()
    print("Flow download_observations done :)")


if __name__ == "__main__":
    main()