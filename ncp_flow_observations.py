from cams_ncp_client.client import CamsNcpApiClient
from prefect import flow
from prefect.variables import Variable
from vito.sas.air.cams_client import CAMSEuropeClient, Pollutant

from _utils import print_env, assert_recent_flow_run


@flow(log_prints=True)
async def download_observations() -> None:
    if Variable.get("debug_python_worker_env", False):
        print_env()

    await assert_recent_flow_run("update-station-data")
    print("Flow download_observations done :)")


if __name__ == "__main__":
    download_observations()