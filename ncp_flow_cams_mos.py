from cams_ncp_client.client import CamsNcpApiClient
from prefect import flow
from vito.sas.air.cams_client import CAMSEuropeClient, Pollutant

from _utils import assert_recent_flow_run


#
# @flow(log_prints=True)
# async def download_cams_netcdf(model_names: List[str]) -> None:
#     # Download the CAMS models asynchronously
#     stats_futures = download_cams_model_europe_api.map(model_names)
#     # wait for all tasks to finish
#     for future in stats_futures:
#         print(f"Task {future} completed with result: {future.result()}")


@flow(log_prints=True)
async def download_cams_mos() -> None:
    await assert_recent_flow_run("update-station-data")
    print(f"TODO: download_cams_mos")
    print("CAMS MOS done :)")



if __name__ == "__main__":
    download_cams_mos()