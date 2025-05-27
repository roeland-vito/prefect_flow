from cams_ncp_client.client import CamsNcpApiClient
from prefect import flow
from prefect.variables import Variable
from vito.sas.air.cams_client import CAMSEuropeClient, Pollutant

from _utils import print_env




@flow(log_prints=True)
async def update_station_data() -> None:
    print(f"TODO: Update station data")


@flow(log_prints=True)
def main() -> None:
    if Variable.get("debug_python_worker_env", False):
        print_env()

    update_station_data()
    print("Flow update_station_data done :)")


if __name__ == "__main__":
    main()