import tempfile
from datetime import datetime
from pathlib import Path
from typing import List, Dict

import requests
from cams_ncp_client.client import CamsNcpApiClient
from prefect import flow, task
from prefect.variables import Variable
from vito.sas.air.cams_client import CAMSEuropeClient, Pollutant

from _utils import print_env, get_secret, ncp_api_client


@task
def download_cams_model_europe_api(model_name: str) -> Dict[str, str]:
    cams_file_client = ncp_api_client().cams_file
    api_key = get_secret("cams_api_key")
    api_url = Variable.get("cams_api_url", "https://ads.atmosphere.copernicus.eu/api")

    with tempfile.TemporaryDirectory() as tmpdir:
        data_folder = Path(tmpdir)
        data_folder.mkdir(parents=True, exist_ok=True)
        print(f"Temporary directory created: {data_folder}")

        date_start = datetime.now().date()
        forecast_days = 4
        print(f"Downloading CAMS model {model_name} from {api_url} to {data_folder.absolute()}")

        session = requests.Session()
        session.verify =  bool(Variable.get("ssl_verify", True))

        return_dict = {}
        for pollutant in Pollutant.all():
            print(f"Downloading CAMS model {model_name} for pollutant {pollutant} to {data_folder.absolute()}")
            cams_europe = CAMSEuropeClient(pollutant=Pollutant.PM10, cams_model_name=model_name, date_start=date_start, forecast_days=forecast_days, verify_ssl=session.verify)
            nc_file = cams_europe.download(data_folder, api_url=api_url, api_key=api_key, session=session)

            cams_file_client.upload_cams_file(year=date_start.year,month=date_start.month,day=date_start.day,cams_model=model_name,pollutant=pollutant,forecast_days=forecast_days,file_path=nc_file)
            return_dict[pollutant] = nc_file.name
    return return_dict


@flow(log_prints=True)
async def download_cams_netcdf(model_names: List[str]) -> None:
    # Download the CAMS models asynchronously
    stats_futures = download_cams_model_europe_api.map(model_names)
    # wait for all tasks to finish
    for future in stats_futures:
        print(f"Task {future} completed with result: {future.result()}")


@flow(log_prints=True)
async def download_cams_netcdf_all() -> None:
    if Variable.get("debug_python_worker_env", False):
        print_env()

    cams_models = Variable.get("cams_models", default=[])
    download_cams_netcdf(cams_models)
    print("CAMS NCP done :)")


if __name__ == "__main__":
    download_cams_netcdf()