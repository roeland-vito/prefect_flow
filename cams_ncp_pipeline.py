from datetime import datetime
from pathlib import Path
from typing import List, Dict
from prefect import flow, task
from prefect.blocks.system import Secret
from prefect.variables import Variable
import requests
from vito.sas.air.cams_client import CAMSEuropeClient, Pollutant
import sys
from cams_ncp_client.client import CamsNcpApiClient
import tempfile

_client: CamsNcpApiClient | None  = None


@task
def download_cams_model(model_name: str) -> Dict[str, str]:
    cams_file_client = api_client().cams_file

    api_key = _get_api_key()
    api_url = "https://ads.atmosphere.copernicus.eu/api"
    # data_folder = _data_folder()

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


def api_client() -> CamsNcpApiClient:
    global _client
    if _client is None:
        api_base_url = Variable.get("cams_ncp_api_base_url", "http://127.0.0.1:5050")
        print(f"Using api_base_url {api_base_url}")
        _client = CamsNcpApiClient(api_base_url)
    return _client


def _get_api_key() -> str:
    # Secret(value="secret!-1234567890").save("cams-api-key", overwrite=True)  # to overwrite a Secret
    secret_block = Secret.load("cams-api-key")
    api_key = secret_block.get()
    if api_key is None or api_key.strip() == "":
        raise ValueError("API key is not set. Please set the API key in Prefect.")
    return api_key


@flow(log_prints=True)
def download_cams(model_names: List[str]) -> None:
    "Download the CAMS models  asynchronously"
    # Task 1: Download CAMS models concurrently
    print(f"Python executable: {sys.executable}")

    stats_futures = download_cams_model.map(model_names)

    # wait for all tasks to finish
    for future in stats_futures:
        print(f"Task {future} completed with result: {future.result()}")


@flow(log_prints=True)
def main() -> None:

    # print current Python executable
    print(f"Python executable: {sys.executable}")

    download_cams([
        "ensemble", "chimere", "emep"
    ])


if __name__ == "__main__":
    main()