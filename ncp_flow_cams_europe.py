import tempfile
from datetime import datetime, date
from pathlib import Path
from typing import List, Optional, Final

import requests
from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner
from prefect.variables import Variable
from pydantic import BaseModel
from vito.sas.air.cams_client import CAMSEuropeClient, Pollutant
from vito.sas.air.cams_ftp_client import CAMSftpClient
from vito.sas.air.utils.cams_utils import crop_to_extent

from _utils import get_secret, ncp_api_client, get_var_object

CAMS_EUROPE_CDS_API: Final[str] = "CDS_API"
CAMS_EUROPE_FTP: Final[str] = "FTP"


class Extent(BaseModel):
    lon_min: float = 2.5  # Field(2.5, description="Minimum longitude")
    lon_max: float = 6.5  # , description="Maximum longitude")
    lat_min: float = 49.0  # , description="Minimum latitude")
    lat_max: float = 52.0  # , description="Maximum latitude")

    class Config:
        frozen = True

    def as_list(self) -> List[float]:
        return [self.lat_max, self.lon_min, self.lat_min, self.lon_max]

@flow(log_prints=True, task_runner=ConcurrentTaskRunner(max_workers=5))
def download_cams_europe(model_names: Optional[List[str]] = None) -> None:
    """Download CAMS Europe models"""
    if model_names is None:
        model_names = Variable.get("cams_models", default=[])

    if not model_names:
        print("No CAMS models specified. Exiting.")
        return

    print(f"Downloading CAMS models: {model_names}")

    # Submit all model tasks concurrently
    task_futures = []
    for model_name in model_names:
        future = download_cams_model_europe_api_for_model.submit(model_name)
        task_futures.append((model_name, future))

    # Wait for all tasks and handle results/errors
    successful_models = []
    failed_models = []

    for model_name, future in task_futures:
        try:
            result = future.result()
            successful_models.append(model_name)
        except Exception as e:
            failed_models.append((model_name, str(e)))
            print(f"✗ Task CAMS Europe for model {model_name} failed: {str(e)}")

    # Final summary
    print(f"✓ Successfully downloaded {len(successful_models)} CAMS models.")
    if failed_models:
        print(f"✗ Failed to run {len(failed_models)} tasks:")
        for model_name, error in failed_models:
            print(f"  Error for model {model_name}: {error}")

    print("CAMS Europe download with CDS API completed :)")


@task(retries=3, retry_delay_seconds=30)
def download_cams_model_europe_api_for_model(model_name: str) -> str:
    """Download CAMS Europe data for a single model"""
    try:
        cams_europe_with = Variable.get("cams_europe_with", default="CDS_API")  # CDS_API or FTP are allowed options
        if CAMS_EUROPE_CDS_API == cams_europe_with:
            print(f"Downloading CAMS Europe data for model {model_name} with CDS API")
            return _download_cams_model_europe_with_cds_api(model_name)
        elif CAMS_EUROPE_FTP == cams_europe_with:
            print(f"Downloading CAMS Europe data for model {model_name} with FTP")
            return _download_cams_model_europe_with_ftp(model_name)
        else:
            raise ValueError(f"Invalid value for cams_europe_with: {cams_europe_with}. Expected 'CDS_API' or 'FTP'.")
    except Exception as e:
        print(f"✗ Error processing model {model_name}: {str(e)}")
        raise

@task
def upload_cams_file(nc_file: Path, base_date: date, model_name: str, pollutant: str, forecast_days=4):
     print(f"upload_cams_file nc_file: {nc_file}, base_date: {base_date}, model_name: {model_name}, pollutant: {pollutant}, forecast_days: {forecast_days}")
     cams_file_client = ncp_api_client().cams_file
     cams_file_client.upload_cams_file(
         year=base_date.year,
         month=base_date.month,
         day=base_date.day,
         cams_model=model_name,
         pollutant=pollutant,
         forecast_days=forecast_days,
         file_path=nc_file
     )


def _download_cams_model_europe_with_ftp(model_name: str) -> str:
    ftp_host = Variable.get("cams_ftp_host", "aux.ecmwf.int")
    ftp_root = Variable.get("cams_ftp_root", "/DATA/CAMS_EUROPE_AIR_QUALITY")
    extent = _get_cams_extent()
    for pollutant in Pollutant.all():
        ftp_client = CAMSftpClient(pollutant=pollutant, ftp_host=ftp_host, ftp_root=ftp_root, ftp_user=get_secret("cams-ftp-user"))
        print(f"Start downloading CAMS data.  model: {model_name} pollutant: {pollutant}")
        with tempfile.TemporaryDirectory() as tmpdir:
            cams_file = ftp_client.download(tmpdir, ftp_pw=get_secret("cams-ftp-pw"))
            # crop the netCDF file to the given extent
            cropped_file = cams_file.parent / f"{cams_file.stem}_cropped.nc"
            print(f"Path cropped file: {cropped_file.absolute()}. extent: {extent}")
            crop_to_extent(cams_file, cropped_file, lon_min=extent.lon_min, lon_max=extent.lon_max, lat_min=extent.lat_min, lat_max=extent.lat_max)
            # upload the cropped file to the CAMS file client
            upload_cams_file(cropped_file, ftp_client.date_start, model_name, pollutant)

    return model_name


def _get_cams_extent() -> Extent:
    return get_var_object("cams_extent", Extent, default=Extent())


def _download_cams_model_europe_with_cds_api(model_name: str) -> str:
    """
    Download CAMS Europe model data using the CDS API.
    """
    print(f"Start downloading  model: {model_name} with CDS API")

    api_key = get_secret("cams-api-key")
    api_url = Variable.get("cams_api_url", "https://ads.atmosphere.copernicus.eu/api")
    cams_area = _get_cams_extent().as_list()
    print(f"Cams area: {cams_area}")
    with tempfile.TemporaryDirectory() as tmpdir:
        data_folder = Path(tmpdir)
        data_folder.mkdir(parents=True, exist_ok=True)
        print(f"Temporary directory created: {data_folder}")

        date_start = datetime.now().date()
        # forecast_days = 4
        print(f"Downloading CAMS model {model_name} from {api_url} to {data_folder.absolute()}")

        session = requests.Session()
        session.verify = bool(Variable.get("ssl_verify", True))

        # Process all pollutants for this model
        pollutant_results = {}
        for pollutant in Pollutant.all():
            print(f"Downloading CAMS model {model_name} for pollutant {pollutant} to {data_folder.absolute()}")
            cams_europe = CAMSEuropeClient(
                pollutant=pollutant,  # Fixed: use actual pollutant instead of hardcoded PM10
                cams_model_name=model_name,
                date_start=date_start,
                verify_ssl=session.verify,
                area=cams_area
            )
            nc_file = cams_europe.download(data_folder, api_url=api_url, api_key=api_key, session=session)

            upload_cams_file(nc_file, date_start, model_name, pollutant)

            pollutant_results[pollutant] = nc_file.name
            print(f"✓ Successfully processed pollutant {pollutant} for model {model_name}")

    print(f"✓ Successfully processed model: {model_name}")
    return model_name

if __name__ == "__main__":
    download_cams_europe()