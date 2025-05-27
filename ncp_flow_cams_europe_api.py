import tempfile
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional

import requests
from cams_ncp_client.client import CamsNcpApiClient
from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner
from prefect.variables import Variable
from vito.sas.air.cams_client import CAMSEuropeClient, Pollutant

from _utils import print_env, get_secret, ncp_api_client


@task(retries=3, retry_delay_seconds=30)
def download_cams_model_europe_api_for_model(model_name: str) -> str:
    """Download CAMS Europe data for a single model"""
    try:
        print(f"Starting download for model: {model_name}")

        cams_file_client = ncp_api_client().cams_file
        api_key = get_secret("cams-api-key")
        api_url = Variable.get("cams_api_url", "https://ads.atmosphere.copernicus.eu/api")

        with tempfile.TemporaryDirectory() as tmpdir:
            data_folder = Path(tmpdir)
            data_folder.mkdir(parents=True, exist_ok=True)
            print(f"Temporary directory created: {data_folder}")

            date_start = datetime.now().date()
            forecast_days = 4
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
                    verify_ssl=session.verify
                )
                nc_file = cams_europe.download(data_folder, api_url=api_url, api_key=api_key, session=session)

                cams_file_client.upload_cams_file(
                    year=date_start.year,
                    month=date_start.month,
                    day=date_start.day,
                    cams_model=model_name,
                    pollutant=pollutant,
                    forecast_days=forecast_days,
                    file_path=nc_file
                )
                pollutant_results[pollutant] = nc_file.name
                print(f"✓ Successfully processed pollutant {pollutant} for model {model_name}")

        print(f"✓ Successfully processed model: {model_name}")
        return model_name
    except Exception as e:
        print(f"✗ Error processing model {model_name}: {str(e)}")
        raise


@flow(log_prints=True, task_runner=ConcurrentTaskRunner(max_workers=5))
def download_cams_europe_api(model_names: Optional[List[str]] = None) -> None:
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


if __name__ == "__main__":
    download_cams_europe_api()