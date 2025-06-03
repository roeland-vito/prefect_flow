import tempfile
from datetime import datetime
from typing import List, Optional, Tuple

from cams_ncp_client.cams_file import CamsFileClient
from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner
from prefect.variables import Variable
from vito.sas.air.utils.cams_utils import Pollutant

from _utils import ncp_api_client


@flow(log_prints=True,
      task_runner=ConcurrentTaskRunner(max_workers=5),
      retries=6, retry_delay_seconds=900)  # Will retry up to 6 times, every 15 minutes)
def process_cams_europe(model_names: Optional[List[str]] = None) -> None:
    """Download CAMS Europe models"""
    if model_names is None:
        model_names = Variable.get("cams_models", default=[])

    if not model_names:
        print("No CAMS models specified. Exiting.")
        return

    pollutants: List[str] = Pollutant.all()
    # obs_client: ObservationClient = ncp_api_client().observation
    cams_file_client: CamsFileClient = ncp_api_client().cams_file
    process_date = datetime.now()

    # Submit all pollutant tasks concurrently
    task_futures = []
    for pollutant in pollutants:
        for model_name in model_names:
            print(f"Processing CAMS model {model_name} for pollutant {pollutant}")
            future = process_cams_model.submit(
                model_name=model_name,
                pollutant=pollutant,
                cams_file_client=cams_file_client,
                process_date=process_date)

            task_futures.append((pollutant, model_name, future))

    # Wait for all tasks and handle results/errors
    successful_runs = []
    failed_runs = []

    for pollutant, model_name, future in task_futures:
        try:
            result = future.result()
            successful_runs.append((pollutant, model_name))
        except Exception as e:
            failed_runs.append((pollutant, model_name, str(e)))
            print(f"✗ Task CAMS MOS for pollutant {pollutant} failed: {str(e)}")

    # Final summary
    print(f"✓ Successfully ran {len(successful_runs)} tasks.")
    if failed_runs:
        print(f"✗ Failed to run {len(failed_runs)} tasks:")
        for pollutant, model_name, error in failed_runs:
            print(f"  Error for pollutant {pollutant} model: {model_name}: {error}")

    print(f"Processing CAMS models: {model_names}")


@task(retries=3, retry_delay_seconds=30)
def process_cams_model(model_name: str, pollutant:str, cams_file_client: CamsFileClient, process_date: datetime) -> Tuple[str, str]:
    """
    Process a single CAMS model for a given pollutant and date.

    Args:
        model_name (str): The name of the CAMS model to process.
        pollutant (str): The pollutant to process.
        cams_file_client (CamsFileClient): The client to interact with CAMS files.
        process_date (datetime): The date for which to process the CAMS data.

    Returns:
        Tuple[str, str]: A tuple containing the pollutant and model name.

    Raises:
        Exception: If processing fails after retries
    """
    try:
        with tempfile.TemporaryDirectory() as tmpdirname:
            cams_nc_file = cams_file_client.download_cams_file(
                year=process_date.year,
                month=process_date.month,
                day=process_date.day,
                pollutant=pollutant,
                cams_model=model_name,
                forecast_days=4,  # Assuming no forecast days for historical data
                output_path=tmpdirname)
            print("cams_nc_file: ", cams_nc_file)
            print("TODO: Process CAMS file ...")
        return pollutant, model_name

    except Exception as e:
        print(f"✗ Error processing cams data. model: {model_name}  error: {str(e)}")
        raise


if __name__ == "__main__":
    process_cams_europe()