import tempfile
from datetime import datetime, UTC
from typing import List, Optional, Tuple

import pandas as pd
from cams_ncp_client.cams_file import CamsFileClient
from cams_ncp_client.forecast import ForecastClient
from cams_ncp_client.model import ModelClient
from cams_ncp_client.schemas.common import ForecastHourly, ForecastModel
from cams_ncp_client.utils.cams_utils import get_station_concentrations
from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner
from prefect.variables import Variable
from vito.sas.air.utils.cams_utils import Pollutant

from _utils import ncp_api_client


@flow(log_prints=True,
      task_runner=ConcurrentTaskRunner(max_workers=5),
      retries=6, retry_delay_seconds=900)  # Will retry up to 6 times, every 15 minutes)
def process_stations_cams_europe(model_names: Optional[List[str]] = None) -> None:
    """Download CAMS Europe models"""
    if model_names is None:
        model_names = Variable.get("cams_models", default=[])

    if not model_names:
        print("No CAMS models specified. Exiting.")
        return

    pollutants: List[str] = Pollutant.all()
    df_stations = ncp_api_client().station.find_stations_df()
    process_date = datetime.now()

    # Submit all pollutant tasks concurrently
    task_futures = []
    for pollutant in pollutants:
        for model_name in model_names:
            print(f"Processing CAMS model {model_name} for pollutant {pollutant}")
            # process_cams_model(df_stations: pd.DataFrame, model_name: str, pollutant:str, process_date: datetime)
            future = process_cams_model.submit(
                df_stations=df_stations,
                model_name=model_name,
                pollutant=pollutant,
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
def process_cams_model(df_stations: pd.DataFrame, model_name: str, pollutant:str, process_date: datetime) -> Tuple[str, str]:
    """
    Process a single CAMS model for a given pollutant and date.

    Args:
        df_stations (pd.DataFrame): DataFrame containing station information. (From station_client.find_stations_df())
        model_name (str): The name of the CAMS model to process.
        pollutant (str): The pollutant to process.
        process_date (datetime): The date for which to process the CAMS data.

    Returns:
        Tuple[str, str]: A tuple containing the pollutant and model name.

    Raises:
        Exception: If processing fails after retries
    """
    try:
        cams_file_client: CamsFileClient =  ncp_api_client().cams_file
        forecast_client: ForecastClient = ncp_api_client().forecast
        model_client: ModelClient = ncp_api_client().model

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
            print("Get CAMS forecasts for each station ...")
            cams_var = _get_cams_var(pollutant)
            model_name_db = f"CAMS_{model_name}"

            forcast_model: ForecastModel = _asssert_cams_model_exists(model_client, model_name_db, model_name)
            station_concentrations_df = get_station_concentrations(df_stations=df_stations, cams_nc_file=cams_nc_file, base_date=process_date, cams_var=cams_var)

            print("station_concentrations_df: ", station_concentrations_df)
            # upload the result_df to the NCP API
            for station_name in df_stations["name"].values:
                station = station_name.strip()
                assert  station in station_concentrations_df.columns , f"Station {station} not found in station_concentrations_df columns."
                station_data = station_concentrations_df[station].copy()
                # set index as column 'timestamp'
                station_data = station_data.reset_index()
                station_data.rename(columns={'index': 'timestamp'}, inplace=True)

                print(f"Creating forecast objects for station: {station}")
                # def create_forecast_objects(df_forecasts: pd.DataFrame, pollutant: str, model_name: str, base_time: datetime) -> List[ForecastHourly]:
                base_time = datetime.combine(process_date, datetime.min.time(), tzinfo=UTC)
                list_forecasts: List[ForecastHourly] = _create_forecast_objects(station_data, pollutant, forcast_model.name, base_time)
                print(f"Uploading data for station: {station}")
                created_forecasts = forecast_client.create_forecasts(list_forecasts)
                print(f"Successfully created {len(created_forecasts)} forecasts for station {station} with pollutant {pollutant} and model {model_name}")
            print(f"✓ Successfully processed CAMS model {model_name} for pollutant {pollutant}")
        return pollutant, model_name

    except Exception as e:
        print(f"✗ Error processing cams data. model: {model_name}  error: {str(e)}")
        raise

def _get_cams_var(pollutant: str) -> str:
    """
    Get the CAMS variable name for a given pollutant.
    """
    cams_var_mapping = {
        "pm10": "pm10_conc",
        "pm25": "pm2p5_conc",
        "no2": "no2_conc",
        "o3": "o3_conc",
        # Add more mappings as needed
    }
    return cams_var_mapping.get(pollutant.lower(), pollutant.lower() + "_conc")


def _asssert_cams_model_exists(model_client: ModelClient, full_model_name: str, cams_model_name) -> ForecastModel:
    assert full_model_name.startswith("CAMS_")
    assert not cams_model_name.startswith("CAMS_")
    models = model_client.find_models(name=full_model_name)
    if len(models) == 0:
        model = model_client.create_model(ForecastModel(
            name=full_model_name,
            description="CAMS model " + cams_model_name,
            model_type="CAMS",
            model_subtype=cams_model_name,
            color="#FF0000"
         ))
        return model
    else:
        return models[0]

def _create_forecast_objects(df_forecasts: pd.DataFrame, pollutant: str, model_name: str, base_time: datetime) -> List[ForecastHourly]:
    """
    Convert a DataFrame of forecast data into a list of ForecastHourly objects.

    Args:
        df_forecasts: DataFrame with forecast data

    Returns:
        List of ForecastHourly objects
    """
    assert base_time.tzinfo is not None
    forecast_list = []

    columns: list[str] = list(df_forecasts.columns)
    assert len(columns) == 2  # first column is 'datetime', second is the station forecast value'
    station_name = columns[1]  # Assuming the second column is the station name
    # Calculate base_time by subtracting lead_time_hour from datetime
    for idx, row in df_forecasts.iterrows():
        # Create forecast object from row data
        forecast_time = row[0]
        value = row[1]
        if pd.isna(value):
            value = None

        assert forecast_time.tzinfo is not None, "forecast_time must have timezone information"

        forecast = ForecastHourly(
            base_time=base_time,
            forecast_time=forecast_time,
            station_name=station_name,
            quantity_name=pollutant,
            model_name=model_name,
            value=value
        )
        forecast_list.append(forecast)

    return forecast_list


if __name__ == "__main__":
    process_cams_europe()