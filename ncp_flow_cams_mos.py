import tempfile
import zipfile
from datetime import date, datetime
from pathlib import Path
from typing import List, Dict
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from cams_ncp_client.model import ModelClient
from cams_ncp_client.schemas.common import ForecastHourly, ForecastModel, MeasuringStation
from cams_ncp_client.station import StationClient
from cams_ncp_client.utils.pandas_utils import read_csv
from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner
from prefect.variables import Variable
from vito.sas.air.cams_client import Pollutant, CAMSMosClient

from _utils import assert_recent_flow_run, get_secret, ncp_api_client


@task(retries=3, retry_delay_seconds=30)
def download_cams_mos_for_pollutant(pollutant: str):
    """Download CAMS MOS data for a single pollutant"""
    try:
        print(f"Starting download for pollutant: {pollutant}")
        # Create a temporary directory for downloading files
        with tempfile.TemporaryDirectory() as tmpdir:
            temp_folder = Path(tmpdir)
            # Download and process only the specified pollutant
            zip_file: Path = _download_cams_mos_zip_file(temp_folder, pollutant, date.today())
            # Extract the zip file in a subdirectory named after the zip file
            subfolder = temp_folder / zip_file.stem
            subfolder.mkdir(parents=True, exist_ok=True)
            with zipfile.ZipFile(zip_file, 'r') as zf:
                zf.extractall(subfolder)
            _upload_cams_mos_data(subfolder)

        print(f"✓ Successfully processed pollutant: {pollutant}")
        return pollutant
    except Exception as e:
        print(f"✗ Error processing pollutant {pollutant}: {str(e)}")
        raise

@flow(log_prints=True, task_runner=ConcurrentTaskRunner(max_workers=5))
def download_cams_mos() -> None:
    assert_recent_flow_run("update-station-data")

    pollutants = list(Pollutant.all())
    # Submit all pollutant tasks concurrently
    task_futures = []
    for pollutant in pollutants:
        future = download_cams_mos_for_pollutant.submit(pollutant)
        task_futures.append((pollutant, future))

    # Wait for all tasks and handle results/errors
    successful_pollutants = []
    failed_pollutants = []

    for pollutant, future in task_futures:
        try:
            result = future.result()
            successful_pollutants.append(pollutant)
        except Exception as e:
            failed_pollutants.append((pollutant, str(e)))
            print(f"✗ Task CAMS MOS for pollutant {pollutant} failed: {str(e)}")

    # Final summary
    print(f"✓ Successfully ran {len(successful_pollutants)} tasks.")
    if failed_pollutants:
        print(f"✗ Failed to run {len(failed_pollutants)} tasks:")
        for pollutant, error in failed_pollutants:
            print(f"  Error for pollutant {pollutant}: {error}")

    print("CAMS MOS done :)")


def _assert_mos_model_exists(model_client: ModelClient) -> ForecastModel:
    """
    Ensure that the MOS model exists in the database, creating it if necessary.
    """
    models = model_client.find_models(name="MOS")
    if len(models) == 0:
        print("Creating MOS model in the database...")
        model = model_client.create_model(ForecastModel(
            name="MOS",
            description="CAMS MOS model",
            model_type="MOS",
            color="#FF0000"
         ))
        return model
    else:
        return models[0]

def _upload_cams_mos_data(dir_data: Path) -> List[ForecastHourly]:
    """
    Upload CAMS MOS data from CSV files in the specified directory.
    """
    print(f"_upload_cams_mos_data from csv files in {dir_data.absolute()}")
    station_client = ncp_api_client().station
    forecast_client =  ncp_api_client().forecast
    model_client =  ncp_api_client().model

    mos_model = _assert_mos_model_exists(model_client)

    df_stations = None
    for csv_file_stations in dir_data.glob("station_list_*.csv"):
        df_stations = read_csv(csv_file_stations, parse_dates=['date_start', 'date_end'])
        # keep the stations with the biggest position_number
        df_stations = df_stations.sort_values(by=['id', 'position_number'], ascending=[True, False])
        df_stations = df_stations.drop_duplicates(subset=['id'], keep='first')

    created_forecasts: List[ForecastHourly] = []
    # iterate over the mos_*.csv files in the directory
    for csv_file in dir_data.glob("mos_*.csv"):
        print(f"Processing file: {csv_file.name}")
        filename = str(csv_file.stem)
        filename_split = filename.split("_")
        # parse reference date from the filename (mos_D2_PM25_hourly_2025-04-01_BE)
        refdate_str = filename_split[4]
        pollutant = filename_split[2]
        refdate: date = datetime.strptime(refdate_str, "%Y-%m-%d").date()
        refdatetime = datetime.combine(refdate, datetime.min.time()).replace(tzinfo=ZoneInfo("UTC"))
        df_forecasts = _read_mos_forecasts_from_csv(csv_file)
        station_eoi_codes = df_forecasts['station_id'].unique()
        if df_stations is not None:
            _assert_station_names_exist(pollutant, station_eoi_codes, df_stations, station_client)

        stations_names = _find_stations(station_eoi_codes, station_client)
        # def create_forecast_objects(df_forecasts: pd.DataFrame, pollutant: str, base_time: datetime, model_name: str) -> List[ForecastHourly]:
        list_forecasts: List[ForecastHourly] = _create_forecast_objects(stations_names, df_forecasts, pollutant, refdatetime, mos_model.name)
        created_forecasts += forecast_client.create_forecasts(list_forecasts)
    return created_forecasts


def _create_forecast_objects(stations_names: Dict[str, str], df_forecasts: pd.DataFrame, pollutant: str, base_time: datetime, model_name: str, timezone=ZoneInfo("UTC")) -> List[ForecastHourly]:
    """
    Convert a DataFrame of forecast data into a list of ForecastHourly objects.
    """
    assert base_time.tzinfo is not None
    forecast_list = []

    # Ensure datetime column is in datetime format
    if not pd.api.types.is_datetime64_any_dtype(df_forecasts['datetime']):
        df_forecasts['datetime'] = pd.to_datetime(df_forecasts['datetime'])

    # Calculate base_time by subtracting lead_time_hour from datetime
    for _, row in df_forecasts.iterrows():
        # Create forecast object from row data
        value = row['conc_mos_micrograms_per_m3']
        if pd.isna(value):
            value = None
        station_eoi_code = row['station_id']
        station_name = stations_names[station_eoi_code]
        forecast_time = row['datetime']

        if forecast_time.tzinfo is None:
            forecast_time = forecast_time.replace(tzinfo=timezone)
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

def _find_stations(station_eoi_codes: List[str], station_client: StationClient) -> Dict[str, str]:
    stations = station_client.find_stations(eoi_code=station_eoi_codes)
    return {station.eoi_code: station.name  for station in stations}


def _assert_station_names_exist(pollutant: str, station_eoi_codes: List[str], df_stations: pd.DataFrame, station_client: StationClient):
    """
    Ensure that all station names exist in the database, creating them if necessary.
    """
    # check if all station names exist in the database
    for station_eoi_code in station_eoi_codes:
        stations = station_client.find_stations(limit=1, eoi_code=station_eoi_code)
        if len(stations) == 0:
            # lookup station name in the dataframe
            station_row = df_stations[df_stations['id'] == station_eoi_code]
            if len(station_row) == 0:
                raise ValueError(f"Station {station_eoi_code} not found in the database and not in the MOS station list")
            else:
                print(f"Creating station {station_eoi_code} for pollutant {pollutant} in the database")
                measuring_station = MeasuringStation(
                    name=station_eoi_code,
                    description=station_eoi_code,
                    lat=station_row['lat'].values[0],
                    lon=station_row['lon'].values[0],
                    altitude=0.0,
                    quantities=[pollutant]
                )
                station_client.create_station(measuring_station)


def _read_mos_forecasts_from_csv(csv_file: Path) -> pd.DataFrame:
    return read_csv(csv_file, parse_dates=['datetime'])


def _download_cams_mos_zip_file(temp_folder: Path, pollutant: str, base_date: date) -> Path:
    """
    Download the CAMS MOS data zip file for a specific pollutant and date.
    """
    session = requests.Session()
    verify_ssl = Variable.get("ssl_verify", True)
    session.verify = verify_ssl

    cams_mos = CAMSMosClient(pollutant=pollutant, date_start=base_date, verify_ssl=verify_ssl)

    api_key = get_secret("cams-api-key")
    api_url = Variable.get("cams_api_url", "https://ads.atmosphere.copernicus.eu/api")

    print(f"Downloading CAMS MOS data for pollutant {pollutant} from {api_url} to {temp_folder.absolute()}")
    zip_file = cams_mos.download(temp_folder, api_url=api_url, api_key=api_key, session=session)

    return zip_file


if __name__ == "__main__":
    download_cams_mos()