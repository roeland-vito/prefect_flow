from datetime import datetime, UTC, timedelta
from statistics import quantiles
from typing import Dict, List

import pandas as pd
import requests
from cams_ncp_client.observation import ObservationClient
from cams_ncp_client.quantity import QuantityClient
from cams_ncp_client.schemas.common import ObservationHourly
from numpy.testing.print_coercion_tables import print_coercion_table
from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner
from prefect.variables import Variable
from vito.sas.air.cams_client import Pollutant
from vito.sas.air.sos_client import SOSClient, Station, Observation


from _utils import print_env, assert_recent_flow_run, ncp_api_client

@task
def download_obs_for_station(sos_client: SOSClient, obs_client: ObservationClient,  start_time: datetime, end_time: datetime, station_name: str, quantity_names: List[str] ) -> Dict[str, List[ObservationHourly]]:
    """
    Download observations for a given station and quantity names.
    Returns a dictionary with pollutant names as keys and lists of ObservationHourly as values.
    """
    return_dict: Dict[str, List[ObservationHourly]] = {}

    for pollutant in quantity_names:
        observations: List[Observation] = sos_client.get_observations(station_name=station_name, pollutant=pollutant.upper(),  start_time=start_time, end_time=end_time)
        observations_hourly: List[ObservationHourly] = _convert_observations_to_hourly(observations)
        created_obs = obs_client.create_observations(observations_hourly)
        # print(f"created {len(created_obs)} observations for station {station_name} and pollutant {pollutant}")
    return return_dict

@flow(log_prints=True)
def download_observations() -> None:
    if Variable.get("debug_python_worker_env", False):
        print_env()

    assert_recent_flow_run("update-station-data")

    session = requests.Session()
    session.verify = Variable.get("ssl_verify", True)
    sos_client = SOSClient(session=session)

    obs_client: ObservationClient  = ncp_api_client().observation
    quantity_client: QuantityClient = ncp_api_client().quantity

    # pollutants:  Dict[str, Pollutant] = sos_client.get_pollutants_cached()
    quantities_df: pd.DataFrame = quantity_client.get_quantities_df()
    quantity_names: List[str] = quantities_df["name"].tolist()
    print("quantity_names: ", quantity_names)
    stations:  Dict[str, Station] = sos_client.get_stations_cached()

    datetime_end = datetime.now(tz=UTC)
    datetime_start = datetime_end - timedelta(hours=8)

    station_names = list(stations.keys())
    results = download_obs_for_station.map(
        sos_client=[sos_client] * len(station_names),
        obs_client=[obs_client] * len(station_names),
        start_time=[datetime_start] * len(station_names),
        end_time=[datetime_end] * len(station_names),
        station_name=station_names,
        quantity_names=[quantity_names] * len(station_names)
    )

    # for station_name, station in stations.items():
    #     download_obs_for_station(
    #         sos_client=sos_client,
    #         obs_client=obs_client,
    #         start_time=datetime_start,
    #         end_time=datetime_end,
    #         station_name=station_name,
    #         quantity_names=quantity_names
    #     )
    print(f"Completed processing {len(results)} stations in parallel")
    print("Flow download_observations done :)")


def _convert_observations_to_hourly(observations: List[Observation]) -> List[ObservationHourly]:
    """
    Convert a list of Observation to ObservationHourly.
    """
    observations_hourly = []
    for obs in observations:
        hourly_obs = ObservationHourly(
            result_time=obs.result_time,
            station_name=obs.station_eoi_code,
            quantity_name=obs.pollutant_name,
            value=obs.value,
            meta_data=obs.meta_data
        )
        observations_hourly.append(hourly_obs)
    return observations_hourly


if __name__ == "__main__":
    download_observations()