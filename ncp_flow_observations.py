from datetime import datetime, UTC, timedelta
from typing import Dict, List

import requests
from cams_ncp_client.observation import ObservationClient
from cams_ncp_client.schemas.common import ObservationHourly
from prefect import flow
from prefect.variables import Variable
from vito.sas.air.cams_client import Pollutant
from vito.sas.air.sos_client import SOSClient, Station, Observation

from _utils import print_env, assert_recent_flow_run, ncp_api_client


@flow(log_prints=True)
def download_observations() -> None:
    if Variable.get("debug_python_worker_env", False):
        print_env()

    assert_recent_flow_run("update-station-data")

    session = requests.Session()
    session.verify = Variable.get("ssl_verify", True)
    sos_client = SOSClient(session=session)

    obs_client: ObservationClient  = ncp_api_client().observation

    pollutants:  Dict[str, Pollutant] = sos_client.get_pollutants_cached()
    stations:  Dict[str, Station] = sos_client.get_stations_cached()

    datetime_end = datetime.now(tz=UTC)
    datetime_start = datetime_end - timedelta(hours=8)

    for station_name, station in stations.items():
        for pollutant in pollutants.values():
            observations: List[Observation] = sos_client.get_observations(station_name=station_name, pollutant=pollutant.name, start_time=datetime_start, end_time=datetime_end)
            # convert to List[ObservationHourly]
            observations_hourly: List[ObservationHourly] = _convert_observations_to_hourly(observations)
            created_obs = obs_client.create_observations(observations_hourly)
            print(f"created {len(created_obs)} observations for station {station_name} and pollutant {pollutant.name}")

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