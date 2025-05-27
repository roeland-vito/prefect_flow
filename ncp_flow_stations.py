from typing import List, Dict, Set

import requests
from cams_ncp_client.schemas.common import MeasuringStation, StationType, AreaType, Quantity, TableData
from cams_ncp_client.station import StationClient
from cams_ncp_client.utils.string_utils import map_to_literal
from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner
from prefect.variables import Variable
from vito.sas.air.ircel_wfs_client import SosStation, IrcelWfsClient
from vito.sas.air.sos_client import Station, SOSClient

from _utils import print_env, ncp_api_client


@task(retries=3, retry_delay_seconds=30)
def update_station(station_client: StationClient, station: SosStation | Station, allowed_quantities: Set[str]):
    try:
        local_code = station.local_code
        stations: TableData[MeasuringStation] = station_client.find_stations(limit=1, name=[local_code])
        if len(stations) == 0:
            measuring_station = station_client.create_station(
                MeasuringStation(name=local_code, description=local_code, lat=0.0, lon=0.0, altitude=0.0))
        else:
            measuring_station = stations[0]

        if isinstance(station, SosStation):
            _update_measuring_station_from_sos_station(station, measuring_station)
        elif isinstance(station, Station):
            _update_measuring_station_from_station(station, measuring_station, allowed_quantities)
        else:
            raise ValueError(f"Unknown station type: {type(station)}")

        station_client.update_station(measuring_station)
        return measuring_station.name

    except Exception as e:
        print(f"✗ Error updating station {station.local_code}: {str(e)}")
        raise


def _update_station_from_ircel_wfs_client(session: requests.Session, station_client: StationClient, quantity_names: Set[str]) -> (List[str], List[tuple]):
    ircel_wfs_client = IrcelWfsClient(session=session)
    # Update station data with data from the WFS client
    print("Starting first loop: Processing SOS stations from WFS client...")
    sos_stations = list(ircel_wfs_client.get_sos_stations())
    print(f"Found {len(sos_stations)} SOS stations to process")

    if sos_stations:
        # Submit all WFS station tasks concurrently
        wfs_task_futures = []
        for sos_station in sos_stations:
            future = update_station.submit(station_client, sos_station, quantity_names)
            wfs_task_futures.append((sos_station.local_code, future))

        # Wait for all WFS tasks and handle results/errors
        wfs_successful = []
        wfs_failed = []

        for station_code, future in wfs_task_futures:
            try:
                result = future.result()
                wfs_successful.append(station_code)
            except Exception as e:
                wfs_failed.append((station_code, str(e)))
                print(f"✗ WFS Station {station_code} failed: {str(e)}")

        print(f"✓ WFS Loop completed: {len(wfs_successful)} successful, {len(wfs_failed)} failed")
        return wfs_successful, wfs_failed
    else:
        print("No SOS stations found from WFS client")
        return [], []

def _update_station_from_sos_client(session: requests.Session, station_client: StationClient, quantity_names: Set[str]) -> (List[str], List[tuple]):
    sos_client = SOSClient(session=session)
    # Update station data with data from the SOS client
    print("Starting second loop: Processing stations from SOS client...")
    sos_stations_list = list(sos_client.get_stations())
    print(f"Found {len(sos_stations_list)} SOS stations to process")

    if sos_stations_list:
        # Submit all SOS station tasks concurrently
        sos_task_futures = []
        for station in sos_stations_list:
            future = update_station.submit(station_client, station, quantity_names)
            sos_task_futures.append((station.local_code, future))

        # Wait for all SOS tasks and handle results/errors
        sos_successful = []
        sos_failed = []

        for station_code, future in sos_task_futures:
            try:
                result = future.result()
                sos_successful.append(station_code)
            except Exception as e:
                sos_failed.append((station_code, str(e)))
                print(f"✗ SOS Station {station_code} failed: {str(e)}")

        print(f"✓ SOS Loop completed: {len(sos_successful)} successful, {len(sos_failed)} failed")
        return sos_successful, sos_failed
    else:
        print("No stations found from SOS client")
        return [], []

@flow(log_prints=True, task_runner=ConcurrentTaskRunner(max_workers=15))
def update_station_data() -> None:
    # if Variable.get("debug_python_worker_env", False):
    #     print_env()

    quantity_names = _update_quantities()
    station_client = ncp_api_client().station

    session = requests.Session()
    session.verify = Variable.get("ssl_verify", True)

    wfs_successful, wfs_failed = _update_station_from_ircel_wfs_client(session, station_client, quantity_names)
    sos_successful, sos_failed = _update_station_from_sos_client(session, station_client, quantity_names)

    print(f"WFS update: {len(wfs_successful)} successful, {len(wfs_failed)} failed")
    if wfs_failed:
        for station_code, error in wfs_failed:
            print(f"  ✗ WFS {station_code}: {error}")

    print(f"SOS update: {len(sos_successful)} successful, {len(sos_failed)} failed")
    if sos_failed:
        for station_code, error in sos_failed:
            print(f"  ✗ SOS {station_code}: {error}")

    print("Flow update_station_data done :)")


def _update_measuring_station_from_sos_station(station: SosStation, measuring_station: MeasuringStation):
    measuring_station.name = station.local_code
    measuring_station.eoi_code = station.eoi_code
    measuring_station.description = station.description
    measuring_station.lat = station.latitude
    measuring_station.lon = station.longitude
    measuring_station.station_type = map_to_literal(station.station_type, StationType)
    measuring_station.area_type = map_to_literal(station.area_type, AreaType)
    if measuring_station.meta_data is None:
        measuring_station.meta_data = {}
    measuring_station.meta_data['zone_code'] = station.zone_code


def _update_measuring_station_from_station(station: Station, measuring_station: MeasuringStation,
                                           allowed_quantities: Set[str]):
    measuring_station.name = station.local_code
    measuring_station.description = station.description
    measuring_station.lat = station.latitude
    measuring_station.lon = station.longitude
    measuring_station.quantities = [pollutant.name.lower() for pollutant in station.pollutants if
                                    pollutant.name.lower() in allowed_quantities]


def _update_quantities() -> Set[str]:
    quantity_client = ncp_api_client().quantity
    quantities = _read_quantities()
    quantity_names = set()
    for quantity in quantities:
        created_quantity = quantity_client.upsert_quantity(quantity)
        print("created_quantity: ", created_quantity)
        quantity_names.add(created_quantity.name)
    return quantity_names


def _read_quantities() -> List[Quantity]:
    quantity_data: List[Dict] = Variable.get("quantities", [])
    return_list: List[Quantity] = []
    for quantity in quantity_data:
        return_list.append(Quantity.model_validate(quantity))
    return return_list


if __name__ == "__main__":
    update_station_data()