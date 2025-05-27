import json
from typing import List, Dict, Set

from cams_ncp_client.utils.string_utils import map_to_literal
from cams_ncp_client.client import CamsNcpApiClient
from cams_ncp_client.client import CamsNcpApiClient
from cams_ncp_client.schemas.common import MeasuringStation, StationType, AreaType, Quantity, TableData
from cams_ncp_client.station import StationClient
from prefect import flow
from prefect.variables import Variable
from vito.sas.air.cams_client import CAMSEuropeClient, Pollutant
from vito.sas.air.ircel_wfs_client import SosStation, IrcelWfsClient
from vito.sas.air.sos_client import Station, SOSClient

from _utils import print_env, ncp_api_client


@flow(log_prints=True)
def update_station_data() -> None:
    if Variable.get("debug_python_worker_env", False):
        print_env()

    quantity_names = _update_quantities()
    print(f"Updated quantities: {quantity_names}")
    station_client = ncp_api_client().station
    sos_client = SOSClient()
    ircel_wfs_client = IrcelWfsClient()

    # update the station data with data from the WFS client
    for sos_station in ircel_wfs_client.get_sos_stations():
        # print("sos_station: ", sos_station)
        _update_station(station_client, sos_station, quantity_names)

    # update the station data with data from the SOS client
    for station in sos_client.get_stations():
        _update_station(station_client, station, quantity_names)

    print("Flow update_station_data done :)")


def _update_station(station_client: StationClient, station: SosStation | Station, allowed_quantities: Set[str]):
    local_code = station.local_code
    stations: TableData[MeasuringStation] = station_client.find_stations(limit=1, name=[local_code])
    if len(stations) == 0:
        measuring_station = station_client.create_station(MeasuringStation(name=local_code, description=local_code, lat=0.0, lon=0.0, altitude=0.0))
    else:
        measuring_station = stations[0]
    if isinstance(station, SosStation):
        _update_measuring_station_from_sos_station(station, measuring_station)
    elif isinstance(station, Station):
        _update_measuring_station_from_station(station, measuring_station, allowed_quantities)
    else:
        raise ValueError(f"Unknown station type: {type(station)}")
    station_client.update_station(measuring_station)


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


def _update_measuring_station_from_station(station: Station, measuring_station: MeasuringStation, allowed_quantities: Set[str]):
    measuring_station.name = station.local_code
    measuring_station.description = station.description
    measuring_station.lat = station.latitude
    measuring_station.lon = station.longitude
    measuring_station.quantities = [pollutant.name.lower() for pollutant in station.pollutants if pollutant.name.lower() in allowed_quantities]


def _update_quantities() -> Set[str]:
    quantity_client = ncp_api_client().quantity
    quantities = _read_quantities()
    quantity_names= set()
    for quantity in quantities:
        created_quantity = quantity_client.upsert_quantity(quantity)
        print("created_quantity: ", created_quantity)
        quantity_names.add(created_quantity.name)
    return quantity_names


def _read_quantities() -> List[Quantity]:
    quantity_data: List[Dict] = Variable.get("quantities", [])
    return_list: List[Quantity] = []
    for quantity in quantity_data:
        return_list.append( Quantity.model_validate(quantity))
    return return_list

if __name__ == "__main__":
    update_station_data()