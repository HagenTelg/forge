import typing
from ..cpd3 import DataStream, DataReader, EditedReader, Name, data_profile_get, detach, profile_data


station_profile_data = detach(profile_data)


station_profile_data['aerosol']['raw']['cpcstatus'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'T1_N61'): 'Tsaturator',
        Name(station, 'raw', 'T2_N61'): 'Tcondenser',
        Name(station, 'raw', 'T3_N61'): 'Toptics',
        Name(station, 'raw', 'T4_N61'): 'Tcabinet',
        Name(station, 'raw', 'Q_N61'): 'Qsample',
        Name(station, 'raw', 'Qu_N61'): 'Qinlet',
        Name(station, 'raw', 'P_N61'): 'Psample',
        Name(station, 'raw', 'Pd1_N61'): 'PDnozzle',
        Name(station, 'raw', 'Pd2_N61'): 'ODorifice',
        Name(station, 'raw', 'A_N61'): 'Alaser',
    }, send
)


def get(station: str, data_name: str, start_epoch_ms: int, end_epoch_ms: int,
        send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
    return data_profile_get(station, data_name, start_epoch_ms, end_epoch_ms, send, station_profile_data)
