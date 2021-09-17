import typing
from ..cpd3 import DataStream, DataReader, Name, data_profile_get, detach, profile_data


station_profile_data = detach(profile_data)


station_profile_data['aerosol']['raw']['pressure'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'P_XM1'): 'ambient',
        Name(station, 'raw', 'P_S11'): 'neph',
        Name(station, 'raw', 'P_S11', {'pm10'}): 'neph',
        Name(station, 'raw', 'P_S11', {'pm25'}): 'neph',
        Name(station, 'raw', 'P_S11', {'pm1'}): 'neph',
    }, send
)

station_profile_data['aerosol']['raw']['cpcstatus'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'Tu_N71'): 'Tinlet',
        Name(station, 'raw', 'TDu_N71'): 'TDinlet',
        Name(station, 'raw', 'Uu_N71'): 'Uinlet',
        Name(station, 'raw', 'T1_N71'): 'Tconditioner',
        Name(station, 'raw', 'T2_N71'): 'Tinitiator',
        Name(station, 'raw', 'T3_N71'): 'Tmoderator',
        Name(station, 'raw', 'T4_N71'): 'Toptics',
        Name(station, 'raw', 'T5_N71'): 'Theatsink',
        Name(station, 'raw', 'T6_N71'): 'Tpcb',
        Name(station, 'raw', 'T7_N71'): 'Tcabinet',
        Name(station, 'raw', 'Q_N71'): 'Qsample',
        Name(station, 'raw', 'P_N71'): 'Psample',
        Name(station, 'raw', 'Pd_N71'): 'PDorifice',
    }, send
)


def get(station: str, data_name: str, start_epoch_ms: int, end_epoch_ms: int,
        send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
    return data_profile_get(station, data_name, start_epoch_ms, end_epoch_ms, send, station_profile_data)
