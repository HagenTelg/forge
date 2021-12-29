import typing
from ..cpd3 import DataStream, DataReader, RealtimeTranslator, Name, data_profile_get, detach, profile_data


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
station_profile_data['aerosol']['realtime']['cpcstatus'] = {
    RealtimeTranslator.Key('T1_N61'): 'Tsaturator',
    RealtimeTranslator.Key('T2_N61'): 'Tcondenser',
    RealtimeTranslator.Key('T3_N61'): 'Toptics',
    RealtimeTranslator.Key('T4_N61'): 'Tcabinet',
    RealtimeTranslator.Key('Q_N61'): 'Qsample',
    RealtimeTranslator.Key('Qu_N61'): 'Qinlet',
    RealtimeTranslator.Key('P_N61'): 'Psample',
    RealtimeTranslator.Key('Pd1_N61'): 'PDnozzle',
    RealtimeTranslator.Key('Pd2_N61'): 'ODorifice',
    RealtimeTranslator.Key('A_N61'): 'Alaser',
}


def get(station: str, data_name: str, start_epoch_ms: int, end_epoch_ms: int,
        send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
    return data_profile_get(station, data_name, start_epoch_ms, end_epoch_ms, send, station_profile_data)
