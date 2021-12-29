import typing
from ..cpd3 import DataStream, DataReader, RealtimeTranslator, Name, data_profile_get, detach, profile_data


station_profile_data = detach(profile_data)


station_profile_data['aerosol']['raw']['cpcstatus'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'Q_Q71'): 'Qcpc',
        Name(station, 'raw', 'Q_Q72'): 'Qdrier',
        Name(station, 'raw', 'Tu_N71'): 'Tinlet',
        Name(station, 'raw', 'T1_N71'): 'Tsaturatorbottom',
        Name(station, 'raw', 'T2_N71'): 'Tsaturatortop',
        Name(station, 'raw', 'T3_N71'): 'Tcondenser',
        Name(station, 'raw', 'T4_N71'): 'Toptics',
        Name(station, 'raw', 'Q1_N71'): 'Qsample',
        Name(station, 'raw', 'Q2_N71'): 'Qsaturator',
        Name(station, 'raw', 'P_N71'): 'Psample',
    }, send
)
station_profile_data['aerosol']['realtime']['cpcstatus'] = {
    RealtimeTranslator.Key('Q_Q71'): 'Qcpc',
    RealtimeTranslator.Key('Q_Q72'): 'Qdrier',
    RealtimeTranslator.Key('Tu_N71'): 'Tinlet',
    RealtimeTranslator.Key('T1_N71'): 'Tsaturatorbottom',
    RealtimeTranslator.Key('T2_N71'): 'Tsaturatortop',
    RealtimeTranslator.Key('T3_N71'): 'Tcondenser',
    RealtimeTranslator.Key('T4_N71'): 'Toptics',
    RealtimeTranslator.Key('Q1_N71'): 'Qsample',
    RealtimeTranslator.Key('Q2_N71'): 'Qsaturator',
    RealtimeTranslator.Key('P_N71'): 'Psample',
}


def get(station: str, data_name: str, start_epoch_ms: int, end_epoch_ms: int,
        send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
    return data_profile_get(station, data_name, start_epoch_ms, end_epoch_ms, send, station_profile_data)
