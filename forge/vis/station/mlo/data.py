import typing
from ..cpd3 import DataStream, DataReader, EditedReader, RealtimeTranslator, Name, data_profile_get, detach, profile_data


station_profile_data = detach(profile_data)


station_profile_data['aerosol']['raw']['cnc'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'N_N71'): 'cnc',
        Name(station, 'raw', 'N_N61'): 'cnc',
        Name(station, 'raw', 'N_N72'): 'cnc2',
        Name(station, 'raw', 'N_N62'): 'cnc2',
    }, send
)
station_profile_data['aerosol']['realtime']['cnc'] = {
    RealtimeTranslator.Key('N_N71'): 'cnc',
    RealtimeTranslator.Key('N_N61'): 'cnc',
    RealtimeTranslator.Key('N_N72'): 'cnc2',
    RealtimeTranslator.Key('N_N62'): 'cnc2',
}
station_profile_data['aerosol']['editing']['cnc'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
    start_epoch_ms, end_epoch_ms, station, 'aerosol', {
        Name(station, 'clean', 'N_N71'): 'cnc',
        Name(station, 'clean', 'N_N61'): 'cnc',
        Name(station, 'clean', 'N_N72'): 'cnc2',
        Name(station, 'clean', 'N_N62'): 'cnc2',
    }, send
)
station_profile_data['aerosol']['clean']['cnc'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'clean', 'N_N71'): 'cnc',
        Name(station, 'clean', 'N_N61'): 'cnc',
        Name(station, 'clean', 'N_N72'): 'cnc2',
        Name(station, 'clean', 'N_N62'): 'cnc2',
    }, send
)
station_profile_data['aerosol']['avgh']['cnc'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'avgh', 'N_N71'): 'cnc',
        Name(station, 'avgh', 'N_N61'): 'cnc',
        Name(station, 'avgh', 'N_N72'): 'cnc2',
        Name(station, 'avgh', 'N_N62'): 'cnc2',
    }, send
)

station_profile_data['aerosol']['raw']['cpcstatus2'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'Tu_N62'): 'Tinlet',
        Name(station, 'raw', 'TDu_N62'): 'TDinlet',
        Name(station, 'raw', 'Uu_N62'): 'Uinlet',
        Name(station, 'raw', 'T1_N62'): 'Tconditioner',
        Name(station, 'raw', 'T2_N62'): 'Tinitiator',
        Name(station, 'raw', 'T3_N62'): 'Tmoderator',
        Name(station, 'raw', 'T4_N62'): 'Toptics',
        Name(station, 'raw', 'T5_N62'): 'Theatsink',
        Name(station, 'raw', 'T6_N62'): 'Tpcb',
        Name(station, 'raw', 'T7_N62'): 'Tcabinet',
        Name(station, 'raw', 'Q_N62'): 'Qsample',
        Name(station, 'raw', 'P_N62'): 'Psample',
        Name(station, 'raw', 'Pd_N62'): 'PDorifice',
    }, send
)
station_profile_data['aerosol']['realtime']['cpcstatus2'] = {
    RealtimeTranslator.Key('Tu_N62'): 'Tinlet',
    RealtimeTranslator.Key('TDu_N62'): 'TDinlet',
    RealtimeTranslator.Key('Uu_N62'): 'Uinlet',
    RealtimeTranslator.Key('T1_N62'): 'Tconditioner',
    RealtimeTranslator.Key('T2_N62'): 'Tinitiator',
    RealtimeTranslator.Key('T3_N62'): 'Tmoderator',
    RealtimeTranslator.Key('T4_N62'): 'Toptics',
    RealtimeTranslator.Key('T5_N62'): 'Theatsink',
    RealtimeTranslator.Key('T6_N62'): 'Tpcb',
    RealtimeTranslator.Key('T7_N62'): 'Tcabinet',
    RealtimeTranslator.Key('Q_N62'): 'Qsample',
    RealtimeTranslator.Key('P_N62'): 'Psample',
    RealtimeTranslator.Key('Pd_N62'): 'PDorifice',
}


def get(station: str, data_name: str, start_epoch_ms: int, end_epoch_ms: int,
        send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
    return data_profile_get(station, data_name, start_epoch_ms, end_epoch_ms, send, station_profile_data)
