import typing
from ..cpd3 import DataStream, DataReader, RealtimeTranslator, Name, data_profile_get, detach, profile_data


station_profile_data = detach(profile_data)


station_profile_data['aerosol']['raw']['temperature'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'Tx_S11'): 'Tnephcell', Name(station, 'raw', 'Ux_S11'): 'Unephcell',
        Name(station, 'raw', 'Tx_S11', {'pm10'}): 'Tnephcell', Name(station, 'raw', 'Ux_S11', {'pm10'}): 'Unephcell',
        Name(station, 'raw', 'Tx_S11', {'pm1'}): 'Tnephcell', Name(station, 'raw', 'Ux_S11', {'pm1'}): 'Unephcell',
        Name(station, 'raw', 'Tx_S11', {'pm25'}): 'Tnephcell', Name(station, 'raw', 'Ux_S11', {'pm25'}): 'Unephcell',

        Name(station, 'raw', 'T_S11'): 'Tneph', Name(station, 'raw', 'U_S11'): 'Uneph',
        Name(station, 'raw', 'T_S11', {'pm10'}): 'Tneph', Name(station, 'raw', 'U_S11', {'pm10'}): 'Uneph',
        Name(station, 'raw', 'T_S11', {'pm1'}): 'Tneph', Name(station, 'raw', 'U_S11', {'pm1'}): 'Uneph',
        Name(station, 'raw', 'T_S11', {'pm25'}): 'Tneph', Name(station, 'raw', 'U_S11', {'pm25'}): 'Uneph',
    }, send
)
station_profile_data['aerosol']['realtime']['temperature'] = {
    RealtimeTranslator.Key('Tx_S11'): 'Tnephcell', RealtimeTranslator.Key('Ux_S11'): 'Unephcell',
    RealtimeTranslator.Key('Tx_S11', {'pm10'}): 'Tnephcell', RealtimeTranslator.Key('Ux_S11', {'pm10'}): 'Unephcell',
    RealtimeTranslator.Key('Tx_S11', {'pm1'}): 'Tnephcell', RealtimeTranslator.Key('Ux_S11', {'pm1'}): 'Unephcell',
    RealtimeTranslator.Key('Tx_S11', {'pm25'}): 'Tnephcell', RealtimeTranslator.Key('Ux_S11', {'pm25'}): 'Unephcell',

    RealtimeTranslator.Key('T_S11'): 'Tneph', RealtimeTranslator.Key('U_S11'): 'Uneph',
    RealtimeTranslator.Key('T_S11', {'pm10'}): 'Tneph', RealtimeTranslator.Key('U_S11', {'pm10'}): 'Uneph',
    RealtimeTranslator.Key('T_S11', {'pm1'}): 'Tneph', RealtimeTranslator.Key('U_S11', {'pm1'}): 'Uneph',
    RealtimeTranslator.Key('T_S11', {'pm25'}): 'Tneph', RealtimeTranslator.Key('U_S11', {'pm25'}): 'Uneph',
}

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
station_profile_data['aerosol']['realtime']['cpcstatus'] = {
    RealtimeTranslator.Key('Tu_N71'): 'Tinlet',
    RealtimeTranslator.Key('TDu_N71'): 'TDinlet',
    RealtimeTranslator.Key('Uu_N71'): 'Uinlet',
    RealtimeTranslator.Key('T1_N71'): 'Tconditioner',
    RealtimeTranslator.Key('T2_N71'): 'Tinitiator',
    RealtimeTranslator.Key('T3_N71'): 'Tmoderator',
    RealtimeTranslator.Key('T4_N71'): 'Toptics',
    RealtimeTranslator.Key('T5_N71'): 'Theatsink',
    RealtimeTranslator.Key('T6_N71'): 'Tpcb',
    RealtimeTranslator.Key('T7_N71'): 'Tcabinet',
    RealtimeTranslator.Key('Q_N71'): 'Qsample',
    RealtimeTranslator.Key('P_N71'): 'Psample',
    RealtimeTranslator.Key('Pd_N71'): 'PDorifice',
}


def get(station: str, data_name: str, start_epoch_ms: int, end_epoch_ms: int,
        send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
    return data_profile_get(station, data_name, start_epoch_ms, end_epoch_ms, send, station_profile_data)
