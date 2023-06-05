import typing
from ..cpd3 import DataStream, DataReader, EditedReader, RealtimeTranslator, Name, data_profile_get, detach, profile_data


station_profile_data = detach(profile_data)


station_profile_data['aerosol']['raw']['cnc'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'N_N61'): 'cnc',
        Name(station, 'raw', 'N_N62'): 'cnc2',
    }, send
)
station_profile_data['aerosol']['realtime']['cnc'] = {
    RealtimeTranslator.Key('N_N61'): 'cnc',
    RealtimeTranslator.Key('N_N62'): 'cnc2',
}
station_profile_data['aerosol']['editing']['cnc'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
    start_epoch_ms, end_epoch_ms, station, 'aerosol', {
        Name(station, 'clean', 'N_N61'): 'cnc',
        Name(station, 'clean', 'N_N62'): 'cnc2',
    }, send
)
station_profile_data['aerosol']['clean']['cnc'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'clean', 'N_N61'): 'cnc',
        Name(station, 'clean', 'N_N62'): 'cnc2',
    }, send
)
station_profile_data['aerosol']['avgh']['cnc'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'avgh', 'N_N61'): 'cnc',
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
        Name(station, 'raw', 'T6_N62'): 'Tcase',
        Name(station, 'raw', 'T7_N62'): 'Tboard',
        Name(station, 'raw', 'TD1_N62'): 'TDgrowth',
        Name(station, 'raw', 'Q_N62'): 'Qsample',
        Name(station, 'raw', 'P_N62'): 'Psample',
        Name(station, 'raw', 'PCT_N62'): 'PCTwick',
        Name(station, 'raw', 'V_N62'): 'Vpulse',
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
    RealtimeTranslator.Key('T6_N62'): 'Tcase',
    RealtimeTranslator.Key('T7_N62'): 'Tboard',
    RealtimeTranslator.Key('TD1_N62'): 'TDgrowth',
    RealtimeTranslator.Key('Q_N62'): 'Qsample',
    RealtimeTranslator.Key('P_N62'): 'Psample',
    RealtimeTranslator.Key('PCT_N62'): 'PCTwick',
    RealtimeTranslator.Key('V_N62'): 'Vpulse',
}


station_profile_data['aerosol']['raw']['temperature'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'T_V51'): 'Tinlet', Name(station, 'raw', 'U_V51'): 'Uinlet',
        Name(station, 'raw', 'T_V02'): 'Taux', Name(station, 'raw', 'U_V02'): 'Uaux',
        Name(station, 'raw', 'T1_XM1'): 'Tambient',
        Name(station, 'raw', 'U1_XM1'): 'Uambient',
        Name(station, 'raw', 'TD1_XM1'): 'TDambient',

        Name(station, 'raw', 'T_V11'): 'Tsample', Name(station, 'raw', 'U_V11'): 'Usample',
        Name(station, 'raw', 'T_V11', {'pm10'}): 'Tsample', Name(station, 'raw', 'U_V11', {'pm10'}): 'Usample',
        Name(station, 'raw', 'T_V11', {'pm1'}): 'Tsample', Name(station, 'raw', 'U_V11', {'pm1'}): 'Usample',
        Name(station, 'raw', 'T_V11', {'pm25'}): 'Tsample', Name(station, 'raw', 'U_V11', {'pm25'}): 'Usample',

        Name(station, 'raw', 'Tu_S11'): 'Tnephinlet', Name(station, 'raw', 'Uu_S11'): 'Unephinlet',
        Name(station, 'raw', 'Tu_S11', {'pm10'}): 'Tnephinlet', Name(station, 'raw', 'Uu_S11', {'pm10'}): 'Unephinlet',
        Name(station, 'raw', 'Tu_S11', {'pm1'}): 'Tnephinlet', Name(station, 'raw', 'Uu_S11', {'pm1'}): 'Unephinlet',
        Name(station, 'raw', 'Tu_S11', {'pm25'}): 'Tnephinlet', Name(station, 'raw', 'Uu_S11', {'pm25'}): 'Unephinlet',

        Name(station, 'raw', 'T_S11'): 'Tneph', Name(station, 'raw', 'U_S11'): 'Uneph',
        Name(station, 'raw', 'T_S11', {'pm10'}): 'Tneph', Name(station, 'raw', 'U_S11', {'pm10'}): 'Uneph',
        Name(station, 'raw', 'T_S11', {'pm1'}): 'Tneph', Name(station, 'raw', 'U_S11', {'pm1'}): 'Uneph',
        Name(station, 'raw', 'T_S11', {'pm25'}): 'Tneph', Name(station, 'raw', 'U_S11', {'pm25'}): 'Uneph',
    }, send
)
station_profile_data['aerosol']['realtime']['temperature'] = {
    RealtimeTranslator.Key('T_V51'): 'Tinlet', RealtimeTranslator.Key('U_V51'): 'Uinlet',
    RealtimeTranslator.Key('T_V02'): 'Taux', RealtimeTranslator.Key('U_V02'): 'Uaux',

    RealtimeTranslator.Key('T_V11'): 'Tsample', RealtimeTranslator.Key('U_V11'): 'Usample',
    RealtimeTranslator.Key('T_V11', {'pm10'}): 'Tsample', RealtimeTranslator.Key('U_V11', {'pm10'}): 'Usample',
    RealtimeTranslator.Key('T_V11', {'pm1'}): 'Tsample', RealtimeTranslator.Key('U_V11', {'pm1'}): 'Usample',
    RealtimeTranslator.Key('T_V11', {'pm25'}): 'Tsample', RealtimeTranslator.Key('U_V11', {'pm25'}): 'Usample',

    RealtimeTranslator.Key('Tu_S11'): 'Tnephinlet', RealtimeTranslator.Key('Uu_S11'): 'Unephinlet',
    RealtimeTranslator.Key('Tu_S11', {'pm10'}): 'Tnephinlet', RealtimeTranslator.Key('Uu_S11', {'pm10'}): 'Unephinlet',
    RealtimeTranslator.Key('Tu_S11', {'pm1'}): 'Tnephinlet', RealtimeTranslator.Key('Uu_S11', {'pm1'}): 'Unephinlet',
    RealtimeTranslator.Key('Tu_S11', {'pm25'}): 'Tnephinlet', RealtimeTranslator.Key('Uu_S11', {'pm25'}): 'Unephinlet',

    RealtimeTranslator.Key('T_S11'): 'Tneph', RealtimeTranslator.Key('U_S11'): 'Uneph',
    RealtimeTranslator.Key('T_S11', {'pm10'}): 'Tneph', RealtimeTranslator.Key('U_S11', {'pm10'}): 'Uneph',
    RealtimeTranslator.Key('T_S11', {'pm1'}): 'Tneph', RealtimeTranslator.Key('U_S11', {'pm1'}): 'Uneph',
    RealtimeTranslator.Key('T_S11', {'pm25'}): 'Tneph', RealtimeTranslator.Key('U_S11', {'pm25'}): 'Uneph',
}

station_profile_data['aerosol']['raw']['wind'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'WS1_XM1'): 'WSgrad', Name(station, 'raw', 'WD1_XM1'): 'WDgrad',
        Name(station, 'raw', 'WS_X1'): 'WSaerosol', Name(station, 'raw', 'WD_X1'): 'WDaerosol',
    }, send
)
station_profile_data['aerosol']['realtime']['wind'] = {
    RealtimeTranslator.Key('WS_X1'): 'WS', RealtimeTranslator.Key('WD_X1'): 'WD',
}
station_profile_data['aerosol']['clean']['wind'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'clean', 'WS1_XM1'): 'WSgrad', Name(station, 'clean', 'WD1_XM1'): 'WDgrad',
        Name(station, 'clean', 'WS_X1'): 'WSaerosol', Name(station, 'clean', 'WD_X1'): 'WDaerosol',
    }, send
)
station_profile_data['aerosol']['avgh']['wind'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'avgh', 'WS1_XM1'): 'WSgrad', Name(station, 'avgh', 'WD1_XM1'): 'WDgrad',
        Name(station, 'avgh', 'WS_X1'): 'WSaerosol', Name(station, 'avgh', 'WD_X1'): 'WDaerosol',
    }, send
)
station_profile_data['aerosol']['editing']['wind'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
    start_epoch_ms, end_epoch_ms, station, 'aerosol', {
        Name(station, 'clean', 'WS1_XM1'): 'WSgrad', Name(station, 'clean', 'WD1_XM1'): 'WDgrad',
        Name(station, 'clean', 'WS_X1'): 'WSaerosol', Name(station, 'clean', 'WD_X1'): 'WDaerosol',
    }, send
)


def get(station: str, data_name: str, start_epoch_ms: int, end_epoch_ms: int,
        send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
    return data_profile_get(station, data_name, start_epoch_ms, end_epoch_ms, send, station_profile_data)
