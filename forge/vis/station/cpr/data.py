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
        Name(station, 'raw', 'T1_N72'): 'Tsaturator',
        Name(station, 'raw', 'T2_N72'): 'Tcondenser',
        Name(station, 'raw', 'T3_N72'): 'Toptics',
        Name(station, 'raw', 'T4_N72'): 'Tcabinet',
        Name(station, 'raw', 'Q_N72'): 'Qsample',
        Name(station, 'raw', 'Qu_N72'): 'Qinlet',
    }, send
)
station_profile_data['aerosol']['realtime']['cpcstatus2'] = {
    RealtimeTranslator.Key('T1_N72'): 'Tsaturator',
    RealtimeTranslator.Key('T2_N72'): 'Tcondenser',
    RealtimeTranslator.Key('T3_N72'): 'Toptics',
    RealtimeTranslator.Key('T4_N72'): 'Tcabinet',
    RealtimeTranslator.Key('Q_N72'): 'Qsample',
    RealtimeTranslator.Key('Qu_N72'): 'Qinlet',
}

station_profile_data['aerosol']['raw']['flow'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'Q_Q11'): 'sample',
        Name(station, 'raw', 'Q_Q11', {'pm10'}): 'sample',
        Name(station, 'raw', 'Q_Q11', {'pm1'}): 'sample',
        Name(station, 'raw', 'Q_Q11', {'pm25'}): 'sample',
        Name(station, 'raw', 'Q_Q12'): 'dilution',
        Name(station, 'raw', 'Pd_P01'): 'pitot',
    }, send
)
station_profile_data['aerosol']['realtime']['flow'] = {
    RealtimeTranslator.Key('Q_Q11'): 'sample',
    RealtimeTranslator.Key('Q_Q11', {'pm10'}): 'sample',
    RealtimeTranslator.Key('Q_Q11', {'pm1'}): 'sample',
    RealtimeTranslator.Key('Q_Q11', {'pm25'}): 'sample',
    RealtimeTranslator.Key('Q_Q12'): 'dilution',
    RealtimeTranslator.Key('Pd_P01'): 'pitot',
}

station_profile_data['aerosol']['raw']['temperature'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'T_V51'): 'Tinlet', Name(station, 'raw', 'U_V51'): 'Uinlet',
        Name(station, 'raw', 'T1_XM1'): 'Tambient',
        Name(station, 'raw', 'U1_XM1'): 'Uambient',
        Name(station, 'raw', 'TD1_XM1'): 'TDambient',
        Name(station, 'raw', 'T1_XM2'): 'Tpwd',

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

        Name(station, 'raw', 'T_V01'): 'Troom', Name(station, 'raw', 'U_V01'): 'Uroom',
    }, send
)
station_profile_data['aerosol']['realtime']['temperature'] = {
    RealtimeTranslator.Key('T_V51'): 'Tinlet', RealtimeTranslator.Key('U_V51'): 'Uinlet',
    RealtimeTranslator.Key('T1_XM1'): 'Tambient',
    RealtimeTranslator.Key('U1_XM1'): 'Uambient',
    RealtimeTranslator.Key('TD1_XM1'): 'TDambient',
    RealtimeTranslator.Key('T1_XM2'): 'Tpwd',

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

    RealtimeTranslator.Key('T_V01'): 'Troom', RealtimeTranslator.Key('U_V01'): 'Uroom',
}

station_profile_data['aerosol']['raw']['clouds'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'WI_XM1'): 'precipitation',
        Name(station, 'raw', 'WZ_XM2'): 'visibility',
        Name(station, 'raw', 'VA_XM1'): 'radiation',
        Name(station, 'raw', 'R_XM1'): 'radiation',
    }, send
)
station_profile_data['aerosol']['realtime']['clouds'] = {
    RealtimeTranslator.Key('WI_XM1'): 'precipitation',
    RealtimeTranslator.Key('WZ_XM2'): 'visibility',
    RealtimeTranslator.Key('VA_XM1'): 'radiation',
    RealtimeTranslator.Key('R_XM1'): 'radiation',
}
station_profile_data['aerosol']['editing']['clouds'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
    start_epoch_ms, end_epoch_ms, station, 'aerosol', {
        Name(station, 'clean', 'WI_XM1'): 'precipitation',
        Name(station, 'clean', 'WZ_XM2'): 'visibility',
        Name(station, 'clean', 'VA_XM1'): 'radiation',
        Name(station, 'clean', 'R_XM1'): 'radiation',
    }, send
)
station_profile_data['aerosol']['clean']['clouds'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'clean', 'WI_XM1'): 'precipitation',
        Name(station, 'clean', 'WZ_XM2'): 'visibility',
        Name(station, 'clean', 'VA_XM1'): 'radiation',
        Name(station, 'clean', 'R_XM1'): 'radiation',
    }, send
)
station_profile_data['aerosol']['avgh']['clouds'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'avgh', 'WI_XM1'): 'precipitation',
        Name(station, 'avgh', 'WZ_XM2'): 'visibility',
        Name(station, 'avgh', 'VA_XM1'): 'radiation',
        Name(station, 'avgh', 'R_XM1'): 'radiation',
    }, send
)

station_profile_data['aerosol']['raw']['hurricane'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'WI_XM3'): 'precipitation',
        Name(station, 'raw', 'WS_XM3'): 'WS',
        Name(station, 'raw', 'P_XM3'): 'pressure',
        Name(station, 'raw', 'Ipa_S81'): 'IBsa',
        Name(station, 'raw', 'Ipb_S81'): 'IBsb',
        Name(station, 'raw', 'Bs_S81'): 'Bs',
    }, send
)
station_profile_data['aerosol']['realtime']['hurricane'] = {
    RealtimeTranslator.Key('WI_XM3'): 'precipitation',
    RealtimeTranslator.Key('WS_XM3'): 'WS',
    RealtimeTranslator.Key('P_XM3'): 'pressure',
    RealtimeTranslator.Key('Ipa_S81'): 'IBsa',
    RealtimeTranslator.Key('Ipb_S81'): 'IBsb',
    RealtimeTranslator.Key('Bs_S81'): 'Bs',
}


def get(station: str, data_name: str, start_epoch_ms: int, end_epoch_ms: int,
        send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
    return data_profile_get(station, data_name, start_epoch_ms, end_epoch_ms, send, station_profile_data)
