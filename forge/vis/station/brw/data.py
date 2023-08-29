import typing
from ..cpd3 import DataStream, DataReader, EditedReader, RealtimeTranslator, Name, data_profile_get, detach, profile_data


station_profile_data = detach(profile_data)

station_profile_data['aerosol']['raw']['cnc'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'N_N61'): 'cnc',
        Name(station, 'raw', 'N_N62'): 'cnc2',
        Name(station, 'raw', 'N_N12'): 'ccn',
    }, send
)
station_profile_data['aerosol']['realtime']['cnc'] = {
    RealtimeTranslator.Key('N_N61'): 'cnc',
    RealtimeTranslator.Key('N_N62'): 'cnc2',
    RealtimeTranslator.Key('N_N12'): 'ccn',
}
station_profile_data['aerosol']['editing']['cnc'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
    start_epoch_ms, end_epoch_ms, station, 'aerosol', {
        Name(station, 'clean', 'N_N61'): 'cnc',
        Name(station, 'clean', 'N_N62'): 'cnc2',
        Name(station, 'clean', 'N_N12'): 'ccn',
    }, send
)
station_profile_data['aerosol']['clean']['cnc'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'clean', 'N_N61'): 'cnc',
        Name(station, 'clean', 'N_N62'): 'cnc2',
        Name(station, 'clean', 'N_N12'): 'ccn',
    }, send
)
station_profile_data['aerosol']['avgh']['cnc'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'avgh', 'N_N61'): 'cnc',
        Name(station, 'avgh', 'N_N62'): 'cnc2',
        Name(station, 'avgh', 'N_N12'): 'ccn',
    }, send
)

station_profile_data['aerosol']['raw']['wind'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'WS1_XM1'): 'WSambient',
        Name(station, 'raw', 'WD1_XM1'): 'WDambient',
        Name(station, 'raw', 'WS_XM2'): 'WSfilter',
        Name(station, 'raw', 'WD_XM2'): 'WDfilter',
    }, send
)
station_profile_data['aerosol']['realtime']['wind'] = {
    RealtimeTranslator.Key('WS_XM2'): 'WS',
    RealtimeTranslator.Key('WD_XM2'): 'WD',
}

station_profile_data['aerosol']['raw']['flow'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'Q_Q11'): 'sample',
        Name(station, 'raw', 'Q_Q11', {'pm10'}): 'sample',
        Name(station, 'raw', 'Q_Q11', {'pm1'}): 'sample',
        Name(station, 'raw', 'Q_Q11', {'pm25'}): 'sample',
        Name(station, 'raw', 'Q_Q21'): 'filter',
        Name(station, 'raw', 'Q_Q31'): 'filter2',
        Name(station, 'raw', 'Pd_P01'): 'pitot',
    }, send
)
station_profile_data['aerosol']['realtime']['flow'] = {
    RealtimeTranslator.Key('Q_Q11'): 'sample',
    RealtimeTranslator.Key('Q_Q11', {'pm10'}): 'sample',
    RealtimeTranslator.Key('Q_Q11', {'pm1'}): 'sample',
    RealtimeTranslator.Key('Q_Q11', {'pm25'}): 'sample',
    RealtimeTranslator.Key('Q_Q21'): 'filter',
    RealtimeTranslator.Key('Q_Q31'): 'filter2',
    RealtimeTranslator.Key('Pd_P01'): 'pitot',
}

station_profile_data['aerosol']['raw']['temperature'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'T_V51'): 'Tinlet', Name(station, 'raw', 'U_V51'): 'Uinlet',
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

        Name(station, 'raw', 'T_V21'): 'Tfilter', Name(station, 'raw', 'U_V21'): 'Ufilter',
        Name(station, 'raw', 'T_V22'): 'Tfilterrack',
    }, send
)
station_profile_data['aerosol']['realtime']['temperature'] = {
    RealtimeTranslator.Key('T_V51'): 'Tinlet', RealtimeTranslator.Key('U_V51'): 'Uinlet',

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

    RealtimeTranslator.Key('T_V21'): 'Tfilter', RealtimeTranslator.Key('U_V21'): 'Ufilter',
    RealtimeTranslator.Key('T_V22'): 'Tfilterrack',
}

station_profile_data['aerosol']['raw']['filterstatus'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, dict(
        [(Name(station, 'raw', f'Fn_F21'), f'Fn')] +
        [(Name(station, 'raw', f'Pd_P2{i+1}'), f'Pd{i+1}') for i in range(8)]
    ), send
)
station_profile_data['aerosol']['realtime']['filterstatus'] = dict(
    [(RealtimeTranslator.Key(f'Fn_F21'), f'Fn')] +
    [(RealtimeTranslator.Key(f'Pd_P2{i+1}'), f'Pd{i+1}') for i in range(8)]
)

station_profile_data['aerosol']['raw']['filterstatus2'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, dict(
        [(Name(station, 'raw', f'Fn_F31'), f'Fn')] +
        [(Name(station, 'raw', f'Pd_P3{i+1}'), f'Pd{i+1}') for i in range(8)]
    ), send
)
station_profile_data['aerosol']['realtime']['filterstatus2'] = dict(
    [(RealtimeTranslator.Key(f'Fn_F31'), f'Fn')] +
    [(RealtimeTranslator.Key(f'Pd_P3{i+1}'), f'Pd{i+1}') for i in range(8)]
)

station_profile_data['aerosol']['raw']['umacstatus'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'T_X1'): 'T',
        Name(station, 'raw', 'V_X1'): 'V',
        Name(station, 'raw', 'T_X3'): 'Tfilter',
        Name(station, 'raw', 'V_X3'): 'Vfilter',
        Name(station, 'raw', 'T_X5'): 'Tfilter2',
        Name(station, 'raw', 'V_X5'): 'Vfilter2',
    }, send
)
station_profile_data['aerosol']['realtime']['umacstatus'] = {
    RealtimeTranslator.Key('T_X1'): 'T',
    RealtimeTranslator.Key('V_X1'): 'V',
    RealtimeTranslator.Key('T_X3'): 'Tfilter',
    RealtimeTranslator.Key('V_X3'): 'Vfilter',
    RealtimeTranslator.Key('T_X5'): 'Tfilter2',
    RealtimeTranslator.Key('V_X5'): 'Vfilter2',
}

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

station_profile_data['aerosol']['raw']['ccnstatus'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'Tu_N12'): 'Tinlet',
        Name(station, 'raw', 'T1_N12'): 'Ttec1',
        Name(station, 'raw', 'T2_N12'): 'Ttec2',
        Name(station, 'raw', 'T3_N12'): 'Ttec3',
        Name(station, 'raw', 'T4_N12'): 'Tsample',
        Name(station, 'raw', 'T5_N12'): 'Topc',
        Name(station, 'raw', 'T6_N12'): 'Tnafion',
        Name(station, 'raw', 'Q1_N12'): 'Qsample',
        Name(station, 'raw', 'Q2_N12'): 'Qsheath',
        Name(station, 'raw', 'Uc_N12'): 'SScalc',
        Name(station, 'raw', 'U_N12'): 'SSset',
    }, send
)
station_profile_data['aerosol']['realtime']['ccnstatus'] = {
    RealtimeTranslator.Key('Tu_N12'): 'Tinlet',
    RealtimeTranslator.Key('T1_N12'): 'Ttec1',
    RealtimeTranslator.Key('T2_N12'): 'Ttec2',
    RealtimeTranslator.Key('T3_N12'): 'Ttec3',
    RealtimeTranslator.Key('T4_N12'): 'Tsample',
    RealtimeTranslator.Key('T5_N12'): 'Topc',
    RealtimeTranslator.Key('T6_N12'): 'Tnafion',
    RealtimeTranslator.Key('Q1_N12'): 'Qsample',
    RealtimeTranslator.Key('Q2_N12'): 'Qsheath',
    RealtimeTranslator.Key('Uc_N12'): 'SScalc',
    RealtimeTranslator.Key('U_N12'): 'SSset',
}


def get(station: str, data_name: str, start_epoch_ms: int, end_epoch_ms: int,
        send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
    return data_profile_get(station, data_name, start_epoch_ms, end_epoch_ms, send, station_profile_data)
