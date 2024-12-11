import typing
from ..cpd3 import DataStream, DataReader, RealtimeTranslator, Name, data_profile_get, detach, profile_data


station_profile_data = detach(profile_data)


station_profile_data['aerosol']['raw']['temperature'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
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

        Name(station, 'raw', 'T_V21'): 'Tline2', Name(station, 'raw', 'U_V21'): 'Uline2',
        Name(station, 'raw', 'T_V21', {'pm10'}): 'Tline2', Name(station, 'raw', 'U_V21', {'pm10'}): 'Uline2',
        Name(station, 'raw', 'T_V21', {'pm1'}): 'Tline2', Name(station, 'raw', 'U_V21', {'pm1'}): 'Uline2',
        Name(station, 'raw', 'T_V21', {'pm25'}): 'Tline2', Name(station, 'raw', 'U_V21', {'pm25'}): 'Uline2',

        Name(station, 'raw', 'T_V31'): 'Tline3', Name(station, 'raw', 'U_V31'): 'Uline3',
        Name(station, 'raw', 'T_V31', {'pm10'}): 'Tline3', Name(station, 'raw', 'U_V31', {'pm10'}): 'Uline3',
        Name(station, 'raw', 'T_V31', {'pm1'}): 'Tline3', Name(station, 'raw', 'U_V31', {'pm1'}): 'Uline3',
        Name(station, 'raw', 'T_V31', {'pm25'}): 'Tline3', Name(station, 'raw', 'U_V31', {'pm25'}): 'Uline3',
    }, send
)
station_profile_data['aerosol']['realtime']['temperature'] = {
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

    RealtimeTranslator.Key('T_V21'): 'Tline2', RealtimeTranslator.Key('U_V21'): 'Uline2',
    RealtimeTranslator.Key('T_V21', {'pm10'}): 'Tline2', RealtimeTranslator.Key('U_V21', {'pm10'}): 'Uline2',
    RealtimeTranslator.Key('T_V21', {'pm1'}): 'Tline2', RealtimeTranslator.Key('U_V21', {'pm1'}): 'Uline2',
    RealtimeTranslator.Key('T_V21', {'pm25'}): 'Tline2', RealtimeTranslator.Key('U_V21', {'pm25'}): 'Uline2',

    RealtimeTranslator.Key('T_V31'): 'Tline3', RealtimeTranslator.Key('U_V31'): 'Uline3',
    RealtimeTranslator.Key('T_V31', {'pm10'}): 'Tline3', RealtimeTranslator.Key('U_V31', {'pm10'}): 'Uline3',
    RealtimeTranslator.Key('T_V31', {'pm1'}): 'Tline3', RealtimeTranslator.Key('U_V31', {'pm1'}): 'Uline3',
    RealtimeTranslator.Key('T_V31', {'pm25'}): 'Tline3', RealtimeTranslator.Key('U_V31', {'pm25'}): 'Uline3',
}

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
        Name(station, 'raw', 'Pd2_N61'): 'PDorifice',
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
    RealtimeTranslator.Key('Pd2_N61'): 'PDorifice',
    RealtimeTranslator.Key('A_N61'): 'Alaser',
}


def get(station: str, data_name: str, start_epoch_ms: int, end_epoch_ms: int,
        send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
    return data_profile_get(station, data_name, start_epoch_ms, end_epoch_ms, send, station_profile_data)
