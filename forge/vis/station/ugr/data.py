import typing
from ..cpd3 import DataStream, DataReader, RealtimeTranslator, Name, data_profile_get, detach, profile_data


station_profile_data = detach(profile_data)


station_profile_data['aerosol']['raw']['maap'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'BacR_A31'): 'Ba',
        Name(station, 'raw', 'XR_A31'): 'X',
    }, send
)
station_profile_data['aerosol']['realtime']['maap'] = {
    RealtimeTranslator.Key('BacR_A31'): 'Ba',
    RealtimeTranslator.Key('XR_A31'): 'X',
}

station_profile_data['aerosol']['raw']['maapstatus'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'P_A31'): 'Psample',
        Name(station, 'raw', 'T1_A31'): 'Tambient',
        Name(station, 'raw', 'T2_A31'): 'Tmeasurementhead',
        Name(station, 'raw', 'T3_A31'): 'Tsystem',
        Name(station, 'raw', 'Q_A31'): 'Qsample',
        Name(station, 'raw', 'IrR_A31'): 'Ir',
        Name(station, 'raw', 'IfR_A31'): 'If',
        Name(station, 'raw', 'IpR_A31'): 'Ip',
        Name(station, 'raw', 'Is1_A31'): 'Is1',
        Name(station, 'raw', 'Is2_A31'): 'Is2',
    }, send
)
station_profile_data['aerosol']['realtime']['maapstatus'] = {
    RealtimeTranslator.Key('P_A31'): 'Psample',
    RealtimeTranslator.Key('T1_A31'): 'Tambient',
    RealtimeTranslator.Key('T2_A31'): 'Tmeasurementhead',
    RealtimeTranslator.Key('T3_A31'): 'Tsystem',
    RealtimeTranslator.Key('Q_A31'): 'Qsample',
    RealtimeTranslator.Key('IrR_A31'): 'Ir',
    RealtimeTranslator.Key('IfR_A31'): 'If',
    RealtimeTranslator.Key('IpR_A31'): 'Ip',
    RealtimeTranslator.Key('Is1_A31'): 'Is1',
    RealtimeTranslator.Key('Is2_A31'): 'Is2',
}


station_profile_data['aerosol']['raw']['aethalometer'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, dict(
        [(Name(station, 'raw', f'Ba{i+1}_A41'), f'Ba{i+1}') for i in range(7)] +
        [(Name(station, 'raw', f'X{i+1}_A41'), f'X{i+1}') for i in range(7)] +
        [(Name(station, 'raw', f'ZFACTOR{i+1}_A41'), f'CF{i+1}') for i in range(7)] +
        [(Name(station, 'raw', f'Ir{i+1}_A41'), f'Ir{i+1}') for i in range(7)]
    ), send
)
station_profile_data['aerosol']['raw']['aethalometerstatus'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'T1_A41'): 'Tcontroller',
        Name(station, 'raw', 'T2_A41'): 'Tsupply',
        Name(station, 'raw', 'T3_A41'): 'Tled',
    }, send
)
station_profile_data['aerosol']['realtime']['aethalometer'] = dict(
    [(RealtimeTranslator.Key(f'Ba{i+1}_A41'), f'Ba{i+1}') for i in range(7)] +
    [(RealtimeTranslator.Key(f'X{i+1}_A41'), f'X{i+1}') for i in range(7)] +
    [(RealtimeTranslator.Key(f'ZFACTOR{i+1}_A41'), f'CF{i+1}') for i in range(7)] +
    [(RealtimeTranslator.Key(f'Ir{i+1}_A41'), f'Ir{i+1}') for i in range(7)]
)
station_profile_data['aerosol']['realtime']['aethalometerstatus'] = {
    RealtimeTranslator.Key('T1_A41'): 'Tcontroller',
    RealtimeTranslator.Key('T2_A41'): 'Tsupply',
    RealtimeTranslator.Key('T3_A41'): 'Tled',
}
station_profile_data['aerosol']['clean']['aethalometer'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, dict(
        [(Name(station, 'clean', f'Ba{i+1}_A41'), f'Ba{i+1}') for i in range(7)] +
        [(Name(station, 'clean', f'X{i+1}_A41'), f'X{i+1}') for i in range(7)] +
        [(Name(station, 'clean', f'ZFACTOR{i+1}_A41'), f'CF{i+1}') for i in range(7)] +
        [(Name(station, 'clean', f'Ir{i+1}_A41'), f'Ir{i+1}') for i in range(7)]
    ), send
)
station_profile_data['aerosol']['avgh']['aethalometer'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, dict(
        [(Name(station, 'avgh', f'Ba{i+1}_A41'), f'Ba{i+1}') for i in range(7)] +
        [(Name(station, 'avgh', f'X{i+1}_A41'), f'X{i+1}') for i in range(7)] +
        [(Name(station, 'avgh', f'ZFACTOR{i+1}_A41'), f'CF{i+1}') for i in range(7)] +
        [(Name(station, 'avgh', f'Ir{i+1}_A41'), f'Ir{i+1}') for i in range(7)]
    ), send
)
station_profile_data['aerosol']['editing']['aethalometer'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
    start_epoch_ms, end_epoch_ms, station, 'aerosol', dict(
        [(Name(station, 'clean', f'Ba{i + 1}_A41'), f'Ba{i + 1}') for i in range(7)] +
        [(Name(station, 'clean', f'X{i + 1}_A41'), f'X{i + 1}') for i in range(7)] +
        [(Name(station, 'clean', f'ZFACTOR{i + 1}_A41'), f'CF{i + 1}') for i in range(7)] +
        [(Name(station, 'clean', f'Ir{i + 1}_A41'), f'Ir{i + 1}') for i in range(7)]
    ), send
)


station_profile_data['aerosol']['raw']['temperature'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'T_V11'): 'Tdryneph', Name(station, 'raw', 'U_V11'): 'Udryneph',
        Name(station, 'raw', 'T_V11', {'pm10'}): 'Tdryneph', Name(station, 'raw', 'U_V11', {'pm10'}): 'Udryneph',
        Name(station, 'raw', 'T_V11', {'pm1'}): 'Tdryneph', Name(station, 'raw', 'U_V11', {'pm1'}): 'Udryneph',
        Name(station, 'raw', 'T_V11', {'pm25'}): 'Tdryneph', Name(station, 'raw', 'U_V11', {'pm25'}): 'Udryneph',
        
        Name(station, 'raw', 'T_V12'): 'Thumidiferinlet', Name(station, 'raw', 'U_V12'): 'Uhumidiferinlet',
        Name(station, 'raw', 'T_V12', {'pm10'}): 'Thumidiferinlet', Name(station, 'raw', 'U_V12', {'pm10'}): 'Uhumidiferinlet',
        Name(station, 'raw', 'T_V12', {'pm1'}): 'Thumidiferinlet', Name(station, 'raw', 'U_V12', {'pm1'}): 'Uhumidiferinlet',
        Name(station, 'raw', 'T_V12', {'pm25'}): 'Thumidiferinlet', Name(station, 'raw', 'U_V12', {'pm25'}): 'Uhumidiferinlet',
        
        Name(station, 'raw', 'T_V13'): 'Thumidiferoutlet', Name(station, 'raw', 'U_V13'): 'Uhumidiferoutlet',
        Name(station, 'raw', 'T_V13', {'pm10'}): 'Thumidiferoutlet', Name(station, 'raw', 'U_V13', {'pm10'}): 'Uhumidiferoutlet',
        Name(station, 'raw', 'T_V13', {'pm1'}): 'Thumidiferoutlet', Name(station, 'raw', 'U_V13', {'pm1'}): 'Uhumidiferoutlet',
        Name(station, 'raw', 'T_V13', {'pm25'}): 'Thumidiferoutlet', Name(station, 'raw', 'U_V13', {'pm25'}): 'Uhumidiferoutlet',
        
        Name(station, 'raw', 'T_V14'): 'Twetneph', Name(station, 'raw', 'U_V14'): 'Uwetneph',
        Name(station, 'raw', 'T_V14', {'pm10'}): 'Twetneph', Name(station, 'raw', 'U_V14', {'pm10'}): 'Uwetneph',
        Name(station, 'raw', 'T_V14', {'pm1'}): 'Twetneph', Name(station, 'raw', 'U_V14', {'pm1'}): 'Uwetneph',
        Name(station, 'raw', 'T_V14', {'pm25'}): 'Twetneph', Name(station, 'raw', 'U_V14', {'pm25'}): 'Uwetneph',

        Name(station, 'raw', 'Tu_S11'): 'Tnephinlet', Name(station, 'raw', 'Uu_S11'): 'Unephinlet',
        Name(station, 'raw', 'Tu_S11', {'pm10'}): 'Tnephinlet', Name(station, 'raw', 'Uu_S11', {'pm10'}): 'Unephinlet',
        Name(station, 'raw', 'Tu_S11', {'pm1'}): 'Tnephinlet', Name(station, 'raw', 'Uu_S11', {'pm1'}): 'Unephinlet',
        Name(station, 'raw', 'Tu_S11', {'pm25'}): 'Tnephinlet', Name(station, 'raw', 'Uu_S11', {'pm25'}): 'Unephinlet',

        Name(station, 'raw', 'T_S11'): 'Tneph', Name(station, 'raw', 'U_S11'): 'Uneph',
        Name(station, 'raw', 'T_S11', {'pm10'}): 'Tneph', Name(station, 'raw', 'U_S11', {'pm10'}): 'Uneph',
        Name(station, 'raw', 'T_S11', {'pm1'}): 'Tneph', Name(station, 'raw', 'U_S11', {'pm1'}): 'Uneph',
        Name(station, 'raw', 'T_S11', {'pm25'}): 'Tneph', Name(station, 'raw', 'U_S11', {'pm25'}): 'Uneph',

        Name(station, 'raw', 'Tu_S12'): 'Tnephinlet2', Name(station, 'raw', 'Uu_S12'): 'Unephinlet2',
        Name(station, 'raw', 'Tu_S12', {'pm10'}): 'Tnephinlet2', Name(station, 'raw', 'Uu_S12', {'pm10'}): 'Unephinlet2',
        Name(station, 'raw', 'Tu_S12', {'pm1'}): 'Tnephinlet2', Name(station, 'raw', 'Uu_S12', {'pm1'}): 'Unephinlet2',
        Name(station, 'raw', 'Tu_S12', {'pm25'}): 'Tnephinlet2', Name(station, 'raw', 'Uu_S12', {'pm25'}): 'Unephinlet2',

        Name(station, 'raw', 'T_S12'): 'Tneph2', Name(station, 'raw', 'U_S12'): 'Uneph2',
        Name(station, 'raw', 'T_S12', {'pm10'}): 'Tneph2', Name(station, 'raw', 'U_S12', {'pm10'}): 'Uneph2',
        Name(station, 'raw', 'T_S12', {'pm1'}): 'Tneph2', Name(station, 'raw', 'U_S12', {'pm1'}): 'Uneph2',
        Name(station, 'raw', 'T_S12', {'pm25'}): 'Tneph2', Name(station, 'raw', 'U_S12', {'pm25'}): 'Uneph2',
    }, send
)
station_profile_data['aerosol']['realtime']['temperature'] = {
    RealtimeTranslator.Key('T_V11'): 'Tdryneph', RealtimeTranslator.Key('U_V11'): 'Udryneph',
    RealtimeTranslator.Key('T_V11', {'pm10'}): 'Tdryneph', RealtimeTranslator.Key('U_V11', {'pm10'}): 'Udryneph',
    RealtimeTranslator.Key('T_V11', {'pm1'}): 'Tdryneph', RealtimeTranslator.Key('U_V11', {'pm1'}): 'Udryneph',
    RealtimeTranslator.Key('T_V11', {'pm25'}): 'Tdryneph', RealtimeTranslator.Key('U_V11', {'pm25'}): 'Udryneph',
    
    RealtimeTranslator.Key('T_V12'): 'Thumidiferinlet', RealtimeTranslator.Key('U_V12'): 'Uhumidiferinlet',
    RealtimeTranslator.Key('T_V12', {'pm10'}): 'Thumidiferinlet', RealtimeTranslator.Key('U_V12', {'pm10'}): 'Uhumidiferinlet',
    RealtimeTranslator.Key('T_V12', {'pm1'}): 'Thumidiferinlet', RealtimeTranslator.Key('U_V12', {'pm1'}): 'Uhumidiferinlet',
    RealtimeTranslator.Key('T_V12', {'pm25'}): 'Thumidiferinlet', RealtimeTranslator.Key('U_V12', {'pm25'}): 'Uhumidiferinlet',
    
    RealtimeTranslator.Key('T_V13'): 'Thumidiferoutlet', RealtimeTranslator.Key('U_V13'): 'Uhumidiferoutlet',
    RealtimeTranslator.Key('T_V13', {'pm10'}): 'Thumidiferoutlet', RealtimeTranslator.Key('U_V13', {'pm10'}): 'Uhumidiferoutlet',
    RealtimeTranslator.Key('T_V13', {'pm1'}): 'Thumidiferoutlet', RealtimeTranslator.Key('U_V13', {'pm1'}): 'Uhumidiferoutlet',
    RealtimeTranslator.Key('T_V13', {'pm25'}): 'Thumidiferoutlet', RealtimeTranslator.Key('U_V13', {'pm25'}): 'Uhumidiferoutlet',
    
    RealtimeTranslator.Key('T_V14'): 'Twetneph', RealtimeTranslator.Key('U_V14'): 'Uwetneph',
    RealtimeTranslator.Key('T_V14', {'pm10'}): 'Twetneph', RealtimeTranslator.Key('U_V14', {'pm10'}): 'Uwetneph',
    RealtimeTranslator.Key('T_V14', {'pm1'}): 'Twetneph', RealtimeTranslator.Key('U_V14', {'pm1'}): 'Uwetneph',
    RealtimeTranslator.Key('T_V14', {'pm25'}): 'Twetneph', RealtimeTranslator.Key('U_V14', {'pm25'}): 'Uwetneph',

    RealtimeTranslator.Key('Tu_S11'): 'Tnephinlet', RealtimeTranslator.Key('Uu_S11'): 'Unephinlet',
    RealtimeTranslator.Key('Tu_S11', {'pm10'}): 'Tnephinlet', RealtimeTranslator.Key('Uu_S11', {'pm10'}): 'Unephinlet',
    RealtimeTranslator.Key('Tu_S11', {'pm1'}): 'Tnephinlet', RealtimeTranslator.Key('Uu_S11', {'pm1'}): 'Unephinlet',
    RealtimeTranslator.Key('Tu_S11', {'pm25'}): 'Tnephinlet', RealtimeTranslator.Key('Uu_S11', {'pm25'}): 'Unephinlet',

    RealtimeTranslator.Key('T_S11'): 'Tneph', RealtimeTranslator.Key('U_S11'): 'Uneph',
    RealtimeTranslator.Key('T_S11', {'pm10'}): 'Tneph', RealtimeTranslator.Key('U_S11', {'pm10'}): 'Uneph',
    RealtimeTranslator.Key('T_S11', {'pm1'}): 'Tneph', RealtimeTranslator.Key('U_S11', {'pm1'}): 'Uneph',
    RealtimeTranslator.Key('T_S11', {'pm25'}): 'Tneph', RealtimeTranslator.Key('U_S11', {'pm25'}): 'Uneph',

    RealtimeTranslator.Key('Tu_S12'): 'Tnephinlet2', RealtimeTranslator.Key('Uu_S12'): 'Unephinlet2',
    RealtimeTranslator.Key('Tu_S12', {'pm10'}): 'Tnephinlet2', RealtimeTranslator.Key('Uu_S12', {'pm10'}): 'Unephinlet2',
    RealtimeTranslator.Key('Tu_S12', {'pm1'}): 'Tnephinlet2', RealtimeTranslator.Key('Uu_S12', {'pm1'}): 'Unephinlet2',
    RealtimeTranslator.Key('Tu_S12', {'pm25'}): 'Tnephinlet2', RealtimeTranslator.Key('Uu_S12', {'pm25'}): 'Unephinlet2',

    RealtimeTranslator.Key('T_S12'): 'Tneph2', RealtimeTranslator.Key('U_S12'): 'Uneph2',
    RealtimeTranslator.Key('T_S12', {'pm10'}): 'Tneph2', RealtimeTranslator.Key('U_S12', {'pm10'}): 'Uneph2',
    RealtimeTranslator.Key('T_S12', {'pm1'}): 'Tneph2', RealtimeTranslator.Key('U_S12', {'pm1'}): 'Uneph2',
    RealtimeTranslator.Key('T_S12', {'pm25'}): 'Tneph2', RealtimeTranslator.Key('U_S12', {'pm25'}): 'Uneph2',
}


station_profile_data['aerosol']['raw']['samplepressure-whole'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'P_S11'): 'neph',
        Name(station, 'raw', 'P_S12'): 'neph2',
    }, send
)
station_profile_data['aerosol']['raw']['samplepressure-pm10'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'P_S11', {'pm10'}): 'neph',
        Name(station, 'raw', 'P_S12', {'pm10'}): 'neph2',
    }, send
)
station_profile_data['aerosol']['raw']['samplepressure-pm25'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'P_S11', {'pm25'}): 'neph',
        Name(station, 'raw', 'P_S12', {'pm25'}): 'neph2',
    }, send
)
station_profile_data['aerosol']['raw']['samplepressure-pm1'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'P_S11', {'pm1'}): 'neph',
        Name(station, 'raw', 'P_S12', {'pm1'}): 'neph2',
    }, send
)
station_profile_data['aerosol']['realtime']['samplepressure-whole'] = {
    RealtimeTranslator.Key('P_S11'): 'neph',
    RealtimeTranslator.Key('P_S12'): 'neph2',
}
station_profile_data['aerosol']['realtime']['samplepressure-pm10'] = {
    RealtimeTranslator.Key('P_S11', {'pm10'}): 'neph',
    RealtimeTranslator.Key('P_S12', {'pm10'}): 'neph2',
}
station_profile_data['aerosol']['realtime']['samplepressure-pm25'] = {
    RealtimeTranslator.Key('P_S11', {'pm25'}): 'neph',
    RealtimeTranslator.Key('P_S12', {'pm25'}): 'neph2',
}
station_profile_data['aerosol']['realtime']['samplepressure-pm1'] = {
    RealtimeTranslator.Key('P_S11', {'pm1'}): 'neph',
    RealtimeTranslator.Key('P_S12', {'pm1'}): 'neph2',
}


station_profile_data['aerosol']['raw']['ccnstatus'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'Tu_N21'): 'Tinlet',
        Name(station, 'raw', 'T1_N21'): 'Ttec1',
        Name(station, 'raw', 'T2_N21'): 'Ttec2',
        Name(station, 'raw', 'T3_N21'): 'Ttec3',
        Name(station, 'raw', 'T4_N21'): 'Tsample',
        Name(station, 'raw', 'T5_N21'): 'Topc',
        Name(station, 'raw', 'T6_N21'): 'Tnafion',
        Name(station, 'raw', 'Q1_N21'): 'Qsample',
        Name(station, 'raw', 'Q2_N21'): 'Qsheath',
        Name(station, 'raw', 'Uc_N21'): 'SScalc',
    }, send
)
station_profile_data['aerosol']['realtime']['ccnstatus'] = {
    RealtimeTranslator.Key('Tu_N21'): 'Tinlet',
    RealtimeTranslator.Key('T1_N21'): 'Ttec1',
    RealtimeTranslator.Key('T2_N21'): 'Ttec2',
    RealtimeTranslator.Key('T3_N21'): 'Ttec3',
    RealtimeTranslator.Key('T4_N21'): 'Tsample',
    RealtimeTranslator.Key('T5_N21'): 'Topc',
    RealtimeTranslator.Key('T6_N21'): 'Tnafion',
    RealtimeTranslator.Key('Q1_N21'): 'Qsample',
    RealtimeTranslator.Key('Q2_N21'): 'Qsheath',
    RealtimeTranslator.Key('Uc_N21'): 'SScalc',
}


def get(station: str, data_name: str, start_epoch_ms: int, end_epoch_ms: int,
        send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
    return data_profile_get(station, data_name, start_epoch_ms, end_epoch_ms, send, station_profile_data)
