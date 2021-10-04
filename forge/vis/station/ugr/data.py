import typing
from ..cpd3 import DataStream, DataReader, Name, data_profile_get, detach, profile_data


station_profile_data = detach(profile_data)


station_profile_data['aerosol']['raw']['maap'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'BacR_A31'): 'Ba',
        Name(station, 'raw', 'XR_A31'): 'X',
    }, send
)
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


station_profile_data['aerosol']['raw']['aethalometer'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, dict(
        [(Name(station, 'raw', f'Ba{i+1}_A42'), f'Ba{i+1}') for i in range(7)] +
        [(Name(station, 'raw', f'X{i+1}_A42'), f'X{i+1}') for i in range(7)] +
        [(Name(station, 'raw', f'ZFACTOR{i+1}_A42'), f'CF{i+1}') for i in range(7)] +
        [(Name(station, 'raw', f'Ir{i+1}_A42'), f'Ir{i+1}') for i in range(7)]
    ), send
)
station_profile_data['aerosol']['raw']['aethalometerstatus'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'T1_A42'): 'Tcontroller',
        Name(station, 'raw', 'T2_A42'): 'Tsupply',
        Name(station, 'raw', 'T3_A42'): 'Tled',
    }, send
)
station_profile_data['aerosol']['clean']['aethalometer'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, dict(
        [(Name(station, 'clean', f'Ba{i+1}_A42'), f'Ba{i+1}') for i in range(7)] +
        [(Name(station, 'clean', f'X{i+1}_A42'), f'X{i+1}') for i in range(7)] +
        [(Name(station, 'clean', f'ZFACTOR{i+1}_A42'), f'CF{i+1}') for i in range(7)] +
        [(Name(station, 'clean', f'Ir{i+1}_A42'), f'Ir{i+1}') for i in range(7)]
    ), send
)
station_profile_data['aerosol']['avgh']['aethalometer'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, dict(
        [(Name(station, 'avgh', f'Ba{i+1}_A42'), f'Ba{i+1}') for i in range(7)] +
        [(Name(station, 'avgh', f'X{i+1}_A42'), f'X{i+1}') for i in range(7)] +
        [(Name(station, 'avgh', f'ZFACTOR{i+1}_A42'), f'CF{i+1}') for i in range(7)] +
        [(Name(station, 'avgh', f'Ir{i+1}_A42'), f'Ir{i+1}') for i in range(7)]
    ), send
)
station_profile_data['aerosol']['editing']['aethalometer'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
    start_epoch_ms, end_epoch_ms, station, 'aerosol', dict(
        [(Name(station, 'clean', f'Ba{i + 1}_A42'), f'Ba{i + 1}') for i in range(7)] +
        [(Name(station, 'clean', f'X{i + 1}_A42'), f'X{i + 1}') for i in range(7)] +
        [(Name(station, 'clean', f'ZFACTOR{i + 1}_A42'), f'CF{i + 1}') for i in range(7)] +
        [(Name(station, 'clean', f'Ir{i + 1}_A42'), f'Ir{i + 1}') for i in range(7)]
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


def get(station: str, data_name: str, start_epoch_ms: int, end_epoch_ms: int,
        send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
    return data_profile_get(station, data_name, start_epoch_ms, end_epoch_ms, send, station_profile_data)
