import typing
from ..cpd3 import DataStream, DataReader, EditedReader, ContaminationReader, EditedContaminationReader, \
    RealtimeTranslator, Name, data_profile_get, detach, profile_data


station_profile_data = detach(profile_data)

station_profile_data['aerosol']['raw']['contamination'] = lambda station, start_epoch_ms, end_epoch_ms, send: ContaminationReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'F1_N41'),
        Name(station, 'raw', 'F1_S11'),
        Name(station, 'raw', 'F1_A11'),
    }, send
)
station_profile_data['aerosol']['clean']['contamination'] = lambda station, start_epoch_ms, end_epoch_ms, send: ContaminationReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'clean', 'F1_N41'),
        Name(station, 'clean', 'F1_S11'),
        Name(station, 'clean', 'F1_A11'),
    }, send
)
station_profile_data['aerosol']['avgh']['contamination'] = lambda station, start_epoch_ms, end_epoch_ms, send: ContaminationReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'avgh', 'F1_N41'),
        Name(station, 'avgh', 'F1_S11'),
        Name(station, 'avgh', 'F1_A11'),
    }, send
)
station_profile_data['aerosol']['editing']['contamination'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedContaminationReader(
    start_epoch_ms, end_epoch_ms, station, 'aerosol', {
        Name(station, 'clean', 'F1_N41'),
        Name(station, 'clean', 'F1_S11'),
        Name(station, 'clean', 'F1_A11'),
    }, send
)

station_profile_data['aerosol']['raw']['cnc'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'N_N41'): 'cnc',
    }, send
)
station_profile_data['aerosol']['realtime']['cnc'] = {
    RealtimeTranslator.Key('N_N41'): 'cnc',
}
station_profile_data['aerosol']['clean']['cnc'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'clean', 'N_N41'): 'cnc',
    }, send
)
station_profile_data['aerosol']['avgh']['cnc'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'avgh', 'N_N41'): 'cnc',
    }, send
)
station_profile_data['aerosol']['editing']['cnc'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
    start_epoch_ms, end_epoch_ms, station, 'aerosol', {
        Name(station, 'clean', 'N_N41'): 'cnc',
    }, send
)

station_profile_data['aerosol']['raw']['aethalometer'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, dict(
        [(Name(station, 'raw', f'Ba{i+1}_A82'), f'Ba{i+1}') for i in range(7)] +
        [(Name(station, 'raw', f'X{i+1}_A82'), f'X{i+1}') for i in range(7)] +
        [(Name(station, 'raw', f'ZFACTOR{i+1}_A82'), f'CF{i+1}') for i in range(7)] +
        [(Name(station, 'raw', f'Ir{i+1}_A82'), f'Ir{i+1}') for i in range(7)]
    ), send
)
station_profile_data['aerosol']['raw']['aethalometerstatus'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'T1_A82'): 'Tcontroller',
        Name(station, 'raw', 'T2_A82'): 'Tsupply',
        Name(station, 'raw', 'T3_A82'): 'Tled',
    }, send
)
station_profile_data['aerosol']['realtime']['aethalometer'] = dict(
    [(RealtimeTranslator.Key(f'Ba{i+1}_A82'), f'Ba{i+1}') for i in range(7)] +
    [(RealtimeTranslator.Key(f'X{i+1}_A82'), f'X{i+1}') for i in range(7)] +
    [(RealtimeTranslator.Key(f'ZFACTOR{i+1}_A82'), f'CF{i+1}') for i in range(7)] +
    [(RealtimeTranslator.Key(f'Ir{i+1}_A82'), f'Ir{i+1}') for i in range(7)]
)
station_profile_data['aerosol']['realtime']['aethalometerstatus'] = {
    RealtimeTranslator.Key('T1_A82'): 'Tcontroller',
    RealtimeTranslator.Key('T2_A82'): 'Tsupply',
    RealtimeTranslator.Key('T3_A82'): 'Tled',
}
station_profile_data['aerosol']['clean']['aethalometer'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, dict(
        [(Name(station, 'clean', f'Ba{i+1}_A82'), f'Ba{i+1}') for i in range(7)] +
        [(Name(station, 'clean', f'X{i+1}_A82'), f'X{i+1}') for i in range(7)] +
        [(Name(station, 'clean', f'ZFACTOR{i+1}_A82'), f'CF{i+1}') for i in range(7)] +
        [(Name(station, 'clean', f'Ir{i+1}_A82'), f'Ir{i+1}') for i in range(7)]
    ), send
)
station_profile_data['aerosol']['avgh']['aethalometer'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, dict(
        [(Name(station, 'avgh', f'Ba{i+1}_A82'), f'Ba{i+1}') for i in range(7)] +
        [(Name(station, 'avgh', f'X{i+1}_A82'), f'X{i+1}') for i in range(7)] +
        [(Name(station, 'avgh', f'ZFACTOR{i+1}_A82'), f'CF{i+1}') for i in range(7)] +
        [(Name(station, 'avgh', f'Ir{i+1}_A82'), f'Ir{i+1}') for i in range(7)]
    ), send
)
station_profile_data['aerosol']['editing']['aethalometer'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
    start_epoch_ms, end_epoch_ms, station, 'aerosol', dict(
        [(Name(station, 'clean', f'Ba{i + 1}_A82'), f'Ba{i + 1}') for i in range(7)] +
        [(Name(station, 'clean', f'X{i + 1}_A82'), f'X{i + 1}') for i in range(7)] +
        [(Name(station, 'clean', f'ZFACTOR{i + 1}_A82'), f'CF{i + 1}') for i in range(7)] +
        [(Name(station, 'clean', f'Ir{i + 1}_A82'), f'Ir{i + 1}') for i in range(7)]
    ), send
)

station_profile_data['met']['raw']['wind'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'WS1_XM1'): 'WSambient', Name(station, 'raw', 'WD1_XM1'): 'WDambient',
        Name(station, 'raw', 'WS2_XM1'): 'WS2', Name(station, 'raw', 'WD2_XM1'): 'WD2',
        Name(station, 'raw', 'WS3_XM1'): 'WS3', Name(station, 'raw', 'WD3_XM1'): 'WD3',
        Name(station, 'raw', 'WS4_XM1'): 'WS4', Name(station, 'raw', 'WD4_XM1'): 'WD4',
    }, send
)
station_profile_data['met']['clean']['wind'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'clean', 'WS1_XM1'): 'WSambient', Name(station, 'clean', 'WD1_XM1'): 'WDambient',
        Name(station, 'clean', 'WS2_XM1'): 'WS2', Name(station, 'clean', 'WD2_XM1'): 'WD2',
        Name(station, 'clean', 'WS3_XM1'): 'WS3', Name(station, 'clean', 'WD3_XM1'): 'WD3',
        Name(station, 'clean', 'WS4_XM1'): 'WS4', Name(station, 'clean', 'WD4_XM1'): 'WD4',
    }, send
)
station_profile_data['met']['avgh']['wind'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'avgh', 'WS1_XM1'): 'WSambient', Name(station, 'avgh', 'WD1_XM1'): 'WDambient',
        Name(station, 'avgh', 'WS2_XM1'): 'WS2', Name(station, 'avgh', 'WD2_XM1'): 'WD2',
        Name(station, 'avgh', 'WS3_XM1'): 'WS3', Name(station, 'avgh', 'WD3_XM1'): 'WD3',
        Name(station, 'avgh', 'WS4_XM1'): 'WS4', Name(station, 'avgh', 'WD4_XM1'): 'WD4',
    }, send
)
station_profile_data['met']['editing']['wind'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
    start_epoch_ms, end_epoch_ms, station, 'met', {
        Name(station, 'clean', 'WS1_XM1'): 'WSambient', Name(station, 'clean', 'WD1_XM1'): 'WDambient',
        Name(station, 'clean', 'WS2_XM1'): 'WS2', Name(station, 'clean', 'WD2_XM1'): 'WD2',
        Name(station, 'clean', 'WS3_XM1'): 'WS3', Name(station, 'clean', 'WD3_XM1'): 'WD3',
        Name(station, 'clean', 'WS4_XM1'): 'WS4', Name(station, 'clean', 'WD4_XM1'): 'WD4',
    }, send
)


def get(station: str, data_name: str, start_epoch_ms: int, end_epoch_ms: int,
        send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
    return data_profile_get(station, data_name, start_epoch_ms, end_epoch_ms, send, station_profile_data)
