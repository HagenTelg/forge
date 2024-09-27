import typing
from ..cpd3 import DataStream, DataReader, EditedReader, RealtimeTranslator, Name, data_profile_get, detach, profile_data


station_profile_data = detach(profile_data)


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
        Name(station, 'raw', 'Q1_A42'): 'Q1',
        Name(station, 'raw', 'Q2_A42'): 'Q2',
    }, send
)
station_profile_data['aerosol']['realtime']['aethalometer'] = dict(
    [(RealtimeTranslator.Key(f'Ba{i+1}_A42'), f'Ba{i+1}') for i in range(7)] +
    [(RealtimeTranslator.Key(f'X{i+1}_A42'), f'X{i+1}') for i in range(7)] +
    [(RealtimeTranslator.Key(f'ZFACTOR{i+1}_A42'), f'CF{i+1}') for i in range(7)] +
    [(RealtimeTranslator.Key(f'Ir{i+1}_A42'), f'Ir{i+1}') for i in range(7)]
)
station_profile_data['aerosol']['realtime']['aethalometerstatus'] = {
    RealtimeTranslator.Key('T1_A42'): 'Tcontroller',
    RealtimeTranslator.Key('T2_A42'): 'Tsupply',
    RealtimeTranslator.Key('T3_A42'): 'Tled',
    RealtimeTranslator.Key('Q1_A42'): 'Q1',
    RealtimeTranslator.Key('Q2_A42'): 'Q2',
}
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


station_profile_data['aerosol']['raw']['maap'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'BacR_A21'): 'Ba',
        Name(station, 'raw', 'XR_A21'): 'X',
    }, send
)
station_profile_data['aerosol']['realtime']['maap'] = {
    RealtimeTranslator.Key('BacR_A21'): 'Ba',
    RealtimeTranslator.Key('XR_A21'): 'X',
}
station_profile_data['aerosol']['editing']['maap'] =lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
    start_epoch_ms, end_epoch_ms, station, 'aerosol', {
        Name(station, 'clean', 'BacR_A21'): 'Ba',
        Name(station, 'clean', 'XR_A21'): 'X',
    }, send
)
station_profile_data['aerosol']['clean']['maap'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'clean', 'BacR_A21'): 'Ba',
        Name(station, 'clean', 'XR_A21'): 'X',
    }, send
)
station_profile_data['aerosol']['avgh']['maap'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'avgh', 'BacR_A21'): 'Ba',
        Name(station, 'avgh', 'XR_A21'): 'X',
    }, send
)

station_profile_data['aerosol']['raw']['maapstatus'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'P_A21'): 'Psample',
        Name(station, 'raw', 'T1_A21'): 'Tambient',
        Name(station, 'raw', 'T2_A21'): 'Tmeasurementhead',
        Name(station, 'raw', 'T3_A21'): 'Tsystem',
        Name(station, 'raw', 'Q_A21'): 'Qsample',
        Name(station, 'raw', 'IrR_A21'): 'Ir',
        Name(station, 'raw', 'IfR_A21'): 'If',
        Name(station, 'raw', 'IpR_A21'): 'Ip',
        Name(station, 'raw', 'Is1_A21'): 'Is1',
        Name(station, 'raw', 'Is2_A21'): 'Is2',
        Name(station, 'raw', 'Is1R_A21'): 'Is1',
        Name(station, 'raw', 'Is2R_A21'): 'Is2',
    }, send
)
station_profile_data['aerosol']['realtime']['maapstatus'] = {
    RealtimeTranslator.Key('P_A21'): 'Psample',
    RealtimeTranslator.Key('T1_A21'): 'Tambient',
    RealtimeTranslator.Key('T2_A21'): 'Tmeasurementhead',
    RealtimeTranslator.Key('T3_A21'): 'Tsystem',
    RealtimeTranslator.Key('Q_A21'): 'Qsample',
    RealtimeTranslator.Key('IrR_A21'): 'Ir',
    RealtimeTranslator.Key('IfR_A21'): 'If',
    RealtimeTranslator.Key('IpR_A21'): 'Ip',
    RealtimeTranslator.Key('Is1_A21'): 'Is1',
    RealtimeTranslator.Key('Is2_A21'): 'Is2',
    RealtimeTranslator.Key('Is1R_A21'): 'Is1',
    RealtimeTranslator.Key('Is2R_A21'): 'Is2',
}


def get(station: str, data_name: str, start_epoch_ms: int, end_epoch_ms: int,
        send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
    return data_profile_get(station, data_name, start_epoch_ms, end_epoch_ms, send, station_profile_data)
