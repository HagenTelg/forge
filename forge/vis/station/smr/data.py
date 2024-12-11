import typing
from ..cpd3 import DataStream, DataReader, EditedReader, RealtimeTranslator, Name, data_profile_get, detach, profile_data


station_profile_data = detach(profile_data)


station_profile_data['aerosol']['raw']['maap-whole'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'BacR_A21'): 'Ba',
        Name(station, 'raw', 'XR_A21'): 'X',
    }, send
)
station_profile_data['aerosol']['raw']['maap-pm10'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'BacR_A21', {'pm10'}): 'Ba',
        Name(station, 'raw', 'XR_A21', {'pm10'}): 'X',
    }, send
)
station_profile_data['aerosol']['raw']['maap-pm25'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'BacR_A21', {'pm25'}): 'Ba',
        Name(station, 'raw', 'XR_A21', {'pm25'}): 'X',
    }, send
)
station_profile_data['aerosol']['raw']['maap-pm1'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'BacR_A21', {'pm1'}): 'Ba',
        Name(station, 'raw', 'XR_A21', {'pm1'}): 'X',
    }, send
)
station_profile_data['aerosol']['realtime']['maap-whole'] = {
    RealtimeTranslator.Key('BacR_A21'): 'Ba',
    RealtimeTranslator.Key('XR_A21'): 'X',
}
station_profile_data['aerosol']['realtime']['maap-pm10'] ={
    RealtimeTranslator.Key('BacR_A21', {'pm10'}): 'Ba',
    RealtimeTranslator.Key('XR_A21', {'pm10'}): 'X',
}
station_profile_data['aerosol']['realtime']['maap-pm25'] = {
    RealtimeTranslator.Key('BacR_A21', {'pm25'}): 'Ba',
    RealtimeTranslator.Key('XR_A21', {'pm25'}): 'X',
}
station_profile_data['aerosol']['realtime']['maap-pm1'] = {
    RealtimeTranslator.Key('BacR_A21', {'pm1'}): 'Ba',
    RealtimeTranslator.Key('XR_A21', {'pm1'}): 'X',
}
station_profile_data['aerosol']['editing']['maap-whole'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
    start_epoch_ms, end_epoch_ms, station, 'aerosol', {
        Name(station, 'clean', 'BacR_A21'): 'Ba',
        Name(station, 'clean', 'XR_A21'): 'X',
    }, send
)
station_profile_data['aerosol']['editing']['maap-pm10'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
    start_epoch_ms, end_epoch_ms, station, 'aerosol', {
        Name(station, 'clean', 'BacR_A21', {'pm10'}): 'Ba',
        Name(station, 'clean', 'XR_A21', {'pm10'}): 'X',
    }, send
)
station_profile_data['aerosol']['editing']['maap-pm25'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
    start_epoch_ms, end_epoch_ms, station, 'aerosol', {
        Name(station, 'clean', 'BacR_A21', {'pm25'}): 'Ba',
        Name(station, 'clean', 'XR_A21', {'pm25'}): 'X',
    }, send
)
station_profile_data['aerosol']['editing']['maap-pm1'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
    start_epoch_ms, end_epoch_ms, station, 'aerosol', {
        Name(station, 'clean', 'BacR_A21', {'pm1'}): 'Ba',
        Name(station, 'clean', 'XR_A21', {'pm1'}): 'X',
    }, send
)
station_profile_data['aerosol']['clean']['maap-whole'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'clean', 'BacR_A21'): 'Ba',
        Name(station, 'clean', 'XR_A21'): 'X',
    }, send
)
station_profile_data['aerosol']['clean']['maap-pm10'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'clean', 'BacR_A21', {'pm10'}): 'Ba',
        Name(station, 'clean', 'XR_A21', {'pm10'}): 'X',
    }, send
)
station_profile_data['aerosol']['clean']['maap-pm25'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'clean', 'BacR_A21', {'pm25'}): 'Ba',
        Name(station, 'clean', 'XR_A21', {'pm25'}): 'X',
    }, send
)
station_profile_data['aerosol']['clean']['maap-pm1'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'clean', 'BacR_A21', {'pm1'}): 'Ba',
        Name(station, 'clean', 'XR_A21', {'pm1'}): 'X',
    }, send
)
station_profile_data['aerosol']['avgh']['maap-whole'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'avgh', 'BacR_A21'): 'Ba',
        Name(station, 'avgh', 'XR_A21'): 'X',
    }, send
)
station_profile_data['aerosol']['avgh']['maap-pm10'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'avgh', 'BacR_A21', {'pm10'}): 'Ba',
        Name(station, 'avgh', 'XR_A21', {'pm10'}): 'X',
    }, send
)
station_profile_data['aerosol']['avgh']['maap-pm25'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'avgh', 'BacR_A21', {'pm25'}): 'Ba',
        Name(station, 'avgh', 'XR_A21', {'pm25'}): 'X',
    }, send
)
station_profile_data['aerosol']['avgh']['maap-pm1'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'avgh', 'BacR_A21', {'pm1'}): 'Ba',
        Name(station, 'avgh', 'XR_A21', {'pm1'}): 'X',
    }, send
)

station_profile_data['aerosol']['raw']['maapstatus'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'P_A21'): 'Psample',
        Name(station, 'raw', 'P_A21', {'pm10'}): 'Psample',
        Name(station, 'raw', 'P_A21', {'pm1'}): 'Psample',
        Name(station, 'raw', 'P_A21', {'pm25'}): 'Psample',

        Name(station, 'raw', 'T1_A21'): 'Tambient',
        Name(station, 'raw', 'T1_A21', {'pm10'}): 'Tambient',
        Name(station, 'raw', 'T1_A21', {'pm1'}): 'Tambient',
        Name(station, 'raw', 'T1_A21', {'pm25'}): 'Tambient',
        Name(station, 'raw', 'T2_A21'): 'Tmeasurementhead',
        Name(station, 'raw', 'T2_A21', {'pm10'}): 'Tmeasurementhead',
        Name(station, 'raw', 'T2_A21', {'pm1'}): 'Tmeasurementhead',
        Name(station, 'raw', 'T2_A21', {'pm25'}): 'Tmeasurementhead',
        Name(station, 'raw', 'T3_A21'): 'Tsystem',
        Name(station, 'raw', 'T3_A21', {'pm10'}): 'Tsystem',
        Name(station, 'raw', 'T3_A21', {'pm1'}): 'Tsystem',
        Name(station, 'raw', 'T3_A21', {'pm25'}): 'Tsystem',

        Name(station, 'raw', 'Q_A21'): 'Qsample',
        Name(station, 'raw', 'Q_A21', {'pm10'}): 'Qsample',
        Name(station, 'raw', 'Q_A21', {'pm1'}): 'Qsample',
        Name(station, 'raw', 'Q_A21', {'pm25'}): 'Qsample',

        Name(station, 'raw', 'IrR_A21'): 'Ir',
        Name(station, 'raw', 'IrR_A21', {'pm10'}): 'Ir',
        Name(station, 'raw', 'IrR_A21', {'pm1'}): 'Ir',
        Name(station, 'raw', 'IrR_A21', {'pm25'}): 'Ir',
        Name(station, 'raw', 'IfR_A21'): 'If',
        Name(station, 'raw', 'IfR_A21', {'pm10'}): 'If',
        Name(station, 'raw', 'IfR_A21', {'pm1'}): 'If',
        Name(station, 'raw', 'IfR_A21', {'pm25'}): 'If',
        Name(station, 'raw', 'IpR_A21'): 'Ip',
        Name(station, 'raw', 'IpR_A21', {'pm10'}): 'Ip',
        Name(station, 'raw', 'IpR_A21', {'pm1'}): 'Ip',
        Name(station, 'raw', 'IpR_A21', {'pm25'}): 'Ip',
        Name(station, 'raw', 'Is1_A21'): 'Is1',
        Name(station, 'raw', 'Is1_A21', {'pm10'}): 'Is1',
        Name(station, 'raw', 'Is1_A21', {'pm1'}): 'Is1',
        Name(station, 'raw', 'Is1_A21', {'pm25'}): 'Is1',
        Name(station, 'raw', 'Is2_A21'): 'Is2',
        Name(station, 'raw', 'Is2_A21', {'pm10'}): 'Is2',
        Name(station, 'raw', 'Is2_A21', {'pm1'}): 'Is2',
        Name(station, 'raw', 'Is2_A21', {'pm25'}): 'Is2',
        Name(station, 'raw', 'Is1R_A21'): 'Is1',
        Name(station, 'raw', 'Is1R_A21', {'pm10'}): 'Is1',
        Name(station, 'raw', 'Is1R_A21', {'pm1'}): 'Is1',
        Name(station, 'raw', 'Is1R_A21', {'pm25'}): 'Is1',
        Name(station, 'raw', 'Is2R_A21'): 'Is2',
        Name(station, 'raw', 'Is2R_A21', {'pm10'}): 'Is2',
        Name(station, 'raw', 'Is2R_A21', {'pm1'}): 'Is2',
        Name(station, 'raw', 'Is2R_A21', {'pm25'}): 'Is2',
    }, send
)
station_profile_data['aerosol']['realtime']['maapstatus'] = {
    RealtimeTranslator.Key('P_A21'): 'Psample',
    RealtimeTranslator.Key('P_A21', {'pm10'}): 'Psample',
    RealtimeTranslator.Key('P_A21', {'pm1'}): 'Psample',
    RealtimeTranslator.Key('P_A21', {'pm25'}): 'Psample',

    RealtimeTranslator.Key('T1_A21'): 'Tambient',
    RealtimeTranslator.Key('T1_A21', {'pm10'}): 'Tambient',
    RealtimeTranslator.Key('T1_A21', {'pm1'}): 'Tambient',
    RealtimeTranslator.Key('T1_A21', {'pm25'}): 'Tambient',
    RealtimeTranslator.Key('T2_A21'): 'Tmeasurementhead',
    RealtimeTranslator.Key('T2_A21', {'pm10'}): 'Tmeasurementhead',
    RealtimeTranslator.Key('T2_A21', {'pm1'}): 'Tmeasurementhead',
    RealtimeTranslator.Key('T2_A21', {'pm25'}): 'Tmeasurementhead',
    RealtimeTranslator.Key('T3_A21'): 'Tsystem',
    RealtimeTranslator.Key('T3_A21', {'pm10'}): 'Tsystem',
    RealtimeTranslator.Key('T3_A21', {'pm1'}): 'Tsystem',
    RealtimeTranslator.Key('T3_A21', {'pm25'}): 'Tsystem',

    RealtimeTranslator.Key('Q_A21'): 'Qsample',
    RealtimeTranslator.Key('Q_A21', {'pm10'}): 'Qsample',
    RealtimeTranslator.Key('Q_A21', {'pm1'}): 'Qsample',
    RealtimeTranslator.Key('Q_A21', {'pm25'}): 'Qsample',

    RealtimeTranslator.Key('IrR_A21'): 'Ir',
    RealtimeTranslator.Key('IrR_A21', {'pm10'}): 'Ir',
    RealtimeTranslator.Key('IrR_A21', {'pm1'}): 'Ir',
    RealtimeTranslator.Key('IrR_A21', {'pm25'}): 'Ir',
    RealtimeTranslator.Key('IfR_A21'): 'If',
    RealtimeTranslator.Key('IfR_A21', {'pm10'}): 'If',
    RealtimeTranslator.Key('IfR_A21', {'pm1'}): 'If',
    RealtimeTranslator.Key('IfR_A21', {'pm25'}): 'If',
    RealtimeTranslator.Key('IpR_A21'): 'Ip',
    RealtimeTranslator.Key('IpR_A21', {'pm10'}): 'Ip',
    RealtimeTranslator.Key('IpR_A21', {'pm1'}): 'Ip',
    RealtimeTranslator.Key('IpR_A21', {'pm25'}): 'Ip',
    RealtimeTranslator.Key('Is1_A21'): 'Is1',
    RealtimeTranslator.Key('Is1_A21', {'pm10'}): 'Is1',
    RealtimeTranslator.Key('Is1_A21', {'pm1'}): 'Is1',
    RealtimeTranslator.Key('Is1_A21', {'pm25'}): 'Is1',
    RealtimeTranslator.Key('Is2_A21'): 'Is2',
    RealtimeTranslator.Key('Is2_A21', {'pm10'}): 'Is2',
    RealtimeTranslator.Key('Is2_A21', {'pm1'}): 'Is2',
    RealtimeTranslator.Key('Is2_A21', {'pm25'}): 'Is2',
    RealtimeTranslator.Key('Is1R_A21'): 'Is1',
    RealtimeTranslator.Key('Is1R_A21', {'pm10'}): 'Is1',
    RealtimeTranslator.Key('Is1R_A21', {'pm1'}): 'Is1',
    RealtimeTranslator.Key('Is1R_A21', {'pm25'}): 'Is1',
    RealtimeTranslator.Key('Is2R_A21'): 'Is2',
    RealtimeTranslator.Key('Is2R_A21', {'pm10'}): 'Is2',
    RealtimeTranslator.Key('Is2R_A21', {'pm1'}): 'Is2',
    RealtimeTranslator.Key('Is2R_A21', {'pm25'}): 'Is2',
}


station_profile_data['aerosol']['raw']['aethalometer-whole'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, dict(
        [(Name(station, 'raw', f'Ir{i+1}_A81'), f'Ir{i+1}') for i in range(7)] +
        [(Name(station, 'raw', f'Bac{i+1}_A81'), f'Ba{i+1}') for i in range(7)] +
        [(Name(station, 'raw', f'X{i+1}_A81'), f'X{i+1}') for i in range(7)]
    ), send
)
station_profile_data['aerosol']['raw']['aethalometer-pm10'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, dict(
        [(Name(station, 'raw', f'Ir{i+1}_A81', {'pm10'}), f'Ir{i+1}') for i in range(7)] +
        [(Name(station, 'raw', f'Bac{i+1}_A81', {'pm10'}), f'Ba{i+1}') for i in range(7)] +
        [(Name(station, 'raw', f'X{i+1}_A81', {'pm10'}), f'X{i+1}') for i in range(7)]
    ), send
)
station_profile_data['aerosol']['raw']['aethalometer-pm25'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, dict(
        [(Name(station, 'raw', f'Ir{i+1}_A81', {'pm25'}), f'Ir{i+1}') for i in range(7)] +
        [(Name(station, 'raw', f'Bac{i+1}_A81', {'pm25'}), f'Ba{i+1}') for i in range(7)] +
        [(Name(station, 'raw', f'X{i+1}_A81', {'pm25'}), f'X{i+1}') for i in range(7)]
    ), send
)
station_profile_data['aerosol']['raw']['aethalometer-pm1'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, dict(
        [(Name(station, 'raw', f'Ir{i+1}_A81', {'pm1'}), f'Ir{i+1}') for i in range(7)] +
        [(Name(station, 'raw', f'Bac{i+1}_A81', {'pm1'}), f'Ba{i+1}') for i in range(7)] +
        [(Name(station, 'raw', f'X{i+1}_A81', {'pm1'}), f'X{i+1}') for i in range(7)]
    ), send
)
station_profile_data['aerosol']['realtime']['aethalometer-whole'] = dict(
    [(RealtimeTranslator.Key(f'Ir{i+1}_A81'), f'Ir{i+1}') for i in range(7)] +
    [(RealtimeTranslator.Key(f'Bac{i+1}_A81'), f'Ba{i+1}') for i in range(7)] +
    [(RealtimeTranslator.Key(f'X{i+1}_A81'), f'X{i+1}') for i in range(7)]
)
station_profile_data['aerosol']['realtime']['aethalometer-pm10'] = dict(
    [(RealtimeTranslator.Key(f'Ir{i+1}_A81', {'pm10'}), f'Ir{i+1}') for i in range(7)] +
    [(RealtimeTranslator.Key(f'Bac{i+1}_A81', {'pm10'}), f'Ba{i+1}') for i in range(7)] +
    [(RealtimeTranslator.Key(f'X{i+1}_A81', {'pm10'}), f'X{i+1}') for i in range(7)]
)
station_profile_data['aerosol']['realtime']['aethalometer-pm25'] = dict(
    [(RealtimeTranslator.Key(f'Ir{i+1}_A81', {'pm25'}), f'Ir{i+1}') for i in range(7)] +
    [(RealtimeTranslator.Key(f'Bac{i+1}_A81', {'pm25'}), f'Ba{i+1}') for i in range(7)] +
    [(RealtimeTranslator.Key(f'X{i+1}_A81', {'pm25'}), f'X{i+1}') for i in range(7)]
)
station_profile_data['aerosol']['realtime']['aethalometer-pm1'] = dict(
    [(RealtimeTranslator.Key(f'Ir{i+1}_A81', {'pm1'}), f'Ir{i+1}') for i in range(7)] +
    [(RealtimeTranslator.Key(f'Bac{i+1}_A81', {'pm1'}), f'Ba{i+1}') for i in range(7)] +
    [(RealtimeTranslator.Key(f'X{i+1}_A81', {'pm1'}), f'X{i+1}') for i in range(7)]
)
station_profile_data['aerosol']['editing']['aethalometer-whole'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
    start_epoch_ms, end_epoch_ms, station, 'aerosol', dict(
        [(Name(station, 'clean', f'Ir{i+1}_A81'), f'Ir{i+1}') for i in range(7)] +
        [(Name(station, 'clean', f'Bac{i+1}_A81'), f'Ba{i+1}') for i in range(7)] +
        [(Name(station, 'clean', f'X{i+1}_A81'), f'X{i+1}') for i in range(7)]
    ), send
)
station_profile_data['aerosol']['editing']['aethalometer-pm10'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
    start_epoch_ms, end_epoch_ms, station, 'aerosol', dict(
        [(Name(station, 'clean', f'Ir{i+1}_A81', {'pm10'}), f'Ir{i+1}') for i in range(7)] +
        [(Name(station, 'clean', f'Bac{i+1}_A81', {'pm10'}), f'Ba{i+1}') for i in range(7)] +
        [(Name(station, 'clean', f'X{i+1}_A81', {'pm10'}), f'X{i+1}') for i in range(7)]
    ), send
)
station_profile_data['aerosol']['editing']['aethalometer-pm25'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
    start_epoch_ms, end_epoch_ms, station, 'aerosol', dict(
        [(Name(station, 'clean', f'Ir{i+1}_A81', {'pm25'}), f'Ir{i+1}') for i in range(7)] +
        [(Name(station, 'clean', f'Bac{i+1}_A81', {'pm25'}), f'Ba{i+1}') for i in range(7)] +
        [(Name(station, 'clean', f'X{i+1}_A81', {'pm25'}), f'X{i+1}') for i in range(7)]
    ), send
)
station_profile_data['aerosol']['editing']['aethalometer-pm1'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
    start_epoch_ms, end_epoch_ms, station, 'aerosol', dict(
        [(Name(station, 'clean', f'Ir{i+1}_A81', {'pm1'}), f'Ir{i+1}') for i in range(7)] +
        [(Name(station, 'clean', f'Bac{i+1}_A81', {'pm1'}), f'Ba{i+1}') for i in range(7)] +
        [(Name(station, 'clean', f'X{i+1}_A81', {'pm1'}), f'X{i+1}') for i in range(7)]
    ), send
)
station_profile_data['aerosol']['clean']['aethalometer-whole'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, dict(
        [(Name(station, 'clean', f'Ir{i+1}_A81'), f'Ir{i+1}') for i in range(7)] +
        [(Name(station, 'clean', f'Bac{i+1}_A81'), f'Ba{i+1}') for i in range(7)] +
        [(Name(station, 'clean', f'X{i+1}_A81'), f'X{i+1}') for i in range(7)]
    ), send
)
station_profile_data['aerosol']['clean']['aethalometer-pm10'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, dict(
        [(Name(station, 'clean', f'Ir{i+1}_A81', {'pm10'}), f'Ir{i+1}') for i in range(7)] +
        [(Name(station, 'clean', f'Bac{i+1}_A81', {'pm10'}), f'Ba{i+1}') for i in range(7)] +
        [(Name(station, 'clean', f'X{i+1}_A81', {'pm10'}), f'X{i+1}') for i in range(7)]
    ), send
)
station_profile_data['aerosol']['clean']['aethalometer-pm25'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, dict(
        [(Name(station, 'clean', f'Ir{i+1}_A81', {'pm25'}), f'Ir{i+1}') for i in range(7)] +
        [(Name(station, 'clean', f'Bac{i+1}_A81', {'pm25'}), f'Ba{i+1}') for i in range(7)] +
        [(Name(station, 'clean', f'X{i+1}_A81', {'pm25'}), f'X{i+1}') for i in range(7)]
    ), send
)
station_profile_data['aerosol']['clean']['aethalometer-pm1'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, dict(
        [(Name(station, 'clean', f'Ir{i+1}_A81', {'pm1'}), f'Ir{i+1}') for i in range(7)] +
        [(Name(station, 'clean', f'Bac{i+1}_A81', {'pm1'}), f'Ba{i+1}') for i in range(7)] +
        [(Name(station, 'clean', f'X{i+1}_A81', {'pm1'}), f'X{i+1}') for i in range(7)]
    ), send
)
station_profile_data['aerosol']['avgh']['aethalometer-whole'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, dict(
        [(Name(station, 'avgh', f'Ir{i+1}_A81'), f'Ir{i+1}') for i in range(7)] +
        [(Name(station, 'avgh', f'Bac{i+1}_A81'), f'Ba{i+1}') for i in range(7)] +
        [(Name(station, 'avgh', f'X{i+1}_A81'), f'X{i+1}') for i in range(7)]
    ), send
)
station_profile_data['aerosol']['avgh']['aethalometer-pm10'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, dict(
        [(Name(station, 'avgh', f'Ir{i+1}_A81', {'pm10'}), f'Ir{i+1}') for i in range(7)] +
        [(Name(station, 'avgh', f'Bac{i+1}_A81', {'pm10'}), f'Ba{i+1}') for i in range(7)] +
        [(Name(station, 'avgh', f'X{i+1}_A81', {'pm10'}), f'X{i+1}') for i in range(7)]
    ), send
)
station_profile_data['aerosol']['avgh']['aethalometer-pm25'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, dict(
        [(Name(station, 'avgh', f'Ir{i+1}_A81', {'pm25'}), f'Ir{i+1}') for i in range(7)] +
        [(Name(station, 'avgh', f'Bac{i+1}_A81', {'pm25'}), f'Ba{i+1}') for i in range(7)] +
        [(Name(station, 'avgh', f'X{i+1}_A81', {'pm25'}), f'X{i+1}') for i in range(7)]
    ), send
)
station_profile_data['aerosol']['avgh']['aethalometer-pm1'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, dict(
        [(Name(station, 'avgh', f'Ir{i+1}_A81', {'pm1'}), f'Ir{i+1}') for i in range(7)] +
        [(Name(station, 'avgh', f'Bac{i+1}_A81', {'pm1'}), f'Ba{i+1}') for i in range(7)] +
        [(Name(station, 'avgh', f'X{i+1}_A81', {'pm1'}), f'X{i+1}') for i in range(7)]
    ), send
)

station_profile_data['aerosol']['raw']['aethalometerstatus'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, dict(
        [(Name(station, 'raw', 'T1_A81'), 'Tcontroller'),
         (Name(station, 'raw', 'T1_A81', {'pm10'}), 'Tcontroller'),
         (Name(station, 'raw', 'T1_A81', {'pm25'}), 'Tcontroller'),
         (Name(station, 'raw', 'T1_A81', {'pm1'}), 'Tcontroller'),
         (Name(station, 'raw', 'T2_A81'), 'Tsupply'),
         (Name(station, 'raw', 'T2_A81', {'pm10'}), 'Tsupply'),
         (Name(station, 'raw', 'T2_A81', {'pm25'}), 'Tsupply'),
         (Name(station, 'raw', 'T2_A81', {'pm1'}), 'Tsupply'),
         (Name(station, 'raw', 'T3_A81'), 'Tled'),
         (Name(station, 'raw', 'T3_A81', {'pm10'}), 'Tled'),
         (Name(station, 'raw', 'T3_A81', {'pm25'}), 'Tled'),
         (Name(station, 'raw', 'T3_A81', {'pm1'}), 'Tled')] +
        [(Name(station, 'raw', f'Ir{i+1}_A81'), f'Ir{i+1}') for i in range(7)] +
        [(Name(station, 'raw', f'Ir{i+1}_A81', {'pm10'}), f'Ir{i+1}') for i in range(7)] +
        [(Name(station, 'raw', f'Ir{i+1}_A81', {'pm25'}), f'Ir{i+1}') for i in range(7)] +
        [(Name(station, 'raw', f'Ir{i+1}_A81', {'pm1'}), f'Ir{i+1}') for i in range(7)] +
        [(Name(station, 'raw', f'ZFACTOR{i+1}_A81'), f'CF{i+1}') for i in range(7)] +
        [(Name(station, 'raw', f'ZFACTOR{i+1}_A81', {'pm10'}), f'CF{i+1}') for i in range(7)] +
        [(Name(station, 'raw', f'ZFACTOR{i+1}_A81', {'pm25'}), f'CF{i+1}') for i in range(7)] +
        [(Name(station, 'raw', f'ZFACTOR{i+1}_A81', {'pm1'}), f'CF{i+1}') for i in range(7)]
    ), send
)
station_profile_data['aerosol']['realtime']['aethalometerstatus'] = dict(
    [(RealtimeTranslator.Key('T1_A81'), 'Tcontroller'),
     (RealtimeTranslator.Key('T1_A81', {'pm10'}), 'Tcontroller'),
     (RealtimeTranslator.Key('T1_A81', {'pm25'}), 'Tcontroller'),
     (RealtimeTranslator.Key('T1_A81', {'pm1'}), 'Tcontroller'),
     (RealtimeTranslator.Key('T2_A81'), 'Tsupply'),
     (RealtimeTranslator.Key('T2_A81', {'pm10'}), 'Tsupply'),
     (RealtimeTranslator.Key('T2_A81', {'pm25'}), 'Tsupply'),
     (RealtimeTranslator.Key('T2_A81', {'pm1'}), 'Tsupply'),
     (RealtimeTranslator.Key('T3_A81'), 'Tled'),
     (RealtimeTranslator.Key('T3_A81', {'pm10'}), 'Tled'),
     (RealtimeTranslator.Key('T3_A81', {'pm25'}), 'Tled'),
     (RealtimeTranslator.Key('T3_A81', {'pm1'}), 'Tled')] +
    [(RealtimeTranslator.Key(f'Ir{i+1}_A81'), f'Ir{i+1}') for i in range(7)] +
    [(RealtimeTranslator.Key(f'Ir{i+1}_A81', {'pm10'}), f'Ir{i+1}') for i in range(7)] +
    [(RealtimeTranslator.Key(f'Ir{i+1}_A81', {'pm25'}), f'Ir{i+1}') for i in range(7)] +
    [(RealtimeTranslator.Key(f'Ir{i+1}_A81', {'pm1'}), f'Ir{i+1}') for i in range(7)] +
    [(RealtimeTranslator.Key(f'ZFACTOR{i+1}_A81'), f'CF{i+1}') for i in range(7)] +
    [(RealtimeTranslator.Key(f'ZFACTOR{i+1}_A81', {'pm10'}), f'CF{i+1}') for i in range(7)] +
    [(RealtimeTranslator.Key(f'ZFACTOR{i+1}_A81', {'pm25'}), f'CF{i+1}') for i in range(7)] +
    [(RealtimeTranslator.Key(f'ZFACTOR{i+1}_A81', {'pm1'}), f'CF{i+1}') for i in range(7)]
)
station_profile_data['aerosol']['editing']['aethalometerstatus'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
    start_epoch_ms, end_epoch_ms, station, 'aerosol', dict(
        [(Name(station, 'clean', 'T1_A81'), 'Tcontroller'),
         (Name(station, 'clean', 'T1_A81', {'pm10'}), 'Tcontroller'),
         (Name(station, 'clean', 'T1_A81', {'pm25'}), 'Tcontroller'),
         (Name(station, 'clean', 'T1_A81', {'pm1'}), 'Tcontroller'),
         (Name(station, 'clean', 'T2_A81'), 'Tsupply'),
         (Name(station, 'clean', 'T2_A81', {'pm10'}), 'Tsupply'),
         (Name(station, 'clean', 'T2_A81', {'pm25'}), 'Tsupply'),
         (Name(station, 'clean', 'T2_A81', {'pm1'}), 'Tsupply'),
         (Name(station, 'clean', 'T3_A81'), 'Tled'),
         (Name(station, 'clean', 'T3_A81', {'pm10'}), 'Tled'),
         (Name(station, 'clean', 'T3_A81', {'pm25'}), 'Tled'),
         (Name(station, 'clean', 'T3_A81', {'pm1'}), 'Tled')] +
        [(Name(station, 'clean', f'Ir{i+1}_A81'), f'Ir{i+1}') for i in range(7)] +
        [(Name(station, 'clean', f'Ir{i+1}_A81', {'pm10'}), f'Ir{i+1}') for i in range(7)] +
        [(Name(station, 'clean', f'Ir{i+1}_A81', {'pm25'}), f'Ir{i+1}') for i in range(7)] +
        [(Name(station, 'clean', f'Ir{i+1}_A81', {'pm1'}), f'Ir{i+1}') for i in range(7)] +
        [(Name(station, 'clean', f'ZFACTOR{i+1}_A81'), f'CF{i+1}') for i in range(7)] +
        [(Name(station, 'clean', f'ZFACTOR{i+1}_A81', {'pm10'}), f'CF{i+1}') for i in range(7)] +
        [(Name(station, 'clean', f'ZFACTOR{i+1}_A81', {'pm25'}), f'CF{i+1}') for i in range(7)] +
        [(Name(station, 'clean', f'ZFACTOR{i+1}_A81', {'pm1'}), f'CF{i+1}') for i in range(7)]
    ), send
)
station_profile_data['aerosol']['clean']['aethalometerstatus'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, dict(
        [(Name(station, 'clean', 'T1_A81'), 'Tcontroller'),
         (Name(station, 'clean', 'T1_A81', {'pm10'}), 'Tcontroller'),
         (Name(station, 'clean', 'T1_A81', {'pm25'}), 'Tcontroller'),
         (Name(station, 'clean', 'T1_A81', {'pm1'}), 'Tcontroller'),
         (Name(station, 'clean', 'T2_A81'), 'Tsupply'),
         (Name(station, 'clean', 'T2_A81', {'pm10'}), 'Tsupply'),
         (Name(station, 'clean', 'T2_A81', {'pm25'}), 'Tsupply'),
         (Name(station, 'clean', 'T2_A81', {'pm1'}), 'Tsupply'),
         (Name(station, 'clean', 'T3_A81'), 'Tled'),
         (Name(station, 'clean', 'T3_A81', {'pm10'}), 'Tled'),
         (Name(station, 'clean', 'T3_A81', {'pm25'}), 'Tled'),
         (Name(station, 'clean', 'T3_A81', {'pm1'}), 'Tled')] +
        [(Name(station, 'clean', f'Ir{i+1}_A81'), f'Ir{i+1}') for i in range(7)] +
        [(Name(station, 'clean', f'Ir{i+1}_A81', {'pm10'}), f'Ir{i+1}') for i in range(7)] +
        [(Name(station, 'clean', f'Ir{i+1}_A81', {'pm25'}), f'Ir{i+1}') for i in range(7)] +
        [(Name(station, 'clean', f'Ir{i+1}_A81', {'pm1'}), f'Ir{i+1}') for i in range(7)] +
        [(Name(station, 'clean', f'ZFACTOR{i+1}_A81'), f'CF{i+1}') for i in range(7)] +
        [(Name(station, 'clean', f'ZFACTOR{i+1}_A81', {'pm10'}), f'CF{i+1}') for i in range(7)] +
        [(Name(station, 'clean', f'ZFACTOR{i+1}_A81', {'pm25'}), f'CF{i+1}') for i in range(7)] +
        [(Name(station, 'clean', f'ZFACTOR{i+1}_A81', {'pm1'}), f'CF{i+1}') for i in range(7)]
    ), send
)
station_profile_data['aerosol']['avgh']['aethalometerstatus'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, dict(
        [(Name(station, 'avgh', 'T1_A81'), 'Tcontroller'),
         (Name(station, 'avgh', 'T1_A81', {'pm10'}), 'Tcontroller'),
         (Name(station, 'avgh', 'T1_A81', {'pm25'}), 'Tcontroller'),
         (Name(station, 'avgh', 'T1_A81', {'pm1'}), 'Tcontroller'),
         (Name(station, 'avgh', 'T2_A81'), 'Tsupply'),
         (Name(station, 'avgh', 'T2_A81', {'pm10'}), 'Tsupply'),
         (Name(station, 'avgh', 'T2_A81', {'pm25'}), 'Tsupply'),
         (Name(station, 'avgh', 'T2_A81', {'pm1'}), 'Tsupply'),
         (Name(station, 'avgh', 'T3_A81'), 'Tled'),
         (Name(station, 'avgh', 'T3_A81', {'pm10'}), 'Tled'),
         (Name(station, 'avgh', 'T3_A81', {'pm25'}), 'Tled'),
         (Name(station, 'avgh', 'T3_A81', {'pm1'}), 'Tled')] +
        [(Name(station, 'avgh', f'Ir{i+1}_A81'), f'Ir{i+1}') for i in range(7)] +
        [(Name(station, 'avgh', f'Ir{i+1}_A81', {'pm10'}), f'Ir{i+1}') for i in range(7)] +
        [(Name(station, 'avgh', f'Ir{i+1}_A81', {'pm25'}), f'Ir{i+1}') for i in range(7)] +
        [(Name(station, 'avgh', f'Ir{i+1}_A81', {'pm1'}), f'Ir{i+1}') for i in range(7)] +
        [(Name(station, 'avgh', f'ZFACTOR{i+1}_A81'), f'CF{i+1}') for i in range(7)] +
        [(Name(station, 'avgh', f'ZFACTOR{i+1}_A81', {'pm10'}), f'CF{i+1}') for i in range(7)] +
        [(Name(station, 'avgh', f'ZFACTOR{i+1}_A81', {'pm25'}), f'CF{i+1}') for i in range(7)] +
        [(Name(station, 'avgh', f'ZFACTOR{i+1}_A81', {'pm1'}), f'CF{i+1}') for i in range(7)]
    ), send
)


station_profile_data['aerosol']['raw']['scattering2-whole'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'BsB_S12'): 'BsB',
        Name(station, 'raw', 'BsG_S12'): 'BsG',
        Name(station, 'raw', 'BsR_S12'): 'BsR',
        Name(station, 'raw', 'BbsB_S12'): 'BbsB',
        Name(station, 'raw', 'BbsG_S12'): 'BbsG',
        Name(station, 'raw', 'BbsR_S12'): 'BbsR',
    }, send
)
station_profile_data['aerosol']['raw']['scattering2-pm10'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'BsB_S12', {'pm10'}): 'BsB',
        Name(station, 'raw', 'BsG_S12', {'pm10'}): 'BsG',
        Name(station, 'raw', 'BsR_S12', {'pm10'}): 'BsR',
        Name(station, 'raw', 'BbsB_S12', {'pm10'}): 'BbsB',
        Name(station, 'raw', 'BbsG_S12', {'pm10'}): 'BbsG',
        Name(station, 'raw', 'BbsR_S12', {'pm10'}): 'BbsR',
    }, send
)
station_profile_data['aerosol']['raw']['scattering2-pm1'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'BsB_S12', {'pm1'}): 'BsB',
        Name(station, 'raw', 'BsG_S12', {'pm1'}): 'BsG',
        Name(station, 'raw', 'BsR_S12', {'pm1'}): 'BsR',
        Name(station, 'raw', 'BbsB_S12', {'pm1'}): 'BbsB',
        Name(station, 'raw', 'BbsG_S12', {'pm1'}): 'BbsG',
        Name(station, 'raw', 'BbsR_S12', {'pm1'}): 'BbsR',
    }, send
)
station_profile_data['aerosol']['raw']['scattering2-pm25'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'BsB_S12', {'pm25'}): 'BsB',
        Name(station, 'raw', 'BsG_S12', {'pm25'}): 'BsG',
        Name(station, 'raw', 'BsR_S12', {'pm25'}): 'BsR',
        Name(station, 'raw', 'BbsB_S12', {'pm25'}): 'BbsB',
        Name(station, 'raw', 'BbsG_S12', {'pm25'}): 'BbsG',
        Name(station, 'raw', 'BbsR_S12', {'pm25'}): 'BbsR',
    }, send
)
station_profile_data['aerosol']['realtime']['scattering2-whole'] = {
    RealtimeTranslator.Key('BsB_S12'): 'BsB',
    RealtimeTranslator.Key('BsG_S12'): 'BsG',
    RealtimeTranslator.Key('BsR_S12'): 'BsR',
    RealtimeTranslator.Key('BbsB_S12'): 'BbsB',
    RealtimeTranslator.Key('BbsG_S12'): 'BbsG',
    RealtimeTranslator.Key('BbsR_S12'): 'BbsR',
}
station_profile_data['aerosol']['realtime']['scattering2-pm10'] = {
    RealtimeTranslator.Key('BsB_S12', {'pm10'}): 'BsB',
    RealtimeTranslator.Key('BsG_S12', {'pm10'}): 'BsG',
    RealtimeTranslator.Key('BsR_S12', {'pm10'}): 'BsR',
    RealtimeTranslator.Key('BbsB_S12', {'pm10'}): 'BbsB',
    RealtimeTranslator.Key('BbsG_S12', {'pm10'}): 'BbsG',
    RealtimeTranslator.Key('BbsR_S12', {'pm10'}): 'BbsR',
}
station_profile_data['aerosol']['realtime']['scattering2-pm1'] = {
    RealtimeTranslator.Key('BsB_S12', {'pm1'}): 'BsB',
    RealtimeTranslator.Key('BsG_S12', {'pm1'}): 'BsG',
    RealtimeTranslator.Key('BsR_S12', {'pm1'}): 'BsR',
    RealtimeTranslator.Key('BbsB_S12', {'pm1'}): 'BbsB',
    RealtimeTranslator.Key('BbsG_S12', {'pm1'}): 'BbsG',
    RealtimeTranslator.Key('BbsR_S12', {'pm1'}): 'BbsR',
}
station_profile_data['aerosol']['realtime']['scattering2-pm25'] = {
    RealtimeTranslator.Key('BsB_S12', {'pm25'}): 'BsB',
    RealtimeTranslator.Key('BsG_S12', {'pm25'}): 'BsG',
    RealtimeTranslator.Key('BsR_S12', {'pm25'}): 'BsR',
    RealtimeTranslator.Key('BbsB_S12', {'pm25'}): 'BbsB',
    RealtimeTranslator.Key('BbsG_S12', {'pm25'}): 'BbsG',
    RealtimeTranslator.Key('BbsR_S12', {'pm25'}): 'BbsR',
}
station_profile_data['aerosol']['editing']['scattering2-whole'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
    start_epoch_ms, end_epoch_ms, station, 'aerosol', {
        Name(station, 'clean', 'BsB_S12'): 'BsB',
        Name(station, 'clean', 'BsG_S12'): 'BsG',
        Name(station, 'clean', 'BsR_S12'): 'BsR',
        Name(station, 'clean', 'BbsB_S12'): 'BbsB',
        Name(station, 'clean', 'BbsG_S12'): 'BbsG',
        Name(station, 'clean', 'BbsR_S12'): 'BbsR',
    }, send
)
station_profile_data['aerosol']['editing']['scattering2-pm10'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
    start_epoch_ms, end_epoch_ms, station, 'aerosol', {
        Name(station, 'clean', 'BsB_S12', {'pm10'}): 'BsB',
        Name(station, 'clean', 'BsG_S12', {'pm10'}): 'BsG',
        Name(station, 'clean', 'BsR_S12', {'pm10'}): 'BsR',
        Name(station, 'clean', 'BbsB_S12', {'pm10'}): 'BbsB',
        Name(station, 'clean', 'BbsG_S12', {'pm10'}): 'BbsG',
        Name(station, 'clean', 'BbsR_S12', {'pm10'}): 'BbsR',
    }, send
)
station_profile_data['aerosol']['editing']['scattering2-pm1'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
    start_epoch_ms, end_epoch_ms, station, 'aerosol', {
        Name(station, 'clean', 'BsB_S12', {'pm1'}): 'BsB',
        Name(station, 'clean', 'BsG_S12', {'pm1'}): 'BsG',
        Name(station, 'clean', 'BsR_S12', {'pm1'}): 'BsR',
        Name(station, 'clean', 'BbsB_S12', {'pm1'}): 'BbsB',
        Name(station, 'clean', 'BbsG_S12', {'pm1'}): 'BbsG',
        Name(station, 'clean', 'BbsR_S12', {'pm1'}): 'BbsR',
    }, send
)
station_profile_data['aerosol']['editing']['scattering2-pm25'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
    start_epoch_ms, end_epoch_ms, station, 'aerosol', {
        Name(station, 'clean', 'BsB_S12', {'pm25'}): 'BsB',
        Name(station, 'clean', 'BsG_S12', {'pm25'}): 'BsG',
        Name(station, 'clean', 'BsR_S12', {'pm25'}): 'BsR',
        Name(station, 'clean', 'BbsB_S12', {'pm25'}): 'BbsB',
        Name(station, 'clean', 'BbsG_S12', {'pm25'}): 'BbsG',
        Name(station, 'clean', 'BbsR_S12', {'pm25'}): 'BbsR',
    }, send
)
station_profile_data['aerosol']['clean']['scattering2-whole'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'clean', 'BsB_S12'): 'BsB',
        Name(station, 'clean', 'BsG_S12'): 'BsG',
        Name(station, 'clean', 'BsR_S12'): 'BsR',
        Name(station, 'clean', 'BbsB_S12'): 'BbsB',
        Name(station, 'clean', 'BbsG_S12'): 'BbsG',
        Name(station, 'clean', 'BbsR_S12'): 'BbsR',
    }, send
)
station_profile_data['aerosol']['clean']['scattering2-pm10'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'clean', 'BsB_S12', {'pm10'}): 'BsB',
        Name(station, 'clean', 'BsG_S12', {'pm10'}): 'BsG',
        Name(station, 'clean', 'BsR_S12', {'pm10'}): 'BsR',
        Name(station, 'clean', 'BbsB_S12', {'pm10'}): 'BbsB',
        Name(station, 'clean', 'BbsG_S12', {'pm10'}): 'BbsG',
        Name(station, 'clean', 'BbsR_S12', {'pm10'}): 'BbsR',
    }, send
)
station_profile_data['aerosol']['clean']['scattering2-pm1'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'clean', 'BsB_S12', {'pm1'}): 'BsB',
        Name(station, 'clean', 'BsG_S12', {'pm1'}): 'BsG',
        Name(station, 'clean', 'BsR_S12', {'pm1'}): 'BsR',
        Name(station, 'clean', 'BbsB_S12', {'pm1'}): 'BbsB',
        Name(station, 'clean', 'BbsG_S12', {'pm1'}): 'BbsG',
        Name(station, 'clean', 'BbsR_S12', {'pm1'}): 'BbsR',
    }, send
)
station_profile_data['aerosol']['clean']['scattering2-pm25'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'clean', 'BsB_S12', {'pm25'}): 'BsB',
        Name(station, 'clean', 'BsG_S12', {'pm25'}): 'BsG',
        Name(station, 'clean', 'BsR_S12', {'pm25'}): 'BsR',
        Name(station, 'clean', 'BbsB_S12', {'pm25'}): 'BbsB',
        Name(station, 'clean', 'BbsG_S12', {'pm25'}): 'BbsG',
        Name(station, 'clean', 'BbsR_S12', {'pm25'}): 'BbsR',
    }, send
)
station_profile_data['aerosol']['avgh']['scattering2-whole'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'avgh', 'BsB_S12'): 'BsB',
        Name(station, 'avgh', 'BsG_S12'): 'BsG',
        Name(station, 'avgh', 'BsR_S12'): 'BsR',
        Name(station, 'avgh', 'BbsB_S12'): 'BbsB',
        Name(station, 'avgh', 'BbsG_S12'): 'BbsG',
        Name(station, 'avgh', 'BbsR_S12'): 'BbsR',
    }, send
)
station_profile_data['aerosol']['avgh']['scattering2-pm10'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'avgh', 'BsB_S12', {'pm10'}): 'BsB',
        Name(station, 'avgh', 'BsG_S12', {'pm10'}): 'BsG',
        Name(station, 'avgh', 'BsR_S12', {'pm10'}): 'BsR',
        Name(station, 'avgh', 'BbsB_S12', {'pm10'}): 'BbsB',
        Name(station, 'avgh', 'BbsG_S12', {'pm10'}): 'BbsG',
        Name(station, 'avgh', 'BbsR_S12', {'pm10'}): 'BbsR',
    }, send
)
station_profile_data['aerosol']['avgh']['scattering2-pm1'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'avgh', 'BsB_S12', {'pm1'}): 'BsB',
        Name(station, 'avgh', 'BsG_S12', {'pm1'}): 'BsG',
        Name(station, 'avgh', 'BsR_S12', {'pm1'}): 'BsR',
        Name(station, 'avgh', 'BbsB_S12', {'pm1'}): 'BbsB',
        Name(station, 'avgh', 'BbsG_S12', {'pm1'}): 'BbsG',
        Name(station, 'avgh', 'BbsR_S12', {'pm1'}): 'BbsR',
    }, send
)
station_profile_data['aerosol']['avgh']['scattering2-pm25'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'avgh', 'BsB_S12', {'pm25'}): 'BsB',
        Name(station, 'avgh', 'BsG_S12', {'pm25'}): 'BsG',
        Name(station, 'avgh', 'BsR_S12', {'pm25'}): 'BsR',
        Name(station, 'avgh', 'BbsB_S12', {'pm25'}): 'BbsB',
        Name(station, 'avgh', 'BbsG_S12', {'pm25'}): 'BbsG',
        Name(station, 'avgh', 'BbsR_S12', {'pm25'}): 'BbsR',
    }, send
)

station_profile_data['aerosol']['raw']['nephzero2'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'BswB_S12'): 'BswB',
        Name(station, 'raw', 'BswG_S12'): 'BswG',
        Name(station, 'raw', 'BswR_S12'): 'BswR',
        Name(station, 'raw', 'BbswB_S12'): 'BbswB',
        Name(station, 'raw', 'BbswG_S12'): 'BbswG',
        Name(station, 'raw', 'BbswR_S12'): 'BbswR',
    }, send
)
station_profile_data['aerosol']['raw']['nephstatus2'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'CfG_S12'): 'CfG',
        Name(station, 'raw', 'CfG_S12', {'pm10'}): 'CfG',
        Name(station, 'raw', 'CfG_S12', {'pm1'}): 'CfG',
        Name(station, 'raw', 'CfG_S12', {'pm25'}): 'CfG',
    }, send
)
station_profile_data['aerosol']['realtime']['nephzero2'] = {
    RealtimeTranslator.Key('BswB_S12'): 'BswB',
    RealtimeTranslator.Key('BswG_S12'): 'BswG',
    RealtimeTranslator.Key('BswR_S12'): 'BswR',
    RealtimeTranslator.Key('BbswB_S12'): 'BbswB',
    RealtimeTranslator.Key('BbswG_S12'): 'BbswG',
    RealtimeTranslator.Key('BbswR_S12'): 'BbswR',
}
station_profile_data['aerosol']['realtime']['nephstatus2'] = {
    RealtimeTranslator.Key('CfG_S12'): 'CfG',
    RealtimeTranslator.Key('CfG_S12', {'pm10'}): 'CfG',
    RealtimeTranslator.Key('CfG_S12', {'pm1'}): 'CfG',
    RealtimeTranslator.Key('CfG_S12', {'pm25'}): 'CfG',
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
station_profile_data['aerosol']['raw']['samplepressure-pm1'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'P_S11', {'pm1'}): 'neph',
        Name(station, 'raw', 'P_S12', {'pm1'}): 'neph2',
    }, send
)
station_profile_data['aerosol']['raw']['samplepressure-pm25'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'P_S11', {'pm25'}): 'neph',
        Name(station, 'raw', 'P_S12', {'pm25'}): 'neph2',
    }, send
)


station_profile_data['aerosol']['raw']['cpcstatus'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'T1_N71'): 'Tsaturator',
        Name(station, 'raw', 'T2_N71'): 'Tcondenser',
        Name(station, 'raw', 'T3_N71'): 'Toptics',
        Name(station, 'raw', 'T4_N71'): 'Tcabinet',
        Name(station, 'raw', 'T5_N71'): 'Twatertrap',
        Name(station, 'raw', 'Q_N71'): 'Qsample',
        Name(station, 'raw', 'Qu_N71'): 'Qinlet',
        Name(station, 'raw', 'P_N71'): 'Psample',
        Name(station, 'raw', 'Pd1_N71'): 'PDnozzle',
        Name(station, 'raw', 'Pdu_N71'): 'PDinlet',
        Name(station, 'raw', 'A_N71'): 'Alaser',
        Name(station, 'raw', 'PCT_N71'): 'PCT',
    }, send
)
station_profile_data['aerosol']['realtime']['cpcstatus'] = {
    RealtimeTranslator.Key('T1_N71'): 'Tsaturator',
    RealtimeTranslator.Key('T2_N71'): 'Tcondenser',
    RealtimeTranslator.Key('T3_N71'): 'Toptics',
    RealtimeTranslator.Key('T4_N71'): 'Tcabinet',
    RealtimeTranslator.Key('T5_N71'): 'Twatertrap',
    RealtimeTranslator.Key('Q_N71'): 'Qsample',
    RealtimeTranslator.Key('Qu_N71'): 'Qinlet',
    RealtimeTranslator.Key('P_N71'): 'Psample',
    RealtimeTranslator.Key('Pd1_N71'): 'PDnozzle',
    RealtimeTranslator.Key('Pd2_N71'): 'PDorifice',
    RealtimeTranslator.Key('Pdu_N71'): 'PDinlet',
    RealtimeTranslator.Key('A_N71'): 'Alaser',
    RealtimeTranslator.Key('PCT_N71'): 'PCT',
}



def get(station: str, data_name: str, start_epoch_ms: int, end_epoch_ms: int,
        send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
    return data_profile_get(station, data_name, start_epoch_ms, end_epoch_ms, send, station_profile_data)
