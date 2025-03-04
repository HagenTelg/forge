import typing
from math import nan
from ..cpd3 import use_cpd3


if use_cpd3("ugr"):
    from ..cpd3 import DataStream, DataReader, EditedReader, Name, RealtimeTranslator, data_profile_get, detach, profile_data

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
    station_profile_data['aerosol']['editing']['maap'] =lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
        start_epoch_ms, end_epoch_ms, station, 'aerosol', {
            Name(station, 'clean', 'BacR_A31'): 'Ba',
            Name(station, 'clean', 'XR_A31'): 'X',
        }, send
    )
    station_profile_data['aerosol']['clean']['maap'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'clean', 'BacR_A31'): 'Ba',
            Name(station, 'clean', 'XR_A31'): 'X',
        }, send
    )
    station_profile_data['aerosol']['avgh']['maap'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'avgh', 'BacR_A31'): 'Ba',
            Name(station, 'avgh', 'XR_A31'): 'X',
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
            Name(station, 'raw', 'Is1R_A31'): 'Is1',
            Name(station, 'raw', 'Is2R_A31'): 'Is2',
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
        RealtimeTranslator.Key('Is1R_A31'): 'Is1',
        RealtimeTranslator.Key('Is2R_A31'): 'Is2',
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
            Name(station, 'raw', 'Q1_A41'): 'Q1',
            Name(station, 'raw', 'Q2_A41'): 'Q2',
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
            Name(station, 'raw', 'U_N21'): 'SSset',
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
        RealtimeTranslator.Key('U_N21'): 'SSset',
    }


    def get(station: str, data_name: str, start_epoch_ms: int, end_epoch_ms: int,
            send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
        return data_profile_get(station, data_name, start_epoch_ms, end_epoch_ms, send, station_profile_data)

else:
    from ..default.data import aerosol_data, aerosol_public, data_get, DataStream, DataRecord, RealtimeRecord, Selection, RealtimeSelection, STANDARD_CUT_SIZE_SPLIT

    data_records = dict()
    data_records.update(aerosol_data)
    data_records.update(aerosol_public)

    for archive in ("raw", "editing", "clean", "avgh"):
        data_records[f"aerosol-{archive}-maap"] = DataRecord({
            "Ba": [Selection(variable_name="light_absorption", wavelength_number=0,
                             instrument_code="thermomaap", instrument_id="A31")],
            "X": [Selection(variable_name="equivalent_black_carbon", wavelength_number=0,
                            instrument_code="thermomaap", instrument_id="A31")],
        })
    data_records[f"aerosol-realtime-maap"] = RealtimeRecord({
        "Ba": [RealtimeSelection("Ba", variable_name="light_absorption", wavelength_number=0,
                                 instrument_code="thermomaap", instrument_id="A31")],
        "X": [RealtimeSelection("X", variable_name="equivalent_black_carbon", wavelength_number=0,
                                instrument_code="thermomaap", instrument_id="A31")],
    })
    data_records["aerosol-raw-maapstatus"] = DataRecord({
        "Psample": [Selection(variable_name="sample_pressure", instrument_code="thermomaap", instrument_id="A31")],
        "Tambient": [Selection(variable_name="sample_temperature", instrument_code="thermomaap", instrument_id="A31")],
        "Tmeasurementhead": [Selection(variable_name="measurement_head_temperature", instrument_code="thermomaap", instrument_id="A31")],
        "Tsystem": [Selection(variable_name="system_temperature", instrument_code="thermomaap", instrument_id="A31")],
        "Qsample": [Selection(variable_name="sample_flow", instrument_code="thermomaap", instrument_id="A31")],
        "Ir": [Selection(variable_name="transmittance", instrument_code="thermomaap", wavelength_number=0, instrument_id="A31")],
        "If": [Selection(variable_name="reference_intensity", instrument_code="thermomaap", wavelength_number=0, instrument_id="A31")],
        "Ip": [Selection(variable_name="sample_intensity", instrument_code="thermomaap", wavelength_number=0, instrument_id="A31")],
        "Is1": [Selection(variable_name="backscatter_135_intensity", instrument_code="thermomaap", wavelength_number=0, instrument_id="A31")],
        "Is2": [Selection(variable_name="backscatter_165_intensity", instrument_code="thermomaap", wavelength_number=0, instrument_id="A31")],
    })
    data_records["aerosol-realtime-maapstatus"] = RealtimeRecord({
        "Psample": [RealtimeSelection("P", variable_name="sample_pressure", instrument_code="thermomaap", instrument_id="A31")],
        "Tambient": [RealtimeSelection("Tsample", variable_name="sample_temperature", instrument_code="thermomaap", instrument_id="A31")],
        "Tmeasurementhead": [RealtimeSelection("Thead", variable_name="measurement_head_temperature", instrument_code="thermomaap", instrument_id="A31")],
        "Tsystem": [RealtimeSelection("Tsystem", variable_name="system_temperature", instrument_code="thermomaap", instrument_id="A31")],
        "Qsample": [RealtimeSelection("Q", variable_name="sample_flow", instrument_code="thermomaap", instrument_id="A31")],
        "Ir": [RealtimeSelection("Ir", variable_name="transmittance", instrument_code="thermomaap", wavelength_number=0, instrument_id="A31")],
        "If": [RealtimeSelection("If", variable_name="reference_intensity", instrument_code="thermomaap", wavelength_number=0, instrument_id="A31")],
        "Ip": [RealtimeSelection("Ip", variable_name="sample_intensity", instrument_code="thermomaap", wavelength_number=0, instrument_id="A31")],
        "Is1": [RealtimeSelection("Is135", variable_name="backscatter_135_intensity", instrument_code="thermomaap", wavelength_number=0, instrument_id="A31")],
        "Is2": [RealtimeSelection("Is165", variable_name="backscatter_165_intensity", instrument_code="thermomaap", wavelength_number=0, instrument_id="A31")],
    })

    data_records["aerosol-raw-temperature"] = DataRecord({
        "Tdryneph": [Selection(variable_id="T_V11")], "Udryneph": [Selection(variable_id="U_V11")],
        "Thumidiferinlet": [Selection(variable_id="T_V12")], "Uhumidiferinlet": [Selection(variable_id="U_V12")],
        "Thumidiferoutlet": [Selection(variable_id="T_V13")], "Uhumidiferoutlet": [Selection(variable_id="U_V13")],
        "Twetneph": [Selection(variable_id="T_V14")], "Uwetneph": [Selection(variable_id="U_V14")],

        "Tnephinlet": [Selection(variable_name="inlet_temperature", require_tags={"scattering"}, exclude_tags={"secondary"})],
        "Unephinlet": [Selection(variable_name="inlet_humidity", require_tags={"scattering"}, exclude_tags={"secondary"})],
        "Tneph": [Selection(variable_name="sample_temperature", require_tags={"scattering"}, exclude_tags={"secondary"})],
        "Uneph": [Selection(variable_name="sample_humidity", require_tags={"scattering"}, exclude_tags={"secondary"})],

        "Tnephinlet2": [Selection(variable_name="inlet_temperature", require_tags={"scattering"}, instrument_id="S12")],
        "Unephinlet2": [Selection(variable_name="inlet_humidity", require_tags={"scattering"}, instrument_id="S12")],
        "Tneph2": [Selection(variable_name="sample_temperature", require_tags={"scattering"}, instrument_id="S12")],
        "Uneph2": [Selection(variable_name="sample_humidity", require_tags={"scattering"}, instrument_id="S12")],
    })
    data_records["aerosol-realtime-temperature"] = RealtimeRecord({
        "Tdryneph": [RealtimeSelection("T_V11", variable_id="T_V11")], "Udryneph": [RealtimeSelection("U_V11", variable_id="U_V11")],
        "Thumidiferinlet": [RealtimeSelection("T_V12", variable_id="T_V12")], "Uhumidiferinlet": [RealtimeSelection("U_V12", variable_id="U_V12")],
        "Thumidiferoutlet": [RealtimeSelection("T_V13", variable_id="T_V13")], "Uhumidiferoutlet": [RealtimeSelection("U_V13", variable_id="U_V13")],
        "Twetneph": [RealtimeSelection("T_V14", variable_id="T_V14")], "Uwetneph": [RealtimeSelection("U_V14", variable_id="U_V14")],

        "Tnephinlet": [RealtimeSelection("Tinlet", variable_name="inlet_temperature", require_tags={"scattering"}, exclude_tags={"secondary"})],
        "Unephinlet": [RealtimeSelection("Uinlet", variable_name="inlet_humidity", require_tags={"scattering"}, exclude_tags={"secondary"})],
        "Tneph": [RealtimeSelection("Tsample", variable_name="sample_temperature", require_tags={"scattering"}, exclude_tags={"secondary"})],
        "Uneph": [RealtimeSelection("Usample", variable_name="sample_humidity", require_tags={"scattering"}, exclude_tags={"secondary"})],

        "Tnephinlet2": [RealtimeSelection("Tinlet", variable_name="inlet_temperature", require_tags={"scattering"}, instrument_id="S12")],
        "Unephinlet2": [RealtimeSelection("Uinlet", variable_name="inlet_humidity", require_tags={"scattering"}, instrument_id="S12")],
        "Tneph2": [RealtimeSelection("Tsample", variable_name="sample_temperature", require_tags={"scattering"}, instrument_id="S12")],
        "Uneph2": [RealtimeSelection("Usample", variable_name="sample_humidity", require_tags={"scattering"}, instrument_id="S12")],
    })

    for record, cut_size in STANDARD_CUT_SIZE_SPLIT:
        data_records[f"aerosol-raw-samplepressure-{record}"] = DataRecord({
            "neph": [Selection(variable_name="sample_pressure", cut_size=cut_size,
                               require_tags={"scattering"}, exclude_tags={"secondary"})],
            "neph2": [Selection(variable_name="sample_pressure", cut_size=cut_size,
                                require_tags={"scattering"}, instrument_id="S12")],
        })
        data_records[f"aerosol-realtime-samplepressure-{record}"] = RealtimeRecord({
            "neph": [RealtimeSelection("Psample", variable_name="sample_pressure", cut_size=cut_size,
                                       require_tags={"scattering"}, exclude_tags={"secondary"})],
            "neph2": [RealtimeSelection("Psample", variable_name="sample_pressure", cut_size=cut_size,
                                        require_tags={"scattering"}, instrument_id="S12")],
        })

    data_records["aerosol-raw-ccnstatus"] = DataRecord({
        "Tinlet": [Selection(variable_name="inlet_temperature", instrument_code="dmtccn")],
        "Ttec1": [Selection(variable_name="tec1_temperature", instrument_code="dmtccn")],
        "Ttec2": [Selection(variable_name="tec2_temperature", instrument_code="dmtccn")],
        "Ttec3": [Selection(variable_name="tec3_temperature", instrument_code="dmtccn")],
        "Tsample": [Selection(variable_name="sample_temperature", instrument_code="dmtccn")],
        "Topc": [Selection(variable_name="opc_temperature", instrument_code="dmtccn")],
        "Tnafion": [Selection(variable_name="nafion_temperature", instrument_code="dmtccn")],
        "Qsample": [Selection(variable_name="sample_flow", instrument_code="dmtccn")],
        "Qsheath": [Selection(variable_name="sheath_flow", instrument_code="dmtccn")],
        "SScalc": [Selection(variable_name="supersaturation_model", instrument_code="dmtccn")],
        "SSset": [Selection(variable_name="supersaturation_setting", instrument_code="dmtccn")],
    })
    data_records["aerosol-realtime-ccnstatus"] = RealtimeRecord({
        "Tinlet": [RealtimeSelection("Tinlet", variable_name="inlet_temperature", instrument_code="dmtccn")],
        "Ttec1": [RealtimeSelection("Ttec1", variable_name="tec1_temperature", instrument_code="dmtccn")],
        "Ttec2": [RealtimeSelection("Ttec2", variable_name="tec2_temperature", instrument_code="dmtccn")],
        "Ttec3": [RealtimeSelection("Ttec3", variable_name="tec3_temperature", instrument_code="dmtccn")],
        "Tsample": [RealtimeSelection("Tsample", variable_name="sample_temperature", instrument_code="dmtccn")],
        "Topc": [RealtimeSelection("Topc", variable_name="opc_temperature", instrument_code="dmtccn")],
        "Tnafion": [RealtimeSelection("Tnafion", variable_name="nafion_temperature", instrument_code="dmtccn")],
        "Qsample": [RealtimeSelection("Q", variable_name="sample_flow", instrument_code="dmtccn")],
        "Qsheath": [RealtimeSelection("Qsheath", variable_name="sheath_flow", instrument_code="dmtccn")],
        "SScalc": [RealtimeSelection("SScalc", variable_name="supersaturation_model", instrument_code="dmtccn")],
        "SSset": [RealtimeSelection("SSset", variable_name="supersaturation_setting", instrument_code="dmtccn")],
    })


    def get(station: str, data_name: str, start_epoch_ms: int, end_epoch_ms: int,
            send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
        return data_get(station, data_name, start_epoch_ms, end_epoch_ms, send, data_records)
