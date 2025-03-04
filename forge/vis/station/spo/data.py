import typing
from ..cpd3 import use_cpd3


if use_cpd3("spo"):
    from ..cpd3 import DataStream, DataReader, EditedReader, RealtimeTranslator, ContaminationReader, EditedContaminationReader, Name, data_profile_get, detach, profile_data

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
            Name(station, 'raw', 'N_N42'): 'cnc2',
        }, send
    )
    station_profile_data['aerosol']['realtime']['cnc'] = {
        RealtimeTranslator.Key('N_N41'): 'cnc',
        RealtimeTranslator.Key('N_N42'): 'cnc2',
    }
    station_profile_data['aerosol']['clean']['cnc'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'clean', 'N_N41'): 'cnc',
            Name(station, 'clean', 'N_N42'): 'cnc2',
        }, send
    )
    station_profile_data['aerosol']['avgh']['cnc'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'avgh', 'N_N41'): 'cnc',
            Name(station, 'avgh', 'N_N42'): 'cnc2',
        }, send
    )
    station_profile_data['aerosol']['editing']['cnc'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
        start_epoch_ms, end_epoch_ms, station, 'aerosol', {
            Name(station, 'clean', 'N_N41'): 'cnc',
            Name(station, 'clean', 'N_N42'): 'cnc2',
        }, send
    )

    station_profile_data['aerosol']['raw']['cpcstatus2'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'Tu_N42'): 'Tinlet',
            Name(station, 'raw', 'TDu_N42'): 'TDinlet',
            Name(station, 'raw', 'Uu_N42'): 'Uinlet',
            Name(station, 'raw', 'T1_N42'): 'Tconditioner',
            Name(station, 'raw', 'T2_N42'): 'Tinitiator',
            Name(station, 'raw', 'T3_N42'): 'Tmoderator',
            Name(station, 'raw', 'T4_N42'): 'Toptics',
            Name(station, 'raw', 'T5_N42'): 'Theatsink',
            Name(station, 'raw', 'T6_N42'): 'Tcase',
            Name(station, 'raw', 'T7_N42'): 'Tboard',
            Name(station, 'raw', 'TD1_N42'): 'TDgrowth',
            Name(station, 'raw', 'Q_N42'): 'Qsample',
            Name(station, 'raw', 'P_N42'): 'Psample',
            Name(station, 'raw', 'PCT_N42'): 'PCTwick',
            Name(station, 'raw', 'V_N42'): 'Vpulse',
        }, send
    )
    station_profile_data['aerosol']['realtime']['cpcstatus2'] = {
        RealtimeTranslator.Key('Tu_N42'): 'Tinlet',
        RealtimeTranslator.Key('TDu_N42'): 'TDinlet',
        RealtimeTranslator.Key('Uu_N42'): 'Uinlet',
        RealtimeTranslator.Key('T1_N42'): 'Tconditioner',
        RealtimeTranslator.Key('T2_N42'): 'Tinitiator',
        RealtimeTranslator.Key('T3_N42'): 'Tmoderator',
        RealtimeTranslator.Key('T4_N42'): 'Toptics',
        RealtimeTranslator.Key('T5_N42'): 'Theatsink',
        RealtimeTranslator.Key('T6_N42'): 'Tcase',
        RealtimeTranslator.Key('T7_N42'): 'Tboard',
        RealtimeTranslator.Key('TD1_N42'): 'TDgrowth',
        RealtimeTranslator.Key('Q_N42'): 'Qsample',
        RealtimeTranslator.Key('P_N42'): 'Psample',
        RealtimeTranslator.Key('PCT_N42'): 'PCTwick',
        RealtimeTranslator.Key('V_N42'): 'Vpulse',
    }

    station_profile_data['aerosol']['raw']['dmps'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'Ns_N11'): 'Dp',
            Name(station, 'raw', 'Nn_N11'): 'dNdlogDp',
            Name(station, 'raw', 'Nb_N11'): 'dN',
            Name(station, 'raw', 'N_N12'): 'Nraw',
        }, send
    )
    station_profile_data['aerosol']['editing']['dmps'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
        start_epoch_ms, end_epoch_ms, station, 'aerosol', {
            Name(station, 'clean', 'Ns_N11'): 'Dp',
            Name(station, 'clean', 'Nn_N11'): 'dNdlogDp',
            Name(station, 'clean', 'Nb_N11'): 'dN',
            Name(station, 'clean', 'N_N12'): 'Nraw',
            Name(station, 'clean', 'N_N11'): 'N',
            Name(station, 'clean', 'BsB_N11'): 'BsB',
            Name(station, 'clean', 'BsG_N11'): 'BsG',
            Name(station, 'clean', 'BsR_N11'): 'BsR',
        }, send
    )
    station_profile_data['aerosol']['clean']['dmps'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'clean', 'Ns_N11'): 'Dp',
            Name(station, 'clean', 'Nn_N11'): 'dNdlogDp',
            Name(station, 'clean', 'Nb_N11'): 'dN',
            Name(station, 'clean', 'N_N12'): 'Nraw',
            Name(station, 'clean', 'N_N11'): 'N',
            Name(station, 'clean', 'BsB_N11'): 'BsB',
            Name(station, 'clean', 'BsG_N11'): 'BsG',
            Name(station, 'clean', 'BsR_N11'): 'BsR',
        }, send
    )
    station_profile_data['aerosol']['avgh']['dmps'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'avgh', 'Ns_N11'): 'Dp',
            Name(station, 'avgh', 'Nn_N11'): 'dNdlogDp',
            Name(station, 'avgh', 'Nb_N11'): 'dN',
            Name(station, 'avgh', 'N_N12'): 'Nraw',
            Name(station, 'avgh', 'N_N11'): 'N',
            Name(station, 'avgh', 'BsB_N11'): 'BsB',
            Name(station, 'avgh', 'BsG_N11'): 'BsG',
            Name(station, 'avgh', 'BsR_N11'): 'BsR',
        }, send
    )
    station_profile_data['aerosol']['raw']['dmpsstatus'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'T1_N11'): 'Taerosol', Name(station, 'raw', 'T2_N11'): 'Tsheath',
            Name(station, 'raw', 'P1_N11'): 'Paerosol', Name(station, 'raw', 'P2_N11'): 'Psheath',
            Name(station, 'raw', 'Q1_N11'): 'Qaerosol', Name(station, 'raw', 'Q2_N11'): 'Qsheath',
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
            Name(station, 'raw', 'Q1_A82'): 'Q1',
            Name(station, 'raw', 'Q2_A82'): 'Q2',
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

    station_profile_data['aerosol']['raw']['pressure'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'P_XM1'): 'ambient',
            Name(station, 'raw', 'P_S11'): 'neph',
            Name(station, 'raw', 'P_S11', {'pm10'}): 'neph',
            Name(station, 'raw', 'P_S11', {'pm25'}): 'neph',
            Name(station, 'raw', 'P_S11', {'pm1'}): 'neph',
        }, send
    )
    station_profile_data['aerosol']['realtime']['pressure'] = {
        RealtimeTranslator.Key('P_XM1'): 'ambient',
        RealtimeTranslator.Key('P_S11'): 'neph',
        RealtimeTranslator.Key('P_S11', {'pm10'}): 'neph',
        RealtimeTranslator.Key('P_S11', {'pm25'}): 'neph',
        RealtimeTranslator.Key('P_S11', {'pm1'}): 'neph',
    }


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

else:
    from ..default.data import aerosol_data, aerosol_public, ozone_data, met_data, radiation_data, data_get, DataStream, \
        DataRecord, RealtimeRecord, Selection, RealtimeSelection, STANDARD_THREE_WAVELENGTHS, STANDARD_CUT_SIZE_SPLIT

    data_records = dict()
    data_records.update(aerosol_data)
    data_records.update(aerosol_public)
    data_records.update(ozone_data)
    data_records.update(met_data)
    data_records.update(radiation_data)

    for archive in ("raw", "editing", "clean", "avgh"):
        data_records[f"aerosol-{archive}-cnc"] = DataRecord({
            "cnc": [Selection(variable_name="number_concentration",
                              require_tags={"cpc"}, exclude_tags={"secondary"})],
            "cnc2": [Selection(variable_name="number_concentration",
                               instrument_id="N42")],
        })
    data_records["aerosol-realtime-cnc"] = RealtimeRecord({
        "cnc": [RealtimeSelection("N", variable_name="number_concentration",
                                  require_tags={"cpc"}, exclude_tags={"secondary"})],
        "cnc2": [RealtimeSelection("N", variable_name="number_concentration",
                                   instrument_id="N42")],
    })

    data_records["aerosol-raw-cpcstatus2"] = DataRecord({
        "Tinlet": [Selection(variable_name="inlet_temperature", instrument_code="admagic200cpc"),
                   Selection(variable_name="inlet_temperature", instrument_code="admagic250cpc")],
        "TDinlet": [Selection(variable_name="inlet_dewpoint", instrument_code="admagic200cpc"),
                    Selection(variable_name="inlet_dewpoint", instrument_code="admagic250cpc")],
        "Uinlet": [Selection(variable_name="inlet_humidity", instrument_code="admagic200cpc"),
                   Selection(variable_name="inlet_humidity", instrument_code="admagic250cpc")],
        "Tconditioner": [Selection(variable_name="conditioner_temperature", instrument_code="admagic200cpc"),
                         Selection(variable_name="conditioner_temperature", instrument_code="admagic250cpc")],
        "Tinitiator": [Selection(variable_name="initiator_temperature", instrument_code="admagic200cpc"),
                       Selection(variable_name="initiator_temperature", instrument_code="admagic250cpc")],
        "Tmoderator": [Selection(variable_name="moderator_temperature", instrument_code="admagic200cpc"),
                       Selection(variable_name="moderator_temperature", instrument_code="admagic250cpc")],
        "Toptics": [Selection(variable_name="optics_temperature", instrument_code="admagic200cpc"),
                    Selection(variable_name="optics_temperature", instrument_code="admagic250cpc")],
        "Theatsink": [Selection(variable_name="heatsink_temperature", instrument_code="admagic200cpc"),
                      Selection(variable_name="heatsink_temperature", instrument_code="admagic250cpc")],
        "Tcase": [Selection(variable_name="case_temperature", instrument_code="admagic250cpc")],
        "Tboard": [Selection(variable_name="pcb_temperature", instrument_code="admagic200cpc"),
                   Selection(variable_name="board_temperature", instrument_code="admagic250cpc")],
        "TDgrowth": [Selection(variable_name="growth_tube_dewpoint", instrument_code="admagic250cpc")],
        "Qsample": [Selection(variable_name="sample_flow", instrument_code="admagic200cpc"),
                    Selection(variable_name="sample_flow", instrument_code="admagic250cpc")],
        "Psample": [Selection(variable_name="pressure", instrument_code="admagic200cpc"),
                    Selection(variable_name="pressure", instrument_code="admagic250cpc")],
        "PCTwick": [Selection(variable_name="wick_saturation", instrument_code="admagic250cpc")],
        "Vpulse": [Selection(variable_name="pulse_height", instrument_code="admagic250cpc")],
    })
    data_records["aerosol-realtime-cpcstatus2"] = RealtimeRecord({
        "Tinlet": [RealtimeSelection("Tinlet", variable_name="inlet_temperature", instrument_code="admagic200cpc"),
                   RealtimeSelection("Tinlet", variable_name="inlet_temperature", instrument_code="admagic250cpc")],
        "TDinlet": [RealtimeSelection("TDinlet", variable_name="inlet_dewpoint", instrument_code="admagic200cpc"),
                    RealtimeSelection("TDinlet", variable_name="inlet_dewpoint", instrument_code="admagic250cpc")],
        "Uinlet": [RealtimeSelection("Uinlet", variable_name="inlet_humidity", instrument_code="admagic200cpc"),
                   RealtimeSelection("Uinlet", variable_name="inlet_humidity", instrument_code="admagic250cpc")],
        "Tconditioner": [
            RealtimeSelection("Tconditioner", variable_name="conditioner_temperature", instrument_code="admagic200cpc"),
            RealtimeSelection("Tconditioner", variable_name="conditioner_temperature",
                              instrument_code="admagic250cpc")],
        "Tinitiator": [
            RealtimeSelection("Tinitiator", variable_name="initiator_temperature", instrument_code="admagic200cpc"),
            RealtimeSelection("Tinitiator", variable_name="initiator_temperature", instrument_code="admagic250cpc")],
        "Tmoderator": [
            RealtimeSelection("Tmoderator", variable_name="moderator_temperature", instrument_code="admagic200cpc"),
            RealtimeSelection("Tmoderator", variable_name="moderator_temperature", instrument_code="admagic250cpc")],
        "Toptics": [RealtimeSelection("Toptics", variable_name="optics_temperature", instrument_code="admagic200cpc"),
                    RealtimeSelection("Toptics", variable_name="optics_temperature", instrument_code="admagic250cpc")],
        "Theatsink": [
            RealtimeSelection("Theatsink", variable_name="heatsink_temperature", instrument_code="admagic200cpc"),
            RealtimeSelection("Theatsink", variable_name="heatsink_temperature", instrument_code="admagic250cpc")],
        "Tcase": [RealtimeSelection("Tcase", variable_name="case_temperature", instrument_code="admagic250cpc")],
        "Tboard": [RealtimeSelection("Tpcb", variable_name="pcb_temperature", instrument_code="admagic200cpc"),
                   RealtimeSelection("Tboard", variable_name="board_temperature", instrument_code="admagic250cpc")],
        "TDgrowth": [
            RealtimeSelection("TDgrowth", variable_name="growth_tube_dewpoint", instrument_code="admagic250cpc")],
        "Qsample": [RealtimeSelection("Q", variable_name="sample_flow", instrument_code="admagic200cpc"),
                    RealtimeSelection("Q", variable_name="sample_flow", instrument_code="admagic250cpc")],
        "Psample": [RealtimeSelection("P", variable_name="pressure", instrument_code="admagic200cpc"),
                    RealtimeSelection("P", variable_name="pressure", instrument_code="admagic250cpc")],
        "PCTwick": [RealtimeSelection("PCTwick", variable_name="wick_saturation", instrument_code="admagic250cpc")],
        "Vpulse": [RealtimeSelection("Vpulse", variable_name="pulse_height", instrument_code="admagic250cpc")],
    })

    data_records["aerosol-raw-pressure"] = DataRecord({
        "ambient": [Selection(variable_id="P", instrument_id="XM1")],
        "neph": [Selection(variable_name="sample_pressure", require_tags={"scattering"}, exclude_tags={"secondary"})],
    })
    data_records["aerosol-realtime-pressure"] = RealtimeRecord({
        "neph": [RealtimeSelection("Psample", variable_name="sample_pressure", require_tags={"scattering"},
                                   exclude_tags={"secondary"})],
    })

    data_records["aerosol-raw-dmps"] = DataRecord({
        "Dp": [Selection(variable_name="diameter", instrument_id="N11")],
        "dN": [Selection(variable_name="number_distribution", instrument_id="N11")],
        "dNdlogDp": [Selection(variable_id="Nn", instrument_id="N11")],
        "Nraw": [Selection(variable_name="raw_number_concentration", instrument_id="N11")],
    })
    data_records["aerosol-raw-dmpsstatus"] = DataRecord({
        "Taerosol": [Selection(variable_name="sample_temperature", instrument_id="N11")],
        "Tsheath": [Selection(variable_name="sheath_temperature", instrument_id="N11")],
        "Paerosol": [Selection(variable_name="sample_pressure", instrument_id="N11")],
        "Psheath": [Selection(variable_name="sheath_pressure", instrument_id="N11")],
        "Qaerosol": [Selection(variable_name="sample_flow", instrument_id="N11")],
        "Qsheath": [Selection(variable_name="sheath_flow", instrument_id="N11")],
    })
    for archive in ("editing", "clean", "avgh"):
        data_records[f"aerosol-{archive}-dmps"] = DataRecord(dict([
            ("Dp", [Selection(variable_name="diameter", instrument_id="N11")]),
            ("dNdlogDp", [Selection(variable_id="Nn", instrument_id="N11")]),
            ("dN", [Selection(variable_name="number_distribution", instrument_id="N11")]),
            ("Nraw", [Selection(variable_name="raw_number_concentration", instrument_id="N11")]),
        ] + [
                (f"Bs{code}", [Selection(variable_id="Bs", wavelength=wavelength,
                                         instrument_id="N11")])
                for code, wavelength in STANDARD_THREE_WAVELENGTHS
        ]))

    for archive in ("raw", "editing", "clean", "avgh"):
        data_records[f"met-{archive}-wind"] = DataRecord({
            "WSambient": [Selection(variable_id="WS1", instrument_id="XM1")],
            "WDambient": [Selection(variable_id="WD1", instrument_id="XM1")],
            "WS2": [Selection(variable_id="WS2", instrument_id="XM1")],
            "WD2": [Selection(variable_id="WD2", instrument_id="XM1")],
            "WS3": [Selection(variable_id="WS3", instrument_id="XM1")],
            "WD3": [Selection(variable_id="WD3", instrument_id="XM1")],
            "WS4": [Selection(variable_id="WS4", instrument_id="XM1")],
            "WD4": [Selection(variable_id="WD4", instrument_id="XM1")],
        })


    def get(station: str, data_name: str, start_epoch_ms: int, end_epoch_ms: int,
            send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
        return data_get(station, data_name, start_epoch_ms, end_epoch_ms, send, data_records)