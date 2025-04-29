import typing
from math import nan
from ..cpd3 import use_cpd3


if use_cpd3("lln"):
    from ..cpd3 import DataStream, DataReader, EditedReader, Name, RealtimeTranslator, data_profile_get, detach, profile_data

    station_profile_data = detach(profile_data)

    station_profile_data['aerosol']['raw']['cpcstatus'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'Tu_N71'): 'Tinlet',
            Name(station, 'raw', 'TDu_N71'): 'TDinlet',
            Name(station, 'raw', 'Uu_N71'): 'Uinlet',
            Name(station, 'raw', 'T1_N71'): 'Tconditioner',
            Name(station, 'raw', 'T2_N71'): 'Tinitiator',
            Name(station, 'raw', 'T3_N71'): 'Tmoderator',
            Name(station, 'raw', 'T4_N71'): 'Toptics',
            Name(station, 'raw', 'T5_N71'): 'Theatsink',
            Name(station, 'raw', 'T6_N71'): 'Tpcb',
            Name(station, 'raw', 'T7_N71'): 'Tcabinet',
            Name(station, 'raw', 'Q_N71'): 'Qsample',
            Name(station, 'raw', 'P_N71'): 'Psample',
            Name(station, 'raw', 'Pd_N71'): 'PDorifice',
        }, send
    )
    station_profile_data['aerosol']['realtime']['cpcstatus'] = {
        RealtimeTranslator.Key('Tu_N71'): 'Tinlet',
        RealtimeTranslator.Key('TDu_N71'): 'TDinlet',
        RealtimeTranslator.Key('Uu_N71'): 'Uinlet',
        RealtimeTranslator.Key('T1_N71'): 'Tconditioner',
        RealtimeTranslator.Key('T2_N71'): 'Tinitiator',
        RealtimeTranslator.Key('T3_N71'): 'Tmoderator',
        RealtimeTranslator.Key('T4_N71'): 'Toptics',
        RealtimeTranslator.Key('T5_N71'): 'Theatsink',
        RealtimeTranslator.Key('T6_N71'): 'Tpcb',
        RealtimeTranslator.Key('T7_N71'): 'Tcabinet',
        RealtimeTranslator.Key('Q_N71'): 'Qsample',
        RealtimeTranslator.Key('P_N71'): 'Psample',
        RealtimeTranslator.Key('Pd_N71'): 'PDorifice',
    }


    station_profile_data['aerosol']['raw']['clap2-whole'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'BaB_A91'): 'BaB',
            Name(station, 'raw', 'BaG_A91'): 'BaG',
            Name(station, 'raw', 'BaR_A91'): 'BaR',
        }, send
    )
    station_profile_data['aerosol']['raw']['clap2-pm10'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'BaB_A91', {'pm10'}): 'BaB',
            Name(station, 'raw', 'BaG_A91', {'pm10'}): 'BaG',
            Name(station, 'raw', 'BaR_A91', {'pm10'}): 'BaR',
        }, send
    )
    station_profile_data['aerosol']['raw']['clap2-pm25'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'BaB_A91', {'pm25'}): 'BaB',
            Name(station, 'raw', 'BaG_A91', {'pm25'}): 'BaG',
            Name(station, 'raw', 'BaR_A91', {'pm25'}): 'BaR',
        }, send
    )
    station_profile_data['aerosol']['raw']['clap2-pm1'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'BaB_A91', {'pm1'}): 'BaB',
            Name(station, 'raw', 'BaG_A91', {'pm1'}): 'BaG',
            Name(station, 'raw', 'BaR_A91', {'pm1'}): 'BaR',
        }, send
    )
    station_profile_data['aerosol']['realtime']['clap2-whole'] = {
        RealtimeTranslator.Key('BaB_A91'): 'BaB',
        RealtimeTranslator.Key('BaG_A91'): 'BaG',
        RealtimeTranslator.Key('BaR_A91'): 'BaR',
    }
    station_profile_data['aerosol']['realtime']['clap2-pm10'] = {
        RealtimeTranslator.Key('BaB_A91', {'pm10'}): 'BaB',
        RealtimeTranslator.Key('BaG_A91', {'pm10'}): 'BaG',
        RealtimeTranslator.Key('BaR_A91', {'pm10'}): 'BaR',
    }
    station_profile_data['aerosol']['realtime']['clap2-pm25'] = {
        RealtimeTranslator.Key('BaB_A91', {'pm25'}): 'BaB',
        RealtimeTranslator.Key('BaG_A91', {'pm25'}): 'BaG',
        RealtimeTranslator.Key('BaR_A91', {'pm25'}): 'BaR',
    }
    station_profile_data['aerosol']['realtime']['clap2-pm1'] = {
        RealtimeTranslator.Key('BaB_A91', {'pm1'}): 'BaB',
        RealtimeTranslator.Key('BaG_A91', {'pm1'}): 'BaG',
        RealtimeTranslator.Key('BaR_A91', {'pm1'}): 'BaR',
    }
    station_profile_data['aerosol']['editing']['clap2-whole'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
        start_epoch_ms, end_epoch_ms, station, 'aerosol', {
            Name(station, 'clean', 'BaB_A91'): 'BaB',
            Name(station, 'clean', 'BaG_A91'): 'BaG',
            Name(station, 'clean', 'BaR_A91'): 'BaR',
        }, send
    )
    station_profile_data['aerosol']['editing']['clap2-pm10'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
        start_epoch_ms, end_epoch_ms, station, 'aerosol', {
            Name(station, 'clean', 'BaB_A91', {'pm10'}): 'BaB',
            Name(station, 'clean', 'BaG_A91', {'pm10'}): 'BaG',
            Name(station, 'clean', 'BaR_A91', {'pm10'}): 'BaR',
        }, send
    )
    station_profile_data['aerosol']['editing']['clap2-pm25'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
        start_epoch_ms, end_epoch_ms, station, 'aerosol', {
            Name(station, 'clean', 'BaB_A91', {'pm25'}): 'BaB',
            Name(station, 'clean', 'BaG_A91', {'pm25'}): 'BaG',
            Name(station, 'clean', 'BaR_A91', {'pm25'}): 'BaR',
        }, send
    )
    station_profile_data['aerosol']['editing']['clap2-pm1'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
        start_epoch_ms, end_epoch_ms, station, 'aerosol', {
            Name(station, 'clean', 'BaB_A91', {'pm1'}): 'BaB',
            Name(station, 'clean', 'BaG_A91', {'pm1'}): 'BaG',
            Name(station, 'clean', 'BaR_A91', {'pm1'}): 'BaR',
        }, send
    )
    station_profile_data['aerosol']['clean']['clap2-whole'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'clean', 'BaB_A91'): 'BaB',
            Name(station, 'clean', 'BaG_A91'): 'BaG',
            Name(station, 'clean', 'BaR_A91'): 'BaR',
        }, send
    )
    station_profile_data['aerosol']['clean']['clap2-pm10'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'clean', 'BaB_A91', {'pm10'}): 'BaB',
            Name(station, 'clean', 'BaG_A91', {'pm10'}): 'BaG',
            Name(station, 'clean', 'BaR_A91', {'pm10'}): 'BaR',
        }, send
    )
    station_profile_data['aerosol']['clean']['clap2-pm25'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'clean', 'BaB_A91', {'pm25'}): 'BaB',
            Name(station, 'clean', 'BaG_A91', {'pm25'}): 'BaG',
            Name(station, 'clean', 'BaR_A91', {'pm25'}): 'BaR',
        }, send
    )
    station_profile_data['aerosol']['clean']['clap2-pm1'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'clean', 'BaB_A91', {'pm1'}): 'BaB',
            Name(station, 'clean', 'BaG_A91', {'pm1'}): 'BaG',
            Name(station, 'clean', 'BaR_A91', {'pm1'}): 'BaR',
        }, send
    )
    station_profile_data['aerosol']['avgh']['clap2-whole'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'avgh', 'BaB_A91'): 'BaB',
            Name(station, 'avgh', 'BaG_A91'): 'BaG',
            Name(station, 'avgh', 'BaR_A91'): 'BaR',
        }, send
    )
    station_profile_data['aerosol']['avgh']['clap2-pm10'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'avgh', 'BaB_A91', {'pm10'}): 'BaB',
            Name(station, 'avgh', 'BaG_A91', {'pm10'}): 'BaG',
            Name(station, 'avgh', 'BaR_A91', {'pm10'}): 'BaR',
        }, send
    )
    station_profile_data['aerosol']['avgh']['clap2-pm25'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'avgh', 'BaB_A91', {'pm25'}): 'BaB',
            Name(station, 'avgh', 'BaG_A91', {'pm25'}): 'BaG',
            Name(station, 'avgh', 'BaR_A91', {'pm25'}): 'BaR',
        }, send
    )
    station_profile_data['aerosol']['avgh']['clap2-pm1'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'avgh', 'BaB_A91', {'pm1'}): 'BaB',
            Name(station, 'avgh', 'BaG_A91', {'pm1'}): 'BaG',
            Name(station, 'avgh', 'BaR_A91', {'pm1'}): 'BaR',
        }, send
    )

    station_profile_data['aerosol']['raw']['clapstatus2'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'IrG_A91'): 'IrG',
            Name(station, 'raw', 'IrG_A91', {'pm10'}): 'IrG',
            Name(station, 'raw', 'IrG_A91', {'pm1'}): 'IrG',
            Name(station, 'raw', 'IrG_A91', {'pm25'}): 'IrG',
            Name(station, 'raw', 'IfG_A91'): 'IfG',
            Name(station, 'raw', 'IfG_A91', {'pm10'}): 'IfG',
            Name(station, 'raw', 'IfG_A91', {'pm1'}): 'IfG',
            Name(station, 'raw', 'IfG_A91', {'pm25'}): 'IfG',
            Name(station, 'raw', 'IpG_A91'): 'IpG',
            Name(station, 'raw', 'IpG_A91', {'pm10'}): 'IpG',
            Name(station, 'raw', 'IpG_A91', {'pm1'}): 'IpG',
            Name(station, 'raw', 'IpG_A91', {'pm25'}): 'IpG',
            Name(station, 'raw', 'Q_A91'): 'Q',
            Name(station, 'raw', 'Q_A91', {'pm10'}): 'Q',
            Name(station, 'raw', 'Q_A91', {'pm1'}): 'Q',
            Name(station, 'raw', 'Q_A91', {'pm25'}): 'Q',
            Name(station, 'raw', 'T1_A91'): 'Tsample',
            Name(station, 'raw', 'T1_A91', {'pm10'}): 'Tsample',
            Name(station, 'raw', 'T1_A91', {'pm1'}): 'Tsample',
            Name(station, 'raw', 'T1_A91', {'pm25'}): 'Tsample',
            Name(station, 'raw', 'T2_A91'): 'Tcase',
            Name(station, 'raw', 'T2_A91', {'pm10'}): 'Tcase',
            Name(station, 'raw', 'T2_A91', {'pm1'}): 'Tcase',
            Name(station, 'raw', 'T2_A91', {'pm25'}): 'Tcase',
            Name(station, 'raw', 'Fn_A91'): 'spot',
        }, send
    )
    station_profile_data['aerosol']['realtime']['clapstatus2'] = {
        RealtimeTranslator.Key('IrG_A91'): 'IrG',
        RealtimeTranslator.Key('IrG_A91', {'pm10'}): 'IrG',
        RealtimeTranslator.Key('IrG_A91', {'pm1'}): 'IrG',
        RealtimeTranslator.Key('IrG_A91', {'pm25'}): 'IrG',
        RealtimeTranslator.Key('IfG_A91'): 'IfG',
        RealtimeTranslator.Key('IfG_A91', {'pm10'}): 'IfG',
        RealtimeTranslator.Key('IfG_A91', {'pm1'}): 'IfG',
        RealtimeTranslator.Key('IfG_A91', {'pm25'}): 'IfG',
        RealtimeTranslator.Key('IpG_A91'): 'IpG',
        RealtimeTranslator.Key('IpG_A91', {'pm10'}): 'IpG',
        RealtimeTranslator.Key('IpG_A91', {'pm1'}): 'IpG',
        RealtimeTranslator.Key('IpG_A91', {'pm25'}): 'IpG',
        RealtimeTranslator.Key('Q_A91'): 'Q',
        RealtimeTranslator.Key('Q_A91', {'pm10'}): 'Q',
        RealtimeTranslator.Key('Q_A91', {'pm1'}): 'Q',
        RealtimeTranslator.Key('Q_A91', {'pm25'}): 'Q',
        RealtimeTranslator.Key('T1_A91'): 'Tsample',
        RealtimeTranslator.Key('T1_A91', {'pm10'}): 'Tsample',
        RealtimeTranslator.Key('T1_A91', {'pm1'}): 'Tsample',
        RealtimeTranslator.Key('T1_A91', {'pm25'}): 'Tsample',
        RealtimeTranslator.Key('T2_A91'): 'Tcase',
        RealtimeTranslator.Key('T2_A91', {'pm10'}): 'Tcase',
        RealtimeTranslator.Key('T2_A91', {'pm1'}): 'Tcase',
        RealtimeTranslator.Key('T2_A91', {'pm25'}): 'Tcase',
        RealtimeTranslator.Key('Fn_A91'): 'spot',
    }


    station_profile_data['aerosol']['raw']['tca'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'X1_M81'): 'X',
        }, send
    )
    station_profile_data['aerosol']['realtime']['tca'] = {
        RealtimeTranslator.Key('X1_M81'): 'X',
    }
    station_profile_data['aerosol']['editing']['tca'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
        start_epoch_ms, end_epoch_ms, station, 'aerosol', {
            Name(station, 'clean', 'X1_M81'): 'X',
        }, send
    )
    station_profile_data['aerosol']['clean']['tca'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'clean', 'X1_M81'): 'X',
        }, send
    )
    station_profile_data['aerosol']['avgh']['tca'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'avgh', 'X1_M81'): 'X',
        }, send
    )

    station_profile_data['aerosol']['raw']['tcastatus'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'X2_M81'): 'CO2',
            Name(station, 'raw', 'T1_M81'): 'Tchamber1',
            Name(station, 'raw', 'T2_M81'): 'Tchamber2',
            Name(station, 'raw', 'T3_M81'): 'Tlicor',
            Name(station, 'raw', 'TD3_M81'): 'TDlicor',
            Name(station, 'raw', 'P_M81'): 'Plicor',
            Name(station, 'raw', 'Q1_M81'): 'Qsample',
            Name(station, 'raw', 'Q2_M81'): 'Qanalytic',
        }, send
    )
    station_profile_data['aerosol']['realtime']['tcastatus'] = {
        RealtimeTranslator.Key('X2_M81'): 'CO2',
        RealtimeTranslator.Key('T1_M81'): 'Tchamber1',
        RealtimeTranslator.Key('T2_M81'): 'Tchamber2',
        RealtimeTranslator.Key('T3_M81'): 'Tlicor',
        RealtimeTranslator.Key('TD3_M81'): 'TDlicor',
        RealtimeTranslator.Key('P_M81'): 'Plicor',
        RealtimeTranslator.Key('Q1_M81'): 'Qsample',
        RealtimeTranslator.Key('Q2_M81'): 'Qanalytic',
    }


    def get(station: str, data_name: str, start_epoch_ms: int, end_epoch_ms: int,
            send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
        return data_profile_get(station, data_name, start_epoch_ms, end_epoch_ms, send, station_profile_data)


else:
    from ..default.data import aerosol_data, aerosol_public, data_get, DataStream, DataRecord, RealtimeRecord, Selection, RealtimeSelection, STANDARD_THREE_WAVELENGTHS, STANDARD_CUT_SIZE_SPLIT

    data_records = dict()
    data_records.update(aerosol_data)
    data_records.update(aerosol_public)

    data_records["aerosol-raw-cpcstatus"] = DataRecord({
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
    data_records["aerosol-realtime-cpcstatus"] = RealtimeRecord({
        "Tinlet": [RealtimeSelection("Tinlet", variable_name="inlet_temperature", instrument_code="admagic200cpc"),
                   RealtimeSelection("Tinlet", variable_name="inlet_temperature", instrument_code="admagic250cpc")],
        "TDinlet": [RealtimeSelection("TDinlet", variable_name="inlet_dewpoint", instrument_code="admagic200cpc"),
                    RealtimeSelection("TDinlet", variable_name="inlet_dewpoint", instrument_code="admagic250cpc")],
        "Uinlet": [RealtimeSelection("Uinlet", variable_name="inlet_humidity", instrument_code="admagic200cpc"),
                   RealtimeSelection("Uinlet", variable_name="inlet_humidity", instrument_code="admagic250cpc")],
        "Tconditioner": [RealtimeSelection("Tconditioner", variable_name="conditioner_temperature", instrument_code="admagic200cpc"),
                         RealtimeSelection("Tconditioner", variable_name="conditioner_temperature", instrument_code="admagic250cpc")],
        "Tinitiator": [RealtimeSelection("Tinitiator", variable_name="initiator_temperature", instrument_code="admagic200cpc"),
                       RealtimeSelection("Tinitiator", variable_name="initiator_temperature", instrument_code="admagic250cpc")],
        "Tmoderator": [RealtimeSelection("Tmoderator", variable_name="moderator_temperature", instrument_code="admagic200cpc"),
                       RealtimeSelection("Tmoderator", variable_name="moderator_temperature", instrument_code="admagic250cpc")],
        "Toptics": [RealtimeSelection("Toptics", variable_name="optics_temperature", instrument_code="admagic200cpc"),
                    RealtimeSelection("Toptics", variable_name="optics_temperature", instrument_code="admagic250cpc")],
        "Theatsink": [RealtimeSelection("Theatsink", variable_name="heatsink_temperature", instrument_code="admagic200cpc"),
                      RealtimeSelection("Theatsink", variable_name="heatsink_temperature", instrument_code="admagic250cpc")],
        "Tcase": [RealtimeSelection("Tcase", variable_name="case_temperature", instrument_code="admagic250cpc")],
        "Tboard": [RealtimeSelection("Tpcb", variable_name="pcb_temperature", instrument_code="admagic200cpc"),
                   RealtimeSelection("Tboard", variable_name="board_temperature", instrument_code="admagic250cpc")],
        "TDgrowth": [RealtimeSelection("TDgrowth", variable_name="growth_tube_dewpoint", instrument_code="admagic250cpc")],
        "Qsample": [RealtimeSelection("Q", variable_name="sample_flow", instrument_code="admagic200cpc"),
                    RealtimeSelection("Q", variable_name="sample_flow", instrument_code="admagic250cpc")],
        "Psample": [RealtimeSelection("P", variable_name="pressure", instrument_code="admagic200cpc"),
                    RealtimeSelection("P", variable_name="pressure", instrument_code="admagic250cpc")],
        "PCTwick": [RealtimeSelection("PCTwick", variable_name="wick_saturation", instrument_code="admagic250cpc")],
        "Vpulse": [RealtimeSelection("Vpulse", variable_name="pulse_height", instrument_code="admagic250cpc")],
    })
    
    for archive in ("raw", "editing", "clean", "avgh"):
        for record, cut_size in STANDARD_CUT_SIZE_SPLIT:
            data_records[f"aerosol-{archive}-clap2-{record}"] = DataRecord(dict([
                (f"Ba{code}", [Selection(variable_name="light_absorption", wavelength=wavelength, cut_size=cut_size,
                                         instrument_id="A91", exclude_tags={"aethalometer", "thermomaap"})])
                for code, wavelength in STANDARD_THREE_WAVELENGTHS
            ]))
    data_records["aerosol-raw-clapstatus2"] = DataRecord({
        "IrG": [Selection(variable_name="transmittance", wavelength=(500, 600),
                          instrument_code="clap", instrument_id="A91"),
                Selection(variable_name="transmittance", wavelength=(500, 600),
                          instrument_code="bmitap", instrument_id="A91")],
        "IfG": [Selection(variable_name="reference_intensity", wavelength=(500, 600),
                          instrument_code="clap", instrument_id="A91"),
                Selection(variable_name="reference_intensity", wavelength=(500, 600),
                          instrument_code="bmitap", instrument_id="A91")],
        "IpG": [Selection(variable_name="sample_intensity", wavelength=(500, 600),
                          instrument_code="clap", instrument_id="A91"),
                Selection(variable_name="sample_intensity", wavelength=(500, 600),
                          instrument_code="bmitap", instrument_id="A91")],
        "Q": [Selection(variable_name="sample_flow",
                        instrument_code="clap", instrument_id="A91"),
              Selection(variable_name="sample_flow",
                        instrument_code="bmitap", instrument_id="A91")],
        "Tsample": [Selection(variable_name="sample_temperature",
                              instrument_code="clap", instrument_id="A91"),
                    Selection(variable_name="sample_temperature",
                              instrument_code="bmitap", instrument_id="A91")],
        "Tcase": [Selection(variable_name="case_temperature",
                            instrument_code="clap", instrument_id="A91"),
                  Selection(variable_name="case_temperature",
                            instrument_code="bmitap", instrument_id="A91")],
        "spot": [Selection(variable_name="spot_number",
                           instrument_code="clap", instrument_id="A91",
                           variable_type=Selection.VariableType.State),
                 Selection(variable_name="spot_number",
                           instrument_code="bmitap", instrument_id="A91",
                           variable_type=Selection.VariableType.State)],
    }, hold_fields={"spot"})
    data_records["aerosol-realtime-clapstatus"] = RealtimeRecord({
        "IrG": [RealtimeSelection("IrG", variable_name="transmittance", wavelength=(500, 600),
                                  instrument_code="clap", instrument_id="A91"),
                RealtimeSelection("IrG", variable_name="transmittance", wavelength=(500, 600),
                                  instrument_code="bmitap", instrument_id="A91")],
        "IfG": [RealtimeSelection("IfG", variable_name="reference_intensity", wavelength=(500, 600),
                                  instrument_code="clap", instrument_id="A91"),
                RealtimeSelection("IfG", variable_name="reference_intensity", wavelength=(500, 600),
                                  instrument_code="bmitap", instrument_id="A91")],
        "IpG": [RealtimeSelection("IpG", variable_name="sample_intensity", wavelength=(500, 600),
                                  instrument_code="clap", instrument_id="A91"),
                RealtimeSelection("IpG", variable_name="sample_intensity", wavelength=(500, 600),
                                  instrument_code="bmitap", instrument_id="A91")],
        "Q": [RealtimeSelection("Q", variable_name="sample_flow",
                                instrument_code="clap", instrument_id="A91"),
              RealtimeSelection("Q", variable_name="sample_flow",
                                instrument_code="bmitap", instrument_id="A91")],
        "Tsample": [RealtimeSelection("Tsample", variable_name="sample_temperature",
                                      instrument_code="clap", instrument_id="A91"),
                    RealtimeSelection("Tsample", variable_name="sample_temperature",
                                      instrument_code="bmitap", instrument_id="A91")],
        "Tcase": [RealtimeSelection("Tcase", variable_name="case_temperature",
                                    instrument_code="clap", instrument_id="A91"),
                  RealtimeSelection("Tcase", variable_name="case_temperature",
                                    instrument_code="bmitap", instrument_id="A91")],
        "spot": [RealtimeSelection("Fn", variable_name="spot_number",
                                   instrument_code="clap", instrument_id="A91",
                                   variable_type=Selection.VariableType.State),
                 RealtimeSelection("Fn", variable_name="spot_number",
                                   instrument_code="bmitap", instrument_id="A91",
                                   variable_type=Selection.VariableType.State)],
    }, hold_fields={"spot"})


    def get(station: str, data_name: str, start_epoch_ms: int, end_epoch_ms: int,
            send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
        return data_get(station, data_name, start_epoch_ms, end_epoch_ms, send, data_records)