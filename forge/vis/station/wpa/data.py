import typing
from ..cpd3 import use_cpd3


if use_cpd3("wpa"):
    from ..cpd3 import DataStream, DataReader, Name, RealtimeTranslator, data_profile_get, detach, profile_data

    station_profile_data = detach(profile_data)


    station_profile_data['aerosol']['raw']['temperature'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'Tx_S11'): 'Tnephcell', Name(station, 'raw', 'Ux_S11'): 'Unephcell',
            Name(station, 'raw', 'Tx_S11', {'pm10'}): 'Tnephcell', Name(station, 'raw', 'Ux_S11', {'pm10'}): 'Unephcell',
            Name(station, 'raw', 'Tx_S11', {'pm1'}): 'Tnephcell', Name(station, 'raw', 'Ux_S11', {'pm1'}): 'Unephcell',
            Name(station, 'raw', 'Tx_S11', {'pm25'}): 'Tnephcell', Name(station, 'raw', 'Ux_S11', {'pm25'}): 'Unephcell',

            Name(station, 'raw', 'T_S11'): 'Tneph', Name(station, 'raw', 'U_S11'): 'Uneph',
            Name(station, 'raw', 'T_S11', {'pm10'}): 'Tneph', Name(station, 'raw', 'U_S11', {'pm10'}): 'Uneph',
            Name(station, 'raw', 'T_S11', {'pm1'}): 'Tneph', Name(station, 'raw', 'U_S11', {'pm1'}): 'Uneph',
            Name(station, 'raw', 'T_S11', {'pm25'}): 'Tneph', Name(station, 'raw', 'U_S11', {'pm25'}): 'Uneph',
        }, send
    )
    station_profile_data['aerosol']['realtime']['temperature'] = {
        RealtimeTranslator.Key('Tx_S11'): 'Tnephcell', RealtimeTranslator.Key('Ux_S11'): 'Unephcell',
        RealtimeTranslator.Key('Tx_S11', {'pm10'}): 'Tnephcell', RealtimeTranslator.Key('Ux_S11', {'pm10'}): 'Unephcell',
        RealtimeTranslator.Key('Tx_S11', {'pm1'}): 'Tnephcell', RealtimeTranslator.Key('Ux_S11', {'pm1'}): 'Unephcell',
        RealtimeTranslator.Key('Tx_S11', {'pm25'}): 'Tnephcell', RealtimeTranslator.Key('Ux_S11', {'pm25'}): 'Unephcell',

        RealtimeTranslator.Key('T_S11'): 'Tneph', RealtimeTranslator.Key('U_S11'): 'Uneph',
        RealtimeTranslator.Key('T_S11', {'pm10'}): 'Tneph', RealtimeTranslator.Key('U_S11', {'pm10'}): 'Uneph',
        RealtimeTranslator.Key('T_S11', {'pm1'}): 'Tneph', RealtimeTranslator.Key('U_S11', {'pm1'}): 'Uneph',
        RealtimeTranslator.Key('T_S11', {'pm25'}): 'Tneph', RealtimeTranslator.Key('U_S11', {'pm25'}): 'Uneph',
    }

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


    def get(station: str, data_name: str, start_epoch_ms: int, end_epoch_ms: int,
            send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
        return data_profile_get(station, data_name, start_epoch_ms, end_epoch_ms, send, station_profile_data)

else:
    from ..default.data import aerosol_data, data_get, DataStream, DataRecord, RealtimeRecord, Selection, RealtimeSelection

    data_records = dict()
    data_records.update(aerosol_data)

    data_records["aerosol-raw-cpcstatus"] = DataRecord({
        "Tinlet": [Selection(variable_name="inlet_temperature", instrument_code="admagic200cpc", exclude_tags={"secondary"}),
                   Selection(variable_name="inlet_temperature", instrument_code="admagic250cpc", exclude_tags={"secondary"})],
        "TDinlet": [Selection(variable_name="inlet_dewpoint", instrument_code="admagic200cpc", exclude_tags={"secondary"}),
                    Selection(variable_name="inlet_dewpoint", instrument_code="admagic250cpc", exclude_tags={"secondary"})],
        "Uinlet": [Selection(variable_name="inlet_humidity", instrument_code="admagic200cpc", exclude_tags={"secondary"}),
                   Selection(variable_name="inlet_humidity", instrument_code="admagic250cpc", exclude_tags={"secondary"})],
        "Tconditioner": [Selection(variable_name="conditioner_temperature", instrument_code="admagic200cpc", exclude_tags={"secondary"}),
                         Selection(variable_name="conditioner_temperature", instrument_code="admagic250cpc", exclude_tags={"secondary"})],
        "Tinitiator": [Selection(variable_name="initiator_temperature", instrument_code="admagic200cpc", exclude_tags={"secondary"}),
                       Selection(variable_name="initiator_temperature", instrument_code="admagic250cpc", exclude_tags={"secondary"})],
        "Tmoderator": [Selection(variable_name="moderator_temperature", instrument_code="admagic200cpc", exclude_tags={"secondary"}),
                       Selection(variable_name="moderator_temperature", instrument_code="admagic250cpc", exclude_tags={"secondary"})],
        "Toptics": [Selection(variable_name="optics_temperature", instrument_code="admagic200cpc", exclude_tags={"secondary"}),
                    Selection(variable_name="optics_temperature", instrument_code="admagic250cpc", exclude_tags={"secondary"})],
        "Theatsink": [Selection(variable_name="heatsink_temperature", instrument_code="admagic200cpc", exclude_tags={"secondary"}),
                      Selection(variable_name="heatsink_temperature", instrument_code="admagic250cpc", exclude_tags={"secondary"})],
        "Tcase": [Selection(variable_name="case_temperature", instrument_code="admagic250cpc", exclude_tags={"secondary"})],
        "Tboard": [Selection(variable_name="pcb_temperature", instrument_code="admagic200cpc", exclude_tags={"secondary"}),
                   Selection(variable_name="board_temperature", instrument_code="admagic250cpc", exclude_tags={"secondary"})],
        "TDgrowth": [Selection(variable_name="growth_tube_dewpoint", instrument_code="admagic250cpc", exclude_tags={"secondary"})],
        "Qsample": [Selection(variable_name="sample_flow", instrument_code="admagic200cpc", exclude_tags={"secondary"}),
                    Selection(variable_name="sample_flow", instrument_code="admagic250cpc", exclude_tags={"secondary"})],
        "Psample": [Selection(variable_name="pressure", instrument_code="admagic200cpc", exclude_tags={"secondary"}),
                    Selection(variable_name="pressure", instrument_code="admagic250cpc", exclude_tags={"secondary"})],
        "PCTwick": [Selection(variable_name="wick_saturation", instrument_code="admagic250cpc", exclude_tags={"secondary"})],
        "Vpulse": [Selection(variable_name="pulse_height", instrument_code="admagic250cpc", exclude_tags={"secondary"})],
    })
    data_records["aerosol-realtime-cpcstatus"] = RealtimeRecord({
        "Tinlet": [RealtimeSelection("Tinlet", variable_name="inlet_temperature", instrument_code="admagic200cpc", exclude_tags={"secondary"}),
                   RealtimeSelection("Tinlet", variable_name="inlet_temperature", instrument_code="admagic250cpc", exclude_tags={"secondary"})],
        "TDinlet": [RealtimeSelection("TDinlet", variable_name="inlet_dewpoint", instrument_code="admagic200cpc", exclude_tags={"secondary"}),
                    RealtimeSelection("TDinlet", variable_name="inlet_dewpoint", instrument_code="admagic250cpc", exclude_tags={"secondary"})],
        "Uinlet": [RealtimeSelection("Uinlet", variable_name="inlet_humidity", instrument_code="admagic200cpc", exclude_tags={"secondary"}),
                   RealtimeSelection("Uinlet", variable_name="inlet_humidity", instrument_code="admagic250cpc", exclude_tags={"secondary"})],
        "Tconditioner": [
            RealtimeSelection("Tconditioner", variable_name="conditioner_temperature", instrument_code="admagic200cpc", exclude_tags={"secondary"}),
            RealtimeSelection("Tconditioner", variable_name="conditioner_temperature",
                              instrument_code="admagic250cpc", exclude_tags={"secondary"})],
        "Tinitiator": [
            RealtimeSelection("Tinitiator", variable_name="initiator_temperature", instrument_code="admagic200cpc", exclude_tags={"secondary"}),
            RealtimeSelection("Tinitiator", variable_name="initiator_temperature", instrument_code="admagic250cpc", exclude_tags={"secondary"})],
        "Tmoderator": [
            RealtimeSelection("Tmoderator", variable_name="moderator_temperature", instrument_code="admagic200cpc", exclude_tags={"secondary"}),
            RealtimeSelection("Tmoderator", variable_name="moderator_temperature", instrument_code="admagic250cpc", exclude_tags={"secondary"})],
        "Toptics": [RealtimeSelection("Toptics", variable_name="optics_temperature", instrument_code="admagic200cpc", exclude_tags={"secondary"}),
                    RealtimeSelection("Toptics", variable_name="optics_temperature", instrument_code="admagic250cpc", exclude_tags={"secondary"})],
        "Theatsink": [
            RealtimeSelection("Theatsink", variable_name="heatsink_temperature", instrument_code="admagic200cpc", exclude_tags={"secondary"}),
            RealtimeSelection("Theatsink", variable_name="heatsink_temperature", instrument_code="admagic250cpc", exclude_tags={"secondary"})],
        "Tcase": [RealtimeSelection("Tcase", variable_name="case_temperature", instrument_code="admagic250cpc", exclude_tags={"secondary"})],
        "Tboard": [RealtimeSelection("Tpcb", variable_name="pcb_temperature", instrument_code="admagic200cpc", exclude_tags={"secondary"}),
                   RealtimeSelection("Tboard", variable_name="board_temperature", instrument_code="admagic250cpc", exclude_tags={"secondary"})],
        "TDgrowth": [
            RealtimeSelection("TDgrowth", variable_name="growth_tube_dewpoint", instrument_code="admagic250cpc", exclude_tags={"secondary"})],
        "Qsample": [RealtimeSelection("Q", variable_name="sample_flow", instrument_code="admagic200cpc", exclude_tags={"secondary"}),
                    RealtimeSelection("Q", variable_name="sample_flow", instrument_code="admagic250cpc", exclude_tags={"secondary"})],
        "Psample": [RealtimeSelection("P", variable_name="pressure", instrument_code="admagic200cpc", exclude_tags={"secondary"}),
                    RealtimeSelection("P", variable_name="pressure", instrument_code="admagic250cpc", exclude_tags={"secondary"})],
        "PCTwick": [RealtimeSelection("PCTwick", variable_name="wick_saturation", instrument_code="admagic250cpc", exclude_tags={"secondary"})],
        "Vpulse": [RealtimeSelection("Vpulse", variable_name="pulse_height", instrument_code="admagic250cpc", exclude_tags={"secondary"})],
    })

    data_records["aerosol-raw-temperature"] = DataRecord({
        "Track": [Selection(variable_id="T_V21")], "Urack": [Selection(variable_id="U_V21")],
        "Tambient": [Selection(variable_id="T1", instrument_id="XM1")],
        "Uambient": [Selection(variable_id="U1", instrument_id="XM1")],
        "TDambient": [Selection(variable_id="TD1", instrument_id="XM1")],

        "Tsample": [Selection(variable_id="T_V11")], "Usample": [Selection(variable_id="U_V11")],

        "Tnephcell": [Selection(variable_name="cell_temperature", require_tags={"scattering"}, exclude_tags={"secondary"})],
        "Unephcell": [Selection(variable_name="cell_humidity", require_tags={"scattering"}, exclude_tags={"secondary"})],
        "Tneph": [Selection(variable_name="sample_temperature", require_tags={"scattering"}, exclude_tags={"secondary"})],
        "Uneph": [Selection(variable_name="sample_humidity", require_tags={"scattering"}, exclude_tags={"secondary"})],
    })
    data_records["aerosol-realtime-temperature"] = RealtimeRecord({
        "Track": [RealtimeSelection("T_V21", variable_id="T_V21")], "Urack": [RealtimeSelection("U_V21", variable_id="U_V21")],
        "Tambient": [RealtimeSelection("Tambient", variable_id="T1", instrument_id="XM1")],
        "Uambient": [RealtimeSelection("Uambient", variable_id="U1", instrument_id="XM1")],
        "TDambient": [RealtimeSelection("TDambient", variable_id="TD1", instrument_id="XM1")],

        "Tsample": [RealtimeSelection("T_V11", variable_id="T_V11")], "Usample": [RealtimeSelection("U_V11", variable_id="U_V11")],

        "Tnephcell": [RealtimeSelection("Tcell", variable_name="cell_temperature", require_tags={"scattering"}, exclude_tags={"secondary"})],
        "Unephcell": [RealtimeSelection("Ucell", variable_name="cell_humidity", require_tags={"scattering"}, exclude_tags={"secondary"})],
        "Tneph": [RealtimeSelection("Tsample", variable_name="sample_temperature", require_tags={"scattering"}, exclude_tags={"secondary"})],
        "Uneph": [RealtimeSelection("Usample", variable_name="sample_humidity", require_tags={"scattering"}, exclude_tags={"secondary"})],
    })


    def get(station: str, data_name: str, start_epoch_ms: int, end_epoch_ms: int,
            send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
        return data_get(station, data_name, start_epoch_ms, end_epoch_ms, send, data_records)