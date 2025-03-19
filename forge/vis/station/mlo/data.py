import typing
from ..cpd3 import use_cpd3


if use_cpd3():
    from ..cpd3 import DataStream, DataReader, EditedReader, RealtimeTranslator, Name, data_profile_get, detach, profile_data

    station_profile_data = detach(profile_data)


    station_profile_data['aerosol']['raw']['cnc'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'N_N61'): 'cnc',
            Name(station, 'raw', 'N_N62'): 'cnc2',
        }, send
    )
    station_profile_data['aerosol']['realtime']['cnc'] = {
        RealtimeTranslator.Key('N_N61'): 'cnc',
        RealtimeTranslator.Key('N_N62'): 'cnc2',
    }
    station_profile_data['aerosol']['editing']['cnc'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
        start_epoch_ms, end_epoch_ms, station, 'aerosol', {
            Name(station, 'clean', 'N_N61'): 'cnc',
            Name(station, 'clean', 'N_N62'): 'cnc2',
        }, send
    )
    station_profile_data['aerosol']['clean']['cnc'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'clean', 'N_N61'): 'cnc',
            Name(station, 'clean', 'N_N62'): 'cnc2',
        }, send
    )
    station_profile_data['aerosol']['avgh']['cnc'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'avgh', 'N_N61'): 'cnc',
            Name(station, 'avgh', 'N_N62'): 'cnc2',
        }, send
    )

    station_profile_data['aerosol']['raw']['cpcstatus2'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'Tu_N62'): 'Tinlet',
            Name(station, 'raw', 'TDu_N62'): 'TDinlet',
            Name(station, 'raw', 'Uu_N62'): 'Uinlet',
            Name(station, 'raw', 'T1_N62'): 'Tconditioner',
            Name(station, 'raw', 'T2_N62'): 'Tinitiator',
            Name(station, 'raw', 'T3_N62'): 'Tmoderator',
            Name(station, 'raw', 'T4_N62'): 'Toptics',
            Name(station, 'raw', 'T5_N62'): 'Theatsink',
            Name(station, 'raw', 'T6_N62'): 'Tcase',
            Name(station, 'raw', 'T7_N62'): 'Tboard',
            Name(station, 'raw', 'TD1_N62'): 'TDgrowth',
            Name(station, 'raw', 'Q_N62'): 'Qsample',
            Name(station, 'raw', 'P_N62'): 'Psample',
            Name(station, 'raw', 'PCT_N62'): 'PCTwick',
            Name(station, 'raw', 'V_N62'): 'Vpulse',
        }, send
    )
    station_profile_data['aerosol']['realtime']['cpcstatus2'] = {
        RealtimeTranslator.Key('Tu_N62'): 'Tinlet',
        RealtimeTranslator.Key('TDu_N62'): 'TDinlet',
        RealtimeTranslator.Key('Uu_N62'): 'Uinlet',
        RealtimeTranslator.Key('T1_N62'): 'Tconditioner',
        RealtimeTranslator.Key('T2_N62'): 'Tinitiator',
        RealtimeTranslator.Key('T3_N62'): 'Tmoderator',
        RealtimeTranslator.Key('T4_N62'): 'Toptics',
        RealtimeTranslator.Key('T5_N62'): 'Theatsink',
        RealtimeTranslator.Key('T6_N62'): 'Tcase',
        RealtimeTranslator.Key('T7_N62'): 'Tboard',
        RealtimeTranslator.Key('TD1_N62'): 'TDgrowth',
        RealtimeTranslator.Key('Q_N62'): 'Qsample',
        RealtimeTranslator.Key('P_N62'): 'Psample',
        RealtimeTranslator.Key('PCT_N62'): 'PCTwick',
        RealtimeTranslator.Key('V_N62'): 'Vpulse',
    }

    station_profile_data['aerosol']['raw']['pressure'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'P_XM1'): 'ambient',
            Name(station, 'raw', 'Pd_P01'): 'pitot',
            Name(station, 'raw', 'Pd_P12'): 'vacuum',
            Name(station, 'raw', 'Pd_P12', {'pm10'}): 'vacuum',
            Name(station, 'raw', 'Pd_P12', {'pm1'}): 'vacuum',
            Name(station, 'raw', 'Pd_P12', {'pm25'}): 'vacuum',
            Name(station, 'raw', 'P_S11'): 'dPneph-whole',
            Name(station, 'raw', 'P_S11', {'pm10'}): 'dPneph-pm10',
            Name(station, 'raw', 'P_S11', {'pm25'}): 'dPneph-pm25',
            Name(station, 'raw', 'P_S11', {'pm1'}): 'dPneph-pm1',
        }, send
    )
    station_profile_data['aerosol']['realtime']['pressure'] = {
        RealtimeTranslator.Key('P_XM1'): 'ambient',
        RealtimeTranslator.Key('P_S11'): 'neph',
        RealtimeTranslator.Key('Pd_P01'): 'pitot',
        RealtimeTranslator.Key('Pd_P12'): 'vacuum',
        RealtimeTranslator.Key('Pd_P12', {'pm10'}): 'vacuum',
        RealtimeTranslator.Key('Pd_P12', {'pm1'}): 'vacuum',
        RealtimeTranslator.Key('Pd_P12', {'pm25'}): 'vacuum',
        RealtimeTranslator.Key('P_S11', {'pm10'}): 'neph',
        RealtimeTranslator.Key('P_S11', {'pm25'}): 'neph',
        RealtimeTranslator.Key('P_S11', {'pm1'}): 'neph',
    }


    def get(station: str, data_name: str, start_epoch_ms: int, end_epoch_ms: int,
            send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
        return data_profile_get(station, data_name, start_epoch_ms, end_epoch_ms, send, station_profile_data)

else:
    from ..default.data import aerosol_data, aerosol_public, ozone_data, ozone_public, met_data, radiation_data, data_get, DataStream, DataRecord, RealtimeRecord, Selection, RealtimeSelection, STANDARD_THREE_WAVELENGTHS, STANDARD_CUT_SIZE_SPLIT

    data_records = dict()
    data_records.update(aerosol_data)
    data_records.update(aerosol_public)
    data_records.update(ozone_data)
    data_records.update(ozone_public)
    data_records.update(met_data)
    data_records.update(radiation_data)

    for archive in ("raw", "editing", "clean", "avgh"):
        data_records[f"aerosol-{archive}-cnc"] = DataRecord({
            "cnc": [Selection(variable_name="number_concentration",
                              require_tags={"cpc"}, exclude_tags={"secondary"})],
            "cnc2": [Selection(variable_name="number_concentration", instrument_id="N62")],
        })
    data_records["aerosol-realtime-cnc"] = RealtimeRecord({
        "cnc": [RealtimeSelection("N", variable_name="number_concentration",
                                  require_tags={"cpc"}, exclude_tags={"secondary"})],
        "cnc2": [RealtimeSelection("N", variable_name="number_concentration",
                                   instrument_id="N62")],
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

    data_records["aerosol-raw-pressure"] = DataRecord(dict([
        ("ambient", [Selection(variable_id="P", instrument_id="XM1")]),
        ("pitot", [Selection(variable_id="Pd_P01")]),
        ("vacuum", [Selection(variable_id="Pd_P12")])] + [
        (f"dPneph-{cut_size}", [Selection(variable_name="sample_pressure", cut_size=cut_size,
                                          require_tags={"scattering"}, exclude_tags={"secondary"})])
        for record, cut_size in STANDARD_CUT_SIZE_SPLIT
    ]))
    data_records["aerosol-realtime-pressure"] = RealtimeRecord(dict([
        ("ambient", [RealtimeSelection("P", variable_id="P", instrument_id="XM1")]),
        ("pitot", [RealtimeSelection("Pd_P01", variable_id="Pd_P01")]),
        ("vacuum", [RealtimeSelection("Pd_P12", variable_id="Pd_P12")])] + [
        (f"dPneph-{cut_size}", [RealtimeSelection("Psample", variable_name="sample_pressure", cut_size=cut_size,
                                                  require_tags={"scattering"}, exclude_tags={"secondary"})])
        for record, cut_size in STANDARD_CUT_SIZE_SPLIT
    ]))


    def get(station: str, data_name: str, start_epoch_ms: int, end_epoch_ms: int,
            send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
        return data_get(station, data_name, start_epoch_ms, end_epoch_ms, send, data_records)

