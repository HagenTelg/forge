import typing
from ..cpd3 import use_cpd3


if use_cpd3("cpr"):
    from ..cpd3 import DataStream, DataReader, EditedReader, RealtimeTranslator, Name, data_profile_get, detach, profile_data

    station_profile_data = detach(profile_data)


    station_profile_data['aerosol']['raw']['cnc'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'N_N71'): 'cnc',
            Name(station, 'raw', 'N_N61'): 'cnc',
            Name(station, 'raw', 'N_N72'): 'cnc2',
            Name(station, 'raw', 'N_N62'): 'cnc2',
        }, send
    )
    station_profile_data['aerosol']['realtime']['cnc'] = {
        RealtimeTranslator.Key('N_N71'): 'cnc',
        RealtimeTranslator.Key('N_N61'): 'cnc',
        RealtimeTranslator.Key('N_N72'): 'cnc2',
        RealtimeTranslator.Key('N_N62'): 'cnc2',
    }
    station_profile_data['aerosol']['editing']['cnc'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
        start_epoch_ms, end_epoch_ms, station, 'aerosol', {
            Name(station, 'clean', 'N_N71'): 'cnc',
            Name(station, 'clean', 'N_N61'): 'cnc',
            Name(station, 'clean', 'N_N72'): 'cnc2',
            Name(station, 'clean', 'N_N62'): 'cnc2',
        }, send
    )
    station_profile_data['aerosol']['clean']['cnc'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'clean', 'N_N71'): 'cnc',
            Name(station, 'clean', 'N_N61'): 'cnc',
            Name(station, 'clean', 'N_N72'): 'cnc2',
            Name(station, 'clean', 'N_N62'): 'cnc2',
        }, send
    )
    station_profile_data['aerosol']['avgh']['cnc'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'avgh', 'N_N71'): 'cnc',
            Name(station, 'avgh', 'N_N61'): 'cnc',
            Name(station, 'avgh', 'N_N72'): 'cnc2',
            Name(station, 'avgh', 'N_N62'): 'cnc2',
        }, send
    )

    station_profile_data['aerosol']['raw']['cpcstatus2'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'T1_N72'): 'Tsaturator',
            Name(station, 'raw', 'T2_N72'): 'Tcondenser',
            Name(station, 'raw', 'T3_N72'): 'Toptics',
            Name(station, 'raw', 'T4_N72'): 'Tcabinet',
            Name(station, 'raw', 'Q_N72'): 'Qsample',
            Name(station, 'raw', 'Qu_N72'): 'Qinlet',
        }, send
    )
    station_profile_data['aerosol']['realtime']['cpcstatus2'] = {
        RealtimeTranslator.Key('T1_N72'): 'Tsaturator',
        RealtimeTranslator.Key('T2_N72'): 'Tcondenser',
        RealtimeTranslator.Key('T3_N72'): 'Toptics',
        RealtimeTranslator.Key('T4_N72'): 'Tcabinet',
        RealtimeTranslator.Key('Q_N72'): 'Qsample',
        RealtimeTranslator.Key('Qu_N72'): 'Qinlet',
    }

    station_profile_data['aerosol']['raw']['wind'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'WS_XM1'): 'WS',
            Name(station, 'raw', 'WD_XM1'): 'WD',
        }, send
    )
    station_profile_data['aerosol']['realtime']['wind'] = {
        RealtimeTranslator.Key('WS_XM1'): 'WS',
        RealtimeTranslator.Key('WD_XM1'): 'WD',
    }
    station_profile_data['aerosol']['clean']['wind'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'clean', 'WS_XM1'): 'WS',
            Name(station, 'clean', 'WD_XM1'): 'WD',
        }, send
    )
    station_profile_data['aerosol']['avgh']['wind'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'avgh', 'WS_XM1'): 'WS',
            Name(station, 'avgh', 'WD_XM1'): 'WD',
        }, send
    )
    station_profile_data['aerosol']['editing']['wind'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
        start_epoch_ms, end_epoch_ms, station, 'aerosol', {
            Name(station, 'clean', 'WS_XM1'): 'WS',
            Name(station, 'clean', 'WD_XM1'): 'WD',
        }, send
    )

    station_profile_data['aerosol']['raw']['flow'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'Q_Q11'): 'sample',
            Name(station, 'raw', 'Q_Q11', {'pm10'}): 'sample',
            Name(station, 'raw', 'Q_Q11', {'pm1'}): 'sample',
            Name(station, 'raw', 'Q_Q11', {'pm25'}): 'sample',
            Name(station, 'raw', 'Q_Q12'): 'dilution',
            Name(station, 'raw', 'Pd_P01'): 'pitot',
        }, send
    )
    station_profile_data['aerosol']['realtime']['flow'] = {
        RealtimeTranslator.Key('Q_Q11'): 'sample',
        RealtimeTranslator.Key('Q_Q11', {'pm10'}): 'sample',
        RealtimeTranslator.Key('Q_Q11', {'pm1'}): 'sample',
        RealtimeTranslator.Key('Q_Q11', {'pm25'}): 'sample',
        RealtimeTranslator.Key('Q_Q12'): 'dilution',
        RealtimeTranslator.Key('Pd_P01'): 'pitot',
    }

    station_profile_data['aerosol']['raw']['temperature'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'T_V51'): 'Tinlet', Name(station, 'raw', 'U_V51'): 'Uinlet',
            Name(station, 'raw', 'T1_XM1'): 'Tambient',
            Name(station, 'raw', 'U1_XM1'): 'Uambient',
            Name(station, 'raw', 'TD1_XM1'): 'TDambient',
            Name(station, 'raw', 'T1_XM2'): 'Tpwd',

            Name(station, 'raw', 'T_V11'): 'Tsample', Name(station, 'raw', 'U_V11'): 'Usample',
            Name(station, 'raw', 'T_V11', {'pm10'}): 'Tsample', Name(station, 'raw', 'U_V11', {'pm10'}): 'Usample',
            Name(station, 'raw', 'T_V11', {'pm1'}): 'Tsample', Name(station, 'raw', 'U_V11', {'pm1'}): 'Usample',
            Name(station, 'raw', 'T_V11', {'pm25'}): 'Tsample', Name(station, 'raw', 'U_V11', {'pm25'}): 'Usample',

            Name(station, 'raw', 'Tu_S11'): 'Tnephinlet', Name(station, 'raw', 'Uu_S11'): 'Unephinlet',
            Name(station, 'raw', 'Tu_S11', {'pm10'}): 'Tnephinlet', Name(station, 'raw', 'Uu_S11', {'pm10'}): 'Unephinlet',
            Name(station, 'raw', 'Tu_S11', {'pm1'}): 'Tnephinlet', Name(station, 'raw', 'Uu_S11', {'pm1'}): 'Unephinlet',
            Name(station, 'raw', 'Tu_S11', {'pm25'}): 'Tnephinlet', Name(station, 'raw', 'Uu_S11', {'pm25'}): 'Unephinlet',

            Name(station, 'raw', 'T_S11'): 'Tneph', Name(station, 'raw', 'U_S11'): 'Uneph',
            Name(station, 'raw', 'T_S11', {'pm10'}): 'Tneph', Name(station, 'raw', 'U_S11', {'pm10'}): 'Uneph',
            Name(station, 'raw', 'T_S11', {'pm1'}): 'Tneph', Name(station, 'raw', 'U_S11', {'pm1'}): 'Uneph',
            Name(station, 'raw', 'T_S11', {'pm25'}): 'Tneph', Name(station, 'raw', 'U_S11', {'pm25'}): 'Uneph',

            Name(station, 'raw', 'T_V01'): 'Troom', Name(station, 'raw', 'U_V01'): 'Uroom',
        }, send
    )
    station_profile_data['aerosol']['realtime']['temperature'] = {
        RealtimeTranslator.Key('T_V51'): 'Tinlet', RealtimeTranslator.Key('U_V51'): 'Uinlet',
        RealtimeTranslator.Key('T1_XM1'): 'Tambient',
        RealtimeTranslator.Key('U1_XM1'): 'Uambient',
        RealtimeTranslator.Key('TD1_XM1'): 'TDambient',
        RealtimeTranslator.Key('T1_XM2'): 'Tpwd',

        RealtimeTranslator.Key('T_V11'): 'Tsample', RealtimeTranslator.Key('U_V11'): 'Usample',
        RealtimeTranslator.Key('T_V11', {'pm10'}): 'Tsample', RealtimeTranslator.Key('U_V11', {'pm10'}): 'Usample',
        RealtimeTranslator.Key('T_V11', {'pm1'}): 'Tsample', RealtimeTranslator.Key('U_V11', {'pm1'}): 'Usample',
        RealtimeTranslator.Key('T_V11', {'pm25'}): 'Tsample', RealtimeTranslator.Key('U_V11', {'pm25'}): 'Usample',

        RealtimeTranslator.Key('Tu_S11'): 'Tnephinlet', RealtimeTranslator.Key('Uu_S11'): 'Unephinlet',
        RealtimeTranslator.Key('Tu_S11', {'pm10'}): 'Tnephinlet', RealtimeTranslator.Key('Uu_S11', {'pm10'}): 'Unephinlet',
        RealtimeTranslator.Key('Tu_S11', {'pm1'}): 'Tnephinlet', RealtimeTranslator.Key('Uu_S11', {'pm1'}): 'Unephinlet',
        RealtimeTranslator.Key('Tu_S11', {'pm25'}): 'Tnephinlet', RealtimeTranslator.Key('Uu_S11', {'pm25'}): 'Unephinlet',

        RealtimeTranslator.Key('T_S11'): 'Tneph', RealtimeTranslator.Key('U_S11'): 'Uneph',
        RealtimeTranslator.Key('T_S11', {'pm10'}): 'Tneph', RealtimeTranslator.Key('U_S11', {'pm10'}): 'Uneph',
        RealtimeTranslator.Key('T_S11', {'pm1'}): 'Tneph', RealtimeTranslator.Key('U_S11', {'pm1'}): 'Uneph',
        RealtimeTranslator.Key('T_S11', {'pm25'}): 'Tneph', RealtimeTranslator.Key('U_S11', {'pm25'}): 'Uneph',

        RealtimeTranslator.Key('T_V01'): 'Troom', RealtimeTranslator.Key('U_V01'): 'Uroom',
    }

    station_profile_data['aerosol']['raw']['clouds'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'WI_XM1'): 'precipitation',
            Name(station, 'raw', 'WZ_XM2'): 'visibility',
            Name(station, 'raw', 'VA_XM1'): 'radiation',
            Name(station, 'raw', 'R_XM1'): 'radiation',
        }, send
    )
    station_profile_data['aerosol']['realtime']['clouds'] = {
        RealtimeTranslator.Key('WI_XM1'): 'precipitation',
        RealtimeTranslator.Key('WZ_XM2'): 'visibility',
        RealtimeTranslator.Key('VA_XM1'): 'radiation',
        RealtimeTranslator.Key('R_XM1'): 'radiation',
    }
    station_profile_data['aerosol']['editing']['clouds'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
        start_epoch_ms, end_epoch_ms, station, 'aerosol', {
            Name(station, 'clean', 'WI_XM1'): 'precipitation',
            Name(station, 'clean', 'WZ_XM2'): 'visibility',
            Name(station, 'clean', 'VA_XM1'): 'radiation',
            Name(station, 'clean', 'R_XM1'): 'radiation',
        }, send
    )
    station_profile_data['aerosol']['clean']['clouds'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'clean', 'WI_XM1'): 'precipitation',
            Name(station, 'clean', 'WZ_XM2'): 'visibility',
            Name(station, 'clean', 'VA_XM1'): 'radiation',
            Name(station, 'clean', 'R_XM1'): 'radiation',
        }, send
    )
    station_profile_data['aerosol']['avgh']['clouds'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'avgh', 'WI_XM1'): 'precipitation',
            Name(station, 'avgh', 'WZ_XM2'): 'visibility',
            Name(station, 'avgh', 'VA_XM1'): 'radiation',
            Name(station, 'avgh', 'R_XM1'): 'radiation',
        }, send
    )

    station_profile_data['aerosol']['raw']['hurricane'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'WS_XM3'): 'WS',
            Name(station, 'raw', 'WD_XM3'): 'WD',
            Name(station, 'raw', 'P_S81'): 'P',
            Name(station, 'raw', 'T_S81'): 'T',
            Name(station, 'raw', 'U_S81'): 'U',
            Name(station, 'raw', 'Ipa_S81'): 'IBsa',
            Name(station, 'raw', 'Ipb_S81'): 'IBsb',
            Name(station, 'raw', 'Bs_S81'): 'Bs',
        }, send
    )
    station_profile_data['aerosol']['realtime']['hurricane'] = {
        RealtimeTranslator.Key('WS_XM3'): 'WS',
        RealtimeTranslator.Key('WD_XM3'): 'WD',
        RealtimeTranslator.Key('P_S81'): 'P',
        RealtimeTranslator.Key('U_S81'): 'U',
        RealtimeTranslator.Key('T_S81'): 'T',
        RealtimeTranslator.Key('Ipa_S81'): 'IBsa',
        RealtimeTranslator.Key('Ipb_S81'): 'IBsb',
        RealtimeTranslator.Key('Bs_S81'): 'Bs',
    }


    def get(station: str, data_name: str, start_epoch_ms: int, end_epoch_ms: int,
            send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
        return data_profile_get(station, data_name, start_epoch_ms, end_epoch_ms, send, station_profile_data)

else:
    from ..default.data import aerosol_data, aerosol_public, data_get, DataStream, DataRecord, RealtimeRecord, Selection, RealtimeSelection

    data_records = dict()
    data_records.update(aerosol_data)
    data_records.update(aerosol_public)

    for archive in ("raw", "editing", "clean", "avgh"):
        data_records[f"aerosol-{archive}-cnc"] = DataRecord({
            "cnc": [Selection(variable_name="number_concentration", require_tags={"cpc"}, exclude_tags={"secondary"})],
            "cnc2": [Selection(variable_name="number_concentration", instrument_id="N72")],
        })
    data_records["aerosol-realtime-cnc"] = RealtimeRecord({
        "cnc": [RealtimeSelection("N", variable_name="number_concentration", require_tags={"cpc"}, exclude_tags={"secondary"})],
        "cnc2": [RealtimeSelection("N", variable_name="number_concentration", instrument_id="N72")],
    })

    data_records["aerosol-raw-cpcstatus2"] = DataRecord({
        "Tsaturator": [Selection(variable_name="saturator_temperature",
                                 instrument_code="tsi377xcpc", instrument_id="N72")],
        "Tcondenser": [Selection(variable_name="condenser_temperature",
                                 instrument_code="tsi377xcpc", instrument_id="N72")],
        "Toptics": [Selection(variable_name="optics_temperature",
                              instrument_code="tsi377xcpc", instrument_id="N72")],
        "Tcabinet": [Selection(variable_name="cabinet_temperature",
                               instrument_code="tsi377xcpc", instrument_id="N72")],
        "Qsample": [Selection(variable_name="sample_flow",
                              instrument_code="tsi377xcpc", instrument_id="N72")],
        "Qinlet": [Selection(variable_name="inlet_flow",
                             instrument_code="tsi377xcpc", instrument_id="N72")],
        "Psample": [Selection(variable_name="pressure",
                              instrument_code="tsi377xcpc", instrument_id="N72")],
        "PDnozzle": [Selection(variable_name="nozzle_pressure_drop",
                               instrument_code="tsi377xcpc", instrument_id="N72")],
        "PDorifice": [Selection(variable_name="orifice_pressure_drop",
                                instrument_code="tsi377xcpc", instrument_id="N72")],
        "Alaser": [Selection(variable_name="laser_current",
                             instrument_code="tsi377xcpc", instrument_id="N72")],
    })
    data_records["aerosol-realtime-cpcstatus2"] = RealtimeRecord({
        "Tsaturator": [RealtimeSelection("Tsaturator", variable_name="saturator_temperature",
                                         instrument_code="tsi377xcpc", instrument_id="N72")],
        "Tcondenser": [RealtimeSelection("Tcondenser", variable_name="condenser_temperature",
                                         instrument_code="tsi377xcpc", instrument_id="N72")],
        "Toptics": [RealtimeSelection("Toptics", variable_name="optics_temperature",
                                      instrument_code="tsi377xcpc", instrument_id="N72")],
        "Tcabinet": [RealtimeSelection("Tcabinet", variable_name="cabinet_temperature",
                                       instrument_code="tsi377xcpc", instrument_id="N72")],
        "Qsample": [RealtimeSelection("Q", variable_name="sample_flow",
                                      instrument_code="tsi377xcpc", instrument_id="N72")],
        "Qinlet": [RealtimeSelection("Qinlet", variable_name="inlet_flow",
                                     instrument_code="tsi377xcpc", instrument_id="N72")],
        "Psample": [RealtimeSelection("P", variable_name="pressure",
                                      instrument_code="tsi377xcpc", instrument_id="N72")],
        "PDnozzle": [RealtimeSelection("PDnozzle", variable_name="nozzle_pressure_drop",
                                       instrument_code="tsi377xcpc", instrument_id="N72")],
        "PDorifice": [RealtimeSelection("PDorifice", variable_name="orifice_pressure_drop",
                                        instrument_code="tsi377xcpc", instrument_id="N72")],
        "Alaser": [RealtimeSelection("Alaser", variable_name="laser_current",
                                     instrument_code="tsi377xcpc", instrument_id="N72")],
    })

    data_records["aerosol-raw-flow"] = DataRecord({
        "sample": [Selection(variable_id="Q_Q11")],
        "dilution": [Selection(variable_id="Q_Q12")],
        "pitot": [Selection(variable_id="Pd_P01")],
    })
    data_records["aerosol-realtime-flow"] = RealtimeRecord({
        "sample": [RealtimeSelection("Q_Q11", variable_id="Q_Q11")],
        "dilution": [RealtimeSelection("Q_Q12", variable_id="Q_Q12")],
        "pitot": [RealtimeSelection("Pd_P01", variable_id="Pd_P01")],
    })


    data_records["aerosol-raw-temperature"] = DataRecord({
        "Tinlet": [Selection(variable_id="T_V51")], "Uinlet": [Selection(variable_id="U_V51")],
        "Tambient": [Selection(variable_id="T1", instrument_id="XM1")],
        "Uambient": [Selection(variable_id="U1", instrument_id="XM1")],
        "TDambient": [Selection(variable_id="TD1", instrument_id="XM1")],
        "Tpwd": [Selection(variable_name="ambient_temperature", instrument_id="XM2")],
        "Troom": [Selection(variable_id="T_V01")], "Uroom": [Selection(variable_id="U_V01")],

        "Tsample": [Selection(variable_id="T_V11")], "Usample": [Selection(variable_id="U_V11")],

        "Tnephinlet": [Selection(variable_name="inlet_temperature", require_tags={"scattering"}, exclude_tags={"secondary"})],
        "Unephinlet": [Selection(variable_name="inlet_humidity", require_tags={"scattering"}, exclude_tags={"secondary"})],
        "Tneph": [Selection(variable_name="sample_temperature", require_tags={"scattering"}, exclude_tags={"secondary"})],
        "Uneph": [Selection(variable_name="sample_humidity", require_tags={"scattering"}, exclude_tags={"secondary"})],
    })
    data_records["aerosol-realtime-temperature"] = RealtimeRecord({
        "Tinlet": [RealtimeSelection("T_V51", variable_id="T_V51")], "Uinlet": [RealtimeSelection("U_V51", variable_id="U_V51")],
        "Tambient": [RealtimeSelection("Tambient", variable_id="T1", instrument_id="XM1")],
        "Uambient": [RealtimeSelection("Uambient", variable_id="U1", instrument_id="XM1")],
        "TDambient": [RealtimeSelection("TDambient", variable_id="TD1", instrument_id="XM1")],
        "Tpwd": [RealtimeSelection("Tambient", variable_name="ambient_temperature", instrument_id="XM2")],
        "Troom": [RealtimeSelection("T_V01", variable_id="T_V01")], "Uroom": [RealtimeSelection("U_V01", variable_id="U_V01")],

        "Tsample": [RealtimeSelection("T_V11", variable_id="T_V11")], "Usample": [RealtimeSelection("U_V11", variable_id="U_V11")],

        "Tnephinlet": [RealtimeSelection("Tinlet", variable_name="inlet_temperature", require_tags={"scattering"}, exclude_tags={"secondary"})],
        "Unephinlet": [RealtimeSelection("Uinlet", variable_name="inlet_humidity", require_tags={"scattering"}, exclude_tags={"secondary"})],
        "Tneph": [RealtimeSelection("Tsample", variable_name="sample_temperature", require_tags={"scattering"}, exclude_tags={"secondary"})],
        "Uneph": [RealtimeSelection("Usample", variable_name="sample_humidity", require_tags={"scattering"}, exclude_tags={"secondary"})],
    })

    for archive in ("raw", "editing", "clean", "avgh"):
        data_records[f"aerosol-{archive}-clouds"] = DataRecord({
            "precipitation": [Selection(variable_name="precipitation_rate", instrument_id="XM1")],
            "visibility": [Selection(variable_name="visibility", instrument_id="XM2")],
            "radiation": [Selection(variable_name="solar_radiation", instrument_id="XM1")],
        })
    data_records[f"aerosol-realtime-clouds"] = RealtimeRecord({
        "precipitation": [RealtimeSelection("WI", variable_name="precipitation_rate", instrument_id="XM1")],
        "visibility": [RealtimeSelection("WZ", variable_name="visibility", instrument_id="XM2")],
        "radiation": [RealtimeSelection("R", variable_name="solar_radiation", instrument_id="XM1")],
    })

    for archive in ("raw", ):
        data_records[f"aerosol-{archive}-hurricane"] = DataRecord({
            "WS": [Selection(variable_name="wind_speed", instrument_id="XM3")],
            "WD": [Selection(variable_name="wind_direction", instrument_id="XM3")],
            "P": [Selection(variable_name="ambient_pressure", instrument_code="purpleair", exclude_tags={"secondary"})],
            "T": [Selection(variable_name="ambient_temperature", instrument_code="purpleair", exclude_tags={"secondary"})],
            "U": [Selection(variable_name="ambient_humidity", instrument_code="purpleair", exclude_tags={"secondary"})],
            "IBsa": [Selection(variable_name="detector_a_intensity", instrument_code="purpleair", exclude_tags={"secondary"})],
            "IBsb": [Selection(variable_name="detector_b_intensity", instrument_code="purpleair", exclude_tags={"secondary"})],
            "Bs": [Selection(variable_name="scattering_coefficient", instrument_code="purpleair", exclude_tags={"secondary"},
                             wavelength_number=0)],
        })
    data_records[f"aerosol-realtime-hurricane"] = RealtimeRecord({
        "WS": [RealtimeSelection("WS", variable_name="wind_speed", instrument_id="XM3")],
        "WD": [RealtimeSelection("WD", variable_name="wind_direction", instrument_id="XM3")],
        "P": [RealtimeSelection("P", variable_name="ambient_pressure", instrument_code="purpleair", exclude_tags={"secondary"})],
        "T": [RealtimeSelection("T", variable_name="ambient_temperature", instrument_code="purpleair", exclude_tags={"secondary"})],
        "U": [RealtimeSelection("U", variable_name="ambient_humidity", instrument_code="purpleair", exclude_tags={"secondary"})],
        "IBsa": [RealtimeSelection("IBsa", variable_name="detector_a_intensity", instrument_code="purpleair", exclude_tags={"secondary"})],
        "IBsb": [RealtimeSelection("IBsb", variable_name="detector_b_intensity", instrument_code="purpleair", exclude_tags={"secondary"})],
        "Bs": [RealtimeSelection("Bs", variable_name="scattering_coefficient", instrument_code="purpleair", exclude_tags={"secondary"},
                                 wavelength_number=0)],
    })


    def get(station: str, data_name: str, start_epoch_ms: int, end_epoch_ms: int,
            send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
        return data_get(station, data_name, start_epoch_ms, end_epoch_ms, send, data_records)