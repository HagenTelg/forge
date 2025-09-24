import typing
from ..cpd3 import use_cpd3


if use_cpd3("brw"):
    from ..cpd3 import DataStream, DataReader, EditedReader, RealtimeTranslator, Name, data_profile_get, detach, profile_data

    station_profile_data = detach(profile_data)

    station_profile_data['aerosol']['raw']['cnc'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'N_N61'): 'cnc',
            # Name(station, 'raw', 'N_N62'): 'cnc2',
            Name(station, 'raw', 'N_N12'): 'ccn',
        }, send
    )
    station_profile_data['aerosol']['realtime']['cnc'] = {
        RealtimeTranslator.Key('N_N61'): 'cnc',
        RealtimeTranslator.Key('N_N62'): 'cnc2',
        RealtimeTranslator.Key('N_N12'): 'ccn',
    }
    station_profile_data['aerosol']['editing']['cnc'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
        start_epoch_ms, end_epoch_ms, station, 'aerosol', {
            Name(station, 'clean', 'N_N61'): 'cnc',
            # Name(station, 'clean', 'N_N62'): 'cnc2',
            Name(station, 'clean', 'N_N12'): 'ccn',
        }, send
    )
    station_profile_data['aerosol']['clean']['cnc'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'clean', 'N_N61'): 'cnc',
            # Name(station, 'clean', 'N_N62'): 'cnc2',
            Name(station, 'clean', 'N_N12'): 'ccn',
        }, send
    )
    station_profile_data['aerosol']['avgh']['cnc'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'avgh', 'N_N61'): 'cnc',
            # Name(station, 'avgh', 'N_N62'): 'cnc2',
            Name(station, 'avgh', 'N_N12'): 'ccn',
        }, send
    )

    station_profile_data['aerosol']['raw']['wind'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'WS1_XM1'): 'WSambient',
            Name(station, 'raw', 'WD1_XM1'): 'WDambient',
            Name(station, 'raw', 'WS_XM2'): 'WSfilter',
            Name(station, 'raw', 'WD_XM2'): 'WDfilter',
        }, send
    )
    station_profile_data['aerosol']['realtime']['wind'] = {
        RealtimeTranslator.Key('WS_XM2'): 'WS',
        RealtimeTranslator.Key('WD_XM2'): 'WD',
    }

    station_profile_data['aerosol']['raw']['flow'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'Q_Q11'): 'sample',
            Name(station, 'raw', 'Q_Q11', {'pm10'}): 'sample',
            Name(station, 'raw', 'Q_Q11', {'pm1'}): 'sample',
            Name(station, 'raw', 'Q_Q11', {'pm25'}): 'sample',
            Name(station, 'raw', 'Q_Q21'): 'filter',
            Name(station, 'raw', 'Q_Q31'): 'filter2',
            # For the period of data when these where size split during Forge deployment (-2024-08-21T16:00:00Z)
            Name(station, 'raw', 'Q_Q21', {'pm1'}): 'filter',
            Name(station, 'raw', 'Q_Q31', {'pm1'}): 'filter2',
            Name(station, 'raw', 'Q_Q21', {'pm10'}): 'filter',
            Name(station, 'raw', 'Q_Q31', {'pm10'}): 'filter2',
            Name(station, 'raw', 'Pd_P01'): 'pitot',
        }, send
    )
    station_profile_data['aerosol']['realtime']['flow'] = {
        RealtimeTranslator.Key('Q_Q11'): 'sample',
        RealtimeTranslator.Key('Q_Q11', {'pm10'}): 'sample',
        RealtimeTranslator.Key('Q_Q11', {'pm1'}): 'sample',
        RealtimeTranslator.Key('Q_Q11', {'pm25'}): 'sample',
        RealtimeTranslator.Key('Q_Q21'): 'filter',
        RealtimeTranslator.Key('Q_Q31'): 'filter2',
        RealtimeTranslator.Key('Q_Q21', {'pm1'}): 'filter',
        RealtimeTranslator.Key('Q_Q31', {'pm1'}): 'filter2',
        RealtimeTranslator.Key('Q_Q21', {'pm10'}): 'filter',
        RealtimeTranslator.Key('Q_Q31', {'pm10'}): 'filter2',
        RealtimeTranslator.Key('Pd_P01'): 'pitot',
    }

    station_profile_data['aerosol']['raw']['temperature'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'T_V51'): 'Tinlet', Name(station, 'raw', 'U_V51'): 'Uinlet',
            Name(station, 'raw', 'T1_XM1'): 'Tambient',
            Name(station, 'raw', 'U1_XM1'): 'Uambient',
            Name(station, 'raw', 'TD1_XM1'): 'TDambient',

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

            Name(station, 'raw', 'T_V21'): 'Tfilter', Name(station, 'raw', 'U_V21'): 'Ufilter',
            Name(station, 'raw', 'T_V22'): 'Tfilterrack',
        }, send
    )
    station_profile_data['aerosol']['realtime']['temperature'] = {
        RealtimeTranslator.Key('T_V51'): 'Tinlet', RealtimeTranslator.Key('U_V51'): 'Uinlet',
        RealtimeTranslator.Key('T_XM2'): 'Tambient',
        RealtimeTranslator.Key('U_XM2'): 'Uambient',
        RealtimeTranslator.Key('TD_XM2'): 'TDambient',

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

        RealtimeTranslator.Key('T_V21'): 'Tfilter', RealtimeTranslator.Key('U_V21'): 'Ufilter',
        RealtimeTranslator.Key('T_V22'): 'Tfilterrack',
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

    station_profile_data['aerosol']['raw']['filterstatus'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, dict(
            [(Name(station, 'raw', f'Fn_F21'), f'Fn')] +
            [(Name(station, 'raw', f'Pd_P2{i+1}'), f'Pd{i+1}') for i in range(8)]
        ), send
    )
    station_profile_data['aerosol']['realtime']['filterstatus'] = dict(
        [(RealtimeTranslator.Key(f'Fn_F21'), f'Fn')] +
        [(RealtimeTranslator.Key(f'Pd_P2{i+1}'), f'Pd{i+1}') for i in range(8)]
    )

    station_profile_data['aerosol']['raw']['filterstatus2'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, dict(
            [(Name(station, 'raw', f'Fn_F31'), f'Fn')] +
            [(Name(station, 'raw', f'Pd_P3{i+1}'), f'Pd{i+1}') for i in range(8)]
        ), send
    )
    station_profile_data['aerosol']['realtime']['filterstatus2'] = dict(
        [(RealtimeTranslator.Key(f'Fn_F31'), f'Fn')] +
        [(RealtimeTranslator.Key(f'Pd_P3{i+1}'), f'Pd{i+1}') for i in range(8)]
    )

    station_profile_data['aerosol']['raw']['umacstatus'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'T_X1'): 'T',
            Name(station, 'raw', 'V_X1'): 'V',
            Name(station, 'raw', 'T_X3'): 'Tfilter',
            Name(station, 'raw', 'V_X3'): 'Vfilter',
            Name(station, 'raw', 'T_X5'): 'Tfilter2',
            Name(station, 'raw', 'V_X5'): 'Vfilter2',
        }, send
    )
    station_profile_data['aerosol']['realtime']['umacstatus'] = {
        RealtimeTranslator.Key('T_X1'): 'T',
        RealtimeTranslator.Key('V_X1'): 'V',
        RealtimeTranslator.Key('T_X3'): 'Tfilter',
        RealtimeTranslator.Key('V_X3'): 'Vfilter',
        RealtimeTranslator.Key('T_X5'): 'Tfilter2',
        RealtimeTranslator.Key('V_X5'): 'Vfilter2',
    }

    station_profile_data['aerosol']['raw']['cpcstatus'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'Tu_N61'): 'Tinlet',
            Name(station, 'raw', 'TDu_N61'): 'TDinlet',
            Name(station, 'raw', 'Uu_N61'): 'Uinlet',
            Name(station, 'raw', 'T1_N61'): 'Tconditioner',
            Name(station, 'raw', 'T2_N61'): 'Tinitiator',
            Name(station, 'raw', 'T3_N61'): 'Tmoderator',
            Name(station, 'raw', 'T4_N61'): 'Toptics',
            Name(station, 'raw', 'T5_N61'): 'Theatsink',
            Name(station, 'raw', 'T6_N61'): 'Tcase',
            Name(station, 'raw', 'T7_N61'): 'Tboard',
            Name(station, 'raw', 'TD1_N61'): 'TDgrowth',
            Name(station, 'raw', 'Q_N61'): 'Qsample',
            Name(station, 'raw', 'P_N61'): 'Psample',
            Name(station, 'raw', 'PCT_N61'): 'PCTwick',
            Name(station, 'raw', 'V_N61'): 'Vpulse',
        }, send
    )
    station_profile_data['aerosol']['realtime']['cpcstatus'] = {
        RealtimeTranslator.Key('Tu_N61'): 'Tinlet',
        RealtimeTranslator.Key('TDu_N61'): 'TDinlet',
        RealtimeTranslator.Key('Uu_N61'): 'Uinlet',
        RealtimeTranslator.Key('T1_N61'): 'Tconditioner',
        RealtimeTranslator.Key('T2_N61'): 'Tinitiator',
        RealtimeTranslator.Key('T3_N61'): 'Tmoderator',
        RealtimeTranslator.Key('T4_N61'): 'Toptics',
        RealtimeTranslator.Key('T5_N61'): 'Theatsink',
        RealtimeTranslator.Key('T6_N61'): 'Tcase',
        RealtimeTranslator.Key('T7_N61'): 'Tboard',
        RealtimeTranslator.Key('TD1_N61'): 'TDgrowth',
        RealtimeTranslator.Key('Q_N61'): 'Qsample',
        RealtimeTranslator.Key('P_N61'): 'Psample',
        RealtimeTranslator.Key('PCT_N61'): 'PCTwick',
        RealtimeTranslator.Key('V_N61'): 'Vpulse',
    }

    station_profile_data['aerosol']['raw']['ccnstatus'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'Tu_N12'): 'Tinlet',
            Name(station, 'raw', 'T1_N12'): 'Ttec1',
            Name(station, 'raw', 'T2_N12'): 'Ttec2',
            Name(station, 'raw', 'T3_N12'): 'Ttec3',
            Name(station, 'raw', 'T4_N12'): 'Tsample',
            Name(station, 'raw', 'T5_N12'): 'Topc',
            Name(station, 'raw', 'T6_N12'): 'Tnafion',
            Name(station, 'raw', 'Q1_N12'): 'Qsample',
            Name(station, 'raw', 'Q2_N12'): 'Qsheath',
            Name(station, 'raw', 'Uc_N12'): 'SScalc',
            Name(station, 'raw', 'U_N12'): 'SSset',
        }, send
    )
    station_profile_data['aerosol']['realtime']['ccnstatus'] = {
        RealtimeTranslator.Key('Tu_N12'): 'Tinlet',
        RealtimeTranslator.Key('T1_N12'): 'Ttec1',
        RealtimeTranslator.Key('T2_N12'): 'Ttec2',
        RealtimeTranslator.Key('T3_N12'): 'Ttec3',
        RealtimeTranslator.Key('T4_N12'): 'Tsample',
        RealtimeTranslator.Key('T5_N12'): 'Topc',
        RealtimeTranslator.Key('T6_N12'): 'Tnafion',
        RealtimeTranslator.Key('Q1_N12'): 'Qsample',
        RealtimeTranslator.Key('Q2_N12'): 'Qsheath',
        RealtimeTranslator.Key('Uc_N12'): 'SScalc',
        RealtimeTranslator.Key('U_N12'): 'SSset',
    }

    def get(station: str, data_name: str, start_epoch_ms: int, end_epoch_ms: int,
            send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
        return data_profile_get(station, data_name, start_epoch_ms, end_epoch_ms, send, station_profile_data)

else:
    from ..default.data import aerosol_data, aerosol_public, ozone_data, ozone_public, met_data, radiation_data, data_get, \
        DataStream, DataRecord, RealtimeRecord, Selection, RealtimeSelection, STANDARD_CUT_SIZE_SPLIT

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
            "ccn": [Selection(variable_name="number_concentration",
                              require_tags={"dmtccn"})],
        })
    data_records["aerosol-realtime-cnc"] = RealtimeRecord({
        "cnc": [RealtimeSelection("N", variable_name="number_concentration",
                                  require_tags={"cpc"}, exclude_tags={"secondary"})],
        "ccn": [RealtimeSelection("N", variable_name="number_concentration",
                                  require_tags={"dmtccn"})],
    })

    data_records[f"aerosol-raw-wind"] = DataRecord({
        "WSambient": [Selection(variable_id="WS1", instrument_id="XM1")],
        "WDambient": [Selection(variable_id="WD1", instrument_id="XM1")],
        "WSfilter": [Selection(variable_id="WS", instrument_id="XM2")],
        "WDfilter": [Selection(variable_id="WD", instrument_id="XM2")],
    })

    data_records["aerosol-raw-flow"] = DataRecord({
        "sample": [Selection(variable_id="Q_Q11")],
        "filter": [Selection(variable_id="Q_Q21")],
        "filter2": [Selection(variable_id="Q_Q31")],
        "pitot": [Selection(variable_id="Pd_P01")],
    })
    data_records["aerosol-realtime-flow"] = RealtimeRecord({
        "sample": [RealtimeSelection("Q_Q11", variable_id="Q_Q11")],
        "filter": [RealtimeSelection("Q_Q21", variable_id="Q_Q21")],
        "filter2": [RealtimeSelection("Q_Q31", variable_id="Q_Q31")],
        "pitot": [RealtimeSelection("Pd_P01", variable_id="Pd_P01")],
    })

    data_records["aerosol-raw-temperature"] = DataRecord({
        "Tinlet": [Selection(variable_id="T_V51")], "Uinlet": [Selection(variable_id="U_V51")],
        "Taux": [Selection(variable_id="T_V01")], "Uaux": [Selection(variable_id="U_V01")],
        "Tambient": [Selection(variable_id="T1", instrument_id="XM1")],
        "Uambient": [Selection(variable_id="U1", instrument_id="XM1")],
        "TDambient": [Selection(variable_id="TD1", instrument_id="XM1")],

        "Tsample": [Selection(variable_id="T_V11")], "Usample": [Selection(variable_id="U_V11")],

        "Tnephinlet": [Selection(variable_name="inlet_temperature", require_tags={"scattering"}, exclude_tags={"secondary"})],
        "Unephinlet": [Selection(variable_name="inlet_humidity", require_tags={"scattering"}, exclude_tags={"secondary"})],
        "Tneph": [Selection(variable_name="sample_temperature", require_tags={"scattering"}, exclude_tags={"secondary"})],
        "Uneph": [Selection(variable_name="sample_humidity", require_tags={"scattering"}, exclude_tags={"secondary"})],

        "Tfilter": [Selection(variable_id="T_V21")], "Ufilter": [Selection(variable_id="U_V21")],
        "Tfilterrack": [Selection(variable_id="T_V22")],
    })
    data_records["aerosol-realtime-temperature"] = RealtimeRecord({
        "Tinlet": [RealtimeSelection("T_V51", variable_id="T_V51")], "Uinlet": [RealtimeSelection("U_V51", variable_id="U_V51")],
        "Taux": [RealtimeSelection("T_V01", variable_id="T_V01")], "Uaux": [RealtimeSelection("U_V01", variable_id="U_V01")],
        "Tambient": [RealtimeSelection("Tambient", variable_id="T1", instrument_id="XM1")],
        "Uambient": [RealtimeSelection("Uambient", variable_id="U1", instrument_id="XM1")],
        "TDambient": [RealtimeSelection("TDambient", variable_id="TD1", instrument_id="XM1")],

        "Tsample": [RealtimeSelection("T_V11", variable_id="T_V11")], "Usample": [RealtimeSelection("U_V11", variable_id="U_V11")],

        "Tnephinlet": [RealtimeSelection("Tinlet", variable_name="inlet_temperature", require_tags={"scattering"}, exclude_tags={"secondary"})],
        "Unephinlet": [RealtimeSelection("Uinlet", variable_name="inlet_humidity", require_tags={"scattering"}, exclude_tags={"secondary"})],
        "Tneph": [RealtimeSelection("Tsample", variable_name="sample_temperature", require_tags={"scattering"}, exclude_tags={"secondary"})],
        "Uneph": [RealtimeSelection("Usample", variable_name="sample_humidity", require_tags={"scattering"}, exclude_tags={"secondary"})],

        "Tfilter": [RealtimeSelection("T_V21", variable_id="T_V21")], "Ufilter": [RealtimeSelection("U_V21", variable_id="U_V21")],
        "Tfilterrack": [RealtimeSelection("T_V22", variable_id="T_V22")],
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

    data_records["aerosol-raw-filterstatus"] = DataRecord(dict(
        [("Fn", [Selection(variable_id="Fn", instrument_id="F21")]),
         ("mode", [Selection(variable_name="mode", instrument_id="F21")])] + [
        (f"Pd{i+1}", [Selection(variable_id=f"Pd_P2{i+1}")])
        for i in range(8)
    ]), hold_fields={"Fn", "mode"})
    data_records["aerosol-realtime-filterstatus"] = RealtimeRecord(dict([
        ("Fn", [RealtimeSelection("Fn", variable_id="Fn", instrument_id="F21")])] + [
        (f"Pd{i+1}", [RealtimeSelection(f"Pd_P2{i+1}", variable_id=f"Pd_P2{i+1}")])
        for i in range(8)
    ]))

    data_records["aerosol-raw-filterstatus2"] = DataRecord(dict(
        [("Fn", [Selection(variable_id="Fn", instrument_id="F31")]),
         ("mode", [Selection(variable_name="mode", instrument_id="F31")])] + [
        (f"Pd{i+1}", [Selection(variable_id=f"Pd_P3{i+1}")])
        for i in range(8)
    ]), hold_fields={"Fn", "mode"})
    data_records["aerosol-realtime-filterstatus2"] = RealtimeRecord(dict([
        ("Fn", [RealtimeSelection("Fn", variable_id="Fn", instrument_id="F31")])] + [
        (f"Pd{i+1}", [RealtimeSelection(f"Pd_P3{i+1}", variable_id=f"Pd_P3{i+1}")])
        for i in range(8)
    ]))

    data_records["aerosol-raw-umacstatus"] = DataRecord({
        "T": [Selection(variable_name="board_temperature", instrument_code="campbellcr1000gmd", exclude_tags={"secondary"}),
              Selection(variable_name="board_temperature", instrument_code="azonixumac1050", exclude_tags={"secondary"})],
        "V": [Selection(variable_name="supply_voltage", instrument_code="campbellcr1000gmd", exclude_tags={"secondary"}),
              Selection(variable_name="board_voltage", instrument_code="azonixumac1050", exclude_tags={"secondary"})],

        "Tfilter": [Selection(variable_id="T", instrument_id="X3")],
        "Vfilter": [Selection(variable_id="V", instrument_id="X3")],
        "Tfilter2": [Selection(variable_id="T", instrument_id="X5")],
        "Vfilter2": [Selection(variable_id="V", instrument_id="X5")],
    })
    data_records["aerosol-realtime-umacstatus"] = RealtimeRecord({
        "T": [RealtimeSelection("T", variable_name="board_temperature", instrument_code="campbellcr1000gmd", exclude_tags={"secondary"}),
              RealtimeSelection("T", variable_name="board_temperature", instrument_code="azonixumac1050", exclude_tags={"secondary"})],
        "V": [RealtimeSelection("V", variable_name="supply_voltage", instrument_code="campbellcr1000gmd", exclude_tags={"secondary"}),
              RealtimeSelection("V", variable_name="board_voltage", instrument_code="azonixumac1050", exclude_tags={"secondary"})],

        "Tfilter": [RealtimeSelection("T", variable_id="T", instrument_id="X3")],
        "Vfilter": [RealtimeSelection("V", variable_id="V", instrument_id="X3")],
        "Tfilter2": [RealtimeSelection("T", variable_id="T", instrument_id="X5")],
        "Vfilter2": [RealtimeSelection("V", variable_id="V", instrument_id="X5")],
    })

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