import typing
from ..cpd3 import use_cpd3


if use_cpd3("egb"):
    from ..cpd3 import DataStream, DataReader, EditedReader, RealtimeTranslator, Name, data_profile_get, detach, profile_data

    station_profile_data = detach(profile_data)


    station_profile_data['aerosol']['raw']['cpcstatus'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'T1_N61'): 'Tsaturator',
            Name(station, 'raw', 'T2_N61'): 'Tcondenser',
            Name(station, 'raw', 'T3_N61'): 'Toptics',
            Name(station, 'raw', 'T4_N61'): 'Tcabinet',
            Name(station, 'raw', 'Q_N61'): 'Qsample',
            Name(station, 'raw', 'Qu_N61'): 'Qinlet',
            Name(station, 'raw', 'P_N61'): 'Psample',
            Name(station, 'raw', 'Pd1_N61'): 'PDnozzle',
            Name(station, 'raw', 'Pd2_N61'): 'PDorifice',
            Name(station, 'raw', 'A_N61'): 'Alaser',
        }, send
    )
    station_profile_data['aerosol']['realtime']['cpcstatus'] = {
        RealtimeTranslator.Key('T1_N61'): 'Tsaturator',
        RealtimeTranslator.Key('T2_N61'): 'Tcondenser',
        RealtimeTranslator.Key('T3_N61'): 'Toptics',
        RealtimeTranslator.Key('T4_N61'): 'Tcabinet',
        RealtimeTranslator.Key('Q_N61'): 'Qsample',
        RealtimeTranslator.Key('Qu_N61'): 'Qinlet',
        RealtimeTranslator.Key('P_N61'): 'Psample',
        RealtimeTranslator.Key('Pd1_N61'): 'PDnozzle',
        RealtimeTranslator.Key('Pd2_N61'): 'PDorifice',
        RealtimeTranslator.Key('A_N61'): 'Alaser',
    }


    station_profile_data['aerosol']['raw']['smps'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'Ns_N12'): 'Dp',
            Name(station, 'raw', 'Nn_N12'): 'dNdlogDp',
            Name(station, 'raw', 'Nb_N12'): 'dN',
        }, send
    )
    station_profile_data['aerosol']['editing']['smps'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
        start_epoch_ms, end_epoch_ms, station, 'aerosol', {
            Name(station, 'clean', 'Ns_N12'): 'Dp',
            Name(station, 'clean', 'Nn_N12'): 'dNdlogDp',
            Name(station, 'clean', 'Nb_N12'): 'dN',
            Name(station, 'clean', 'N_N12'): 'N',
            Name(station, 'clean', 'BsB_N12'): 'BsB',
            Name(station, 'clean', 'BsG_N12'): 'BsG',
            Name(station, 'clean', 'BsR_N12'): 'BsR',
        }, send
    )
    station_profile_data['aerosol']['clean']['smps'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'clean', 'Ns_N12'): 'Dp',
            Name(station, 'clean', 'Nn_N12'): 'dNdlogDp',
            Name(station, 'clean', 'Nb_N12'): 'dN',
            Name(station, 'clean', 'N_N12'): 'N',
            Name(station, 'clean', 'BsB_N12'): 'BsB',
            Name(station, 'clean', 'BsG_N12'): 'BsG',
            Name(station, 'clean', 'BsR_N12'): 'BsR',
        }, send
    )
    station_profile_data['aerosol']['avgh']['smps'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'avgh', 'Ns_N12'): 'Dp',
            Name(station, 'avgh', 'Nn_N12'): 'dNdlogDp',
            Name(station, 'avgh', 'Nb_N12'): 'dN',
            Name(station, 'avgh', 'N_N12'): 'N',
            Name(station, 'avgh', 'BsB_N12'): 'BsB',
            Name(station, 'avgh', 'BsG_N12'): 'BsG',
            Name(station, 'avgh', 'BsR_N12'): 'BsR',
        }, send
    )


    station_profile_data['aerosol']['raw']['grimm'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'Ns_N11'): 'Dp',
            Name(station, 'raw', 'Nn_N11'): 'dNdlogDp',
            Name(station, 'raw', 'Nb_N11'): 'dN',
        }, send
    )
    station_profile_data['aerosol']['realtime']['grimm'] = {
        RealtimeTranslator.Key('Ns_N11'): 'Dp',
        RealtimeTranslator.Key('Nn_N11'): 'dNdlogDp',
        RealtimeTranslator.Key('Nb_N11'): 'dN',
    }
    station_profile_data['aerosol']['editing']['grimm'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
        start_epoch_ms, end_epoch_ms, station, 'aerosol', {
            Name(station, 'clean', 'Ns_N11'): 'Dp',
            Name(station, 'clean', 'Nn_N11'): 'dNdlogDp',
            Name(station, 'clean', 'Nb_N11'): 'dN',
            Name(station, 'clean', 'N_N11'): 'N',
            Name(station, 'clean', 'BsB_N11'): 'BsB',
            Name(station, 'clean', 'BsG_N11'): 'BsG',
            Name(station, 'clean', 'BsR_N11'): 'BsR',
        }, send
    )
    station_profile_data['aerosol']['clean']['grimm'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'clean', 'Ns_N11'): 'Dp',
            Name(station, 'clean', 'Nn_N11'): 'dNdlogDp',
            Name(station, 'clean', 'Nb_N11'): 'dN',
            Name(station, 'clean', 'N_N11'): 'N',
            Name(station, 'clean', 'BsB_N11'): 'BsB',
            Name(station, 'clean', 'BsG_N11'): 'BsG',
            Name(station, 'clean', 'BsR_N11'): 'BsR',
        }, send
    )
    station_profile_data['aerosol']['avgh']['grimm'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'avgh', 'Ns_N11'): 'Dp',
            Name(station, 'avgh', 'Nn_N11'): 'dNdlogDp',
            Name(station, 'avgh', 'Nb_N11'): 'dN',
            Name(station, 'avgh', 'N_N11'): 'N',
            Name(station, 'avgh', 'BsB_N11'): 'BsB',
            Name(station, 'avgh', 'BsG_N11'): 'BsG',
            Name(station, 'avgh', 'BsR_N11'): 'BsR',
        }, send
    )

    station_profile_data['aerosol']['raw']['grimmstatus'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'Q_N11'): 'Qsample',
        }, send
    )
    station_profile_data['aerosol']['realtime']['grimmstatus'] = {
        RealtimeTranslator.Key('Q_N11'): 'Qsample',
    }


    def get(station: str, data_name: str, start_epoch_ms: int, end_epoch_ms: int,
            send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
        return data_profile_get(station, data_name, start_epoch_ms, end_epoch_ms, send, station_profile_data)

else:
    from ..default.data import aerosol_data, aerosol_public, data_get, DataStream, DataRecord, RealtimeRecord, Selection, RealtimeSelection, STANDARD_THREE_WAVELENGTHS

    data_records = dict()
    data_records.update(aerosol_data)
    data_records.update(aerosol_public)

    data_records["aerosol-raw-cpcstatus"] = DataRecord({
        "Tsaturator": [Selection(variable_name="saturator_temperature",
                                 instrument_code="tsi377xcpc", exclude_tags={"secondary"})],
        "Tcondenser": [Selection(variable_name="condenser_temperature",
                                 instrument_code="tsi377xcpc", exclude_tags={"secondary"})],
        "Toptics": [Selection(variable_name="optics_temperature",
                              instrument_code="tsi377xcpc", exclude_tags={"secondary"})],
        "Tcabinet": [Selection(variable_name="cabinet_temperature",
                               instrument_code="tsi377xcpc", exclude_tags={"secondary"})],
        "Qsample": [Selection(variable_name="sample_flow",
                              instrument_code="tsi377xcpc", exclude_tags={"secondary"})],
        "Qinlet": [Selection(variable_name="inlet_flow",
                             instrument_code="tsi377xcpc", exclude_tags={"secondary"})],
        "Psample": [Selection(variable_name="pressure",
                              instrument_code="tsi377xcpc", exclude_tags={"secondary"})],
        "PDnozzle": [Selection(variable_name="nozzle_pressure_drop",
                               instrument_code="tsi377xcpc", exclude_tags={"secondary"})],
        "PDorifice": [Selection(variable_name="orifice_pressure_drop",
                                instrument_code="tsi377xcpc", exclude_tags={"secondary"})],
        "Alaser": [Selection(variable_name="laser_current",
                             instrument_code="tsi377xcpc", exclude_tags={"secondary"})],
    })
    data_records["aerosol-realtime-cpcstatus"] = RealtimeRecord({
        "Tsaturator": [RealtimeSelection("Tsaturator", variable_name="saturator_temperature",
                                         instrument_code="tsi377xcpc", exclude_tags={"secondary"})],
        "Tcondenser": [RealtimeSelection("Tcondenser", variable_name="condenser_temperature",
                                         instrument_code="tsi377xcpc", exclude_tags={"secondary"})],
        "Toptics": [RealtimeSelection("Toptics", variable_name="optics_temperature",
                                      instrument_code="tsi377xcpc", exclude_tags={"secondary"})],
        "Tcabinet": [RealtimeSelection("Tcabinet", variable_name="cabinet_temperature",
                                       instrument_code="tsi377xcpc", exclude_tags={"secondary"})],
        "Qsample": [RealtimeSelection("Q", variable_name="sample_flow",
                                      instrument_code="tsi377xcpc", exclude_tags={"secondary"})],
        "Qinlet": [RealtimeSelection("Qinlet", variable_name="inlet_flow",
                                     instrument_code="tsi377xcpc", exclude_tags={"secondary"})],
        "Psample": [RealtimeSelection("P", variable_name="pressure",
                                      instrument_code="tsi377xcpc", exclude_tags={"secondary"})],
        "PDnozzle": [RealtimeSelection("PDnozzle", variable_name="nozzle_pressure_drop",
                                       instrument_code="tsi377xcpc", exclude_tags={"secondary"})],
        "PDorifice": [RealtimeSelection("PDorifice", variable_name="orifice_pressure_drop",
                                        instrument_code="tsi377xcpc", exclude_tags={"secondary"})],
        "Alaser": [RealtimeSelection("Alaser", variable_name="laser_current",
                                     instrument_code="tsi377xcpc", exclude_tags={"secondary"})],
    })

    data_records["aerosol-raw-grimm"] = DataRecord({
        "Dp": [Selection(variable_id="Ns", instrument_code="grimm110xopc")],
        "dNdlogDp": [Selection(variable_id="Nn", instrument_code="grimm110xopc")],
        "dN": [Selection(variable_id="Nb", instrument_code="grimm110xopc")],
    })
    for archive in ("editing", "clean", "avgh"):
        data_records[f"aerosol-{archive}-grimm"] = DataRecord(dict([
            ("Dp", [Selection(variable_id="Ns", instrument_code="grimm110xopc")]),
            ("dNdlogDp", [Selection(variable_id="Nn", instrument_code="grimm110xopc")]),
            ("dN", [Selection(variable_id="Nb", instrument_code="grimm110xopc")]),
            ("N", [Selection(variable_id="N", instrument_code="grimm110xopc")])] + [
                (f"Bs{code}", [Selection(variable_id="Bs", wavelength=wavelength,
                                         instrument_code="grimm110xopc")])
                for code, wavelength in STANDARD_THREE_WAVELENGTHS
            ])
        )
    data_records["aerosol-raw-grimmstatus"] = DataRecord({
        "Qsample": [Selection(variable_name="sample_flow", instrument_code="grimm110xopc")],
    })
    data_records["aerosol-realtime-grimmstatus"] = RealtimeRecord({
        "Qsample": [RealtimeSelection("Q", variable_name="sample_flow", instrument_code="grimm110xopc")],
    })

    data_records["aerosol-raw-smps"] = DataRecord({
        "Dp": [Selection(variable_id="Ns", instrument_id="N12")],
        "dNdlogDp": [Selection(variable_id="Nn", instrument_id="N12")],
        "dN": [Selection(variable_id="Nb", instrument_id="N12")],
    })
    for archive in ("editing", "clean", "avgh"):
        data_records[f"aerosol-{archive}-smps"] = DataRecord(dict([
            ("Dp", [Selection(variable_id="Ns", instrument_id="N12")]),
            ("dNdlogDp", [Selection(variable_id="Nn", instrument_id="N12")]),
            ("dN", [Selection(variable_id="Nb", instrument_id="N12")]),
            ("N", [Selection(variable_id="N", instrument_id="N12")])] + [
                (f"Bs{code}", [Selection(variable_id="Bs", wavelength=wavelength,
                                         instrument_id="N12")])
                for code, wavelength in STANDARD_THREE_WAVELENGTHS
            ])
        )

    def get(station: str, data_name: str, start_epoch_ms: int, end_epoch_ms: int,
            send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
        return data_get(station, data_name, start_epoch_ms, end_epoch_ms, send, data_records)
