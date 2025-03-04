import typing
from math import nan
from ..cpd3 import use_cpd3


if use_cpd3("spl"):
    from ..cpd3 import DataStream, DataReader, EditedReader, Name, RealtimeTranslator, data_profile_get, detach, profile_data

    station_profile_data = detach(profile_data)


    station_profile_data['aerosol']['raw']['cnc'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'N_N71'): 'cnc',
            Name(station, 'raw', 'N_N74'): 'cnc2',
            Name(station, 'raw', 'N_N73'): 'ccn',
        }, send
    )
    station_profile_data['aerosol']['realtime']['cnc'] = {
        RealtimeTranslator.Key('N_N71'): 'cnc',
        RealtimeTranslator.Key('N_N74'): 'cnc2',
        RealtimeTranslator.Key('N_N73'): 'ccn',
    }
    station_profile_data['aerosol']['editing']['cnc'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
        start_epoch_ms, end_epoch_ms, station, 'aerosol', {
            Name(station, 'clean', 'N_N71'): 'cnc',
            Name(station, 'clean', 'N_N74'): 'cnc2',
            Name(station, 'clean', 'N_N73'): 'ccn',
        }, send
    )
    station_profile_data['aerosol']['clean']['cnc'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'clean', 'N_N71'): 'cnc',
            Name(station, 'clean', 'N_N74'): 'cnc2',
            Name(station, 'clean', 'N_N73'): 'ccn',
        }, send
    )
    station_profile_data['aerosol']['avgh']['cnc'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'avgh', 'N_N71'): 'cnc',
            Name(station, 'avgh', 'N_N74'): 'cnc2',
            Name(station, 'avgh', 'N_N73'): 'ccn',
        }, send
    )

    station_profile_data['aerosol']['raw']['cpcstatus'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'Q_Q71'): 'Qsample',
            Name(station, 'raw', 'Q_Q72'): 'Qdrier',
            Name(station, 'raw', 'T1_N71'): 'Tsaturator',
            Name(station, 'raw', 'T2_N71'): 'Tcondenser',
        }, send
    )
    station_profile_data['aerosol']['raw']['cpcstatus2'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'T1_N74'): 'Tsaturator',
            Name(station, 'raw', 'T2_N74'): 'Tcondenser',
            Name(station, 'raw', 'T3_N74'): 'Toptics',
            Name(station, 'raw', 'T4_N74'): 'Tcabinet',
            Name(station, 'raw', 'Q_N74'): 'Qsample',
            Name(station, 'raw', 'Qu_N74'): 'Qinlet',
            Name(station, 'raw', 'P_N74'): 'Psample',
            Name(station, 'raw', 'Pd1_N74'): 'PDnozzle',
            Name(station, 'raw', 'Pd2_N74'): 'PDorifice',
            Name(station, 'raw', 'A_N74'): 'Alaser',
        }, send
    )
    station_profile_data['aerosol']['realtime']['cpcstatus'] = {
        RealtimeTranslator.Key('Q_Q71'): 'Qsample',
        RealtimeTranslator.Key('Q_Q72'): 'Qdrier',
        RealtimeTranslator.Key('T1_N71'): 'Tsaturator',
        RealtimeTranslator.Key('T2_N71'): 'Tcondenser',
    }
    station_profile_data['aerosol']['realtime']['cpcstatus2'] = {
        RealtimeTranslator.Key('T1_N74'): 'Tsaturator',
        RealtimeTranslator.Key('T2_N74'): 'Tcondenser',
        RealtimeTranslator.Key('T3_N74'): 'Toptics',
        RealtimeTranslator.Key('T4_N74'): 'Tcabinet',
        RealtimeTranslator.Key('Q_N74'): 'Qsample',
        RealtimeTranslator.Key('Qu_N74'): 'Qinlet',
        RealtimeTranslator.Key('P_N74'): 'Psample',
        RealtimeTranslator.Key('Pd1_N74'): 'PDnozzle',
        RealtimeTranslator.Key('Pd2_N74'): 'PDorifice',
        RealtimeTranslator.Key('A_N74'): 'Alaser',
    }


    station_profile_data['aerosol']['raw']['ccnstatus'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'Tu_N73'): 'Tinlet',
            Name(station, 'raw', 'T1_N73'): 'Ttec1',
            Name(station, 'raw', 'T2_N73'): 'Ttec2',
            Name(station, 'raw', 'T3_N73'): 'Ttec3',
            Name(station, 'raw', 'T4_N73'): 'Tsample',
            Name(station, 'raw', 'T5_N73'): 'Topc',
            Name(station, 'raw', 'T6_N73'): 'Tnafion',
            Name(station, 'raw', 'Q1_N73'): 'Qsample',
            Name(station, 'raw', 'Q2_N73'): 'Qsheath',
            Name(station, 'raw', 'Uc_N73'): 'SScalc',
            Name(station, 'raw', 'U_N73'): 'SSset',
        }, send
    )
    station_profile_data['aerosol']['realtime']['ccnstatus'] = {
        RealtimeTranslator.Key('Tu_N73'): 'Tinlet',
        RealtimeTranslator.Key('T1_N73'): 'Ttec1',
        RealtimeTranslator.Key('T2_N73'): 'Ttec2',
        RealtimeTranslator.Key('T3_N73'): 'Ttec3',
        RealtimeTranslator.Key('T4_N73'): 'Tsample',
        RealtimeTranslator.Key('T5_N73'): 'Topc',
        RealtimeTranslator.Key('T6_N73'): 'Tnafion',
        RealtimeTranslator.Key('Q1_N73'): 'Qsample',
        RealtimeTranslator.Key('Q2_N73'): 'Qsheath',
        RealtimeTranslator.Key('Uc_N73'): 'SScalc',
        RealtimeTranslator.Key('U_N73'): 'SSset',
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
            "cnc": [Selection(variable_name="number_concentration",
                              require_tags={"cpc"}, exclude_tags={"secondary"})],
            "cnc2": [Selection(variable_name="number_concentration", instrument_id="N74")],
            "ccn": [Selection(variable_name="number_concentration",
                              require_tags={"dmtccn"})],
        })
    data_records["aerosol-realtime-cnc"] = RealtimeRecord({
        "cnc": [RealtimeSelection("N", variable_name="number_concentration",
                                  require_tags={"cpc"}, exclude_tags={"secondary"})],
        "cnc2": [RealtimeSelection("N", variable_name="number_concentration",
                                   instrument_id="N74")],
        "ccn": [RealtimeSelection("N", variable_name="number_concentration",
                                  require_tags={"dmtccn"})],
    })

    data_records["aerosol-raw-cpcstatus"] = DataRecord({
        "Qsample": [Selection(variable_name="sample_flow",
                              require_tags={"cpc"}, exclude_tags={"secondary"}),
                    Selection(variable_id="Q_Q71"), Selection(variable_id="Q_Q61")],
        "Qdrier": [Selection(variable_id="Q_Q72"), Selection(variable_id="Q_Q62")],
        "Tsaturator": [Selection(variable_name="saturator_temperature", instrument_code="tsi3010cpc",
                                 exclude_tags={"secondary"})],
        "Tcondenser": [Selection(variable_name="condenser_temperature", instrument_code="tsi3010cpc",
                                 exclude_tags={"secondary"})],
    })
    data_records["aerosol-realtime-cpcstatus"] = RealtimeRecord({
        "Qsample": [RealtimeSelection("Q", variable_name="sample_flow",
                                      require_tags={"cpc"}, exclude_tags={"secondary"}),
                    RealtimeSelection("Q_Q71", variable_id="Q_Q71"),
                    RealtimeSelection("Q_Q61", variable_id="Q_Q61")],
        "Qdrier": [RealtimeSelection("Q_Q72", variable_id="Q_Q72"),
                   RealtimeSelection("Q_Q62", variable_id="Q_Q62")],
        "Tsaturator": [RealtimeSelection("Tsaturator", variable_name="saturator_temperature",
                                         instrument_code="tsi3010cpc", exclude_tags={"secondary"})],
        "Tcondenser": [RealtimeSelection("Tcondenser", variable_name="condenser_temperature",
                                         instrument_code="tsi3010cpc", exclude_tags={"secondary"})],
    })

    data_records["aerosol-raw-cpcstatus2"] = DataRecord({
        "Tsaturator": [Selection(variable_name="saturator_temperature",
                                 instrument_code="tsi377xcpc", instrument_id="N74")],
        "Tcondenser": [Selection(variable_name="condenser_temperature",
                                 instrument_code="tsi377xcpc", instrument_id="N74")],
        "Toptics": [Selection(variable_name="optics_temperature",
                              instrument_code="tsi377xcpc", instrument_id="N74")],
        "Tcabinet": [Selection(variable_name="cabinet_temperature",
                               instrument_code="tsi377xcpc", instrument_id="N74")],
        "Qsample": [Selection(variable_name="sample_flow",
                              instrument_code="tsi377xcpc", instrument_id="N74")],
        "Qinlet": [Selection(variable_name="inlet_flow",
                             instrument_code="tsi377xcpc", instrument_id="N74")],
        "Psample": [Selection(variable_name="pressure",
                              instrument_code="tsi377xcpc", instrument_id="N74")],
        "PDnozzle": [Selection(variable_name="nozzle_pressure_drop",
                               instrument_code="tsi377xcpc", instrument_id="N74")],
        "PDorifice": [Selection(variable_name="orifice_pressure_drop",
                                instrument_code="tsi377xcpc", instrument_id="N74")],
        "Alaser": [Selection(variable_name="laser_current",
                             instrument_code="tsi377xcpc", instrument_id="N74")],
    })
    data_records["aerosol-realtime-cpcstatus2"] = RealtimeRecord({
        "Tsaturator": [RealtimeSelection("Tsaturator", variable_name="saturator_temperature",
                                         instrument_code="tsi377xcpc", instrument_id="N74")],
        "Tcondenser": [RealtimeSelection("Tcondenser", variable_name="condenser_temperature",
                                         instrument_code="tsi377xcpc", instrument_id="N74")],
        "Toptics": [RealtimeSelection("Toptics", variable_name="optics_temperature",
                                      instrument_code="tsi377xcpc", instrument_id="N74")],
        "Tcabinet": [RealtimeSelection("Tcabinet", variable_name="cabinet_temperature",
                                       instrument_code="tsi377xcpc", instrument_id="N74")],
        "Qsample": [RealtimeSelection("Q", variable_name="sample_flow",
                                      instrument_code="tsi377xcpc", instrument_id="N74")],
        "Qinlet": [RealtimeSelection("Qinlet", variable_name="inlet_flow",
                                     instrument_code="tsi377xcpc", instrument_id="N74")],
        "Psample": [RealtimeSelection("P", variable_name="pressure",
                                      instrument_code="tsi377xcpc", instrument_id="N74")],
        "PDnozzle": [RealtimeSelection("PDnozzle", variable_name="nozzle_pressure_drop",
                                       instrument_code="tsi377xcpc", instrument_id="N74")],
        "PDorifice": [RealtimeSelection("PDorifice", variable_name="orifice_pressure_drop",
                                        instrument_code="tsi377xcpc", instrument_id="N74")],
        "Alaser": [RealtimeSelection("Alaser", variable_name="laser_current",
                                     instrument_code="tsi377xcpc", instrument_id="N74")],
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
