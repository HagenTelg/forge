import typing
from math import nan
from ..cpd3 import use_cpd3


if use_cpd3("arn"):
    from ..cpd3 import DataStream, DataReader, EditedReader, Name, RealtimeTranslator, data_profile_get, detach, profile_data

    station_profile_data = detach(profile_data)


    station_profile_data['aerosol']['raw']['contamination'] = lambda station, start_epoch_ms, end_epoch_ms, send: ContaminationReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'F1_N23'),
            Name(station, 'raw', 'F1_S11'),
            Name(station, 'raw', 'F1_A11'),
        }, send
    )
    station_profile_data['aerosol']['clean']['contamination'] = lambda station, start_epoch_ms, end_epoch_ms, send: ContaminationReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'clean', 'F1_N23'),
            Name(station, 'clean', 'F1_S11'),
            Name(station, 'clean', 'F1_A11'),
        }, send
    )
    station_profile_data['aerosol']['avgh']['contamination'] = lambda station, start_epoch_ms, end_epoch_ms, send: ContaminationReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'avgh', 'F1_N23'),
            Name(station, 'avgh', 'F1_S11'),
            Name(station, 'avgh', 'F1_A11'),
        }, send
    )
    station_profile_data['aerosol']['editing']['contamination'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedContaminationReader(
        start_epoch_ms, end_epoch_ms, station, 'aerosol', {
            Name(station, 'clean', 'F1_N23'),
            Name(station, 'clean', 'F1_S11'),
            Name(station, 'clean', 'F1_A11'),
        }, send
    )

    station_profile_data['aerosol']['raw']['cnc'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'N_N23'): 'cnc',
        }, send
    )
    station_profile_data['aerosol']['realtime']['cnc'] = {
        RealtimeTranslator.Key('N_N23'): 'cnc',
    }
    station_profile_data['aerosol']['clean']['cnc'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'clean', 'N_N23'): 'cnc',
        }, send
    )
    station_profile_data['aerosol']['avgh']['cnc'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'avgh', 'N_N23'): 'cnc',
        }, send
    )
    station_profile_data['aerosol']['editing']['cnc'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
        start_epoch_ms, end_epoch_ms, station, 'aerosol', {
            Name(station, 'clean', 'N_N23'): 'cnc',
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
            Name(station, 'raw', 'T_Q11'): 'Tsample',
            Name(station, 'raw', 'T_Q11', {'pm10'}): 'Tsample',
            Name(station, 'raw', 'T_Q11', {'pm1'}): 'Tsample',
            Name(station, 'raw', 'T_Q11', {'pm25'}): 'Tsample',

            Name(station, 'raw', 'T_Q12'): 'Tdilution',
            Name(station, 'raw', 'T_Q12', {'pm10'}): 'Tdilution',
            Name(station, 'raw', 'T_Q12', {'pm1'}): 'Tdilution',
            Name(station, 'raw', 'T_Q12', {'pm25'}): 'Tdilution',

            Name(station, 'raw', 'Tu_S11'): 'Tnephinlet', Name(station, 'raw', 'Uu_S11'): 'Unephinlet',
            Name(station, 'raw', 'Tu_S11', {'pm10'}): 'Tnephinlet', Name(station, 'raw', 'Uu_S11', {'pm10'}): 'Unephinlet',
            Name(station, 'raw', 'Tu_S11', {'pm1'}): 'Tnephinlet', Name(station, 'raw', 'Uu_S11', {'pm1'}): 'Unephinlet',
            Name(station, 'raw', 'Tu_S11', {'pm25'}): 'Tnephinlet', Name(station, 'raw', 'Uu_S11', {'pm25'}): 'Unephinlet',

            Name(station, 'raw', 'T_S11'): 'Tneph', Name(station, 'raw', 'U_S11'): 'Uneph',
            Name(station, 'raw', 'T_S11', {'pm10'}): 'Tneph', Name(station, 'raw', 'U_S11', {'pm10'}): 'Uneph',
            Name(station, 'raw', 'T_S11', {'pm1'}): 'Tneph', Name(station, 'raw', 'U_S11', {'pm1'}): 'Uneph',
            Name(station, 'raw', 'T_S11', {'pm25'}): 'Tneph', Name(station, 'raw', 'U_S11', {'pm25'}): 'Uneph',
        }, send
    )
    station_profile_data['aerosol']['realtime']['temperature'] = {
        RealtimeTranslator.Key('T_Q11'): 'Tsample',
        RealtimeTranslator.Key('T_Q11', {'pm10'}): 'Tsample',
        RealtimeTranslator.Key('T_Q11', {'pm1'}): 'Tsample',
        RealtimeTranslator.Key('T_Q11', {'pm25'}): 'Tsample',

        RealtimeTranslator.Key('T_Q12'): 'Tdilution',
        RealtimeTranslator.Key('T_Q12', {'pm10'}): 'Tdilution',
        RealtimeTranslator.Key('T_Q12', {'pm1'}): 'Tdilution',
        RealtimeTranslator.Key('T_Q12', {'pm25'}): 'Tdilution',

        RealtimeTranslator.Key('Tu_S11'): 'Tnephinlet', RealtimeTranslator.Key('Uu_S11'): 'Unephinlet',
        RealtimeTranslator.Key('Tu_S11', {'pm10'}): 'Tnephinlet', RealtimeTranslator.Key('Uu_S11', {'pm10'}): 'Unephinlet',
        RealtimeTranslator.Key('Tu_S11', {'pm1'}): 'Tnephinlet', RealtimeTranslator.Key('Uu_S11', {'pm1'}): 'Unephinlet',
        RealtimeTranslator.Key('Tu_S11', {'pm25'}): 'Tnephinlet', RealtimeTranslator.Key('Uu_S11', {'pm25'}): 'Unephinlet',

        RealtimeTranslator.Key('T_S11'): 'Tneph', RealtimeTranslator.Key('U_S11'): 'Uneph',
        RealtimeTranslator.Key('T_S11', {'pm10'}): 'Tneph', RealtimeTranslator.Key('U_S11', {'pm10'}): 'Uneph',
        RealtimeTranslator.Key('T_S11', {'pm1'}): 'Tneph', RealtimeTranslator.Key('U_S11', {'pm1'}): 'Uneph',
        RealtimeTranslator.Key('T_S11', {'pm25'}): 'Tneph', RealtimeTranslator.Key('U_S11', {'pm25'}): 'Uneph',
    }

    station_profile_data['aerosol']['raw']['pressure'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'P_Q12'): 'dilution',
        }, send
    )
    station_profile_data['aerosol']['realtime']['pressure'] = {
        RealtimeTranslator.Key('P_Q12'): 'dilution',
    }

    station_profile_data['aerosol']['raw']['samplepressure-whole'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'P_S11'): 'neph',
            Name(station, 'raw', 'P_Q11'): 'sample',
        }, send
    )
    station_profile_data['aerosol']['raw']['samplepressure-pm10'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'P_S11', {'pm10'}): 'neph',
            Name(station, 'raw', 'P_Q11', {'pm10'}): 'sample',
        }, send
    )
    station_profile_data['aerosol']['raw']['samplepressure-pm25'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'P_S11', {'pm25'}): 'neph',
            Name(station, 'raw', 'P_Q11', {'pm25'}): 'sample',
        }, send
    )
    station_profile_data['aerosol']['raw']['samplepressure-pm1'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'P_S11', {'pm1'}): 'neph',
            Name(station, 'raw', 'P_Q11', {'pm1'}): 'sample',
        }, send
    )
    station_profile_data['aerosol']['realtime']['samplepressure-whole'] = {
        RealtimeTranslator.Key('P_S11'): 'neph',
        RealtimeTranslator.Key('P_Q11'): 'sample',
    }
    station_profile_data['aerosol']['realtime']['samplepressure-pm10'] = {
        RealtimeTranslator.Key('P_S11', {'pm10'}): 'neph',
        RealtimeTranslator.Key('P_Q11', {'pm10'}): 'sample',
    }
    station_profile_data['aerosol']['realtime']['samplepressure-pm25'] = {
        RealtimeTranslator.Key('P_S11', {'pm25'}): 'neph',
        RealtimeTranslator.Key('P_Q11', {'pm25'}): 'sample',
    }
    station_profile_data['aerosol']['realtime']['samplepressure-pm1'] =  {
        RealtimeTranslator.Key('P_S11', {'pm1'}): 'neph',
        RealtimeTranslator.Key('P_Q11', {'pm1'}): 'sample',
    }


    station_profile_data['aerosol']['raw']['cpcstatus'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'T1_N23'): 'Tsaturator',
            Name(station, 'raw', 'T2_N23'): 'Tcondenser',
            Name(station, 'raw', 'T3_N23'): 'Toptics',
            Name(station, 'raw', 'T4_N23'): 'Tcabinet',
            Name(station, 'raw', 'Q_N23'): 'Qsample',
            Name(station, 'raw', 'Qu_N23'): 'Qinlet',
            Name(station, 'raw', 'P_N23'): 'Psample',
            Name(station, 'raw', 'Pd1_N23'): 'PDnozzle',
            Name(station, 'raw', 'Pd2_N23'): 'PDorifice',
            Name(station, 'raw', 'A_N23'): 'Alaser',
        }, send
    )
    station_profile_data['aerosol']['realtime']['cpcstatus'] = {
        RealtimeTranslator.Key('T1_N23'): 'Tsaturator',
        RealtimeTranslator.Key('T2_N23'): 'Tcondenser',
        RealtimeTranslator.Key('T3_N23'): 'Toptics',
        RealtimeTranslator.Key('T4_N23'): 'Tcabinet',
        RealtimeTranslator.Key('Q_N23'): 'Qsample',
        RealtimeTranslator.Key('Qu_N23'): 'Qinlet',
        RealtimeTranslator.Key('P_N23'): 'Psample',
        RealtimeTranslator.Key('Pd1_N23'): 'PDnozzle',
        RealtimeTranslator.Key('Pd2_N23'): 'PDorifice',
        RealtimeTranslator.Key('A_N23'): 'Alaser',
    }


    def get(station: str, data_name: str, start_epoch_ms: int, end_epoch_ms: int,
            send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
        return data_profile_get(station, data_name, start_epoch_ms, end_epoch_ms, send, station_profile_data)

else:
    from ..default.data import aerosol_data, aerosol_public, data_get, DataStream, DataRecord, RealtimeRecord, Selection, RealtimeSelection, STANDARD_CUT_SIZE_SPLIT

    data_records = dict()
    data_records.update(aerosol_data)
    data_records.update(aerosol_public)

    data_records["aerosol-raw-flow"] = DataRecord({
        "sample": [Selection(variable_id="Q", instrument_id="Q11")],
        "dilution": [Selection(variable_id="Q", instrument_id="Q12")],
    })
    data_records["aerosol-realtime-flow"] = RealtimeRecord({
        "sample": [RealtimeSelection("Q", variable_id="Q", instrument_id="Q11")],
        "dilution": [RealtimeSelection("Q", variable_id="Q", instrument_id="Q12")],
    })

    data_records["aerosol-raw-temperature"] = DataRecord({
        "Tsample": [Selection(variable_id="T", instrument_id="Q11")], "Usample": [Selection(variable_id="U", instrument_id="Q11")],
        "Tdilution": [Selection(variable_id="T", instrument_id="Q12")], "Udilution": [Selection(variable_id="U", instrument_id="Q12")],

        "Tnephinlet": [Selection(variable_name="inlet_temperature", require_tags={"scattering"}, exclude_tags={"secondary"})],
        "Unephinlet": [Selection(variable_name="inlet_humidity", require_tags={"scattering"}, exclude_tags={"secondary"})],
        "Tneph": [Selection(variable_name="sample_temperature", require_tags={"scattering"}, exclude_tags={"secondary"})],
        "Uneph": [Selection(variable_name="sample_humidity", require_tags={"scattering"}, exclude_tags={"secondary"})],
    })
    data_records["aerosol-realtime-temperature"] = RealtimeRecord({
        "Tsample": [RealtimeSelection("T", variable_id="T", instrument_id="Q11")], "Usample": [RealtimeSelection("U", variable_id="U", instrument_id="Q11")],
        "Tdilution": [RealtimeSelection("T", variable_id="T", instrument_id="Q12")], "Udilution": [RealtimeSelection("U", variable_id="U", instrument_id="Q12")],

        "Tnephinlet": [RealtimeSelection("Tinlet", variable_name="inlet_temperature", require_tags={"scattering"}, exclude_tags={"secondary"})],
        "Unephinlet": [RealtimeSelection("Uinlet", variable_name="inlet_humidity", require_tags={"scattering"}, exclude_tags={"secondary"})],
        "Tneph": [RealtimeSelection("Tsample", variable_name="sample_temperature", require_tags={"scattering"}, exclude_tags={"secondary"})],
        "Uneph": [RealtimeSelection("Usample", variable_name="sample_humidity", require_tags={"scattering"}, exclude_tags={"secondary"})],
    })

    data_records["aerosol-raw-pressure"] = DataRecord({
        "dilution": [Selection(variable_id="P", instrument_id="Q12")],
    })
    data_records["aerosol-realtime-pressure"] = RealtimeRecord({
        "dilution": [RealtimeSelection("P", variable_id="P", instrument_id="Q12")],
    })
    for record, cut_size in STANDARD_CUT_SIZE_SPLIT:
        data_records[f"aerosol-raw-samplepressure-{record}"] = DataRecord({
            "neph": [Selection(variable_name="sample_pressure", cut_size=cut_size,
                               require_tags={"scattering"}, exclude_tags={"secondary"})],
            "sample": [Selection(variable_id="P", cut_size=cut_size, instrument_id="Q11")],
        })
    for record, cut_size in STANDARD_CUT_SIZE_SPLIT:
        data_records[f"aerosol-realtime-samplepressure-{record}"] = RealtimeRecord({
            "neph": [RealtimeSelection("Psample", variable_name="sample_pressure", cut_size=cut_size,
                                       require_tags={"scattering"}, exclude_tags={"secondary"})],
            "sample": [RealtimeSelection("P", variable_id="P", cut_size=cut_size, instrument_id="Q11")],
        })

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

    def get(station: str, data_name: str, start_epoch_ms: int, end_epoch_ms: int,
            send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
        return data_get(station, data_name, start_epoch_ms, end_epoch_ms, send, data_records)
