import typing
from ..cpd3 import use_cpd3


if use_cpd3("amy"):
    from ..cpd3 import DataStream, DataReader, RealtimeTranslator, Name, data_profile_get, detach, profile_data

    station_profile_data = detach(profile_data)


    station_profile_data['aerosol']['raw']['cpcstatus'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'Q_Q71'): 'Qcpc',
            Name(station, 'raw', 'Q_Q72'): 'Qdrier',
            Name(station, 'raw', 'Tu_N71'): 'Tinlet',
            Name(station, 'raw', 'T1_N71'): 'Tsaturatorbottom',
            Name(station, 'raw', 'T2_N71'): 'Tsaturatortop',
            Name(station, 'raw', 'T3_N71'): 'Tcondenser',
            Name(station, 'raw', 'T4_N71'): 'Toptics',
            Name(station, 'raw', 'Q1_N71'): 'Qsample',
            Name(station, 'raw', 'Q2_N71'): 'Qsaturator',
            Name(station, 'raw', 'P_N71'): 'Psample',
        }, send
    )
    station_profile_data['aerosol']['realtime']['cpcstatus'] = {
        RealtimeTranslator.Key('Q_Q71'): 'Qcpc',
        RealtimeTranslator.Key('Q_Q72'): 'Qdrier',
        RealtimeTranslator.Key('Tu_N71'): 'Tinlet',
        RealtimeTranslator.Key('T1_N71'): 'Tsaturatorbottom',
        RealtimeTranslator.Key('T2_N71'): 'Tsaturatortop',
        RealtimeTranslator.Key('T3_N71'): 'Tcondenser',
        RealtimeTranslator.Key('T4_N71'): 'Toptics',
        RealtimeTranslator.Key('Q1_N71'): 'Qsample',
        RealtimeTranslator.Key('Q2_N71'): 'Qsaturator',
        RealtimeTranslator.Key('P_N71'): 'Psample',
    }


    def get(station: str, data_name: str, start_epoch_ms: int, end_epoch_ms: int,
            send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
        return data_profile_get(station, data_name, start_epoch_ms, end_epoch_ms, send, station_profile_data)

else:
    from ..default.data import aerosol_data, aerosol_public, data_get, DataStream, DataRecord, RealtimeRecord, Selection, RealtimeSelection

    data_records = dict()
    data_records.update(aerosol_data)
    data_records.update(aerosol_public)

    data_records["aerosol-raw-cpcstatus"] = DataRecord({
        "Qcpc": [Selection(variable_id="Q_Q71")],
        "Qdrier": [Selection(variable_id="Q_Q72")],

        "Tinlet": [Selection(variable_name="inlet_temperature", instrument_code="bmi1710cpc", exclude_tags={"secondary"}),
                   Selection(variable_name="inlet_temperature", instrument_code="bmi1720cpc", exclude_tags={"secondary"})],
        "Tsaturatorbottom": [Selection(variable_name="saturator_bottom_temperature", instrument_code="bmi1710cpc", exclude_tags={"secondary"}),
                             Selection(variable_name="saturator_bottom_temperature", instrument_code="bmi1720cpc", exclude_tags={"secondary"})],
        "Tsaturatortop": [Selection(variable_name="saturator_top_temperature", instrument_code="bmi1710cpc", exclude_tags={"secondary"}),
                          Selection(variable_name="saturator_top_temperature", instrument_code="bmi1720cpc", exclude_tags={"secondary"})],
        "Tcondenser": [Selection(variable_name="condenser_temperature", instrument_code="bmi1710cpc", exclude_tags={"secondary"}),
                       Selection(variable_name="condenser_temperature", instrument_code="bmi1720cpc", exclude_tags={"secondary"})],
        "Toptics": [Selection(variable_name="optics_temperature", instrument_code="bmi1710cpc", exclude_tags={"secondary"}),
                    Selection(variable_name="optics_temperature", instrument_code="bmi1720cpc", exclude_tags={"secondary"})],
        "Qsample": [Selection(variable_name="sample_flow", instrument_code="bmi1710cpc", exclude_tags={"secondary"}),
                    Selection(variable_name="sample_flow", instrument_code="bmi1720cpc", exclude_tags={"secondary"})],
        "Qsaturator": [Selection(variable_name="saturator_flow", instrument_code="bmi1710cpc", exclude_tags={"secondary"}),
                       Selection(variable_name="saturator_flow", instrument_code="bmi1720cpc", exclude_tags={"secondary"})],
        "Psample": [Selection(variable_name="inlet_pressure", instrument_code="bmi1720cpc", exclude_tags={"secondary"})],
    })
    data_records["aerosol-realtime-cpcstatus"] = RealtimeRecord({
        "Qcpc": [RealtimeSelection("Q_Q71", variable_id="Q_Q71")],
        "Qdrier": [RealtimeSelection("Q_Q72", variable_id="Q_Q72")],

        "Tinlet": [RealtimeSelection("Tinlet", variable_name="inlet_temperature", instrument_code="bmi1710cpc", exclude_tags={"secondary"}),
                   RealtimeSelection("Tinlet", variable_name="inlet_temperature", instrument_code="bmi1720cpc", exclude_tags={"secondary"})],
        "Tsaturatorbottom": [RealtimeSelection("Tsaturatorbottom", variable_name="saturator_bottom_temperature", instrument_code="bmi1710cpc", exclude_tags={"secondary"}),
                             RealtimeSelection("Tsaturatorbottom", variable_name="saturator_bottom_temperature", instrument_code="bmi1720cpc", exclude_tags={"secondary"})],
        "Tsaturatortop": [RealtimeSelection("Tsaturatortop", variable_name="saturator_top_temperature", instrument_code="bmi1710cpc", exclude_tags={"secondary"}),
                          RealtimeSelection("Tsaturatortop", variable_name="saturator_top_temperature", instrument_code="bmi1720cpc", exclude_tags={"secondary"})],
        "Tcondenser": [RealtimeSelection("Tcondenser", variable_name="condenser_temperature", instrument_code="bmi1710cpc", exclude_tags={"secondary"}),
                       RealtimeSelection("Tcondenser", variable_name="condenser_temperature", instrument_code="bmi1720cpc", exclude_tags={"secondary"})],
        "Toptics": [RealtimeSelection("Toptics", variable_name="optics_temperature", instrument_code="bmi1710cpc", exclude_tags={"secondary"}),
                    RealtimeSelection("Toptics", variable_name="optics_temperature", instrument_code="bmi1720cpc", exclude_tags={"secondary"})],
        "Qsample": [RealtimeSelection("Q", variable_name="sample_flow", instrument_code="bmi1710cpc", exclude_tags={"secondary"}),
                    RealtimeSelection("Q", variable_name="sample_flow", instrument_code="bmi1720cpc", exclude_tags={"secondary"})],
        "Qsaturator": [RealtimeSelection("Qsaturator", variable_name="saturator_flow", instrument_code="bmi1710cpc", exclude_tags={"secondary"}),
                       RealtimeSelection("Qsaturator", variable_name="saturator_flow", instrument_code="bmi1720cpc", exclude_tags={"secondary"})],
        "Psample": [RealtimeSelection("P", variable_name="inlet_pressure", instrument_code="bmi1720cpc", exclude_tags={"secondary"})],
    })

    def get(station: str, data_name: str, start_epoch_ms: int, end_epoch_ms: int,
            send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
        return data_get(station, data_name, start_epoch_ms, end_epoch_ms, send, data_records)