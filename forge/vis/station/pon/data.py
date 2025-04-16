import typing
from ..cpd3 import use_cpd3


if use_cpd3("pon"):
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
        }, send
    )
    station_profile_data['aerosol']['realtime']['cpcstatus'] = {
        RealtimeTranslator.Key('T1_N61'): 'Tsaturator',
        RealtimeTranslator.Key('T2_N61'): 'Tcondenser',
        RealtimeTranslator.Key('T3_N61'): 'Toptics',
        RealtimeTranslator.Key('T4_N61'): 'Tcabinet',
        RealtimeTranslator.Key('Q_N61'): 'Qsample',
        RealtimeTranslator.Key('Qu_N61'): 'Qinlet',
    }


    station_profile_data['aerosol']['raw']['smps'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'Ns_N11'): 'Dp',
            Name(station, 'raw', 'Nn_N11'): 'dNdlogDp',
            Name(station, 'raw', 'Nb_N11'): 'dN',
        }, send
    )
    station_profile_data['aerosol']['editing']['smps'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
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
    station_profile_data['aerosol']['clean']['smps'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
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
    station_profile_data['aerosol']['avgh']['smps'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
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


    def get(station: str, data_name: str, start_epoch_ms: int, end_epoch_ms: int,
            send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
        return data_profile_get(station, data_name, start_epoch_ms, end_epoch_ms, send, station_profile_data)


else:
    from ..default.data import aerosol_data, aerosol_public,data_get, DataStream, DataRecord, RealtimeRecord, Selection, RealtimeSelection

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
    })

    def get(station: str, data_name: str, start_epoch_ms: int, end_epoch_ms: int,
            send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
        return data_get(station, data_name, start_epoch_ms, end_epoch_ms, send, data_records)
