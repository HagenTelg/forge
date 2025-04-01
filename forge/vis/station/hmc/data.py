import typing
from math import nan
from ..cpd3 import use_cpd3


if use_cpd3("hmc"):
    from ..cpd3 import DataStream, DataReader, EditedReader, Name, RealtimeTranslator, data_profile_get, detach, profile_data

    station_profile_data = detach(profile_data)


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


    station_profile_data['met']['raw']['wind'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'WS_XM1'): 'WSambient',
            Name(station, 'raw', 'WD_XM1'): 'WDambient',
        }, send
    )
    station_profile_data['met']['realtime']['wind'] = {
        RealtimeTranslator.Key('WS_XM1'): 'WSambient',
        RealtimeTranslator.Key('WD_XM1'): 'WDambient',
    }
    station_profile_data['met']['clean']['wind'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'clean', 'WS_XM1'): 'WSambient',
            Name(station, 'clean', 'WD_XM1'): 'WDambient',
        }, send
    )
    station_profile_data['met']['avgh']['wind'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'avgh', 'WS_XM1'): 'WSambient',
            Name(station, 'avgh', 'WD_XM1'): 'WDambient',
        }, send
    )
    station_profile_data['met']['editing']['wind'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
        start_epoch_ms, end_epoch_ms, station, 'met', {
            Name(station, 'clean', 'WS_XM1'): 'WSambient',
            Name(station, 'clean', 'WD_XM1'): 'WDambient',
        }, send
    )

    station_profile_data['aerosol']['raw']['purpleair'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
        start_epoch_ms, end_epoch_ms, {
            Name(station, 'raw', 'P_S81'): 'P',
            Name(station, 'raw', 'T_S81'): 'T',
            Name(station, 'raw', 'U_S81'): 'U',
            Name(station, 'raw', 'Ipa_S81'): 'IBsa',
            Name(station, 'raw', 'Ipb_S81'): 'IBsb',
            Name(station, 'raw', 'Bs_S81'): 'Bs',
        }, send
    )
    station_profile_data['aerosol']['realtime']['purpleair'] = {
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
    from ..default.data import aerosol_data, met_data, data_get, DataStream, DataRecord, RealtimeRecord, Selection, RealtimeSelection

    data_records = dict()
    data_records.update(aerosol_data)
    data_records.update(met_data)

    for archive in ("raw", "editing", "clean", "avgh"):
        data_records[f"aerosol-{archive}-purpleair"] = DataRecord({
            "T": [Selection(variable_name="ambient_temperature", instrument_code="purpleair", exclude_tags={"secondary"})],
            "U": [Selection(variable_name="ambient_humidity", instrument_code="purpleair", exclude_tags={"secondary"})],
            "P": [Selection(variable_name="ambient_pressure", instrument_code="purpleair", exclude_tags={"secondary"})],
            "IBsa": [Selection(variable_name="detector_a_intensity", instrument_code="purpleair", exclude_tags={"secondary"})],
            "IBsb": [Selection(variable_name="detector_b_intensity", instrument_code="purpleair", exclude_tags={"secondary"})],
            "Bs": [Selection(variable_id="Bs", instrument_code="purpleair", exclude_tags={"secondary"})],
        })
    data_records["aerosol-realtime-purpleair"] = RealtimeRecord({
        "T": [RealtimeSelection("T", variable_name="ambient_temperature", instrument_code="purpleair", exclude_tags={"secondary"})],
        "U": [RealtimeSelection("U", variable_name="ambient_humidity", instrument_code="purpleair", exclude_tags={"secondary"})],
        "P": [RealtimeSelection("P", variable_name="ambient_pressure", instrument_code="purpleair", exclude_tags={"secondary"})],
        "IBsa": [RealtimeSelection("Ipa", variable_name="detector_a_intensity", instrument_code="purpleair", exclude_tags={"secondary"})],
        "IBsb": [RealtimeSelection("Ipb", variable_name="detector_b_intensity", instrument_code="purpleair", exclude_tags={"secondary"})],
        "Bs": [RealtimeSelection("Bs", variable_id="Bs", instrument_code="purpleair", exclude_tags={"secondary"})],
    })

    def get(station: str, data_name: str, start_epoch_ms: int, end_epoch_ms: int,
            send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
        return data_get(station, data_name, start_epoch_ms, end_epoch_ms, send, data_records)
