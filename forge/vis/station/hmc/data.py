import typing
from ..cpd3 import DataStream, DataReader, EditedReader, RealtimeTranslator, Name, data_profile_get, detach, profile_data


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


def get(station: str, data_name: str, start_epoch_ms: int, end_epoch_ms: int,
        send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
    return data_profile_get(station, data_name, start_epoch_ms, end_epoch_ms, send, station_profile_data)
