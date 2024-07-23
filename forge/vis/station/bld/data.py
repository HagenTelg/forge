import typing
from ..cpd3 import DataStream, RealtimeTranslator, data_profile_get, detach, profile_data, Name, DataReader


station_profile_data = detach(profile_data)


class RealtimeData(RealtimeTranslator.Data):
    PUBLIC_DATA = {
        'public-realtime-ozone': 'ozone-realtime-ozone'
    }

    def get(self, key, default=None) -> typing.Callable[[str, int, int, typing.Callable], typing.Optional[DataStream]]:
        key = self.PUBLIC_DATA.get(key, key)
        return super().get(key, default)


station_profile_data['public'] = {
    'realtime': RealtimeData('public', {
        'ozone': {
            RealtimeTranslator.Key('X_G81'): 'ozone',
        },
    }),
}



station_profile_data['ozone']['raw']['nox'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'X1_G82'): 'no2',
        Name(station, 'raw', 'X2_G82'): 'no',
        Name(station, 'raw', 'X3_G82'): 'nox',
    }, send
)
station_profile_data['ozone']['editing']['nox'] = lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
    start_epoch_ms, end_epoch_ms, station, 'aerosol', {
        Name(station, 'clean', 'X1_G82'): 'no2',
        Name(station, 'clean', 'X2_G82'): 'no',
        Name(station, 'clean', 'X3_G82'): 'nox',
    }, send
)
station_profile_data['ozone']['clean']['nox'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'clean', 'X1_G82'): 'no2',
        Name(station, 'clean', 'X2_G82'): 'no',
        Name(station, 'clean', 'X3_G82'): 'nox',

    }, send
)
station_profile_data['ozone']['avgh']['nox'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'avgh', 'X1_G82'): 'no2',
        Name(station, 'avgh', 'X2_G82'): 'no',
        Name(station, 'avgh', 'X3_G82'): 'nox',
    }, send
)

station_profile_data['ozone']['raw']['noxstatus'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'T1_G82'): 'Tmanifold',
        Name(station, 'raw', 'T2_G82'): 'Toven',
        Name(station, 'raw', 'T3_G82'): 'Tbox',
        Name(station, 'raw', 'P_G82'): 'Psample',
    }, send
)
station_profile_data['ozone']['realtime']['noxstatus'] = {
    RealtimeTranslator.Key('T1_G82'): 'Tmanifold',
    RealtimeTranslator.Key('T2_G82'): 'Toven',
    RealtimeTranslator.Key('T3_G82'): 'Tbox',
    RealtimeTranslator.Key('P_G82'): 'Psample',
}


def get(station: str, data_name: str, start_epoch_ms: int, end_epoch_ms: int,
        send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
    return data_profile_get(station, data_name, start_epoch_ms, end_epoch_ms, send, station_profile_data)
