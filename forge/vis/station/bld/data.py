import typing
from ..cpd3 import DataStream, RealtimeTranslator, data_profile_get, detach, profile_data


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


def get(station: str, data_name: str, start_epoch_ms: int, end_epoch_ms: int,
        send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
    return data_profile_get(station, data_name, start_epoch_ms, end_epoch_ms, send, station_profile_data)
