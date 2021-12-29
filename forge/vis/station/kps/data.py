import typing
from ..cpd3 import DataStream, DataReader, RealtimeTranslator, Name, data_profile_get, detach, profile_data


station_profile_data = detach(profile_data)


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


def get(station: str, data_name: str, start_epoch_ms: int, end_epoch_ms: int,
        send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
    return data_profile_get(station, data_name, start_epoch_ms, end_epoch_ms, send, station_profile_data)
