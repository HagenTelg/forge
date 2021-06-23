import typing
from ..cpd3 import DataStream, DataReader, Name, data_profile_get, detach, profile_data


station_profile_data = detach(profile_data)

station_profile_data['aerosol']['raw']['dmpsstatus'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'T1_N11'): 'Taerosol', Name(station, 'raw', 'T2_N11'): 'Tsheath',
        Name(station, 'raw', 'P1_N11'): 'Paerosol', Name(station, 'raw', 'P2_N11'): 'Psheath',
        Name(station, 'raw', 'Q1_N11'): 'Qaerosol', Name(station, 'raw', 'Q2_N11'): 'Qsheath',
    }, send
)
station_profile_data['aerosol']['raw']['popsstatus'] = lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
    start_epoch_ms, end_epoch_ms, {
        Name(station, 'raw', 'T1_N21'): 'Tpressure',
        Name(station, 'raw', 'T2_N21'): 'Tlaser',
        Name(station, 'raw', 'T3_N21'): 'Tinternal',

        Name(station, 'raw', 'Q_N21'): 'Qsample',

        Name(station, 'raw', 'P_N21'): 'Pboard',
    }, send
)


def get(station: str, data_name: str, start_epoch_ms: int, end_epoch_ms: int,
        send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
    return data_profile_get(station, data_name, start_epoch_ms, end_epoch_ms, send, station_profile_data)
