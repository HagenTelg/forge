import typing
from ..cpd3 import use_cpd3


if not use_cpd3("euk"):
    from ..default.data import ozone_data, ozone_public, data_get, DataStream

    data_records = dict()
    data_records.update(ozone_data)
    data_records.update(ozone_public)


    def get(station: str, data_name: str, start_epoch_ms: int, end_epoch_ms: int,
            send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
        return data_get(station, data_name, start_epoch_ms, end_epoch_ms, send, data_records)
