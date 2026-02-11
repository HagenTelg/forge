import typing
from ..cpd3 import use_cpd3


if not use_cpd3("cao"):
    from ..default.data import data_get, met_data, DataStream, DataRecord, Selection

    data_records = dict()
    data_records.update(met_data)

    for archive in ("raw", "editing", "clean", "avgh"):
        data_records[f"met-{archive}-tach"] = DataRecord({
            "C1": [Selection(variable_id="C1", instrument_id="XM1")],
            "C2": [Selection(variable_id="C2", instrument_id="XM1")],
            "C3": [Selection(variable_id="C3", instrument_id="XM1")],
        })

    def get(station: str, data_name: str, start_epoch_ms: int, end_epoch_ms: int,
            send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
        return data_get(station, data_name, start_epoch_ms, end_epoch_ms, send, data_records)
