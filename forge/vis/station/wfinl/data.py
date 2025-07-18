import typing
from math import nan
from ..default.data import data_get, DataStream, DataRecord, RealtimeRecord, Selection, RealtimeSelection, STANDARD_THREE_WAVELENGTHS, STANDARD_CUT_SIZE_SPLIT

data_records = dict()

data_records["aerosol-raw-t640status"] = DataRecord({
    "Tsample": [Selection(variable_name="sample_temperature", instrument_code="teledynet640")],
    "Tambient": [Selection(variable_name="ambient_temperature", instrument_code="teledynet640")],
    "Tasc": [Selection(variable_name="asc_temperature", instrument_code="teledynet640")],
    "Tled": [Selection(variable_name="led_temperature", instrument_code="teledynet640")],
    "Tbox": [Selection(variable_name="box_temperature", instrument_code="teledynet640")],
    "Usample": [Selection(variable_name="sample_humidity", instrument_code="teledynet640")],
    "Psample": [Selection(variable_name="pressure", instrument_code="teledynet640")],
    "Qsample": [Selection(variable_name="sample_flow", instrument_code="teledynet640")],
    "Qbypass": [Selection(variable_name="bypass_flow", instrument_code="teledynet640")],
})
data_records["aerosol-realtime-t640status"] = RealtimeRecord({
    "Tsample": [RealtimeSelection("Tsample", variable_name="sample_temperature", instrument_code="teledynet640")],
    "Tambient": [RealtimeSelection("Tambient", variable_name="ambient_temperature", instrument_code="teledynet640")],
    "Tasc": [RealtimeSelection("Tasc", variable_name="asc_temperature", instrument_code="teledynet640")],
    "Tled": [RealtimeSelection("Tled", variable_name="led_temperature", instrument_code="teledynet640")],
    "Tbox": [RealtimeSelection("Tbox", variable_name="box_temperature", instrument_code="teledynet640")],
    "Usample": [RealtimeSelection("Usample", variable_name="sample_humidity", instrument_code="teledynet640")],
    "Psample": [RealtimeSelection("Psample", variable_name="pressure", instrument_code="teledynet640")],
    "Qsample": [RealtimeSelection("Qsample", variable_name="sample_flow", instrument_code="teledynet640")],
    "Qbypass": [RealtimeSelection("Qbypass", variable_name="bypass_flow", instrument_code="teledynet640")],
})
for archive in ("raw", "editing", "clean", "avgh"):
    for record, selected_size in (("whole", nan), ("pm10", 10.0), ("pm25", 2.5), ("pm1", 1.0)):
        data_records[f"aerosol-{archive}-t640-{record}"] = DataRecord({
            "X": [
                Selection(variable_name="mass_concentration", instrument_code="teledynet640",
                          dimension_at=(("diameter", selected_size), )),
            ],
        })
for record, selected_size, suffix in (("pm10", 10.0, "10"), ("pm25", 2.5, "25"), ("pm1", 1.0, "1")):
    data_records[f"aerosol-realtime-t640-{record}"] = RealtimeRecord({
        "X": [RealtimeSelection(f"X{suffix}", variable_name="mass_concentration",
                                instrument_code="teledynet640",
                                dimension_at=(("diameter", selected_size),))],
    })

def get(station: str, data_name: str, start_epoch_ms: int, end_epoch_ms: int,
        send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
    return data_get(station, data_name, start_epoch_ms, end_epoch_ms, send, data_records)
