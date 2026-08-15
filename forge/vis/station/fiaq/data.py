import typing
from math import nan
from ..default.data import aerosol_data, ozone_data, ozone_public, radiation_data, data_get, \
    DataStream, DataRecord, RealtimeRecord, \
    Selection, RealtimeSelection

data_records = dict()
data_records.update(aerosol_data)
data_records.update(ozone_data)
data_records.update(ozone_public)
data_records.update(radiation_data)

for archive in ("raw", "editing", "clean", "avgh"):
    data_records[f"ozone-{archive}-nox"] = DataRecord({
        "no2": [Selection(variable_name="nitrogen_dioxide_mixing_ratio", exclude_tags={"secondary"})],
        "no": [Selection(variable_name="nitrogen_monoxide_mixing_ratio", exclude_tags={"secondary"})],
        "nox": [Selection(variable_name="nox_mixing_ratio", exclude_tags={"secondary"})],
    })
data_records[f"ozone-realtime-nox"] = RealtimeRecord({
    "no2": [RealtimeSelection("XNO2", variable_name="nitrogen_dioxide_mixing_ratio", exclude_tags={"secondary"})],
    "no": [RealtimeSelection("XNO", variable_name="nitrogen_monoxide_mixing_ratio", exclude_tags={"secondary"})],
    "nox": [RealtimeSelection("XNOx", variable_name="nox_mixing_ratio", exclude_tags={"secondary"})],
})

data_records[f"ozone-raw-noxstatus"] = DataRecord({
    "Tmanifold": [Selection(variable_name="manifold_temperature", instrument_code="teledynen500", exclude_tags={"secondary"})],
    "Toven": [Selection(variable_name="oven_temperature", instrument_code="teledynen500", exclude_tags={"secondary"})],
    "Tbox": [Selection(variable_name="box_temperature", instrument_code="teledynen500", exclude_tags={"secondary"})],
    "Psample": [Selection(variable_name="pressure", instrument_code="teledynen500", exclude_tags={"secondary"})],
})
data_records[f"ozone-realtime-noxstatus"] = RealtimeRecord({
    "Tmanifold": [RealtimeSelection("Tmanifold", variable_name="manifold_temperature", instrument_code="teledynen500", exclude_tags={"secondary"})],
    "Toven": [RealtimeSelection("Toven", variable_name="oven_temperature", instrument_code="teledynen500", exclude_tags={"secondary"})],
    "Tbox": [RealtimeSelection("Tbox", variable_name="box_temperature", instrument_code="teledynen500", exclude_tags={"secondary"})],
    "Psample": [RealtimeSelection("Psample", variable_name="pressure", instrument_code="teledynen500", exclude_tags={"secondary"})],
})


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