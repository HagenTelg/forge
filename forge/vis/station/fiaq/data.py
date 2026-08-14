import typing
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

def get(station: str, data_name: str, start_epoch_ms: int, end_epoch_ms: int,
        send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
    return data_get(station, data_name, start_epoch_ms, end_epoch_ms, send, data_records)