import typing
from forge.vis.dashboard import Record
from forge.vis.dashboard.basic import BasicRecord
from forge.vis.dashboard.fileingest import FileIngestRecord
from forge.vis.dashboard.acquisition import AcquisitionIngestRecord
from forge.vis.dashboard.telemetry import TelemetryRecord


code_records: typing.Dict[str, Record] = {
    TelemetryRecord.CODE: TelemetryRecord(),
    'acquisition-ingest-cpd3': AcquisitionIngestRecord.simple_override(
        name="CPD3 acquisition data processing",
    ),
    'acquisition-ingest-cpd3-forge': AcquisitionIngestRecord.simple_override(
        name="CPD3 Forge data processing",
    ),
    'acquisition-transfer-data': FileIngestRecord.simple_override(
        name="Acquisition data transfer",
    ),
    'acquisition-transfer-backup': FileIngestRecord.simple_override(
        name="Acquisition computer backup transfer",
    ),
    'acquisition-telemetry-uplink': BasicRecord.simple_override(
        name="Telemetry uplink",
    ),
    'acquisition-telemetry-tunnel': BasicRecord.simple_override(
        name="Fallback SSH remote access",
        offline=4 * 60 * 60,
    ),
    'met-raw-ingest-cr1000': FileIngestRecord.simple_override(
        name="Ingest observatories meteorological data",
        offline=50 * 60 * 60,
    ),
    'radiation-raw-ingest-scaled': FileIngestRecord.simple_override(
        name="Ingest scaled radiation data",
    ),
    'radiation-editing-ingest-basemod': BasicRecord.simple_override(
        name="Ingest radiation edits from basemod.dat",
    ),
}


_default_record = BasicRecord()


def record(station: typing.Optional[str], code: str) -> typing.Optional[Record]:
    return code_records.get(code, _default_record)
