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
    'acquisition-ingest-data': AcquisitionIngestRecord.simple_override(
        name="Acquisition data processing",
    ),
    'acquisition-transfer-data': FileIngestRecord.simple_override(
        name="Acquisition data transfer",
    ),
    'acquisition-transfer-backup': FileIngestRecord.simple_override(
        name="Acquisition computer backup transfer",
        offline=(26 + 12) * 60 * 60,
    ),
    'acquisition-telemetry-uplink': BasicRecord.simple_override(
        name="Telemetry uplink",
        offline=(26 + 12) * 60 * 60,
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


def detach_irregular_reporting(threshold: float = 0) -> typing.Dict[str, Record]:
    result = dict(code_records)
    for code in (
            'acquisition-ingest-cpd3',
            'acquisition-ingest-cpd3-forge',
            'acquisition-ingest-data',
            'acquisition-transfer-data',
            'acquisition-transfer-backup',
            'acquisition-telemetry-uplink',
            'acquisition-telemetry-tunnel',
            'met-raw-ingest-cr1000',
            'radiation-raw-ingest-scaled',
    ):
        result[code] = result[code].simple_override(offline=threshold)
    return result


_default_record = BasicRecord()


def record(station: typing.Optional[str], code: str) -> typing.Optional[Record]:
    return code_records.get(code, _default_record)
