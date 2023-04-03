import typing
from ..default.dashboard import code_records as base_code_records, record as base_record, Record, FileIngestRecord


code_records = dict(base_code_records)

code_records['gas-raw-ingest-ccg'] = FileIngestRecord.simple_override(
    name="Ingest CCG gas data",
),
code_records['met-raw-ingest-ccg'] = FileIngestRecord.simple_override(
    name="Ingest CCG meteorological data",
),


def record(station: typing.Optional[str], code: str) -> typing.Optional[Record]:
    return code_records.get(code) or base_record(station, code)
