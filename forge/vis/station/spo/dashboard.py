import typing
from ..default.dashboard import code_records as base_code_records, record as base_record, Record, BasicRecord


code_records = dict(base_code_records)

for code in ('acquisition-ingest-cpd3', 'met-raw-ingest-cr1000', 'radiation-raw-ingest-scaled'):
    code_records[code] = base_code_records[code].simple_override(
        offline=50 * 60 * 60,
    )


def record(station: typing.Optional[str], code: str) -> typing.Optional[Record]:
    return code_records.get(code) or base_record(station, code)
