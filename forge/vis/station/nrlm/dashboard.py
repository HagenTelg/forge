import typing
from ..default.dashboard import detach_irregular_reporting, record as base_record, Record


code_records = detach_irregular_reporting()


def record(station: typing.Optional[str], code: str) -> typing.Optional[Record]:
    return code_records.get(code) or base_record(station, code)
