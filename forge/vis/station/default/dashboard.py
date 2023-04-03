import typing
from forge.vis.dashboard import Record
from forge.vis.dashboard.basic import BasicRecord, BasicEntry


code_records: typing.Dict[str, Record] = {
    'acquisition-ingest-cpd3': BasicRecord.simple_override(
        name="CPD3 acquisition data processing",
    ),
    'met-raw-ingest-cr1000': BasicRecord.simple_override(
        name="Ingest GML observatories meteorological data",
    ),
    'radiation-raw-ingest-scaled': BasicRecord.simple_override(
        name="Ingest scaled radiation data",
    ),
    'radiation-editing-ingest-basemod': BasicRecord.simple_override(
        name="Ingest radiation edits from basemod.dat",
    ),
}


_default_record = BasicRecord()


def record(station: typing.Optional[str], code: str) -> typing.Optional[Record]:
    return code_records.get(code, _default_record)
