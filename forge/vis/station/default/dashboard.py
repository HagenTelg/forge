import typing
from forge.vis.dashboard import Record
from forge.vis.dashboard.basic import BasicRecord, BasicEntry


code_records: typing.Dict[str, Record] = {

}


_default_record = BasicRecord()


def record(station: typing.Optional[str], code: str) -> typing.Optional[Record]:
    return code_records.get(code, _default_record)
