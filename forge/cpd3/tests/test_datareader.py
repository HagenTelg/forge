import typing
import base64
from forge.cpd3.datareader import StandardDataInput, RecordInput, Identity, Name


class InputBuffer(StandardDataInput):
    def __init__(self):
        super().__init__()
        self.result = list()

    def value_ready(self, identity: Identity, value: typing.Any) -> None:
        self.result.append((identity, value))


class RecordBuffer(RecordInput):
    def __init__(self):
        super().__init__()
        self.result = list()

    def record_ready(self, start: typing.Optional[float], end: typing.Optional[float],
                     record: typing.Dict[Name, typing.Any]) -> None:
        self.result.append((start, end, record))


contents = base64.b64decode("""
xNMCFQCAA2JvcwNyYXcHQnNHX1MxMQNwbTEgAAEAAACPJiPYQQAAAJ4mI9hBAAAAAAAAAQAAAAAA
APA/FQCAA2JvcwNyYXcHQnNSX1MxMQNwbTEgAAEAAACPJiPYQQAAAJ4mI9hBAAAAAAEAAQAAAAAA
AABAIAABAAAAniYj2EEAAACtJiPYQQAAAAAAAAEAAAAAAAAIQA==
""")


def test_datainput():
    buffer = InputBuffer()
    buffer.incoming_raw(contents[3:])

    base = Name("bos", "raw", "BsG_S11", {"pm1"})
    assert buffer.result == [
        (Identity(name=base, start=1619827260, end=1619827320), 1.0),
        (Identity(name=base, variable="BsR_S11", start=1619827260, end=1619827320), 2.0),
        (Identity(name=base, start=1619827320, end=1619827380), 3.0),
    ]


def test_recordinput():
    buffer = RecordBuffer()
    buffer.incoming_raw(contents[3:])
    buffer.flush()

    BsG = Name("bos", "raw", "BsG_S11", {"pm1"})
    BsR = Name("bos", "raw", "BsR_S11", {"pm1"})
    assert buffer.result == [
        (1619827260, 1619827320, {BsG: 1.0, BsR: 2.0}),
        (1619827320, 1619827380, {BsG: 3.0}),
    ]
