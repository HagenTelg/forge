import typing
import base64
from forge.cpd3.datawriter import StandardDataOutput, Name, Identity


class OutputBuffer(StandardDataOutput):
    def __init__(self):
        super().__init__()
        self.result = bytearray()

    def output_ready(self, packet: bytes) -> None:
        self.result += self.raw_encode(packet)


contents = base64.b64decode("""
xNMCEgCAA25pbANyYXcHQnNHX1MxMQAgAAEAAABAjq/YQQAAAE+Or9hBAAAAAAAAAQAAAAAAAPA/
EgCAA25pbANyYXcHQnNCX1MxMQAgAAEAAABAjq/YQQAAAE+Or9hBAAAAAAEAAQAAAAAAAABAIAAB
AAAAT46v2EEAAABejq/YQQAAAAAAAAEAAAAAAAAIQA==
""")


def test_dataoutput():
    base = Name("nil", "raw", "BsG_S11")

    buffer = OutputBuffer()
    buffer.incoming_value(Identity(name=base, start=1656633600, end=1656633660), 1.0)
    buffer.incoming_value(Identity(name=base, variable="BsB_S11", start=1656633600, end=1656633660), 2.0)
    buffer.incoming_value(Identity(name=base, start=1656633660, end=1656633720), 3.0)
    buffer.finish()

    assert (buffer.RAW_HEADER + buffer.result) == contents
