from enum import IntEnum
from .serializer import AcquisitionBusSerializer


class PersistenceLevel(IntEnum):
    DATA = 0  # No persistence
    STATE = 1  # Connection persistent, sent after source
    SOURCE = 2  # Connection persistent, sent after system state
    SYSTEM = 3  # Bus controller/global persistent, sent first


_value_protocol = AcquisitionBusSerializer(double_float=True)

serialize_value = _value_protocol.serialize_value
deserialize_value = _value_protocol.deserialize_value
serialize_string = _value_protocol.serialize_string
deserialize_string = _value_protocol.deserialize_string
