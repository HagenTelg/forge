import typing
import asyncio
from enum import IntEnum
from forge.acquisition.bus.serializer import AcquisitionBusSerializer


class ConnectionType(IntEnum):
    ACQUISITION = 0
    DISPLAY = 1


class PacketType(IntEnum):
    DATA = 0
    INSTRUMENT_ADD = 1
    INSTRUMENT_UPDATE = 2
    INSTRUMENT_REMOVE = 3
    INSTRUMENT_STATE = 4
    EVENT_LOG = 5
    COMMAND = 6
    BYPASS = 7
    WRITE_MESSAGE_LOG = 8
    RESTART_SYSTEM = 9
    CHAT = 10


_value_protocol = AcquisitionBusSerializer(double_float=False)

serialize_value = _value_protocol.serialize_value
deserialize_value = _value_protocol.deserialize_value
