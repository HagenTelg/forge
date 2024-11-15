import asyncio
import struct
from enum import IntEnum, unique


class ProtocolError(RuntimeError):
    pass


class Handshake(IntEnum):
    CLIENT_TO_SERVER = 0xEAD568C0
    SERVER_TO_CLIENT = 0x3462A633
    CLIENT_READY = 0xA6CBA125
    SERVER_READY = 0x52EB140A
    PROTOCOL_VERSION = 3


@unique
class ClientPacket(IntEnum):
    HEARTBEAT = 0
    CLOSE = 1

    TRANSACTION_BEGIN = 2
    TRANSACTION_COMMIT = 3
    TRANSACTION_ABORT = 4
    SET_TRANSACTION_STATUS = 5

    READ_FILE = 6
    WRITE_FILE = 7
    REMOVE_FILE = 8

    LOCK_READ = 9
    LOCK_WRITE = 10

    SEND_NOTIFICATION = 11
    LISTEN_NOTIFICATION = 12
    ACKNOWLEDGE_NOTIFICATION = 13

    ACQUIRE_INTENT = 14
    RELEASE_INTENT = 15

    LIST_FILES = 16


@unique
class ServerPacket(IntEnum):
    HEARTBEAT = 0

    TRANSACTION_STARTED = 1
    TRANSACTION_COMPLETE = 2
    TRANSACTION_ABORTED = 3

    READ_FILE_DATA = 4
    WRITE_FILE_DATA = 5
    REMOVE_FILE_OK = 6

    READ_LOCK_ACQUIRED = 7
    READ_LOCK_DENIED = 8
    WRITE_LOCK_ACQUIRED = 9
    WRITE_LOCK_DENIED = 10

    NOTIFICATION_QUEUED = 11
    NOTIFICATION_LISTENING = 12
    NOTIFICATION_RECEIVED = 13

    INTENT_ACQUIRED = 14
    INTENT_RELEASED = 15
    INTENT_HIT = 16

    LIST_RESULT = 17


@unique
class ServerDiagnosticRequest(IntEnum):
    LIST_CONNECTIONS = 0
    LIST_INTENTS = 2
    LIST_LOCKS = 3
    LIST_NOTIFICATION_LISTENERS = 4
    LIST_NOTIFICATION_WAIT = 5
    TRANSACTION_DETAILS = 6

    CLOSE_CONNECTION = 100


def write_string(writer: asyncio.StreamWriter, s: str) -> None:
    raw = s.encode('utf-8')
    writer.write(struct.pack('<H', len(raw)))
    writer.write(raw)


async def read_string(reader: asyncio.StreamReader) -> str:
    size = struct.unpack('<H', await reader.readexactly(2))[0]
    raw = await reader.readexactly(size)
    return raw.decode('utf-8')
