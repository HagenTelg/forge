from enum import IntEnum


class ServerConnectionType(IntEnum):
    TO_REMOTE = 0
    INITIATE_CONNECTION = 1


class InitiateConnectionStatus(IntEnum):
    OK = 0
    PERMISSION_DENIED = 1
    TARGET_NOT_FOUND = 2


class ToRemotePacketType(IntEnum):
    DATA = 0
    CONNECTION_CLOSE = 1
    SSH_CONNECTION_OPEN = 2


class FromRemotePacketType(IntEnum):
    DATA = 0
    CONNECTION_OPEN = 1
    CONNECTION_CLOSED = 2
