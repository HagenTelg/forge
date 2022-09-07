from enum import IntEnum


class FileType(IntEnum):
    DATA = 0
    BACKUP = 1
    AUXILIARY = 2


class Compression(IntEnum):
    NONE = 0
    ZSTD = 1


class ServerConnectionType(IntEnum):
    ADD_FILE = 0
    GET_FILES = 1


class AddFileOperation(IntEnum):
    CHUNK = 0
    COMPLETE = 1
    ABORT = 2


class GetFileOperation(IntEnum):
    CHUNK = 0
    COMPLETE = 1
    ABORT = 2
