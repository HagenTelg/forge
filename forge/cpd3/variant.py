import typing
import struct
from collections import OrderedDict, deque
from enum import Enum


class Overlay(str):
    pass


class Matrix(list):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.shape: typing.List[int] = list()


class Keyframe(dict):
    pass


class Metadata(dict):
    pass


class MetadataChildren(dict):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.children: typing.Dict[str, typing.Any] = dict()


class _ID(Enum):
    Empty = 0
    Real = 1
    Integer = 2
    Boolean = 3
    String_v1 = 4
    Bytes = 5

    Flags_v1 = 6
    Array_v1 = 7
    Matrix_v1 = 8
    Hash_v1 = 9
    Keyframe_v1 = 10

    MetadataReal_v1 = 11
    MetadataInteger_v1 = 12
    MetadataBoolean_v1 = 13
    MetadataString_v1 = 14
    MetadataBytes_v1 = 15
    MetadataFlags_v1 = 16
    MetadataArray_v1 = 17
    MetadataMatrix_v1 = 18
    MetadataHash_v1 = 19
    MetadataKeyframe_v1 = 20
    Overlay_v1 = 21

    String_v2 = 22
    LocalizeString_v2 = 23
    Flags_v2 = 24

    Hash_v2 = 25
    Array_v2 = 26
    Matrix_v2 = 27
    Keyframe_v2 = 28

    MetadataReal_v2 = 29
    MetadataInteger_v2 = 30
    MetadataBoolean_v2 = 31
    MetadataString_v2 = 32
    MetadataBytes_v2 = 33
    MetadataFlags_v2 = 34
    MetadataArray_v2 = 35
    MetadataMatrix_v2 = 36
    MetadataHash_v2 = 37
    MetadataKeyframe_v2 = 38

    Overlay_v2 = 39


_deserializers: typing.Dict[_ID, typing.Callable[[bytearray], typing.Any]] = dict()
_serializers: typing.Dict[typing.Any, typing.Callable[[typing.Any], bytes]] = dict()


def _serialize_short_length(n: int) -> bytes:
    if n < 0xFF:
        return bytes([n])
    return struct.pack('<BI', 0xFF, n)
def _deserialize_short_length(data: bytearray) -> int:
    n = data[0]
    del data[0]
    if n == 0xFF:
        n = struct.unpack('<I', data[:4])[0]
        del data[:4]
    return n


def _deserialize_empty(data: bytearray) -> None:
    return None
_deserializers[_ID.Empty] = _deserialize_empty
def _serialize_empty(value: typing.Optional) -> bytes:
    return bytes([_ID.Empty.value])
_serializers[type(None)] = _serialize_empty


def _deserialize_real(data: bytearray) -> float:
    result = struct.unpack('<d', data[:8])[0]
    del data[:8]
    return result
_deserializers[_ID.Real] = _deserialize_real
def _serialize_real(value: float) -> bytes:
    return struct.pack('<Bd', _ID.Real.value, value)
_serializers[float] = _serialize_real


def _deserialize_integer(data: bytearray) -> int:
    result = struct.unpack('<q', data[:8])[0]
    del data[:8]
    return result
_deserializers[_ID.Integer] = _deserialize_integer
def _serialize_integer(value: int) -> bytes:
    return struct.pack('<Bq', _ID.Integer.value, value)
_serializers[int] = _serialize_integer


def _deserialize_boolean(data: bytearray) -> bool:
    result = (data[0] != 0)
    del data[0]
    return result
_deserializers[_ID.Boolean] = _deserialize_boolean
def _serialize_boolean(value: bool) -> bytes:
    return struct.pack('<BB', _ID.Boolean.value, 1 if value else 0)
_serializers[bool] = _serialize_boolean


def _deserialize_bytes(data: bytearray) -> bytes:
    n = struct.unpack('<I', data[:4])[0]
    if n == 0xffffffff:
        del data[:4]
        return bytes()
    result = bytes(data[4:4+n])
    del data[:4+n]
    return result
_deserializers[_ID.Bytes] = _deserialize_bytes
def _serialize_bytes(value: typing.Union[bytes, bytearray]) -> bytes:
    return struct.pack('<BI', _ID.Bytes.value, len(value)) + value
_serializers[bytes] = _serialize_bytes
_serializers[bytearray] = _serialize_bytes


def _deserialize_qstring(data: bytearray) -> str:
    n = struct.unpack('<I', data[:4])[0]
    if n == 0xffffffff:
        del data[:4]
        return str()
    result = data[4:4+n].decode('utf-8')
    del data[:4+n]
    return result
def _deserialize_string_v1(data: bytearray) -> str:
    result = _deserialize_qstring(data)
    n_localized = struct.unpack('<I', data[:4])[0]
    del data[:4]
    for i in range(n_localized):
        _deserialize_qstring(data)
        _deserialize_qstring(data)
    return result
_deserializers[_ID.String_v1] = _deserialize_string_v1
def deserialize_short_string(data: bytearray) -> str:
    n = _deserialize_short_length(data)
    result = data[:n].decode('utf-8')
    del data[:n]
    return result
_deserializers[_ID.String_v2] = deserialize_short_string
def _deserialize_localized_string(data: bytearray) -> str:
    result = deserialize_short_string(data)
    n_localized = struct.unpack('<I', data[:4])[0]
    del data[:4]
    for i in range(n_localized):
        _deserialize_qstring(data)
        _deserialize_qstring(data)
    return result
_deserializers[_ID.LocalizeString_v2] = _deserialize_localized_string
def serialize_short_string(value: str) -> bytes:
    encoded = value.encode('utf-8')
    return _serialize_short_length(len(encoded)) + encoded
def _serialize_string(value: str) -> bytes:
    return struct.pack('<B', _ID.String_v2.value) + serialize_short_string(value)
_serializers[str] = _serialize_string


def _deserialize_flags_v1(data: bytearray) -> typing.Set[str]:
    n = struct.unpack('<I', data[:4])[0]
    del data[:4]
    result: typing.Set[str] = set()
    for i in range(n):
        result.add(_deserialize_qstring(data))
    return result
_deserializers[_ID.Flags_v1] = _deserialize_flags_v1
def _deserialize_flags_v2(data: bytearray) -> typing.Set[str]:
    n = _deserialize_short_length(data)
    result: typing.Set[str] = set()
    for i in range(n):
        result.add(deserialize_short_string(data))
    return result
_deserializers[_ID.Flags_v2] = _deserialize_flags_v2
def _serialize_flags(value: typing.Set[str]) -> bytes:
    result = bytearray(_serialize_short_length(len(value)))
    for flag in value:
        result += serialize_short_string(flag)
    return struct.pack('<B', _ID.Flags_v2.value) + result
_serializers[set] = _serialize_flags


def _deserialize_overlay_v1(data: bytearray) -> Overlay:
    return Overlay(_deserialize_qstring(data))
_deserializers[_ID.Overlay_v1] = _deserialize_overlay_v1
def _deserialize_overlay_v2(data: bytearray) -> Overlay:
    n = struct.unpack('<I', data[:4])[0]
    result = Overlay(data[4:4 + n].decode('utf-8'))
    del data[:4+n]
    return result
_deserializers[_ID.Overlay_v2] = _deserialize_overlay_v2
def _serialize_overlay(value: Overlay) -> bytes:
    encoded = value.encode('utf-8')
    return struct.pack('<BI', _ID.Overlay_v2.value, len(encoded)) + encoded
_serializers[Overlay] = _serialize_overlay


def _deserialize_array_v1(data: bytearray) -> typing.List[typing.Any]:
    n = struct.unpack('<I', data[:4])[0]
    del data[:4]
    result = list()
    for i in range(n):
        result.append(deserialize(data))
    return result
_deserializers[_ID.Array_v1] = _deserialize_array_v1
def _deserialize_array_v2(data: bytearray) -> typing.List[typing.Any]:
    n = _deserialize_short_length(data)
    result = list()
    for i in range(n):
        result.append(deserialize(data))
    return result
_deserializers[_ID.Array_v2] = _deserialize_array_v2
def _serialize_array(value: typing.List[typing.Any]) -> bytes:
    result = bytearray(_serialize_short_length(len(value)))
    for c in value:
        result += serialize(c)
    return struct.pack('<B', _ID.Array_v2.value) + result
_serializers[list] = _serialize_array
_serializers[deque] = _serialize_array


def _deserialize_matrix_v1(data: bytearray) -> Matrix:
    n = struct.unpack('<I', data[:4])[0]
    del data[:4]
    result = Matrix()
    for i in range(n):
        result.append(deserialize(data))
    n = data[0]
    del data[0]
    for i in range(n):
        result.shape.append(struct.unpack('<I', data[:4])[0])
        del data[:4]
    return result
_deserializers[_ID.Matrix_v1] = _deserialize_matrix_v1
def _deserialize_matrix_v2(data: bytearray) -> Matrix:
    n = _deserialize_short_length(data)
    result = Matrix()
    for i in range(n):
        result.append(deserialize(data))
    n = data[0]
    del data[0]
    for i in range(n):
        result.shape.append(_deserialize_short_length(data))
    return result
_deserializers[_ID.Matrix_v2] = _deserialize_matrix_v2
def _serialize_matrix(value: Matrix) -> bytes:
    result = bytearray(_serialize_short_length(len(value)))
    for c in value:
        result += serialize(c)
    result.append(len(value.shape))
    for n in value.shape:
        result += _serialize_short_length(n)
    return struct.pack('<B', _ID.Matrix_v2.value) + result
_serializers[Matrix] = _serialize_matrix


def _deserialize_hash_v1(data: bytearray) -> typing.Dict[str, typing.Any]:
    n = struct.unpack('<I', data[:4])[0]
    del data[:4]
    result: typing.Dict[str, typing.Any] = dict()
    for i in range(n):
        key = _deserialize_qstring(data)
        value = deserialize(data)
        result[key] = value
    return result
_deserializers[_ID.Hash_v1] = _deserialize_hash_v1
def _deserialize_hash_v2(data: bytearray) -> typing.Dict[str, typing.Any]:
    n = _deserialize_short_length(data)
    result: typing.Dict[str, typing.Any] = dict()
    for i in range(n):
        key = deserialize_short_string(data)
        value = deserialize(data)
        result[key] = value
    return result
_deserializers[_ID.Hash_v2] = _deserialize_hash_v2
def _serialize_hash(value: typing.Dict[str, typing.Any]) -> bytes:
    result = bytearray(_serialize_short_length(len(value)))
    for k, v in value.items():
        result += serialize_short_string(k)
        result += serialize(v)
    return struct.pack('<B', _ID.Hash_v2.value) + result
_serializers[dict] = _serialize_hash
_serializers[OrderedDict] = _serialize_hash


def _deserialize_keyframe_v1(data: bytearray) -> Keyframe:
    n = struct.unpack('<I', data[:4])[0]
    del data[:4]
    result = Keyframe()
    for i in range(n):
        key = struct.unpack('<d', data[:8])[0]
        value = deserialize(data)
        result[key] = value
    return result
_deserializers[_ID.Keyframe_v1] = _deserialize_keyframe_v1
def _deserialize_keyframe_v2(data: bytearray) -> Keyframe:
    n = _deserialize_short_length(data)
    result = Keyframe()
    for i in range(n):
        key = struct.unpack('<d', data[:8])[0]
        del data[:8]
        value = deserialize(data)
        result[key] = value
    return result
_deserializers[_ID.Keyframe_v2] = _deserialize_keyframe_v2
def _serialize_keyframe(value: Keyframe) -> bytes:
    result = bytearray(_serialize_short_length(len(value)))
    for k, v in value.items():
        result += struct.pack('<d', k)
        result += serialize(v)
    return struct.pack('<B', _ID.Keyframe_v2.value) + result
_serializers[Keyframe] = _serialize_keyframe


def _declare_metadata(v1: _ID, v2: _ID, container: typing.Type[Metadata]):
    def deserialize_v1(data: bytearray) -> container:
        n = struct.unpack('<I', data[:4])[0]
        del data[:4]
        result = container()
        for i in range(n):
            key = _deserialize_qstring(data)
            value = deserialize(data)
            result[key] = value
        return result
    _deserializers[v1] = deserialize_v1

    def deserialize_v2(data: bytearray) -> container:
        n = _deserialize_short_length(data)
        result = container()
        for i in range(n):
            key = deserialize_short_string(data)
            value = deserialize(data)
            result[key] = value
        return result
    _deserializers[v2] = deserialize_v2

    def serialize_v2(value: container) -> bytes:
        result = bytearray(_serialize_short_length(len(value)))
        for k, v in value.items():
            result += serialize_short_string(k)
            result += serialize(v)
        return struct.pack('<B', v2.value) + result
    _serializers[container] = serialize_v2


class MetadataReal(Metadata):
    pass
_declare_metadata(_ID.MetadataReal_v1, _ID.MetadataReal_v2, MetadataReal)

class MetadataInteger(Metadata):
    pass
_declare_metadata(_ID.MetadataInteger_v1, _ID.MetadataInteger_v2, MetadataInteger)

class MetadataBoolean(Metadata):
    pass
_declare_metadata(_ID.MetadataBoolean_v1, _ID.MetadataBoolean_v2, MetadataBoolean)

class MetadataString(Metadata):
    pass
_declare_metadata(_ID.MetadataString_v1, _ID.MetadataString_v2, MetadataString)

class MetadataBytes(Metadata):
    pass
_declare_metadata(_ID.MetadataBytes_v1, _ID.MetadataBytes_v2, MetadataBytes)

class MetadataArray(Metadata):
    pass
_declare_metadata(_ID.MetadataArray_v1, _ID.MetadataArray_v2, MetadataArray)

class MetadataMatrix(Metadata):
    pass
_declare_metadata(_ID.MetadataMatrix_v1, _ID.MetadataMatrix_v2, MetadataMatrix)

class MetadataKeyframe(Metadata):
    pass
_declare_metadata(_ID.MetadataKeyframe_v1, _ID.MetadataKeyframe_v2, MetadataKeyframe)


def _declare_metadata_children(v1: _ID, v2: _ID, container: typing.Type[MetadataChildren]):
    def deserialize_v1(data: bytearray) -> container:
        n = struct.unpack('<I', data[:4])[0]
        del data[:4]
        result = container()
        for i in range(n):
            key = _deserialize_qstring(data)
            value = deserialize(data)
            result[key] = value
        n = struct.unpack('<I', data[:4])[0]
        del data[:4]
        for i in range(n):
            key = _deserialize_qstring(data)
            value = deserialize(data)
            result.children[key] = value
        return result
    _deserializers[v1] = deserialize_v1

    def deserialize_v2(data: bytearray) -> container:
        n = _deserialize_short_length(data)
        result = container()
        for i in range(n):
            key = deserialize_short_string(data)
            value = deserialize(data)
            result[key] = value
        n = _deserialize_short_length(data)
        for i in range(n):
            key = deserialize_short_string(data)
            value = deserialize(data)
            result.children[key] = value
        return result
    _deserializers[v2] = deserialize_v2

    def serialize_v2(value: container) -> bytes:
        result = bytearray(_serialize_short_length(len(value)))
        for k, v in value.items():
            result += serialize_short_string(k)
            result += serialize(v)
        result += _serialize_short_length(len(value.children))
        for k, v in value.children.items():
            result += serialize_short_string(k)
            result += serialize(v)
        return struct.pack('<B', v2.value) + result
    _serializers[container] = serialize_v2


class MetadataFlags(MetadataChildren):
    pass
_declare_metadata_children(_ID.MetadataFlags_v1, _ID.MetadataFlags_v2, MetadataFlags)

class MetadataHash(MetadataChildren):
    pass
_declare_metadata_children(_ID.MetadataHash_v1, _ID.MetadataHash_v2, MetadataHash)


def deserialize(data: typing.Union[bytearray, bytes]) -> typing.Any:
    if isinstance(data, bytes):
        data = bytearray(data)
    type_code = data[0]
    del data[0]
    return _deserializers[_ID(type_code)](data)


def serialize(variant: typing.Any) -> bytes:
    return _serializers[type(variant)](variant)
