import typing
import asyncio
import logging
import struct
from starlette.websockets import WebSocket
from forge.authsocket import WebsocketBinary as AuthSocket
from forge.cpd3.identity import Name as RealtimeName
from forge.cpd3.variant import deserialize as deserialize_variant, serialize as serialize_variant
from .protocol import PROTOCOL_VERSION, PacketFromAcquisition, PacketToAcquisition, DataBlockType


_LOGGER = logging.getLogger(__name__)


class AcquisitionSocket(AuthSocket):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.has_instantaneous_data: bool = None
        self.websocket: WebSocket = None
        self._in_data_block: bool = False
        self._data_block_contents: typing.Dict[RealtimeName, typing.Any] = dict()
        self._name_lookup: typing.List[RealtimeName] = list()
        self._name_lookup_overwrite: int = 0

    async def handshake(self, websocket: WebSocket, data: bytes) -> bool:
        if len(data) < 2:
            _LOGGER.debug(f"Invalid handshake data for {self.display_id}")
            return False
        version, has_instant = struct.unpack('<BB', data[:2])
        if version != PROTOCOL_VERSION:
            _LOGGER.debug(f"Incompatible protocol version for {self.display_id}")
            return False
        self.has_instantaneous_data = (has_instant != 0)
        self.websocket = websocket
        return True

    @staticmethod
    def _deserialize_name_and_value(data: bytes) -> typing.Tuple[str, typing.Any]:
        data = bytearray(data)
        name_length = (struct.unpack('<H', data[:2]))[0]
        del data[:2]
        name = (data[:name_length]).decode('utf-8')
        del data[:name_length]
        value = deserialize_variant(data)
        return name, value

    @staticmethod
    def _deserialize_array_length(data: bytearray) -> int:
        count = data[0]
        del data[:1]
        if count != 0xFF:
            return count
        count = (struct.unpack('<I', data[:4]))[0]
        del data[:4]
        return count

    async def _handle_data_block(self, data: bytes) -> None:
        packet_type = DataBlockType(data[0])
        if packet_type == DataBlockType.FLOATS:
            data = bytearray(data[1:])
            while len(data) > 0:
                name_index, value = struct.unpack('<Hf', data[:6])
                del data[:6]
                self._data_block_contents[self._name_lookup[name_index]] = value
        elif packet_type == DataBlockType.ARRAYS_OF_FLOATS:
            data = bytearray(data[1:])
            while len(data) > 0:
                name_index = (struct.unpack('<H', data[:2]))[0]
                del data[:2]
                count = self._deserialize_array_length(data)
                contents: typing.List[float] = list()
                for i in range(count):
                    contents.append(struct.unpack('<f', data[(i*4):(i*4+4)])[0])
                del data[:(count*4)]
                self._data_block_contents[self._name_lookup[name_index]] = contents
        elif packet_type == DataBlockType.VARIANT:
            data = bytearray(data[1:])
            while len(data) > 0:
                name_index = (struct.unpack('<H', data[:2]))[0]
                del data[:2]
                value = deserialize_variant(data)
                self._data_block_contents[self._name_lookup[name_index]] = value
        elif packet_type == DataBlockType.FINAL:
            if len(self._data_block_contents) > 0:
                await self.incoming_data(self._data_block_contents)
            self._data_block_contents.clear()
            self._in_data_block = False
        else:
            raise ValueError(f"Invalid data block type {packet_type}")

    def _handle_define_names(self, data: bytes) -> None:
        data = bytearray(data)
        while len(data) > 0:
            name = RealtimeName.deserialize(data)
            if len(self._name_lookup) <= 0xFFFF:
                self._name_lookup.append(name)
            else:
                self._name_lookup[self._name_lookup_overwrite] = name
                self._name_lookup_overwrite = (self._name_lookup_overwrite + 1) & 0xFFFF

    async def websocket_data(self, websocket: WebSocket, data: bytes) -> None:
        if self._in_data_block:
            return await self._handle_data_block(data)

        packet_type = PacketFromAcquisition(data[0])
        if packet_type == PacketFromAcquisition.DATA_BLOCK_BEGIN:
            self._in_data_block = True
            return await self._handle_data_block(data[1:])
        elif packet_type == PacketFromAcquisition.DEFINE_NAMES:
            self._handle_define_names(data[1:])
        elif packet_type == PacketFromAcquisition.EVENT:
            await self.incoming_event(deserialize_variant(data[1:]))
        elif packet_type == PacketFromAcquisition.AUTOPROBE_STATE:
            await self.autoprobe_state_updated(deserialize_variant(data[1:]))
        elif packet_type == PacketFromAcquisition.INTERFACE_INFORMATION:
            interface_name, info = self._deserialize_name_and_value(data[1:])
            await self.interface_information_updated(interface_name, info)
        elif packet_type == PacketFromAcquisition.INTERFACE_STATE:
            interface_name, state = self._deserialize_name_and_value(data[1:])
            await self.interface_state_updated(interface_name, state)
        else:
            raise ValueError(f"Invalid acquisition packet type {packet_type}")

    async def on_disconnect(self, websocket: WebSocket, close_code):
        self.websocket = None
        await super().on_disconnect(websocket, close_code)

    async def message_log(self, entry: typing.Dict[str, typing.Any]) -> None:
        if not self.websocket:
            return
        await self.websocket.send_bytes(struct.pack('<B', PacketToAcquisition.MESSAGE_LOG.value) +
                                        serialize_variant(entry))

    async def command(self, command: typing.Any = None, target: typing.Optional[str] = None) -> None:
        if not self.websocket:
            return
        if not target:
            target = ''
        raw = target.encode('utf-8')
        await self.websocket.send_bytes(struct.pack('<BH', PacketToAcquisition.COMMAND.value, len(raw)) +
                                        raw + serialize_variant(command))
        
    async def _command_and_string(self, command: PacketToAcquisition, s: str) -> None:
        if not self.websocket:
            return
        raw = s.encode('utf-8')
        await self.websocket.send_bytes(struct.pack('<BH', command.value) + raw)

    async def bypass_set(self, flag: str = 'Bypass') -> None:
        return await self._command_and_string(PacketToAcquisition.BYPASS_FLAG_SET, flag)

    async def bypass_clear(self, flag: str = 'Bypass') -> None:
        return await self._command_and_string(PacketToAcquisition.BYPASS_FLAG_CLEAR, flag)

    async def bypass_clear_all(self) -> None:
        if not self.websocket:
            return
        await self.websocket.send_bytes(struct.pack('<B', PacketToAcquisition.BYPASS_FLAGS_CLEAR_ALL.value))

    async def flag_set(self, flag: str = 'Contaminated') -> None:
        return await self._command_and_string(PacketToAcquisition.SYSTEM_FLAG_SET, flag)

    async def flag_clear(self, flag: str = 'Contaminated') -> None:
        return await self._command_and_string(PacketToAcquisition.SYSTEM_FLAG_CLEAR, flag)

    async def flag_clear_all(self) -> None:
        if not self.websocket:
            return
        await self.websocket.send_bytes(struct.pack('<B', PacketToAcquisition.SYSTEM_FLAGS_CLEAR_ALL.value))

    async def system_flush(self, duration: float = None) -> None:
        if not self.websocket:
            return
        if duration is None:
            duration = -1.0
        await self.websocket.send_bytes(struct.pack('<Bd', PacketToAcquisition.SYSTEM_FLUSH.value, duration))

    async def restart_acquisition_system(self,) -> None:
        if not self.websocket:
            return
        await self.websocket.send_bytes(struct.pack('<B', PacketToAcquisition.RESTART_ACQUISITION_SYSTEM.value))

    async def incoming_data(self, values: typing.Dict[RealtimeName, typing.Any]) -> None:
        pass

    async def incoming_event(self, event: typing.Dict[str, typing.Any]) -> None:
        pass

    async def autoprobe_state_updated(self, state: typing.Dict[str, typing.Any]) -> None:
        pass

    async def interface_information_updated(self, interface_name: str,
                                            information: typing.Optional[typing.Dict[str, typing.Any]]) -> None:
        pass

    async def interface_state_updated(self, interface_name: str,
                                      state: typing.Optional[typing.Dict[str, typing.Any]]) -> None:
        pass

