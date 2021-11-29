import pytest
import asyncio
import typing
import struct
import os
from forge.telemetry.tunnel.hub import Server
from forge.telemetry.tunnel.protocol import ServerConnectionType, FromRemotePacketType, ToRemotePacketType, InitiateConnectionStatus


async def start_server(tmp_path):
    async def connection(r: asyncio.StreamReader, w: asyncio.StreamWriter) -> None:
        await Server.connection(None, r, w)

    socket_name = str(tmp_path / 'backend.sock')
    try:
        os.unlink(socket_name)
    except OSError:
        pass
    return await asyncio.start_unix_server(connection, path=str(tmp_path / 'backend.sock'))


async def connect_socket(tmp_path) -> typing.Tuple[asyncio.StreamReader, asyncio.StreamWriter]:
    return await asyncio.open_unix_connection(str(tmp_path / 'backend.sock'))


async def read_remote_data(reader: asyncio.StreamReader, expected_id: int, expected_length: int) -> bytes:
    result = bytes()
    while len(result) < expected_length:
        packet = await reader.readexactly(5)
        packet_type, connection_id, data_length = struct.unpack('<BHH', packet)
        assert packet_type == ToRemotePacketType.DATA
        assert connection_id == expected_id
        assert data_length > 0
        data = await reader.readexactly(data_length)
        result += data
    return result


@pytest.mark.asyncio
async def test_basic(tmp_path):
    server = await start_server(tmp_path)
    
    host_id = b'a' * 32
    
    remote_reader, remote_writer = await connect_socket(tmp_path)
    remote_writer.write(struct.pack('<B',  ServerConnectionType.TO_REMOTE.value))
    remote_writer.write(host_id)
    await remote_writer.drain()
    
    connection_reader, connection_writer = await connect_socket(tmp_path)
    connection_writer.write(struct.pack('<B',  ServerConnectionType.INITIATE_CONNECTION.value))
    connection_writer.write(host_id)
    await connection_writer.drain()

    packet = await remote_reader.readexactly(3)
    packet_type, connection_id = struct.unpack('<BH', packet)
    assert packet_type == ToRemotePacketType.SSH_CONNECTION_OPEN.value
    assert connection_id >= 0
    remote_writer.write(struct.pack('<BH', FromRemotePacketType.CONNECTION_OPEN.value, connection_id))
    await remote_writer.drain()

    packet = await connection_reader.readexactly(1)
    packet_type = struct.unpack('<B', packet)[0]
    assert packet_type == InitiateConnectionStatus.OK.value

    data = b'1' * 10
    connection_writer.write(data)
    await connection_writer.drain()
    packet = await read_remote_data(remote_reader, connection_id, 10)
    assert packet == data

    data = b'2' * 11
    remote_writer.write(struct.pack('<BHH', FromRemotePacketType.DATA, connection_id, len(data)))
    remote_writer.write(data)
    await remote_writer.drain()
    packet = await connection_reader.readexactly(11)
    assert packet == data

    connection_writer.close()
    packet = await remote_reader.readexactly(3)
    packet_type, closed_connection_id = struct.unpack('<BH', packet)
    assert packet_type == ToRemotePacketType.CONNECTION_CLOSE.value
    assert closed_connection_id == connection_id

    server.close()
    await server.wait_closed()


@pytest.mark.asyncio
async def test_remote_closed(tmp_path):
    server = await start_server(tmp_path)

    host_id = b'b' * 32

    remote_reader, remote_writer = await connect_socket(tmp_path)
    remote_writer.write(struct.pack('<B', ServerConnectionType.TO_REMOTE.value))
    remote_writer.write(host_id)
    await remote_writer.drain()

    connection_reader, connection_writer = await connect_socket(tmp_path)
    connection_writer.write(struct.pack('<B', ServerConnectionType.INITIATE_CONNECTION.value))
    connection_writer.write(host_id)
    await connection_writer.drain()

    packet = await remote_reader.readexactly(3)
    packet_type, connection_id = struct.unpack('<BH', packet)
    assert packet_type == ToRemotePacketType.SSH_CONNECTION_OPEN.value
    assert connection_id >= 0
    remote_writer.write(struct.pack('<BH', FromRemotePacketType.CONNECTION_OPEN.value, connection_id))
    await remote_writer.drain()

    packet = await connection_reader.readexactly(1)
    packet_type = struct.unpack('<B', packet)[0]
    assert packet_type == InitiateConnectionStatus.OK.value

    remote_writer.write(struct.pack('<BH', FromRemotePacketType.CONNECTION_CLOSED, connection_id))
    await remote_writer.drain()

    packet = await connection_reader.read(1)
    assert not packet

    server.close()
    await server.wait_closed()


@pytest.mark.asyncio
async def test_remote_disconnect(tmp_path):
    server = await start_server(tmp_path)

    host_id = b'c' * 32

    remote_reader, remote_writer = await connect_socket(tmp_path)
    remote_writer.write(struct.pack('<B', ServerConnectionType.TO_REMOTE.value))
    remote_writer.write(host_id)
    await remote_writer.drain()

    connection_reader, connection_writer = await connect_socket(tmp_path)
    connection_writer.write(struct.pack('<B', ServerConnectionType.INITIATE_CONNECTION.value))
    connection_writer.write(host_id)
    await connection_writer.drain()

    packet = await remote_reader.readexactly(3)
    packet_type, connection_id = struct.unpack('<BH', packet)
    assert packet_type == ToRemotePacketType.SSH_CONNECTION_OPEN.value
    assert connection_id >= 0
    remote_writer.write(struct.pack('<BH', FromRemotePacketType.CONNECTION_OPEN.value, connection_id))
    await remote_writer.drain()

    packet = await connection_reader.readexactly(1)
    packet_type = struct.unpack('<B', packet)[0]
    assert packet_type == InitiateConnectionStatus.OK.value

    remote_writer.close()

    packet = await connection_reader.read(1)
    assert not packet

    server.close()
    await server.wait_closed()


@pytest.mark.asyncio
async def test_unknown_id(tmp_path):
    server = await start_server(tmp_path)

    host_id = b'd' * 32

    remote_reader, remote_writer = await connect_socket(tmp_path)
    remote_writer.write(struct.pack('<B', ServerConnectionType.TO_REMOTE.value))
    remote_writer.write(host_id)
    await remote_writer.drain()

    connection_reader, connection_writer = await connect_socket(tmp_path)
    connection_writer.write(struct.pack('<B', ServerConnectionType.INITIATE_CONNECTION.value))
    connection_writer.write(b'z' * 32)
    await connection_writer.drain()

    packet = await connection_reader.readexactly(1)
    packet_type = struct.unpack('<B', packet)[0]
    assert packet_type == InitiateConnectionStatus.TARGET_NOT_FOUND.value

    packet = await connection_reader.read(1)
    assert not packet

    server.close()
    await server.wait_closed()
