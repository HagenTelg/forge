import typing
import asyncio
import logging
import argparse
import signal
import threading
import sys
import os
import struct
import enum
import time
from forge.formattime import format_iso8601_time
from forge.acquisition.serial.multiplexer.protocol import ToMultiplexer, FromMultiplexer


_LOGGER = logging.getLogger(__name__)


def sync_forward(read: typing.BinaryIO, write: typing.BinaryIO) -> None:
    try:
        while True:
            data = os.read(read.fileno(), 4096)
            if not data:
                break
            os.write(write.fileno(), data)
    finally:
        try:
            write.close()
        except OSError:
            pass
        try:
            read.close()
        except OSError:
            pass


async def open_stdio() -> typing.Tuple[asyncio.StreamReader, asyncio.StreamWriter]:
    loop = asyncio.get_event_loop()
    reader = asyncio.StreamReader()
    r_protocol = asyncio.StreamReaderProtocol(reader)

    try:
        await loop.connect_read_pipe(lambda: r_protocol, sys.stdin)
    except ValueError:
        # connect_*_pipe bails out of the file is a regular file, which can happen for stdio redirection
        read, write = os.pipe()
        read = os.fdopen(read, mode='rb')
        write = os.fdopen(write, mode='wb')
        await loop.connect_read_pipe(lambda: r_protocol, read)
        threading.Thread(target=sync_forward, args=(sys.stdin, write,), daemon=True).start()

    try:
        w_transport, w_protocol = await loop.connect_write_pipe(asyncio.streams.FlowControlMixin, sys.stdout)
    except ValueError:
        # Like with read, this can happen for redirection
        read, write = os.pipe()
        read = os.fdopen(read, mode='rb')
        write = os.fdopen(write, mode='wb')
        w_transport, w_protocol = await loop.connect_write_pipe(asyncio.streams.FlowControlMixin, write)
        threading.Thread(target=sync_forward, args=(read, sys.stdout.buffer,), daemon=True).start()
    writer = asyncio.StreamWriter(w_transport, w_protocol, reader, loop)

    return reader, writer


async def write_uplink_data(uplink: asyncio.StreamWriter, data: bytes, packet_type: ToMultiplexer) -> None:
    while data:
        packet_length = min(0xFF, len(data))
        uplink.write(struct.pack('<BB', packet_type.value, packet_length))
        uplink.write(data[0:packet_length])
        if packet_length >= len(data):
            break
        data = data[packet_length:]
    await uplink.drain()


async def read_stdin_lines(reader: asyncio.StreamReader, uplink: asyncio.StreamWriter,
                           packet_type: ToMultiplexer) -> None:
    while True:
        line = await reader.readline()
        if not line:
            break
        if line.endswith(b'\n'):
            line = line[:-1]
        line = line + b'\r\n'
        await write_uplink_data(uplink, line, packet_type)


async def read_stdin_raw(reader: asyncio.StreamReader, uplink: asyncio.StreamWriter,
                         packet_type: ToMultiplexer) -> None:
    while True:
        data = await reader.read(0xFF)
        if not data:
            break
        await write_uplink_data(uplink, data, packet_type)


class EavesdroppedDirection(enum.Enum):
    FROM_SERIAL_PORT = enum.auto()
    TO_SERIAL_PORT = enum.auto()


async def read_eavesdropped(evs: asyncio.StreamReader):
    read_buffer = bytearray()

    def extract_read_packet():
        if len(read_buffer) < 2:
            return None
        packet_length = struct.unpack_from('<B', read_buffer, offset=1)[0]
        packet_end = 2 + packet_length
        if len(read_buffer) < packet_end:
            return None
        data = bytes(read_buffer[2:packet_end])
        del read_buffer[0:packet_end]
        return data

    while True:
        data = await evs.read(4096)
        if not data:
            break
        read_buffer += data

        while read_buffer:
            packet_type = FromMultiplexer(struct.unpack_from('<B', read_buffer)[0])

            if packet_type == FromMultiplexer.FROM_SERIAL_PORT:
                data = extract_read_packet()
                if data:
                    yield data, EavesdroppedDirection.FROM_SERIAL_PORT
            elif packet_type == FromMultiplexer.TO_SERIAL_PORT:
                data = extract_read_packet()
                if data:
                    yield data, EavesdroppedDirection.TO_SERIAL_PORT
            else:
                raise ValueError(f"Unsupported packet type {packet_type}")


def output_display_data(output: asyncio.StreamWriter, data: bytearray, direction: EavesdroppedDirection,
                        time_origin: typing.Optional[float] = None,
                        output_unterminated: bool = True) -> None:
    if not time_origin:
        time_origin = time.time()
    timestamp = format_iso8601_time(time_origin, milliseconds=True).encode('utf-8')
    while data:
        end_of_line = data.find(b'\r')
        if end_of_line > 0:
            check = data.find(b'\n', 0, end_of_line)
            if check >= 0:
                end_of_line = check
        elif end_of_line < 0:
            end_of_line = data.find(b'\n')

        if end_of_line == 0:
            del data[0]
            continue

        if end_of_line < 0:
            if not output_unterminated:
                break
            line_data = bytes(data)
            data.clear()
        else:
            line_data = data[0:end_of_line]
            del data[0:end_of_line]

        output.write(timestamp)
        if direction == EavesdroppedDirection.TO_SERIAL_PORT:
            output.write(b'< ')
        else:
            output.write(b'> ')
        output.write(line_data)
        output.write(b'\n')


async def display_raw(evs: asyncio.StreamReader, output: asyncio.StreamWriter) -> None:
    async for data, direction in read_eavesdropped(evs):
        output_display_data(output, bytearray(data), direction)
        await output.drain()


async def display_lines(evs: asyncio.StreamReader, output: asyncio.StreamWriter) -> None:
    line_buffers = {
        EavesdroppedDirection.FROM_SERIAL_PORT: bytearray(),
        EavesdroppedDirection.TO_SERIAL_PORT: bytearray(),
    }

    flush_tasks: typing.Dict[EavesdroppedDirection, asyncio.Task] = dict()

    async def flush_buffer(buffer: bytearray, direction: EavesdroppedDirection) -> None:
        time_origin = time.time()
        await asyncio.sleep(0.25)
        output_display_data(output, buffer, direction, time_origin=time_origin)

    async for data, direction in read_eavesdropped(evs):
        buffer = line_buffers.get(direction)
        if buffer is None:
            continue
        buffer += data
        output_display_data(output, buffer, direction, output_unterminated=False)
        await output.drain()

        existing_flush = flush_tasks.get(direction)
        if existing_flush and existing_flush.done():
            t = existing_flush
            existing_flush = None
            del flush_tasks[direction]
            await t

        if not buffer and existing_flush:
            del flush_tasks[direction]
            try:
                existing_flush.cancel()
            except:
                pass
            try:
                await existing_flush
            except asyncio.CancelledError:
                pass
        elif buffer and not existing_flush:
            flush_tasks[direction] = asyncio.ensure_future(flush_buffer(buffer, direction))

    for t in list(flush_tasks.values()):
        try:
            t.cancel()
        except:
            pass
        try:
            await t
        except asyncio.CancelledError:
            pass

    for direction, data in line_buffers.items():
        output_display_data(output, data, direction)
    await output.drain()


def signal_terminate_tasks(*tasks: asyncio.Task):
    def run_termination():
        for t in tasks:
            try:
                t.cancel()
            except:
                pass

    loop = asyncio.get_event_loop()
    loop.add_signal_handler(signal.SIGINT, run_termination)
    loop.add_signal_handler(signal.SIGTERM, run_termination)


async def run(args) -> None:
    evs_reader, evs_writer = await asyncio.open_unix_connection(args.socket)
    local_reader, local_writer = await open_stdio()

    if args.write_multiplexed:
        local_read_target = ToMultiplexer.WRITE_MULTIPLEXED
    else:
        local_read_target = ToMultiplexer.WRITE_SERIAL_PORT
    if args.unbuffered:
        input_task = asyncio.ensure_future(read_stdin_raw(local_reader, evs_writer, local_read_target))
    else:
        input_task = asyncio.ensure_future(read_stdin_lines(local_reader, evs_writer, local_read_target))

    if args.unbuffered:
        output_task = asyncio.ensure_future(display_raw(evs_reader, local_writer))
    else:
        output_task = asyncio.ensure_future(display_lines(evs_reader, local_writer))

    signal_terminate_tasks(input_task, output_task)

    try:
        await output_task
    except asyncio.CancelledError:
        pass

    try:
        input_task.cancel()
    except:
        pass
    try:
        await input_task
    except asyncio.CancelledError:
        pass


def main():
    parser = argparse.ArgumentParser(description="Acquisition serial eavesdropper client.")

    parser.add_argument('--debug',
                        dest='debug', action='store_true',
                        help="enable debug output")
    parser.add_argument('--unbuffered',
                        dest='unbuffered', action='store_true',
                        help="disable line buffering")
    parser.add_argument('--write-multiplexed',
                        dest='write_multiplexed', action='store_true',
                        help="write to downstream connections instead of the serial port")

    parser.add_argument('socket',
                        help="eavesdropper socket path")

    args = parser.parse_args()
    if args.debug:
        root_logger = logging.getLogger()
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(name)-40s %(message)s')
        handler.setFormatter(formatter)
        root_logger.setLevel(logging.DEBUG)
        root_logger.addHandler(handler)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(run(args))
    loop.close()


if __name__ == '__main__':
    main()
