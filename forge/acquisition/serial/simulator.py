import typing
import asyncio
import termios
import os
import logging
import argparse
from forge.acquisition.instrument.streaming import StreamingSimulator
from .util import standard_termios


class _TTYContext:
    def __init__(self, master: int, slave: int, target: str):
        self.master = master
        self.slave = slave
        self.target = target
        self.tty_name = os.ttyname(slave)

        tio = termios.tcgetattr(self.slave)
        standard_termios(tio)
        termios.tcsetattr(self.slave, termios.TCSANOW, tio)

        umask = os.umask(0o666) | 0o111
        os.umask(umask)
        os.fchmod(self.slave, 0o666 & ~umask)
        try:
            os.fchown(self.slave, os.geteuid(), os.getegid())
        except PermissionError:
            pass

        os.set_blocking(self.master, False)

        try:
            os.unlink(self.target)
        except OSError:
            pass
        os.symlink(self.tty_name, self.target)

    def drain(self) -> None:
        termios.tcdrain(self.slave)

    def close(self) -> None:
        if self.master is not None:
            os.close(self.master)
            self.master = None
        if self.slave is not None:
            os.close(self.slave)
            self.slave = None
        if self.target is not None:
            try:
                os.unlink(self.target)
            except OSError:
                pass
            self.target = None

    def __del__(self):
        self.close()


class _SimulatorReader(asyncio.StreamReader):
    def __init__(self, tty_context: _TTYContext):
        super().__init__()
        self.tty_context = tty_context


class _SimulatorWriter(asyncio.StreamWriter):
    def __init__(self, transport, protocol, reader, loop, tty_context: _TTYContext):
        super().__init__(transport, protocol, reader, loop)
        self.tty_context = tty_context

    async def drain(self) -> None:
        await super().drain()
        self.tty_context.drain()

    async def wait_closed(self) -> None:
        self.tty_context.close()


async def create_simulator_streams(output: str) -> typing.Tuple[asyncio.StreamReader, asyncio.StreamWriter]:
    master, slave = os.openpty()
    context = _TTYContext(master, slave, output)
    read = os.fdopen(os.dup(master), mode='rb')
    write = os.fdopen(os.dup(master), mode='wb')

    loop = asyncio.get_event_loop()
    reader = _SimulatorReader(context)
    await loop.connect_read_pipe(lambda: asyncio.StreamReaderProtocol(reader), read)

    transport, protocol = await loop.connect_write_pipe(asyncio.streams.FlowControlMixin, write)
    writer = _SimulatorWriter(transport, protocol, reader, loop, context)

    return reader, writer


def arguments() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Forge acquisition simulated instrument.")

    parser.add_argument('--debug',
                        dest='debug', action='store_true',
                        help="enable debug output")

    parser.add_argument('tty',
                        help="output pseudoterminal")

    return parser


def parse_arguments(parser: typing.Optional[argparse.ArgumentParser] = None) -> str:
    if parser is None:
        parser = arguments()

    args, _ = parser.parse_known_args()
    if args.debug:
        root_logger = logging.getLogger()
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(name)-40s %(message)s')
        handler.setFormatter(formatter)
        root_logger.setLevel(logging.DEBUG)
        root_logger.addHandler(handler)

    return args.tty


def run(output: str, simulator: typing.Type[StreamingSimulator], *args, **kwargs) -> None:
    async def inner():
        reader, writer = await create_simulator_streams(output)
        active_simulator = simulator(reader, writer, *args, **kwargs)
        await active_simulator.run()
        writer.close()
        await writer.wait_closed()

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(inner())
    loop.close()
