import typing
import asyncio
from forge.acquisition.instrument.streaming import StreamingSimulator


class Simulator(StreamingSimulator):
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        super().__init__(reader, writer)

        self.unpolled_interval = 1.0
        self._unpolled_task: typing.Optional[asyncio.Task] = None

        self.data_WS = 1.5
        self.data_WD = 57.0

        self.status = 0

    async def _unpolled(self) -> typing.NoReturn:
        while True:
            frame = (
                "a "
                f"{self.data_WS:.2f} "
                f"{self.data_WD:.1f} "
                f"{self.status:02X}"
            ).encode('ascii')
            cs = 0
            for v in frame:
                cs ^= v
            self.writer.write(frame)
            self.writer.write(b"*%02X\r" % cs)

            await asyncio.sleep(self.unpolled_interval)

    async def _stop_unpolled(self) -> None:
        t = self._unpolled_task
        self._unpolled_task = None
        if not t:
            return
        try:
            t.cancel()
        except:
            pass
        try:
            await t
        except:
            pass

    async def _start_unpolled(self) -> None:
        await self._stop_unpolled()
        self._unpolled_task = asyncio.ensure_future(self._unpolled())

    async def run(self) -> typing.NoReturn:
        try:
            await self._start_unpolled()
            while True:
                line = await self.reader.readuntil(b'\r')
                line = line.strip()
                if not line:
                    continue

                try:
                    if b"\x1B" in line:
                        await self._stop_unpolled()
                        self.writer.write(b'CMD ERR\rCONSOLE\r')
                    elif line == b'XX':
                        self.writer.write(b'RESUMING UNPOLLED\r')
                        await self._start_unpolled()
                    elif line.startswith(B"SET02"):
                        self.writer.write(b'>SET02\r')
                    elif line.startswith(B"SET04"):
                        self.writer.write(b'>SET04\r')
                    elif line.startswith(B"SET10"):
                        self.writer.write(b'>SET10\r')
                    elif line.startswith(B"SET13"):
                        self.writer.write(b'>SET13\r')
                    elif line.startswith(B"SET15"):
                        self.writer.write(b'>SET15\r')
                    elif line.startswith(B"SET10"):
                        self.unpolled_interval = int(line[5:]) / 1000.0
                        self.writer.write(b'>SET10\r')
                    else:
                        raise ValueError
                except (ValueError, IndexError):
                    self.writer.write(b'CMD ERR\r')
                await self.writer.drain()
        finally:
            await self._stop_unpolled()


if __name__ == '__main__':
    from forge.acquisition.serial.simulator import parse_arguments, run
    run(parse_arguments(), Simulator)
