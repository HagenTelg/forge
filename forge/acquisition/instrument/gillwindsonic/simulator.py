import typing
import asyncio
from forge.acquisition.instrument.streaming import StreamingSimulator


class Simulator(StreamingSimulator):
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        super().__init__(reader, writer)

        self.unpolled_interval = 1.0
        self._unpolled_task: typing.Optional[asyncio.Task] = None

        self.data_WS = 2.75
        self.data_WD = 229.0

        self.status = 0

    async def _unpolled(self) -> typing.NoReturn:
        while True:
            frame = (
                "Q,"
                f"{self.data_WD:.0f},"
                f"{self.data_WS:05.2f},"
                "M,"
                f"{self.status:02X},"
            ).encode('ascii')
            cs = 0
            for v in frame:
                cs ^= v
            self.writer.write(b"\x02")
            self.writer.write(frame)
            self.writer.write(b"\x03%02X\r\n" % cs)

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
                    if line.startswith(b'*'):
                        await self._stop_unpolled()
                    elif line == b'D1':
                        self.writer.write(b'D1\r\nY16120001\r\n')
                    elif line == b'D2':
                        self.writer.write(b'D2\r\n2368-110-01\r\n')
                    elif line == b'D3':
                        self.writer.write(b'D3\r\nM2,U1,O1,L1,P1,B3,H1,NQ,F1,E2,T1,S4,C2,G0,K50,\r\n')
                    elif line == b'Q':
                        self.writer.write(
                            b"WINDSONIC (Gill Instruments Ltd)\n"
                            b"2368-110-01\n"
                            b"RS232 (CFG)\n"
                            b"CHECKSUM ROM:E6D1 E6D1 *PASS*\n"
                            b"CHECKSUM FAC:09EA 09EA *PASS*\n"
                            b"CHECKSUM ENG:17FB 17FB *PASS*\n"
                            b"CHECKSUM CAL:CC55 CC55 *PASS*"
                        )
                        await self._start_unpolled()
                    elif line.startswith(b"U") or line.startswith(b"O") or line.startswith(b"M") or line.startswith(b"P"):
                        self.writer.write(line + b"\r\n")
                    else:
                        raise ValueError
                except (ValueError, IndexError):
                    self.writer.write(b'ERROR\r')
                await self.writer.drain()
        finally:
            await self._stop_unpolled()


if __name__ == '__main__':
    from forge.acquisition.serial.simulator import parse_arguments, run
    run(parse_arguments(), Simulator)
