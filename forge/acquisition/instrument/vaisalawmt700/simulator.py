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
        self.data_Tsonic = 21.0
        self.data_Ttransducer = 23.0
        self.data_Vsupply = 12.0
        self.data_Vheater = 11.0

        self.flags = 0

    async def _unpolled(self) -> typing.NoReturn:
        while True:
            frame = (
                "$"
                f"{self.data_WS:5.2f},"
                f"{self.data_WD:.2f},"
                f"{self.data_WS+1:5.2f},"
                f"{self.data_WS-1:5.2f},"
                f"{self.data_Tsonic:.2f},"
                f"{self.data_Vheater:.1f},"
                f"{self.data_Vsupply:.1f},"
                f"{self.data_Ttransducer:.1f},"
                f"{self.flags},"
            ).encode('ascii')
            cs = 0
            for v in frame:
                cs ^= v
            self.writer.write(frame)
            self.writer.write(b"%02X\r" % cs)

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
                    if b"$0OPEN" in line:
                        await self._stop_unpolled()
                        self.writer.write(b'OK\r>')
                    elif line == b'CLOSE':
                        self.writer.write(b'closed\r')
                        await self._start_unpolled()
                    elif line == b"START":
                        self.writer.write(b'OK\r')
                    elif line.startswith(B"VERSION"):
                        self.writer.write(b'WMT700 v123\r')
                    elif line == b"G serial_n":
                        self.writer.write(b'>s serial_n    ,4567\r')
                    elif line == b"G serial_pcb":
                        self.writer.write(b'89\r')
                    elif line == b"G cal_date":
                        self.writer.write(b'>s cal_date   ,20180101\r')
                    elif line.startswith(B"S messages,"):
                        self.writer.write(b'>s messages,1\r')
                    elif line.startswith(B"S wndUnit,"):
                        self.writer.write(b'>s wndUnit,1\r')
                    elif line.startswith(B"S wndVector,"):
                        self.writer.write(b'>s wndVector,!1\r')
                    elif line.startswith(B"S autoSend,"):
                        self.writer.write(b'>s autoSend,!1\r')
                    elif line.startswith(B"S autoInt,"):
                        self.unpolled_interval = float(line[10:])
                        self.writer.write(b'>s autoInt,!1\r')
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
