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
        self.data_WSgust = 2.0
        self.data_WI = 0.5
        self.data_P = 880.0
        self.data_Uambient = 35.0
        self.data_Tambient = 21.0
        self.data_Tinternal = 22.0
        self.data_Theater = 23.0
        self.data_Vsupply = 12.0
        self.data_Vreference = 3.5
        self.data_Vheater = 11.0

        self.data_R: typing.Optional[float] = None
        self.data_Ld: typing.Optional[float] = None

    def _send_nmea(self, frame: bytes) -> None:
        cs = 0
        for v in frame:
            cs ^= v
        self.writer.write(b"$")
        self.writer.write(frame)
        self.writer.write(b"*%02X\r\n" % cs)

    async def _unpolled(self) -> typing.NoReturn:
        while True:
            self._send_nmea((
                "WIXDR,"
                f"A,{self.data_WD:.1f},D,1,"
                f"S,{self.data_WS:.1f},M,1,"
                f"S,{self.data_WSgust:.1f},M,2"
            ).encode('ascii'))

            self._send_nmea((
                "WIXDR,"
                f"P,{self.data_P:.1f},H,0,"
                f"C,{self.data_Tambient:.1f},C,0,"
                f"C,{self.data_Tinternal:.1f},C,1,"
                f"H,{self.data_Uambient:.1f},P,0"
            ).encode('ascii'))

            self._send_nmea((
                "WIXDR,"
                f"R,{self.data_WI:.1f},M,0"
            ).encode('ascii'))

            frame = bytes()
            if self.data_R:
                if frame:
                    frame += b","
                frame += f"U,{self.data_R:.1f},V,3".encode('ascii')
            if self.data_Ld:
                if frame:
                    frame += b","
                frame += f"U,{self.data_Ld:.1f},V,4".encode('ascii')
            if frame:
                self._send_nmea(b"WIXDR," + frame)

            self._send_nmea((
                "WIXDR,"
                f"C,{self.data_Theater:.1f},C,2,"
                f"U,{self.data_Vsupply:.1f},V,0,"
                f"U,{self.data_Vheater:.1f},N,1,"
                f"U,{self.data_Vreference:.1f},V,2"
                # f"G,HEL___,,4"
            ).encode('ascii'))

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

                def split_fields(raw: bytes) -> typing.Dict[bytes, bytes]:
                    result = dict()
                    for f in raw.split(b','):
                        (key, value) = f.split(b'=')
                        result[key.upper()] = value
                    return result

                try:
                    if line == b"?" or line == b"0":
                        self.writer.write(b"0\r\n")
                    elif line == b"0XZ":
                        self._send_nmea(b"WITXT,01,01,07,Start-up")
                    elif line == b"0XU":
                        self.writer.write(b"0XU,A=0,P=" + (b'N' if self._unpolled_task is not None else b'Q') +
                                          b",T=T,C=2,I=1,B=19200,D=8,S=1,L=10,N=WXT530,V=1.00\r\n")
                    elif line.startswith(b"0XU,"):
                        settings = split_fields(line[4:])
                        if settings.get(b"M") == b"N":
                            await self._start_unpolled()
                        elif settings.get(b"M"):
                            await self._stop_unpolled()
                        self.writer.write(line + b"\r\n")
                    elif line == b"0WU":
                        self.writer.write(b"0WU,R=11111111&11111111,I=1,A=1,G=3,U=M,D=0,N=T,F=4\r\n")
                    elif line.startswith(b"0XU,"):
                        split_fields(line[4:])
                        self.writer.write(line + b"\r\n")
                    elif line == b"0TU":
                        self.writer.write(b"0TU,R=11111111&11111111,I=1,P=H,T=C\r\n")
                    elif line.startswith(b"0TU,"):
                        split_fields(line[4:])
                        self.writer.write(line + b"\r\n")
                    elif line == b"0RU":
                        self.writer.write(b"0RU,R=11111111&11111111,I=1,P=H,T=C\r\n")
                    elif line.startswith(b"0RU,"):
                        split_fields(line[4:])
                        self.writer.write(line + b"\r\n")
                    elif line == b"0SU":
                        self.writer.write(b"0SU,R=11111111&11111111,I=1,P=H,T=C\r\n")
                    elif line.startswith(b"0SU,"):
                        split_fields(line[4:])
                        self.writer.write(line + b"\r\n")
                    elif line == b"0IU":
                        self.writer.write(b"0SU,R=11111111&11111111\r\n")
                    elif line.startswith(b"0IU,"):
                        split_fields(line[4:])
                        self.writer.write(line + b"\r\n")
                    else:
                        raise ValueError
                except (ValueError, IndexError):
                    pass
                await self.writer.drain()
        finally:
            await self._stop_unpolled()


if __name__ == '__main__':
    from forge.acquisition.serial.simulator import parse_arguments, run
    run(parse_arguments(), Simulator)
