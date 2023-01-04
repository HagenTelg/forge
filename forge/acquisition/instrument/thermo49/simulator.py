import typing
import asyncio
import enum
from forge.acquisition.instrument.streaming import StreamingSimulator


class Simulator(StreamingSimulator):
    class InstrumentMode:
        MODE_49c = 0
        MODE_49i = 1
        MODE_49c_Legacy1 = 2
        MODE_49c_Legacy2 = 3

    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        super().__init__(reader, writer)

        self.mode = self.InstrumentMode.MODE_49c

        self.data_X = 123.0
        self.data_Qa = 1.5
        self.data_Qb = 2.5
        self.data_Ca = 100000
        self.data_Cb = 200000
        self.data_Psample = 800.0
        self.data_Tsample = 21.0
        self.data_Tlamp = 23.0
        self.data_Qozonator = 2.0
        self.data_Tozonator = 24.0
        self.data_Vlamp = 12.0
        self.data_Vozonator = 12.5

        self.flags = 0xC100000

        self.sum_delimiter: typing.Optional[bytes] = None

    def _response(self, data: typing.Union[str, bytes]) -> None:
        if isinstance(data, str):
            data = data.encode('ascii')
        self.writer.write(data)
        if not self.sum_delimiter:
            self.writer.write(b'\r')
            return

        s = sum(data)
        self.writer.write(self.sum_delimiter)
        self.writer.write(f"sum {s:04x}\r".encode('ascii'))

    async def run(self) -> typing.NoReturn:
        while True:
            line = await self.reader.readuntil(b'\r')
            line = line.strip()

            try:
                if line == b'set mode remote':
                    self._response(b'set mode remote ok')
                elif line == b'set sample':
                    self._response(b'set sample ok')
                elif line == b'set temp comp on':
                    self._response(b'set temp comp on ok')
                elif line == b'set pres comp on':
                    self._response(b'set pres comp on ok')
                elif line.startswith(b'set time '):
                    self._response(line + b' ok')
                elif line.startswith(b'set date '):
                    self._response(line + b' ok')
                elif line.startswith(b'set format '):
                    self._response(line + b' ok')
                elif line == b'o3 bkg':
                    self._response(b'o3 bkg 1.25E0 ppb')
                elif line == b'o3':
                    self._response(f"o3 {self.data_X:e} ppb")
                elif line == b'o3 coef':
                    if self.mode in (self.InstrumentMode.MODE_49c, self.InstrumentMode.MODE_49i):
                        self._response(b'o3 coef 1.0')
                elif line == b'bench temp':
                    self._response(f"bench temp {self.data_Tsample:.1f} dec C")
                elif line == b'lamp temp':
                    self._response(f"lamp temp {self.data_Tlamp:.1f} dec C")
                elif line == b'pres':
                    self._response(f"pres {self.data_Psample / 1.333224:.1f} mm Hg")
                elif line == b'cell a int':
                    self._response(f"cell a int {self.data_Ca:.0f} Hz")
                elif line == b'cell b int':
                    self._response(f"cell b int {self.data_Cb:.0f} Hz")
                elif line == b'flow a':
                    self._response(f"flow a {self.data_Qa:.1f} l/m")
                elif line == b'flow b':
                    self._response(f"flow b {self.data_Qb:.1f} l/m")
                elif line == b'flags':
                    self._response(f"flags {self.flags:08x}")
                elif line == b'oz flow' and self.mode == self.InstrumentMode.MODE_49c:
                    self._response(f"oz flow {self.data_Qozonator:.1f} l/m")
                elif line == b'o3 lamp temp' and self.mode == self.InstrumentMode.MODE_49i:
                    self._response(f"o3 lamp temp {self.data_Tozonator:.1f} deg C")
                elif line == b'lamp voltage bench' and self.mode == self.InstrumentMode.MODE_49i:
                    self._response(f"lamp voltage bench {self.data_Vlamp:.1f} V")
                elif line == b'lamp voltage oz' and self.mode == self.InstrumentMode.MODE_49i:
                    self._response(f"lamp voltage oz {self.data_Vozonator:.1f} V")
                elif line == b'host name' and self.mode == self.InstrumentMode.MODE_49i:
                    self._response(b'host name ISERIES')
                elif line == b'l1' and self.mode == self.InstrumentMode.MODE_49i:
                    self._response(b'l1 10 %')
                elif line == b'program no':
                    if self.mode == self.InstrumentMode.MODE_49c:
                        self._response(b'program no processor 49 00000100 link 49L 00000100')
                    elif self.mode == self.InstrumentMode.MODE_49i:
                        self._response(b'program no iSeries 49i 01.00.01.074')
                    elif self.mode == self.InstrumentMode.MODE_49c_Legacy2:
                        self._response(b'program no processor 49 000009 00 clink 49L000009 00')
                    else:
                        pass
                elif line.startswith(b'set avg time '):
                    if self.mode != self.InstrumentMode.MODE_49c_Legacy1:
                        self._response(line + b' ok')
                else:
                    raise ValueError
            except (ValueError, IndexError):
                self.writer.write(line + b' bad cmd\r')


if __name__ == '__main__':
    from forge.acquisition.serial.simulator import parse_arguments, run
    run(parse_arguments(), Simulator)
