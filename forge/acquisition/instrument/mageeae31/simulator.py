import typing
import asyncio
import time
from math import exp
from forge.acquisition.instrument.streaming import StreamingSimulator


class Simulator(StreamingSimulator):
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        super().__init__(reader, writer)

        self.data_Q = 1.0

        self.data_If = [float(i * 0.25 + 0.5) for i in range(7)]
        self.data_Ip = [float(i * 0.1 + 0.3) for i in range(7)]
        self.data_Ifz = [float(i * 0.025 + 0.05) for i in range(7)]
        self.data_Ipz = [float(i * 0.01 + 0.03) for i in range(7)]
        self.data_X = [float(i + 30) for i in range(7)]
        self.data_ATN = [float(i + 1.0) for i in range(7)]
        self.data_PCTbypass = 100.0

        self.serial_number: typing.Optional[int] = None
        self.compressed_output: bool = False
        self.unpolled_interval = 120.0

    @property
    def data_X1(self) -> float:
        return self.data_X[0]

    @property
    def data_If1(self) -> float:
        return self.data_If[0]

    @property
    def data_Ip1(self) -> float:
        return self.data_Ip[0]

    @property
    def data_Ifz1(self) -> float:
        return self.data_Ifz[0]

    @property
    def data_Ipz1(self) -> float:
        return self.data_Ipz[0]

    @property
    def data_ATN1(self) -> float:
        return self.data_ATN[0]

    @property
    def data_Ir(self) -> typing.List[float]:
        return [exp(v / -100.0) for v in self.data_ATN]

    @property
    def data_Ir1(self) -> float:
        return self.data_Ir[0]

    _MONTH_NAMES = {
        1: "jan",
        2: "feb",
        3: "mar",
        4: "apr",
        5: "may",
        6: "jun",
        7: "jul",
        8: "aug",
        9: "sep",
        10: "oct",
        11: "nov",
        12: "dec",
    }

    _COMPRESSION_RADIX = b"abcdefghijklmnopqrstuvwxyzBCDEFGHIJKLMNOPQRSTUVWXY"

    def _compress_decimal(self, value: int, digits: int) -> bytes:
        result = bytearray([self._COMPRESSION_RADIX[0]] * digits)
        for i in range(digits):
            result[(digits - 1) - i] = self._COMPRESSION_RADIX[value % len(self._COMPRESSION_RADIX)]
            value //= len(self._COMPRESSION_RADIX)
        return bytes(result)

    def _compressed_channel(self, widx: int) -> bytes:
        return (
            self._compress_decimal(round((self.data_ATN[widx] + 10.0) * 1000.0), 3) +
            self._compress_decimal(round(self.data_Ipz[widx] * 10000.0), 2) +
            self._compress_decimal(round(self.data_Ifz[widx] * 10000.0), 2) +
            self._compress_decimal(round((self.data_Ip[widx] + 1.0) * 10000.0), 3) +
            self._compress_decimal(round((self.data_If[widx] + 1.0) * 10000.0), 3) +
            self._compress_decimal(round((self.data_PCTbypass / 100.0) * 50.0 - 1.0), 1) +
            self._compress_decimal(round(self.data_Q * 10), 2)
        )

    def _emit_record(self) -> None:
        if self.serial_number is not None:
            self.writer.write(b"%d," % self.serial_number)

        ts = time.gmtime()
        self.writer.write((
            f"\"{ts.tm_mday:02}-{self._MONTH_NAMES[ts.tm_mon]}-{ts.tm_year % 100:02}\",\"{ts.tm_hour:02}:{ts.tm_min:02}\""
        ).encode('ascii'))
        for i in range(7):
            self.writer.write((
                f",{self.data_X[i]:6.2f}"
            ).encode('ascii'))

        if self.compressed_output:
            for i in range(7):
                self.writer.write(b",")
                self.writer.write(self._compressed_channel(i))
            self.writer.write(b"\r")
            return

        self.writer.write((
            f",{self.data_Q:.1f}"
        ).encode('ascii'))

        for i in range(7):
            self.writer.write((
                f",{self.data_Ipz[i]}"
                f",{self.data_Ip[i]}"
                f",{self.data_Ifz[i]}"
                f",{self.data_If[i]}"
                f",{self.data_PCTbypass / 100.0:.2f}"
                f",{self.data_ATN[i]:.3f}"
            ).encode('ascii'))

        self.writer.write(b"\r")

    async def run(self) -> typing.NoReturn:
        while True:
            self._emit_record()
            await asyncio.sleep(self.unpolled_interval)


if __name__ == '__main__':
    from forge.acquisition.serial.simulator import parse_arguments, run
    run(parse_arguments(), Simulator)
