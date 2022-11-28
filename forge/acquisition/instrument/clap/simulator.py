import typing
import asyncio
import time
import struct
import enum
from math import nan
from forge.acquisition.instrument.streaming import StreamingSimulator
from forge.units import flow_lpm_to_m3s


class Simulator(StreamingSimulator):
    class _Menu:
        MAIN = "main"
        CFG = "cfg"
        CAL = "cal"

    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        super().__init__(reader, writer)

        self._unpolled_task: typing.Optional[asyncio.Task] = None

        self.data_elapsed_seconds: int = 0
        self.data_Ff: int = 2
        self.data_Fn: int = 1
        self.data_Q: float = 1.0
        self.data_Qt: float = 0
        self.data_Tcase = 21.0
        self.data_Tsample = 22.0
        self.data_ID: typing.List[float] = [float(i + 100) for i in range(10)]
        self.data_IB: typing.List[float] = [float(i*100 + 101000) for i in range(10)]
        self.data_IG: typing.List[float] = [float(i*100 + 102000) for i in range(10)]
        self.data_IR: typing.List[float] = [float(i*100 + 103000) for i in range(10)]

        self.delta_IB: typing.List[float] = [0.0] * 10
        self.delta_IG: typing.List[float] = [0.0] * 10
        self.delta_IR: typing.List[float] = [0.0] * 10

        self.is_changing = False
        self.flags = 0

        self.flow_calibration = [0.25, 0.5, 0.75, 1.0]

    @property
    def data_Ip(self) -> typing.List[float]:
        if self.data_Fn == 0:
            return [nan, nan, nan]
        return [self.data_IB[self.data_Fn], self.data_IG[self.data_Fn], self.data_IR[self.data_Fn]]
    
    @property
    def data_IpB(self) -> float:
        return self.data_Ip[0]
    
    @property
    def data_IpG(self) -> float:
        return self.data_Ip[1]
    
    @property
    def data_IpR(self) -> float:
        return self.data_Ip[2]

    @property
    def data_If(self) -> typing.List[float]:
        if self.data_Fn == 0:
            return [nan, nan, nan]
        if (self.data_Fn % 2) == 0:
            detector = 0
        else:
            detector = 9
        return [self.data_IB[detector], self.data_IG[detector], self.data_IR[detector]]

    @property
    def data_IfB(self) -> float:
        return self.data_If[0]

    @property
    def data_IfG(self) -> float:
        return self.data_If[1]

    @property
    def data_IfR(self) -> float:
        return self.data_If[2]

    @property
    def data_In(self) -> typing.List[float]:
        Ip = self.data_Ip
        If = self.data_If
        return [Ip[i] / If[i] for i in range(3)]

    def data_Ir(self, In0: typing.List[float]) -> typing.List[float]:
        In = self.data_In
        return [In[i] / In0[i] for i in range(3)]

    @staticmethod
    def _encode_float(v: float) -> bytes:
        raw = struct.pack('<f', v)
        return raw.hex().encode('ascii')

    async def _unpolled(self) -> typing.NoReturn:
        while True:
            effective_flags = self.flags
            if self.is_changing:
                effective_flags |= 0x0001
            self.writer.write((
                f"03, "
                f"{effective_flags:04X}, "
                f"{self.data_elapsed_seconds:08X}, "
                f"{self.data_Ff:04X}, "
                f"{self.data_Fn:02X}, "
                f"{self.data_Q:.3f}, "                
                f"{self.data_Qt:.3f}, "
                f"{self.data_Tcase:.2f}, "                
                f"{self.data_Tsample:.2f}"
            ).encode('ascii'))
            for ch in range(10):
                self.writer.write(b", ")
                self.writer.write(self._encode_float(self.data_ID[ch]))
                self.writer.write(b", ")
                self.writer.write(self._encode_float(self.data_IR[ch]))
                self.writer.write(b", ")
                self.writer.write(self._encode_float(self.data_IG[ch]))
                self.writer.write(b", ")
                self.writer.write(self._encode_float(self.data_IB[ch]))
            self.writer.write(b"\r")

            if self.data_Fn != 0:
                self.data_elapsed_seconds += 1
                self.data_Qt += flow_lpm_to_m3s(self.data_Q)
            for ch in range(10):
                self.data_IB[ch] += self.delta_IB[ch]
                self.data_IG[ch] += self.delta_IG[ch]
                self.data_IR[ch] += self.delta_IR[ch]

            await asyncio.sleep(1.0)

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

    def start_filter_change(self) -> None:
        self.is_changing = True
        self.data_Fn = 0
        self.data_Qt = 0.0
        self.data_elapsed_seconds = 0

    def stop_filter_change(self) -> None:
        self.is_changing = False
        self.data_Ff += 1

    async def run(self) -> typing.NoReturn:
        active_menu = self._Menu.MAIN
        try:
            await self._start_unpolled()
            while True:
                line = await self.reader.readuntil(b'\r')
                line = line.strip()

                try:
                    if active_menu == self._Menu.MAIN:
                        if line == b'main':
                            self.writer.write(b'main\r')
                        elif line == b'hide':
                            await self._stop_unpolled()
                            self.writer.write(b'unpolled reports disabled\r')
                        elif line == b'show':
                            await self._start_unpolled()
                            self.writer.write(b'unpolled reports enabled\r')
                        elif line == b'cal':
                            self.writer.write(b'calibration menu\r')
                            active_menu = self._Menu.CAL
                        elif line == b'cfg':
                            self.writer.write(b'configuration menu\r')
                            active_menu = self._Menu.CFG
                        elif line == b'go':
                            self.stop_filter_change()
                            self.writer.write(b'filter change end\r')
                        elif line == b'stop':
                            self.start_filter_change()
                            self.writer.write(b'filter change start\r')
                        elif line.startswith(b'spot='):
                            self.data_Fn = int(line[5:])
                            self.writer.write(b'spot = %d\r' % self.data_Fn)
                        else:
                            raise ValueError
                    elif active_menu == self._Menu.CFG:
                        if line == b'main':
                            self.writer.write(b'main menu\r')
                            active_menu = self._Menu.MAIN
                        elif line == b'sn':
                            self.writer.write(b'sn = 10.005\r')
                        elif line == b'fw':
                            self.writer.write(b'fw = 10.114\r')
                        else:
                            raise ValueError
                    elif active_menu == self._Menu.CAL:
                        if line == b'main':
                            self.writer.write(b'main menu\r')
                            active_menu = self._Menu.MAIN
                        elif line == b'flow':
                            self.writer.write(b'flow = ')
                            self.writer.write(b', '.join([(b'%e' % v) for v in self.flow_calibration]))
                            self.writer.write(b'\r')
                        elif line.startswith(b'flow='):
                            constants = line[5:].split(b',')
                            if len(constants) != 4:
                                raise ValueError
                            self.flow_calibration = [float(v) for v in constants]
                        else:
                            raise ValueError
                    else:
                        raise ValueError
                except (ValueError, IndexError):
                    self.writer.write(b'?\r')
                await self.writer.drain()
        finally:
            await self._stop_unpolled()


if __name__ == '__main__':
    from forge.acquisition.serial.simulator import parse_arguments, run
    run(parse_arguments(), Simulator)
