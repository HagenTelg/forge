import typing
import asyncio
import struct
from forge.acquisition.instrument.modbus import ModbusSimulator, ModbusException, ModbusExceptionCode


class Simulator(ModbusSimulator):
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        super().__init__(reader, writer)

        self.data_X = 50.0
        self.data_Tsample = 21.0
        self.data_Tlamp = 22.0
        self.data_Psample = 800.0
        self.data_Ppump = 700.0
        self.data_Q = 1.5
        self.data_Ca = 5000.0
        self.data_Cag = 5.0
        self.data_Cb = 6000.0
        self.data_Cbg = 6.0
        self.data_Alamp = 12.0
        self.data_Aheater = 7.0

        self.alarms = [0] * 13

    @staticmethod
    def _respond_float(first: bool, value: float) -> bytes:
        raw = struct.pack('<f', value)
        if first:
            return bytes([raw[1], raw[0]])
        else:
            return bytes([raw[3], raw[2]])

    @staticmethod
    def _respond_string(offset: int, contents: str) -> bytes:
        raw = contents.encode('utf-8')
        slice_begin = offset*2
        result = raw[slice_begin:slice_begin+2]
        while len(result) < 2:
            result = result + bytes([0])
        return result

    async def get_holding_register(self, index: int) -> bytes:
        if index >= 515 and index <= 515+7:
            return self._respond_string(index-515, "12345")
        elif index >= 523 and index <= 523+16:
            return self._respond_string(index-523, "49iQ 1.6.10.33674")
        elif index >= 539 and index <= 539+8:
            return self._respond_string(index-539, "iQSeries")
        elif index >= 7555 and index <= 7555+3:
            return self._respond_string(index-7555, "ppb")
        elif index >= 1457 and index < 1457+len(self.alarms):
            index -= 1457
            return struct.pack('>H', self.alarms[index])

        if index == 2100 or index == 2101:
            return self._respond_float(index == 2100, self.data_X)
        elif index == 1413 or index == 1414:
            return self._respond_float(index == 1413, self.data_Q)
        elif index == 1441 or index == 1442:
            return self._respond_float(index == 1441, self.data_Psample / 1.333224)
        elif index == 1444 or index == 1445:
            return self._respond_float(index == 1444, self.data_Ppump / 1.333224)
        elif index == 1470 or index == 1471:
            return self._respond_float(index == 1470, self.data_Tsample)
        elif index == 1472 or index == 1473:
            return self._respond_float(index == 1472, self.data_Tlamp)
        elif index == 1474 or index == 1475:
            return self._respond_float(index == 1474, self.data_Alamp)
        elif index == 1451 or index == 1452:
            return self._respond_float(index == 1451, self.data_Aheater)
        elif index == 1453 or index == 1454:
            return self._respond_float(index == 1453, self.data_Ca)
        elif index == 1455 or index == 1456:
            return self._respond_float(index == 1455, self.data_Cb)
        elif index == 2108 or index == 2109:
            return self._respond_float(index == 2108, self.data_Cag)
        elif index == 2110 or index == 2111:
            return self._respond_float(index == 2110, self.data_Cbg)

        raise ModbusException(ModbusExceptionCode.ILLEGAL_DATA_ADDRESS)

    async def set_holding_register(self, index: int, value: bytes) -> None:
        set_value = struct.unpack('>H', value)[0]
        if index == 5207:
            if set_value == 0:
                return
            elif set_value == 3:
                return
        elif index == 5235:
            if set_value == 1:
                return
        elif index >= 5185 and index <= 5191:
            return
        return await super().set_holding_register(index, value)


if __name__ == '__main__':
    from forge.acquisition.serial.simulator import parse_arguments, run
    run(parse_arguments(), Simulator)
