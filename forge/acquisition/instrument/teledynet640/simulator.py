import typing
import asyncio
import struct
from forge.acquisition.instrument.modbus import ModbusSimulator, ModbusException, ModbusExceptionCode


class Simulator(ModbusSimulator):
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        super().__init__(reader, writer)

        self.data_X1 = 10.0
        self.data_X25 = 15.0
        self.data_X10 = 20.0
        self.data_Pambient = 880.0
        self.data_Tsample = 21.0
        self.data_Tambient = 18.0
        self.data_Tasc = 30.0
        self.data_Tled = 45.0
        self.data_Tbox = 40.0
        self.data_Usample = 25.0
        self.data_Qsample = 5.0
        self.data_Qbypass = 0.0
        self.data_spandev = 0.25
        self.data_PCTpump = 42.0
        self.data_PCTvalve = 5.0
        self.data_PCTasc = 6.0
        self.data_spandev = 0.5

        self.flags = 0

    async def get_input_value(self, index: int) -> bool:
        if index > 8:
            raise ModbusException(ModbusExceptionCode.ILLEGAL_DATA_ADDRESS)
        return (self.flags & (1 << index)) != 0

    @staticmethod
    def _respond_float(index: int, value: float) -> bytes:
        raw = struct.pack('>f', value)
        if index % 2 == 0:
            return raw[:2]
        else:
            return raw[2:]

    async def get_input_register(self, index: int) -> bytes:
        start_index = (index // 2) * 2
        if start_index == 6:
            return self._respond_float(index, self.data_X10)
        elif start_index == 8:
            return self._respond_float(index, self.data_X25)
        elif start_index == 32:
            return self._respond_float(index, self.data_Tled)
        elif start_index == 34:
            return self._respond_float(index, self.data_Pambient / 10.0)
        elif start_index == 36:
            return self._respond_float(index, self.data_Usample)
        elif start_index == 38:
            return self._respond_float(index, self.data_Tbox)
        elif start_index == 40:
            return self._respond_float(index, self.data_Tambient)
        elif start_index == 42:
            return self._respond_float(index, self.data_Tasc)
        elif start_index == 44:
            return self._respond_float(index, self.data_Tsample)
        elif start_index == 46:
            return self._respond_float(index, self.data_Qsample)
        elif start_index == 48:
            return self._respond_float(index, self.data_Qbypass)
        elif start_index == 56:
            return self._respond_float(index, self.data_PCTpump)
        elif start_index == 58:
            return self._respond_float(index, self.data_PCTvalve)
        elif start_index == 60:
            return self._respond_float(index, self.data_PCTasc)
        elif start_index == 64 and self.data_X1 is not None:
            return self._respond_float(index, self.data_X1)
        elif start_index == 86:
            return self._respond_float(index, self.data_spandev)

        raise ModbusException(ModbusExceptionCode.ILLEGAL_DATA_ADDRESS)


if __name__ == '__main__':
    from forge.acquisition.serial.simulator import parse_arguments, run
    run(parse_arguments(), Simulator)
