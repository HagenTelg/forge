import typing
import asyncio
import struct
from forge.acquisition.instrument.modbus import ModbusSimulator, ModbusException, ModbusExceptionCode


class Simulator(ModbusSimulator):
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        super().__init__(reader, writer)

        self.data_XNO2 = 10.0
        self.data_XNO = 9.0
        self.data_XNOx = 12.0
        self.data_Tmanifold = 22.0
        self.data_Toven = 25.0
        self.data_Tbox = 23.0
        self.data_Psample = 800.0
        self.data_PCTmanifold = 40.0
        self.data_PCToven = 50.0
        self.data_Bax = 30.0

        self.flags = 0

    async def get_input_value(self, index: int) -> bool:
        if index > 12:
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
        if start_index == 12:
            return self._respond_float(index, self.data_XNO2)
        elif start_index == 18:
            return self._respond_float(index, self.data_Bax)
        elif start_index == 20:
            return self._respond_float(index, self.data_Toven)
        elif start_index == 22:
            return self._respond_float(index, self.data_PCToven)
        elif start_index == 24:
            return self._respond_float(index, self.data_Tmanifold)
        elif start_index == 26:
            return self._respond_float(index, self.data_PCTmanifold)
        elif start_index == 34:
            return self._respond_float(index, self.data_Psample / 33.86388158)
        elif start_index == 36:
            return self._respond_float(index, self.data_Tbox)
        elif start_index == 56:
            return self._respond_float(index, self.data_XNO)
        elif start_index == 76:
            return self._respond_float(index, self.data_XNOx)

        raise ModbusException(ModbusExceptionCode.ILLEGAL_DATA_ADDRESS)


if __name__ == '__main__':
    from forge.acquisition.serial.simulator import parse_arguments, run
    run(parse_arguments(), Simulator)
