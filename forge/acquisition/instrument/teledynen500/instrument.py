import typing
import asyncio
import logging
import enum
import struct
import time
from math import isfinite
from forge.tasks import wait_cancelable
from forge.units import pressure_inHg_to_hPa
from ..modbus import ModbusInstrument, StreamingContext, CommunicationsError, ModbusException
from ..parse import parse_flags_mapped

_LOGGER = logging.getLogger(__name__)
_INSTRUMENT_TYPE = __name__.split('.')[-2]


class Instrument(ModbusInstrument):
    INSTRUMENT_TYPE = _INSTRUMENT_TYPE
    MANUFACTURER = "Teledyne"
    MODEL = "N500"
    DISPLAY_LETTER = "G"
    TAGS = frozenset({"ozone", _INSTRUMENT_TYPE})

    class _Register(enum.IntEnum):
        NO2_CONCENTRATION = 12
        NO_CONCENTRATION = 56
        NOx_CONCENTRATION = 76
        MANIFOLD_TEMPERATURE = 24
        OVEN_TEMPERATURE = 20
        BOX_TEMPERATURE = 36
        SAMPLE_PRESSURE = 34
        MANIFOLD_DUTY_CYCLE = 26
        OVEN_DUTY_CYCLE = 22
        REALTIME_LOSS = 18

    def __init__(self, context: StreamingContext):
        super().__init__(context)

        self._report_interval: float = float(context.config.get('REPORT_INTERVAL', default=1.0))

        self._sleep_time: float = 0.0

        self.data_Psample = self.input("Psample")
        self.register_map: typing.Dict[Instrument._Register, Instrument.Input] = {
            self._Register.NO2_CONCENTRATION: self.input("XNO2"),
            self._Register.NO_CONCENTRATION: self.input("XNO"),
            self._Register.NOx_CONCENTRATION: self.input("XNOx"),
            self._Register.MANIFOLD_TEMPERATURE: self.input("Tmanifold"),
            self._Register.OVEN_TEMPERATURE: self.input("Toven"),
            self._Register.BOX_TEMPERATURE: self.input("Tbox"),
            self._Register.MANIFOLD_DUTY_CYCLE: self.input("PCTmanifold"),
            self._Register.OVEN_DUTY_CYCLE: self.input("PCToven"),
            self._Register.REALTIME_LOSS: self.input("Bax"),
        }

        self.input_flags: typing.Dict[int, Instrument.Notification] = dict()
        self.notify_auto_calibration_failed = self.notification("auto_calibration_sequence_failed")
        self.instrument_report = self.report(
            self.variable_no2(self.register_map[self._Register.NO2_CONCENTRATION], code="X1"),
            self.variable_no(self.register_map[self._Register.NO_CONCENTRATION], code="X2"),
            self.variable_nox(self.register_map[self._Register.NOx_CONCENTRATION], code="X3"),

            self.variable_air_pressure(self.data_Psample, "pressure", code="P"),

            self.variable_temperature(self.register_map[self._Register.MANIFOLD_TEMPERATURE],
                                      "manifold_temperature", code="T1",
                                      attributes={'long_name': "internal manifold temperature"}),
            self.variable_temperature(self.register_map[self._Register.OVEN_TEMPERATURE],
                                      "oven_temperature", code="T2",
                                      attributes={'long_name': "optical assembly oven temperature"}),
            self.variable_temperature(self.register_map[self._Register.BOX_TEMPERATURE],
                                      "box_temperature", code="T3",
                                      attributes={'long_name': "instrument internal box temperature"}),

            flags=[
                self.flag_input(self.input_flags, 0, "caps_board_communication_error", is_warning=True),
                self.flag_input(self.input_flags, 1, "cell_pressure_out_of_range", is_warning=True),
                self.flag_input(self.input_flags, 2, "reference_out_of_range", is_warning=True),
                self.flag_input(self.input_flags, 3, "ozone_pressure_out_of_range", is_warning=True),
                self.flag_input(self.input_flags, 4, "ozone_tower_communications_error", is_warning=True),
                self.flag_input(self.input_flags, 5, "system_reset_warning"),
                self.flag_input(self.input_flags, 6, "sample_flow_out_of_range", is_warning=True),
                self.flag_input(self.input_flags, 7, "sample_pressure_out_of_range", is_warning=True),
                self.flag_input(self.input_flags, 8, "sample_temperature_out_of_range"),
                self.flag(self.notify_auto_calibration_failed, preferred_bit=(1 << 10)),
            ],
        )
        self.all_input_flags = list(self.input_flags.keys()) + [10, 11, 12]

    async def _read_float_input(self, reg: "Instrument._Register") -> float:
        index = int(reg)
        raw = await wait_cancelable(self.read_input_registers(index, index+1), 10.0)
        return struct.unpack('>f', raw)[0]

    async def start_communications(self) -> None:
        if not self.writer:
            raise CommunicationsError

        await wait_cancelable(self.read_mapped_inputs(self.all_input_flags), 10.0)

        manifold_duty = await self._read_float_input(self._Register.MANIFOLD_DUTY_CYCLE)
        if not isfinite(manifold_duty) or manifold_duty < 0.0 or manifold_duty > 100.0:
            raise CommunicationsError(f"invalid manifold duty cycle {manifold_duty}")

        no2 = await self._read_float_input(self._Register.NO2_CONCENTRATION)
        if not isfinite(no2) or no2 < -100.0 or no2 > 1E5:
            raise CommunicationsError(f"invalid NO2 concentration {no2}")

        self._sleep_time = 0.0

    async def communicate(self) -> None:
        if self._sleep_time > 0.0:
            await asyncio.sleep(self._sleep_time)
            self._sleep_time = 0.0
        begin_read = time.monotonic()

        flags = await wait_cancelable(self.read_mapped_inputs(self.all_input_flags), 10.0)
        parse_flags_mapped(flags, self.input_flags)
        self.notify_auto_calibration_failed(bool(flags.get(10) or flags.get(11) or flags.get(12)))

        for reg, inp in self.register_map.items():
            inp(await self._read_float_input(reg))

        self.data_Psample(pressure_inHg_to_hPa(await self._read_float_input(self._Register.SAMPLE_PRESSURE)))

        self.instrument_report()

        end_read = time.monotonic()
        self._sleep_time = self._report_interval - (end_read - begin_read)
