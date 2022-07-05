import typing
import asyncio
import logging
import enum
import struct
import time
from math import isfinite
from forge.tasks import wait_cancelable
from ..modbus import ModbusInstrument, StreamingContext, CommunicationsError, ModbusException
from ..parse import parse_flags_mapped

_LOGGER = logging.getLogger(__name__)
_INSTRUMENT_TYPE = __name__.split('.')[-2]


class Instrument(ModbusInstrument):
    INSTRUMENT_TYPE = _INSTRUMENT_TYPE
    MANUFACTURER = "Teledyne"
    MODEL = "T640"
    DISPLAY_LETTER = "M"
    TAGS = frozenset({"aerosol", "mass", _INSTRUMENT_TYPE})

    class _Register(enum.IntEnum):
        # 32-bit integers, but read nonsense
        # PUMP_TACHOMETER = 0
        # AMPLITUDE_HISTOGRAM_COUNT = 2
        # LENGTH_DISTRIBUTION_COUNT = 4

        # float
        PM10_CONCENTRATION = 6
        PM25_CONCENTRATION = 8
        PM1_CONCENTRATION = 64
        RH = 36
        SAMPLE_TEMPERATURE = 44
        AMBIENT_TEMPERATURE = 40
        ASC_TUBE_TEMPERATURE = 42
        LED_TEMPERATURE = 32
        BOX_TEMPERATURE = 38
        AMBIENT_PRESSURE = 34
        SAMPLE_FLOW = 46
        BYPASS_FLOW = 48
        SPAN_DEVIATION = 86
        PUMP_DUTY_CYCLE = 56
        PROPORTIONAL_VALVE_DUTY_CYCLE = 58
        ASC_HEATER_DUTY_CYCLE = 60

    def __init__(self, context: StreamingContext):
        super().__init__(context)

        self._report_interval: float = float(context.config.get('REPORT_INTERVAL', default=1.0))
        self._have_pm1_option: typing.Optional[bool] = context.config.get('PM1')

        self._sleep_time: float = 0.0

        self.data_X1 = self.input("X1")
        self.data_X25 = self.input("X25")
        self.data_X10 = self.input("X10")
        self.data_Pambient = self.input("Pambient")
        self.register_map: typing.Dict[Instrument._Register, Instrument.Input] = {
            self._Register.AMBIENT_TEMPERATURE: self.input("Tambient"),
            self._Register.SAMPLE_TEMPERATURE: self.input("Tsample"),
            self._Register.ASC_TUBE_TEMPERATURE: self.input("Tasc"),
            self._Register.LED_TEMPERATURE: self.input("Tled"),
            self._Register.BOX_TEMPERATURE: self.input("Tbox"),
            self._Register.RH: self.input("Usample"),
            self._Register.SAMPLE_FLOW: self.input("Qsample"),
            self._Register.BYPASS_FLOW: self.input("Qbypass"),
            self._Register.SPAN_DEVIATION: self.input("spandev"),
            self._Register.PUMP_DUTY_CYCLE: self.input("PCTpump"),
            self._Register.PROPORTIONAL_VALVE_DUTY_CYCLE: self.input("PCTvalve"),
            self._Register.ASC_HEATER_DUTY_CYCLE: self.input("PCTasc"),
        }

        self.data_diameter = self.persistent("diameter", save_value=False, send_to_bus=False)
        if self._have_pm1_option is not None:
            if self._have_pm1_option:
                self.data_diameter([1.0, 2.5, 10.0])
            else:
                self.data_diameter([2.5, 10.0])
        dimension_diameter = self.dimension_size_distribution_diameter(self.data_diameter, attributes={
            'long_name': "upper particle diameter threshold",
            'units': 'um',
            'C_format': "%4.1f"
        })

        self.data_X = self.input_array("X", send_to_bus=False)
        if self.data_X.field.use_cut_size is None:
            self.data_X.field.use_cut_size = False

        self.input_flags: typing.Dict[int, Instrument.Notification] = dict()
        self.instrument_report = self.report(
            self.variable_array(self.data_X, dimension_diameter, "mass_concentration", code="X", attributes={
                'long_name': "mass concentration of particles derived from Lorenz-Mie calculation of OPC scattering",
                'units': "ug m-3",
                'C_format': "%7.2f"
            }),

            self.variable_air_pressure(self.data_Pambient, "pressure", code="P"),

            self.variable_sample_flow(self.register_map[self._Register.SAMPLE_FLOW],
                                      code="Q1", attributes={'C_format': "%4.2f"}),
            self.variable_flow(self.register_map[self._Register.BYPASS_FLOW],
                               "bypass_flow", code="Q2", attributes={'long_name': "inlet stack bypass flow"}),

            self.variable_air_rh(self.register_map[self._Register.RH],
                                 "sample_humidity", code="U1"),

            self.variable_air_temperature(self.register_map[self._Register.SAMPLE_TEMPERATURE],
                                          "sample_temperature", code="T1"),
            self.variable_temperature(self.register_map[self._Register.AMBIENT_TEMPERATURE],
                                      "ambient_temperature", code="T2",
                                      attributes={'long_name': "ambient temperature measured by the external sensor"}),
            self.variable_temperature(self.register_map[self._Register.ASC_TUBE_TEMPERATURE],
                                      "asc_temperature", code="T3",
                                      attributes={'long_name': "aerosol sample conditioner tube jacket temperature"}),
            self.variable_temperature(self.register_map[self._Register.LED_TEMPERATURE],
                                      "led_temperature", code="T4",
                                      attributes={'long_name': "OPC LED temperature"}),
            self.variable_temperature(self.register_map[self._Register.BOX_TEMPERATURE],
                                      "box_temperature", code="T5",
                                      attributes={'long_name': "internal box temperature"}),

            self.variable(self.register_map[self._Register.SPAN_DEVIATION],
                          "span_deviation", code="ZSPAN", attributes={
                'long_name': "span deviation",
                'C_format': "%6.1f"
            }),

            flags=[
                self.flag_input(self.input_flags, 0, "box_temperature_out_of_range", is_warning=True),
                self.flag_input(self.input_flags, 1, "sample_flow_out_of_range", is_warning=True),
                self.flag_input(self.input_flags, 2, "internal_serial_timeout", is_warning=True),
                self.flag_input(self.input_flags, 3, "system_reset_warning"),
                self.flag_input(self.input_flags, 5, "sample_temperature_out_of_range", is_warning=True),
                self.flag_input(self.input_flags, 6, "bypass_flow_out_of_range", is_warning=True),
                self.flag_input(self.input_flags, 7, "system_fault_warning", is_warning=True),
            ],
            auxiliary_variables=[
                self.variable(self.data_X1),
                self.variable(self.data_X25),
                self.variable(self.data_X10),
            ],
        )

    async def _read_float_input(self, reg: "Instrument._Register") -> float:
        index = int(reg)
        raw = await wait_cancelable(self.read_input_registers(index, index+1), 10.0)
        return struct.unpack('>f', raw)[0]

    async def start_communications(self) -> None:
        if not self.writer:
            raise CommunicationsError

        await wait_cancelable(self.read_mapped_inputs(self.input_flags.keys()), 10.0)

        pump_duty = await self._read_float_input(self._Register.PUMP_DUTY_CYCLE)
        if not isfinite(pump_duty) or pump_duty < 0.0 or pump_duty > 100.0:
            raise CommunicationsError(f"invalid pump duty cycle {pump_duty}")

        if self._have_pm1_option is None:
            try:
                pm1 = await self._read_float_input(self._Register.PM1_CONCENTRATION)
                if not isfinite(pm1) or pm1 < -20.0 or pm1 > 1E5:
                    raise ModbusException(0)
                self._have_pm1_option = True
                self.data_diameter([1.0, 2.5, 10.0])
                _LOGGER.debug("PM1 option present")
            except (ModbusException, asyncio.TimeoutError):
                self._have_pm1_option = False
                self.data_diameter([2.5, 10.0])
                _LOGGER.debug("PM1 option absent")

        pm10 = await self._read_float_input(self._Register.PM10_CONCENTRATION)
        if not isfinite(pm10) or pm10 < -100.0:
            raise CommunicationsError(f"invalid PM10 concentration {pm10}")

        self._sleep_time = 0.0

    async def communicate(self) -> None:
        if self._sleep_time > 0.0:
            await asyncio.sleep(self._sleep_time)
            self._sleep_time = 0.0
        begin_read = time.monotonic()

        flags = await wait_cancelable(self.read_mapped_inputs(self.input_flags.keys()), 10.0)
        parse_flags_mapped(flags, self.input_flags)

        for reg, inp in self.register_map.items():
            inp(await self._read_float_input(reg))

        self.data_Pambient(await self._read_float_input(self._Register.AMBIENT_PRESSURE) * 10.0)

        self.data_X10(await self._read_float_input(self._Register.PM10_CONCENTRATION))
        self.data_X25(await self._read_float_input(self._Register.PM25_CONCENTRATION))
        if self._have_pm1_option:
            self.data_X1(await self._read_float_input(self._Register.PM1_CONCENTRATION))
            self.data_X([float(self.data_X1), float(self.data_X25), float(self.data_X10)])
        else:
            self.data_X([float(self.data_X25), float(self.data_X10)])

        self.instrument_report()

        end_read = time.monotonic()
        self._sleep_time = self._report_interval - (end_read - begin_read)
