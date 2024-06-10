import typing
import asyncio
import logging
import enum
import struct
import time
from math import isfinite
from forge.tasks import wait_cancelable
from forge.units import pressure_mmHg_to_hPa
from ..modbus import ModbusInstrument, StreamingContext, CommunicationsError
from ..parse import parse_flags_mapped

_LOGGER = logging.getLogger(__name__)
_INSTRUMENT_TYPE = __name__.split('.')[-2]


class Instrument(ModbusInstrument):
    INSTRUMENT_TYPE = _INSTRUMENT_TYPE
    MANUFACTURER = "Thermo"
    MODEL = "49iQ"
    DISPLAY_LETTER = "Z"
    TAGS = frozenset({"ozone", _INSTRUMENT_TYPE})
    SERIAL_PORT = {'baudrate': 9600}

    class _Register(enum.IntEnum):
        # string
        SERIAL_NUMBER = 515  # 7 long
        FIRMWARE_VERSION = 523  # 16 long
        HOST_NAME = 539  # 8 long
        GAS_UNITS = 7555  # 3 long

        # 16-bit integer
        SET_CLOCK_DATA = 5185  # 6 registers set simultaneously: hour, minute, second, month, day, year
        SET_CLOCK_SOURCE = 5207  # 3 to begin, 0 to end
        SET_CLOCK_COMMIT = 5235  # set to 1 after data
        ALARM_INTENSITY_A_HIGH = 1457
        ALARM_INTENSITY_B_HIGH = 1458
        LAMP_TEMPERATURE_SHORT = 1459
        LAMP_TEMPERATURE_OPEN = 1460
        SAMPLE_TEMPERATURE_SHORT = 1461
        SAMPLE_TEMPERATURE_OPEN = 1462
        LAMP_CONNECTION_ALARM = 1463
        LAMP_SHORT = 1464
        COMMUNICATIONS_ALARM = 1465
        POWER_SUPPLY_ALARM = 1466
        LAMP_CURRENT_ALARM = 1467
        LAMP_TEMPERATURE_ALARM = 1468
        SAMPLE_TEMPERATURE_ALARM = 1469

        # float
        CONCENTRATION = 2100
        FLOW_A = 1413
        # FLOW_B = 2134  # returns garbage
        PHOTOMETER_PRESSURE_A = 1441
        # PHOTOMETER_PRESSURE_B = 1447  # returns garbage
        PUMP_PRESSURE = 1444
        SAMPLE_TEMPERATURE = 1470
        LAMP_TEMPERATURE = 1472
        LAMP_CURRENT = 1474
        LAMP_HEATER_CURRENT = 1451
        COUNTS_A = 1453
        COUNTS_A_NOISE = 2108
        COUNTS_B = 1455
        COUNTS_B_NOISE = 2110

    def __init__(self, context: StreamingContext):
        super().__init__(context)

        self._report_interval: float = float(context.config.get('REPORT_INTERVAL', default=1.0))

        self._sleep_time: float = 0.0

        self.register_map: typing.Dict[Instrument._Register, Instrument.Input] = {
            self._Register.CONCENTRATION: self.input("X"),
            self._Register.SAMPLE_TEMPERATURE: self.input("Tsample"),
            self._Register.LAMP_TEMPERATURE: self.input("Tlamp"),
            self._Register.PHOTOMETER_PRESSURE_A: self.input("Psample"),
            self._Register.PUMP_PRESSURE: self.input("Ppump"),
            self._Register.FLOW_A: self.input("Q"),
            self._Register.COUNTS_A: self.input("Ca"),
            self._Register.COUNTS_A_NOISE: self.input("Cag"),
            self._Register.COUNTS_B: self.input("Cb"),
            self._Register.COUNTS_B_NOISE: self.input("Cbg"),
            self._Register.LAMP_CURRENT: self.input("Alamp"),
            self._Register.LAMP_HEATER_CURRENT: self.input("Aheater"),
        }
        self.register_notification: typing.Dict[Instrument._Register, Instrument.Notification] = {
            self._Register.ALARM_INTENSITY_A_HIGH: self.notification("alarm_intensity_a_high"),
            self._Register.ALARM_INTENSITY_B_HIGH: self.notification("alarm_intensity_b_high"),
            self._Register.LAMP_TEMPERATURE_SHORT: self.notification("lamp_temperature_short", is_warning=True),
            self._Register.LAMP_TEMPERATURE_OPEN: self.notification("lamp_temperature_open", is_warning=True),
            self._Register.SAMPLE_TEMPERATURE_SHORT: self.notification("sample_temperature_short", is_warning=True),
            self._Register.SAMPLE_TEMPERATURE_OPEN: self.notification("sample_temperature_open", is_warning=True),
            self._Register.LAMP_CONNECTION_ALARM: self.notification("lamp_connection_alarm", is_warning=True),
            self._Register.LAMP_SHORT: self.notification("lamp_short", is_warning=True),
            self._Register.COMMUNICATIONS_ALARM: self.notification("communications_alarm", is_warning=True),
            self._Register.POWER_SUPPLY_ALARM: self.notification("power_supply_alarm", is_warning=True),
            self._Register.LAMP_CURRENT_ALARM: self.notification("lamp_current_alarm", is_warning=True),
            self._Register.LAMP_TEMPERATURE_ALARM: self.notification("lamp_temperature_alarm"),
            self._Register.SAMPLE_TEMPERATURE_ALARM: self.notification("sample_temperature_alarm"),
        }

        self.instrument_report = self.report(
            self.variable_ozone(self.register_map[self._Register.CONCENTRATION], code="X",),
            self.variable_air_pressure(self.register_map[self._Register.PHOTOMETER_PRESSURE_A],
                                       "sample_pressure", code="P1",
                                       attributes={'long_name': "photometer A pressure"}),
            self.variable_air_temperature(self.register_map[self._Register.SAMPLE_TEMPERATURE],
                                          "sample_temperature", code="T1",
                                          attributes={'long_name': "sample bench temperature"}),
            self.variable_temperature(self.register_map[self._Register.LAMP_TEMPERATURE],
                                      "lamp_temperature", code="T2",
                                      attributes={'long_name': "lamp temperature"}),
            self.variable_pressure(self.register_map[self._Register.PUMP_PRESSURE],
                                   "pump_pressure", code="P2",
                                   attributes={'long_name': "pump pressure"}),
            self.variable_sample_flow(self.register_map[self._Register.FLOW_A],
                                      "cell_a_flow", code="Q",
                                      attributes={'long_name': "air flow rate through cell A"}),

            self.variable(self.register_map[self._Register.COUNTS_A],
                          "cell_a_count_rate", code="C1", attributes={
                'long_name': "cell A intensity count rate",
                'units': "Hz",
                'C_format': "%7.0f"
            }),
            self.variable(self.register_map[self._Register.COUNTS_B],
                          "cell_b_count_rate", code="C2", attributes={
                'long_name': "cell B intensity count rate",
                'units': "Hz",
                'C_format': "%7.0f"
            }),

            self.variable(self.register_map[self._Register.LAMP_CURRENT],
                          "lamp_current", code="VA1", attributes={
                'long_name': "lamp current",
                'units': "mA",
                'C_format': "%5.2f"
            }),
            self.variable(self.register_map[self._Register.LAMP_HEATER_CURRENT],
                          "lamp_heater_current", code="VA2", attributes={
                'long_name': "lamp heater current",
                'units': "A",
                'C_format': "%5.2f"
            }),

            flags=[self.flag(n) for n in self.register_notification.values()],
        )

    async def _read_float_input(self, reg: "Instrument._Register") -> float:
        # Floats are sent as a little endian IEEE 754 float, but with each half sent as a big endian
        # U16 (this was probably the result of a little endian machine casting the float into two
        # U16s then sending it through a generic encoder that byte swaps them).
        index = int(reg)
        raw: bytes = await wait_cancelable(self.read_holding_registers(index, index+1), 10.0)
        decoded = bytes([raw[1], raw[0], raw[3], raw[2]])
        return struct.unpack('<f', decoded)[0]

    async def _read_string(self, reg: "Instrument._Register", count: int) -> str:
        index = int(reg)
        raw: bytes = await wait_cancelable(self.read_holding_registers(index, index + count - 1), 10.0)
        try:
            final_byte = raw.index(b'\x00')
            raw = raw[:final_byte]
        except ValueError:
            pass
        try:
            return raw.decode('utf-8')
        except UnicodeDecodeError:
            return raw.decode('ascii')

    async def start_communications(self) -> None:
        if not self.writer:
            raise CommunicationsError

        await wait_cancelable(self.read_mapped_holding_integers(self.register_notification.keys()), 10.0)

        serial_number = await self._read_string(self._Register.SERIAL_NUMBER, 7)
        self.set_serial_number(serial_number)

        firmware_version = await self._read_string(self._Register.FIRMWARE_VERSION, 16)
        parts = firmware_version.split(' ', 1)
        if len(parts) == 2 and "iQ" in parts[0]:
            self.set_instrument_info('model', parts[0])
            self.set_firmware_version(parts[1])
        else:
            self.set_firmware_version(serial_number)

        host_name = await self._read_string(self._Register.SERIAL_NUMBER, 8)
        # self.set_instrument_info('host_name', host_name)

        gas_units = await self._read_string(self._Register.GAS_UNITS, 3)
        if gas_units != "ppb":
            index = int(self._Register.GAS_UNITS)
            await wait_cancelable(self.write_registers(index, index+2, b"ppb"), 10.0)

        await wait_cancelable(self.write_integer_register(self._Register.SET_CLOCK_SOURCE, 3), 10.0)
        ts = time.gmtime()
        await wait_cancelable(self.write_registers(
            int(self._Register.SET_CLOCK_DATA), int(self._Register.SET_CLOCK_DATA)+5,
            struct.pack('>HHHHHH', ts.tm_hour, ts.tm_min, ts.tm_sec, ts.tm_mon, ts.tm_mday, ts.tm_year)
        ), 10.0)
        await wait_cancelable(self.write_integer_register(self._Register.SET_CLOCK_COMMIT, 1), 10.0)
        await wait_cancelable(self.write_integer_register(self._Register.SET_CLOCK_SOURCE, 0), 10.0)

        concentration = await self._read_float_input(self._Register.CONCENTRATION)
        if not isfinite(concentration) or concentration < -100.0:
            raise CommunicationsError(f"invalid concentration {concentration}")

        self._sleep_time = 0.0

    async def communicate(self) -> None:
        if self._sleep_time > 0.0:
            await asyncio.sleep(self._sleep_time)
            self._sleep_time = 0.0
        begin_read = time.monotonic()

        flags = await wait_cancelable(self.read_mapped_holding_integers(self.register_notification.keys()), 10.0)
        flags = {r: bool(v) for r, v in flags.items()}
        parse_flags_mapped(flags, self.register_notification)

        for reg, inp in self.register_map.items():
            value = await self._read_float_input(reg)
            if reg in (self._Register.PHOTOMETER_PRESSURE_A, self._Register.PUMP_PRESSURE):
                value = pressure_mmHg_to_hPa(value)
            inp(value)

        self.instrument_report()

        end_read = time.monotonic()
        self._sleep_time = self._report_interval - (end_read - begin_read)
