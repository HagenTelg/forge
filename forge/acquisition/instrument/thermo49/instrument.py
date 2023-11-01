import typing
import asyncio
import time
import enum
import re
from forge.tasks import wait_cancelable
from forge.units import concentration_ppm_to_ppb, pressure_mmHg_to_hPa, ONE_ATM_IN_HPA
from ..streaming import StreamingInstrument, StreamingContext, CommunicationsError
from ..parse import parse_number, parse_flags_bits
from ..record import Report

_INSTRUMENT_TYPE = __name__.split('.')[-2]
_FIELD_SPLIT = re.compile(rb"\s+")
_PROGRAM_NO_49i = re.compile(rb"iSeries\s+49\S*\s+(\S+)", re.IGNORECASE)
_PROGRAM_NO_49c = re.compile(rb"processor\s+49\S*\s+(\S+)\s+[\s\d]*c?link\s+49\D*\s*(\S+)", re.IGNORECASE)


def _convert_ozone_units(value: float, units: typing.List[bytes]) -> float:
    if len(units) != 1:
        raise CommunicationsError(f"invalid ozone units {units}")
    units = units[0]
    if units == b"ppb":
        return value
    elif units == b"ppm":
        return concentration_ppm_to_ppb(value)

    # My reading of the manual is that this uses a fixed temperature defaulting to 20C and that there's no way to
    # read the actual temperature.  So we use the air density at 20C for lack of anything better.
    air_weight_kgm3 = 1.2041
    if units == b"mg/m3" or units == b"mg/m\xFC":
        return value * 1E9 / (air_weight_kgm3 * 1E-6)
    elif units == b"ug/m3" or units == b"ug/m\xFC":
        return value * 1E9 / (air_weight_kgm3 * 1E-9)

    raise CommunicationsError(f"invalid ozone units {units}")


def _ignore_units(value: float, _: typing.List[bytes]) -> float:
    return value


class Instrument(StreamingInstrument):
    INSTRUMENT_TYPE = _INSTRUMENT_TYPE
    MANUFACTURER = "Thermo"
    MODEL = "49"
    DISPLAY_LETTER = "Z"
    TAGS = frozenset({"ozone", _INSTRUMENT_TYPE})
    SERIAL_PORT = {'baudrate': 9600}

    class _Type(enum.Enum):
        TYPE_49c = 0
        TYPE_49c_LEGACY = 1
        TYPE_49i = 2

    _AVERAGE_TIME = (
        (0, 10.0),
        (1, 20.0),
        (2, 30.0),
        (3, 60.0),
        (4, 90.0),
        (5, 120.0),
        (6, 180.0),
        (7, 240.0),
        (8, 300.0),
    )

    _LEGACY_49C_DELAY = 0.25

    def __init__(self, context: StreamingContext):
        super().__init__(context)

        self._report_interval: int = int(context.config.get('REPORT_INTERVAL', default=10))
        self._address: int = int(context.config.get('ADDRESS', default=0))

        self._type: "Instrument._Type" = self._Type.TYPE_49c
        self._sleep_time: float = 0.0

        self.data_X = self.input("X")
        self.data_Qa = self.input("Qa")
        self.data_Qb = self.input("Qb")
        self.data_Ca = self.input("Ca")
        self.data_Cb = self.input("Cb")
        self.data_Psample = self.input("Psample")
        self.data_Tsample = self.input("Tsample")
        self.data_Tlamp = self.input("Tlamp")

        self.data_Qozonator = self.input("Qozonator")

        self.data_Tozonator = self.input("Tozonator")
        self.data_Vlamp = self.input("Vlamp")
        self.data_Vozonator = self.input("Vozonator")

        self.data_bitflags = self.input("bitflags")

        self.ozone_var = self.variable_ozone(self.data_X, code="X")
        self.bit_flags: typing.Dict[int, Instrument.Notification] = dict()
        self.instrument_report = self.report(
            self.ozone_var,
            self.variable_air_pressure(self.data_Psample, "sample_pressure", code="P",
                                       attributes={'long_name': "sample bench pressure"}),
            self.variable_air_temperature(self.data_Tsample, "sample_temperature", code="T1",
                                          attributes={'long_name': "sample bench temperature"}),
            self.variable_temperature(self.data_Tlamp, "lamp_temperature", code="T2",
                                      attributes={'long_name': "measurement lamp temperature"}),
            self.variable_sample_flow(self.data_Qa, "cell_a_flow", code="Q1", attributes={
                'long_name': "air flow rate through cell A",
                'C_format': "%6.3f"
            }),
            self.variable_sample_flow(self.data_Qb, "cell_b_flow", code="Q2", attributes={
                'long_name': "air flow rate through cell B",
                'C_format': "%6.3f"
            }),
            self.variable(self.data_Ca, "cell_a_count_rate", code="C1", attributes={
                'long_name': "cell A intensity count rate",
                'units': "Hz",
                'C_format': "%7.0f"
            }),
            self.variable(self.data_Cb, "cell_b_count_rate", code="C2", attributes={
                'long_name': "cell B intensity count rate",
                'units': "Hz",
                'C_format': "%7.0f"
            }),

            flags=[
                self.flag_bit(self.bit_flags, 0x00000001, "alarm_sample_temperature_low", is_warning=True),
                self.flag_bit(self.bit_flags, 0x00000002, "alarm_sample_temperature_high", is_warning=True),
                self.flag_bit(self.bit_flags, 0x00000004, "alarm_lamp_temperature_low", is_warning=True),
                self.flag_bit(self.bit_flags, 0x00000008, "alarm_lamp_temperature_high", is_warning=True),
                self.flag_bit(self.bit_flags, 0x00000010, "alarm_ozonator_temperature_low", is_warning=True),
                self.flag_bit(self.bit_flags, 0x00000020, "alarm_ozonator_temperature_high", is_warning=True),
                self.flag_bit(self.bit_flags, 0x00000040, "alarm_pressure_low", is_warning=True),
                self.flag_bit(self.bit_flags, 0x00000080, "alarm_pressure_high", is_warning=True),
                self.flag_bit(self.bit_flags, 0x00000100, "alarm_flow_a_low", is_warning=True),
                self.flag_bit(self.bit_flags, 0x00000200, "alarm_flow_a_high", is_warning=True),
                self.flag_bit(self.bit_flags, 0x00000400, "alarm_flow_b_low", is_warning=True),
                self.flag_bit(self.bit_flags, 0x00000800, "alarm_flow_b_high", is_warning=True),
                self.flag_bit(self.bit_flags, 0x00001000, "alarm_intensity_a_low", is_warning=True),
                self.flag_bit(self.bit_flags, 0x00002000, "alarm_intensity_a_high", is_warning=True),
                self.flag_bit(self.bit_flags, 0x00004000, "alarm_intensity_b_low", is_warning=True),
                self.flag_bit(self.bit_flags, 0x00008000, "alarm_intensity_b_high", is_warning=True),
                self.flag_bit(self.bit_flags, 0x00100000, "ozonator_on"),
                self.flag_bit(self.bit_flags, 0x04000000, "pressure_compensation"),
                self.flag_bit(self.bit_flags, 0x08000000, "temperature_compensation"),
                self.flag_bit(self.bit_flags, 0x20000000, "service_mode"),
                self.flag_bit(self.bit_flags, 0x40000000, "alarm_ozone_low", is_warning=True),
                self.flag_bit(self.bit_flags, 0x80000000, "alarm_ozone_high", is_warning=True),
            ],
        )
        self.instrument_report.record.data_record.report_interval = self._report_interval

        self.report_49c: typing.Optional[Report] = None
        self.report_49i: typing.Optional[Report] = None

    def _declare_49c(self) -> None:
        if self.report_49c is not None:
            return
        self.report_49c = self.report(
            self.variable_sample_flow(self.data_Qozonator, "ozonator_flow", code="Q3", attributes={
                'long_name': "ozonator flow rate",
                'C_format': "%5.3f",
            }),
        )

    def _declare_49i(self) -> None:
        if self.report_49i is not None:
            return
        self.report_49i = self.report(
            self.variable_temperature(self.data_Tozonator, "ozonator_temperature", code="T3",
                                      attributes={'long_name': "ozonator lamp temperature"}),
            self.variable(self.data_Vlamp, "lamp_voltage", code="V1", attributes={
                'long_name': "measurement lamp voltage",
                'units': "V",
                'C_format': "%4.1f"
            }),
            self.variable(self.data_Vozonator, "ozonator_voltage", code="V2", attributes={
                'long_name': "ozonator lamp voltage",
                'units': "V",
                'C_format': "%4.1f"
            }),
        )

    async def _read_response(self) -> bytes:
        # Frames look like:
        #  response\r
        #  response*\r
        #  \xFFresponse\r
        #  response\nsum hhhh\r
        #  response\x80sum hhhh\r
        #  response*\nsum hhhh\r
        while True:
            line = await self.read_line()
            line = line.strip()
            if not line:
                continue

            # Split when delimited by 0x80
            try:
                idx = line.index(b'\x80sum', 1)
                line = line[:idx]
            except ValueError:
                pass

            # Remove odd prefixes
            if line[0] == 0x80 or line[0] == 0xFF:
                line = line[1:]
                if not line:
                    continue

            # Remove odd suffixes
            if line[-1] == 0x80 or line[-1] == 0xFF or line[-1] == ord(b'*'):
                line = line[:-1]
                if not line:
                    continue

            # Ignore any checksum responses
            if len(line) == 8 and line.startswith(b'sum '):
                continue

            return line

    def _send_command(self, command: bytes) -> None:
        if self._address != 0:
            self.writer.write(bytes(((self._address + 128) & 0xFF, )))
        self.writer.write(command)
        self.writer.write(b'\r')

    async def _command_response(self, command: bytes) -> bytes:
        self._send_command(command)
        response = await wait_cancelable(self._read_response(), 2.0)
        if not response.startswith(command + b" "):
            raise CommunicationsError(f"invalid response {command}: {response}")
        response = response[len(command) + 1:]
        response = response.strip()
        return response

    async def _command_number(self, command: bytes,
                              units: typing.Callable[[float, typing.List[bytes]], float] = None) -> float:
        response = await self._command_response(command)
        if units is None:
            return parse_number(response)
        fields = _FIELD_SPLIT.split(response)
        if len(fields) < 2:
            raise CommunicationsError("no units")
        value = parse_number(fields[0])
        return units(value, fields[1:])

    async def start_communications(self) -> None:
        if not self.writer:
            raise CommunicationsError

        await self.drain_reader(1.0)

        self._send_command(b"set mode remote")
        try:
            data: bytes = await wait_cancelable(self._read_response(), 2.0)
        except (asyncio.TimeoutError, TimeoutError):
            await asyncio.sleep(self._LEGACY_49C_DELAY)
            # Allow no response to the first one, but require one from the second
            self._send_command(b"set mode remote")
            data: bytes = await wait_cancelable(self._read_response(), 2.0)
        if data != b"set mode remote ok":
            raise CommunicationsError(f"remote mode failure: {data}")

        await asyncio.sleep(self._LEGACY_49C_DELAY)
        self._send_command(b"program no")
        try:
            data: bytes = await wait_cancelable(self._read_response(), 2.0)
            if data == b"bad cmd":
                self._type = self._Type.TYPE_49c_LEGACY
            else:
                match_49c = _PROGRAM_NO_49c.search(data)
                if match_49c:
                    self._type = self._Type.TYPE_49c
                    self.set_firmware_version(match_49c.group(1))
                    # self.set_instrument_info('link_version', match_49c.group(2).decode('ascii'))
                else:
                    match_49i = _PROGRAM_NO_49i.search(data)
                    if match_49i:
                        self._type = self._Type.TYPE_49i
                        self.set_firmware_version(match_49i.group(1))
                    else:
                        raise CommunicationsError(f"invalid program no: {data}")
        except (asyncio.TimeoutError, TimeoutError):
            # Old 49c do not respond to this
            self._type = self._Type.TYPE_49c_LEGACY

        async def ok_command(command: bytes) -> None:
            await asyncio.sleep(self._LEGACY_49C_DELAY)
            response: bytes = await self._command_response(command)
            if response != b"ok":
                raise CommunicationsError(f"invalid response {command}: {response}")

        async def ok_or_legacy(command: bytes, bad_cmd_legacy: bool = False) -> None:
            await asyncio.sleep(self._LEGACY_49C_DELAY)
            try:
                response = await self._command_response(command)
                if response == b"ok":
                    return
            except (asyncio.TimeoutError, TimeoutError):
                if self._type == self._Type.TYPE_49c or self._Type.TYPE_49c_LEGACY:
                    self._type = self._Type.TYPE_49c_LEGACY
                    return
                raise

            if bad_cmd_legacy and response == b"bad cmd":
                if self._type == self._Type.TYPE_49c or self._Type.TYPE_49c_LEGACY:
                    self._type = self._Type.TYPE_49c_LEGACY
                    return

            raise CommunicationsError(f"invalid response {command}: {response}")

        async def value_or_legacy(command: bytes) -> typing.Optional[bytes]:
            await asyncio.sleep(self._LEGACY_49C_DELAY)
            try:
                response: bytes = await self._command_response(command)
            except (asyncio.TimeoutError, TimeoutError):
                if self._type == self._Type.TYPE_49c or self._Type.TYPE_49c_LEGACY:
                    self._type = self._Type.TYPE_49c_LEGACY
                    return None
                raise
            return response

        async def number_or_legacy(
                command: bytes,
                units: typing.Callable[[float, typing.List[bytes]], float] = None,
                bad_cmd_legacy: bool = False) -> typing.Optional[float]:
            response = await value_or_legacy(command)
            if response is None:
                return None

            if bad_cmd_legacy and response == b"bad cmd":
                if self._type == self._Type.TYPE_49c or self._Type.TYPE_49c_LEGACY:
                    self._type = self._Type.TYPE_49c_LEGACY
                    return None

            if units is not None:
                fields = _FIELD_SPLIT.split(response)
                if len(fields) < 2:
                    raise CommunicationsError("no units")
                value = parse_number(fields[0])
                return units(value, fields[1:])
            return parse_number(response)

        # Disable checksum response
        await ok_command(b"set format 00")

        average_code = 0
        for code, interval in self._AVERAGE_TIME:
            if interval > self._report_interval:
                break
            average_code = code
        await ok_or_legacy(f"set avg time {average_code}".encode('ascii'))

        # Report at ambient
        await ok_or_legacy(b"set temp comp on")
        await ok_or_legacy(b"set pres comp on")

        # Sampling mode
        await ok_command(b"set sample")

        # Set time
        ts = time.gmtime()
        await ok_or_legacy(f"set date {ts.tm_mon:02}-{ts.tm_mday:02}-{ts.tm_year%100:02}".encode('ascii'))
        await ok_or_legacy(f"set time {ts.tm_hour:02}:{ts.tm_min:02}:{ts.tm_sec:02}".encode('ascii'))

        o3_bkg = await number_or_legacy(b"o3 bkg", units=_convert_ozone_units)
        if o3_bkg is not None:
            self.ozone_var.data.attributes['instrument_background'] = o3_bkg
        o3_bkg = await number_or_legacy(b"o3 coef")
        if o3_bkg is not None:
            self.ozone_var.data.attributes['instrument_coefficient'] = o3_bkg

        if self._type == self._Type.TYPE_49c:
            # Disambiguation for 49c legacy
            await number_or_legacy(b"oz flow", units=_ignore_units, bad_cmd_legacy=True)
        elif self._type == self._Type.TYPE_49i:
            host_name = await self._command_response(b"host name")
            # self.set_instrument_info('host_name', host_name.decode('ascii'))
            await self._command_response(b"l1")

        await asyncio.sleep(self._LEGACY_49C_DELAY)
        response = await self._command_response(b"flags")
        try:
            int(response, 16)
        except (ValueError, OverflowError):
            raise CommunicationsError(f"invalid flags {response}")

        await asyncio.sleep(self._LEGACY_49C_DELAY)
        await self._command_number(b"o3", units=_convert_ozone_units)

        if self._type == self._Type.TYPE_49c:
            self._declare_49c()
            self.set_instrument_info('model', "49c")
        elif self._type == self._Type.TYPE_49i:
            self._declare_49i()
            self.set_instrument_info('model', "49i")
        else:
            self.set_instrument_info('model', "49c")

        self._sleep_time = 0.0

    async def communicate(self) -> None:
        if self._type == self._Type.TYPE_49c_LEGACY:
            if self._sleep_time < self._LEGACY_49C_DELAY:
                self._sleep_time = self._LEGACY_49C_DELAY
        if self._sleep_time > 0.0:
            await asyncio.sleep(self._sleep_time)
            self._sleep_time = 0.0
        begin_read = time.monotonic()

        async def command_delay():
            # Legacy 49c misbehaves when commands are issued too fast
            if self._type != self._Type.TYPE_49c_LEGACY:
                return
            await asyncio.sleep(0.25)

        self.data_X(await self._command_number(b"o3", units=_convert_ozone_units))
        await command_delay()
        self.data_Tlamp(await self._command_number(b"lamp temp", units=_ignore_units))
        await command_delay()
        self.data_Ca(await self._command_number(b"cell a int", units=_ignore_units))
        await command_delay()
        self.data_Cb(await self._command_number(b"cell b int", units=_ignore_units))
        await command_delay()
        self.data_Qa(await self._command_number(b"flow a", units=_ignore_units))
        await command_delay()
        self.data_Qb(await self._command_number(b"flow b", units=_ignore_units))
        await command_delay()

        if self._type == self._Type.TYPE_49c:
            self.data_Qozonator(await self._command_number(b"oz flow", units=_ignore_units))
            await command_delay()
        elif self._type == self._Type.TYPE_49i:
            self.data_Tozonator(await self._command_number(b"o3 lamp temp", units=_ignore_units))
            await command_delay()
            self.data_Vlamp(await self._command_number(b"lamp voltage bench", units=_ignore_units))
            await command_delay()
            self.data_Vozonator(await self._command_number(b"lamp voltage oz", units=_ignore_units))
            await command_delay()

        async def read_report_condition(command: bytes) -> typing.Tuple[float, typing.Optional[float]]:
            response = await self._command_response(command)
            fields = _FIELD_SPLIT.split(response)
            if len(fields) == 0:
                raise CommunicationsError(f"invalid response to {command}")
            value = parse_number(fields[0])
            for i in range(1, len(fields)-1):
                check_word = fields[i]
                if check_word == b"actual":
                    actual = parse_number(fields[i+1])
                    return actual, value
            return value, None

        Tsample, Tstp = await read_report_condition(b"bench temp")
        await command_delay()
        self.data_Tsample(Tsample)

        Psample, Pstp = await read_report_condition(b"pres")
        await command_delay()
        self.data_Psample(pressure_mmHg_to_hPa(Psample))
        if Pstp is not None:
            Pstp = pressure_mmHg_to_hPa(Pstp)

        flags = await self._command_response(b"flags")
        flags = parse_flags_bits(flags, self.bit_flags)
        self.data_bitflags(flags)
        if flags & 0x04000000:
            self.ozone_var.data.use_standard_pressure = False
            self.instrument_report.record.data_record.standard_pressure = None
        else:
            self.ozone_var.data.use_standard_pressure = True
            self.instrument_report.record.data_record.standard_pressure = Tstp
        if flags & 0x08000000:
            self.ozone_var.data.use_standard_temperature = False
            self.instrument_report.record.data_record.standard_pressure = None
        else:
            self.ozone_var.data.use_standard_temperature = True
            self.instrument_report.record.data_record.standard_pressure = Pstp

        self.instrument_report()
        if self.report_49c is not None:
            self.report_49c()
        if self.report_49i is not None:
            self.report_49i()

        end_read = time.monotonic()
        self._sleep_time = self._report_interval - (end_read - begin_read)
