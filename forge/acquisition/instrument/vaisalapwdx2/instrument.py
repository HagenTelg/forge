import typing
import asyncio
import time
import serial
import re
from forge.tasks import wait_cancelable
from forge.units import distance_m_to_km
from ..streaming import StreamingInstrument, StreamingContext, CommunicationsError
from ..parse import parse_number

_INSTRUMENT_TYPE = __name__.split('.')[-2]
_VERSION = re.compile(
    rb"VAISALA\s+"
    rb"(PWD\S+)\s+"
    rb"v\s*(\S+(?:\s+\d{4}-\d{2}-\d{2})?)\s+"
    rb"SN\s*:\s*(\S+)"
    rb"(?:\s+ID\s+STRING\s*:\s*(\S+))?",
    flags=re.IGNORECASE
)
_RESPONSE_START = re.compile(rb"\x01PW\s+(\S+)\x02")


class Instrument(StreamingInstrument):
    INSTRUMENT_TYPE = _INSTRUMENT_TYPE
    MANUFACTURER = "Vaisala"
    MODEL = "PWDx2"
    DISPLAY_LETTER = "V"
    TAGS = frozenset({"met", "aerosol", _INSTRUMENT_TYPE})
    SERIAL_PORT = {'baudrate': 9600, 'parity': serial.PARITY_EVEN, 'bytesize': serial.SEVENBITS}

    def __init__(self, context: StreamingContext):
        super().__init__(context)

        self._report_interval: float = float(context.config.get('REPORT_INTERVAL', default=15.0))
        configured_id = context.config.get('INSTRUMENT_ID')
        if configured_id:
            self._configured_id: typing.Optional[bytes] = str(configured_id).encode('ascii')
        else:
            self._configured_id: typing.Optional[bytes] = None
        self._queried_id: typing.Optional[bytes] = None
        self._sleep_time: float = 0.0

        self.data_WZ = self.input("WZ")
        self.data_WZ10Min = self.input("WZ10Min")
        self.data_WI = self.input("WI")

        self.data_WX = self.persistent("WX")
        self.data_WX15Min = self.input("WX15Min")
        self.data_WX1Hour = self.input("WX1Hour")
        self.data_nws_code = self.persistent("nws_code")

        self.data_Tambient = self.input("Tambient")
        self.data_Tinternal = self.input("Tinternal")
        self.data_Tdrd = self.input("Tdrd")

        self.data_Csignal = self.input("Csignal")
        self.data_Coffset = self.input("Coffset")
        self.data_Cdrift = self.input("Cdrift")
        self.data_Cdrd = self.input("Cdrd")
        self.data_I = self.input("I")

        self.data_BsTx = self.input("BsTx")
        self.data_BsTxChange = self.input("BsTxChange")
        self.data_BsRx = self.input("BsRx")
        self.data_BsRxChange = self.input("BsRxChange")

        self.data_Vsupply = self.input("Vsupply")
        self.data_Vpositive = self.input("Vpositive")
        self.data_Vnegative = self.input("Vnegative")
        self.data_Vled = self.input("Vled")
        self.data_Vambient = self.input("Vambient")

        def wrap_input(i: Instrument.Input) -> typing.Callable[[bytes], typing.Any]:
            def wrapped(value: bytes):
                i(parse_number(value))
            return wrapped

        self._status_match: typing.List[typing.Tuple["re.Pattern", typing.Dict[int, typing.Callable[[bytes], typing.Any]]]] = [
            (re.compile(rb"SIGNAL\*?\s+\*?(\S+)\s+OFFSET\*?\s+\*?(\S+)\s+DRIFT\*?\s+\*?(\S+)"), {
                1: wrap_input(self.data_Csignal),
                2: wrap_input(self.data_Coffset),
                3: wrap_input(self.data_Cdrift),
            }),
            (re.compile(rb"REC\. BACKSCATTER\*?\s+\*?(\S+)\s+CHANGE\*?\s+\*?(\S+)"), {
                1: wrap_input(self.data_BsRx),
                2: wrap_input(self.data_BsRxChange),
            }),
            (re.compile(rb"TR\. (?:BACKSCATTER|B)\*?\s+\*?(\S+)\s+CHANGE\*?\s+\*?(\S+)"), {
                1: wrap_input(self.data_BsTx),
                2: wrap_input(self.data_BsTxChange),
            }),
            (re.compile(rb"LEDI\*?\s+\*?(\S+)"), {
                1: wrap_input(self.data_Vled),
            }),
            (re.compile(rb"AMBL\*?\s+\*?(\S+)"), {
                1: wrap_input(self.data_Vambient),
            }),
            (re.compile(rb"VBB\*?\s+\*?(\S+)"), {
                1: wrap_input(self.data_Vsupply),
            }),
            (re.compile(rb"P12\*?\s+\*?(\S+)"), {
                1: wrap_input(self.data_Vpositive),
            }),
            (re.compile(rb"M12\*?\s+\*?(\S+)"), {
                1: wrap_input(self.data_Vnegative),
            }),
            (re.compile(rb"TS\*?\s+\*?(\S+)\s+TB\*?\s+\*?(\S+)"), {
                1: wrap_input(self.data_Tambient),
                2: wrap_input(self.data_Tinternal),
            }),
            (re.compile(rb"TDRD\*?\s+\*?(\S+)\s+(?:\d+\s+)?DRD\*?\s+\*?(\S+)\s+(?:\d+\s+)?DRY\*?\s+\*?\S+"), {
                1: wrap_input(self.data_Tdrd),
                2: wrap_input(self.data_Cdrd),
            }),
            (re.compile(rb"BL\*?\s+\*?(\S+)"), {
                1: wrap_input(self.data_I),
            }),
            (re.compile(rb"^RELAYS\s*\S+.*$"), {}),
            (re.compile(rb"^HOOD HEATERS\s*\S+.*$"), {}),
            (re.compile(rb"^HARDWARE\s*:"), {}),
        ]

        self.notify_hardware_error = self.notification("hardware_error", is_warning=True)
        self.notify_backscatter_range = self.notification("backscatter_range")
        notify_transmitter_range = self.notification("transmitter_range", is_warning=True)
        notify_receiver_range = self.notification("receiver_range", is_warning=True)
        self._notify_possible: typing.Set[Instrument.Notification] = {
            self.notify_hardware_error,
            self.notify_backscatter_range,
            notify_transmitter_range,
            notify_receiver_range
        }
        self._notify_present: typing.Set[Instrument.Notification] = set()

        self._status_text: typing.List[typing.Tuple[bytes, Instrument.Notification]] = [
            (b"backscatter high", self.notify_hardware_error),
            (b"backscatter high", self.notify_backscatter_range),
            (b"transmitter error", notify_transmitter_range),
            (b"transmitter error", self.notify_hardware_error),
            (b"power error", self.notification("power_range", is_warning=True)),
            (b"power error", self.notify_hardware_error),
            (b"offset error", self.notification("offset_range", is_warning=True)),
            (b"offset error", self.notify_hardware_error),
            (b"signal error", self.notification("signal_error", is_warning=True)),
            (b"signal error", self.notify_hardware_error),
            (b"receiver error", notify_receiver_range),
            (b"receiver error", self.notify_hardware_error),
            (b"data ram error", self.notification("data_ram_error", is_warning=True)),
            (b"data ram error", self.notify_hardware_error),
            (b"eeprom error", self.notification("eeprom_error", is_warning=True)),
            (b"eeprom error", self.notify_hardware_error),
            (b"ts sensor error", self.notification("temperature_range")),
            (b"ts sensor error", self.notify_hardware_error),
            (b"drd error", self.notification("rain_range")),
            (b"drd error", self.notify_hardware_error),
            (b"luminance sensor error", self.notification("luminance_range")),
            (b"luminance sensor error", self.notify_hardware_error),
            (b"transmitter intensity low", notify_transmitter_range),
            (b"transmitter intensity low", self.notify_hardware_error),
            (b"receiver saturated", notify_receiver_range),
            (b"receiver saturated", self.notify_hardware_error),
            (b"offset drifted", self.notification("offset_drift")),
            (b"offset drifted", self.notify_hardware_error),
            (b"visibility not calibrated", self.notification("visibility_not_calibrated")),
            (b"visibility not calibrated", self.notify_hardware_error),
        ]
        for _, n in self._status_text:
            self._notify_possible.add(n)

        self.instrument_report = self.report(
            self.variable(self.data_WZ, "visibility", code="WZ", attributes={
                'long_name': "visibility distance",
                'units': "km",
                'C_format': "%7.3f"
            }),
            self.variable(self.data_WI, "precipitation_rate", code="WI", attributes={
                'long_name': "precipitation rate",
                'units': "mm h-1",
                'C_format': "%7.3f"
            }),

            self.variable_air_temperature(self.data_Tambient, "ambient_temperature", code="T1",
                                          attributes={'long_name': "ambient temperature"}),
            self.variable_temperature(self.data_Tinternal, "internal_temperature", code="T2",
                                      attributes={'long_name': "internal circuit board temperature"}),
            self.variable_temperature(self.data_Tdrd, "drd_temperature", code="T3",
                                      attributes={'long_name': "DRD precipitation sensor temperature"}),

            self.variable(self.data_I, "background_luminance", code="I", attributes={
                'long_name': "background luminance sensor reading",
                'units': "cd m-2",
                'C_format': "%8.2f"
            }),

            self.variable(self.data_Vled, "led_control_voltage", code="V1", attributes={
                'long_name': "LED transmitter control voltage",
                'units': "V",
                'C_format': "%5.2f"
            }),
            self.variable(self.data_Vambient, "ambient_light_voltage", code="V2", attributes={
                'long_name': "ambient light receiver output voltage",
                'units': "V",
                'C_format': "%5.2f"
            }),

            self.variable(self.data_Csignal, "signal_frequency", code="C1", attributes={
                'long_name': "frequency of the transmission signal between the transducer and processor, inversely proportional to visibility",
                'units': "Hz",
                'C_format': "%8.2f"
            }),
            self.variable(self.data_Coffset, "offset_frequency", code="C2", attributes={
                'long_name': "measurement signal offset and the lowest possible frequency measurement",
                'units': "Hz",
                'C_format': "%6.2f"
            }),

            self.variable(self.data_BsRx, "receiver_contamination", code="ZBsp", attributes={
                'long_name': "receiver contamination backscatter measurement",
                'C_format': "%5.1f"
            }),
            self.variable(self.data_BsTx, "transmitter_contamination", code="ZBsx", attributes={
                'long_name': "transmitter contamination backscatter control voltage",
                'units': "V",
                'C_format': "%5.1f"
            }),

            flags=[self.flag(n) for n in self._notify_possible],
        )

        self.present_weather = self.change_event(
            self.state_unsigned_integer(self.data_WX, "synop_weather_code", code="WX", attributes={
                'long_name': "WMO SYNOP weather code",
                'C_format': "%2llu"
            }),
            self.state_string(self.data_nws_code, "nws_weather_code", code="ZWXNWS", attributes={
                'long_name': "NWS weather code",
            }),
        )

        self.parameters_record = self.context.data.constant_record("parameters")
        self.parameter_system = self.parameters_record.string("system_parameters", attributes={
            'long_name': "instrument response to the PAR command, representing general operating parameters",
        })
        self.parameter_weather = self.parameters_record.string("weather_parameters", attributes={
            'long_name': "instrument response to the WPAR command, representing weather identification parameters",
        })

    def _handle_version_match(self, matched: re.Match) -> None:
        model = matched.group(1)
        if model:
            self.set_instrument_info('model', model.decode('utf-8'))
        fw = matched.group(2)
        if fw:
            self.set_firmware_version(fw)
        sn = matched.group(3)
        if sn:
            self.set_serial_number(sn)
        self._queried_id = matched.group(4)

    async def start_communications(self) -> None:
        if self.writer:

            # Reset line status
            self.writer.write(b"CLOSE\r")
            await self.writer.drain()
            await self.drain_reader(1.0)

            async def wait_open():
                while True:
                    line: bytes = await self.read_line()
                    if b"OPENED FOR OPERATOR COMMANDS" in line:
                        return

            # Enter command mode
            if self._configured_id:
                self.writer.write(b"OPEN " + self._configured_id + b"\r")
                await wait_cancelable(wait_open(), 5.0)
            else:
                self.writer.write(b"OPEN\r")
                try:
                    await wait_cancelable(wait_open(), 5.0)
                except (asyncio.TimeoutError, TimeoutError):
                    self.writer.write(b"OPEN *\r")
                    await wait_cancelable(wait_open(), 5.0)
            await self.drain_reader(0.5)

            # Set on board time
            ts = time.gmtime()
            self.writer.write(f"DATE {ts.tm_year:04d} {ts.tm_mon:02d} {ts.tm_mday:02d}\r".encode('ascii'))
            await self.writer.drain()
            await self.drain_reader(1.0)
            ts = time.gmtime()
            self.writer.write(f"TIME {ts.tm_hour:02d} {ts.tm_min:02d} {ts.tm_sec:02d}\r".encode('ascii'))
            await self.writer.drain()
            await self.drain_reader(0.5)

            # Disable unpolled
            self.writer.write(b"AMES 0 0\r")
            await self.writer.drain()
            await self.drain_reader(0.5)

            self.writer.write(b"PAR\r")
            lines = await self.read_multiple_lines(total=5.0, first=2.0, tail=2.0)
            if b"SYSTEM PARAMETERS" in lines[0]:
                del lines[0]
            self.parameter_system("\n".join([l.decode('utf-8', 'backslashreplace') for l in lines]))
            for l in lines:
                matched = _VERSION.search(l)
                if not matched:
                    continue
                self._handle_version_match(matched)

            self.writer.write(b"WPAR\r")
            try:
                lines = await self.read_multiple_lines(total=5.0, first=2.0, tail=2.0)
                if b"WEATHER PARAMETERS" in lines[0]:
                    del lines[0]
                while len(lines) > 0 and lines[0] == b"ERROR":
                    del lines[0]
                if lines:
                    self.parameter_weather("\n".join([l.decode('utf-8', 'backslashreplace') for l in lines]))
            except (asyncio.TimeoutError, TimeoutError):
                pass

            self.writer.write(b"STA\r")
            lines = await self.read_multiple_lines(total=5.0, first=2.0, tail=2.0)
            self._process_status(lines)

            async def wait_close():
                while True:
                    line: bytes = await self.read_line()
                    if b"LINE CLOSED" in line:
                        return

            self.writer.write(b"CLOSE\r")
            await wait_cancelable(wait_close(), 5.0)
            await self.drain_reader(0.5)

        self._sleep_time = 0.0
        await self.communicate()
        self._sleep_time = 0.0

    @property
    def _poll_id(self) -> bytes:
        if self._configured_id:
            return self._configured_id
        return self._queried_id or b" 1"

    def _process_status(self, lines: typing.List[bytes]) -> None:
        if b"PWD STATUS" in lines[0]:
            del lines[0]
        if len(lines) < 2:
            raise CommunicationsError
        for l in lines:
            l = bytearray(l)
            matched = _VERSION.search(l)
            if matched:
                self._handle_version_match(matched)
                del l[matched.start():matched.end()]
                l = l.strip()
            for pattern, targets in self._status_match:
                matched = pattern.search(l)
                if not matched:
                    continue
                for g, c in targets.items():
                    c(matched.group(g))
                del l[matched.start():matched.end()]
                l = l.strip()
            if not l:
                continue
            l = l.lower()
            for check, s in self._status_text:
                if check not in l:
                    continue
                self._notify_present.add(s)

    def _process_message(self, line: bytes) -> None:
        line = bytearray(line)
        match = _RESPONSE_START.match(line)
        if match:
            if match.group(1) != b"1" and match.group(1) != self._poll_id.strip():
                raise CommunicationsError(f"invalid response ID in {line}")
            del line[:match.end()]
        if line.endswith(b"\x03"):
            del line[-1:]
        line = line.strip()
        if len(line) < 3:
            raise CommunicationsError

        fields = line.split()
        try:
            (
                alarms, WZ, WZ10Min,
                nws_code, WX, WX15Min, WX1Hour,
                WI,
                _,  # Cumulative water
                _,  # Cumulative snow
            ) = fields
        except ValueError:
            raise CommunicationsError(f"invalid number of fields in {line}")

        if len(alarms) != 2:
            raise CommunicationsError(f"invalid alarms in {line}")
        if alarms[:1] == b"1":
            # Visibility alarm 1
            pass
        elif alarms[:1] == b"2":
            # Visibility alarm 2
            pass
        elif alarms[:1] == b"3":
            # Visibility alarm 3
            pass
        elif alarms[:1] == b"0":
            pass
        else:
            raise CommunicationsError(f"invalid alarms in {line}")

        if alarms[1:] == b"1":
            self._notify_present.add(self.notify_hardware_error)
        elif alarms[1:] == b"2":
            self._notify_present.add(self.notify_hardware_error)
        elif alarms[1:] == b"3":
            self._notify_present.add(self.notify_backscatter_range)
        elif alarms[1:] == b"4":
            self._notify_present.add(self.notify_backscatter_range)
        elif alarms[1:] == b"0":
            pass
        else:
            raise CommunicationsError(f"invalid alarms in {line}")

        self.data_WZ(distance_m_to_km(parse_number(WZ)))
        self.data_WZ10Min(distance_m_to_km(parse_number(WZ10Min)))

        # Check for PWDx0 mode, which replaces all other fields with "/"
        for check in (nws_code, WX, WX15Min, WX1Hour, WI):
            if check != b"/" * len(check):
                break
        else:
            return

        self.data_WI(parse_number(WI))

        def parse_synop_code(raw: bytes) -> int:
            try:
                v = int(raw)
                if v < 0 or v > 99:
                    raise ValueError
            except ValueError as e:
                raise CommunicationsError from e
            return v

        self.data_WX15Min(parse_synop_code(WX15Min))
        self.data_WX1Hour(parse_synop_code(WX1Hour))
        self.data_WX(parse_synop_code(WX))
        try:
            self.data_nws_code(nws_code.decode('ascii'))
        except UnicodeDecodeError as e:
            raise CommunicationsError from e

    async def communicate(self) -> None:
        self._notify_present.clear()

        if not self.writer:
            line: bytes = await wait_cancelable(self.read_line(), self._report_interval * 2 + 1.0)
            self._process_message(line)
            for n in self._notify_possible:
                n(n in self._notify_present)
            self.instrument_report()
            return

        if self._sleep_time > 0.0:
            await asyncio.sleep(self._sleep_time)
            self._sleep_time = 0.0
        begin_read = time.monotonic()

        self.writer.write(b"\r\x05PW " + self._poll_id + b" 2\r")
        line: bytes = await wait_cancelable(self.read_line(), 5.0)
        self._process_message(line)

        self.writer.write(b"\r\x05PW " + self._poll_id + b" 3\r")
        lines = await self.read_multiple_lines(total=5.0, first=2.0, tail=2.0)
        match = _RESPONSE_START.match(lines[0])
        if match:
            if match.group(1) != b"1" and match.group(1) != self._poll_id.strip():
                raise CommunicationsError(f"invalid response ID in {line}")
            lines[0] = lines[0][match.end():].strip()
        if lines[-1].endswith(b"\x03"):
            lines[-1] = lines[-1][-1:].strip()
        while len(lines) > 0 and not lines[-1]:
            del lines[-1]
        while len(lines) > 0 and not lines[0]:
            del lines[0]
        if len(lines) < 1:
            raise CommunicationsError("empty status response")
        self._process_status(lines)

        for n in self._notify_possible:
            n(n in self._notify_present)
        self.instrument_report()
        end_read = time.monotonic()
        self._sleep_time = self._report_interval - (end_read - begin_read)
