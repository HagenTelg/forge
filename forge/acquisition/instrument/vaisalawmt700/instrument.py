import typing
import asyncio
from forge.tasks import wait_cancelable
from ..streaming import StreamingInstrument, StreamingContext, CommunicationsError
from ..parse import parse_number, parse_flags_bits

_INSTRUMENT_TYPE = __name__.split('.')[-2]


class Instrument(StreamingInstrument):
    INSTRUMENT_TYPE = _INSTRUMENT_TYPE
    MANUFACTURER = "Vaisala"
    MODEL = "WMT700"
    DISPLAY_LETTER = "I"
    TAGS = frozenset({"met", "aerosol", _INSTRUMENT_TYPE})
    SERIAL_PORT = {'baudrate': 9600}

    def __init__(self, context: StreamingContext):
        super().__init__(context)

        self._report_interval: float = float(context.config.get('REPORT_INTERVAL', default=1))
        self._address: bytes = str(context.config.get('ADDRESS', default="0")).encode('ascii')

        self.data_WS = self.input("WS")
        self.data_WD = self.input("WD")
        self.data_Tsonic = self.input("Tsonic")
        self.data_Ttransducer = self.input("Ttransducer")
        self.data_Vsupply = self.input("Vsupply")
        self.data_Vheater = self.input("Vheater")

        self.bit_flags: typing.Dict[int, Instrument.Notification] = dict()
        self.instrument_report = self.report(
            *self.variable_winds(self.data_WS, self.data_WD, code=""),

            self.variable_air_temperature(self.data_Tsonic, "sonic_temperature", code="T1",
                                          attributes={'long_name': "air temperature used for speed of sound calculations"}),
            self.variable_temperature(self.data_Ttransducer, "transducer_temperature", code="T2",
                                      attributes={'long_name': "temperature of ultrasonic transducer"}),

            flags=[
                self.flag_bit(self.bit_flags, 0x0001, "temperature_sensor_1_failure", is_warning=True),
                self.flag_bit(self.bit_flags, 0x0002, "temperature_sensor_2_failure", is_warning=True),
                self.flag_bit(self.bit_flags, 0x0004, "temperature_sensor_3_failure", is_warning=True),
                self.flag_bit(self.bit_flags, 0x0008, "heater_failure", is_warning=True),
                self.flag_bit(self.bit_flags, 0x0010, "supply_voltage_high", is_warning=True),
                self.flag_bit(self.bit_flags, 0x0020, "supply_voltage_low", is_warning=True),
                self.flag_bit(self.bit_flags, 0x0040, "wind_speed_high"),
                self.flag_bit(self.bit_flags, 0x0080, "sonic_temperature_out_of_range", is_warning=True),
                self.flag_bit(self.bit_flags, 0x0100, "low_wind_validity"),
                self.flag_bit(self.bit_flags, 0x0400, "blocked_sensor", is_warning=True),
                self.flag_bit(self.bit_flags, 0x1000, "high_noise_level"),
            ]
        )

    async def start_communications(self) -> None:
        if self.writer:
            # Stop reports
            self.writer.write(b"\r\r\n\r\r$" + self._address + b"OPEN\r")
            await self.writer.drain()
            await self.drain_reader(self._report_interval + 2.0)

            # Enable error messages
            self.writer.write(b"S messages,1\r")
            await self.writer.drain()
            await self.drain_reader(2.0)

            def is_error(response: bytes) -> bool:
                return b"ERROR" in response or b"Command does not exist" in response or b"unknown parameter" in response

            # Get firmware version
            self.writer.write(b"VERSION\r")
            data: bytes = await wait_cancelable(self.read_line(), 2.0)
            if is_error(data):
                raise CommunicationsError(f"invalid version response {data}")
            if data.startswith(b">"):
                data = data[1:].strip()
            if data.startswith(b"WMT 700"):
                data = data[7:].strip()
            elif data.startswith(b"WMT700"):
                data = data[6:].strip()
            self.set_firmware_version(data)

            # Get serial number
            self.writer.write(b"G serial_n\r")
            data: bytes = await wait_cancelable(self.read_line(), 2.0)
            if is_error(data):
                raise CommunicationsError(f"invalid serial number response {data}")
            if data.startswith(b">"):
                data = data[1:].strip()
            if data.lower().startswith(b"s serial_n"):
                data = data[10:].strip()
            if data.startswith(b","):
                data = data[1:].strip()
            self.set_serial_number(data)

            # Get PCB number
            # self.writer.write(b"G serial_pcb\r")
            # data: bytes = await wait_cancelable(self.read_line(), 2.0)
            # if is_error(data):
            #     raise CommunicationsError(f"invalid serial number response {data}")
            # if data.startswith(b">"):
            #     data = data[1:].strip()
            # if data.lower().startswith(b"s serial_pcb"):
            #     data = data[12:].strip()
            # if data.startswith(b","):
            #     data = data[1:].strip()

            # Get serial number
            self.writer.write(b"G cal_date\r")
            data: bytes = await wait_cancelable(self.read_line(), 2.0)
            if is_error(data):
                raise CommunicationsError(f"invalid serial number response {data}")
            if data.startswith(b">"):
                data = data[1:].strip()
            if data.lower().startswith(b"s cal_date"):
                data = data[10:].strip()
            if data.startswith(b","):
                data = data[1:].strip()
            self.set_instrument_info('calibration', data.decode('utf-8', 'backslashreplace'))

            # Set m/s units
            self.writer.write(b"S wndUnit,0\r")
            data: bytes = await wait_cancelable(self.read_line(), 2.0)
            if is_error(data):
                raise CommunicationsError(f"invalid set response {data}")

            # Set vector averaging
            self.writer.write(b"S wndVector,1\r")
            data: bytes = await wait_cancelable(self.read_line(), 2.0)
            if is_error(data):
                raise CommunicationsError(f"invalid set response {data}")

            # Set report type
            self.writer.write(b"S autoSend,24\r")
            data: bytes = await wait_cancelable(self.read_line(), 2.0)
            if is_error(data):
                raise CommunicationsError(f"invalid set response {data}")

            # Set output interval
            report_interval = round(self._report_interval / 0.25) * 0.25
            if report_interval < 0.25:
                report_interval = 0.25
            elif report_interval > 1000:
                report_interval = 1000
            self.writer.write(b"S autoInt,%.2f\r" % report_interval)
            data: bytes = await wait_cancelable(self.read_line(), 2.0)
            if is_error(data):
                raise CommunicationsError(f"invalid set response {data}")

            self.writer.write(b"START\r")
            await self.writer.drain()
            await self.drain_reader(1.0)
            self.writer.write(b"CLOSE\r")
            await self.writer.drain()

        # Flush the first record
        await self.drain_reader(self._report_interval + 1.0)
        await wait_cancelable(self.read_line(), self._report_interval * 2 + 1)

        # Process a valid record
        await self.communicate()

    async def communicate(self) -> None:
        line: bytes = await wait_cancelable(self.read_line(), self._report_interval * 2 + 1)
        if len(line) < 4:
            raise CommunicationsError

        checksum = line[-2:]
        try:
            checksum = int(checksum.strip(), 16)
            if checksum < 0 or checksum > 0xFF:
                raise ValueError
        except ValueError:
            raise CommunicationsError(f"invalid checksum in {line}")

        frame = line[:-2]
        v = 0
        for b in frame:
            v ^= b
        if v != checksum:
            raise CommunicationsError(f"checksum mismatch on {line} (got {v:02X})")

        if frame[0:1] != b"$" or frame[-1:] != b",":
            raise CommunicationsError(f"invalid framing in {line}")
        frame = frame[1:-1]

        fields = frame.strip().split(b",")
        try:
            (
                WS, WD,
                _,  # Max wind speed
                _,  # Min wind speed
                Tsonic, Vheater, Vsupply, Ttransducer,
                flags
            ) = fields
        except ValueError:
            raise CommunicationsError(f"invalid number of fields in {line}")

        self.data_WS(parse_number(WS))
        self.data_WD(parse_number(WD))
        self.data_Tsonic(parse_number(Tsonic))
        self.data_Ttransducer(parse_number(Ttransducer))
        self.data_Vheater(parse_number(Vheater))
        self.data_Vsupply(parse_number(Vsupply))

        parse_flags_bits(flags, self.bit_flags, base=10)

        self.instrument_report()
