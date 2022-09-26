import typing
import asyncio
import time
from forge.units import flow_ccm_to_lpm
from ..streaming import StreamingInstrument, StreamingContext, CommunicationsError
from ..parse import parse_number, parse_datetime_field, parse_flags_bits

_INSTRUMENT_TYPE = __name__.split('.')[-2]


class Instrument(StreamingInstrument):
    INSTRUMENT_TYPE = _INSTRUMENT_TYPE
    MANUFACTURER = "Aerosol Dynamics"
    MODEL = "MAGIC 250"
    DISPLAY_LETTER = "C"
    TAGS = frozenset({"aerosol", "cpc", _INSTRUMENT_TYPE})
    SERIAL_PORT = {'baudrate': 115200}

    def __init__(self, context: StreamingContext):
        super().__init__(context)

        self._report_interval: int = int(context.config.get('REPORT_INTERVAL', default=1))

        self.data_N = self.input("N")
        self.data_C = self.input("C")
        self.data_Q = self.input("Q")
        self.data_Qinstrument = self.input("Qinstrument")
        self.data_P = self.input("P")
        self.data_Vpulse = self.input("Vpulse")
        self.data_PCTwick = self.input("PCTwick")

        self.data_Tinlet = self.input("Tinlet")
        self.data_Tconditioner = self.input("Tconditioner")
        self.data_Tinitiator = self.input("Tinitiator")
        self.data_Tmoderator = self.input("Tmoderator")
        self.data_Toptics = self.input("Toptics")
        self.data_Theatsink = self.input("Theatsink")
        self.data_Tcase = self.input("Tcase")
        self.data_Uinlet = self.input("Uinlet")
        self.data_TDinlet = self.input("TDinlet")
        self.data_TDgrowth = self.input("TDgrowth")

        if not self.data_N.comment and self.data_Q.comment:
            self.data_N.comment = self.data_Q.comment

        self.bit_flags: typing.Dict[int, Instrument.Notification] = dict()
        self.instrument_report = self.report(
            self.variable_number_concentration(self.data_N, code="N"),
            self.variable_sample_flow(self.data_Q, code="Q",
                                      attributes={'C_format': "%5.3f"}),
            self.variable_air_pressure(self.data_P, "pressure", code="P",
                                       attributes={'long_name': "absolute pressure"}),
            self.variable_air_temperature(self.data_Tinlet, "inlet_temperature", code="Tu",
                                          attributes={'long_name': "air temperature at the instrument inlet"}),
            self.variable_temperature(self.data_Tconditioner, "conditioner_temperature", code="T1",
                                      attributes={'long_name': "temperature of the conditioner (1st stage)"}),
            self.variable_temperature(self.data_Tinitiator, "initiator_temperature", code="T2",
                                      attributes={'long_name': "temperature of the initiator (2nd stage)"}),
            self.variable_temperature(self.data_Tmoderator, "moderator_temperature", code="T3",
                                      attributes={'long_name': "temperature of the moderator (3rd stage)"}),
            self.variable_temperature(self.data_Toptics, "optics_temperature", code="T4",
                                      attributes={'long_name': "temperature of the optics head"}),
            self.variable_temperature(self.data_Theatsink, "heatsink_temperature", code="T5",
                                      attributes={'long_name': "temperature of the heatsink"}),
            self.variable_temperature(self.data_Tcase, "case_temperature", code="T6",
                                      attributes={'long_name': "temperature inside the metal case"}),
            self.variable_air_rh(self.data_Uinlet, "inlet_humidity", code="Uu",
                                 attributes={'long_name': "relative humidity at the instrument inlet"}),
            self.variable_air_dewpoint(self.data_TDinlet, "inlet_dewpoint", code="TDu",
                                       attributes={'long_name': "dewpoint calculated from inlet temperature and humidity"}),
            self.variable_dewpoint(self.data_TDgrowth, "growth_tube_dewpoint", code="TD1",
                                   attributes={'long_name': "estimated dewpoint at the start of the growth tube"}),
            self.variable(self.data_PCTwick, "wick_saturation", code="PCT", attributes={
                'long_name': "wick saturation measured between initiator and moderator",
                'units': "%",
                'C_format': "%3.0f"
            }),
            self.variable(self.data_Vpulse, "pulse_height", code="V", attributes={
                'long_name': "upper pulse height threshold",
                'units': "mV",
                'C_format': "%4.0f"
            }),

            flags=[
                self.flag_bit(self.bit_flags, 0x000001, "conditioner_temperature_out_of_range", is_warning=True),
                self.flag_bit(self.bit_flags, 0x000002, "initiator_temperature_out_of_range", is_warning=True),
                self.flag_bit(self.bit_flags, 0x000004, "moderator_temperature_out_of_range", is_warning=True),
                self.flag_bit(self.bit_flags, 0x000008, "optics_temperature_out_of_range", is_warning=True),
                self.flag_bit(self.bit_flags, 0x000010, "laser_off", is_warning=True),
                self.flag_bit(self.bit_flags, 0x000020, "pump_off", is_warning=True),
                self.flag_bit(self.bit_flags, 0x000040, "rh_data_stale"),
                self.flag_bit(self.bit_flags, 0x000080, "i2c_communication_error"),
                self.flag_bit(self.bit_flags, 0x000100, "rh_sensor_error", is_warning=True),
                self.flag_bit(self.bit_flags, 0x000200, "overheat", is_warning=True),
                self.flag_bit(self.bit_flags, 0x000400, "dry_wick", is_warning=True),
                self.flag_bit(self.bit_flags, 0x000800, "fallback_humidifier_dewpoint", is_warning=True),
                self.flag_bit(self.bit_flags, 0x001000, "dewpoint_calculation_error"),
                self.flag_bit(self.bit_flags, 0x002000, "wick_sensor_out_of_range", is_warning=True),
                self.flag_bit(self.bit_flags, 0x004000, "flash_full"),
                self.flag_bit(self.bit_flags, 0x008000, "fram_data_invalid"),
                # 0x010000 reserved
                self.flag_bit(self.bit_flags, 0x020000, "thermistor_fault", is_warning=True),
                self.flag_bit(self.bit_flags, 0x040000, "sample_flow_out_of_range", is_warning=True),
                # 0x080000 reserved
                self.flag_bit(self.bit_flags, 0x100000, "ic2_multiplexer_error"),
                self.flag_bit(self.bit_flags, 0x200000, "low_clock_battery"),
                self.flag_bit(self.bit_flags, 0x400000, "clock_stopped"),
            ],

        )

    async def start_communications(self) -> None:
        if self.writer:
            # Stop reports
            self.writer.write(b"\r\rLog,0\r")
            await self.writer.drain()
            await self.drain_reader(1.0)
            self.writer.write(b"Log,0\r")
            await self.writer.drain()
            await self.drain_reader(1.0)

            self.writer.write(b"hdr\r")
            await self.writer.drain()
            hdr = await self.read_multiple_lines(total=5.0, first=2.0, tail=1.0)
            if hdr[0].startswith(b"hdr"):  # Ignore the echo
                del hdr[0]
            if not hdr:
                raise CommunicationsError("no header response")
            hdr = b",".join(hdr)
            if b"Concentration" not in hdr or b"PulseHeight.Thres2" not in hdr:
                raise CommunicationsError(f"header response: {hdr}")

            self.writer.write(b"wadc\r")
            await self.writer.drain()
            data: bytes = await asyncio.wait_for(self.read_line(), 2.0)
            if data.startswith(b"wadc"):  # Ignore the echo
                data: bytes = await asyncio.wait_for(self.read_line(), 2.0)
            wadc = parse_number(data)
            if wadc < 0 or wadc > 0xFFFF:
                raise CommunicationsError(f"wadc read: {hdr}")

            self.writer.write(b"rv\r")
            await self.writer.drain()
            rv = await self.read_multiple_lines(total=5.0, first=2.0, tail=1.0)
            if rv[0].startswith(b"rv"):  # Ignore the echo
                del rv[0]
            if not rv:
                raise CommunicationsError("no version response")
            for line in rv:
                if line.startswith(b"ERROR"):
                    raise CommunicationsError(f"invalid rv response: {rv}")
                elif line.startswith(b"Serial Number:"):
                    self.set_serial_number(line[14:].strip())
                elif line.startswith(b"FW Ver:"):
                    self.set_firmware_version(line[7:].strip())

            ts = time.gmtime()
            self.writer.write(f"rtc,{ts.tm_hour:02}:{ts.tm_min:02}:{ts.tm_sec:02}\r".encode('ascii'))
            await self.writer.drain()
            data: bytes = await asyncio.wait_for(self.read_line(), 2.0)
            if data.startswith(b"rtc,"):  # Ignore the echo
                data: bytes = await asyncio.wait_for(self.read_line(), 2.0)
            if data != b"OK":
                raise CommunicationsError(f"set time response: {data}")

            self.writer.write(f"rtc,{ts.tm_year%100:02}/{ts.tm_mon:02}/{ts.tm_mday:02}\r".encode('ascii'))
            await self.writer.drain()
            data: bytes = await asyncio.wait_for(self.read_line(), 2.0)
            if data.startswith(b"rtc,"):  # Ignore the echo
                data: bytes = await asyncio.wait_for(self.read_line(), 2.0)
            if data != b"OK":
                raise CommunicationsError(f"set date response: {data}")

            self.writer.write(f"Log,{self._report_interval}\r".encode('ascii'))
            await self.writer.drain()

        # Flush the first record
        await self.drain_reader(0.5)
        await asyncio.wait_for(self.read_line(), self._report_interval * 3 + 5)

        # Process a valid record
        await self.communicate()

    async def communicate(self) -> None:
        line: bytes = await asyncio.wait_for(self.read_line(), self._report_interval + 5)
        if len(line) < 3:
            raise CommunicationsError

        fields = line.split(b',')
        try:
            (
                date_time, N, TDinlet, Tinlet, Uinlet,
                Tconditioner, Tinitiator, Tmoderator, Toptics, Theatsink, Tcase,
                PCTwick,
                _,  # Moderator set point
                TDgrowth, P, Q,
                _,  # Interval time, number of seconds elapsed in counting interval
                _,  # Corrected live time, as fraction of interval * 10000
                _,  # Measured dead time, as fraction of interval * 10000
                C, pulseInfo, flags,
                _,  # Flags description
                serial_number
            ) = fields
        except ValueError:
            raise CommunicationsError(f"invalid number of fields in {line}")
        try:
            (pulseHeight, _) = pulseInfo.split(b'.')
        except ValueError:
            raise CommunicationsError(f"invalid pulse information in {line}")

        parse_datetime_field(date_time, date_separator=b'/')

        self.data_TDinlet(parse_number(TDinlet))
        self.data_Tinlet(parse_number(Tinlet))
        self.data_Uinlet(parse_number(Uinlet))
        self.data_Tconditioner(parse_number(Tconditioner))
        self.data_Tinitiator(parse_number(Tinitiator))
        self.data_Tmoderator(parse_number(Tmoderator))
        self.data_Toptics(parse_number(Toptics))
        self.data_Theatsink(parse_number(Theatsink))
        self.data_Tcase(parse_number(Tcase))
        self.data_TDgrowth(parse_number(TDgrowth))
        self.data_P(parse_number(P))
        self.data_C(parse_number(C))
        self.data_PCTwick(parse_number(PCTwick))
        self.data_Vpulse(parse_number(pulseHeight))

        Qinstrument = self.data_Qinstrument(flow_ccm_to_lpm(parse_number(Q)))
        Q = self.data_Q(Qinstrument)

        N = parse_number(N)
        N *= Qinstrument / Q
        self.data_N(N)

        if serial_number:
            self.set_serial_number(serial_number)

        parse_flags_bits(flags, self.bit_flags)

        self.instrument_report()
