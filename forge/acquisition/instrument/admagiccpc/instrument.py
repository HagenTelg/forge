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
    MODEL = "MAGIC 200"
    DISPLAY_LETTER = "C"
    TAGS = frozenset({"aerosol", "cpc", _INSTRUMENT_TYPE})
    SERIAL_PORT = {'baudrate': 115200}

    def __init__(self, context: StreamingContext):
        super().__init__(context)

        self._report_interval: int = int(context.config.get('REPORT_INTERVAL', default=1))

        self.data_N = self.input("N")
        self.data_Clower = self.input("Clower")
        self.data_Cupper = self.input("Cupper")
        self.data_Q = self.input("Q")
        self.data_Qinstrument = self.input("Qinstrument")
        self.data_P = self.input("P")
        self.data_PD = self.input("PD")
        self.data_V = self.input("V")

        self.data_Tinlet = self.input("Tinlet")
        self.data_Tconditioner = self.input("Tconditioner")
        self.data_Tinitiator = self.input("Tinitiator")
        self.data_Tmoderator = self.input("Tmoderator")
        self.data_Toptics = self.input("Toptics")
        self.data_Theatsink = self.input("Theatsink")
        self.data_Tpcb = self.input("Tpcb")
        self.data_Tcabinet = self.input("Tcabinet")
        self.data_Uinlet = self.input("Uinlet")
        self.data_TDinlet = self.input("TDinlet")

        if not self.data_N.comment and self.data_Q.comment:
            self.data_N.comment = self.data_Q.comment

        self.bit_flags: typing.Dict[int, Instrument.Notification] = dict()
        self.instrument_report = self.report(
            self.variable_number_concentration(self.data_N, code="N"),
            self.variable_sample_flow(self.data_Q, code="Q",
                                      attributes={'C_format': "%5.3f"}),
            self.variable_air_pressure(self.data_P, "pressure", code="P",
                                       attributes={'long_name': "absolute pressure"}),
            self.variable_delta_pressure(self.data_PD, "orifice_pressure_drop", code="Pd",
                                         attributes={
                                             'long_name': "pressure difference across the flow monitoring orifice",
                                             'C_format': "%4.1f",
                                         }),
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
            self.variable_temperature(self.data_Tpcb, "pcb_temperature", code="T6",
                                      attributes={'long_name': "temperature of the PCB"}),
            self.variable_temperature(self.data_Tcabinet, "cabinet_temperature", code="T7",
                                      attributes={'long_name': "temperature inside the cabinet"}),
            self.variable_air_rh(self.data_Uinlet, "inlet_humidity", code="Uu",
                                 attributes={'long_name': "relative humidity at the instrument inlet"}),
            self.variable_air_dewpoint(self.data_TDinlet, "inlet_dewpoint", code="TDu",
                                       attributes={'long_name': "dewpoint calculated from inlet temperature and humidity"}),

            flags=[
                self.flag_bit(self.bit_flags, 0x0001, "conditioner_temperature_out_of_range", is_warning=True),
                self.flag_bit(self.bit_flags, 0x0002, "initiator_temperature_out_of_range", is_warning=True),
                self.flag_bit(self.bit_flags, 0x0004, "moderator_temperature_out_of_range", is_warning=True),
                self.flag_bit(self.bit_flags, 0x0008, "optics_temperature_out_of_range", is_warning=True),
                self.flag_bit(self.bit_flags, 0x0010, "laser_off", is_warning=True),
                self.flag_bit(self.bit_flags, 0x0020, "pump_off", is_warning=True),
                self.flag_bit(self.bit_flags, 0x0040, "rh_data_stale"),
                self.flag_bit(self.bit_flags, 0x0080, "i2c_communication_error"),
                self.flag_bit(self.bit_flags, 0x0100, "rh_sensor_error", is_warning=True),
                self.flag_bit(self.bit_flags, 0x0200, "overheat", is_warning=True),
                # 0x0400 reserved
                self.flag_bit(self.bit_flags, 0x0800, "moderator_in_absolute_mode"),
                self.flag_bit(self.bit_flags, 0x1000, "water_pump_activated"),
                self.flag_bit(self.bit_flags, 0x2000, "invalid_flash_record"),
                self.flag_bit(self.bit_flags, 0x4000, "flash_full"),
                self.flag_bit(self.bit_flags, 0x8000, "fram_data_invalid"),
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

            self.writer.write(b"poll\r")
            await self.writer.drain()
            data: bytes = await asyncio.wait_for(self.read_line(), 2.0)
            if data.startswith(b"poll"):  # Ignore the echo
                data: bytes = await asyncio.wait_for(self.read_line(), 2.0)
            fields = data.split(b",")
            if len(fields) == 1:
                N = parse_number(fields[0])
                if N < 0.0:
                    raise CommunicationsError(f"invalid concentration: {N}")
            elif len(fields) == 9:
                N = parse_number(fields[0])
                if N < 0.0:
                    raise CommunicationsError(f"invalid concentration: {N}")
                Q = parse_number(fields[7])
                if Q < 0.0:
                    raise CommunicationsError(f"invalid flow: {Q}")
                P = parse_number(fields[8])
                if P < 10.0 or P > 1500.0:
                    raise CommunicationsError(f"invalid pressure: {P}")
            else:
                raise CommunicationsError(f"invalid poll response: {data}")

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
                date_time, N, TDinlet,
                Tinlet, Uinlet, Tconditioner, Tinitiator, Tmoderator, Toptics, Theatsink, Tpcb, Tcabinet,
                V, PD, P, Q,
                _,  # Interval time, number of seconds elapsed in counting interval
                _,  # Corrected live time, as fraction of interval * 10000
                _,  # Measured dead time, as fraction of interval * 10000
                Clower, Cupper, flags,
                _,  # Flags description
                serial_number
            ) = fields
        except ValueError:
            raise CommunicationsError(f"invalid number of fields in {line}")

        parse_datetime_field(date_time, date_separator=b'/')

        self.data_TDinlet(parse_number(TDinlet))
        self.data_Tinlet(parse_number(Tinlet))
        self.data_Uinlet(parse_number(Uinlet))
        self.data_Tconditioner(parse_number(Tconditioner))
        self.data_Tinitiator(parse_number(Tinitiator))
        self.data_Tmoderator(parse_number(Tmoderator))
        self.data_Toptics(parse_number(Toptics))
        self.data_Theatsink(parse_number(Theatsink))
        self.data_Tpcb(parse_number(Tpcb))
        self.data_Tcabinet(parse_number(Tcabinet))
        self.data_V(parse_number(V))
        self.data_PD(parse_number(PD))
        self.data_P(parse_number(P))
        self.data_Clower(parse_number(Clower))
        self.data_Cupper(parse_number(Cupper))

        Qinstrument = self.data_Qinstrument(flow_ccm_to_lpm(parse_number(Q)))
        Q = self.data_Q(Qinstrument)

        N = parse_number(N)
        N *= Qinstrument / Q
        self.data_N(N)

        if serial_number:
            self.set_serial_number(serial_number)

        parse_flags_bits(flags, self.bit_flags)

        self.instrument_report()
