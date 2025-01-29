import typing
import asyncio
import logging
import time
from forge.tasks import wait_cancelable
from forge.units import flow_ccm_to_lpm
from ..streaming import StreamingInstrument, StreamingContext, CommunicationsError
from ..parse import parse_number, parse_datetime_field, parse_flags_bits
from .parameters import Parameters

_LOGGER = logging.getLogger(__name__)
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
        self._apply_parameters: typing.Optional[Parameters] = None
        if context.config.get('PARAMETERS'):
            self._apply_parameters = Parameters()
            self._apply_parameters.load(context.config.section('PARAMETERS'))

        self.active_parameters = Parameters()
        self.data_parameters = self.persistent("parameters", save_value=False)

        self.data_N = self.input("N")
        self.data_Ninstrument = self.input("Ninstrument", send_to_bus=False)
        self.data_C = self.input("C")
        self.data_Q = self.input("Q")
        self.data_Qinstrument = self.input("Qinstrument")
        self.data_P = self.input("P")
        self.data_Vpulse = self.input("Vpulse")
        self.data_PCTwick = self.input("PCTwick")
        self.data_Vpwr = self.input("Vpwr")
        self.data_PDflow = self.input("PDflow")
        self.data_Cwick = self.input("Cwick")

        self.data_Tinlet = self.input("Tinlet")
        self.data_Tconditioner = self.input("Tconditioner")
        self.data_Tinitiator = self.input("Tinitiator")
        self.data_Tmoderator = self.input("Tmoderator")
        self.data_Toptics = self.input("Toptics")
        self.data_Theatsink = self.input("Theatsink")
        self.data_Tcase = self.input("Tcase")
        self.data_Tboard = self.input("Tboard")
        self.data_Uinlet = self.input("Uinlet")
        self.data_TDinlet = self.input("TDinlet")
        self.data_TDgrowth = self.input("TDgrowth")

        if not self.data_N.field.comment and self.data_Q.field.comment:
            self.data_N.field.comment = self.data_Q.field.comment

        self.notify_sample_flow_out_of_range = self.notification("sample_flow_out_of_range", is_warning=True)
        self.sample_flow_out_of_range_valid: bool = True

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
            self.variable_temperature(self.data_Tboard, "board_temperature", code="T7",
                                      attributes={'long_name': "temperature of the PCB as measured by the differential pressure sensor"}),
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
                self.flag_bit(self.bit_flags, 0x0000001, "conditioner_temperature_out_of_range", is_warning=True),
                self.flag_bit(self.bit_flags, 0x0000002, "initiator_temperature_out_of_range", is_warning=True),
                self.flag_bit(self.bit_flags, 0x0000004, "moderator_temperature_out_of_range", is_warning=True),
                self.flag_bit(self.bit_flags, 0x0000008, "optics_temperature_out_of_range", is_warning=True),
                self.flag_bit(self.bit_flags, 0x0000010, "laser_off", is_warning=True),
                self.flag_bit(self.bit_flags, 0x0000020, "pump_off", is_warning=True),
                self.flag_bit(self.bit_flags, 0x0000040, "rh_data_stale"),
                self.flag_bit(self.bit_flags, 0x0000080, "i2c_communication_error"),
                self.flag_bit(self.bit_flags, 0x0000100, "rh_sensor_error", is_warning=True),
                self.flag_bit(self.bit_flags, 0x0000200, "overheat", is_warning=True),
                self.flag_bit(self.bit_flags, 0x0000400, "dry_wick", is_warning=True),
                self.flag_bit(self.bit_flags, 0x0000800, "fallback_humidifier_dewpoint", is_warning=True),
                self.flag_bit(self.bit_flags, 0x0001000, "dewpoint_calculation_error"),
                self.flag_bit(self.bit_flags, 0x0002000, "wick_sensor_out_of_range", is_warning=True),
                self.flag_bit(self.bit_flags, 0x0004000, "flash_full"),
                self.flag_bit(self.bit_flags, 0x0008000, "fram_data_invalid"),
                self.flag_bit(self.bit_flags, 0x0010000, "flash_size_error"),
                self.flag_bit(self.bit_flags, 0x0020000, "thermistor_fault", is_warning=True),
                self.flag(self.notify_sample_flow_out_of_range, 0x0040000),
                # 0x080000 wick sensor sampled
                self.flag_bit(self.bit_flags, 0x0100000, "i2c_multiplexer_error"),
                self.flag_bit(self.bit_flags, 0x0200000, "low_clock_battery"),
                self.flag_bit(self.bit_flags, 0x0400000, "clock_stopped"),
                self.flag_bit(self.bit_flags, 0x0800000, "differential_pressure_saturated", is_warning=True),
                self.flag_bit(self.bit_flags, 0x1000000, "laser_diode_fault", is_warning=True),
                # 0x2000000 sd card missing
                # 0x4000000 sd card not writing
            ],
        )

        # Definition for short form data, long form varies between firmware versions, so don't attempt to default it
        self._record_fields: typing.List[typing.Optional[typing.Callable[[bytes], typing.Any]]] = [
            lambda date_time: parse_datetime_field(date_time, date_separator=b'/'),
            lambda N: self.data_Ninstrument(parse_number(N)),
            lambda TDinlet: self.data_TDinlet(parse_number(TDinlet)),
            lambda Tinlet: self.data_Tinlet(parse_number(Tinlet)),
            lambda Uinlet: self.data_Uinlet(parse_number(Uinlet)),
            lambda Tconditioner: self.data_Tconditioner(parse_number(Tconditioner)),
            lambda Tinitiator: self.data_Tinitiator(parse_number(Tinitiator)),
            lambda Tmoderator: self.data_Tmoderator(parse_number(Tmoderator)),
            lambda Toptics: self.data_Toptics(parse_number(Toptics)),
            lambda Theatsink: self.data_Theatsink(parse_number(Theatsink)),
            lambda Tcase: self.data_Tcase(parse_number(Tcase)),
            lambda PCTwick: self.data_PCTwick(parse_number(PCTwick)),
            None,  # Moderator set point
            lambda TDgrowth: self.data_TDgrowth(parse_number(TDgrowth)),
            lambda P: self.data_P(parse_number(P)),
            lambda Q: self.data_Qinstrument(flow_ccm_to_lpm(parse_number(Q))),
            None,  # Interval time, number of seconds elapsed in counting interval
            None,  # Corrected live time, as fraction of interval * 10000
            None,  # Measured dead time, as fraction of interval * 10000
            lambda C: self.data_C(parse_number(C)),
            self._parse_pulse_info,
            self._parse_flags,
            None,  # Flags description
            lambda serial_number: self.set_serial_number(serial_number) if serial_number else None,
        ]
        self._have_wadc: bool = False

        self.parameters_record = self.context.data.constant_record("parameters")
        self.active_parameters.record(self.parameters_record)
        self.parameter_raw = self.parameters_record.string("instrument_parameters", attributes={
            'long_name': "raw responses to parameters read",
        })

        self._save_request: bool = False
        self.context.bus.connect_command('set_parameters', self.set_parameters)
        self.context.bus.connect_command('save_settings', self.save_settings)

        from ..tcp import TCPContext
        self._telnet_login = isinstance(context, TCPContext)

    async def _read_parameters(self) -> None:
        self.writer.write(b"sus\r")
        sus = await self.read_multiple_lines(total=5.0, first=2.0, tail=1.0)
        raw_parameters = "\n".join([l.decode('utf-8', 'backslashreplace') for l in sus])
        if sus[0].startswith(b"sus") or sus[0].startswith(b"su:"):
            del sus[0]
        self.active_parameters.parse_sus(sus)

        for t in (b"tcon", b"tini", b"tmod", b"topt"):
            self.writer.write(t + b"\r")
            resp = await self.read_multiple_lines(total=5.0, first=2.0, tail=1.0)
            raw_parameters += "\n\n"
            raw_parameters += "\n".join([l.decode('utf-8', 'backslashreplace') for l in resp])
            if resp[0].startswith(t) and b':' not in resp[0]:
                del resp[0]
            if b':' in resp[0]:
                resp[0] =  (resp[0].split(b':', 1))[1].strip()
            target = getattr(self.active_parameters, t.decode('ascii'), None)
            if target is None:
                target = self.active_parameters.Temperature()
                setattr(self.active_parameters, t.decode('ascii'), target)
            target.parse(t, resp)

        self.writer.write(b"tspid\r")
        tspid = await self.read_multiple_lines(total=5.0, first=2.0, tail=1.0)
        raw_parameters += "\n\n"
        raw_parameters += "\n".join([l.decode('utf-8', 'backslashreplace') for l in tspid])
        self.parameter_raw(raw_parameters)

    async def _set_pending_parameters(self) -> None:
        apply = self._apply_parameters
        self._apply_parameters = None
        commands = apply.set_commands()
        if not commands:
            return
        for cmd in commands:
            _LOGGER.debug(f"Setting parameter {cmd.decode('ascii')}")
            self.writer.write(cmd + b"\r")
            data: bytes = await wait_cancelable(self.read_line(), 2.0)
            if data.startswith(cmd.split(b',', 1)[0]) and b':' not in data:  # Ignore the echo
                data: bytes = await wait_cancelable(self.read_line(), 2.0)
            if b':' in data:
                data: bytes = (data.split(b':', 1))[1].strip()
            if data != b"OK":
                raise CommunicationsError(f"error setting parameter {cmd}: {data}")

    def _parse_power_supply_voltage(self, Vpwr: bytes) -> None:
        Vpwr = Vpwr.strip()
        if Vpwr.endswith(b'V'):
            Vpwr = Vpwr[:-1]
        self.data_Vpwr(parse_number(Vpwr))

    def _parse_pulse_info(self, pulse_info: bytes) -> None:
        try:
            (pulseHeight, _) = pulse_info.split(b'.')
        except ValueError:
            raise CommunicationsError(f"invalid pulse information")
        self.data_Vpulse(parse_number(pulseHeight))

    def _parse_flags(self, flags: bytes) -> None:
        flags = parse_flags_bits(flags, self.bit_flags)
        if self.sample_flow_out_of_range_valid and (flags & 0x040000) != 0:
            self.notify_sample_flow_out_of_range(True)
        else:
            self.notify_sample_flow_out_of_range(False)

    async def start_communications(self) -> None:
        if self.writer:
            if self._telnet_login:
                # Attempt a login
                self.writer.write(b"root\r")
                await self.writer.drain()
                await self.drain_reader(0.5)
                self.writer.write(b"\r")
                await self.writer.drain()
                await self.drain_reader(0.5)

            # Stop reports
            self.writer.write(b"\r\rlog,off\r")
            await self.writer.drain()
            await self.drain_reader(0.25)
            self.writer.write(b"log,0\r")
            await self.writer.drain()
            await self.drain_reader(0.25)
            self.writer.write(b"log,off\r")
            await self.writer.drain()
            await self.drain_reader(0.25)

            self.writer.write(b"logl,1\r")
            await self.writer.drain()
            await self.drain_reader(0.25)

            self.writer.write(b"logl,off\r")
            await self.writer.drain()
            await self.drain_reader(self._report_interval * 2.0 + 1.0)

            self.writer.write(b"hdr,0\r")
            await self.writer.drain()
            await self.drain_reader(0.25)

            self.writer.write(b"hdr\r")
            await self.writer.drain()
            hdr = await self.read_multiple_lines(total=5.0, first=2.0, tail=1.0)
            if hdr[0].startswith(b"hdr"):  # Ignore the echo
                del hdr[0]
            if not hdr:
                raise CommunicationsError("no header response")
            if hdr[0].isdigit():  # Ignore return setting of number of headers
                del hdr[0]
            if not hdr:
                raise CommunicationsError("no header response")

            self._record_fields.clear()
            have_concentration = False
            have_pulse_info = False
            have_terminal_status = False
            self._have_wadc = False
            for header_line in hdr:
                if header_line.endswith(b','):
                    header_line = header_line[:-1]
                for field_name in header_line.split(b','):
                    have_terminal_status = False
                    field_name = field_name.lower().strip()
                    if field_name == b"year time" or field_name == b"timestamp" or field_name == b"date_____time":
                        self._record_fields.append(lambda date_time: parse_datetime_field(date_time, date_separator=b'/'))
                    elif field_name == b"concentration" or field_name == b"conc":
                        have_concentration = True
                        self._record_fields.append(lambda N: self.data_Ninstrument(parse_number(N)))
                    elif field_name == b"dewpoint" or field_name == b"dewp":
                        self._record_fields.append(lambda TDinlet: self.data_TDinlet(parse_number(TDinlet)))
                    elif field_name == b"input t" or field_name == b"tin":
                        self._record_fields.append(lambda Tinlet: self.data_Tinlet(parse_number(Tinlet)))
                    elif field_name == b"input rh" or field_name == b"rhin":
                        self._record_fields.append(lambda Uinlet: self.data_Uinlet(parse_number(Uinlet)))
                    elif field_name == b"cond t" or field_name == b"tcon":
                        self._record_fields.append(lambda Tconditioner: self.data_Tconditioner(parse_number(Tconditioner)))
                    elif field_name == b"init t" or field_name == b"tini":
                        self._record_fields.append(lambda Tinitiator: self.data_Tinitiator(parse_number(Tinitiator)))
                    elif field_name == b"mod t" or field_name == b"tmod":
                        self._record_fields.append(lambda Tmoderator: self.data_Tmoderator(parse_number(Tmoderator)))
                    elif field_name == b"opt t" or field_name == b"topt":
                        self._record_fields.append(lambda Toptics: self.data_Toptics(parse_number(Toptics)))
                    elif field_name == b"heatsink t" or field_name == b"thsk":
                        self._record_fields.append(lambda Theatsink: self.data_Theatsink(parse_number(Theatsink)))
                    elif field_name == b"case t" or field_name == b"tcab":
                        self._record_fields.append(lambda Tcase: self.data_Tcase(parse_number(Tcase)))
                    elif field_name == b"wicksensor" or field_name == b"wick":
                        self._record_fields.append(lambda PCTwick: self.data_PCTwick(parse_number(PCTwick)))
                    elif field_name == b"modset" or field_name == b"mset":
                        # Moderator set point
                        self._record_fields.append(None)
                    elif field_name == b"humidifer exit dp" or field_name == b"hdp":
                        self._record_fields.append(lambda TDgrowth: self.data_TDgrowth(parse_number(TDgrowth)))
                    elif field_name == b"wadc" or field_name == b"wickadc":
                        self._have_wadc = True
                        self._record_fields.append(lambda Cwick: self.data_Cwick(parse_number(Cwick)))
                    elif field_name == b"board temperature":
                        self._record_fields.append(lambda Tboard: self.data_Tboard(parse_number(Tboard)))
                    elif field_name == b"power supply voltage" or field_name == b"psv":
                        self._record_fields.append(self._parse_power_supply_voltage)
                    elif field_name == b"diff. press":
                        self._record_fields.append(lambda PDflow: self.data_PDflow(parse_number(PDflow)))
                    elif field_name == b"abs. press." or field_name == b"pamb":
                        self._record_fields.append(lambda P: self.data_P(parse_number(P)))
                    elif field_name == b"flow (cc/min)" or field_name == b"flow":
                        self._record_fields.append(lambda Q: self.data_Qinstrument(flow_ccm_to_lpm(parse_number(Q))))
                    elif field_name == b"log interval":
                        # Interval time, number of seconds elapsed in counting interval
                        self._record_fields.append(None)
                    elif field_name == b"corrected live time" or field_name == b"livet":
                        # Corrected live time, as fraction of interval * 10000
                        self._record_fields.append(None)
                    elif field_name == b"measured dead time" or field_name == b"deadt":
                        # Measured dead time, as fraction of interval * 10000
                        self._record_fields.append(None)
                    elif field_name == b"raw counts" or field_name == b"pcnt":
                        self._record_fields.append(lambda C: self.data_C(parse_number(C)))
                    elif field_name == b"pulseheight.thres2" or field_name == b"dhr2.pctl" or field_name == b"pht2.%":
                        have_pulse_info = True
                        self._record_fields.append(self._parse_pulse_info)
                    elif field_name == b"pht3":
                        # 84% pulse height threshold
                        self._record_fields.append(None)
                    elif field_name == b"pht4":
                        # 16% pulse height threshold
                        self._record_fields.append(None)
                    elif field_name == b"status(hex code)":
                        self._record_fields.append(self._parse_flags)
                    elif field_name == b"status":
                        self._record_fields.append(self._parse_flags)
                        have_terminal_status = True
                    elif field_name == b"status(ascii)":
                        # Flags description
                        self._record_fields.append(None)
                    elif field_name == b"serial number":
                        self._record_fields.append(lambda serial_number: self.set_serial_number(serial_number) if serial_number and serial_number != b"0" else None)
                    else:
                        _LOGGER.debug(f"Unrecognized record field {field_name}")
                        self._record_fields.append(None)

            if not have_concentration or not have_pulse_info:
                raise CommunicationsError(f"header response: {b','.join(hdr)}")
            if have_terminal_status:
                self._record_fields.append(None)  # Flags description
                self._record_fields.append(lambda serial_number: self.set_serial_number(serial_number) if serial_number else None)

            self.writer.write(b"wadc\r")
            await self.writer.drain()
            data: bytes = await wait_cancelable(self.read_line(), 2.0)
            if data.startswith(b"wadc") and b':' not in data:  # Ignore the echo
                data: bytes = await wait_cancelable(self.read_line(), 2.0)
            if b':' in data:
                data: bytes = (data.split(b':', 1))[1].strip()
            try:
                wdac_start = data.index(b"adc=")
                data = data[wdac_start+4:]
            except ValueError:
                pass
            wadc = parse_number(data)
            if wadc < 0 or wadc > 1023:
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
                    sn = line[14:].strip()
                    if sn != b"0":
                        self.set_serial_number(sn)
                elif line.startswith(b"FW Ver:"):
                    ver = line[7:].strip()
                    self.set_firmware_version(ver)
                    # Randomly set and so not valid on this firmware
                    if ver == b'3.03a':
                        self.sample_flow_out_of_range_valid = False
                    else:
                        self.sample_flow_out_of_range_valid = True

            ts = time.gmtime()
            self.writer.write(f"rtc,{ts.tm_hour:02}:{ts.tm_min:02}:{ts.tm_sec:02}\r".encode('ascii'))
            await self.writer.drain()
            data: bytes = await wait_cancelable(self.read_line(), 2.0)
            if data.startswith(b"rtc,") and not data.endswith(b': OK'):  # Ignore the echo
                data: bytes = await wait_cancelable(self.read_line(), 2.0)
            if b':' in data:
                data: bytes = (data.split(b':'))[-1].strip()
            if data != b"OK":
                raise CommunicationsError(f"set time response: {data}")

            self.writer.write(f"rtc,{ts.tm_year%100:02}/{ts.tm_mon:02}/{ts.tm_mday:02}\r".encode('ascii'))
            await self.writer.drain()
            data: bytes = await wait_cancelable(self.read_line(), 2.0)
            if data.startswith(b"rtc,") and not data.endswith(b': OK'):  # Ignore the echo
                data: bytes = await wait_cancelable(self.read_line(), 2.0)
            if b':' in data:
                data: bytes = (data.split(b':'))[-1].strip()
            if data != b"OK":
                raise CommunicationsError(f"set date response: {data}")

            if self._apply_parameters:
                await self._set_pending_parameters()
            await self._read_parameters()
            self.data_parameters(self.active_parameters.persistent(), oneshot=True)

            self.writer.write(b"labels,0\r")
            await self.writer.drain()
            await self.drain_reader(0.25)

            self.writer.write(f"logl,{self._report_interval}\r".encode('ascii'))
            await self.writer.drain()
            await self.drain_reader(0.25)

            self.writer.write(b"logl,on\r")
            await self.writer.drain()
            try:
                data: bytes = await wait_cancelable(self.read_line(), self._report_interval * 3 + 5)
                if b'command not found' in data:
                    raise ValueError
                if data.startswith(b'logl'):
                    data: bytes = await wait_cancelable(self.read_line(), self._report_interval * 3 + 5)
                    if b'command not found' in data:
                        raise ValueError
            except (asyncio.TimeoutError, ValueError):
                self.writer.write(f"log,{self._report_interval}\r".encode('ascii'))
                await self.writer.drain()
                await self.drain_reader(0.25)

        # Flush the first record
        await self.drain_reader(0.5)
        await wait_cancelable(self.read_line(), self._report_interval * 3 + 5)

        # Process a valid record
        await self.communicate()

    async def _apply_updates(self) -> None:
        if not self.writer:
            return
        if not self._apply_parameters and not self._save_request:
            return

        self.writer.write(b"log,0\r")
        await self.writer.drain()
        await self.drain_reader(0.25)
        self.writer.write(b"log,off\r")
        await self.writer.drain()
        await self.drain_reader(0.25)

        self.writer.write(b"logl,0\r")
        await self.writer.drain()
        await self.drain_reader(0.25)
        self.writer.write(b"logl,off\r")
        await self.writer.drain()
        await self.drain_reader(self._report_interval * 2.0 + 1.0)

        if self._apply_parameters:
            await self._set_pending_parameters()
            await self._read_parameters()
            self.data_parameters(self.active_parameters.persistent(), oneshot=True)

        if self._save_request:
            _LOGGER.debug("Saving settings")
            self._save_request = False
            self.writer.write(b"svs\r")
            await self.writer.drain()
            await self.drain_reader(5.0)

        self.writer.write(f"logl,{self._report_interval}\r".encode('ascii'))
        await self.writer.drain()
        await self.drain_reader(0.25)

        self.writer.write(b"logl,on\r")
        await self.writer.drain()

        await self.drain_reader(0.5)
        await wait_cancelable(self.read_line(), self._report_interval * 3 + 5)

    def set_parameters(self, parameters: typing.Dict[str, typing.Any]) -> None:
        if not isinstance(parameters, dict):
            return
        to_set = Parameters()
        to_set.load(parameters)
        if self._apply_parameters:
            self._apply_parameters.overlay(to_set)
        else:
            self._apply_parameters = to_set

    def save_settings(self, _) -> None:
        self._save_request = True

    async def communicate(self) -> None:
        await self._apply_updates()

        line: bytes = await wait_cancelable(self.read_line(), self._report_interval + 5)
        if len(line) < 3:
            raise CommunicationsError

        fields = line.split(b',')
        if len(fields) != len(self._record_fields):
            raise CommunicationsError(f"invalid number of fields in {line}")
        for field_index in range(len(fields)):
            handler = self._record_fields[field_index]
            if handler:
                handler(fields[field_index])

        Qinstrument = self.data_Qinstrument.value
        Q = self.data_Q(Qinstrument)

        N = self.data_Ninstrument.value
        N *= Qinstrument / Q
        self.data_N(N)

        if not self._have_wadc:
            PCTwick = self.data_PCTwick.value
            wmin = self.active_parameters.wmin
            wmax = self.active_parameters.wmax
            if wmin is not None and wmax is not None:
                Cwick = wmax + PCTwick * ((wmin - wmax) / 100.0)
                self.data_Cwick(Cwick)

        self.instrument_report()
