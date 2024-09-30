import typing
import asyncio
import logging
import time
import re
import enum
import serial
from math import nan, isfinite, exp, log
from collections import deque
from forge.tasks import wait_cancelable
from forge.units import flow_m3s_to_lpm, mass_ng_to_ug, flow_lpm_to_m3s
from ..streaming import StreamingInstrument, StreamingContext, CommunicationsError
from ..parse import parse_number, parse_date_and_time, parse_flags_bits
from ..array import ArrayInput

_LOGGER = logging.getLogger(__name__)
_INSTRUMENT_TYPE = __name__.split('.')[-2]

_SERIAL_NUMBER = re.compile(br"SERIAL\s*NUMBER\s*(?:-\s*)?(\d+)", flags=re.IGNORECASE)
_FIRMWARE_VERSION = re.compile(br"THERMO.*v(\d\S*)", flags=re.IGNORECASE)
_PARAMETERS_VERSIONS = re.compile(br"THERMO.*v([\d.,]+).*SERIAL\s*NUMBER\s*(?:-\s*)?(\d+)", flags=re.IGNORECASE)
_PF12_START = re.compile(rb"\d{2,4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}(?:\s+-?\d+(?:\.\d*)?){9}")
_PF12_END = re.compile(rb"\d+(?:\s+-?\d+(?:\.\d*)?){5}")
_SIGMA_BC = re.compile(rb"SIGMA\s+BC:\s*(\d+(?:\.\d*)?)")


class Instrument(StreamingInstrument):
    INSTRUMENT_TYPE = _INSTRUMENT_TYPE
    MANUFACTURER = "Thermo"
    MODEL = "MAAP"
    DISPLAY_LETTER = "A"
    TAGS = frozenset({"aerosol", "absorption", _INSTRUMENT_TYPE})
    SERIAL_PORT = {'baudrate': 9600, 'parity': serial.PARITY_EVEN, 'bytesize': serial.SEVENBITS, 'stopbits': serial.STOPBITS_TWO}

    DEFAULT_SPOT_SIZE = 200.0
    DEFAULT_EBC_EFFICIENCY = 6.6

    class _EBCUnits(enum.Enum):
        Unknown = 0
        ug = 1
        ng = 2

        @staticmethod
        def parse(s) -> "Instrument._EBCUnits":
            if isinstance(s, str):
                s = s.lower()
                if s == "u" or s == "ug" or s == "ug/m3" or s == "ugm3":
                    return Instrument._EBCUnits.ug
                elif s == "n" or s == "ng" or s == "ng/m3" or s == "ngm3":
                    return Instrument._EBCUnits.ng
            return Instrument._EBCUnits.Unknown

    class _IntensityFilteringPoint:
        def __init__(self, time: float, If0: float, Ip0: float):
            self.time = time
            self.If0 = If0
            self.Ip0 = Ip0

    def __init__(self, context: StreamingContext):
        super().__init__(context)

        self._advance_transmittance: float = float(context.config.get('ADVANCE_TRANSMITTANCE', default=0.7))
        self._advance_hours: int = int(context.config.get('ADVANCE_HOURS', default=100))
        self.wavelength: float = float(context.config.get('WAVELENGTH', default=670))

        self._address: typing.Optional[int] = context.config.get('ADDRESS')
        if self._address is not None:
            self._address = int(self._address)
            if self._address < 0:
                self._address = None

        config_spot_size = context.config.get('SPOT')
        if config_spot_size is not None:
            self.spot_size: float = float(config_spot_size)
        else:
            self.spot_size: float = self.DEFAULT_SPOT_SIZE

        self._configured_ebc_efficiency: typing.Optional[float] = context.config.get('EBC_EFFICIENCY')
        self._reported_ebc_efficiency: typing.Optional[float] = None

        self._ebc_units: "Instrument._EBCUnits" = self._EBCUnits.Unknown
        units = context.config.get('EBC_UNITS')
        if units:
            self._ebc_units = self._EBCUnits.parse(units)
        self._identified_ebc_units: "Instrument._EBCUnits" = self._ebc_units
        self._identified_unit_uncertain: bool = False

        self._prior_volume_time: typing.Optional[float] = None
        self._prior_absorption_time: typing.Optional[float] = None
        self._prior_In: typing.Optional[float] = None
        self._have_high_precision_flow: bool = False
        self._have_intensity_transmittance: typing.Optional[float] = None
        self._have_calculated_absorption: bool = False

        self._intensity_filtering_interval: float = float(context.config.get('INTENSITY_FILTER_INTERVAL', default=10 * 60))
        self._intensity_filter_points: typing.Deque["Instrument._IntensityFilteringPoint"] = deque()

        self.data_Bac = self.input("Bac")
        self.data_Ba = self.input("Ba")
        self.data_X = self.input("X")
        self.data_In = self.input("In", send_to_bus=False)
        self.data_Ir = self.input("Ir")
        self.data_If = self.input("If")
        self.data_Ip = self.input("Ip")
        self.data_Is135 = self.input("Is135")
        self.data_Is165 = self.input("Is165")
        self.data_SSA = self.input("SSA")
        self.data_Tsample = self.input("Tsample")
        self.data_Thead = self.input("Thead")
        self.data_Tsystem = self.input("Tsystem")
        self.data_P = self.input("P")
        self.data_PDorifice = self.input("PDorifice")
        self.data_PDvacuum = self.input("PDvacuum")
        self.data_Q = self.input("Q")
        self.data_Qinstrument = self.input("Qinstrument", send_to_bus=False)
        self.data_Ld = self.input("Ld", send_to_bus=False)
        self.data_PCT = self.input("PCT")

        self._wavelength_arrays: typing.Dict[Instrument.Input, ArrayInput] = {
            i: self.input_array(i.name + "_", send_to_bus=False) for i in [
                self.data_Bac,
                self.data_Ba,
                self.data_X,
                self.data_Ir,
                self.data_If,
                self.data_Ip,
                self.data_Is135,
                self.data_Is165,
                self.data_SSA,
            ]
        }

        self.data_error_status = self.persistent("error_status")

        if not self.data_Ld.field.comment and self.data_Q.field.comment:
            self.data_Ld.field.comment = self.data_Q.field.comment
        if not self.data_Ba.field.comment and self.data_Q.field.comment:
            self.data_Ba.field.comment = self.data_Q.field.comment
        if config_spot_size is not None:
            self.data_Ld.field.add_comment(context.config.comment('SPOT'))

        def at_stp(s: Instrument.Variable):
            s.data.use_standard_pressure = True
            s.data.use_standard_temperature = True
            return s

        self.data_wavelength = self.persistent("wavelength", save_value=False, send_to_bus=False)
        self.data_wavelength([self.wavelength])
        dimension_wavelength = self.dimension_wavelength(self.data_wavelength)
        self.bit_flags: typing.Dict[int, Instrument.Notification] = dict()
        self.flag_spot_advancing = self.flag_bit(self.bit_flags, 0x000001, "spot_advancing")
        self.instrument_report = self.report(
            at_stp(self.variable_ebc(self._wavelength_arrays[self.data_X], dimension_wavelength, code="X")),
            at_stp(self.variable_absorption(self._wavelength_arrays[self.data_Bac], dimension_wavelength, code="Bac")),

            at_stp(self.variable_absorption(self._wavelength_arrays[self.data_Ba], dimension_wavelength,
                                            "uncorrected_light_absorption", code="Ba", attributes={
                'long_name': "uncorrected light absorption coefficient at STP",
                'standard_name': None,
            })),

            self.variable_transmittance(self._wavelength_arrays[self.data_Ir], dimension_wavelength, code="Ir"),
            self.variable_array(self._wavelength_arrays[self.data_If], dimension_wavelength, "reference_intensity",
                                code="If", attributes={
                'long_name': "reference detector signal",
                'C_format': "%7.2f",
            }),
            self.variable_array(self._wavelength_arrays[self.data_Ip], dimension_wavelength, "forward_intensity",
                                code="Ip", attributes={
                'long_name': "forward detector signal",
                'C_format': "%7.2f",
            }),
            self.variable_array(self._wavelength_arrays[self.data_Is135], dimension_wavelength,
                                "backscatter_135_intensity", code="Is1", attributes={
                'long_name': "135 degree backscatter detector signal",
                'C_format': "%7.2f",
            }),
            self.variable_array(self._wavelength_arrays[self.data_Is165], dimension_wavelength,
                                "backscatter_165_intensity", code="Is2", attributes={
                'long_name': "165 degree backscatter detector signal",
                'C_format': "%7.2f",
            }),

            self.variable_array(self._wavelength_arrays[self.data_SSA], dimension_wavelength,
                                "single_scattering_albedo", code="ZSSA", attributes={
                'long_name': "model result scattering / extinction of the aerosol-filter layer",
                'C_format': "%8.6f"
            }),

            at_stp(self.variable_sample_flow(self.data_Q, code="Q", attributes={'C_format': "%6.3f"})),
            at_stp(self.variable_rate(self.data_Ld, "path_length_change", code="Ld", attributes={
                'long_name': "change in path sample path length (flow/area)",
                'units': "m",
                'C_format': "%7.4f",
            })),

            self.variable_air_temperature(self.data_Tsample, "sample_temperature", code="T1"),
            self.variable_temperature(self.data_Thead, "measurement_head_temperature", code="T2",
                                      attributes={'long_name': "measuring head temperature"}),

            self.variable_temperature(self.data_Tsystem, "system_temperature", code="T3",
                                      attributes={'long_name': "system temperature"}),

            self.variable_air_pressure(self.data_P, "sample_pressure", code="P",
                                       attributes={'long_name': "sample pressure"}),

            self.variable_delta_pressure(self.data_PDorifice, "orifice_pressure_drop", code="Pd1", attributes={
                'long_name': "pressure drop from ambient to orifice face",
                'C_format': "%7.2f",
            }),
            self.variable_delta_pressure(self.data_PDvacuum, "vacuum_pressure_drop", code="Pd2", attributes={
                'long_name': "vacuum pressure pump drop across orifice",
                'C_format': "%7.2f",
            }),

            flags=[
                self.flag_spot_advancing,
                self.flag_bit(self.bit_flags, 0x000002, "zero"),
                self.flag_bit(self.bit_flags, 0x000008, "pump_off"),
                self.flag_bit(self.bit_flags, 0x000010, "manual_operation"),
                self.flag_bit(self.bit_flags, 0x000020, "calibration_enabled"),
                self.flag_bit(self.bit_flags, 0x000080, "mains_on"),
                self.flag_bit(self.bit_flags, 0x000100, "led_too_weak", is_warning=True),
                self.flag_bit(self.bit_flags, 0x010000, "memory_error", is_warning=True),
                self.flag_bit(self.bit_flags, 0x020000, "mechanical_error", is_warning=True),
                self.flag_bit(self.bit_flags, 0x040000, "pressure_error", is_warning=True),
                self.flag_bit(self.bit_flags, 0x080000, "flow_error", is_warning=True),
                self.flag_bit(self.bit_flags, 0x100000, "detector_error", is_warning=True),
                self.flag_bit(self.bit_flags, 0x200000, "temperature_error", is_warning=True),
            ],
        )

        self.instrument_state = self.change_event(
            self.state_unsigned_integer(self.data_error_status, "error_status", attributes={
                'long_name': "detailed error status bits",
                'C_format': "%016llX",
            }),
        )

        self.context.bus.connect_command('spot_advance', self._command_spot_advance)
        self._spot_advanced_queued: bool = False

        self.parameters_record = self.context.data.constant_record("parameters")
        self.parameters_record.float_attr("mass_absorption_efficiency", self, '_ebc_efficiency', attributes={
            'long_name': "the efficiency factor used to convert absorption coefficients into an equivalent black carbon",
            'units': "m2 g",
        })
        self.parameters_record.float_attr("spot_area", self, 'spot_size', attributes={
            'long_name': "sampling spot area",
            'C_format': "%5.2f",
            'units': "mm2",
        })
        self.parameter_raw = self.parameters_record.string("instrument_parameters", attributes={
            'long_name': "instrument format 8 record",
        })

    def _command_spot_advance(self, _) -> None:
        _LOGGER.debug("Received spot advance command")
        self._spot_advanced_queued = True

    @property
    def _ebc_efficiency(self) -> float:
        if self._configured_ebc_efficiency:
            return self._configured_ebc_efficiency
        if self._reported_ebc_efficiency:
            return self._reported_ebc_efficiency
        return self.DEFAULT_EBC_EFFICIENCY

    def _send_command(self, command: bytes) -> None:
        if self._address is not None:
            self.writer.write(b"%d:" % self._address)
        self.writer.write(command)
        self.writer.write(b"\r\n")

    async def _read_parameters(self) -> typing.List[bytes]:
        result_lines: typing.List[bytes] = list()

        async def read_start():
            while True:
                line = await self.read_line()
                matched = _PARAMETERS_VERSIONS.search(line)
                if not matched:
                    continue

                self.set_firmware_version(matched.group(1))
                sn = matched.group(2)
                # Unset values
                if sn != b"32767" and sn != b"32768":
                    self.set_serial_number(sn)

                result_lines.append(line)
                return

        async def read_tail():
            while True:
                line = await self.read_line()
                result_lines.append(line)
                if line.upper() == b"END":
                    return

        await wait_cancelable(read_start(), 10.0)
        try:
            await wait_cancelable(read_tail(), 30.0)
        except asyncio.TimeoutError:
            pass

        return result_lines

    async def start_communications(self) -> None:
        self._identified_ebc_units = self._ebc_units
        self._identified_unit_uncertain = False

        if self.writer:
            # Stop reports
            self._send_command(b"D 0")
            await self.writer.drain()
            await self.drain_reader(0.5)
            # Flush more and clear error counter
            self._send_command(b"N")
            await self.writer.drain()
            await self.drain_reader(0.5)

            # Set to english
            self._send_command(b"KK 1")
            await self.writer.drain()
            await self.drain_reader(0.5)

            # Read version
            self._send_command(b"v")
            version = await self.read_multiple_lines(total=5.0, first=5.0, tail=2.0)
            for line in version:
                matched = _SERIAL_NUMBER.search(line)
                if matched:
                    sn = matched.group(1)
                    # Unset values
                    if sn != b"32767" and sn != b"32768":
                        self.set_serial_number(sn)

                matched = _FIRMWARE_VERSION.search(line)
                if matched:
                    self.set_firmware_version(matched.group(1))

            # Read address
            self._send_command(b"?")
            line: bytes = await wait_cancelable(self.read_line(), 3.0)
            try:
                int(line)
            except (ValueError, OverflowError):
                raise CommunicationsError(f"invalid address {line}")

            # Set clock
            ts = time.gmtime()
            self._send_command(f"Z{ts.tm_year % 100:02}{ts.tm_mon:02}{ts.tm_mday:02}{ts.tm_hour:02}:{ts.tm_min:02}:{ts.tm_sec:02}".encode('ascii'))
            await self.writer.drain()
            await self.drain_reader(0.5)

            # Set output minutes
            self._send_command(b"d2 0")
            await self.writer.drain()
            await self.drain_reader(0.5)

            # Set output seconds
            self._send_command(b"d3 0")
            await self.writer.drain()
            await self.drain_reader(0.5)

            # Set 1-minute averaging
            self._send_command(b"K0 0")
            await self.writer.drain()
            await self.drain_reader(0.5)

            if self._advance_transmittance >= 0.0:
                tr = round(self._advance_transmittance * 100)
                if tr < 0:
                    tr = 0
                elif tr > 99:
                    tr = 99
                self._send_command(b"K1 %d" % tr)
                await self.writer.drain()
                await self.drain_reader(0.5)

            hours = self._advance_hours
            if hours < 1:
                hours = 1
            elif hours > 100:
                hours = 100
            self._send_command(b"K2 %d" % hours)
            await self.writer.drain()
            await self.drain_reader(0.5)

            # Disable fixed hour advance
            self._send_command(b"K3 0")
            await self.writer.drain()
            await self.drain_reader(0.5)

            # Set 0C STP
            self._send_command(b"KN 0")
            await self.writer.drain()
            await self.drain_reader(0.5)

            # Set reporting to STP
            self._send_command(b"KM 1")
            await self.writer.drain()
            await self.drain_reader(0.5)

            # Set output to parameters list
            self._send_command(b"D 8")
            await self.writer.drain()
            await self.drain_reader(0.5)

            # Read parameters
            self._send_command(b"P")
            try:
                parameters = await self._read_parameters()
            except (TimeoutError, asyncio.TimeoutError):
                await self.drain_reader(5.0)
                self._send_command(b"P")
                parameters = await self._read_parameters()
            self.parameter_raw("\n".join([l.decode('utf-8', 'backslashreplace') for l in parameters]))
            for l in parameters:
                matched = _SIGMA_BC.match(l)
                if matched:
                    self._reported_ebc_efficiency = parse_number(matched.group(1))

            # Read error counter
            self._send_command(b"N")
            line: bytes = await wait_cancelable(self.read_line(), 3.0)
            try:
                errors = int(line)
            except (ValueError, OverflowError):
                raise CommunicationsError(f"invalid error counter {line}")
            if errors != 0:
                raise CommunicationsError

            # Set to PF12
            self._send_command(b"D 12")

        # Flush out anything partial
        await self.drain_reader(2.0)
        await wait_cancelable(self.read_line(), 122.0)
        await wait_cancelable(self.read_line(), 122.0)

        # Process a valid record
        line = await wait_cancelable(self.read_line(), 122.0)
        self._process_pf12_complete(line)
        if self.writer:
            await self._interactive_poll()

    def _process_intensities(self, Ip0: float, If0: float) -> None:
        If = float(self.data_If)
        Ip = float(self.data_Ip)
        if If == 0.0:
            return
        self.data_In(Ip / If)

        # Normally filter start intensities shouldn't change, but (some?) MAAPs appear to report random drops.
        # So the filter here just takes the X minute max, reset if a filter change is detected.  This can
        # result in a Tr>1 near the start of the filter if the actual filter change is missed, but that's
        # better than the constant up/down the anomalous behavior produces.
        now = time.monotonic()
        cutoff = now - self._intensity_filtering_interval
        while len(self._intensity_filter_points) > 0 and self._intensity_filter_points[0].time <= cutoff:
            self._intensity_filter_points.popleft()
        self._intensity_filter_points.append(self._IntensityFilteringPoint(now, If0, Ip0))
        If0 = max([p.If0 for p in self._intensity_filter_points])
        Ip0 = max([p.Ip0 for p in self._intensity_filter_points])

        if Ip0 == 0.0 or If0 == 0.0:
            return

        self._have_intensity_transmittance = now
        self.data_Ir(float(self.data_In) / (Ip0 / If0))

    def _sample_volume_to_flow(self, volume: float) -> None:
        # This is supposedly m3/h, but it looks more like m3 per sample, so we'll use that instead

        now = time.monotonic()
        if not self._have_high_precision_flow and self._prior_volume_time is not None:
            dT = now - self._prior_volume_time
            Qinstrument = self.data_Qinstrument(flow_m3s_to_lpm(volume / dT))
            self.data_Q(Qinstrument)

        self._prior_volume_time = now

    def _process_pf12_start(self, line: bytes, last_field_space: bool = True) -> typing.Optional[bytes]:
        fields = line.split()
        try:
            (
                raw_data, raw_time,
                Ip, Is135, Is165, If,
                Ip0,
                _,  # Zero 135 signal
                _,  # Zero 165 signal
                If0,
                volume,
            ) = fields
        except ValueError:
            raise CommunicationsError(f"invalid number of fields in {line}")

        parse_date_and_time(raw_data, raw_time)
        self.data_Ip(parse_number(Ip))
        self.data_Is135(parse_number(Is135))
        self.data_Is165(parse_number(Is165))
        self.data_If(parse_number(If))

        Ip0 = parse_number(Ip0)
        If0 = parse_number(If0)
        self._process_intensities(Ip0, If0)

        last_field_extra = None
        if not last_field_space:
            # When we don't have a space separator, we can get things like "0.0060000"
            try:
                decimal = volume.index(b".")
            except ValueError:
                return None
            last_field_extra = volume[decimal+4:]
            volume = volume[:decimal+4]
        self._sample_volume_to_flow(parse_number(volume))
        return last_field_extra

    def _process_concentrations(self, X: bytes, X_uncorrected: bytes) -> None:
        X_digits = X
        X = parse_number(X)
        X_uncorrected_digits = X_uncorrected
        X_uncorrected = parse_number(X_uncorrected)

        def identify_ebc_units() -> "Instrument._EBCUnits":
            if b'.' in X_digits or b'.' in X_uncorrected_digits:
                # If it has decimal places, it's in ug
                self._identified_unit_uncertain = False
                return Instrument._EBCUnits.ug

            Ba = float(self.data_Ba)
            if isfinite(Ba) and Ba > 0.0 and self._have_calculated_absorption:
                calc_ebc_ug = Ba / self._ebc_efficiency
                calc_ebc_ng = calc_ebc_ug * 1E3
                if abs(calc_ebc_ug) >= 10.0 and abs(X_uncorrected) >= 10.0:
                    if abs(X_uncorrected - calc_ebc_ug) < abs(X_uncorrected - calc_ebc_ng):
                        self._identified_unit_uncertain = False
                        return self._EBCUnits.ug
                    else:
                        self._identified_unit_uncertain = False
                        return self._EBCUnits.ng

            if X > 1000.0 or X_uncorrected > 1000.0:
                # Sufficiently large and it's probably in ng
                self._identified_unit_uncertain = False
                return Instrument._EBCUnits.ug

            self._identified_unit_uncertain = True
            return Instrument._EBCUnits.ug

        ebc_units = self._identified_ebc_units
        if ebc_units == self._EBCUnits.Unknown or self._identified_unit_uncertain:
            ebc_units = identify_ebc_units()
        self._identified_ebc_units = ebc_units

        def apply_correction(raw: float) -> float:
            if ebc_units == self._EBCUnits.Unknown:
                return nan
            elif ebc_units == self._EBCUnits.ng:
                raw = mass_ng_to_ug(raw)
            raw *= float(self.data_Qinstrument.value) / float(self.data_Q)
            # Area appears to be fixed on the instrument, so apply a constant
            raw *= (self.spot_size / 200.0)
            return raw

        X = self.data_X(apply_correction(X))
        self._have_calculated_absorption = False
        # self.data_Ba(apply_correction(X_uncorrected) * self._ebc_efficiency)
        self.data_Bac(X * self._ebc_efficiency)

    def _process_pf12_end(self, line: bytes) -> None:
        fields = line.split()
        try:
            (
                _,  # Iterations to converge,
                SSA, LOD,
                _,  # Filter loading
                X, X_uncorrected
            ) = fields
        except ValueError:
            raise CommunicationsError(f"invalid number of fields in {line}")

        self.data_SSA(parse_number(SSA))

        LOD = parse_number(LOD)
        if not self._have_intensity_transmittance or self._have_intensity_transmittance < (time.monotonic() - self._intensity_filtering_interval):
            self.data_Ir(exp(LOD / 100.0))

        self._process_concentrations(X, X_uncorrected)

    def _process_pf12_complete(self, line: bytes) -> None:
        fields = line.split()
        try:
            (
                raw_data, raw_time,
                Ip, Is135, Is165, If,
                Ip0,
                _,  # Zero 135 signal
                _,  # Zero 165 signal
                If0,
                volume,
                _,  # Iterations to converge,
                SSA, LOD,
                _,  # Filter loading
                X, X_uncorrected
            ) = fields
        except ValueError:
            raise CommunicationsError(f"invalid number of fields in {line}")

        parse_date_and_time(raw_data, raw_time)
        self.data_Ip(parse_number(Ip))
        self.data_Is135(parse_number(Is135))
        self.data_Is165(parse_number(Is165))
        self.data_If(parse_number(If))

        Ip0 = parse_number(Ip0)
        If0 = parse_number(If0)
        self._process_intensities(Ip0, If0)

        self._sample_volume_to_flow(parse_number(volume))

        self.data_SSA(parse_number(SSA))

        LOD = parse_number(LOD)
        if not self._have_intensity_transmittance or self._have_intensity_transmittance < (time.monotonic() - self._intensity_filtering_interval):
            self.data_Ir(exp(LOD / 100.0))

        self._process_concentrations(X, X_uncorrected)

    async def _interactive_poll(self) -> None:
        async def command_response(command: bytes, embedded_spaces: bool = False) -> bytes:
            self._send_command(command)
            response: bytes = await wait_cancelable(self.read_line(), 2.0)

            async def process_pf12_end(response: bytes) -> bytes:
                # Got the model result line, so wait for the actual response line
                pf12 = _PF12_END.fullmatch(response)
                if not pf12:
                    return response
                self._process_pf12_end(response)
                try:
                    response = await wait_cancelable(self.read_line(), 2.0)
                except (TimeoutError, asyncio.TimeoutError):
                    # Response got lost?
                    self._send_command(command)
                    response = await wait_cancelable(self.read_line(), 2.0)
                return response

            response = await process_pf12_end(response)

            # Not a PF12 response, so this is the actual response
            pf12 = _PF12_START.match(response)
            if not pf12:
                return response

            actual_response = response[pf12.end(0):]
            pf12_response = response[:pf12.end(0)]
            if not embedded_spaces:
                pf12_space = actual_response[:1].isspace()
                self._process_pf12_start(pf12_response.strip(), pf12_space)
                resend_command = not pf12_space
            else:
                extra = self._process_pf12_start(pf12_response.strip(), False)
                if extra:
                    # Anything not part of the PF12 component means stuff got concatenated, so try again
                    resend_command = True
                else:
                    resend_command = False

            # Can't distinguish response, so resend
            if resend_command:
                self._send_command(command)
                actual_response = await wait_cancelable(self.read_line(), 2.0)
                # Make sure it's not the PF12 tail
                actual_response = await process_pf12_end(actual_response)

            actual_response = actual_response.strip()
            return actual_response

        # Note that the manual is wrong for what these actually are, so
        # this is based on what firmware 1.33 was outputting
        self.data_Tsample(parse_number(await command_response(b"J0")))
        self.data_Thead(parse_number(await command_response(b"J1")))
        self.data_Tsystem(parse_number(await command_response(b"J2")))
        self.data_PDorifice(parse_number(await command_response(b"J3")))
        self.data_PDvacuum(parse_number(await command_response(b"J4")))
        self.data_P(parse_number(await command_response(b"J5")))
        Q = parse_number(await command_response(b"JK")) / 60.0
        # Qt = parse_number(await command_response(b"JN"))
        self.data_PCT(parse_number(await command_response(b"JM")) * (100.0 / 4096.0))
        status = await command_response(b"#", True)

        self._have_high_precision_flow = True
        Qinstrument = self.data_Qinstrument(Q)
        self.data_Q(Qinstrument)

        fields = status.split()
        try:
            (
                error_status1,
                error_status2,
                error_status3,
                error_status4,
                device_status
            ) = fields
        except ValueError:
            raise CommunicationsError(f"invalid number of fields in {status}")

        parse_flags_bits(device_status, self.bit_flags)
        if bool(self.flag_spot_advancing.source):
            self._intensity_filter_points.clear()
            self._have_intensity_transmittance = None

        try:
            error_status1 = int(error_status1, 16)
            if error_status1 < 0 or error_status1 > 0xFFFF:
                raise ValueError
            error_status2 = int(error_status2, 16)
            if error_status2 < 0 or error_status2 > 0xFFFF:
                raise ValueError
            error_status3 = int(error_status3, 16)
            if error_status3 < 0 or error_status3 > 0xFFFF:
                raise ValueError
            error_status4 = int(error_status4, 16)
            if error_status4 < 0 or error_status4 > 0xFFFF:
                raise ValueError
        except (ValueError, OverflowError):
            raise CommunicationsError("invalid error status")

        self.data_error_status(
            (error_status1 << 48) |
            (error_status2 << 32) |
            (error_status3 << 16) |
            (error_status4 << 0)
        )

    def _calculate_absorption(self) -> None:
        now = time.monotonic()
        t0 = self._prior_absorption_time
        self._prior_absorption_time = now

        if t0 is None:
            return

        Q = float(self.data_Q)
        if not isfinite(Q):
            return

        dQt = flow_lpm_to_m3s(Q) * (now - t0)
        Ld = self.data_Ld(dQt / (self.spot_size * 1E-6))

        In = float(self.data_In)
        In0 = self._prior_In
        self._prior_In = In
        if In0 is None or not isfinite(In) or not isfinite(In0) or In <= 0.0 or In0 <= 0.0 or Ld <= 0.0:
            return

        self._have_calculated_absorption = True
        self.data_Ba((log(In0 / In) / Ld) * 1E6)

    async def communicate(self) -> None:
        self._have_high_precision_flow = False

        line: bytes = await wait_cancelable(self.read_line(), 122.0)
        if _PF12_START.match(line):
            # If we have the start of a PF12 then we have the complete contents as well: we don't have a query issued
            # so, we got the buffer flush
            self._process_pf12_complete(line)
        else:
            # Otherwise, this is the remaining part of the PF12 line (the model results)
            self._process_pf12_end(line)

        if self.writer:
            await self._interactive_poll()

            if self._spot_advanced_queued:
                _LOGGER.debug("Sending spot advance command")
                self._spot_advanced_queued = False
                self._send_command(b"F")

        self._calculate_absorption()

        for i, w in self._wavelength_arrays.items():
            w([float(i)])

        self.instrument_report()
