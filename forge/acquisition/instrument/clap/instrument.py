import typing
import asyncio
import logging
import struct
import re
from math import isfinite, nan, log
from forge.tasks import wait_cancelable
from forge.units import flow_lpm_to_m3s
from forge.solver import polynomial as polynomial_solve
from ..streaming import StreamingInstrument, StreamingContext, CommunicationsError, BaseBusInterface
from ..parse import parse_number, parse_flags_bits
from ..variable import Input
from ..array import ArrayInput
from ..state import Persistent
from .control import Control

_LOGGER = logging.getLogger(__name__)
_INSTRUMENT_TYPE = __name__.split('.')[-2]
_VALUE_EQUAL = re.compile(rb"[^=]*=\s*(.+)")
_TEN_DOT = re.compile(rb"10\.(\d+)")
_ALL_ZERO = re.compile(rb"0+")
_CALIBRATION_SPLIT = re.compile(rb"[:;,]")


class Instrument(StreamingInstrument):
    INSTRUMENT_TYPE = _INSTRUMENT_TYPE
    MANUFACTURER = "GML"
    MODEL = "CLAP"
    DISPLAY_LETTER = "W"
    TAGS = frozenset({"aerosol", "absorption", _INSTRUMENT_TYPE})
    SERIAL_PORT = {'baudrate': 57600}

    DEFAULT_SPOT_SIZE = 19.9

    WAVELENGTHS = (
        (467.0, "B"),
        (528.0, "G"),
        (652.0, "R"),
    )

    def __init__(self, context: StreamingContext):
        super().__init__(context)

        self.set_hardware_flow_calibration: typing.Optional[typing.List[float]] = None
        calibration = context.config.get('HARDWARE_FLOW_CALIBRATION')
        if calibration:
            self.set_hardware_flow_calibration = list()
            for c in calibration:
                self.set_hardware_flow_calibration.append(float(c))
        self.current_hardware_flow_calibration: typing.Optional[typing.List[float]] = self.set_hardware_flow_calibration

        self.spot_size: typing.List[float] = [self.DEFAULT_SPOT_SIZE] * 8
        config_spot_size = context.config.get('SPOT')
        if isinstance(config_spot_size, list):
            self.spot_size = [float(v) for v in config_spot_size]
            if len(self.spot_size) != 8:
                raise ValueError("invalid number of spots")
        elif config_spot_size is not None:
            config_spot_size = float(config_spot_size)
            self.spot_size = [config_spot_size] * 8

        intensity_details = bool(context.config.get('INTENSITY_DETAILS', default=False))

        self.data_Q = self.input("Q")
        self.data_Vflow = self.input("Vflow")
        self.data_Tsample = self.input("Tsample")
        self.data_Tcase = self.input("Tcase")
        self.data_Ld = self.input("Ld", send_to_bus=False)
        self.data_Fn = self.persistent('Fn')
        self.data_Ff = self.persistent('Ff')

        self.data_I_wavelength: typing.List[ArrayInput] = list()
        self.data_Ba_wavelength: typing.List[Input] = list()
        self.data_Ir_wavelength: typing.List[Input] = list()
        self.data_Ip_wavelength: typing.List[Input] = list()
        self.data_If_wavelength: typing.List[Input] = list()
        self.data_Iin0_wavelength: typing.List[Persistent] = list()
        self.data_Iinw0_wavelength: typing.List[Persistent] = list()
        for _, code in self.WAVELENGTHS:
            self.data_I_wavelength.append(self.input_array("I" + code, send_to_bus=intensity_details))
            self.data_Ba_wavelength.append(self.input("Ba" + code))
            self.data_Ir_wavelength.append(self.input("Ir" + code))
            self.data_Ip_wavelength.append(self.input("Ip" + code))
            self.data_If_wavelength.append(self.input("If" + code))
            self.data_Iin0_wavelength.append(self.persistent("Iin0" + code, send_to_bus=intensity_details))
            self.data_Iinw0_wavelength.append(self.persistent("Iinw0" + code, send_to_bus=intensity_details))

        self.data_wavelength = self.persistent("wavelength", save_value=False, send_to_bus=False)
        self.data_wavelength([wl for wl, _ in self.WAVELENGTHS])
        self.data_ID = self.input_array("ID", send_to_bus=intensity_details)
        self.data_Ba = self.input_array("Ba", send_to_bus=False)
        self.data_Ir = self.input_array("Ir", send_to_bus=False)
        self.data_Ip = self.input_array("Ip", send_to_bus=False)
        self.data_If = self.input_array("If", send_to_bus=False)

        self.data_In0 = self.persistent("In0", send_to_bus=intensity_details)
        self._Ir0: typing.Optional[typing.List[float]] = None

        self.notify_wait_spot_stability = self.notification('wait_spot_stability')
        self.notify_filter_baseline = self.notification('filter_baseline')
        self.notify_filter_change = self.notification('filter_change')
        self.notify_white_filter_change = self.notification('white_filter_change')
        self.notify_bypass_wait_spot_stability = self.notification('bypass_wait_spot_stability')
        self.notify_need_filter_change = self.notification('need_filter_change', is_warning=True)
        self.notify_need_white_filter_change = self.notification('need_white_filter_change', is_warning=True)
        self.notify_filter_was_not_white = self.notification('filter_was_not_white', is_warning=True)

        if self.set_hardware_flow_calibration:
            self.data_Q.field.add_comment(context.config.comment('HARDWARE_FLOW_CALIBRATION'))
        if not self.data_Ld.field.comment and self.data_Q.field.comment:
            self.data_Ld.field.comment = self.data_Q.field.comment
        if not self.data_Ba.field.comment and self.data_Q.field.comment:
            self.data_Ba.field.comment = self.data_Q.field.comment
        if config_spot_size is not None:
            self.data_Ld.field.add_comment(context.config.comment('SPOT'))

        self.variable_Q = self.variable_sample_flow(self.data_Q, code="Q", attributes={'C_format': "%6.3f"}).at_stp()

        dimension_wavelength = self.dimension_wavelength(self.data_wavelength)
        self.bit_flags: typing.Dict[int, Instrument.Notification] = dict()
        self.instrument_report = self.report(
            self.variable_absorption(self.data_Ba, dimension_wavelength, code="Ba").at_stp(),
            self.variable_transmittance(self.data_Ir, dimension_wavelength, code="Ir"),
            self.variable_Q,
            self.variable_air_temperature(self.data_Tsample, "sample_temperature", code="T1"),
            self.variable_temperature(self.data_Tcase, "case_temperature", code="T2",
                                      attributes={'long_name': "case temperature"}),
            self.variable_rate(self.data_Ld, "path_length_change", code="Ld", attributes={
                'long_name': "change in path sample path length (flow/area)",
                'units': "m",
                'C_format': "%7.4f",
            }).at_stp(),
            self.variable_array(self.data_Ip, dimension_wavelength, "sample_intensity", code="Ip", attributes={
                'long_name': "active spot sample intensity",
                'C_format': "%10.2f",
            }),
            self.variable_array(self.data_If, dimension_wavelength, "reference_intensity", code="If", attributes={
                'long_name': "active reference intensity",
                'C_format': "%10.2f",
            }),

            flags=[
                self.flag_bit(self.bit_flags, 0x0002, "flow_error", is_warning=True),
                self.flag_bit(self.bit_flags, 0x0100, "led_error", is_warning=True),
                self.flag_bit(self.bit_flags, 0x0200, "temperature_out_of_range", is_warning=True),
                self.flag_bit(self.bit_flags, 0x0400, "case_temperature_control_error"),

                self.flag(self.notify_filter_change, preferred_bit=0x0001),
                self.flag(self.notify_wait_spot_stability),
                self.flag(self.notify_filter_baseline),
                self.flag(self.notify_white_filter_change),
                self.flag(self.notify_bypass_wait_spot_stability),
                self.flag(self.notify_need_filter_change),
                self.flag(self.notify_need_white_filter_change),
                self.flag(self.notify_filter_was_not_white),
            ],

            auxiliary_variables=(
                [self.variable(w) for w in self.data_Ba_wavelength] +
                [self.variable_last_valid(w) for w in self.data_Ir_wavelength] +
                [self.variable(w) for w in self.data_Ip_wavelength] +
                [self.variable(w) for w in self.data_If_wavelength]
            ),
        )

        self.filter_state = self.change_event(
            self.state_unsigned_integer(self.data_Ff, "filter_id", code="Ff", attributes={
                'long_name': "filter identifier",
            }),
            self.state_unsigned_integer(self.data_Fn, "spot_number", code="Fn", attributes={
                'long_name': "active spot number",
            }),
            self.state_measurement_array(self.data_In0, dimension_wavelength, "spot_normalization", code="In", attributes={
                'long_name': "sample/reference intensity at spot sampling start",
                'units': "1",
                'C_format': "%9.7f",
            }),
        )

        self.parameters_record = self.context.data.constant_record("parameters")
        self.parameters_record.array_float_attr(
            "hardware_flow_calibration", self, 'current_hardware_flow_calibration',
            attributes={
                'long_name': "calibration polynomial applied to the raw voltage on the instrument to convert to lpm",
                'C_format': "%8.5f",
            }
        )
        if self.data_Q.calibration:
            self.parameters_record.array_float_attr(
                "acquisition_flow_calibration", self.data_Q, 'calibration',
                attributes={
                    'long_name': "calibration polynomial applied to the instrument reported flow",
                    'C_format': "%5.3f",
                }
            )
        self.parameters_record.array_float_attr(
            "spot_area", self, 'spot_size',
            attributes={
                'long_name': "sampling spot area",
                'C_format': "%5.2f",
                'units': "mm2",
            }
        )

        self.control = Control(self)

    @staticmethod
    def _parse_instrument_float(value: bytes) -> float:
        raw = bytes.fromhex(value.strip().decode('ascii'))
        f = struct.unpack('>f', raw)[0]
        if not isfinite(f):
            raise CommunicationsError(f"invalid number {value}")
        return f

    @staticmethod
    def _parse_instrument_int(value: bytes, maximum: int = None) -> int:
        try:
            value = int(value.strip(), 16)
            if value < 0:
                raise ValueError
            if maximum is not None and value > maximum:
                raise ValueError
            return value
        except (ValueError, OverflowError):
            raise CommunicationsError(f"invalid number {value}")

    class InstrumentState:
        def __init__(self, is_changing: bool, elapsed_seconds: int, Ff: int, Fn: int):
            self.is_changing = is_changing
            self.elapsed_seconds = elapsed_seconds
            self.Ff = Ff
            self.Fn = Fn

    async def _get_main_report(self) -> bytes:
        while True:
            line: bytes = await self.read_line()
            check = line.strip()
            if check.startswith(b'03'):
                return line

    async def process_record(self) -> "Instrument.InstrumentState":
        line: bytes = await wait_cancelable(self._get_main_report(), 3.0)
        if len(line) < 3:
            raise CommunicationsError

        fields = line.split(b',')
        try:
            (
                record_code, flags, elapsed_seconds, Ff, Fn,
                Q,
                _,  # Total volume
                Tcase, Tsample, *Iraw
            ) = fields
        except ValueError:
            raise CommunicationsError(f"invalid number of fields in {line}")
        if len(Iraw) != 40:
            raise CommunicationsError(f"invalid number of intensities in {line}")
        if record_code != b'03':
            raise CommunicationsError(f"invalid invalid record code in {line}")

        Q = parse_number(Q)
        self.data_Q(Q)
        self.data_Tcase(parse_number(Tcase))
        self.data_Tsample(parse_number(Tsample))
        if self.current_hardware_flow_calibration:
            center_voltage = 2.5
            Vflow = polynomial_solve(self.current_hardware_flow_calibration, Q, guess=center_voltage)
            if not Vflow:
                self.data_Vflow(nan)
            elif len(Vflow) == 1:
                self.data_Vflow(Vflow[0])
            else:
                best: typing.Optional[float] = None
                for v in Vflow:
                    if best is None or abs(best - center_voltage) > abs(v - center_voltage):
                        best = v
                self.data_Vflow(best)

        ID: typing.List[float] = list()
        I_wavelength: typing.List[typing.List[float]] = list()
        for i in range(len(self.WAVELENGTHS)):
            I_wavelength.append(list())
        for spot in range(10):
            try:
                ID.append(self._parse_instrument_float(Iraw[spot * 4]))
                # Instrument reports in R, G, B order
                I_wavelength[2].append(self._parse_instrument_float(Iraw[spot * 4 + 1]))
                I_wavelength[1].append(self._parse_instrument_float(Iraw[spot * 4 + 2]))
                I_wavelength[0].append(self._parse_instrument_float(Iraw[spot * 4 + 3]))
            except (struct.error, ValueError) as e:
                raise CommunicationsError(f"invalid spot {spot} intensity") from e

        self.data_ID(ID)
        for i in range(len(self.WAVELENGTHS)):
            self.data_I_wavelength[i](I_wavelength[i])

        flags = parse_flags_bits(flags, self.bit_flags)
        is_changing = (flags & 0x0001) != 0
        elapsed_seconds = self._parse_instrument_int(elapsed_seconds)
        Ff = self._parse_instrument_int(Ff)
        Fn = self._parse_instrument_int(Fn, 8)
        return self.InstrumentState(is_changing, elapsed_seconds, Ff, Fn)

    @staticmethod
    def _parse_ten_dot(data: bytes) -> typing.Optional[bytes]:
        matched = _VALUE_EQUAL.fullmatch(data.strip())
        if matched is None:
            raise CommunicationsError(f"invalid response {data}")
        sn = matched.group(1)
        matched = _ALL_ZERO.fullmatch(sn)
        if matched:
            return None
        matched = _TEN_DOT.fullmatch(sn)
        if matched:
            return matched.group(1)
        return sn

    @staticmethod
    def _parse_flow_calibration(data: bytes) -> typing.List[float]:
        matched = _VALUE_EQUAL.fullmatch(data.strip())
        if matched is None:
            raise CommunicationsError(f"invalid response {data}")
        fields = _CALIBRATION_SPLIT.split(matched.group(1).strip())
        if len(fields) > 4:
            raise CommunicationsError(f"invalid number of calibration coefficients in {data}")
        calibration: typing.List[float] = list()
        for f in fields:
            calibration.append(parse_number(f))
        return calibration

    @staticmethod
    def _flow_calibration_equal(a: typing.List[float], b: typing.List[float]) -> bool:
        for i in range(max(len(a), len(b))):
            ca = a[i] if i < len(a) else 0.0
            cb = b[i] if i < len(b) else 0.0
            delta = abs(ca - cb)
            norm = max(abs(ca), abs(cb))
            if norm < 1E-8:
                norm = 1.0
            delta /= norm
            if delta > 1E-5:
                return False
        return True

    async def start_communications(self) -> None:
        if self.writer:
            # Stop reports
            self.writer.write(b"\r\r\rmain\r\r")
            await self.writer.drain()
            await self.drain_reader(1.0)
            self.writer.write(b"main\r")
            await self.writer.drain()
            await self.drain_reader(1.0)
            self.writer.write(b"hide\r")
            await self.writer.drain()
            await self.drain_reader(1.0)

            # Change menu
            self.writer.write(b"cfg\r")
            await self.writer.drain()
            await self.drain_reader(1.0)

            # Read serial number
            self.writer.write(b"sn\r")
            data: bytes = await wait_cancelable(self.read_line(), 3.0)
            sn = self._parse_ten_dot(data)
            if sn:
                sn = sn.decode('ascii')
                self.set_serial_number(sn)
                self.control.apply_serial_number(sn)
            await self.drain_reader(0.5)

            # Read firmware version
            self.writer.write(b"fw\r")
            data: bytes = await wait_cancelable(self.read_line(), 3.0)
            fw = self._parse_ten_dot(data)
            if fw:
                self.set_firmware_version(fw)
            await self.drain_reader(1.0)

            # Change menu
            self.writer.write(b"main\r")
            await self.writer.drain()
            await self.drain_reader(1.0)
            self.writer.write(b"cal\r")
            await self.writer.drain()
            await self.drain_reader(1.0)

            # Read flow calibration
            self.writer.write(b"flow\r")
            data: bytes = await wait_cancelable(self.read_line(), 3.0)
            current_calibration = self._parse_flow_calibration(data)

            if self.set_hardware_flow_calibration and not self._flow_calibration_equal(
                    current_calibration, self.set_hardware_flow_calibration):
                original_calibration = current_calibration
                for t in range(5):
                    self.writer.write(b"flow=" +
                                      b",".join([(b"%.8e" % c) for c in self.set_hardware_flow_calibration]) +
                                      b"\r")
                    await self.writer.drain()
                    await self.drain_reader(1.0)

                    self.writer.write(b"flow\r")
                    data: bytes = await wait_cancelable(self.read_line(), 3.0)
                    current_calibration = self._parse_flow_calibration(data)
                    if self._flow_calibration_equal(current_calibration, self.set_hardware_flow_calibration):
                        self.context.bus.log(f"Hardware flow calibration changed.", {
                            "configured_calibration": self.set_hardware_flow_calibration,
                            "original_calibration": original_calibration,
                        })
                        _LOGGER.debug("Changed hardware flow calibration")
                        break

                    await self.drain_reader(1.0)
                else:
                    self.context.bus.log(f"Unable to change hardware flow calibration.", {
                        "configured_calibration": self.set_hardware_flow_calibration,
                        "read_calibration": current_calibration,
                        "original_calibration": original_calibration,
                    }, type=BaseBusInterface.LogType.ERROR)
                    _LOGGER.warning("Error applying configured hardware flow calibration")

            if current_calibration:
                self.variable_Q.data.attributes['hardware_calibration_polynomial'] = current_calibration
            self.current_hardware_flow_calibration = current_calibration

            # Change menu
            await self.drain_reader(1.0)
            self.writer.write(b"main\r")
            await self.writer.drain()
            await self.drain_reader(1.0)

            # Start unpolled
            self.writer.write(b"show\r")
            await self.writer.drain()

        # Flush the first record
        await self.drain_reader(0.5)
        await wait_cancelable(self.read_line(), 3.0)

        # Process a valid record
        state = await self.process_record()
        await self.control.communications_established(state)

    @staticmethod
    def active_spot_index(spot_index: int) -> typing.Tuple[int, int]:
        sample_index = spot_index
        if spot_index % 2 == 0:
            reference_index = 0
        else:
            reference_index = 9
        return sample_index, reference_index

    def _calculate_intensities(self, spot_index: int) -> None:
        sample_index, reference_index = self.active_spot_index(spot_index)

        for widx in range(len(self.data_Ip_wavelength)):
            self.data_Ip_wavelength[widx](float(self.data_I_wavelength[widx][sample_index]))
            self.data_If_wavelength[widx](float(self.data_I_wavelength[widx][reference_index]))

        self.data_Ip([float(c) for c in self.data_Ip_wavelength])
        self.data_If([float(c) for c in self.data_If_wavelength])

    def _invalid_intensities(self) -> None:
        for c in self.data_Ip_wavelength:
            c(nan)
        for c in self.data_If_wavelength:
            c(nan)
        self.data_Ip([nan for _ in self.data_Ip_wavelength])
        self.data_If([nan for _ in self.data_If_wavelength])

    def _calculate_path_length(self, spot_index: int, elapsed_seconds: float = 1.0) -> None:
        spot_area = spot_index <= len(self.spot_size) and self.spot_size[spot_index - 1] or nan
        Q = float(self.data_Q)
        if isfinite(spot_area) and isfinite(Q) and spot_area > 0.0:
            dQt = flow_lpm_to_m3s(Q) * elapsed_seconds
            self.data_Ld(dQt / (spot_area * 1E-6))
        else:
            self.data_Ld(nan)

    def _invalid_path_length(self) -> None:
        self.data_Ld(nan)

    def _calculate_transmittance(self) -> None:
        for widx in range(len(self.data_Ip_wavelength)):
            Ip = float(self.data_Ip_wavelength[widx])
            If = float(self.data_If_wavelength[widx])

            if isfinite(Ip) and isfinite(If) and If != 0.0:
                In = Ip / If
            else:
                In = nan

            if self.data_In0.value and widx < len(self.data_In0.value):
                In0 = self.data_In0.value[widx]
            else:
                In0 = nan

            if isfinite(In) and isfinite(In0) and In0 != 0.0:
                self.data_Ir_wavelength[widx](In / In0)
            else:
                self.data_Ir_wavelength[widx](nan)

        Ir = [float(c) for c in self.data_Ir_wavelength]
        self.data_Ir(Ir)
        self.control.handle_advance_spot(Ir)

    def _invalid_transmittance(self) -> None:
        for c in self.data_Ir_wavelength:
            c(nan)
        self.data_Ir([nan for _ in self.data_Ir_wavelength])

    def _calculate_absorption(self, log_values: bool = True) -> None:
        Ld = float(self.data_Ld)
        for widx in range(len(self.data_Ba_wavelength)):
            Ir = float(self.data_Ir_wavelength[widx])
            if self._Ir0 and widx < len(self._Ir0):
                Ir0 = self._Ir0[widx]
            else:
                Ir0 = nan

            if isfinite(Ld) and isfinite(Ir) and isfinite(Ir0) and Ld > 0.0 and Ir > 0.0 and Ir0 > 0.0:
                self.data_Ba_wavelength[widx]((log(Ir0 / Ir) / Ld) * 1E6)
            else:
                self.data_Ba_wavelength[widx](nan)

        if log_values:
            self.data_Ba([float(c) for c in self.data_Ba_wavelength])
        else:
            self.data_Ba([nan for _ in self.data_Ba_wavelength])

        self._Ir0 = [float(c) for c in self.data_Ir_wavelength]

    def _invalid_absorption(self) -> None:
        for c in self.data_Ba_wavelength:
            c(nan)
        self.data_Ba([nan for _ in self.data_Ba_wavelength])
        self._Ir0 = None

    async def apply_instrument_command(
            self, command: bytes,
            condition: typing.Callable[["Instrument.InstrumentState"], bool]) -> "Instrument.InstrumentState":
        self.writer.write(command)
        await self.writer.drain()
        await self.drain_reader(0.5)

        while True:
            await self.read_line()
            state = await self.process_record()
            if condition(state):
                return state

    async def communicate(self) -> None:
        state = await self.process_record()
        state = await self.control.process(state)

        self.data_Fn(state.Fn)
        self.data_Ff(state.Ff)

        if self.control.intensities_valid:
            self._calculate_intensities(self.control.active_spot_number)
        else:
            self._invalid_intensities()
        if self.control.path_length_valid:
            self._calculate_path_length(self.control.active_spot_number)
        else:
            self._invalid_path_length()
        if self.control.transmittance_valid:
            self._calculate_transmittance()
        else:
            self._invalid_transmittance()
        if self.control.absorption_valid:
            self._calculate_absorption(self.control.absorption_logged)
        else:
            self._invalid_absorption()
        self.instrument_report()

    async def run(self) -> typing.NoReturn:
        try:
            await super().run()
        finally:
            await self.control.shutdown()

