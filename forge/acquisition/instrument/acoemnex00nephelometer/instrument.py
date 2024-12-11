import typing
import logging
import asyncio
import time
import enum
import struct
import datetime
from math import nan, isfinite
from forge.tasks import wait_cancelable
from forge.units import ONE_ATM_IN_HPA, ZERO_C_IN_K
from ..streaming import StreamingInstrument, StreamingContext, CommunicationsError, BaseBusInterface
from ..state import Persistent, ChangeEvent
from ..variable import Input
from ..array import ArrayInput
from ..record import Report

_LOGGER = logging.getLogger(__name__)
_INSTRUMENT_TYPE = __name__.split('.')[-2]


class Instrument(StreamingInstrument):
    INSTRUMENT_TYPE = _INSTRUMENT_TYPE
    MANUFACTURER = "Acoem"
    MODEL = "NE-x00"
    DISPLAY_LETTER = "N"
    TAGS = frozenset({"aerosol", "scattering", _INSTRUMENT_TYPE})
    SERIAL_PORT = {'baudrate': 38400}

    WAVELENGTHS = (
        (450.0, "B"),
        (525.0, "G"),
        (635.0, "R"),
    )

    @enum.unique
    class CurrentOperation(enum.IntEnum):
        Normal = 0
        Zero = 1
        Span = 2
        ZeroAdjustCompleted = 3
        ZeroCheckCompleted = 4
        SpanCalibrationCompleted = 5
        SpanCheckCompleted = 6

    _CALIBRATION_STATES = (
        CurrentOperation.Zero,
        CurrentOperation.Span,
    )

    @enum.unique
    class MeasurementState(enum.IntEnum):
        Initializing = 0
        Dark = 1
        Reference = 2
        Sample = 3

    @enum.unique
    class SamplingMode(enum.IntEnum):
        Normal = 0
        Zero = 1
        Spancheck = 2

    @enum.unique
    class _Command(enum.IntEnum):
        Error = 0
        GetInstrumentType = 1
        GetVersion = 2
        Reset = 3
        GetValues = 4
        SetValues = 5
        GetLoggingConfig = 6
        GetLoggedData = 7

        def __str__(self):
            if self == Instrument._Command.Error:
                return "error (0)"
            elif self == Instrument._Command.GetInstrumentType:
                return "get instrument type (1)"
            elif self == Instrument._Command.GetVersion:
                return "get version (2)"
            elif self == Instrument._Command.Reset:
                return "reset (3)"
            elif self == Instrument._Command.GetValues:
                return "get values (4)"
            elif self == Instrument._Command.SetValues:
                return "set values (5)"
            elif self == Instrument._Command.GetLoggingConfig:
                return "get logging config (6)"
            elif self == Instrument._Command.GetLoggedData:
                return "get logged data (7)"
            return super().__str__()

    @enum.unique
    class _Error(enum.IntEnum):
        ChecksumFailed = 0
        InvalidCommandByte = 1
        InvalidParameter = 2
        InvalidMessageLength = 3
        MediaNotConnected = 8
        MediaBusy = 9

        def __str__(self):
            if self == Instrument._Error.ChecksumFailed:
                return "checksum failed"
            elif self == Instrument._Error.InvalidCommandByte:
                return "invalid command"
            elif self == Instrument._Error.InvalidParameter:
                return "invalid parameter"
            elif self == Instrument._Error.InvalidMessageLength:
                return "invalid message length"
            elif self == Instrument._Error.MediaNotConnected:
                return "media not connected"
            elif self == Instrument._Error.MediaBusy:
                return "media busy"
            return super().__str__()

    @enum.unique
    class _ConstructedParameter(enum.IntEnum):
        Bs = 1,
        Cr = 6
        Cs = 11
        Cd = 13
        Cf = 15
        CalM = 20
        CalC = 21
        DataSetIndex = 27

        def to_id(self, wavelength: typing.Union[int, float], angle: typing.Union[int, float]) -> int:
            return int(self) * 1_000_000 + int(round(wavelength)) * 1_000 + int(round(angle))

    @enum.unique
    class _Parameter(enum.IntEnum):
        Clock = 1
        NumberOfWavelengths = 4001
        NumberOfAngles = 4002
        WavelengthValuesStart = 4004
        AngleValuesStart = 4008
        CurrentOperation = 4035
        MeasurementState = 4036
        SampleTemperature = 5001  # K
        SamplePressure = 5002  # mBar
        SampleRH = 5003  # %
        ChassisTemperature = 5004  # K
        ChassisPressure = 5005  # mBar
        ChassisRH = 5006  # %
        Flow = 5010

    def __init__(self, context: StreamingContext):
        super().__init__(context)

        self._report_interval: float = float(context.config.get('REPORT_INTERVAL', default=1.0))
        self._serial_id: int = int(context.config.get('SERIAL_ID', default=0))
        self._set_instrument_time = bool(context.config.get('SET_INSTRUMENT_TIME', default=True))
        self._polar_mode: typing.Optional[bool] = context.config.get('POLAR')
        self._enable_backscatter: typing.Optional[bool] = context.config.get('BACKSCATTER')
        self._last_data_set_index: typing.Optional[int] = None
        self._sleep_time: float = 0.0

        self._instrument_backscatter: bool = self._enable_backscatter
        self._instrument_polar: bool = self._polar_mode

        self.data_Tsample = self.input("Tsample")
        self.data_Usample = self.input("Usample")
        self.data_Psample = self.input("Psample")
        self.data_Tchassis = self.input("Tchassis")
        self.data_Uchassis = self.input("Uchassis")
        self.data_Pchassis = self.input("Pchassis")
        self.data_Q = self.input("Q")
        self.data_Cd = self.input("Cd")

        self.data_current_operation = self.persistent_enum("current_operation", self.CurrentOperation,
                                                           send_to_bus=False)
        self.data_sampling = self.persistent_enum("sampling", self.SamplingMode, send_to_bus=False)

        self.data_Bs_wavelength: typing.List[Input] = list()
        self.data_Bbs_wavelength: typing.List[Input] = list()
        self.data_Bsn_wavelength: typing.List[ArrayInput] = list()
        self.data_Bsw_wavelength: typing.List[Persistent] = list()
        self.data_Bbsw_wavelength: typing.List[Persistent] = list()
        self.data_Bswd_wavelength: typing.List[Persistent] = list()
        self.data_Bbswd_wavelength: typing.List[Persistent] = list()
        self.data_Bsnw_wavelength: typing.List[Persistent] = list()
        self.data_Cs_wavelength: typing.List[Input] = list()
        self.data_Cbs_wavelength: typing.List[Input] = list()
        self.data_Csn_wavelength: typing.List[ArrayInput] = list()
        self.data_Cr_wavelength: typing.List[Input] = list()
        self.data_Cbr_wavelength: typing.List[Input] = list()
        self.data_Crn_wavelength: typing.List[ArrayInput] = list()
        self.data_Cf_wavelength: typing.List[Input] = list()
        self.data_Cd_wavelength: typing.List[Input] = list()
        for _, code in self.WAVELENGTHS:
            self.data_Bs_wavelength.append(self.input("Bs" + code))
            self.data_Bbs_wavelength.append(self.input("Bbs" + code))
            self.data_Bsn_wavelength.append(self.input_array("Bsn" + code, send_to_bus=False))
            self.data_Bsw_wavelength.append(self.persistent("Bsw" + code))
            self.data_Bbsw_wavelength.append(self.persistent("Bbsw" + code))
            self.data_Bswd_wavelength.append(self.persistent("Bswd" + code))
            self.data_Bbswd_wavelength.append(self.persistent("Bbswd" + code))
            self.data_Bsnw_wavelength.append(self.persistent("Bsnw" + code, send_to_bus=False))
            self.data_Cs_wavelength.append(self.input("Cs" + code))
            self.data_Cbs_wavelength.append(self.input("Cbs" + code))
            self.data_Csn_wavelength.append(self.input_array("Csn" + code, send_to_bus=False))
            self.data_Cr_wavelength.append(self.input("Cr" + code))
            self.data_Cbr_wavelength.append(self.input("Cbr" + code))
            self.data_Crn_wavelength.append(self.input_array("Crn" + code, send_to_bus=False))
            self.data_Cf_wavelength.append(self.input("Cf" + code))
            self.data_Cd_wavelength.append(self.input("Cd" + code))

        self.data_angle = self.persistent("angle", save_value=False, send_to_bus=False)
        self.data_angle([0.0, 90.0])
        self.data_wavelength = self.persistent("wavelength", save_value=False, send_to_bus=False)
        self.data_wavelength([wl for wl, _ in self.WAVELENGTHS])
        self.data_Bs = self.input_array("Bs")  # Sent to the bus because it has the zero data removed
        self.data_Bbs = self.input_array("Bbs")
        self.data_Bsn = self.input_array("Bsn", send_to_bus=False, dimensions=2)
        self.data_Bsw = self.persistent("Bsw", send_to_bus=False)
        self.data_Bbsw = self.persistent("Bbsw", send_to_bus=False)
        self.data_Bsnw = self.persistent("Bsnw", send_to_bus=False)
        self.data_Cs = self.input_array("Cs", send_to_bus=False)
        self.data_Cbs = self.input_array("Cbs", send_to_bus=False)
        self.data_Csn = self.input_array("Csn", send_to_bus=False, dimensions=2)
        self.data_Cr = self.input_array("Cr", send_to_bus=False)
        self.data_Cbr = self.input_array("Cbr", send_to_bus=False)
        self.data_Crn = self.input_array("Crn", send_to_bus=False, dimensions=2)
        self.data_Cf = self.input_array("Cf", send_to_bus=False)

        self.notify_zero = self.notification("zero")
        self.notify_spancheck = self.notification("spancheck")

        self.dimension_wavelength = self.dimension_wavelength(self.data_wavelength)
        self.bit_flags: typing.Dict[int, Instrument.Notification] = dict()

        self._instrument_stp_variables: typing.List[Instrument.Variable] = list()

        def at_instrument_stp(s: typing.Union[Instrument.Variable, Instrument.State]):
            self._instrument_stp_variables.append(s)
            s.data.use_standard_pressure = True
            s.data.use_standard_temperature = True
            return s

        self.instrument_report = self.report(
            at_instrument_stp(self.variable_total_scattering(self.data_Bs, self.dimension_wavelength, code="Bs")),
            at_instrument_stp(self.variable_back_scattering(self.data_Bbs, self.dimension_wavelength, code="Bbs")),

            at_instrument_stp(self.variable_sample_flow(self.data_Q, code="Q")),
            self.variable_air_pressure(self.data_Psample, "sample_pressure", code="P",
                                       attributes={'long_name': "measurement cell pressure"}),
            self.variable_pressure(self.data_Pchassis, "chassis_pressure", code="Px",
                                   attributes={'long_name': "chassis internal pressure"}),
            self.variable_air_temperature(self.data_Tsample, "sample_temperature", code="T",
                                          attributes={'long_name': "measurement cell temperature"}),
            self.variable_temperature(self.data_Tchassis, "chassis_temperature", code="Tx",
                                      attributes={'long_name': "chassis internal temperature"}),
            self.variable_air_rh(self.data_Usample, "sample_humidity", code="U",
                                 attributes={'long_name': "measurement cell relative humidity"}),
            self.variable_rh(self.data_Uchassis, "chassis_humidity", code="Ux",
                             attributes={'long_name': "chassis internal relative humidity"}),

            self.variable(self.data_Cd, "dark_counts", code="Cd", attributes={
                'long_name': "dark count rate",
                'units': "Hz",
                'C_format': "%7.0f"
            }),
            self.variable_array(self.data_Cs, self.dimension_wavelength, "scattering_counts", code="Cs", attributes={
                'long_name': "total scattering photon count rate",
                'units': "Hz",
                'C_format': "%7.0f"
            }),
            self.variable_array(self.data_Cbs, self.dimension_wavelength, "backscattering_counts", code="Cbs",
                                attributes={
                                    'long_name': "backwards hemispheric scattering photon count rate",
                                    'units': "Hz",
                                    'C_format': "%7.0f"
                                }),
            self.variable_array(self.data_Cf, self.dimension_wavelength, "reference_counts", code="Cf", attributes={
                'long_name': "reference shutter photon count rate",
                'units': "Hz",
                'C_format': "%7.0f"
            }),

            flags=[
                self.flag(self.notify_zero, 0x2000),
                self.flag(self.notify_spancheck),
            ],

            auxiliary_variables=(
                    [self.variable(s) for s in self.data_Bs_wavelength] +
                    [self.variable(s) for s in self.data_Bbs_wavelength] +
                    [self.variable(s) for s in self.data_Cs_wavelength] +
                    [self.variable(s) for s in self.data_Cbs_wavelength] +
                    [self.variable(s) for s in self.data_Cf_wavelength]
            )
        )
        self.polar_report: typing.Optional[Report] = None

        self.instrument_state = self.change_event(
            self.state_enum(self.data_sampling, attributes={
                'long_name': "sampling mode",
            }),
            self.state_enum(self.data_current_operation, attributes={
                'long_name': "current operation mode",
            }),
        )

        self.zero_state = self.change_event(
            at_instrument_stp(self.state_wall_total_scattering(self.data_Bsw, self.dimension_wavelength, code="Bsw")),
            at_instrument_stp(self.state_wall_back_scattering(self.data_Bbsw, self.dimension_wavelength, code="Bbsw")),
            name="zero",
        )
        self.polar_zero_state: typing.Optional[ChangeEvent] = None

        self.parameters_record = self.context.data.constant_record("parameters")

        self.parameter_cal_slope = self.parameters_record.array_float("calibration_slope", attributes={
            'long_name': "instrument calibration slope",
            'units': "Mm",
            'C_format': "%7.3f",
        })
        self.parameter_cal_offset = self.parameters_record.array_float("calibration_offset", attributes={
            'long_name': "instrument calibration offset",
            'units': "1",
            'C_format': "%.3e",
        })

        self._reboot_request: bool = False
        self.context.bus.connect_command('reboot', self.reboot)

    def _declare_polar(self) -> None:
        if self.polar_report:
            return

        self.dimension_angle = self.dimension(self.data_angle, "angle", code="Bn", attributes={
            'long_name': "polar scattering start angle (zero is total scattering)",
            'units': "degrees",
            'C_format': "%2.0f"
        })

        def at_instrument_stp(s: typing.Union[Instrument.Variable, Instrument.State]):
            self._instrument_stp_variables.append(s)
            s.data.use_standard_pressure = True
            s.data.use_standard_temperature = True
            return s

        self.polar_report = self.report(
            at_instrument_stp(self.variable_array(self.data_Bsn, [self.dimension_angle, self.dimension_wavelength],
                                  "polar_scattering_coefficient", code="Bsn", attributes={
                'long_name': "polar light scattering coefficient",
                'units': "Mm-1",
                'C_format': "%7.2f"
            })),
            self.variable_array(self.data_Csn, [self.dimension_angle, self.dimension_wavelength],
                                "polar_scattering_counts", code="Csn", attributes={
                'long_name': "polar scattering photon count rate",
                'units': "Hz",
                'C_format': "%7.0f"
            }),
        )

        var = self.state_measurement_array(self.data_Bsnw, [self.dimension_angle, self.dimension_wavelength],
                                           "polar_wall_scattering_coefficient", code="Bsnw", attributes={
                'long_name': "polar light scattering coefficient from wall signal",
                'units': "Mm-1",
                'C_format': "%7.2f"
            })
        self._instrument_stp_variables.append(var)
        var.data.use_standard_pressure = self._instrument_stp_variables[0].data.use_standard_pressure
        var.data.use_standard_temperature = self._instrument_stp_variables[0].data.use_standard_temperature
        self.polar_zero_state = self.change_event(
            var,
            name="polar_zero")
        self.polar_zero_state.data_record.standard_temperature = self.zero_state.data_record.standard_temperature
        self.polar_zero_state.data_record.standard_pressure = self.zero_state.data_record.standard_pressure

    def _find_angle(self, target: float) -> typing.Optional[int]:
        for angle in range(len(self.data_angle.value)):
            if abs(self.data_angle.value[angle] - target) < 5.0:
                return angle
        return None

    @property
    def _have_backscatter(self) -> bool:
        if self._enable_backscatter is not None:
            return bool(self._enable_backscatter)
        if self._instrument_backscatter is not None and not self._instrument_backscatter:
            return False
        return self._back_scattering_index is not None

    @property
    def _scatterings_valid(self) -> bool:
        if self.data_sampling.value != self.SamplingMode.Normal:
            return False
        if self.data_current_operation.value not in (
                self.CurrentOperation.Normal,
                self.CurrentOperation.ZeroAdjustCompleted,
                self.CurrentOperation.ZeroCheckCompleted,
                self.CurrentOperation.SpanCalibrationCompleted,
                self.CurrentOperation.SpanCheckCompleted,
        ):
            return False
        if bool(self.notify_zero):
            return False
        if bool(self.notify_spancheck):
            return False
        return True

    @property
    def _is_polar(self) -> bool:
        if self._polar_mode is not None:
            return bool(self._polar_mode)
        return bool(self._instrument_polar)

    @property
    def _total_scattering_index(self) -> typing.Optional[int]:
        if not self._is_polar:
            return 0
        return self._find_angle(0.0)

    @property
    def _total_scattering_angle(self) -> typing.Optional[float]:
        angle = self._total_scattering_index
        if angle is None:
            return None
        return self.data_angle.value[angle]

    @property
    def _back_scattering_index(self) -> typing.Optional[int]:
        if self._enable_backscatter is not None:
            if not bool(self._enable_backscatter):
                return None
        if not self._is_polar:
            if len(self.data_angle.value) < 2:
                return None
            return -1
        return self._find_angle(90.0)

    @property
    def _back_scattering_angle(self) -> typing.Optional[float]:
        angle = self._back_scattering_index
        if angle is None:
            return None
        return self.data_angle.value[angle]

    @staticmethod
    def _checksum(data: bytes) -> int:
        r = 0
        for b in data:
            r ^= b
        return r

    async def _send_packet(self, command: "Instrument._Command", payload: bytes = None) -> None:
        command = int(command)
        packet = struct.pack('>BBBBH', 0x02, self._serial_id, command, 0x03,
                             (len(payload) if payload else 0))
        if payload:
            packet += payload
        checksum = self._checksum(packet)
        self.writer.write(packet + struct.pack('>BB', checksum, 0x04))

    async def _receive_packet(self, command: typing.Optional["Instrument._Command"] = None) -> bytes:
        while True:
            header = await self.reader.readexactly(6)
            stx, serial_id, response, etx, data_length = struct.unpack('>BBBBH', header)
            if stx != 0x02:
                raise CommunicationsError(f"invalid STX in header {stx}")
            if etx != 0x03:
                raise CommunicationsError(f"invalid ETX in header {stx}")
            try:
                response = self._Command(response)
            except ValueError as e:
                raise CommunicationsError(f"invalid response type {response}") from e

            if data_length:
                data = await self.reader.readexactly(data_length)
            else:
                data = bytes()
            footer = await self.reader.readexactly(2)
            received_checksum, eot = struct.unpack('>BB', footer)
            if eot != 0x04:
                raise CommunicationsError(f"invalid EOT in footer {eot}")
            calculated_checksum = self._checksum(header + data)
            if received_checksum != calculated_checksum:
                raise CommunicationsError(
                    f"checksum mismatch, calculated {calculated_checksum:02X} but got {received_checksum:02X}")

            if self._serial_id != 0 and serial_id != self._serial_id:
                continue

            if response == self._Command.Error:
                if len(data) < 2:
                    raise CommunicationsError("error response too short")
                error_code = struct.unpack('>H', data[:2])[0]
                try:
                    error_code = self._Error(error_code)
                except ValueError as e:
                    raise CommunicationsError(f"recognized error code {error_code}") from e
                raise CommunicationsError(f"error reported: {error_code}")
            if command is not None and command != response:
                raise CommunicationsError(f"response type mismatch, expecting {command} but got {response}")

            return data

    async def _command_response(self, command: "Instrument._Command", payload: bytes = None) -> bytes:
        await self._send_packet(command, payload)
        return await wait_cancelable(self._receive_packet(), 4.0)

    async def _read_value(self, parameter: typing.Union["Instrument._Parameter", int], format: str = '>f'):
        data = await self._command_response(self._Command.GetValues, struct.pack('>I', int(parameter)))
        return struct.unpack(format, data)[0]

    async def _read_values(self, *parameters: typing.Union["Instrument._Parameter", int], format: str = 'f') -> "typing.Tuple[float, ...]":
        data = await self._command_response(
            self._Command.GetValues,
            struct.pack('>' + str(len(parameters)) + 'I', *([int(p) for p in parameters]))
        )
        return struct.unpack('>' + str(len(parameters)) + format, data)

    async def _set_value(self, parameter: "Instrument._Parameter", value: bytes) -> None:
        data = struct.pack('>I', int(parameter)) + value
        await self._send_packet(self._Command.SetValues, data)

    @staticmethod
    def _calculate_zero_change(prior: float, current: float) -> float:
        if prior is None or current is None:
            return nan
        if not isfinite(prior) or not isfinite(current):
            return nan
        return current - prior

    async def _set_time(self, always_set: bool = False) -> None:
        if not self._set_instrument_time and not always_set:
            return

        instrument_time_packed = await self._read_value(self._Parameter.Clock, '>I')
        second = instrument_time_packed & 0b111111
        minute = (instrument_time_packed >> 6) & 0b111111
        hour = (instrument_time_packed >> 12) & 0b11111
        day = (instrument_time_packed >> 17) & 0b11111
        month = (instrument_time_packed >> 22) & 0b1111
        year = ((instrument_time_packed >> 26) & 0b111111) + 2000
        try:
            instrument_time = datetime.datetime(
                year, month, day,
                hour, minute, second,
                tzinfo=datetime.timezone.utc
            ).timestamp()
        except ValueError:
            raise CommunicationsError("invalid instrument time bits (0x%08X)", instrument_time_packed)
        if self._set_instrument_time and (abs(instrument_time - time.time()) > 10.0 or always_set):
            ts = time.gmtime()
            local_time_packed = \
                (ts.tm_sec & 0b111111) | \
                ((ts.tm_min & 0b111111) << 6) | \
                ((ts.tm_hour & 0b11111) << (6 + 6)) | \
                ((ts.tm_mday & 0b11111) << (5 + 6 + 6)) | \
                ((ts.tm_mon & 0b1111) << (5 + 5 + 6 + 6)) | \
                (((ts.tm_year - 2000) & 0b111111) << (4 + 5 + 5 + 6 + 6))
            await self._set_value(self._Parameter.Clock, struct.pack('>I', local_time_packed))

    async def _read_calibration(self, deduplicate: bool = False) -> None:
        values = await self._read_values(
            *([
                  self._ConstructedParameter.CalM.to_id(wavelength, angle)
                  for wavelength, _ in self.WAVELENGTHS
                  for angle in self.data_angle.value
              ] + [
                  self._ConstructedParameter.CalC.to_id(wavelength, angle)
                  for wavelength, _ in self.WAVELENGTHS
                  for angle in self.data_angle.value
              ])
        )
        if len(values) != len(self.WAVELENGTHS)*len(self.data_angle.value)*2:
            raise CommunicationsError(f"invalid response length")

        cal_m = values[:len(self.WAVELENGTHS)*len(self.data_angle.value)]
        cal_c = values[len(self.WAVELENGTHS)*len(self.data_angle.value):]
        self.parameter_cal_slope(cal_m)
        self.parameter_cal_offset(cal_c)

        Bsnw: typing.List[typing.List[float]] = list()
        for angle in range(len(self.data_angle.value)):
            Bsnw.append([nan] * len(self.WAVELENGTHS))
        for angle in range(len(self.data_angle.value)):
            for wavelength in range(len(self.WAVELENGTHS)):
                M = cal_m[angle + wavelength*len(self.data_angle.value)]
                C = cal_c[angle + wavelength*len(self.data_angle.value)]
                if abs(M) < 1E-9:
                    continue
                Bsnw[angle][wavelength] = C / M

        for wavelength in range(len(self.data_Bsnw_wavelength)):
            self.data_Bsnw_wavelength[wavelength]([angle[wavelength] for angle in Bsnw],
                                                  oneshot=True, deduplicate=deduplicate)
        Bsnw = self.data_Bsnw([
            [c.value[angle] for c in self.data_Bsnw_wavelength] for angle in range(len(Bsnw))
        ], oneshot=True, deduplicate=deduplicate)

        angle = self._total_scattering_index
        if angle is not None and angle < len(Bsnw):
            for wavelength in range(len(self.WAVELENGTHS)):
                prior = self.data_Bsw_wavelength[wavelength].value
                current = self.data_Bsw_wavelength[wavelength](Bsnw[angle][wavelength],
                                                               oneshot=True, deduplicate=deduplicate)
                self.data_Bswd_wavelength[wavelength](self._calculate_zero_change(prior, current))
            self.data_Bsw([c.value for c in self.data_Bsw_wavelength],
                          oneshot=True, deduplicate=deduplicate)

        angle = self._back_scattering_index
        if angle is not None and angle < len(Bsnw):
            for wavelength in range(len(self.WAVELENGTHS)):
                prior = self.data_Bbsw_wavelength[wavelength].value
                current = self.data_Bbsw_wavelength[wavelength](Bsnw[angle][wavelength],
                                                                oneshot=True, deduplicate=deduplicate)
                self.data_Bbswd_wavelength[wavelength](self._calculate_zero_change(prior, current))
            self.data_Bbsw([c.value for c in self.data_Bbsw_wavelength],
                           oneshot=True, deduplicate=deduplicate)

    async def _read_data(self) -> bool:
        try:
            (Tsample, Psample, Usample, Tchassis, Pchassis, Uchassis, Q, Cd, *constructed) = await self._read_values(
                *([
                      self._Parameter.SampleTemperature,
                      self._Parameter.SamplePressure,
                      self._Parameter.SampleRH,
                      self._Parameter.ChassisTemperature,
                      self._Parameter.ChassisPressure,
                      self._Parameter.ChassisRH,
                      self._Parameter.Flow,
                      self._ConstructedParameter.Cd.to_id(self.WAVELENGTHS[0][0], self.data_angle.value[0]),
                  ] + [
                      self._ConstructedParameter.Bs.to_id(wavelength, angle)
                      for angle in self.data_angle.value
                      for wavelength, _ in self.WAVELENGTHS
                  ] + [
                      self._ConstructedParameter.Cr.to_id(wavelength, angle)
                      for angle in self.data_angle.value
                      for wavelength, _ in self.WAVELENGTHS
                  ] + [
                      self._ConstructedParameter.Cs.to_id(wavelength, angle)
                      for angle in self.data_angle.value
                      for wavelength, _ in self.WAVELENGTHS
                  ] + [
                      self._ConstructedParameter.Cf.to_id(wavelength, self.data_angle.value[0])
                      for wavelength, _ in self.WAVELENGTHS
                  ])
            )
            len_wavelength_angle = len(self.WAVELENGTHS) * len(self.data_angle.value)
            Bsn = constructed[:len_wavelength_angle]
            del constructed[:len_wavelength_angle]
            Crn = constructed[:len_wavelength_angle]
            del constructed[:len_wavelength_angle]
            Csn = constructed[:len_wavelength_angle]
            del constructed[:len_wavelength_angle]
            Cf = constructed[:len(self.WAVELENGTHS)]
            del constructed[:len(self.WAVELENGTHS)]
        except ValueError:
            raise CommunicationsError(f"invalid response length")

        try:
            (data_set_index, current_operation) = await self._read_values(
                self._ConstructedParameter.DataSetIndex.to_id(self.WAVELENGTHS[0][0], self.data_angle.value[0]),
                self._Parameter.CurrentOperation,
                format='I',
            )
            current_operation = self.CurrentOperation(current_operation)
        except ValueError:
            raise CommunicationsError(f"invalid response length")

        if self.data_current_operation.value in self._CALIBRATION_STATES and current_operation not in self._CALIBRATION_STATES:
            _LOGGER.debug("Processing calibration update")
            await self._read_calibration()

        self.data_current_operation(current_operation)
        self.data_Tsample(Tsample - ZERO_C_IN_K)
        self.data_Psample(Psample)
        self.data_Usample(Usample)
        self.data_Tchassis(Tchassis - ZERO_C_IN_K)
        self.data_Pchassis(Pchassis)
        self.data_Uchassis(Uchassis)
        self.data_Q(Q)
        self.data_Cd(Cd)

        if data_set_index == self._last_data_set_index:
            return False
        self._last_data_set_index = data_set_index

        def to_output(flat: typing.List[float]) -> typing.List[typing.List[float]]:
            return [
                flat[angle * len(self.WAVELENGTHS):(angle+1) * len(self.WAVELENGTHS)]
                for angle in range(len(self.data_angle.value))
            ]

        # Transform to angle major and B, G, R
        Bsn = to_output(Bsn)
        Crn = to_output(Crn)
        Csn = to_output(Csn)

        angle = self._total_scattering_index
        if angle is not None:
            for wavelength in range(len(self.data_Bs_wavelength)):
                self.data_Bs_wavelength[wavelength](Bsn[angle][wavelength])
            for wavelength in range(len(self.data_Cs_wavelength)):
                self.data_Cs_wavelength[wavelength](Csn[angle][wavelength])
            for wavelength in range(len(self.data_Cr_wavelength)):
                self.data_Cr_wavelength[wavelength](Crn[angle][wavelength])
        angle = self._back_scattering_index
        if angle < 0 and len(Bsn) == 1:
            angle = None
        if angle is not None:
            for wavelength in range(len(self.data_Bbs_wavelength)):
                self.data_Bbs_wavelength[wavelength](Bsn[angle][wavelength])
            for wavelength in range(len(self.data_Cbs_wavelength)):
                self.data_Cbs_wavelength[wavelength](Csn[angle][wavelength])
            for wavelength in range(len(self.data_Cbr_wavelength)):
                self.data_Cbr_wavelength[wavelength](Crn[angle][wavelength])

        for wavelength in range(len(self.data_Bsn_wavelength)):
            self.data_Bsn_wavelength[wavelength]([Bsn[angle][wavelength] for angle in range(len(Bsn))])
        for wavelength in range(len(self.data_Crn_wavelength)):
            self.data_Crn_wavelength[wavelength]([Crn[angle][wavelength] for angle in range(len(Crn))])
        for wavelength in range(len(self.data_Csn_wavelength)):
            self.data_Csn_wavelength[wavelength]([Csn[angle][wavelength] for angle in range(len(Csn))])
        for wavelength in range(len(self.data_Cf_wavelength)):
            self.data_Cf_wavelength[wavelength](Cf[wavelength])

        return True

    def _update_state(self) -> None:
        sampling_mode = self.SamplingMode.Normal

        if self.data_current_operation.value == self.CurrentOperation.Zero:
            sampling_mode = self.SamplingMode.Zero
            self.notify_zero(True)
            self.notify_spancheck(False)
        elif self.data_current_operation.value == self.CurrentOperation.Span:
            sampling_mode = self.SamplingMode.Spancheck
            self.notify_zero(True)
            self.notify_spancheck(False)
        else:
            self.notify_zero(False)
            self.notify_spancheck(False)

        self.data_sampling(sampling_mode)

    def _output_data(self) -> None:
        if self._scatterings_valid:
            self.data_Bs([float(c) for c in self.data_Bs_wavelength])
            if self._have_backscatter:
                self.data_Bbs([float(c) for c in self.data_Bbs_wavelength])
            self.data_Bsn([
                [wavelength.value[angle] for wavelength in self.data_Bsn_wavelength]
                for angle in range(len(self.data_angle.value))
            ])
        else:
            self.data_Bs([nan for _ in self.data_Bs_wavelength])
            if self._have_backscatter:
                self.data_Bbs([nan for _ in self.data_Bbs_wavelength])
            self.data_Bsn([
                [nan for _ in self.data_Bsn_wavelength]
                for _ in range(len(self.data_angle.value))
            ])

        self.data_Cf([float(v) for v in self.data_Cf_wavelength])
        self.data_Cs([float(v) for v in self.data_Cs_wavelength])
        self.data_Cr([float(v) for v in self.data_Cr_wavelength])
        if self._have_backscatter:
            self.data_Cbs([float(v) for v in self.data_Cbs_wavelength])
            self.data_Cbr([float(v) for v in self.data_Cbr_wavelength])

        self.data_Csn([
            [wavelength.value[angle] for wavelength in self.data_Csn_wavelength]
            for angle in range(len(self.data_angle.value))
        ])
        self.data_Crn([
            [wavelength.value[angle] for wavelength in self.data_Crn_wavelength]
            for angle in range(len(self.data_angle.value))
        ])

        self.instrument_report()
        if self.polar_report:
            self.polar_report()

    async def start_communications(self) -> None:
        if not self.writer:
            raise CommunicationsError
        self._instrument_polar = None

        await self.drain_reader(2.0)

        instrument_type = await self._command_response(self._Command.GetInstrumentType)
        if len(instrument_type) < 16:
            raise CommunicationsError("instrument type too short")
        model, variant, sub_type, measurement_range = struct.unpack('>IIII', instrument_type[:16])
        if model != 158:
            raise CommunicationsError(f"unsupported instrument model {model}")
        if variant > 0:
            if variant == 100:
                if self._instrument_polar is None:
                    self._instrument_polar = False
                if self._instrument_backscatter is None:
                    self._instrument_backscatter = False
                self.set_instrument_info('model', "NE-100")
            elif variant == 300:
                if self._instrument_polar is None:
                    self._instrument_polar = False
                self.set_instrument_info('model', "NE-300")
            elif variant == 400:
                if self._instrument_polar is None:
                    self._instrument_polar = True
                self.set_instrument_info('model', "NE-400")
            else:
                self.set_instrument_info('model', f"NE-{variant}")

        instrument_version = await self._command_response(self._Command.GetVersion)
        if len(instrument_version) < 8:
            raise CommunicationsError("instrument version too short")
        build_number, branch_number = struct.unpack('>II', instrument_version[:8])
        self.set_firmware_version(f"{build_number}-{branch_number}")

        number_of_angles = await self._read_value(self._Parameter.NumberOfAngles, '>I')
        if number_of_angles < 1 or number_of_angles > 20:
            raise CommunicationsError(f"invalid number of angles {number_of_angles}")
        angles: typing.List[float] = list(await self._read_values(*[
            int(self._Parameter.AngleValuesStart) + i for i in range(number_of_angles)
        ]))
        for a in angles:
            if a < 0.0 or a > 90.0:
                raise CommunicationsError(f"invalid angle {a}")
        self.data_angle(angles)

        current_operation = await self._read_value(self._Parameter.CurrentOperation, '>I')
        try:
            self.CurrentOperation(current_operation)
        except ValueError:
            raise CommunicationsError(f"invalid operation state {current_operation}")

        await self._read_calibration(True)

        await self._set_time(True)

        if self._instrument_polar is None:
            if len(angles) > 2:
                self._instrument_polar = True
            elif abs(angles[0]) >= 5.0:
                self._instrument_polar = True
            elif len(angles) == 2 and abs(angles[-1] - 90) >= 5.0:
                self._instrument_polar = True
            else:
                self._instrument_polar = False
        if self._is_polar:
            self._declare_polar()

        self._sleep_time = 0.0

    def reboot(self, _) -> None:
        self._reboot_request = True

    async def communicate(self) -> None:
        if not self.writer:
            raise CommunicationsError

        if self._reboot_request:
            await self._send_packet(self._Command.Reset, b"REALLY")
            await asyncio.sleep(5.0)
            raise CommunicationsError("instrument rebooting")

        if self._sleep_time > 0.0:
            await asyncio.sleep(self._sleep_time)
            self._sleep_time = 0.0
        begin_read = time.monotonic()

        updated = await self._read_data()
        self._update_state()
        if updated:
            self._output_data()

        await self._set_time()

        end_read = time.monotonic()
        self._sleep_time = self._report_interval - (end_read - begin_read)
