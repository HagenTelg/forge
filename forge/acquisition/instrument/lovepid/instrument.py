import typing
import asyncio
import logging
import time
from enum import Enum
from math import nan, isfinite
from forge.tasks import wait_cancelable
from forge.acquisition import LayeredConfiguration
from ..streaming import StreamingInstrument, StreamingContext, CommunicationsError
from ..flexio import AnalogInput, AnalogOutput
from ..state import State

_LOGGER = logging.getLogger(__name__)
_INSTRUMENT_TYPE = __name__.split('.')[-2]


def _parse_address(address: typing.Union[str, int]) -> int:
    if isinstance(address, str):
        address = int(address, 16)
    else:
        address = int(address)
    if address <= 0 or address >= 0x3FF:
        raise ValueError(f"Address {address:X} out of range")
    if address == 0x100 or address == 0x200 or address == 0x300:
        raise ValueError(f"Address {address:X} is reserved")
    return address


def _display_format(digits: int) -> str:
    if digits == 0:
        return "%4.0f"
    return f"%5.{digits}f"


class Instrument(StreamingInstrument):
    INSTRUMENT_TYPE = _INSTRUMENT_TYPE
    MANUFACTURER = "Love"
    MODEL = "PID"
    DISPLAY_LETTER = "P"
    TAGS = frozenset({"aerosol", _INSTRUMENT_TYPE})
    SERIAL_PORT = {'baudrate': 9600, 'rs485': True}

    DEFAULT_ADDRESSES = [0x31, 0x32, 0x33, 0x34]

    class _AnalogInput(AnalogInput):
        def __init__(self, name: str, config: LayeredConfiguration, inp: StreamingInstrument.Input,
                     controller: "Instrument._Controller"):
            super().__init__(name, config, inp)
            self.controller = controller
            self.attributes['address'] = f"{controller.address:X}"
            if controller.decimal_digits is not None:
                self.attributes['C_format'] = _display_format(controller.decimal_digits)

        @classmethod
        def construct(cls, instrument: "Instrument", name: str,
                      config: LayeredConfiguration) -> typing.Optional["Instrument._AnalogInput"]:
            try:
                address = _parse_address(config.get('ADDRESS'))
            except (ValueError, TypeError):
                _LOGGER.warning(f"Invalid analog input address for {name}", exc_info=True)
                return None
            return cls(name, config, instrument.input(name), instrument._lookup_controller(address))

    class _AnalogOutput(AnalogOutput):
        REMEMBER_CHANGES = True

        def __init__(self, name: str, config: LayeredConfiguration, instrument: "Instrument",
                     controller: "Instrument._Controller"):
            super().__init__(name, config)
            self.instrument = instrument
            self.controller = controller
            self.attributes['address'] = f"{controller.address:X}"
            self.command_channel = controller.address
            if controller.decimal_digits is not None:
                self.attributes['C_format'] = _display_format(controller.decimal_digits)

        def command_received(self) -> None:
            self.instrument._output_changed_wake()

        @classmethod
        def construct(cls, instrument: "Instrument", name: str,
                      config: typing.Union[int, LayeredConfiguration]) -> typing.Optional["Instrument._AnalogOutput"]:
            try:
                address = _parse_address(config.get('ADDRESS'))
            except (TypeError, ValueError):
                _LOGGER.warning(f"Invalid analog output address for {name}", exc_info=True)
                return None
            return cls(name, config.section('OUTPUT'), instrument, instrument._lookup_controller(address))

        def __call__(self, value: float) -> None:
            if self.controller.manual_mode:
                value = round(value * 10.0) / 10.0
            elif self.controller.reported_decimal_digits is not None:
                scale_factor = ([1.0, 10.0, 100.0, 1000.0])[self.controller.reported_decimal_digits]
                value = round(value * scale_factor) / scale_factor
            super().__call__(value)

    class _StatusResponse:
        def __init__(self, payload: bytes):
            if len(payload) != 8:
                raise CommunicationsError(f"invalid status packet {payload}")
            try:
                status1 = int(payload[:2], 16)
            except (ValueError, TypeError):
                raise CommunicationsError(f"invalid status 1 in {payload}")
            try:
                status2 = int(payload[2:4], 16)
            except (ValueError, TypeError):
                raise CommunicationsError(f"invalid status 2 in {payload}")
            try:
                value = int(payload[4:])
            except (ValueError, TypeError):
                raise CommunicationsError(f"invalid value in {payload}")

            self.manual_mode: bool = (status1 & 0x80) != 0
            self.remote_mode: bool = (status1 & 0x40) != 0
            self.has_error: bool = (status1 & 0x10) != 0
            self.alarm1: bool = (status1 & 0x08) != 0
            self.alarm2: bool = (status1 & 0x04) != 0
            self.active_setpoint: int = (status1 & 0x03)
            self.activity_timeout: bool = (status2 & 0x80) != 0
            decimal_point: int = (status2 >> 4) & 0x3
            self.engineering_units: int = (status2 >> 1) & 0x3
            value_negative: bool = (status2 & 0x1) != 0

            value_scale: float = ([1.0, 0.1, 0.01, 0.001])[decimal_point]

            self.value: float = value * value_scale
            if value_negative:
                self.value = -self.value

    class _ErrorStatusResponse:
        def __init__(self, payload: bytes):
            if len(payload) != 4:
                raise CommunicationsError(f"invalid error status packet {payload}")
            try:
                status1 = int(payload[:2], 16)
            except (ValueError, TypeError):
                raise CommunicationsError(f"invalid status 1 in {payload}")
            try:
                status2 = int(payload[2:4], 16)
            except (ValueError, TypeError):
                raise CommunicationsError(f"invalid status 2 in {payload}")

            self.exception_code: int = status1 | (status2 << 8)

    class _ValueResponse:
        def __init__(self, payload: bytes):
            if len(payload) != 6:
                raise CommunicationsError(f"invalid value response {payload}")
            try:
                status = int(payload[:2], 16)
            except (ValueError, TypeError):
                raise CommunicationsError(f"invalid status 1 in {payload}")
            try:
                value = int(payload[2:])
            except (ValueError, TypeError):
                raise CommunicationsError(f"invalid value in {payload}")

            decimal_point: int = (status >> 4) & 0x3
            self.engineering_units: int = (status >> 1) & 0x3
            value_negative: bool = (status & 0x1) != 0

            value_scale: float = ([1.0, 0.1, 0.01, 0.001])[decimal_point]

            self.value: float = value * value_scale
            if value_negative:
                self.value = -self.value

    class _Controller:
        def __init__(self, address: int, index: int, config: typing.Optional[typing.Dict[str, typing.Any]] = None):
            self.address = address
            self.index = index

            self.manual_mode: typing.Optional[bool] = None
            if address & 0xF == 0x4:
                self.manual_mode = True
            if config:
                self.manual_mode = config.get("MANUAL_MODE", self.manual_mode)
            if self.manual_mode is not None:
                self.manual_mode = bool(self.manual_mode)

            self.decimal_digits: typing.Optional[int] = None
            if config:
                self.decimal_digits = config.get("DIGITS")
            if self.decimal_digits is not None:
                self.decimal_digits = int(self.decimal_digits)
                if self.decimal_digits < 0 or self.decimal_digits > 3:
                    raise ValueError(f"Invalid number of digits ({self.decimal_digits}) for controller {self.address:X}")

            self.reported_id: typing.Optional[str] = None
            self.reported_model: typing.Optional[str] = None
            self.reported_firmware: typing.Optional[str] = None
            self.reported_decimal_digits: typing.Optional[int] = None

            self.current_input: typing.Optional[float] = None
            self.current_setpoint: typing.Optional[float] = None

        @property
        def packet_identifier(self) -> bytes:
            if self.address <= 0xFF:
                return b'L'
            elif self.address <= 0x1FF:
                return b'O'
            elif self.address <= 0x2FF:
                return b'V'
            elif self.address <= 0x3FF:
                return b'E'
            raise RuntimeError

        @property
        def packet_address(self) -> bytes:
            return b"%02X" % (self.address & 0xFF)

        @staticmethod
        def packet_checksum(payload: typing.Iterable[int]) -> bytes:
            s = 0
            for v in payload:
                s += v
            return b"%02X" % (s & 0xFF)

        def assemble_packet(self, payload: typing.Union[bytes, bytearray]) -> bytes:
            packet = bytearray([0x02])
            packet += self.packet_identifier
            packet += self.packet_address
            packet += payload
            packet += self.packet_checksum(packet[2:])
            packet.append(0x03)
            return bytes(packet)

    class _Command(Enum):
        READ_STATUS = b"00"
        READ_ERROR_STATUS = b"05"
        READ_SP1 = b"0101"
        WRITE_SP1 = b"0200"
        READ_MANUAL_SP1 = b"0153"
        WRITE_MANUAL_SP1 = b"0266"
        READ_OUTPUT_PERCENT = b"0156"
        READ_DECIMAL_POINT = b"031A"
        WRITE_DECIMAL_POINT = b"025C"
        READ_EEPROM_SAVE = b"032C"
        DISABLE_EEPROM_SAVE = b"0441"
        SET_REMOTE_MODE = b"0400"
        DISABLE_MANUAL_MODE = b"0408"
        ENABLE_MANUAL_MODE = b"0409"
        READ_CONTROLLER_DATA = b"0700"
        READ_FIRMWARE_DATA = b"0702"

    def _lookup_controller(self, address: int) -> "Instrument._Controller":
        for controller in self.controllers:
            if address == controller.address:
                return controller
        raise ValueError(f"Controller {address:X} not found")

    def __init__(self, context: StreamingContext):
        super().__init__(context)

        self._relaxed_framing = bool(context.config.get('RELAXED_FRAMING', default=False))
        self._report_interval: float = float(context.config.get('REPORT_INTERVAL', default=1.0))
        self._sleep_time: float = 0.0

        command_delay: float = float(context.config.get('COMMAND_DELAY', default=0.1))
        if command_delay > 0.0:
            async def delay():
                await asyncio.sleep(command_delay)

            self._command_delay = delay
        else:
            async def delay():
                pass

            self._command_delay = delay

        self.data_value = self.input_array("value")
        self.data_raw = self.input_array("raw")
        self.data_setpoint = self.persistent("setpoint", save_value=False)
        self.data_control = self.input_array("control")

        self.controllers: typing.List[Instrument._Controller] = [
            self._Controller(self.DEFAULT_ADDRESSES[i], i) for i in range(len(self.DEFAULT_ADDRESSES))
        ]
        controller_config = context.config.get('CONTROLLERS')
        if isinstance(controller_config, list):
            self.controllers.clear()
            for controller in controller_config:
                idx = len(self.controllers)
                if isinstance(controller, dict):
                    address = _parse_address(controller['ADDRESS'])
                    self.controllers.append(self._Controller(address, idx, controller))
                else:
                    address = _parse_address(controller)
                    self.controllers.append(self._Controller(address, idx))
        elif isinstance(controller_config, dict):
            self.controllers.clear()
            for key, controller in controller_config.items():
                default_address = None
                try:
                    default_address = int(key, 16)
                except (TypeError, ValueError):
                    pass
                idx = len(self.controllers)
                address = _parse_address(controller.get('ADDRESS', default_address))
                self.controllers.append(self._Controller(address, idx, controller))
        if len(self.controllers) == 0:
            raise ValueError("No controllers configured")

        self._analog_inputs: typing.List[Instrument._AnalogInput] = self._AnalogInput.create_inputs(self)
        self._analog_outputs: typing.List[Instrument._AnalogOutput] = self._AnalogOutput.create_outputs(self,
                                                                                                        section='DATA')

        for no_cut in (self.data_value, self.data_raw, self.data_control):
            if no_cut.field.use_cut_size is None:
                no_cut.field.use_cut_size = False

        variable_names: typing.List[str] = [""] * len(self.controllers)
        self.instrument_info['variable'] = variable_names
        self.instrument_info['address'] = [controller.address for controller in self.controllers]
        variables: typing.List[Instrument.Variable] = list()
        for inp in self._analog_inputs:
            variable_names[inp.controller.index] = inp.name
            if inp.variable:
                variables.append(inp.variable)

        self.analog_input_report = self.report(
            *variables,

            self.variable_array(self.data_raw, name='controller_value', code='ZINPUTS', attributes={
                'long_name': "raw controller values",
                'C_format': "%5.2f"
            }),

            record=self.record_downstream(),
        )

        setpoints: typing.List[State] = list()
        for out in self._analog_outputs:
            if out.state is not None:
                setpoints.append(out.state)
        if setpoints:
            self.setpoint_changed = self.change_event(*setpoints, name='setpoint')
        else:
            self.setpoint_changed = None

        self._output_changed: typing.Optional[asyncio.Event] = None

    def _output_changed_wake(self) -> None:
        if not self._output_changed:
            return
        self._output_changed.set()

    async def _read_response(self, controller: "Instrument._Controller") -> bytes:
        if self._relaxed_framing:
            await self.reader.readuntil(b'\x02')
            header = b"\x02" + await self.reader.readexactly(3)
        else:
            header = await self.reader.readexactly(4)
        if header[0] != 0x02:
            raise CommunicationsError(f"STX missing in header {header}")
        if header[1:2] != controller.packet_identifier:
            raise CommunicationsError(f"invalid response identifier in header {header}")
        if header[2:4].upper() != controller.packet_address:
            raise CommunicationsError(f"invalid response address in header {header}")
        try:
            contents = await self.reader.readuntil(b'\x06')
        except (asyncio.IncompleteReadError, asyncio.LimitOverrunError) as e:
            raise CommunicationsError("no ACK in packet") from e
        contents = contents[:-1]
        if len(contents) < 3:
            raise CommunicationsError("packet too short")
        if contents[:1] == b'N':
            try:
                error_code = int(contents[1:3], 16)
            except (TypeError, ValueError):
                raise CommunicationsError(f"invalid error code in packet {contents}")
            raise CommunicationsError(f"error code {error_code:02X} received from controller {controller.address:X}")
        response_checksum = contents[-2:].upper()
        calculated_checksum = controller.packet_checksum(header[1:] + contents[:-2])
        if response_checksum != calculated_checksum:
            raise CommunicationsError(f"checksum mismatch in {contents}, calculated {calculated_checksum} but got {response_checksum}")
        return contents[:-2]

    async def _command_response(self, controller: "Instrument._Controller",
                                command: "Instrument._Command", data: bytes = None) -> bytes:
        payload = command.value
        if data:
            payload = payload + data
        self.writer.write(controller.assemble_packet(payload))
        return await self._read_response(controller)

    async def _retry_command(self, controller: "Instrument._Controller",
                             command: "Instrument._Command", data: bytes = None) -> bytes:
        await self._command_delay()

        retry = 0
        while True:
            try:
                return await wait_cancelable(self._command_response(controller, command, data), min(2.0 + retry, 4.0))
            except (asyncio.TimeoutError, CommunicationsError):
                if retry >= 4:
                    raise
                retry += 1

            await self.writer.drain()
            await self.drain_reader(0.5)

    async def _start_controller_communications(self, controller: "Instrument._Controller") -> None:
        _LOGGER.debug(f"Starting communications with controller {controller.address:X}")

        controller.current_input = None
        controller.current_setpoint = None

        status = await self._retry_command(controller, self._Command.READ_STATUS)
        status = self._StatusResponse(status)

        if not status.remote_mode:
            await self._retry_command(controller, self._Command.SET_REMOTE_MODE)
        if status.manual_mode and not controller.manual_mode:
            await self._retry_command(controller, self._Command.DISABLE_MANUAL_MODE)
        elif not status.manual_mode and controller.manual_mode:
            await self._retry_command(controller, self._Command.ENABLE_MANUAL_MODE)

        try:
            await self._command_delay()
            response = await wait_cancelable(self._command_response(controller, self._Command.READ_EEPROM_SAVE), 4.0)
            if response[:1] != b'0' and (len(response) == 1 or response[1:2] != b'0'):
                await self._command_delay()
                await wait_cancelable(self._command_response(controller, self._Command.DISABLE_EEPROM_SAVE), 4.0)
        except (asyncio.TimeoutError, CommunicationsError):
            # Not fatal, since some controllers do not support it
            _LOGGER.debug(f"Ignoring failed EEPROM manipulation on controller {controller.address:X}",
                          exc_info=True)
            await self.writer.drain()
            await self.drain_reader(2.0)

        response = await self._retry_command(controller, self._Command.READ_CONTROLLER_DATA)
        if response[:4] == b'LOVE':
            response = response[4:]
        try:
            week = int(response[0:2])
        except (TypeError, ValueError):
            raise CommunicationsError(f"invalid controller production week in {response}")
        try:
            year = int(response[2:4])
        except (TypeError, ValueError):
            raise CommunicationsError(f"invalid controller production year in {response}")
        td = time.gmtime()
        current_century = td.tm_year - (td.tm_year % 100)
        year += current_century
        if year > td.tm_year + 50:
            year -= 100
        model = response[4:].decode('ascii')
        controller.reported_model = model
        controller.reported_id = f"{year:04d}w{week:02d}-{model}"

        response = await self._retry_command(controller, self._Command.READ_FIRMWARE_DATA)
        if len(response) >= 2:
            controller.reported_firmware = response.decode('ascii')

        response = await self._retry_command(controller, self._Command.READ_DECIMAL_POINT)
        try:
            digits = int(response, 16)
        except (TypeError, ValueError):
            raise CommunicationsError(f"invalid number of decimal digits in {response}")
        if digits < 0 or digits > 3:
            raise CommunicationsError(f"invalid number of decimal digits {digits}")

        if controller.decimal_digits is not None and digits != controller.decimal_digits:
            await self._retry_command(controller, self._Command.WRITE_DECIMAL_POINT,
                                      b"000%02X00" % controller.decimal_digits)
            digits = controller.decimal_digits
        controller.reported_decimal_digits = digits

    async def start_communications(self) -> None:
        if not self.writer:
            raise CommunicationsError

        await self.writer.drain()
        await self.drain_reader(1.0)

        for controller in self.controllers:
            try:
                await self._start_controller_communications(controller)
            except CommunicationsError as e:
                raise CommunicationsError(f"error starting communications with controller {controller.address:X}") from e
            except asyncio.TimeoutError as e:
                raise asyncio.TimeoutError(f"timeout starting communications with controller {controller.address:X}") from e

        await self._update_all_analog_out(force=True)
        self._sleep_time = 0.0

        self.set_firmware_version(" ".join([controller.reported_firmware for controller in self.controllers]))
        for inp in self._analog_inputs:
            if not inp.controller.reported_id:
                continue
            inp.variable.data.attributes['controller'] = inp.controller.reported_id

    async def _write_analog_output(self, controller: "Instrument._Controller", value: float) -> float:
        if controller.manual_mode:
            # Manual mode is fixed at 1 decimal place
            output_number = round(value * 10.0)
            if output_number <= 0:
                payload = b"0000"
                output_number = 0
            elif output_number >= 1000:
                payload = b"1000"
                output_number = 1000
            else:
                payload = b"%04d" % output_number
            payload = payload + b"00"

            response = await self._retry_command(controller, self._Command.WRITE_MANUAL_SP1, payload)
            if response != b'00':
                raise CommunicationsError(f"invalid setpoint change response {response}")

            return output_number / 10.0

        scale_factor = ([1.0, 10.0, 100.0, 1000.0])[controller.reported_decimal_digits]
        if value < 0.0:
            sign = b"11"
            scale_factor = -scale_factor
        else:
            sign = b"00"
        output_number = round(value * scale_factor)
        if output_number <= 0:
            payload = b"0000"
            output_number = 0
        elif output_number >= 1000:
            payload = b"1000"
            output_number = 1000
        else:
            payload = b"%04d" % output_number
        payload = payload + sign

        response = await self._retry_command(controller, self._Command.WRITE_SP1, payload)
        if response != b'00':
            raise CommunicationsError(f"invalid setpoint change response {response}")

        return output_number / scale_factor

    async def _update_all_analog_out(self, force: bool = False) -> None:
        for out in self._analog_outputs:
            value = out.value
            if value is None:
                continue
            if not isfinite(value):
                continue
            if not force and value == out.controller.current_setpoint:
                continue

            try:
                applied_value = await self._write_analog_output(out.controller, value)
            except CommunicationsError as e:
                raise CommunicationsError(f"error setting controller {out.controller.address:X} output") from e
            except asyncio.TimeoutError as e:
                raise asyncio.TimeoutError(f"timeout setting controller {out.controller.address:X} output") from e
            out.current_setpoint = applied_value

    async def communicate(self) -> None:
        if self._sleep_time > 0.0:
            try:
                await wait_cancelable(self._output_changed.wait(), self._sleep_time)
            except asyncio.TimeoutError:
                pass
            self._sleep_time = 0.0
        self._output_changed.clear()
        begin_read = time.monotonic()

        await self._update_all_analog_out()

        raw_values: typing.List[float] = [nan] * len(self.controllers)
        setpoint_values: typing.List[float] = [nan] * len(self.controllers)
        output_values: typing.List[float] = [nan] * len(self.controllers)
        for controller in self.controllers:
            try:
                status = await self._retry_command(controller, self._Command.READ_STATUS)
                status = self._StatusResponse(status)
                raw_values[controller.index] = status.value
                controller.current_input = status.value

                if False and status.has_error:
                    error_status = await self._retry_command(controller, self._Command.READ_ERROR_STATUS)
                    error_status = self._ErrorStatusResponse(error_status)

                if status.manual_mode:
                    setpoint = await self._retry_command(controller, self._Command.READ_MANUAL_SP1)
                else:
                    setpoint = await self._retry_command(controller, self._Command.READ_SP1)
                setpoint = self._ValueResponse(setpoint)
                setpoint_values[controller.index] = setpoint.value
                controller.current_setpoint = setpoint.value

                output_percent = await self._retry_command(controller, self._Command.READ_OUTPUT_PERCENT)
                output_percent = self._ValueResponse(output_percent)
                output_values[controller.index] = output_percent.value
            except CommunicationsError as e:
                raise CommunicationsError(f"error updating controller {controller.address:X}") from e
            except asyncio.TimeoutError as e:
                raise asyncio.TimeoutError(f"timeout updating controller {controller.address:X}") from e

        self.data_raw(raw_values)
        self.data_setpoint(setpoint_values)
        self.data_control(output_values)

        calibrated_values: typing.List[float] = list()
        for inp in self._analog_inputs:
            inp(inp.controller.current_input)

            while inp.controller.index >= len(calibrated_values):
                calibrated_values.append(nan)
            calibrated_values[inp.controller.index] = inp.value
        self.data_value(calibrated_values)

        self.analog_input_report()

        for out in self._analog_outputs:
            out.persistent(out.controller.current_setpoint)

        end_read = time.monotonic()
        self._sleep_time = self._report_interval - (end_read - begin_read)

    async def run(self) -> typing.NoReturn:
        self._output_changed = asyncio.Event()
        try:
            await super().run()
        finally:
            self._output_changed = None
