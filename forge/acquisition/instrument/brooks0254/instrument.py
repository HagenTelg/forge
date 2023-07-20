import typing
import asyncio
import logging
import time
from math import nan, isfinite
from forge.tasks import wait_cancelable
from forge.acquisition import LayeredConfiguration
from ..streaming import StreamingInstrument, StreamingContext, CommunicationsError
from ..flexio import AnalogInput, AnalogOutput
from ..state import State
from ..parse import parse_number

_LOGGER = logging.getLogger(__name__)
_INSTRUMENT_TYPE = __name__.split('.')[-2]


def _parse_channel(channel: typing.Union[str, int]) -> int:
    channel = int(channel)
    if channel <= 0 or channel > 4:
        raise ValueError(f"Channel {channel} out of range")
    return channel


class Instrument(StreamingInstrument):
    INSTRUMENT_TYPE = _INSTRUMENT_TYPE
    MANUFACTURER = "Brooks"
    MODEL = "0254"
    DISPLAY_LETTER = "P"
    TAGS = frozenset({"aerosol", _INSTRUMENT_TYPE})
    SERIAL_PORT = {'baudrate': 9600}

    CHANNELS = 4

    class _AnalogInput(AnalogInput):
        def __init__(self, name: str, config: LayeredConfiguration, inp: StreamingInstrument.Input, channel: int):
            super().__init__(name, config, inp)
            self.channel: int = channel
            self.attributes['channel'] = str(channel)

        @classmethod
        def construct(cls, instrument: "Instrument", name: str,
                      config: LayeredConfiguration) -> typing.Optional["Instrument._AnalogInput"]:
            channel = config.get('CHANNEL')
            try:
                channel = int(channel)
                if channel < 1 or channel > Instrument.CHANNELS:
                    raise ValueError
            except (ValueError, TypeError):
                _LOGGER.warning(f"Invalid analog input channel for {name}", exc_info=True)
                return None
            return cls(name, config, instrument.input(name), channel)

    class _AnalogOutput(AnalogOutput):
        def __init__(self, name: str, config: LayeredConfiguration, channel: int, instrument: "Instrument"):
            super().__init__(name, config)
            self.channel = channel
            self.attributes['channel'] = str(channel)
            self.command_channel = channel
            self.last_output_value: typing.Optional[float] = None
            self.instrument = instrument

        def command_received(self) -> None:
            self.instrument._output_changed_wake()

        @classmethod
        def construct(cls, instrument: "Instrument", name: str,
                      config: typing.Union[int, LayeredConfiguration]) -> typing.Optional["Instrument._AnalogOutput"]:
            if not isinstance(config, LayeredConfiguration):
                try:
                    channel = int(config)
                    if channel <= 0 or channel > Instrument.CHANNELS:
                        raise ValueError
                except (TypeError, ValueError):
                    _LOGGER.warning(f"Invalid analog output channel for {name}", exc_info=True)
                    return None
                return cls(name, LayeredConfiguration(), channel, instrument)

            try:
                channel = int(config.get('CHANNEL'))
                if channel <= 0 or channel > Instrument.CHANNELS:
                    raise ValueError
            except (TypeError, ValueError):
                _LOGGER.warning(f"Invalid analog output channel for {name}", exc_info=True)
                return None
            return cls(name, config, channel, instrument)

    def __init__(self, context: StreamingContext):
        super().__init__(context)

        self._report_interval: float = float(context.config.get('REPORT_INTERVAL', default=1.0))
        self._address: typing.Optional[int] = context.config.get('ADDRESS')
        self._sleep_time: float = 0.0

        self.data_value = self.input_array("value")
        self.data_raw = self.input_array("raw")
        self.data_setpoint = self.persistent("setpoint", save_value=False)

        self._analog_inputs: typing.List[Instrument._AnalogInput] = self._AnalogInput.create_inputs(self)
        self._analog_outputs: typing.List[Instrument._AnalogOutput] = self._AnalogOutput.create_outputs(
            self, section='DATA'
        )

        for no_cut in (self.data_value, self.data_raw):
            if no_cut.field.use_cut_size is None:
                no_cut.field.use_cut_size = False

        variable_names: typing.List[str] = [""] * self.CHANNELS
        self.instrument_info['variable'] = variable_names
        variables: typing.List[Instrument.Variable] = list()
        for inp in self._analog_inputs:
            if inp.channel and inp.channel >= 1 and inp.channel <= self.CHANNELS:
                variable_names[inp.channel - 1] = inp.name
            if inp.variable:
                variables.append(inp.variable)

        self.analog_input_report = self.report(
            *variables,

            self.variable_array(self.data_raw, name='process_value', code='ZINPUTS', attributes={
                'long_name': "raw process values",
                'C_format': "%5.3f"
            }),

            record=self.record_downstream(),
        )

        self.parameters_record = self.context.data.constant_record("parameters")
        self.parameter_raw = self.parameters_record.string("instrument_parameters", attributes={
            'long_name': "instrument AZV responses",
        })

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

    def _send_command(self, command: bytes, port: int = None) -> None:
        data = bytearray(b"AZ")
        if self._address is not None:
            data += b"%05d" % self._address
        if port is not None:
            data += b".%02d" % port
        data += command
        data += b"\r"
        self.writer.write(bytes(data))

    async def _command_response(self, command: bytes, port: int = None) -> bytes:
        self._send_command(command, port)
        response: bytes = await wait_cancelable(self.read_line(), 2.0)
        if len(response) <= 6 or response[:3] != b"AZ," or response[-3:-2] != b",":
            raise CommunicationsError(f"invalid response {response}")
        try:
            checksum = int(response[-2:], 16)
        except ValueError:
            raise CommunicationsError(f"invalid checksum for {response}")

        s = 0
        for b in response[2:-2]:
            s = (s + b) & 0xFF
        # "Negation" from the manual means the lowest order byte of a > 1 byte
        # two's complement negation
        s = (~s + 1) & 0xFF
        if s != checksum:
            raise CommunicationsError(f"checksum mismatch on {response} (got {s:02X})")

        # Send the ACK regardless of the contents
        data = bytearray(b"AZ")
        if self._address is not None:
            data += b"%05d" % self._address

        return response[3:-3]

    async def start_communications(self) -> None:
        if not self.writer:
            raise CommunicationsError

        # Reset
        self.writer.write(b"\x1BAZ\r")
        await self.writer.drain()
        await self.drain_reader(1.0)

        # Enable send (pseudo XON)
        self._send_command(b"S")
        await self.writer.drain()
        await self.drain_reader(1.0)

        # Identify
        response: bytes = await self._command_response(b"I")
        fields = response.split(b',')
        try:
            (
                address,
                response_type,
                manufacturer,
                model,
                _,  # Number of ports
                firmware_version,
                start_vector,
            ) = fields
        except ValueError:
            raise CommunicationsError(f"invalid number of fields in {response}")
        if len(address) != 5 or not address.isdigit():
            raise CommunicationsError(f"invalid version {response}")
        if self._address is not None:
            try:
                if int(address) != int(self._address):
                    raise CommunicationsError(f"address mismatch {response}")
            except (ValueError, OverflowError):
                raise CommunicationsError(f"invalid version {response}")
        if response_type != b"4":
            raise CommunicationsError(f"invalid version {response}")
        if b"BROOKS" not in manufacturer.upper():
            raise CommunicationsError(f"invalid version {response}")
        if len(start_vector) != 4:
            raise CommunicationsError(f"invalid version {response}")

        model = model.strip()
        if model[:5].upper() == b"MODEL":
            model = model[5:].strip()
        self.set_instrument_info('model', model.decode('utf-8'))

        firmware_version = firmware_version.strip()
        if firmware_version[:1].upper() == b"V":
            firmware_version = firmware_version[1:].strip()
        self.set_firmware_version(firmware_version)

        parameter_lines: typing.List[str] = list()
        for i in range(self.CHANNELS):
            channel = i * 2 + 1
            self._send_command(b"V", port=channel)
            input_values = await self.read_multiple_lines(total=5.0, first=2.0, tail=1.0)
            parameter_lines.extend([l.decode('utf-8', 'backslashreplace') for l in input_values])

            self._send_command(b"V", port=channel+1)
            output_values = await self.read_multiple_lines(total=5.0, first=2.0, tail=1.0)
            parameter_lines.extend([l.decode('utf-8', 'backslashreplace') for l in output_values])

        self._send_command(b"V", port=(self.CHANNELS*2) + 1)
        global_values = await self.read_multiple_lines(total=5.0, first=2.0, tail=1.0)
        parameter_lines.extend([l.decode('utf-8', 'backslashreplace') for l in global_values])
        self.parameter_raw("\n".join(parameter_lines))

        await self._update_all_analog_out(force=True)
        await self._read_all()
        self._sleep_time = 0.0

    async def _read_all(self) -> None:
        raw_values: typing.List[float] = [nan] * self.CHANNELS
        setpoint_values: typing.List[float] = [nan] * self.CHANNELS

        for i in range(self.CHANNELS):
            channel = i * 2 + 1

            response: bytes = await self._command_response(b"K", port=channel)
            fields = response.split(b',')
            try:
                (
                    _,  # Address
                    _,  # Port number
                    _,  # Response type
                    _,  # Totalizer value
                    pv,
                    _, _, _, _, _, _, _,    # Reserved values
                ) = fields
            except ValueError:
                raise CommunicationsError(f"invalid number of fields in {response}")

            raw_values[i] = parse_number(pv)

            response: bytes = await self._command_response(b"P01?", port=channel+1)
            fields = response.split(b',')
            try:
                (
                    _,  # Address
                    _,  # Response type
                    _,  # Parameter definition
                    sp,
                ) = fields
            except ValueError:
                raise CommunicationsError(f"invalid number of fields in {response}")

            setpoint_values[i] = parse_number(sp)

        self.data_raw(raw_values)
        self.data_setpoint(setpoint_values)

        calibrated_values: typing.List[float] = list()
        for inp in self._analog_inputs:
            raw_index = inp.channel - 1
            inp(raw_values[raw_index])

            while raw_index >= len(calibrated_values):
                calibrated_values.append(nan)
            calibrated_values[raw_index] = inp.value
        self.data_value(calibrated_values)

        self.analog_input_report()

        for out in self._analog_outputs:
            raw_index = out.channel - 1
            out.persistent(setpoint_values[raw_index])

    async def _update_all_analog_out(self, force: bool = False) -> None:
        for out in self._analog_outputs:
            value = out.value
            if value is None:
                continue
            if not isfinite(value):
                continue
            if not force and value == out.last_output_value:
                continue

            await self._command_response(b"P01=%.3f" % value, port=(out.channel * 2))
            out.last_output_value = value

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
        await self._read_all()

        end_read = time.monotonic()
        self._sleep_time = self._report_interval - (end_read - begin_read)

    async def run(self) -> typing.NoReturn:
        self._output_changed = asyncio.Event()
        try:
            await super().run()
        finally:
            self._output_changed = None
