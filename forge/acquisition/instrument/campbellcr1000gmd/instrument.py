import typing
import asyncio
import logging
import time
from math import nan, isfinite
from forge.tasks import wait_cancelable
from forge.acquisition import LayeredConfiguration
from forge.data.structure.variable import variable_flags
from ..streaming import StreamingInstrument, StreamingContext, CommunicationsError
from ..flexio import AnalogInput, AnalogOutput, DigitalOutput, CutSize
from ..state import State
from ..parse import parse_number

_LOGGER = logging.getLogger(__name__)
_INSTRUMENT_TYPE = __name__.split('.')[-2]


class Instrument(StreamingInstrument):
    INSTRUMENT_TYPE = _INSTRUMENT_TYPE
    MANUFACTURER = "Campbell"
    MODEL = "CR1000-GML"
    DISPLAY_LETTER = "U"
    TAGS = frozenset({"aerosol", _INSTRUMENT_TYPE})
    SERIAL_PORT = {'baudrate': 115200}

    ANALOG_INPUT_COUNT = 32
    ANALOG_INPUT_TEMPERATURE = -1
    ANALOG_INPUT_VOLTAGE = -2

    ANALOG_OUTPUT_COUNT = 8
    DIGITAL_OUTPUT_COUNT = 20

    class _AnalogInput(AnalogInput):
        def __init__(self, name: str, config: LayeredConfiguration, inp: StreamingInstrument.Input, channel: int):
            super().__init__(name, config, inp)
            self.channel: int = channel
            self.attributes['channel'] = str(channel)

        @classmethod
        def construct(cls, instrument: "Instrument", name: str,
                      config: LayeredConfiguration) -> typing.Optional["Instrument._AnalogInput"]:
            channel = config.get('CHANNEL')
            if isinstance(channel, str):
                channel = channel.lower()
                if channel == 't' or channel == 'temperature':
                    channel = Instrument.ANALOG_INPUT_TEMPERATURE
                elif channel == 'v' or channel == 'voltage':
                    channel = Instrument.ANALOG_INPUT_VOLTAGE
            else:
                try:
                    channel = int(channel)
                    if channel < 1 or channel > Instrument.ANALOG_INPUT_COUNT:
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
                    if channel < 0 or channel >= Instrument.ANALOG_OUTPUT_COUNT:
                        raise ValueError
                except (TypeError, ValueError):
                    _LOGGER.warning(f"Invalid analog output channel for {name}", exc_info=True)
                    return None
                return cls(name, LayeredConfiguration(), channel, instrument)

            try:
                channel = int(config.get('CHANNEL'))
                if channel < 0 or channel >= Instrument.ANALOG_OUTPUT_COUNT:
                    raise ValueError
            except (TypeError, ValueError):
                _LOGGER.warning(f"Invalid analog output channel for {name}", exc_info=True)
                return None
            return cls(name, config, channel, instrument)

    class _DigitalOutput(DigitalOutput):
        def __init__(self, name: str, config: LayeredConfiguration, channel: int, instrument: "Instrument"):
            super().__init__(name, config)
            self.channel = channel
            self.bit: int = (1 << channel)
            self.instrument = instrument
            if channel < 63:
                try:
                    self.command_bit = self.bit
                except OverflowError:
                    pass

        def command_received(self) -> None:
            self.instrument._output_changed_wake()

        @classmethod
        def construct(cls, instrument: "Instrument", name: str,
                      config: typing.Union[int, LayeredConfiguration]) -> typing.Optional["Instrument._DigitalOutput"]:
            if not isinstance(config, LayeredConfiguration):
                try:
                    channel = int(config)
                    if channel < 0 or channel >= Instrument.DIGITAL_OUTPUT_COUNT:
                        raise ValueError
                except (TypeError, ValueError):
                    _LOGGER.warning(f"Invalid digital output channel for {name}", exc_info=True)
                    return None
                return cls(name, LayeredConfiguration(), channel, instrument)

            try:
                channel = int(config.get('CHANNEL'))
                if channel < 0 or channel > Instrument.DIGITAL_OUTPUT_COUNT:
                    raise ValueError
            except (TypeError, ValueError):
                _LOGGER.warning(f"Invalid digital output channel for {name}", exc_info=True)
                return None
            return cls(name, config, channel, instrument)

    def __init__(self, context: StreamingContext):
        super().__init__(context)

        self._report_interval: float = float(context.config.get('REPORT_INTERVAL', default=1.0))
        self._sleep_time: float = 0.0

        self.data_T = self.input("T")
        self.data_V = self.input("V")

        self.data_value = self.input_array("value")
        self.data_raw = self.input_array("raw")
        self.data_analog_outputs = self.persistent("output", save_value=False)
        self.data_digital_outputs = self.persistent("digital", save_value=False)

        self._analog_inputs: typing.List[Instrument._AnalogInput] = self._AnalogInput.create_inputs(self)

        for no_cut in (self.data_T, self.data_V, self.data_value, self.data_raw):
            if no_cut.field.use_cut_size is None:
                no_cut.field.use_cut_size = False

        variable_names: typing.List[str] = [""] * self.ANALOG_INPUT_COUNT
        self.instrument_info['variable'] = variable_names
        variables: typing.List[Instrument.Variable] = list()
        for inp in self._analog_inputs:
            if inp.channel and inp.channel < self.ANALOG_INPUT_COUNT:
                variable_names[inp.channel] = inp.name
            if inp.variable:
                variables.append(inp.variable)

        self.analog_input_report = self.report(
            *variables,

            self.variable_array(self.data_raw, name='analog_input', code='ZINPUTS', attributes={
                'long_name': "raw analog input voltages",
                'units': "V",
                'C_format': "%5.3f"
            }),

            self.variable(self.data_V, "supply_voltage", code="V", attributes={
                'long_name': "supply voltage",
                'units': "V",
                'C_format': "%6.3f"
            }),
            self.variable_temperature(self.data_T, "board_temperature", code="T", attributes={
                'long_name': "control board temperature",
            }),

            record=self.record_downstream(),
        )

        state: typing.List[State] = list()

        self._analog_outputs: typing.List[Instrument._AnalogOutput] = self._AnalogOutput.create_outputs(self)
        analog_output_names: typing.List[str] = list()
        self.instrument_info['output'] = analog_output_names        
        for out in self._analog_outputs:
            while out.channel >= len(analog_output_names):
                analog_output_names.append("")
            analog_output_names[out.channel] = out.name
            if out.state is not None:
                state.append(out.state)

        self._digital_outputs: typing.List[Instrument._DigitalOutput] = self._DigitalOutput.create_outputs(self)
        digital_output_names: typing.List[str] = list()
        self.instrument_info['digital'] = digital_output_names
        self._digital_state: typing.Optional[int] = None
        self._apply_digital_state: typing.Optional[int] = None
        for out in self._digital_outputs:
            while out.channel >= len(digital_output_names):
                digital_output_names.append("")
            digital_output_names[out.channel] = out.name

        self.cut_size = CutSize(self.context.cutsize_config)

        if bool(context.config.get('LOG_DIGITAL_STATE', default=False)):
            digital_state = self.state_unsigned_integer(
                self.data_digital_outputs, 'digital_output', code='F2', attributes={
                    'long_name': "digital output state",
                    'standard_name': None,
                })

            state_flags: typing.Dict[int, str] = dict()
            for out in self._digital_outputs:
                if out.channel > 63:
                    continue
                try:
                    bit = (1 << out.channel)
                except OverflowError:
                    continue
                state_flags[bit] = out.name

            def configure(var):
                variable_flags(var, state_flags)

            digital_state.data.configure_variable = configure

            state = [digital_state] + state
        if state:
            self.state_changed = self.change_event(*state)
        else:
            self.state_changed = None

        self.context.bus.connect_command('set_digital_output', self._override_digital_state)
        self._output_changed: typing.Optional[asyncio.Event] = None

    def _override_digital_state(self, data: int) -> None:
        try:
            bits = int(data)
        except (ValueError, TypeError, OverflowError):
            return
        self._apply_digital_state = bits
        self._output_changed_wake()

    def _output_changed_wake(self) -> None:
        if not self._output_changed:
            return
        self._output_changed.set()

    async def start_communications(self) -> None:
        if not self.writer:
            raise CommunicationsError

        await self.writer.drain()
        await self.drain_reader(1.0)
        self.writer.write(b"RST\r")

        line: bytes = await wait_cancelable(self.read_line(), 2.0)
        if line[:4] != b'STA,':
            raise CommunicationsError(f"invalid state response {line}")
        fields = line.split(b',')
        if len(fields) != 4 + self.ANALOG_INPUT_COUNT:
            raise CommunicationsError(f"invalid number of state fields in {line}")

        await self._update_all_digital_out(force=True)
        await self._update_all_analog_out(force=True)
        self._sleep_time = 0.0

    async def _write_digital_state(self, update_state: int) -> int:
        self.writer.write(f"SDO,{update_state:08x}\r".encode('ascii'))
        line: bytes = await self.read_line()
        if line[:4] != b'SDA,':
            raise CommunicationsError(f"invalid digital set response {line}")

        fields = line.split(b',')
        try:
            (_, digital_state) = fields
        except ValueError:
            raise CommunicationsError(f"invalid number of fields in {line}")

        try:
            digital_state = int(digital_state.strip(), 16)
        except (ValueError, OverflowError):
            raise CommunicationsError(f"invalid digital state {digital_state}")

        if digital_state != update_state:
            _LOGGER.warning(f"Error changing digital state, set {update_state:08X} but got {digital_state:08X}")

        return digital_state

    async def _write_analog_output(self, channel: int, value: float) -> float:
        self.writer.write(f"SAO,{channel},{value:.6f}\r".encode('ascii'))
        line: bytes = await self.read_line()
        if line[:4] != b'SAA,':
            raise CommunicationsError(f"invalid analog set response {line}")

        fields = line.split(b',')
        try:
            (_, response_channel, analog_value) = fields
        except ValueError:
            raise CommunicationsError(f"invalid number of fields in {line}")

        try:
            response_channel = int(response_channel)
        except (ValueError, OverflowError):
            raise CommunicationsError(f"invalid analog channel {response_channel}")
        analog_value = parse_number(analog_value)

        if abs(analog_value - value) > 0.1:
            _LOGGER.warning(f"Error changing analog output {channel}, set {value} but got {analog_value}")

        return analog_value

    async def _update_all_digital_out(self, force: bool = False) -> None:
        if self._apply_digital_state:
            update_state = self._apply_digital_state
        else:
            update_state = self._digital_state or 0
        for out in self._digital_outputs:
            is_set = out.value
            if is_set is None:
                continue
            if is_set:
                update_state |= out.bit
            else:
                update_state &= ~out.bit

        if force or update_state != self._digital_state:
            update_state = await wait_cancelable(self._write_digital_state(update_state), 2.0)
            self._digital_state = update_state

        self._apply_digital_state = None

    async def _update_all_analog_out(self, force: bool = False) -> None:
        for out in self._analog_outputs:
            value = out.value
            if value is None:
                continue
            if not isfinite(value):
                continue
            if not force and value == out.last_output_value:
                continue

            applied_value = await wait_cancelable(self._write_analog_output(out.channel, value), 2.0)
            out.last_output_value = applied_value

    async def communicate(self) -> None:
        if self._sleep_time > 0.0:
            try:
                await wait_cancelable(self._output_changed.wait(), self._sleep_time)
            except asyncio.TimeoutError:
                pass
            self._sleep_time = 0.0
        self._output_changed.clear()
        begin_read = time.monotonic()

        self.writer.write(b"RST\r")

        line: bytes = await wait_cancelable(self.read_line(), 2.0)
        if line[:4] != b'STA,':
            raise CommunicationsError(f"invalid state response {line}")
        fields = line.split(b',')
        try:
            (
                _,  # Response code
                digital_state, *ain, V, T
            ) = fields
        except ValueError:
            raise CommunicationsError(f"invalid number of fields in {line}")
        if len(ain) != self.ANALOG_INPUT_COUNT:
            raise CommunicationsError(f"invalid number of analog inputs in {line}")
        for i in range(len(ain)):
            value: bytes = ain[i].strip()
            if value.lower() == 'nan':
                ain[i] = nan
                continue
            ain[i] = parse_number(value)

        try:
            digital_state = int(digital_state.strip(), 16)
        except (ValueError, OverflowError):
            raise CommunicationsError(f"invalid digital state {digital_state}")

        V = parse_number(V)
        T = parse_number(T)
        self.data_V(V)
        self.data_T(T)
        self._digital_state = digital_state

        active_cut_size, next_cut_size = self.cut_size.advance()
        is_bypassed = self.context.bus.bypassed
        for out in self._digital_outputs:
            out.update_cut_size(active_cut_size.size)
            out.update_bypass(is_bypassed)
        await self._update_all_digital_out()

        await self._update_all_analog_out()

        analog_output_values: typing.List[float] = list()
        for out in self._analog_outputs:
            value = out.value
            if value is None:
                continue
            if not isfinite(value):
                continue
            while out.channel >= len(analog_output_values):
                analog_output_values.append(nan)
            analog_output_values[out.channel] = value

        self.data_digital_outputs(self._digital_state)
        self.data_analog_outputs(analog_output_values)

        self.data_raw(ain)

        calibrated_values: typing.List[float] = list()
        for inp in self._analog_inputs:
            if inp.channel == self.ANALOG_INPUT_TEMPERATURE:
                inp(T)
                continue
            elif inp.channel == self.ANALOG_INPUT_VOLTAGE:
                inp(V)
                continue
            if inp.channel >= len(ain):
                continue

            inp(ain[inp.channel])

            while inp.channel >= len(calibrated_values):
                calibrated_values.append(nan)
            calibrated_values[inp.channel] = inp.value
        self.data_value(calibrated_values)

        self.analog_input_report()

        end_read = time.monotonic()
        self._sleep_time = self._report_interval - (end_read - begin_read)
        if next_cut_size != active_cut_size:
            delay = next_cut_size.next_time - time.time()
            if delay < 0.001:
                delay = 0.001
            self._sleep_time = min(self._sleep_time, delay)

    async def _send_shutdown_state(self) -> None:
        if not self.writer:
            return

        updated_digital_state: typing.Optional[int] = 0
        for out in self._digital_outputs:
            if out.shutdown_state is None:
                continue
            if updated_digital_state is None:
                updated_digital_state = 0
            if out.shutdown_state:
                updated_digital_state |= out.bit
            else:
                updated_digital_state &= ~out.bit
        if updated_digital_state is not None:
            await wait_cancelable(self._write_digital_state(updated_digital_state), 2.0)

    async def run(self) -> typing.NoReturn:
        self._output_changed = asyncio.Event()
        try:
            await super().run()
        finally:
            self._output_changed = None
            try:
                await self._send_shutdown_state()
            except:
                _LOGGER.debug("Error sending shutdown state", exc_info=True)
