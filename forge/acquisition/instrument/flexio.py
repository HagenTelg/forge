import typing
import asyncio
from math import isfinite
from forge.acquisition import LayeredConfiguration
from .standard import StandardInstrument
from .variable import Input, Variable
from .state import Persistent, State
from ..cutsize import CutSize


def _merge_attribute(config: LayeredConfiguration, attributes: typing.Dict[str, str]) -> None:
    attrs = config.get('ATTRIBUTES')
    if attrs:
        for attr in attrs.keys():
            attributes[attr] = str(config.get(attr))

    long_name = config.get("LONG_NAME", default=config.get('DESCRIPTION'))
    if long_name:
        attributes['long_name'] = long_name
    C_format = config.get("C_FORMAT", default=config.get('FORMAT'))
    if C_format:
        attributes['C_format'] = C_format
    units = config.get('UNITS')
    if units:
        attributes['units'] = units


class AnalogInput:
    def __init__(self, name: str, config: LayeredConfiguration, inp: Input):
        self.name = name
        self.config = config
        self.input = inp
        self.variable: typing.Optional[Variable] = None
        self.attributes: typing.Dict[str, str] = dict()

    def __call__(self, value: float) -> None:
        self.input(value)

    @property
    def value(self) -> typing.Optional[float]:
        return self.input.value
        
    @classmethod
    def construct(cls, instrument: StandardInstrument, name: str,
                  config: LayeredConfiguration) -> typing.Optional["AnalogInput"]:
        raise NotImplementedError

    @classmethod
    def create_inputs(cls, instrument: StandardInstrument):
        variable_config = instrument.context.config.get('DATA')
        if not isinstance(variable_config, dict):
            return list()

        type_map: typing.Dict[str, typing.Callable] = {
            'temperature': instrument.variable_temperature,
            't': instrument.variable_temperature,
            'air_temperature': instrument.variable_air_temperature,
            'air_t': instrument.variable_air_temperature,
            'rh': instrument.variable_rh,
            'u': instrument.variable_rh,
            'air_rh': instrument.variable_air_rh,
            'air_u': instrument.variable_air_rh,
            'pressure': instrument.variable_pressure,
            'p': instrument.variable_pressure,
            'air_pressure': instrument.variable_air_pressure,
            'air_p': instrument.variable_air_pressure,
            'flow': instrument.variable_flow,
            'q': instrument.variable_flow,
            'sample_flow': instrument.variable_sample_flow,
            'sample_q': instrument.variable_sample_flow,
            'delta_pressure': instrument.variable_delta_pressure,
            'dp': instrument.variable_delta_pressure,
        }

        analog_inputs: typing.List["AnalogInput"] = list()
        names = list(variable_config.keys())
        names.sort()
        for name in names:
            ain = cls.construct(instrument, name, instrument.context.config.section('DATA', name))
            if ain is None:
                continue
            analog_inputs.append(ain)
            if ain.config.get('DISABLE_LOGGING'):
                continue

            _merge_attribute(ain.config, ain.attributes)

            type_name = ain.config.get('TYPE')
            if isinstance(type_name, str):
                type_name = type_name.lower()
            create = type_map.get(type_name, instrument.variable)
            ain.variable = create(ain.input,
                                  name=ain.config.get('FIELD'),
                                  code=name,
                                  attributes=ain.attributes)
        return analog_inputs


class AnalogOutput:
    def __init__(self, name: str, config: LayeredConfiguration):
        self.name = name
        self.config = config
        self.persistent: Persistent = None
        self.state: typing.Optional[State] = None
        self.attributes: typing.Dict[str, str] = dict()

        self.command_channel: typing.Optional[typing.Any] = None

        self.shutdown_value: typing.Optional[float] = None

    def __call__(self, value: float) -> None:
        self.persistent(value)

    @property
    def value(self) -> typing.Optional[float]:
        return self.persistent.value

    @value.setter
    def value(self, v: float) -> None:
        self.persistent(v)

    def command_received(self) -> None:
        pass

    def _command_set_analog_channel(self, data: typing.Dict[str, typing.Any]) -> None:
        if not self.command_channel:
            return
        if not isinstance(data, dict):
            return
        channel = data.get('channel')
        value = data.get('value')
        try:
            channel = type(self.command_channel)(channel)
            value = float(value)
        except (ValueError, TypeError, OverflowError):
            return
        if channel != self.command_channel:
            return
        if not isfinite(value):
            return
        self.persistent(value)
        self.command_received()

    def _command_set_analog(self, data: typing.Dict[str, typing.Any]) -> None:
        if not isinstance(data, dict):
            return
        output = data.get('output')
        value = data.get('value')
        try:
            output = str(output)
            value = float(value)
        except (ValueError, TypeError, OverflowError):
            return
        if output != self.name:
            return
        if not isfinite(value):
            return
        self.persistent(value)
        self.command_received()

    @classmethod
    def construct(cls, instrument: StandardInstrument, name: str,
                  config: LayeredConfiguration) -> typing.Optional["AnalogOutput"]:
        raise NotImplementedError

    @classmethod
    def create_outputs(cls, instrument: StandardInstrument, section: str = 'ANALOG_OUTPUT'):
        output_config = instrument.context.config.get(section)
        if not isinstance(output_config, dict):
            return list()

        analog_outputs: typing.List["AnalogOutput"] = list()
        names = list(output_config.keys())
        names.sort()
        for name in names:
            aot = cls.construct(instrument, name, instrument.context.config.section_or_constant(section, name))
            if aot is None:
                continue
            analog_outputs.append(aot)

            instrument.context.bus.connect_command('set_analog_channel', aot._command_set_analog_channel)
            instrument.context.bus.connect_command('set_analog', aot._command_set_analog)

            if not isinstance(aot.config, LayeredConfiguration):
                aot.persistent = instrument.persistent(name, send_to_bus=False, save_value=False)
                continue

            save_value = bool(aot.config.get('RESTORE_VALUE', default=False))

            if not aot.config.get('ENABLE_LOGGING'):
                aot.persistent = instrument.persistent(name, send_to_bus=False, save_value=save_value)
            else:
                _merge_attribute(aot.config, aot.attributes)

                aot.persistent = instrument.persistent(name, send_to_bus=True, save_value=save_value)
                aot.state = instrument.state_float(aot.persistent,
                                                   name=aot.config.get('FIELD'),
                                                   code=name,
                                                   attributes=aot.attributes)
            if not save_value:
                initial = aot.config.get('INITIAL')
                if initial is not None:
                    aot.persistent(float(initial), oneshot=True)

            shutdown = aot.config.get('SHUTDOWN')
            if shutdown is not None:
                aot.shutdown_state = float(shutdown)

        return analog_outputs


class DigitalOutput:
    def __init__(self, name: str, config: LayeredConfiguration):
        self.name = name
        self.config = config
        self.persistent: Persistent = None

        self.command_bit: typing.Optional[int] = None

        self.cut_size_state: typing.Dict[CutSize.Size, bool] = dict()
        self.bypass_state: typing.Dict[bool, bool] = dict()
        self.shutdown_state: typing.Optional[bool] = None
        
        self._prior_cut_size: typing.Optional[CutSize.Size] = None
        self._prior_bypass: typing.Optional[bool] = None

    def __call__(self, value: bool) -> None:
        self.persistent(value)

    @property
    def value(self) -> typing.Optional[bool]:
        return self.persistent.value

    @value.setter
    def value(self, v: bool) -> None:
        self.persistent(v)

    def _configure_cut_size(self, config: typing.Union[str, dict]) -> None:
        if not isinstance(config, dict):
            set_state = CutSize.Size.parse(config)
            for clear in CutSize.Size:
                self.cut_size_state[clear] = False
            self.cut_size_state[set_state] = True
            return

        for cut_size, state in config.items():
            cut_size = CutSize.Size.parse(cut_size)
            state = bool(state)
            self.cut_size_state[cut_size] = state

    def _configure_bypass(self, config: typing.Union[bool, dict]) -> None:
        if not isinstance(config, dict):
            config = bool(config)
            self.bypass_state[config] = True
            self.bypass_state[not config] = False
            return

        for bypass, state in config.items():
            if isinstance(bypass, str):
                bypass = bypass.lower()
                if bypass == 'on' or bypass == 'true' or bypass == 't' or bypass == 'bypass':
                    bypass = True
                elif bypass == 'off' or bypass == 'false' or bypass == 'false' or bypass == 'sample':
                    bypass = False
            bypass = bool(bypass)
            state = bool(state)
            self.bypass_state[bypass] = state

    def command_received(self) -> None:
        pass

    def _command_set_digital_output(self, data: int) -> None:
        if not self.command_bit:
            return
        try:
            bits = int(data)
        except (ValueError, TypeError, OverflowError):
            return
        is_set = (bits & self.command_bit) != 0
        self.persistent(is_set)
        self.command_received()

    def _command_set_digital(self, data: typing.Dict[str, typing.Any]) -> None:
        if not isinstance(data, dict):
            return
        output = data.get('output')
        value = data.get('value')
        try:
            output = str(output)
            value = bool(value)
        except (ValueError, TypeError, OverflowError):
            return
        if output != self.name:
            return
        self.persistent(value)
        self.command_received()

    @classmethod
    def construct(cls, instrument: StandardInstrument, name: str,
                  config: LayeredConfiguration) -> typing.Optional["DigitalOutput"]:
        raise NotImplementedError

    @classmethod
    def create_outputs(cls, instrument: StandardInstrument, section: str = 'DIGITAL'):
        output_config = instrument.context.config.get(section)
        if not isinstance(output_config, dict):
            return list()

        digital_outputs: typing.List["DigitalOutput"] = list()
        names = list(output_config.keys())
        names.sort()
        for name in names:
            dot = cls.construct(instrument, name, instrument.context.config.section_or_constant(section, name))
            if dot is None:
                continue
            digital_outputs.append(dot)

            instrument.context.bus.connect_command('set_digital_output', dot._command_set_digital_output)
            instrument.context.bus.connect_command('set_digital', dot._command_set_digital)

            if not isinstance(dot.config, LayeredConfiguration):
                dot.persistent = instrument.persistent(name, send_to_bus=False, save_value=False)
                continue

            save_value = bool(dot.config.get('RESTORE_VALUE', default=False))

            dot.persistent = instrument.persistent(name, send_to_bus=False, save_value=save_value)
            if not save_value:
                initial = dot.config.get('INITIAL')
                if initial is not None:
                    dot.persistent(bool(initial), oneshot=True)

            shutdown = dot.config.get('SHUTDOWN')
            if shutdown is not None:
                dot.shutdown_state = bool(shutdown)

            cut_size = dot.config.get('CUT_SIZE')
            if cut_size is not None:
                dot._configure_cut_size(cut_size)

            bypass = dot.config.get('BYPASS')
            if bypass is not None:
                dot._configure_bypass(bypass)
        return digital_outputs

    def update_cut_size(self, cut_size: typing.Optional[CutSize.Size]) -> None:
        if cut_size is None:
            self._prior_cut_size = None
            return
        if cut_size == self._prior_cut_size:
            return
        self._prior_cut_size = cut_size
        target = self.cut_size_state.get(cut_size)
        if target is None:
            return
        self.persistent(target)

    def update_bypass(self, bypassed: bool) -> None:
        if bypassed == self._prior_bypass:
            return
        self._prior_bypass = bypassed
        target = self.bypass_state.get(bypassed)
        if target is None:
            return
        self.persistent(target)
