import enum
import typing
import time
import forge.data.structure.variable as netcdf_var
from forge.acquisition import LayeredConfiguration
from forge.acquisition.util import parse_interval
from .base import BaseInstrument, BaseContext, BaseDataOutput
from .variable import Input, Variable, VariableRate, VariableLastValid, VariableVectorMagnitude, VariableVectorDirection
from .array import ArrayInput, ArrayVariable, ArrayVariableLastValid
from .flag import Notification, Flag
from .dimension import Dimension
from .record import Report, Record
from .state import Persistent, PersistentEnum, State, ChangeEvent


def _declare_variable_type(configure: typing.Callable[[netcdf_var.Variable], None],
                           default_name: str = None):
    def method(self: "StandardInstrument", source: Input, name: str = None, code: str = None,
               attributes: typing.Dict[str, typing.Any] = None):
        v = self.variable(source, name or default_name, code, attributes)
        v.data.configure_variable = configure
        return v

    return method


def _declare_variable_array_type(configure: typing.Callable[[netcdf_var.Variable], None],
                                 default_name: str = None):
    def method(self: "StandardInstrument", source: ArrayInput, dimension: typing.Optional[Dimension] = None,
               name: str = None, code: str = None, attributes: typing.Dict[str, typing.Any] = None):
        v = self.variable_array(source, dimension, name or default_name, code, attributes)
        v.data.configure_variable = configure
        return v

    return method


def _declare_state_type(value_type: typing.Type, field_type: typing.Type[BaseDataOutput.Field]):
    class StateHandler(State):
        class Field(field_type):
            def __init__(self, name: str):
                super().__init__(name)
                self.state: typing.Optional["StateHandler"] = None
                self.override: typing.Optional[value_type] = None
                self.use_cut_size = False
                self.template = BaseDataOutput.Field.Template.STATE

            @property
            def value(self) -> value_type:
                if self.override is not None:
                    return self.override
                return self.state.source.value

        def apply_override(self, value: typing.Optional[value_type]) -> None:
            self.data.override = value

    def method(self: "StandardInstrument", source: Persistent, name: str = None, code: str = None,
               attributes: typing.Dict[str, typing.Any] = None, automatic: bool = True):
        if not attributes:
            attributes = dict()
        return StateHandler(self, source, name, code, attributes, automatic)

    return method


def _declare_measurement_state_type(configure: typing.Callable[[netcdf_var.Variable], None],
                                    default_name: str = None):
    def method(self: "StandardInstrument", source: Persistent, name: str = None, code: str = None,
               attributes: typing.Dict[str, typing.Any] = None, automatic: bool = True):
        s = self.state_measurement(source, name or default_name, code, attributes, automatic)
        s.data.configure_variable = configure
        return s

    return method


def _declare_measurement_state_array_type(configure: typing.Callable[[netcdf_var.Variable], None],
                                          default_name: str = None):
    def method(self: "StandardInstrument", source: Persistent, dimension: typing.Optional[Dimension] = None,
               name: str = None, code: str = None,
               attributes: typing.Dict[str, typing.Any] = None, automatic: bool = True):
        s = self.state_measurement_array(source, dimension, name or default_name, code, attributes, automatic)
        s.data.configure_variable = configure
        return s

    return method


def _declare_dimension_type(configure: typing.Callable[[netcdf_var.Variable], None],
                            default_name: str = None):
    def method(self: "StandardInstrument", source: Persistent, name: str = None, code: str = None,
               attributes: typing.Dict[str, typing.Any] = None):
        d = self.dimension(source, name or default_name, code, attributes)
        d.data.configure_variable = configure
        return d

    return method


class StandardInstrument(BaseInstrument):
    def __init__(self, context: BaseContext):
        super().__init__(context)
        self.context.bus.bypass_updated = self._bypass_state_changed

        self._instrument_state_updated: bool = True
        self._instrument_info_updated: bool = True
        self._is_communicating: bool = False

        self.instrument_info: typing.Dict[str, typing.Any] = {
            'type': self.INSTRUMENT_TYPE,
            'manufacturer': self.MANUFACTURER,
            'model': self.MODEL,
            'display_letter': self.DISPLAY_LETTER,
            'tags': self.context.data.tags,
        }

        manufacturer = context.config.get('MANUFACTURER')
        if manufacturer:
            self.instrument_info['manufacturer'] = manufacturer
        model = context.config.get('MODEL')
        if model:
            self.instrument_info['model'] = model
        serial_number = context.config.get('SERIAL_NUMBER')
        if serial_number:
            serial_number = self._simplify_serial_number(serial_number)
        if serial_number:
            self.instrument_info['serial_number'] = serial_number

        display_id = context.config.get('DISPLAY_NAME')
        if not display_id:
            display_id = self.context.bus.source
        self.instrument_info['display_id'] = display_id

        display_letter = context.config.get('DISPLAY_LETTER')
        if display_letter is not None:
            if not display_letter:
                self.instrument_info.pop('display_letter', None)
            else:
                self.instrument_info['display_letter'] = display_letter

        self._dynamic_instrument_info: typing.Dict[str, typing.Any] = dict()

        self._average_state_updated: bool = False
        self._local_bypass: bool = False
        self._prior_bypass_state: bool = False

        bypass_config = self.context.config.section('BYPASS')
        self._bypass_flush_time = parse_interval(bypass_config.get('FLUSH_TIME'), default=62.0)

        self._input_names: typing.Set[str] = set()
        self._inputs: typing.List[BaseInstrument.Input] = list()

        self._notification_names: typing.Set[str] = set()
        self._notifications: typing.List[Notification] = list()

        self._flags: typing.List[Flag] = list()

        self._record_names: typing.Set[str] = set()
        self._records: typing.List[Record] = list()

        self._persistent_names: typing.Set[str] = set()
        self._persistent: typing.List[Persistent] = list()

        self._change_event_names: typing.Set[str] = set()
        self._change_events: typing.List[ChangeEvent] = list()

        self._instrument_metadata_record: typing.Optional[BaseDataOutput.ConstantRecord] = None
        self._instrument_metadata_fields: typing.Dict[str, StandardInstrument._MetadataField] = dict()

    def notification_state_changed(self) -> None:
        self._instrument_state_updated = True

    def _bypass_state_changed(self) -> None:
        self._instrument_state_updated = True
        self._average_state_updated = True

    @property
    def is_communicating(self) -> bool:
        return self._is_communicating

    @is_communicating.setter
    def is_communicating(self, value: bool) -> None:
        if not self._is_communicating and value:
            # Discard pending data created during start communications
            for i in self._inputs:
                i.drop_queued()
            for r in self._records:
                r.drop_queued()
            for p in self._persistent:
                p.drop_queued()
        if not value:
            # Discard instrument info if we don't currently have communications (retry failed)
            self._dynamic_instrument_info.clear()

        if self._is_communicating != value:
            self._instrument_state_updated = True
        self._is_communicating = value

    @staticmethod
    def _simplify_serial_number(n: typing.Union[str, bytes, int, float]) -> typing.Optional[typing.Union[str, int]]:
        if isinstance(n, int):
            return n
        if isinstance(n, float):
            try:
                return int(n)
            except (ValueError, OverflowError):
                return str(n)
        if isinstance(n, bytes):
            try:
                n = n.decode('utf-8')
            except UnicodeDecodeError:
                n = n.decode('ascii')
        n = n.strip()
        if n.startswith('#'):
            n = n[1:]
        try:
            return int(n)
        except (ValueError, OverflowError):
            pass
        if not n:
            return None
        return n

    def set_instrument_info(self, key: str, value: typing.Any) -> None:
        if value is None:
            if key not in self._dynamic_instrument_info:
                return
            del self._dynamic_instrument_info[key]
        else:
            if self._dynamic_instrument_info.get(key) == value:
                return
            self._dynamic_instrument_info[key] = value
        self._instrument_info_updated = True

    def set_serial_number(self, serial_number: typing.Union[bytes, str, int, float]) -> None:
        self.set_instrument_info('serial_number', self._simplify_serial_number(serial_number))

    def set_firmware_version(self, firmware_version: typing.Union[bytes, str, int, float]) -> None:
        self.set_instrument_info('firmware_version', self._simplify_serial_number(firmware_version))

    @property
    def bypassed(self) -> bool:
        return self._local_bypass or self.context.bus.bypassed

    @bypassed.setter
    def bypassed(self, state: bool) -> None:
        state = bool(state)
        if self._local_bypass != state:
            self._bypass_state_changed()
        self._local_bypass = state

    def input(self, name: str, send_to_bus: bool = True) -> Input:
        if name in self._input_names:
            raise ValueError(f"duplicate input name {name}")
        self._input_names.add(name)

        i = Input(self, name, self.context.config.section_or_constant('DATA', name), send_to_bus)
        if not isinstance(i.config, LayeredConfiguration):
            i.field.add_comment(self.context.config.comment('DATA', name))
        self._inputs.append(i)
        return i

    def input_array(self, name: str, send_to_bus: bool = True, dimensions: int = 1) -> ArrayInput:
        if name in self._input_names:
            raise ValueError(f"duplicate input name {name}")
        self._input_names.add(name)

        i = ArrayInput(self, name, self.context.config.section_or_constant('DATA', name),
                       send_to_bus=send_to_bus, dimensions=dimensions)
        if not isinstance(i.config, LayeredConfiguration):
            i.field.add_comment(self.context.config.comment('DATA', name))
        self._inputs.append(i)
        return i

    def variable(self, source: Input, name: str = None, code: str = None,
                 attributes: typing.Dict[str, typing.Any] = None) -> Variable:
        if not attributes:
            attributes = dict()
        return Variable(self, source, name, code, attributes)

    def variable_rate(self, source: Input, name: str = None, code: str = None,
                 attributes: typing.Dict[str, typing.Any] = None) -> VariableRate:
        if not attributes:
            attributes = dict()
        return VariableRate(self, source, name, code, attributes)

    def variable_last_valid(self, source: Input, name: str = None, code: str = None,
                 attributes: typing.Dict[str, typing.Any] = None) -> VariableRate:
        if not attributes:
            attributes = dict()
        return VariableLastValid(self, source, name, code, attributes)

    def variable_array(self, source: ArrayInput,
                       dimensions: typing.Optional[typing.Union[Dimension, typing.Iterable[Dimension]]] = None,
                       name: str = None, code: str = None,
                       attributes: typing.Dict[str, typing.Any] = None) -> ArrayVariable:
        if not attributes:
            attributes = dict()
        return ArrayVariable(self, source, dimensions, name, code, attributes)

    def variable_array_last_valid(self, source: ArrayInput,
                                  dimensions: typing.Optional[
                                      typing.Union[Dimension, typing.Iterable[Dimension]]] = None,
                                  name: str = None, code: str = None,
                                  attributes: typing.Dict[str, typing.Any] = None) -> ArrayVariableLastValid:
        if not attributes:
            attributes = dict()
        return ArrayVariableLastValid(self, source, dimensions, name, code, attributes)

    def variable_number_concentration(self, source: Input, name: str = None, code: str = None,
                                      attributes: typing.Dict[str, typing.Any] = None) -> Variable:
        v = self.variable(source, name or "number_concentration", code, attributes)

        def t(var: netcdf_var.Variable) -> None:
            netcdf_var.variable_number_concentration(var,
                                                     is_stp=(v.data.use_standard_temperature and
                                                             v.data.use_standard_pressure))
        v.data.configure_variable = t
        return v

    def variable_winds(self, speed: Input, direction: Input, name_suffix: str = None,
                       name_speed: str = None, name_direction: str = None,
                       code: str = None, code_speed: str = None, code_direction: str = None,
                       attributes: typing.Dict[str, typing.Any] = None,
                       attributes_speed: typing.Dict[str, typing.Any] = None,
                       attributes_direction: typing.Dict[str, typing.Any] = None) -> typing.Tuple[Variable, Variable]:
        if not name_speed:
            name_speed = "wind_speed"
            if name_suffix:
                name_speed = name_speed + name_suffix
        if code_speed is None and code is not None:
            code_speed = "WS" + code
        if attributes_speed is not None:
            if attributes is not None:
                c = attributes.copy()
                c.update(attributes_speed)
                attributes_speed = c
        else:
            attributes_speed = attributes
            if not attributes_speed:
                attributes_speed = dict()
        variable_speed = VariableVectorMagnitude(self, speed, name_speed, code_speed,
                                                 attributes_speed)
        variable_speed.data.configure_variable = netcdf_var.variable_wind_speed

        if not name_direction:
            name_direction = "wind_direction"
            if name_suffix:
                name_direction = name_direction + name_suffix
        if code_direction is None and code is not None:
            code_direction = "WS" + code
        if attributes_direction is not None:
            if attributes is not None:
                c = attributes.copy()
                c.update(attributes_direction)
                attributes_direction = c
        else:
            attributes_direction = attributes
            if not attributes_direction:
                attributes_direction = dict()
        variable_direction = VariableVectorDirection(self, direction, variable_speed, name_direction, code_direction,
                                                     attributes_direction)
        variable_direction.data.configure_variable = netcdf_var.variable_wind_direction

        return variable_speed, variable_direction

    def variable_total_scattering(self, source: ArrayInput,
                                  dimension: typing.Optional[Dimension] = None,
                                  name: str = None, code: str = None,
                                  attributes: typing.Dict[str, typing.Any] = None) -> ArrayVariable:
        v = self.variable_array(source, dimension, name or "scattering_coefficient",
                                code, attributes)

        def t(var: netcdf_var.Variable) -> None:
            netcdf_var.variable_total_scattering(var,
                                                 is_stp=(v.data.use_standard_temperature and
                                                         v.data.use_standard_pressure),
                                                 is_dried=v.data.is_dried)
        v.data.configure_variable = t
        return v

    def variable_back_scattering(self, source: ArrayInput,
                                  dimension: typing.Optional[Dimension] = None,
                                  name: str = None, code: str = None,
                                  attributes: typing.Dict[str, typing.Any] = None) -> ArrayVariable:
        v = self.variable_array(source, dimension, name or "backscattering_coefficient",
                                code, attributes)

        def t(var: netcdf_var.Variable) -> None:
            netcdf_var.variable_back_scattering(var,
                                                is_stp=(v.data.use_standard_temperature and
                                                        v.data.use_standard_pressure),
                                                is_dried=v.data.is_dried)
        v.data.configure_variable = t
        return v

    def variable_absorption(self, source: ArrayInput,
                            dimension: typing.Optional[Dimension] = None,
                            name: str = None, code: str = None,
                            attributes: typing.Dict[str, typing.Any] = None) -> ArrayVariable:
        v = self.variable_array(source, dimension, name or "light_absorption",
                                code, attributes)

        def t(var: netcdf_var.Variable) -> None:
            netcdf_var.variable_absorption(var,
                                           is_stp=(v.data.use_standard_temperature and
                                                   v.data.use_standard_pressure),
                                           is_dried=v.data.is_dried)
        v.data.configure_variable = t
        return v

    def variable_transmittance(self, source: ArrayInput,
                               dimension: typing.Optional[Dimension] = None,
                               name: str = None, code: str = None,
                               attributes: typing.Dict[str, typing.Any] = None) -> ArrayVariableLastValid:
        v = self.variable_array_last_valid(source, dimension, name or "transmittance",
                                           code, attributes)
        v.data.configure_variable = netcdf_var.variable_transmittance
        return v

    variable_ozone = _declare_variable_type(netcdf_var.variable_ozone, "ozone_mixing_ratio")
    variable_co2 = _declare_variable_type(netcdf_var.variable_co2, "carbon_dioxide_mixing_ratio")
    variable_temperature = _declare_variable_type(netcdf_var.variable_temperature)
    variable_air_temperature = _declare_variable_type(netcdf_var.variable_air_temperature)
    variable_dewpoint = _declare_variable_type(netcdf_var.variable_dewpoint)
    variable_air_dewpoint = _declare_variable_type(netcdf_var.variable_air_dewpoint)
    variable_rh = _declare_variable_type(netcdf_var.variable_rh)
    variable_air_rh = _declare_variable_type(netcdf_var.variable_air_rh)
    variable_pressure = _declare_variable_type(netcdf_var.variable_pressure)
    variable_air_pressure = _declare_variable_type(netcdf_var.variable_air_pressure)
    variable_delta_pressure = _declare_variable_type(netcdf_var.variable_delta_pressure)
    variable_flow = _declare_variable_type(netcdf_var.variable_flow)
    variable_sample_flow = _declare_variable_type(netcdf_var.variable_sample_flow, "sample_flow")

    variable_ebc = _declare_variable_array_type(
        netcdf_var.variable_ebc, "equivalent_black_carbon")
    variable_size_distribution_dN = _declare_variable_array_type(
        netcdf_var.variable_size_distribution_dN, "number_distribution")
    variable_size_distribution_dNdlogDp = _declare_variable_array_type(
        netcdf_var.variable_size_distribution_dNdlogDp, "normalized_number_distribution")

    def notification(self, name: str, is_warning=False) -> Notification:
        if name in self._notification_names:
            raise ValueError(f"duplicate notification name {name}")
        self._notification_names.add(name)

        n = Notification(self, name, is_warning)
        self._notifications.append(n)
        return n

    def flag(self, source: Notification, preferred_bit: typing.Optional[int] = None) -> Flag:
        f = Flag(self, source)
        self._flags.append(f)
        if preferred_bit:
            f.data.preferred_bit = preferred_bit
        return f

    def flag_bit(self, lookup: typing.Dict[int, Notification], bit: int, name: str, **kwargs) -> Flag:
        n = self.notification(name, **kwargs)
        lookup[bit] = n
        f = self.flag(n, preferred_bit=bit)
        return f

    def record(self, name: str = "data", apply_cutsize: bool = True, automatic: bool = True) -> Record:
        if name in self._record_names:
            raise ValueError(f"duplicate record name {name}")
        self._record_names.add(name)

        r = Record(self, name, apply_cutsize, automatic)

        # Make the assumption that anything with a cut size is on the system bypass (meaning bypassed while
        # acquisition is offline), so it would need a spinup flush too
        if not r.cutsize.constant_size:
            default_spinup = self._bypass_flush_time
        else:
            default_spinup = 0.0
        spinup_time = parse_interval(self.context.average_config.get("SPINUP_TIME"), default=default_spinup)
        if spinup_time > 0.0:
            r.average.start_flush(spinup_time)

        self._records.append(r)
        return r

    def report(self, *fields: BaseInstrument.Variable,
               flags: typing.Optional[typing.Iterable[BaseInstrument.Flag]] = None,
               auxiliary_variables: typing.Optional[typing.Iterable[BaseInstrument.Variable]] = None,
               record: typing.Optional[Record] = None,
               automatic: bool = True) -> Report:
        if not record:
            if self._records:
                record = self._records[0]
            else:
                record = self.record()

        return Report(self, record, fields, flags or (), auxiliary_variables or (), automatic)

    def persistent(self, name: str, send_to_bus: bool = True, save_value: bool = True) -> Persistent:
        if name in self._persistent_names:
            raise ValueError(f"duplicate persistent name {name}")
        self._persistent_names.add(name)

        p = Persistent(self, name, send_to_bus, save_value)

        if p.save_value:
            value, effective_time = self.context.persistent.load(name)
            if value is not None:
                p.load_prior(value, effective_time)

        self._persistent.append(p)
        return p

    def persistent_enum(self, name: str, enum_type: typing.Type[enum.Enum],
                        send_to_bus: bool = True, save_value: bool = True) -> PersistentEnum:
        if name in self._persistent_names:
            raise ValueError(f"duplicate persistent name {name}")
        self._persistent_names.add(name)

        p = PersistentEnum(self, name, enum_type, send_to_bus, save_value)

        if p.save_value:
            value, effective_time = self.context.persistent.load(name)
            if value is not None:
                p.load_prior(value, effective_time)

        self._persistent.append(p)
        return p

    state_float = _declare_state_type(float, BaseDataOutput.Float)
    state_integer = _declare_state_type(int, BaseDataOutput.Integer)
    state_unsigned_integer = _declare_state_type(int, BaseDataOutput.UnsignedInteger)
    state_string = _declare_state_type(str, BaseDataOutput.String)

    def state_array(self, source: Persistent,
                    dimensions: typing.Optional[typing.Union[Dimension, typing.Iterable[Dimension]]] = None,
                    name: str = None, code: str = None,
                    attributes: typing.Dict[str, typing.Any] = None, automatic: bool = True):
        if dimensions:
            if isinstance(dimensions, Dimension):
                dimensions = [dimensions]
            else:
                dimensions = list(dimensions)

        class StateHandler(State):
            class Field(BaseDataOutput.ArrayFloat):
                def __init__(self, name: str):
                    super().__init__(name)
                    self.state: typing.Optional["StateHandler"] = None
                    self.override: typing.Optional[typing.Union[typing.List[float], typing.List[typing.List]]] = None
                    self.template = BaseDataOutput.Field.Template.STATE

                @property
                def value(self) -> typing.Union[typing.List[float], typing.List[typing.List]]:
                    if self.override is not None:
                        return self.override
                    return self.state.source.value

                @property
                def dimensions(self) -> typing.Optional[typing.List[BaseDataOutput.ArrayFloat]]:
                    if self.state.dimensions:
                        return [d.data for d in self.state.dimensions]
                    return None

            def __init__(self, instrument: BaseInstrument, source: Persistent,
                         dimensions: typing.Optional[typing.Iterable[Dimension]],
                         name: str, code: typing.Optional[str], attributes: typing.Dict[str, typing.Any],
                         automatic: bool):
                super().__init__(instrument, source, name, code, attributes, automatic)
                self.dimensions = dimensions

            def apply_override(self, value: typing.Optional[typing.Union[typing.List[float],
                                                                         typing.List[typing.List]]]) -> None:
                self.data.override = value

        if not attributes:
            attributes = dict()
        return StateHandler(self, source, dimensions, name, code, attributes, automatic)

    def state_enum(self, source: PersistentEnum, typename: typing.Optional[str] = None,
                   name: str = None, code: str = None,
                   attributes: typing.Dict[str, typing.Any] = None, automatic: bool = True):
        enum_type = source.enum_type
        
        class StateHandler(State):
            class Field(BaseDataOutput.Enum):
                def __init__(self, name: str):
                    super().__init__(name)
                    self.state: typing.Optional["StateHandler"] = None
                    self.override: typing.Optional[enum_type] = None
                    self.template = BaseDataOutput.Field.Template.STATE

                @property
                def value(self) -> typing.Union[int, str]:
                    if self.override is not None:
                        return self.override.value
                    return self.state.source.value

                @property
                def enum(self) -> typing.Type[enum.Enum]:
                    return enum_type

                @property
                def typename(self) -> str:
                    if typename:
                        return typename
                    return super().typename

            def apply_override(self, value: typing.Optional[typing.List[float]]) -> None:
                self.data.override = value

        if not attributes:
            attributes = dict()
        return StateHandler(self, source, name, code, attributes, automatic)

    def state_measurement(self, source: Persistent, name: str = None, code: str = None,
                          attributes: typing.Dict[str, typing.Any] = None, automatic: bool = True):
        s = self.state_float(source, name, code, attributes, automatic)
        s.data.template = BaseDataOutput.Field.Template.STATE_MEASUREMENT
        s.data.use_cut_size = False
        return s

    def state_measurement_array(self, source: Persistent,
                                dimensions: typing.Optional[typing.Union[Dimension, typing.Iterable[Dimension]]] = None,
                                name: str = None, code: str = None,
                                attributes: typing.Dict[str, typing.Any] = None, automatic: bool = True):
        s = self.state_array(source, dimensions, name, code, attributes, automatic)
        s.data.template = BaseDataOutput.Field.Template.STATE_MEASUREMENT
        s.data.use_cut_size = False
        return s

    state_temperature = _declare_measurement_state_type(netcdf_var.variable_temperature)
    state_pressure = _declare_measurement_state_type(netcdf_var.variable_pressure)
    state_wall_total_scattering = _declare_measurement_state_array_type(
        netcdf_var.variable_wall_total_scattering, "wall_scattering_coefficient")
    state_wall_back_scattering = _declare_measurement_state_array_type(
        netcdf_var.variable_wall_back_scattering, "wall_backscattering_coefficient")

    def change_event(self, *state: State, name: str = "state") -> ChangeEvent:
        if name in self._change_event_names:
            raise ValueError(f"duplicate change event name {name}")
        self._change_event_names.add(name)

        e = ChangeEvent(self, name, state)
        self._change_events.append(e)
        return e

    def dimension(self, source: Persistent, name: str = None, code: str = None,
                  attributes: typing.Dict[str, typing.Any] = None) -> Dimension:
        if not attributes:
            attributes = dict()
        return Dimension(self, source, name, code, attributes)

    dimension_wavelength = _declare_dimension_type(
        netcdf_var.variable_wavelength, "wavelength")
    dimension_size_distribution_diameter = _declare_dimension_type(
        netcdf_var.variable_size_distribution_Dp, "diameter")
    dimension_size_distribution_diameter_electrical = _declare_dimension_type(
        netcdf_var.variable_size_distribution_Dp_electrical_mobility, "diameter")

    class _MetadataField(BaseDataOutput.String):
        def __init__(self, name: str):
            super().__init__(name)
            self.template = BaseDataOutput.Field.Template.METADATA
            self.latest_value: typing.Optional[str] = None

        @property
        def value(self) -> str:
            return self.latest_value

    def _update_instrument_metadata(self, info: typing.Dict[str, typing.Any]) -> None:
        for name, long_name in (
                ('manufacturer', "instrument manufacturer name"),
                ('model', "instrument model"),
                ('serial_number', "instrument serial number"),
                ('firmware_version', "instrument firmware version information"),
                ('calibration', "instrument calibration information"),
        ):
            value = info.get(name, None)
            target = self._instrument_metadata_fields.get(name)
            if not value:
                if not target:
                    continue
            else:
                if not target:
                    target = self._MetadataField(name)
                    target.attributes['long_name'] = long_name
                    self._instrument_metadata_fields[name] = target

                    if self._instrument_metadata_record is None:
                        self._instrument_metadata_record = self.context.data.constant_record("instrument")
                    self._instrument_metadata_record.constants.append(target)

            target.latest_value = value

    async def _send_instrument_state(self) -> None:
        notifications: typing.Set[str] = set()
        in_warning = False

        for n in self._notifications:
            if not n.value:
                continue
            if n.is_warning:
                in_warning = True
            notifications.add(n.name)

        state: typing.Dict[str, typing.Any] = {
            'communicating': self.is_communicating,
            'bypassed': self.bypassed,
            'warning': in_warning,
            'notifications': list(notifications),
        }
        await self.context.bus.set_instrument_state(state)

    def _update_averaging(self) -> None:
        is_bypassed = self.bypassed
        was_bypassed = self._prior_bypass_state
        self._prior_bypass_state = is_bypassed

        averaging_enabled = not is_bypassed
        for rec in self._records:
            rec.average.set_averaging(averaging_enabled)

        if not is_bypassed and was_bypassed:
            for rec in self._records:
                rec.average.start_flush(self._bypass_flush_time)

    async def emit(self) -> None:
        if self._instrument_info_updated:
            self._instrument_info_updated = False

            info: typing.Dict[str, typing.Any] = dict()
            info.update(self.instrument_info)
            info.update(self._dynamic_instrument_info)
            self._update_instrument_metadata(info)
            await self.context.bus.set_instrument_info(info)

        if self._instrument_state_updated:
            self._instrument_state_updated = False
            await self._send_instrument_state()

        if self._average_state_updated:
            self._average_state_updated = False
            self._update_averaging()

        data_record: typing.Dict[str, typing.Union[float, typing.List[float]]] = dict()
        for i in self._inputs:
            i.assemble_data(data_record)

        now = time.time()
        did_average = False
        for rec in self._records:
            if await rec.emit(now):
                did_average = True

        if did_average:
            # After all records have averaged, output an "average" of any inputs not included otherwise so things
            # (uplink) can display the values anyway
            average_record: typing.Dict[str, typing.Union[float, typing.List[float]]] = dict()
            for i in self._inputs:
                i.assemble_unaveraged(average_record)
            if average_record:
                await self.context.bus.emit_averaged_extra(average_record)

        for p in self._persistent:
            await p.emit(now)

        for rec in self._change_events:
            await rec.emit(now)

        if data_record:
            await self.context.bus.emit_data_record(data_record)
