import typing
import time
import forge.data.structure.variable as netcdf_var
from forge.acquisition import LayeredConfiguration
from forge.acquisition.util import parse_interval
from .base import BaseInstrument, BaseContext, BaseDataOutput
from .variable import Input, Variable
from .flag import Notification, Flag
from .record import Report, Record
from .state import Persistent, State, ChangeEvent


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
        self._inputs: typing.List[Input] = list()

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

    def set_serial_number(self, serial_number: typing.Union[bytes, str]) -> None:
        self.set_instrument_info('serial_number', self._simplify_serial_number(serial_number))

    def set_firmware_version(self, firmware_version: typing.Union[bytes, str]) -> None:
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

    def input(self, name: str) -> Input:
        if name in self._input_names:
            raise ValueError(f"duplicate input name {name}")
        self._input_names.add(name)

        i = Input(self, name, self.context.config.section_or_constant('DATA', name))
        if not isinstance(i.config, LayeredConfiguration):
            i.comment = self.context.config.comment('DATA', name)
        self._inputs.append(i)
        return i

    def variable(self, source: Input, name: str = None, code: str = None,
                 attributes: typing.Dict[str, typing.Any] = None) -> Variable:
        if not attributes:
            attributes = dict()
        return Variable(self, source, name, code, attributes)

    def variable_number_concentration(self, source: Input, name: str = None, code: str = None,
                                      attributes: typing.Dict[str, typing.Any] = None) -> Variable:
        v = self.variable(source, name or "number_concentration", code, attributes)

        def t(var: netcdf_var.Variable) -> None:
            netcdf_var.variable_number_concentration(var, is_stp=(v.data.use_standard_temperature and
                                                                  v.data.use_standard_pressure))
        v.data.configure_variable = t
        return v

    @staticmethod
    def _declare_variable_type(configure: typing.Callable[[netcdf_var.Variable], None],
                               default_name: str = None):
        def method(self: "StandardInstrument", source: Input, name: str = None, code: str = None,
                   attributes: typing.Dict[str, typing.Any] = None):
            v = self.variable(source, name or default_name, code, attributes)
            v.data.configure_variable = configure
            return v
        return method

    variable_ozone = _declare_variable_type(netcdf_var.variable_ozone, "ozone_mixing_ratio")
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

    def notification(self, name: str, is_warning=False) -> Notification:
        if name in self._notification_names:
            raise ValueError(f"duplicate notification name {name}")
        self._notification_names.add(name)

        n = Notification(self, name, is_warning)
        self._notifications.append(n)
        return n

    def flag(self, source: Notification) -> Flag:
        f = Flag(self, source)
        self._flags.append(f)
        return f

    def flag_bit(self, lookup: typing.Dict[int, Notification], bit: int, name: str, **kwargs) -> Flag:
        n = self.notification(name, **kwargs)
        lookup[bit] = n
        f = self.flag(n)
        f.data.preferred_bit = bit
        return f

    def record(self, name: str = "data", apply_cutsize: bool = True, automatic: bool = True) -> Record:
        if name in self._record_names:
            raise ValueError(f"duplicate record name {name}")
        self._record_names.add(name)

        r = Record(self, name, apply_cutsize, automatic)
        self._records.append(r)
        return r

    def report(self, *fields: BaseInstrument.Variable,
               flags: typing.Optional[typing.Iterable[BaseInstrument.Flag]] = None,
               record: typing.Optional[Record] = None) -> Report:
        if not record:
            if self._records:
                record = self._records[0]
            else:
                record = self.record()

        return Report(self, record, fields, flags or ())

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

    @staticmethod
    def _declare_state_type(value_type: typing.Type, field_type: typing.Type[BaseDataOutput.Field]):
        class StateHandler(State):
            class Field(field_type):
                def __init__(self, name: str):
                    super().__init__(name)
                    self.persistent: typing.Optional[Persistent] = None
                    self.override: typing.Optional[value_type] = None

                @property
                def value(self) -> value_type:
                    if self.override is not None:
                        return self.override
                    return self.persistent.value

            def apply_override(self, value: typing.Optional[value_type]) -> None:
                self.data.override = value

        def method(self: "BaseInstrument", source: Persistent, name: str = None, code: str = None,
                   attributes: typing.Dict[str, typing.Any] = None, automatic: bool = True):
            if not attributes:
                attributes = dict()
            return StateHandler(self, source, name, code, attributes, automatic)
        return method

    state_float = _declare_state_type(float, BaseDataOutput.Float)
    state_integer = _declare_state_type(int, BaseDataOutput.Integer)
    state_unsigned_integer = _declare_state_type(int, BaseDataOutput.UnsignedInteger)
    state_string = _declare_state_type(str, BaseDataOutput.String)

    def change_event(self, *state: State, name: str = "state") -> ChangeEvent:
        if name in self._change_event_names:
            raise ValueError(f"duplicate change event name {name}")
        self._change_event_names.add(name)

        e = ChangeEvent(self, name, state)
        self._change_events.append(e)
        return e

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
        await self.context.bus.emit_instrument_state(state)

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
            await self.context.bus.emit_instrument_info(info)

        if self._instrument_state_updated:
            self._instrument_state_updated = False
            await self._send_instrument_state()

        if self._average_state_updated:
            self._average_state_updated = False
            self._update_averaging()

        data_record: typing.Dict[str, float] = dict()
        for i in self._inputs:
            i.assemble_data(data_record)
        if data_record:
            await self.context.bus.emit_data_record(data_record)

        now = time.time()
        for rec in self._records:
            await rec.emit(now)

        for p in self._persistent:
            await p.emit(now)

        for rec in self._change_events:
            await rec.emit(now)
