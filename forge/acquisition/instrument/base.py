import typing
from enum import Enum, IntEnum, auto
from forge.acquisition import LayeredConfiguration
from forge.acquisition.average import AverageRecord
from forge.acquisition.cutsize import CutSize
from forge.data.structure.variable import Variable as NetCDFVariable


class CommunicationsError(RuntimeError):
    pass


class BaseDataOutput:
    def __init__(self, station: str, source: str):
        self.station = station
        self.source = source
        self.tags: typing.Set[str] = set()

    class Field:
        class Template(Enum):
            NONE = auto()
            METADATA = auto()
            STATE = auto()
            CUT_SIZE = auto()
            DIMENSION = auto()
            MEASUREMENT = auto()
            STATE_MEASUREMENT = auto()

        def __init__(self, name: str):
            self.name: str = name
            self.template: BaseDataOutput.Field.Template = self.Template.NONE
            self.configure_variable: typing.Optional[typing.Callable[[NetCDFVariable], None]] = None
            self.attributes: typing.Dict[str, typing.Any] = dict()
            self.use_cut_size: bool = True
            self.use_standard_pressure: bool = False
            self.use_standard_temperature: bool = False
            self.is_dried: bool = True

    class Float(Field):
        @property
        def value(self) -> float:
            raise NotImplementedError

    class Integer(Field):
        @property
        def value(self) -> int:
            raise NotImplementedError

    class UnsignedInteger(Field):
        @property
        def value(self) -> int:
            raise NotImplementedError

    class String(Field):
        @property
        def value(self) -> str:
            raise NotImplementedError

    class Enum(Field):
        @property
        def value(self) -> typing.Union[int, str]:
            raise NotImplementedError

        @property
        def enum(self) -> typing.Type[Enum]:
            raise NotImplementedError

        @property
        def typename(self) -> str:
            return self.name + "_t"

    class ArrayFloat(Field):
        @property
        def value(self) -> typing.Union[typing.List[float], typing.List[typing.List]]:
            raise NotImplementedError

        @property
        def dimensions(self) -> typing.Optional[typing.List["BaseDataOutput.ArrayFloat"]]:
            return None

    class Flag:
        def __init__(self, name: str):
            self.name = name
            self.preferred_bit: typing.Optional[int] = None

        @property
        def value(self) -> bool:
            raise NotImplementedError

    class Record:
        def __init__(self):
            self.constants: typing.List[BaseDataOutput.Field] = list()
            self.standard_temperature: typing.Optional[float] = None
            self.standard_pressure: typing.Optional[float] = None

        def add_variable(self, field: "BaseDataOutput.Field") -> None:
            pass

        def add_flag(self, field: "BaseDataOutput.Flag") -> None:
            pass

    class MeasurementRecord(Record):
        def __init__(self):
            super().__init__()
            self.report_interval: typing.Optional[float] = None

        def __call__(self, start_time: float, end_time: float, total_seconds: float, total_samples: int) -> None:
            pass

    def measurement_record(self, name: str) -> "BaseDataOutput.MeasurementRecord":
        return self.MeasurementRecord()

    class StateRecord(Record):
        def __call__(self, now: float, historical: bool = False) -> None:
            pass

    def state_record(self, name: str) -> "BaseDataOutput.StateRecord":
        return self.StateRecord()

    class ConstantRecord:
        def __init__(self):
            self.constants: typing.List[BaseDataOutput.Field] = list()
            self.standard_temperature: typing.Optional[float] = None
            self.standard_pressure: typing.Optional[float] = None

        class _Constant:
            def __init__(self):
                self._value = None

            def __call__(self, value) -> None:
                self._value = value

            @property
            def value(self):
                return self._value

            @value.setter
            def value(self, value) -> None:
                self._value = value

        class _Lookup:
            def __init__(self, source, attr: str):
                self._source = source
                self._attr = attr

            @property
            def value(self):
                return getattr(self._source, self._attr, None)

        def string(self, name: str, attributes: typing.Dict[str, typing.Any] = None):
            class _Field(BaseDataOutput.ConstantRecord._Constant, BaseDataOutput.String):
                def __init__(self, name: str, attributes: typing.Dict[str, typing.Any] = None):
                    BaseDataOutput.ConstantRecord._Constant.__init__(self)
                    BaseDataOutput.String.__init__(self, name)
                    self.template = self.Template.METADATA
                    if attributes:
                        self.attributes.update(attributes)
            f = _Field(name, attributes)
            self.constants.append(f)
            return f

        def string_attr(self, name: str, source, attr: str, attributes: typing.Dict[str, typing.Any] = None):
            class _Field(BaseDataOutput.ConstantRecord._Lookup, BaseDataOutput.String):
                def __init__(self, name: str, source, attr: str, attributes: typing.Dict[str, typing.Any] = None):
                    BaseDataOutput.ConstantRecord._Lookup.__init__(self, source, attr)
                    BaseDataOutput.String.__init__(self, name)
                    self.template = self.Template.METADATA
                    if attributes:
                        self.attributes.update(attributes)
            f = _Field(name, source, attr, attributes)
            self.constants.append(f)
            return f

        def float(self, name: str, attributes: typing.Dict[str, typing.Any] = None):
            class _Field(BaseDataOutput.ConstantRecord._Constant, BaseDataOutput.Float):
                def __init__(self, name: str, attributes: typing.Dict[str, typing.Any] = None):
                    BaseDataOutput.ConstantRecord._Constant.__init__(self)
                    BaseDataOutput.Float.__init__(self, name)
                    self.template = self.Template.METADATA
                    if attributes:
                        self.attributes.update(attributes)
            f = _Field(name, attributes)
            self.constants.append(f)
            return f

        def float_attr(self, name: str, source, attr: str, attributes: typing.Dict[str, typing.Any] = None):
            class _Field(BaseDataOutput.ConstantRecord._Lookup, BaseDataOutput.Float):
                def __init__(self, name: str, source, attr: str, attributes: typing.Dict[str, typing.Any] = None):
                    BaseDataOutput.ConstantRecord._Lookup.__init__(self, source, attr)
                    BaseDataOutput.Float.__init__(self, name)
                    self.template = self.Template.METADATA
                    if attributes:
                        self.attributes.update(attributes)
            f = _Field(name, source, attr, attributes)
            self.constants.append(f)
            return f

        def integer(self, name: str, attributes: typing.Dict[str, typing.Any] = None):
            class _Field(BaseDataOutput.ConstantRecord._Constant, BaseDataOutput.Integer):
                def __init__(self, name: str, attributes: typing.Dict[str, typing.Any] = None):
                    BaseDataOutput.ConstantRecord._Constant.__init__(self)
                    BaseDataOutput.Integer.__init__(self, name)
                    self.template = self.Template.METADATA
                    if attributes:
                        self.attributes.update(attributes)
            f = _Field(name, attributes)
            self.constants.append(f)
            return f

        def integer_attr(self, name: str, source, attr: str, attributes: typing.Dict[str, typing.Any] = None):
            class _Field(BaseDataOutput.ConstantRecord._Lookup, BaseDataOutput.Integer):
                def __init__(self, name: str, source, attr: str, attributes: typing.Dict[str, typing.Any] = None):
                    BaseDataOutput.ConstantRecord._Lookup.__init__(self, source, attr)
                    BaseDataOutput.Integer.__init__(self, name)
                    self.template = self.Template.METADATA
                    if attributes:
                        self.attributes.update(attributes)
            f = _Field(name, source, attr, attributes)
            self.constants.append(f)
            return f

        def unsigned_integer(self, name: str, attributes: typing.Dict[str, typing.Any] = None):
            class _Field(BaseDataOutput.ConstantRecord._Constant, BaseDataOutput.UnsignedInteger):
                def __init__(self, name: str, attributes: typing.Dict[str, typing.Any] = None):
                    BaseDataOutput.ConstantRecord._Constant.__init__(self)
                    BaseDataOutput.UnsignedInteger.__init__(self, name)
                    self.template = self.Template.METADATA
                    if attributes:
                        self.attributes.update(attributes)
            f = _Field(name, attributes)
            self.constants.append(f)
            return f

        def unsigned_integer_attr(self, name: str, source, attr: str, attributes: typing.Dict[str, typing.Any] = None):
            class _Field(BaseDataOutput.ConstantRecord._Lookup, BaseDataOutput.UnsignedInteger):
                def __init__(self, name: str, source, attr: str, attributes: typing.Dict[str, typing.Any] = None):
                    BaseDataOutput.ConstantRecord._Lookup.__init__(self, source, attr)
                    BaseDataOutput.UnsignedInteger.__init__(self, name)
                    self.template = self.Template.METADATA
                    if attributes:
                        self.attributes.update(attributes)
            f = _Field(name, source, attr, attributes)
            self.constants.append(f)
            return f

        def array_float(self, name: str, attributes: typing.Dict[str, typing.Any] = None):
            class _Field(BaseDataOutput.ConstantRecord._Constant, BaseDataOutput.ArrayFloat):
                def __init__(self, name: str, attributes: typing.Dict[str, typing.Any] = None):
                    BaseDataOutput.ConstantRecord._Constant.__init__(self)
                    BaseDataOutput.ArrayFloat.__init__(self, name)
                    self.template = self.Template.METADATA
                    if attributes:
                        self.attributes.update(attributes)
            f = _Field(name, attributes)
            self.constants.append(f)
            return f

        def array_float_attr(self, name: str, source, attr: str, attributes: typing.Dict[str, typing.Any] = None):
            class _Field(BaseDataOutput.ConstantRecord._Lookup, BaseDataOutput.ArrayFloat):
                def __init__(self, name: str, source, attr: str, attributes: typing.Dict[str, typing.Any] = None):
                    BaseDataOutput.ConstantRecord._Lookup.__init__(self, source, attr)
                    BaseDataOutput.ArrayFloat.__init__(self, name)
                    self.template = self.Template.METADATA
                    if attributes:
                        self.attributes.update(attributes)
            f = _Field(name, source, attr, attributes)
            self.constants.append(f)
            return f


    def constant_record(self, name: str) -> "BaseDataOutput.ConstantRecord":
        return self.ConstantRecord()

    async def start(self) -> None:
        pass

    async def shutdown(self) -> None:
        pass


class BaseBusInterface:
    def __init__(self, source: str):
        self.source = source
        self.bypass_updated: typing.Optional[typing.Callable[[], None]] = None

    async def set_instrument_info(self, contents: typing.Dict[str, typing.Any]) -> None:
        pass

    async def set_instrument_state(self, contents: typing.Dict[str, typing.Any]) -> None:
        pass

    async def emit_data_record(self, contents: typing.Dict[str, typing.Union[float, typing.List[float]]]) -> None:
        pass

    async def emit_average_record(self, contents: typing.Dict[str, typing.Union[float, typing.List[float]]],
                                  cutsize: CutSize.Size = CutSize.Size.WHOLE) -> None:
        pass

    async def emit_averaged_extra(self, contents: typing.Dict[str, typing.Union[float, typing.List[float]]]) -> None:
        pass

    async def set_state_value(self, name: str, contents: typing.Any) -> None:
        pass

    async def set_bypass_held(self, held: bool) -> None:
        pass

    class LogType(Enum):
        INFO = "info"
        COMMUNICATIONS_ESTABLISHED = "communications_established"
        COMMUNICATIONS_LOST = "communications_lost"
        ERROR = "error"

    def log(self, message: str, auxiliary: typing.Dict[str, typing.Any] = None,
            type: "BaseBusInterface.LogType" = None) -> None:
        pass

    def connect_data(self, source: typing.Optional[str], field: str,
                     target: typing.Callable[[typing.Any], None]) -> None:
        pass

    def connect_command(self, command: str, handler: typing.Callable[[typing.Any], None]) -> None:
        pass

    async def start(self) -> None:
        pass

    async def shutdown(self) -> None:
        pass

    @property
    def bypassed(self) -> bool:
        return False


class BasePersistentInterface:
    def __init__(self):
        self.version: int = 1

    def load(self, name: str) -> typing.Tuple[typing.Any, typing.Optional[float]]:
        return None, None

    async def save(self, name: str, value: typing.Any, effective_time: typing.Optional[float]) -> None:
        pass


class BaseContext:
    def __init__(self, config: LayeredConfiguration, data: BaseDataOutput, bus: BaseBusInterface,
                 persistent: BasePersistentInterface):
        self.config = config
        self.data = data
        self.bus = bus
        self.persistent = persistent
        self.average_config: LayeredConfiguration = LayeredConfiguration()
        self.cutsize_config: LayeredConfiguration = LayeredConfiguration()


class BaseInstrument:
    INSTRUMENT_TYPE: str = None
    MANUFACTURER: typing.Optional[str] = None
    MODEL: typing.Optional[str] = None
    DISPLAY_LETTER: typing.Optional[str] = None
    TAGS: typing.Set[str] = frozenset()
    PERSISTENT_VERSION: int = 1

    def __init__(self, context: BaseContext):
        self.context = context

    @property
    def is_communicating(self) -> bool:
        raise NotImplementedError

    @is_communicating.setter
    def is_communicating(self, value: bool) -> None:
        raise NotImplementedError

    def notification_state_changed(self) -> None:
        pass

    async def emit(self, incomplete: bool = False) -> None:
        pass

    async def run(self) -> typing.NoReturn:
        raise NotImplementedError

    class Input:
        def __init__(self, instrument: "BaseInstrument", name: str):
            self.instrument = instrument
            self.name = name

        def __call__(self, value: float) -> float:
            raise NotImplementedError

        def __float__(self) -> float:
            raise NotImplementedError

        def drop_queued(self) -> None:
            raise NotImplementedError

        def assemble_data(self, record: typing.Dict[str, typing.Union[float, typing.List[float]]]) -> None:
            raise NotImplementedError

        def assemble_unaveraged(self, record: typing.Dict[str, typing.Union[float, typing.List[float]]]) -> None:
            raise NotImplementedError

    class Notification:
        def __init__(self, instrument: "BaseInstrument", name: str, is_warning: bool):
            self.instrument = instrument
            self.name = name
            self.is_warning = is_warning

        def __call__(self, value: bool) -> bool:
            raise NotImplementedError

        def __bool__(self) -> bool:
            raise NotImplementedError

    class Record:
        def __init__(self, instrument: "BaseInstrument", name: str):
            self.instrument = instrument
            self.name = name
            self.average = AverageRecord(self.instrument.context.average_config)

        def __call__(self) -> None:
            raise NotImplementedError

    class Report:
        def __init__(self, instrument: "BaseInstrument"):
            self.instrument = instrument

        def __call__(self) -> None:
            raise NotImplementedError

    class DataField:
        Field: BaseDataOutput.Field = BaseDataOutput.Field

        def __init__(self, instrument: "BaseInstrument", name: str,
                     code: typing.Optional[str], attributes: typing.Dict[str, typing.Any]):
            self.instrument = instrument
            self.name = name
            self.code = code

            self.data = self.Field(self.name)
            self.data.attributes.update(attributes)

            if self.code and 'variable_id' not in self.data.attributes:
                self.data.attributes['variable_id'] = self.code

        def __call__(self) -> None:
            pass

        def __repr__(self) -> str:
            return f"DataField({self.name})"

        def attach_to_record(self, record: "BaseInstrument.Record") -> None:
            pass

    class Variable(DataField):
        def __call__(self) -> None:
            raise NotImplementedError

    class Flag:
        Output: BaseDataOutput.Flag = BaseDataOutput.Flag

        def __init__(self, instrument: "BaseInstrument", name: str):
            self.instrument = instrument
            self.data = self.Output(name)
            self.name = name

        def __bool__(self) -> bool:
            raise NotImplementedError

        def __repr__(self) -> str:
            return f"Flag({self.name})"

        def __call__(self) -> None:
            raise NotImplementedError

        def attach_to_record(self, record: "BaseInstrument.Record") -> None:
            pass

    class Persistent:
        def __init__(self, instrument: "BaseInstrument", name: str):
            self.instrument = instrument
            self.name = name

        def __call__(self, value: typing.Any, deduplicate: bool = True):
            raise NotImplementedError

        async def emit(self, now: float) -> None:
            raise NotImplementedError

    class State(DataField):
        pass

    class ChangeEvent:
        def __init__(self, instrument: "BaseInstrument", name: str):
            self.instrument = instrument
            self.name = name

        def __call__(self) -> None:
            raise NotImplementedError

        async def emit(self, now: float) -> None:
            raise NotImplementedError

    class Dimension(DataField):
        Field: BaseDataOutput.ArrayFloat = BaseDataOutput.ArrayFloat


class BaseSimulator:
    def __init__(self):
        pass

    async def run(self) -> typing.NoReturn:
        raise NotImplementedError
