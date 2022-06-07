import typing
from enum import Enum, auto
from forge.acquisition import LayeredConfiguration
from forge.acquisition.average import AverageRecord
from forge.acquisition.cutsize import CutSize
from forge.data.structure.variable import Variable as NetCDFVariable


class CommunicationsError(RuntimeError):
    pass


class BaseDataOutput:
    class Field:
        class Template(Enum):
            NONE = auto()
            METADATA = auto()
            STATE = auto()
            CUT_SIZE = auto()
            MEASUREMENT = auto()

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

    class ArrayFloat(Field):
        @property
        def value(self) -> typing.List[float]:
            raise NotImplementedError

        @property
        def dimension(self) -> typing.Optional["BaseDataOutput.ArrayFloat"]:
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

    async def emit_instrument_info(self, contents: typing.Dict[str, typing.Any]) -> None:
        pass

    async def emit_instrument_state(self, contents: typing.Dict[str, typing.Any]) -> None:
        pass

    async def emit_data_record(self, contents: typing.Dict[str, float]) -> None:
        pass

    async def emit_average_record(self, contents: typing.Dict[str, float],
                                  cutsize: CutSize.Size = CutSize.Size.WHOLE) -> None:
        pass

    async def emit_state_value(self, name: str, contents: typing.Any) -> None:
        pass

    def connect_data(self, source: typing.Optional[str], field: str,
                     target: typing.Callable[[typing.Any], None]) -> None:
        pass

    async def start(self) -> None:
        pass

    async def shutdown(self) -> None:
        pass

    @property
    def bypassed(self) -> bool:
        return False


class BasePersistentInterface:
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

    async def emit(self) -> None:
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
        def __init__(self, instrument: "BaseInstrument", name: str, code: typing.Optional[str],
                     attributes: typing.Dict[str, typing.Any]):
            super().__init__(instrument, name, code, attributes)
            self.code = code

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
        def __init__(self, instrument: "BaseInstrument", name: str,
                     code: typing.Optional[str], attributes: typing.Dict[str, typing.Any]):
            super().__init__(instrument, name, code, attributes)
            self.code = code

            if self.code and 'variable_id' not in self.data.attributes:
                self.data.attributes['variable_id'] = self.code

    class ChangeEvent:
        def __init__(self, instrument: "BaseInstrument", name: str):
            self.instrument = instrument
            self.name = name

        def __call__(self) -> None:
            raise NotImplementedError

        async def emit(self, now: float) -> None:
            raise NotImplementedError


class BaseSimulator:
    def __init__(self):
        pass

    async def run(self) -> typing.NoReturn:
        raise NotImplementedError
