import typing
from math import nan, isfinite
from forge.acquisition import LayeredConfiguration
from forge.acquisition.average import AverageRecord
from .base import BaseInstrument, BaseDataOutput


class InputFieldControl:
    def __init__(self, config: LayeredConfiguration = None):
        self.use_standard_temperature: typing.Optional[bool] = None
        self.use_standard_pressure: typing.Optional[bool] = None
        self.is_dried: typing.Optional[bool] = None
        self.use_cut_size: typing.Optional[bool] = None

        self.comment: typing.Optional[str] = None
        self.override_description: typing.Optional[str] = None

        if config is None:
            return

        use_stp = config.get('STP')
        if use_stp is not None:
            use_stp = bool(use_stp)
            self.use_standard_temperature = use_stp
            self.use_standard_pressure = use_stp

        use_standard_temperature = config.get('STANDARD_TEMPERATURE')
        if use_standard_temperature is not None:
            use_standard_temperature = bool(use_standard_temperature)
            self.use_standard_temperature = use_standard_temperature

        use_standard_pressure = config.get('STANDARD_PRESSURE')
        if use_standard_pressure is not None:
            use_standard_pressure = bool(use_standard_pressure)
            self.use_standard_pressure = use_standard_pressure

        is_dried = config.get('DRIED')
        if is_dried is not None:
            is_dried = bool(is_dried)
            self.is_dried = is_dried

        use_cut_size = config.get('CUT_SIZE')
        if use_cut_size is not None:
            use_cut_size = bool(use_cut_size)
            self.use_cut_size = use_cut_size

    def add_comment(self, comment: str) -> None:
        if not comment:
            return
        if not self.comment:
            self.comment = comment
        else:
            self.comment = self.comment + "\n" + comment
            
    def apply(self, data: BaseDataOutput.Field, 
              calibration: typing.Optional[typing.List[float]] = None) -> None:
        if self.use_standard_temperature is not None:
            data.use_standard_temperature = self.use_standard_temperature
        if self.use_standard_pressure is not None:
            data.use_standard_pressure = self.use_standard_pressure
        if self.is_dried is not None:
            data.is_dried = self.is_dried
        if self.use_cut_size is not None:
            data.use_cut_size = self.use_cut_size

        if calibration and 'calibration_polynomial' not in data.attributes:
            data.attributes['calibration_polynomial'] = calibration
        if self.override_description and 'measurement_source_override' not in data.attributes:
            data.attributes['measurement_source_override'] = self.override_description
        if self.comment and 'comment' not in data.attributes:
            data.attributes['comment'] = self.comment


class Input(BaseInstrument.Input):
    def __init__(self, instrument: BaseInstrument, name: str, config: LayeredConfiguration,
                 send_to_bus: bool = True):
        super().__init__(instrument, name)
        self.instrument = instrument
        self.config = config
        self.send_to_bus = send_to_bus

        self.value: float = nan
        self._queued_data: typing.Optional[float] = None
        self.attached_to_record: bool = False
        self._queued_unaveraged: typing.Optional[float] = None

        self._overridden = False
        self._override_value: float = nan

        self.calibration: typing.List[float] = list()
        self.field = InputFieldControl()

        if isinstance(self.config, float):
            self.calibration.append(self.config)
            return

        if isinstance(self.config, str):
            fields = self.config.split(':', 2)
            self.field.override_description = str(self.config)
            if len(fields) == 1:
                self.instrument.context.bus.connect_data(None, fields[0], self._incoming_override)
            else:
                self.instrument.context.bus.connect_data(fields[0], fields[1], self._incoming_override)
            self._overridden = True
            return

        if isinstance(self.config, list):
            for c in self.config:
                self.calibration.append(float(c))
            return

        self.field = InputFieldControl(config)

        override_field = self.config.get('INPUT')
        if override_field:
            source = self.config.get('INSTRUMENT')
            self.instrument.context.bus.connect_data(source, override_field, self._incoming_override)
            self._overridden = True

            if source:
                self.field.override_description = f"{source}:{override_field}"
            else:
                self.field.override_description = override_field

            self.field.add_comment(self.config.comment('INPUT'))

        calibration = self.config.get('CALIBRATION')
        if calibration:
            for c in calibration:
                self.calibration.append(float(c))

            self.field.add_comment(self.config.comment('CALIBRATION'))
        else:
            scale = self.config.get('SCALE')
            if scale:
                self.calibration.append(0.0)
                self.calibration.append(float(scale))
                self.field.add_comment(self.config.comment('SCALE'))

    def __repr__(self) -> str:
        return f"Input({self.name}={self.value})"

    def __call__(self, value: float) -> float:
        if self._overridden:
            value = self._override_value
        if value is None:
            value = nan

        if self.calibration:
            result = 0.0
            accumulator = 1.0
            for c in self.calibration:
                result += c * accumulator
                accumulator *= value
            value = result

        self.value = value
        if self.send_to_bus:
            self._queued_data = value
            self._queued_unaveraged = value
        return self.value

    def __float__(self) -> float:
        return self.value

    def _incoming_override(self, value: typing.Any) -> None:
        if value is None:
            self._override_value = nan
            return
        try:
            self._override_value = float(value)
        except (ValueError, TypeError):
            return
        if not isfinite(self._override_value):
            self._override_value = nan

    def drop_queued(self) -> None:
        self._queued_data = None
        self._queued_unaveraged = None

    def assemble_data(self, record: typing.Dict[str, typing.Union[float, typing.List[float]]]) -> None:
        if self._queued_data is None:
            return
        record[self.name] = self._queued_data
        self._queued_data = None

    def average_consumed(self) -> None:
        self._queued_unaveraged = None

    def assemble_unaveraged(self, record: typing.Dict[str, typing.Union[float, typing.List[float]]]) -> None:
        if self.attached_to_record:
            return
        if self._queued_unaveraged is None:
            return
        record[self.name] = self._queued_unaveraged
        self._queued_unaveraged = None


class Variable(BaseInstrument.Variable):
    class Field(BaseDataOutput.Float):
        def __init__(self, name: str):
            super().__init__(name)
            self.variable: typing.Optional[Variable] = None
            self.template = BaseDataOutput.Field.Template.MEASUREMENT

        @property
        def value(self) -> float:
            return self.variable.value

    def __init__(self, instrument: BaseInstrument, source: Input,
                 name: str, code: typing.Optional[str], attributes: typing.Dict[str, typing.Any]):
        super().__init__(instrument, name or source.name, code, attributes)
        self.data.variable = self
        self.source = source
        self.average: typing.Optional[AverageRecord.Variable] = None
        source.field.apply(self.data, source.calibration)

    @property
    def value(self) -> float:
        if not self.average:
            return nan
        return float(self.average)

    def __float__(self) -> float:
        return self.value

    def assemble_average(self, record: typing.Dict[str, typing.Union[float, typing.List[float]]]) -> None:
        record[self.source.name] = self.value
        self.source.average_consumed()

    def __call__(self) -> None:
        self.average(float(self.source))

    def __repr__(self) -> str:
        return f"Variable({self.name} {self.source.name})"

    def clear(self) -> None:
        self.average.clear()

    def attach_to_record(self, record: BaseInstrument.Record) -> None:
        if self.average is None:
            self.average = record.average.variable()
        elif not record.average.has_entry(self.average):
            raise ValueError(f"variable {self.name} from {self.source.name} attached to multiple records")
        self.source.attached_to_record = True


class VariableRate(Variable):
    def __init__(self, instrument: BaseInstrument, source: Input,
                 name: str, code: typing.Optional[str], attributes: typing.Dict[str, typing.Any]):
        if 'cell_methods' not in attributes:
            attributes['cell_methods'] = "time: sum"
        super().__init__(instrument, source, name, code, attributes)

    def attach_to_record(self, record: BaseInstrument.Record) -> None:
        if self.average is None:
            self.average = record.average.rate()
        elif not record.average.has_entry(self.average):
            raise ValueError(f"variable {self.name} from {self.source.name} attached to multiple records")
        self.source.attached_to_record = True


class VariableLastValid(Variable):
    def __init__(self, instrument: BaseInstrument, source: Input,
                 name: str, code: typing.Optional[str], attributes: typing.Dict[str, typing.Any]):
        if 'cell_methods' not in attributes:
            attributes['cell_methods'] = "time: last"
        super().__init__(instrument, source, name, code, attributes)

    def attach_to_record(self, record: BaseInstrument.Record) -> None:
        if self.average is None:
            self.average = record.average.last_valid()
        elif not record.average.has_entry(self.average):
            raise ValueError(f"variable {self.name} from {self.source.name} attached to multiple records")
        self.source.attached_to_record = True


class VariableVectorMagnitude(Variable):
    def attach_to_record(self, record: BaseInstrument.Record) -> None:
        if self.average is None:
            self.average = record.average.vector()
        elif not record.average.has_entry(self.average):
            raise ValueError(f"variable {self.name} from {self.source.name} attached to multiple records")
        self.source.attached_to_record = True

    @property
    def value(self) -> float:
        if not self.average:
            return nan
        return self.average.magnitude

    def __call__(self) -> None:
        pass


class VariableVectorDirection(Variable):
    def __init__(self, instrument: BaseInstrument, source: Input, magnitude: VariableVectorMagnitude,
                 name: str, code: typing.Optional[str], attributes: typing.Dict[str, typing.Any]):
        if 'cell_methods' not in attributes:
            attributes['cell_methods'] = f"time: mean {magnitude.data.name}: vector_magnitude"
        if 'cell_methods' not in magnitude.data.attributes:
            magnitude.data.attributes['cell_methods'] = f"time: mean {name}: vector_direction"
        super().__init__(instrument, source, name, code, attributes)
        self.magnitude = magnitude

    def attach_to_record(self, record: BaseInstrument.Record) -> None:
        self.magnitude.attach_to_record(record)
        self.source.attached_to_record = True

    @property
    def value(self) -> float:
        if not self.average:
            return nan
        return self.magnitude.average.direction

    def __call__(self) -> None:
        self.magnitude.average(float(self.magnitude.source), float(self.source))
