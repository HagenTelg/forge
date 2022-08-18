import typing
from math import nan, isfinite
from forge.acquisition import LayeredConfiguration
from forge.acquisition.average import AverageRecord
from .base import BaseInstrument, BaseDataOutput


class Input(BaseInstrument.Input):
    def __init__(self, instrument: BaseInstrument, name: str, config: LayeredConfiguration):
        super().__init__(instrument, name)
        self.instrument = instrument
        self.config = config

        self.value: float = nan
        self._queued_data: typing.Optional[float] = None
        self.attached_to_record: bool = False
        self._queued_unaveraged: typing.Optional[float] = None

        self._overridden = False
        self._override_value: float = nan

        self.calibration: typing.List[float] = list()
        self.comment: typing.Optional[str] = None
        self.override_description: typing.Optional[str] = None

        if isinstance(self.config, float):
            self.calibration.append(self.config)
            return

        if isinstance(self.config, str):
            fields = self.config.split(':', 2)
            self.override_description = str(self.config)
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

        override_field = self.config.get('INPUT')
        if override_field:
            source = self.config.get('INSTRUMENT')
            self.instrument.context.bus.connect_data(source, override_field, self._incoming_override)
            self._overridden = True

            if source:
                self.override_description = f"{source}:{override_field}"
            else:
                self.override_description = override_field

            if not self.comment:
                self.comment = self.config.comment('INPUT')
            else:
                self.comment = self.comment + "\n" + self.config.comment('INPUT')

        calibration = self.config.get('CALIBRATION')
        if calibration:
            for c in calibration:
                self.calibration.append(float(c))

            if not self.comment:
                self.comment = self.config.comment('CALIBRATION')
            else:
                self.comment = self.comment + "\n" + self.config.comment('CALIBRATION')

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
            return float(self.variable)

    def __init__(self, instrument: BaseInstrument, source: Input,
                 name: str, code: typing.Optional[str], attributes: typing.Dict[str, typing.Any]):
        super().__init__(instrument, name or source.name, code, attributes)
        self.data.variable = self
        self.source = source
        self.average: typing.Optional[AverageRecord.Variable] = None

        if self.source.calibration and 'calibration_polynomial' not in self.data.attributes:
            self.data.attributes['calibration_polynomial'] = self.source.calibration
        if self.source.override_description and 'measurement_source_override' not in self.data.attributes:
            self.data.attributes['measurement_source_override'] = self.source.override_description
        if self.source.comment and 'comment' not in self.data.attributes:
            self.data.attributes['comment'] = self.source.comment

    def __float__(self) -> float:
        if not self.average:
            return nan
        return float(self.average)

    def assemble_average(self, record: typing.Dict[str, typing.Union[float, typing.List[float]]]) -> None:
        record[self.source.name] = float(self)
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
