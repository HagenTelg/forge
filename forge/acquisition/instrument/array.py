import typing
from math import nan, isfinite
from forge.acquisition import LayeredConfiguration
from forge.acquisition.average import AverageRecord
from .base import BaseInstrument, BaseDataOutput
from .dimension import Dimension


def _normalize_array(value: typing.Iterable[float]) -> typing.List[float]:
    result: typing.List[float] = list()
    for v in value:
        if v is None:
            v = nan
        else:
            v = float(v)
            if not isfinite(v):
                v = nan
        result.append(v)
    return result


class ArrayInput(BaseInstrument.Input):
    def __init__(self, instrument: BaseInstrument, name: str, config: LayeredConfiguration,
                 send_to_bus: bool = True):
        super().__init__(instrument, name)
        self.instrument = instrument
        self.config = config
        self.send_to_bus = send_to_bus

        self.value: typing.List[float] = list()
        self._queued_data: typing.Optional[typing.List[float]] = None
        self.attached_to_record: bool = False
        self._queued_unaveraged: typing.Optional[typing.List[float]] = None

        self._overridden = False
        self._override_value: typing.List[float] = list()

        self.calibration: typing.List[float] = list()
        self.constant: typing.Optional[typing.List[float]] = None
        self.comment: typing.Optional[str] = None
        self.override_description: typing.Optional[str] = None

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
            self._override_value = _normalize_array(self.config)
            self._overridden = True
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

            comment = self.config.comment('INPUT')
            if comment:
                if not self.comment:
                    self.comment = comment
                else:
                    self.comment = self.comment + "\n" + comment

        constant = self.config.get('CONSTANT')
        if constant:
            self._override_value = _normalize_array(constant)
            self._overridden = True

            comment = self.config.comment('CONSTANT')
            if comment:
                if not self.comment:
                    self.comment = self.config.comment('CONSTANT')
                else:
                    self.comment = self.comment + "\n" + comment

        calibration = self.config.get('CALIBRATION')
        if calibration:
            for c in calibration:
                self.calibration.append(float(c))

            comment = self.config.comment('CALIBRATION')
            if comment:
                if not self.comment:
                    self.comment = comment
                else:
                    self.comment = self.comment + "\n" + comment

    def _convert_value(self, value: float) -> float:
        if value is None:
            value = nan
        else:
            value = float(value)
            if not isfinite(value):
                value = nan

        if self.calibration:
            result = 0.0
            accumulator = 1.0
            for c in self.calibration:
                result += c * accumulator
                accumulator *= value
            value = result

        return value

    def __call__(self, value: typing.Iterable[float]) -> typing.Iterable[float]:
        if self._overridden:
            value = self._override_value
        elif value is None:
            value = []
        else:
            value = _normalize_array(value)

        self.value.clear()
        for v in value:
            self.value.append(self._convert_value(v))

        if self.send_to_bus:
            self._queued_data = value
            self._queued_unaveraged = value
        return self.value

    def __getitem__(self, key):
        return self.value[key]

    def __float__(self) -> float:
        return nan

    def _incoming_override(self, value: typing.Any) -> None:
        if value is None:
            self._override_value = []
            return
        try:
            self._override_value = _normalize_array(value)
        except (ValueError, TypeError):
            return

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


class ArrayVariable(BaseInstrument.Variable):
    class Field(BaseDataOutput.ArrayFloat):
        def __init__(self, name: str):
            super().__init__(name)
            self.variable: typing.Optional[ArrayVariable] = None
            self.template = BaseDataOutput.Field.Template.MEASUREMENT

        @property
        def value(self) -> typing.List[float]:
            return self.variable.value

        @property
        def dimension(self) -> typing.Optional["BaseDataOutput.ArrayFloat"]:
            if self.variable.dimension:
                return self.variable.dimension.data
            return None

    def __init__(self, instrument: BaseInstrument, source: ArrayInput, dimension: typing.Optional[Dimension],
                 name: str, code: typing.Optional[str], attributes: typing.Dict[str, typing.Any]):
        super().__init__(instrument, name or source.name, code, attributes)
        self.data.variable = self
        self.dimension = dimension
        self.source = source
        self.average: typing.Optional[AverageRecord.Array] = None

        if self.source.calibration and 'calibration_polynomial' not in self.data.attributes:
            self.data.attributes['calibration_polynomial'] = self.source.calibration
        if self.source.override_description and 'measurement_source_override' not in self.data.attributes:
            self.data.attributes['measurement_source_override'] = self.source.override_description
        if self.source.comment and 'comment' not in self.data.attributes:
            self.data.attributes['comment'] = self.source.comment

    @property
    def value(self) -> typing.List[float]:
        if not self.average:
            return []
        return self.average.value

    def __getitem__(self, key):
        if not self.average:
            raise IndexError
        return self.average.value[key]

    def assemble_average(self, record: typing.Dict[str, typing.Union[float, typing.List[float]]]) -> None:
        record[self.source.name] = self.value
        self.source.average_consumed()

    def __call__(self) -> None:
        self.average(self.source.value)

    def __repr__(self) -> str:
        return f"ArrayVariable({self.name} {self.source.name})"

    def clear(self) -> None:
        self.average.clear()

    def attach_to_record(self, record: BaseInstrument.Record) -> None:
        if self.average is None:
            self.average = record.average.array()
        elif not record.average.has_entry(self.average):
            raise ValueError(f"variable {self.name} from {self.source.name} attached to multiple records")
        self.source.attached_to_record = True
