import typing
import asyncio
import time
from math import nan
from forge.acquisition import LayeredConfiguration
from forge.acquisition.cutsize import CutSize
from forge.acquisition.util import parse_interval
from .base import BaseInstrument, BaseDataOutput


class Record(BaseInstrument.Record):
    class _CurrentCutSizeField(BaseDataOutput.Float):
        def __init__(self, record: "Record"):
            super().__init__("cut_size")
            self.template = BaseDataOutput.Field.Template.CUT_SIZE
            self.record = record

        @property
        def value(self) -> float:
            if not self.record._active_size or self.record._active_size.size == CutSize.Size.WHOLE:
                return nan
            return self.record._active_size.size.value

    class _CutSizeSchedule(CutSize):
        def __init__(self, config: typing.Optional[LayeredConfiguration]):
            super().__init__(config)
            if isinstance(config, LayeredConfiguration):
                self.flush_time: float = parse_interval(config.get("FLUSH_TIME"), default=62.0)
            else:
                self.flush_time: float = 62.0

    def __init__(self, instrument: BaseInstrument, name: str, apply_cutsize: bool, automatic: bool):
        super().__init__(instrument, name)
        self.automatic = automatic

        if apply_cutsize:
            self.cutsize = self._CutSizeSchedule(self.instrument.context.cutsize_config)
        else:
            self.cutsize = self._CutSizeSchedule(None)
        self.reports: typing.Set[Report] = set()
        self.automatic_reports: typing.Set[Report] = set()

        self.variables: typing.Set[BaseInstrument.Variable] = set()
        self.auxiliary_variables: typing.Set[BaseInstrument.Variable] = set()
        self._variable_names: typing.Set[str] = set()

        self.flags: typing.Set[BaseInstrument.Flag] = set()
        self._flag_names: typing.Set[str] = set()

        self._automatic_updated_reports: typing.Set[BaseInstrument.Report] = set()
        self._queued: bool = False

        self.data_record = self.instrument.context.data.measurement_record(name)
        self._active_size: CutSize.Active = self.cutsize.current()

        now = time.time()
        self._active_size.activate(now)
        if self.cutsize.constant_size:
            if self._active_size.size != CutSize.Size.WHOLE:
                self.data_record.constants.append(self._CurrentCutSizeField(self))
        else:
            self.data_record.add_variable(self._CurrentCutSizeField(self))
            flushing_for = (self._active_size.scheduled_time + self.cutsize.flush_time) - now
            if flushing_for > 0.0:
                self.average.start_flush(flushing_for, now=now)

    def __repr__(self) -> str:
        return ("Record(" + self.name + "=[" + repr(self.reports) +
                f"],auto={len(self._automatic_updated_reports)}/{len(self.automatic_reports)})")

    def __call__(self) -> None:
        self._queued = True

    def report_generated(self, report: BaseInstrument.Report) -> None:
        if not self.automatic:
            return
        if report not in self.automatic_reports:
            return
        if report in self._automatic_updated_reports:
            self._queued = True
        self._automatic_updated_reports.add(report)
        if len(self._automatic_updated_reports) == len(self.automatic_reports):
            self._queued = True

    async def emit(self, now: float) -> bool:
        if not self._queued:
            return False
        self._queued = False
        self._automatic_updated_reports.clear()

        try:
            a = self.average(now=now)
            if not a:
                return False

            average_data: typing.Dict[str, typing.Union[float, typing.List[float]]] = dict()
            for rep in self.reports:
                for var in rep.variables:
                    var.assemble_average(average_data)
                for var in rep.auxiliary_variables:
                    var.assemble_average(average_data)
            if average_data:
                await self.instrument.context.bus.emit_average_record(average_data, self._active_size.size)

            self.data_record(a.start_time, a.end_time, a.total_seconds, a.total_samples)
        finally:
            if not self.cutsize.constant_size:
                self._active_size = self.cutsize.current(now=now)
                if self._active_size.activate():
                    self.average.start_flush((self._active_size.scheduled_time + self.cutsize.flush_time) - now,
                                             now=now)
        return True

    def drop_queued(self) -> None:
        self._queued = False
        self.average.reset()

    def attach_variable(self, var: BaseInstrument.Variable) -> None:
        if var in self.auxiliary_variables:
            raise ValueError(f"variable {repr(var)} already a display variable in record {self.name}")
        if var in self.variables:
            return

        if var.data.name in self._variable_names:
            raise ValueError(f"duplicate variable {repr(var)} in record {self.name}")
        self._variable_names.add(var.data.name)

        var.attach_to_record(self)
        self.variables.add(var)
        self.data_record.add_variable(var.data)

    def attach_flag(self, flag: BaseInstrument.Flag) -> None:
        if flag in self.flags:
            return

        if flag.data.name in self._flag_names:
            raise ValueError(f"duplicate flag {repr(flag)} in record {self.name}")
        self._flag_names.add(flag.data.name)

        flag.attach_to_record(self)
        self.flags.add(flag)
        self.data_record.add_flag(flag.data)

    def attach_auxiliary_variable(self, var: BaseInstrument.Variable) -> None:
        if var in self.variables:
            raise ValueError(f"variable {repr(var)} already a data variable in record {self.name}")
        if var in self.auxiliary_variables:
            return

        var.attach_to_record(self)
        self.auxiliary_variables.add(var)


class Report(BaseInstrument.Report):
    def __init__(self, instrument: BaseInstrument, record: Record,
                 variables: typing.Iterable[BaseInstrument.Variable],
                 flags: typing.Iterable[BaseInstrument.Flag],
                 auxiliary_variables: typing.Iterable[BaseInstrument.Variable],
                 automatic: bool):
        super().__init__(instrument)
        self.record = record
        self.variables: typing.List[BaseInstrument.Variable] = list()
        self.auxiliary_variables: typing.List[BaseInstrument.Variable] = list()
        self.flags: typing.List[BaseInstrument.Flag] = list()

        for v in variables:
            if v is None:
                continue
            self.attach_variable(v)

        for v in auxiliary_variables:
            if v is None:
                continue
            self.auxiliary_variables.append(v)
            self.record.attach_auxiliary_variable(v)

        for f in flags:
            if f is None:
                continue
            self.flags.append(f)
            self.record.attach_flag(f)

        self.record.reports.add(self)
        if automatic:
            self.record.automatic_reports.add(self)

    def attach_variable(self, variable: BaseInstrument.Variable) -> None:
        self.variables.append(variable)
        self.record.attach_variable(variable)

    def __repr__(self) -> str:
        return "Report(" + repr(self.variables) + "," + repr(self.flags) + ")"

    def __call__(self) -> None:
        if not self.instrument.is_communicating:
            return

        for v in self.variables:
            v()
        for v in self.auxiliary_variables:
            v()
        for f in self.flags:
            f()
        self.record.report_generated(self)
