import typing
import asyncio
import time
from math import nan
from forge.units import ONE_ATM_IN_HPA
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
            for var in self.variables:
                var.assemble_average(average_data)
            for var in self.auxiliary_variables:
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

    def set_averaging(self, enabled: bool) -> None:
        self.average.set_averaging(enabled)

    def start_flush(self, duration: float, now: float = None) -> None:
        self.average.start_flush(duration, now=now)

    def attach_report(self, report: "Report", automatic: bool = True) -> None:
        self.reports.add(report)
        if automatic:
            self.automatic_reports.add(report)

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


class DownstreamRecord(Record):
    def __init__(self, instrument: BaseInstrument, name: str, upstream_name: str, automatic: bool):
        super().__init__(instrument, name, True, automatic)
        self.upstream_record: typing.Optional[Record] = None
        self.upstream_name = upstream_name

    def _create_upstream(self) -> None:
        if self.upstream_record is not None:
            return
        self.upstream_record = Record(self.instrument, self.upstream_name, False, self.automatic)
        self.upstream_record.reports.update(self.reports)
        self.upstream_record.automatic_reports.update(self.automatic_reports)

        # Explicitly not on the cut size, so assume no flush needed on startup due to un-bypass
        spinup_time = parse_interval(self.instrument.context.average_config.get("SPINUP_TIME"), default=0)
        if spinup_time > 0.0:
            self.upstream_record.average.start_flush(spinup_time)

    def __call__(self) -> None:
        super()()
        if self.upstream_record:
            self.upstream_record()

    def report_generated(self, report: BaseInstrument.Report) -> None:
        super().report_generated(report)
        if self.upstream_record:
            self.upstream_record.report_generated(report)

    async def emit(self, now: float) -> bool:
        did_emit = False
        if self.upstream_record:
            did_emit = (await self.upstream_record.emit(now)) or did_emit
        did_emit = (await super().emit(now)) or did_emit
        return did_emit

    def drop_queued(self) -> None:
        super().drop_queued()

    # Note: we don't need to override the averaging/flush since the upstream isn't affected by the bypass to
    # begin with

    def attach_report(self, report: "Report", automatic: bool = True) -> None:
        super().attach_report(report, automatic)
        if self.upstream_record:
            self.upstream_record.attach_report(report, automatic)

    def attach_variable(self, var: BaseInstrument.Variable) -> None:
        if var.data.use_cut_size is not None and not var.data.use_cut_size:
            if not self.cutsize.constant_size or self._active_size.size != CutSize.Size.WHOLE:
                self._create_upstream()
                self.upstream_record.attach_variable(var)
                return
        super().attach_variable(var)

    def attach_auxiliary_variable(self, var: BaseInstrument.Variable) -> None:
        if var.data.use_cut_size is not None and not var.data.use_cut_size:
            if not self.cutsize.constant_size or self._active_size.size != CutSize.Size.WHOLE:
                self._create_upstream()
                self.upstream_record.attach_auxiliary_variable(var)
                return
        super().attach_auxiliary_variable(var)


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
            self.variables.append(v)
            self.record.attach_variable(v)
            if v.data.use_standard_temperature and self.record.data_record.standard_temperature is None:
                self.record.data_record.standard_temperature = 0.0
            if v.data.use_standard_pressure and self.record.data_record.standard_pressure is None:
                self.record.data_record.standard_pressure = ONE_ATM_IN_HPA

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

        self.record.attach_report(self, automatic)

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
