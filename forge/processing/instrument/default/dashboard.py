import typing
import logging
import numpy as np
from math import isfinite
from abc import ABC, abstractmethod
from netCDF4 import Dataset, Group, Variable
from forge.dashboard.report.action import DashboardAction
from forge.timeparse import parse_iso8601_time, parse_iso8601_duration
from forge.processing.datafile import record_times, true_ranges
from .flags import DashboardFlag
from ..lookup import instrument_data

_LOGGER = logging.getLogger(__name__)


class Analyzer:
    def __init__(self, station: str, root: Dataset, target: DashboardAction):
        self.station = station
        self.root = root
        self.target = target
        self.instrument: str = root.instrument
        self.source: str = root.instrument_id

        self.expected_record_interval: typing.Optional[float] = None
        time_coverage_resolution = getattr(self.root, "time_coverage_resolution", None)
        if time_coverage_resolution is not None:
            self.expected_record_interval = parse_iso8601_duration(str(time_coverage_resolution))

        self.file_start_time: typing.Optional[float] = None
        time_coverage_start = getattr(self.root, "time_coverage_start", None)
        if time_coverage_start is not None:
            self.file_start_time = parse_iso8601_time(str(time_coverage_start)).timestamp()

        self.file_end_time: typing.Optional[float] = None
        time_coverage_end = getattr(self.root, "time_coverage_end", None)
        if time_coverage_end is not None:
            self.file_end_time = parse_iso8601_time(str(time_coverage_end)).timestamp()

    def record_analyzer(self, group: Group) -> typing.Optional["RecordAnalyzer"]:
        if group.name == "data":
            return DataRecord(self, group)
        return None

    def analyze(self) -> None:
        for g in self.root.groups.values():
            converter = self.record_analyzer(g)
            if not converter:
                continue
            converter.analyze()


class ConditionAccumulator:
    def __init__(self, analyzer: Analyzer, code: str, severity: DashboardAction.Severity):
        self.analyzer = analyzer
        self.code = code
        self.severity = severity
        self._start: typing.Optional[float] = None
        self._end: typing.Optional[float] = None

    @classmethod
    def from_instrument_code(cls, analyzer: Analyzer, dashboard_code: str,
                             severity: DashboardAction.Severity) -> "ConditionAccumulator":
        return cls(analyzer, analyzer.source + "-" + analyzer.instrument + "-" + dashboard_code, severity)

    def _complete(self) -> None:
        if not self._start:
            return
        condition_start = self._start
        condition_end = self._end
        self._start = None
        self._end = None

        if not condition_end:
            condition_end = self.analyzer.file_end_time
        if not condition_end:
            return

        self.analyzer.target.conditions.append(self.analyzer.target.Condition(
            self.code, self.severity,
            start_time=condition_start, end_time=condition_end
        ))

    def __call__(self, start_time: typing.Optional[float], end_time: typing.Optional[float], present: bool) -> None:
        if not present:
            return self._complete()
        if not self._start:
            if not start_time:
                start_time = self.analyzer.file_start_time
            if not start_time:
                return
            self._start = start_time
        self._end = end_time

    def finish(self) -> None:
        return self._complete()

    def emit_true(self, truth_values: np.ndarray, times: np.ndarray) -> None:
        for start_index, end_index in true_ranges(truth_values):
            start_time = float(times[start_index][0])
            end_time = float(times[end_index - 1][1])

            self.analyzer.target.conditions.append(self.analyzer.target.Condition(
                self.code, self.severity,
                start_time=start_time, end_time=end_time
            ))


class RecordAnalyzer(ABC):
    def __init__(self, analyzer: Analyzer, group: Group):
        self.analyzer = analyzer
        self.group = group

    @abstractmethod
    def analyze(self) -> None:
        pass


class DataRecord(RecordAnalyzer):
    def __init__(self, analyzer: Analyzer, record: Group):
        super().__init__(analyzer, record)

        self.times = record_times(
            self.group.variables["time"][...] / 1000.0,
            expected_record_interval=self.analyzer.expected_record_interval,
            file_start_time=self.analyzer.file_start_time,
            file_end_time=self.analyzer.file_end_time,
        ).T

    def analyze(self) -> None:
        self.set_data_watchdog()
        if not self.analyzer.instrument:
            return
        self.convert_system_flags()

    def convert_system_flags(self) -> None:
        system_flags = self.group.variables.get("system_flags")
        if system_flags is None:
            return
        dashboard_flags = instrument_data(self.analyzer.instrument, 'flags', 'dashboard_flags')
        if not dashboard_flags:
            return
        self.convert_flags(system_flags, dashboard_flags)

    def convert_flags(self, flags_variable: Variable,
                      flags_output: typing.Dict[str, DashboardFlag]) -> None:
        flag_meanings = flags_variable.flag_meanings.split(' ')
        flag_masks = flags_variable.flag_masks
        bit_lookup: typing.Dict[int, ConditionAccumulator] = dict()
        for i in range(len(flag_meanings)):
            flag_name = flag_meanings[i]
            dashboard_flag = flags_output.get(flag_name)
            if dashboard_flag is None:
                continue
            if not dashboard_flag.instrument_flag:
                continue
            if len(flag_meanings) == 1:
                flag_bits = int(flag_masks)
            else:
                flag_bits = flag_masks[i]
            bit_lookup[flag_bits] = ConditionAccumulator.from_instrument_code(
                self.analyzer, flag_name, dashboard_flag.severity
            )

        flags_variable = flags_variable[...]
        for bit, flag in bit_lookup.items():
            flag.emit_true(np.bitwise_and(flags_variable, bit) != 0, self.times)

    def set_data_watchdog(self) -> None:
        for var in self.group.variables.values():
            if var.name == "system_flags":
                continue
            if len(var.dimensions) < 1:
                continue
            if "time" not in var.dimensions:
                continue
            measurement_type = getattr(var, "coverage_content_type")
            if measurement_type != "physicalMeasurement":
                continue
            if not np.issubdtype(var.dtype, np.floating):
                continue
            if not np.any(np.isfinite(var)):
                continue

            self.analyzer.target.watchdogs.add(self.analyzer.target.Watchdog(
                self.analyzer.source + "-data", self.analyzer.target.Severity.ERROR,
                None,
                last_seen=self.analyzer.file_end_time,
            ))
            break


class SpancheckRecord(RecordAnalyzer):
    def analyze(self) -> None:
        if not self.analyzer.instrument:
            return
        event_times = self.group.variables.get("time")
        if event_times is None:
            return
        percent_errors = self.percent_errors()
        if not percent_errors:
            return
        for i in range(len(event_times)):
            spancheck_time: float = float(event_times[i]) / 1000.0
            if not self.analyzer.file_start_time:
                # If we don't have a start time, assume the first instance is the ongoing case
                if i == 0:
                    continue
            elif spancheck_time < self.analyzer.file_start_time:
                continue
            if self.analyzer.file_end_time and spancheck_time > self.analyzer.file_end_time:
                continue

            values = np.concatenate([p[i].flatten() for p in percent_errors])
            average_percent_error = np.nanmean(values)

            data = ",".join([
                f"{v:.2f}" if isfinite(v) else "" for v in (average_percent_error, *list(values))
            ])

            self.analyzer.target.events.append(self.analyzer.target.Event(
                self.analyzer.source + "-" + self.analyzer.instrument + "-spancheck",
                self.analyzer.target.Severity.INFO,
                data,
                occurred_at=spancheck_time
            ))

    def percent_errors(self) -> typing.List[Variable]:
        result: typing.List[Variable] = list()
        for name in ("scattering_percent_error", "backscattering_percent_error"):
            var = self.group.variables.get(name)
            if var is None:
                continue
            result.append(var)
        return result


def analyze_acquisition(station: str, root: Dataset, target: DashboardAction) -> None:
    analyzer = Analyzer(station, root, target)
    analyzer.analyze()
