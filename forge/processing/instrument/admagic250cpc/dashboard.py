import typing
from math import isfinite
from netCDF4 import Dataset, Group
from forge.dashboard.report.action import DashboardAction
from ..default.dashboard import Analyzer as BaseAnalyzer, DataRecord as BaseDataRecord, RecordAnalyzer, ConditionAccumulator


class DataRecord(BaseDataRecord):
    def process_pulse_height(self):
        pulse_height = self.group.variables.get("pulse_height")
        if pulse_height is None:
            return
        pulse_height = pulse_height[...]
        accumulator = ConditionAccumulator.from_instrument_code(self.analyzer, "pulse_height_low",
                                                                self.analyzer.target.Severity.ERROR)
        accumulator.emit_true(pulse_height < 400.0, self.times)

    def analyze(self) -> None:
        if not self.analyzer.instrument:
            return
        self.process_pulse_height()
        super().analyze()


class Analyzer(BaseAnalyzer):
    def record_analyzer(self, group: Group) -> typing.Optional["RecordAnalyzer"]:
        if group.name == "data":
            return DataRecord(self, group)
        return super().record_analyzer(group)


def analyze_acquisition(station: str, root: Dataset, target: DashboardAction) -> None:
    analyzer = Analyzer(station, root, target)
    analyzer.analyze()
