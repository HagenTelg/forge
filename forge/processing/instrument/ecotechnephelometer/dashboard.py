import typing
import numpy as np
from netCDF4 import Dataset, Group
from forge.dashboard.report.action import DashboardAction
from ..default.dashboard import Analyzer as BaseAnalyzer, DataRecord as BaseDataRecord, RecordAnalyzer, ConditionAccumulator, SpancheckRecord


class DataRecord(BaseDataRecord):
    def process_rh(self):
        sensor_rh = self.group.variables.get("sample_humidity")
        if sensor_rh is None:
            return
        sensor_rh = sensor_rh[...]
        accumulator = ConditionAccumulator.from_instrument_code(self.analyzer, "rh_suspect",
                                                                self.analyzer.target.Severity.ERROR)
        accumulator.emit_true(np.any([
            sensor_rh < -5.0,
            sensor_rh > 99.0
        ], axis=0), self.times)

    def analyze(self) -> None:
        if not self.analyzer.instrument:
            return
        self.process_rh()
        super().analyze()


class Analyzer(BaseAnalyzer):
    def record_analyzer(self, group: Group) -> typing.Optional["RecordAnalyzer"]:
        if group.name == "data":
            return DataRecord(self, group)
        elif group.name == "spancheck":
            return SpancheckRecord(self, group)
        return super().record_analyzer(group)


def analyze_acquisition(station: str, root: Dataset, target: DashboardAction) -> None:
    analyzer = Analyzer(station, root, target)
    analyzer.analyze()
