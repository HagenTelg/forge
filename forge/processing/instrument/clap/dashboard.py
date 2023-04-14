import typing
from netCDF4 import Dataset, Group
from forge.dashboard.report.action import DashboardAction
from ..default.dashboard import Analyzer as BaseAnalyzer, RecordAnalyzer


class StateRecord(RecordAnalyzer):
    def process_spot_number(self):
        spot_number = self.group.variables.get("spot_number")
        if spot_number is None:
            return
        spot_number = int(spot_number[-1])
        if spot_number >= 8:
            self.analyzer.target.notifications.add(self.analyzer.target.Notification(
                self.analyzer.source + "-" + self.analyzer.instrument + "-finalspot",
                self.analyzer.target.Severity.WARNING,
            ))

    def analyze(self) -> None:
        if not self.analyzer.instrument:
            return
        self.process_spot_number()


class Analyzer(BaseAnalyzer):
    def record_analyzer(self, group: Group) -> typing.Optional["RecordAnalyzer"]:
        if group.name == "state":
            return StateRecord(self, group)
        return super().record_analyzer(group)


def analyze_acquisition(station: str, root: Dataset, target: DashboardAction) -> None:
    analyzer = Analyzer(station, root, target)
    analyzer.analyze()
