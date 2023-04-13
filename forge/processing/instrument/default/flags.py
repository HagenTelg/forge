import typing
from forge.dashboard import Severity


class DashboardFlag:
    def __init__(self, severity: Severity, title: str, text: typing.Optional[str] = None,
                 instrument_flag: bool = True):
        self.severity = severity
        self.title = title
        self.text = text
        self.instrument_flag = instrument_flag


dashboard_flags: typing.Dict[str, DashboardFlag] = dict()
