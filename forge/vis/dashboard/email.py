import typing
from forge.dashboard import CONFIGURATION
from forge.dashboard.database import Severity
from .entry import Entry


class EmailContents:
    def __init__(self, entry: Entry, severity: typing.Optional["Severity"],
                 text: str, html: typing.Optional[str]):
        self.entry = entry
        self.severity = severity
        self.text = text
        self.html = html

    @property
    def subject(self) -> str:
        if self.entry.station:
            if self.entry.status.abnormal:
                return f"{self.entry.status.name}: {self.entry.station.upper()} - {self.entry.display or self.entry.code}"
            else:
                return f"Report: {self.entry.station.upper()} - {self.entry.display or self.entry.code}"
        else:
            if self.entry.status.abnormal:
                return f"{self.entry.status.name}: {self.entry.display or self.entry.code}"
            else:
                return f"Report: {self.entry.display or self.entry.code}"

    @property
    def reply_to(self) -> typing.Set[str]:
        return set(CONFIGURATION.get('DASHBOARD.EMAIL.REPLY', []))

    @property
    def send_to(self) -> typing.Set[str]:
        return set()

    @property
    def expose_all_recipients(self) -> bool:
        return False
