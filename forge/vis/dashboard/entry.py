import typing
import enum


class Entry:
    class Status(enum.Enum):
        OK = "ok"
        OFFLINE = "offline"
        FAILED = "failed"

        @property
        def abnormal(self) -> bool:
            return self == Entry.Status.OFFLINE or self == self == Entry.Status.FAILED

    def __init__(self, station: typing.Optional[str], code: str, status: "Entry.Status",
                 updated: float):
        self.station = station
        self.code = code
        self.status = status
        self.updated = updated

    def to_status(self) -> typing.Dict[str, typing.Any]:
        result = {
            'station': self.station or '',
            'code': self.code,
            'status': self.status.value,
            'updated_ms': self.updated_ms,
        }
        if self.station:
            result['station'] = self.station
        display = self.display
        if display:
            result['display'] = display
        return result

    @property
    def display(self) -> typing.Optional[str]:
        return None

    @property
    def updated_ms(self) -> int:
        return int(round(self.updated * 1000))
