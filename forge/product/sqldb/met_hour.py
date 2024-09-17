import typing
from .met import Update as BaseUpdate


class Update(BaseUpdate):
    @property
    def archive(self) -> str:
        return "avgh"

    @property
    def table_name(self) -> str:
        return f"{self.station}_hour"

    PRIMARY_KEY = [
        BaseUpdate.DateColumn("date"),
        BaseUpdate.HourColumn("hour"),
    ]
    EXTRA_KEY = [
        BaseUpdate.FractionalYearColumn("dd"),
    ]
