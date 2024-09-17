import typing
from .met import Update as BaseUpdate


class Update(BaseUpdate):
    @property
    def archive(self) -> str:
        return "clean"

    @property
    def table_name(self) -> str:
        return f"{self.station}_minute"

    PRIMARY_KEY = [
        BaseUpdate.DateColumn("date"),
        BaseUpdate.TimeColumn("time"),
    ]
    EXTRA_KEY = [
        BaseUpdate.FractionalYearColumn("dd"),
    ]
