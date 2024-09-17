import typing
from .aerosol import Update as BaseUpdate


class Update(BaseUpdate):
    @property
    def archive(self) -> str:
        return "avgh"

    @property
    def table_name(self) -> str:
        return f"{self.station}_hr"

    PRIMARY_KEY = [
        BaseUpdate.DateColumn("date"),
        BaseUpdate.HourColumn("hour"),
    ]
    EXTRA_KEY = [
        BaseUpdate.FractionalYearColumn("dd"),
    ]
