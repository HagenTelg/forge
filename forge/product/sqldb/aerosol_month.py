import typing
from .aerosol import Update as BaseUpdate


class Update(BaseUpdate):
    @property
    def archive(self) -> str:
        return "avgm"

    @property
    def table_name(self) -> str:
        return f"{self.station}_mo"

    PRIMARY_KEY = [
        BaseUpdate.DateColumn("date"),
    ]
    EXTRA_KEY = [
        BaseUpdate.FractionalYearColumn("dd"),
    ]
