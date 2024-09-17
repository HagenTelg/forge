import typing
from .aerosol import Update as BaseUpdate


class Update(BaseUpdate):
    @property
    def archive(self) -> str:
        return "avgd"

    @property
    def table_name(self) -> str:
        return f"{self.station}_da"

    PRIMARY_KEY = [
        BaseUpdate.DateColumn("date"),
    ]
    EXTRA_KEY = [
        BaseUpdate.FractionalYearColumn("dd"),
    ]
