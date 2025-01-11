import typing
import asyncio
from netCDF4 import Dataset
from pathlib import Path
from forge.temp import WorkingDirectory
from . import TableUpdate, InstrumentSelection


class Update(TableUpdate):
    @property
    def data_selection(self) -> typing.Iterable[InstrumentSelection]:
        return [InstrumentSelection(instrument_id=["XM1"])]

    async def __call__(self) -> None:
        async with WorkingDirectory() as data_directory:
            data_directory = Path(data_directory)

            await self.fetch_instrument_files(self.data_selection, data_directory)

            data_columns = [
                self.VariableColumn("P", {"variable_id": "P"}),
                self.VariableColumn( "WI", {"variable_id": "WI"}),
                self.VariableColumn( "U", [{"variable_id": "U"}, {"variable_id": "U1"}]),
                self.VariableColumn( "T", [{"variable_id": "T"}, {"variable_id": "T1"}]),
                self.VariableColumn( "T1", {"variable_id": "T2"}),
                self.VariableColumn( "T2", {"variable_id": "T3"}),
                self.VariableColumn( "WS", [{"variable_id": "WS"}, {"variable_id": "WS1"}]),
                self.VariableColumn( "WD", [{"variable_id": "WD"}, {"variable_id": "WD1"}]),
                self.VariableColumn( "WDg", [{"variable_id": "WD"}, {"variable_id": "WD1"}],
                                    statistics="stability_factor"),
            ]

            async for segment_files in self.aligned_files(data_directory):
                updates = list()
                for file in segment_files:
                    for c in data_columns:
                        u = c(file)
                        if u:
                            updates.append(u)
                await self.apply_updates(updates)

