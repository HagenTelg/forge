import typing
import asyncio
from netCDF4 import Dataset
from pathlib import Path
from forge.temp import WorkingDirectory
from . import TableUpdate, InstrumentSelection


class Update(TableUpdate):
    @property
    def data_selection(self) -> typing.Iterable[InstrumentSelection]:
        return [InstrumentSelection(instrument_id=["XI"])]

    @property
    def conditions_selection(self) -> typing.Iterable[InstrumentSelection]:
        return [InstrumentSelection(
            require_tags=["scattering"],
            exclude_tags=["secondary"],
        )]

    def _is_data_file(self, root: Dataset) -> bool:
        for check in self.data_selection:
            if check.matches_file(root):
                return True
        return False

    async def __call__(self) -> None:
        async with WorkingDirectory() as data_directory:
            data_directory = Path(data_directory)

            await self.fetch_instrument_files(list(self.data_selection) + list(self.conditions_selection),
                                              data_directory)

            data_columns = [
                self.CoarseCutVariableColumn(
                    "BsB0_value", {"variable_name": "scattering_coefficient"}, wavelength_index=0),
                self.CoarseCutVariableColumn(
                    "BsB0_stddev", {"variable_name": "scattering_coefficient"}, wavelength_index=0,
                    statistics="stddev"),
                self.CoarseCutVariableColumn(
                    "BsG0_value", {"variable_name": "scattering_coefficient"}, wavelength_index=1),
                self.CoarseCutVariableColumn(
                    "BsG0_stddev", {"variable_name": "scattering_coefficient"}, wavelength_index=1,
                    statistics="stddev"),
                self.CoarseCutVariableColumn(
                    "BsR0_value", {"variable_name": "scattering_coefficient"}, wavelength_index=2),
                self.CoarseCutVariableColumn(
                    "BsR0_stddev", {"variable_name": "scattering_coefficient"}, wavelength_index=2,
                    statistics="stddev"),
                self.CoarseCutVariableColumn(
                    "BbsB0_value", {"variable_name": "backscattering_coefficient"}, wavelength_index=0),
                self.CoarseCutVariableColumn(
                    "BbsB0_stddev", {"variable_name": "backscattering_coefficient"}, wavelength_index=0,
                    statistics="stddev"),
                self.CoarseCutVariableColumn(
                    "BbsG0_value", {"variable_name": "backscattering_coefficient"}, wavelength_index=1),
                self.CoarseCutVariableColumn(
                    "BbsG0_stddev", {"variable_name": "backscattering_coefficient"}, wavelength_index=1,
                    statistics="stddev"),
                self.CoarseCutVariableColumn(
                    "BbsR0_value", {"variable_name": "backscattering_coefficient"}, wavelength_index=2),
                self.CoarseCutVariableColumn(
                    "BbsR0_stddev", {"variable_name": "backscattering_coefficient"}, wavelength_index=2,
                    statistics="stddev"),

                self.FineCutVariableColumn(
                    "BsB1_value", {"variable_name": "scattering_coefficient"}, wavelength_index=0),
                self.FineCutVariableColumn(
                    "BsB1_stddev", {"variable_name": "scattering_coefficient"}, wavelength_index=0,
                    statistics="stddev"),
                self.FineCutVariableColumn(
                    "BsG1_value", {"variable_name": "scattering_coefficient"}, wavelength_index=1),
                self.FineCutVariableColumn(
                    "BsG1_stddev", {"variable_name": "scattering_coefficient"}, wavelength_index=1,
                    statistics="stddev"),
                self.FineCutVariableColumn(
                    "BsR1_value", {"variable_name": "scattering_coefficient"}, wavelength_index=2),
                self.FineCutVariableColumn(
                    "BsR1_stddev", {"variable_name": "scattering_coefficient"}, wavelength_index=2,
                    statistics="stddev"),
                self.FineCutVariableColumn(
                    "BbsB1_value", {"variable_name": "backscattering_coefficient"}, wavelength_index=0),
                self.FineCutVariableColumn(
                    "BbsB1_stddev", {"variable_name": "backscattering_coefficient"}, wavelength_index=0,
                    statistics="stddev"),
                self.FineCutVariableColumn(
                    "BbsG1_value", {"variable_name": "backscattering_coefficient"}, wavelength_index=1),
                self.FineCutVariableColumn(
                    "BbsG1_stddev", {"variable_name": "backscattering_coefficient"}, wavelength_index=1,
                    statistics="stddev"),
                self.FineCutVariableColumn(
                    "BbsR1_value", {"variable_name": "backscattering_coefficient"}, wavelength_index=2),
                self.FineCutVariableColumn(
                    "BbsR1_stddev", {"variable_name": "backscattering_coefficient"}, wavelength_index=2,
                    statistics="stddev"),

                self.CoarseCutVariableColumn(
                    "BaB0_value", {"variable_name": "light_absorption"}, wavelength_index=0),
                self.CoarseCutVariableColumn(
                    "BaB0_stddev", {"variable_name": "light_absorption"}, wavelength_index=0,
                    statistics="stddev"),
                self.CoarseCutVariableColumn(
                    "BaG0_value", {"variable_name": "light_absorption"}, wavelength_index=1),
                self.CoarseCutVariableColumn(
                    "BaG0_stddev", {"variable_name": "light_absorption"}, wavelength_index=1,
                    statistics="stddev"),
                self.CoarseCutVariableColumn(
                    "BaR0_value", {"variable_name": "light_absorption"}, wavelength_index=2),
                self.CoarseCutVariableColumn(
                    "BaR0_stddev", {"variable_name": "light_absorption"}, wavelength_index=2,
                    statistics="stddev"),

                self.FineCutVariableColumn(
                    "BaB1_value", {"variable_name": "light_absorption"}, wavelength_index=0),
                self.FineCutVariableColumn(
                    "BaB1_stddev", {"variable_name": "light_absorption"}, wavelength_index=0,
                    statistics="stddev"),
                self.FineCutVariableColumn(
                    "BaG1_value", {"variable_name": "light_absorption"}, wavelength_index=1),
                self.FineCutVariableColumn(
                    "BaG1_stddev", {"variable_name": "light_absorption"}, wavelength_index=1,
                    statistics="stddev"),
                self.FineCutVariableColumn(
                    "BaR1_value", {"variable_name": "light_absorption"}, wavelength_index=2),
                self.FineCutVariableColumn(
                    "BaR1_stddev", {"variable_name": "light_absorption"}, wavelength_index=2,
                    statistics="stddev"),

                self.VariableColumn(
                    "N_value", {"variable_name": "number_concentration"}),
                self.VariableColumn(
                    "N_stddev", {"variable_name": "number_concentration"}, statistics="stddev"),
            ]

            extra_columns = [
                self.CoarseCutVariableColumn(
                    "Ts0_value", {"variable_name": "sample_temperature"}),
                self.CoarseCutVariableColumn(
                    "Ts0_stddev", {"variable_name": "sample_temperature"}, statistics="stddev"),
                self.CoarseCutVariableColumn(
                    "RHs0_value", {"variable_name": "sample_humidity"}),
                self.CoarseCutVariableColumn(
                    "RHs0_stddev", {"variable_name": "sample_humidity"}, statistics="stddev"),
                self.CoarseCutVariableColumn(
                    "Ps0_value", {"variable_name": "sample_temperature"}),
                self.CoarseCutVariableColumn(
                    "Ps0_stddev", {"variable_name": "sample_temperature"}, statistics="stddev"),

                self.FineCutVariableColumn(
                    "Ts1_value", {"variable_name": "sample_temperature"}),
                self.FineCutVariableColumn(
                    "Ts1_stddev", {"variable_name": "sample_temperature"}, statistics="stddev"),
                self.FineCutVariableColumn(
                    "RHs1_value", {"variable_name": "sample_humidity"}),
                self.FineCutVariableColumn(
                    "RHs1_stddev", {"variable_name": "sample_humidity"}, statistics="stddev"),
                self.FineCutVariableColumn(
                    "Ps1_value", {"variable_name": "sample_temperature"}),
                self.FineCutVariableColumn(
                    "Ps1_stddev", {"variable_name": "sample_temperature"}, statistics="stddev"),
            ]

            async for update_start, update_end, update_files in self.aligned_files(data_directory):
                updates = list()
                for file in update_files:
                    if self._is_data_file(file):
                        for c in data_columns:
                            u = c(file)
                            if u:
                                updates.append(u)
                    for c in extra_columns:
                        u = c(file)
                        if u:
                            updates.append(u)
                await self.apply_updates(update_start, update_end, updates)

