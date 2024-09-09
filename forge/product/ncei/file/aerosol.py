import typing
import logging
import time
import numpy as np
from pathlib import Path
from tempfile import TemporaryDirectory
from netCDF4 import Dataset, Variable
from forge.product.selection import InstrumentSelection, VariableSelection
from . import NCEIFile

_LOGGER = logging.getLogger(__name__)


class File(NCEIFile):
    @property
    def absorption_instrument(self) -> typing.Iterable[InstrumentSelection]:
        return [InstrumentSelection(
            require_tags=["absorption"],
            exclude_tags=["secondary", "aethalometer", "thermomaap"],
        )]

    @property
    def scattering_instrument(self) -> typing.Iterable[InstrumentSelection]:
        return [InstrumentSelection(
            require_tags=["scattering"],
            exclude_tags=["secondary"],
        )]

    @property
    def cpc_instrument(self) -> typing.Iterable[InstrumentSelection]:
        return [InstrumentSelection(
            require_tags=["cpc"],
            exclude_tags=["secondary"],
        )]

    @property
    def tags(self) -> typing.Set[str]:
        return {"aerosol"}

    @property
    def file_root_name(self) -> str:
        return "ESRL-GMD-AEROSOL_v1.0_HOUR"

    async def __call__(self, output_directory: Path) -> None:
        with TemporaryDirectory() as data_directory:
            data_directory = Path(data_directory)
            for subdir in ("absorption", "scattering", "cpc"):
                (data_directory / subdir).mkdir()
            await self.fetch_instrument_files({
                "absorption": self.absorption_instrument,
                "scattering": self.scattering_instrument,
                "cpc": self.cpc_instrument,
            }, data_directory)

            cpc_instrument = self.MergeInstrument()
            cpc_number_concentration = self.MergeVariable()
            async for root in self.iter_data_files(data_directory / "cpc"):
                cpc_instrument.integrate_file(root)
                cpc_number_concentration.integrate_selected(
                    root,
                    {"variable_name": "number_concentration"},
                    {"standard_name": "number_concentration_of_ambient_aerosol_particles_in_air"},
                )

            absorption_instrument = self.MergeInstrument()
            absorption_value = self.MergeVariable()
            async for root in self.iter_data_files(data_directory / "absorption"):
                absorption_instrument.integrate_file(root)
                absorption_value.integrate_selected(
                    root,
                    {"variable_name": "light_absorption"},
                    {"standard_name": "volume_absorption_coefficient_in_air_due_to_dried_aerosol_particles"},
                    {"standard_name": "volume_extinction_coefficient_in_air_due_to_ambient_aerosol_particles"},
                )

            scattering_instrument = self.MergeInstrument()
            scattering_total = self.MergeVariable()
            scattering_back = self.MergeVariable()
            scattering_pressure = self.MergeVariable()
            scattering_temperature = self.MergeVariable()
            scattering_humidity = self.MergeVariable()
            async for root in self.iter_data_files(data_directory / "scattering"):
                scattering_instrument.integrate_file(root)
                scattering_total.integrate_selected(
                    root,
                    {"variable_name": "scattering_coefficient"},
                    {"standard_name": "volume_scattering_coefficient_in_air_due_to_dried_aerosol_particles"},
                )
                scattering_back.integrate_selected(
                    root,
                    {"variable_name": "backscattering_coefficient"},
                    {"standard_name": "volume_backwards_scattering_coefficient_in_air_due_to_dried_aerosol_particles"},
                )
                scattering_pressure.integrate_selected(
                    root,
                    {"standard_name": "air_pressure"},
                )
                scattering_temperature.integrate_selected(
                    root,
                    {"standard_name": "air_temperature"},
                )
                scattering_humidity.integrate_selected(
                    root,
                    {"standard_name": "relative_humidity"},
                )

            async with self.output_file(output_directory, cpc_number_concentration, absorption_value,
                                        scattering_total, scattering_back) as file:
                if file is None:
                    _LOGGER.debug("No data found")
                    return

                file.root.keywords = "GCMD:DOC/NOAA/ESRL/GMD,GCMD:NEPHELOMETERS,GCMD:AEROSOL MONITOR,GCMD:CNC"

                if absorption_value.has_data:
                    g = await file.instrument(absorption_instrument, "light_absorption")
                    await file.variable(g, "absorption_coefficient", absorption_value, {
                        "standard_name": "volume_absorption_coefficient_in_air_due_to_dried_aerosol_particles",
                        "long_name": "absorption coefficient at STP",
                        "units": "Mm-1",
                        "C_format": "%7.2f",
                    }, is_stp=True)

                if cpc_number_concentration.has_data:
                    g = await file.instrument(cpc_instrument, "particle_concentration")
                    await file.variable(g, "number_concentration", cpc_number_concentration, {
                        "standard_name": "number_concentration_of_aerosol_particles_at_stp_in_air",
                        "long_name": "particle number concentration at STP",
                        "units": "cm-3",
                        "C_format": "%7.1f",
                    }, is_stp=True)

                if scattering_total.has_data or scattering_back.has_data:
                    g = await file.instrument(scattering_instrument, "light_scattering")
                    await file.variable(g, "scattering_coefficient", scattering_total, {
                        "standard_name": "volume_scattering_coefficient_in_air_due_to_dried_aerosol_particles",
                        "long_name": "total scattering coefficient at STP",
                        "units": "Mm-1",
                        "C_format": "%7.2f",
                    }, is_stp=True)
                    await file.variable(g, "backscattering_coefficient", scattering_back, {
                        "standard_name": "volume_backwards_scattering_coefficient_in_air_due_to_dried_aerosol_particles",
                        "long_name": "backwards hemispheric scattering coefficient at STP",
                        "units": "Mm-1",
                        "C_format": "%7.2f",
                    }, is_stp=True)
                    await file.variable(g, "sample_pressure", scattering_pressure, {
                        "standard_name": "air_pressure",
                        "long_name": "sample pressure",
                        "units": "hPa",
                        "C_format": "%6.1f",
                    })
                    await file.variable(g, "sample_temperature", scattering_temperature, {
                        "standard_name": "air_temperature",
                        "long_name": "sample temperature",
                        "units": "degC",
                        "C_format": "%5.1f",
                    })
                    await file.variable(g, "sample_humidity", scattering_humidity, {
                        "standard_name": "relative_humidity",
                        "long_name": "relative humidity",
                        "units": "%",
                        "C_format": "%5.1f",
                    })
