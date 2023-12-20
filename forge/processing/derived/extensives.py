import typing
import logging
import netCDF4
import numpy as np
import forge.data.structure.variable as netcdf_var
from forge.data.flags import parse_flags
from forge.data.structure.stp import standard_temperature, standard_pressure
from ..context import SelectedData, SelectedVariable
from ..context.variable import EmptySelectedVariable
from .wavelength import adjust_wavelengths, AdjustWavelengthParameters

_LOGGER = logging.getLogger(__name__)


def calculate_scattering(
        absorption: np.ndarray,
        extinction: np.ndarray,
) -> np.ndarray:
    return extinction - absorption


def calculate_absorption(
        scattering: np.ndarray,
        extinction: np.ndarray,
) -> np.ndarray:
    return extinction - scattering


def calculate_extinction(
        scattering: np.ndarray,
        absorption: np.ndarray,
) -> np.ndarray:
    return scattering + absorption


def write_extensives(
        extensives: SelectedData,
        cpc,
        scattering,
        absorption,
        extinction=None,
        wavelengths: typing.Union[typing.List[float], typing.Tuple[float, ...]] = (450.0, 550.0, 700.0),
        is_stp: bool = True,
        wavelength_adjustment: typing.Optional[AdjustWavelengthParameters] = None,
) -> typing.Tuple[SelectedVariable, SelectedVariable, SelectedVariable, SelectedVariable, SelectedVariable]:
    extensives.set_wavelengths(wavelengths)

    def find_variable(source: SelectedData, selector, *auxiliary) -> typing.Optional[typing.Union[SelectedVariable, typing.Tuple[SelectedVariable, ...]]]:
        for var in source.select_variable(selector, *auxiliary, commit_variable=False, commit_auxiliary=False):
            return var
        return None

    def setup_variable(destination: SelectedVariable) -> None:
        if is_stp:
            standard_temperature(destination.parent)
            standard_pressure(destination.parent)
            ancillary_variables = set(getattr(destination.variable, "ancillary_variables", "").split())
            ancillary_variables.add("standard_pressure")
            ancillary_variables.add("standard_temperature")
            destination.variable.ancillary_variables = " ".join(ancillary_variables)

        destination.variable.cell_methods = "time: mean"

    def copy_metadata(output: SelectedVariable, group_name: str, source: SelectedVariable) -> None:
        def get_root(var: SelectedVariable):
            root = var.parent
            while True:
                n = root.parent
                if n is None:
                    break
                root = n
            return root

        output_root = get_root(output)
        source_root = get_root(source)

        dest_instrument = output_root.groups.get('instrument')
        if dest_instrument is None:
            dest_instrument = output_root.createGroup('instrument')

        dest_group = dest_instrument.createGroup(group_name)
        for copy_attr in ("instrument_id", "forge_tags", "acquisition_start_time", "instrument_vocabulary",
                          "instrument", "history"):
            value = getattr(source_root, copy_attr, None)
            if value is None:
                continue
            setattr(dest_group, copy_attr, value)

        source_group = source_root.groups.get('instrument', None)
        if source_group is not None:
            for name, source_var in source_group.variables.items():
                if len(source_var.dimensions) > 0:
                    continue
                if isinstance(source_var.datatype, netCDF4.EnumType):
                    continue
                dest_var = dest_group.createVariable(name, source_var.dtype)
                dest_var[0] = source_var[0]
                for attr in source_var.ncattrs():
                    dest_var.setncattr(attr, source_var.getncattr(attr))

    def apply_wavelengths(destination: SelectedVariable, source: SelectedVariable) -> None:
        destination[:] = adjust_wavelengths(source, wavelengths, parameters=wavelength_adjustment)

    cpc_var = None
    if cpc:
        cpc = SelectedData.ensure_data(cpc)
        cpc_var = find_variable(cpc, (
                {"variable_name": "number_concentration"},
                {"standard_name": "number_concentration_of_ambient_aerosol_particles_in_air"},
        ))

    total_scattering_var = None
    back_scattering_var = None
    if scattering:
        scattering = SelectedData.ensure_data(scattering)
        total_scattering_var = find_variable(scattering, (
                {"variable_name": "scattering_coefficient"},
                {"standard_name": "volume_scattering_coefficient_in_air_due_to_dried_aerosol_particles"},
        ))
        back_scattering_var = find_variable(scattering, (
            {"variable_name": "backscattering_coefficient"},
            {"standard_name": "volume_backwards_scattering_coefficient_in_air_due_to_dried_aerosol_particles"},
        ))

    absorption_var = None
    if absorption:
        absorption = SelectedData.ensure_data(absorption)
        absorption_var = find_variable(absorption, (
                {"variable_name": "light_absorption"},
                {"standard_name": "volume_absorption_coefficient_in_air_due_to_dried_aerosol_particles"},
        ))

    extinction_var = None
    if extinction:
        extinction = SelectedData.ensure_data(extinction)
        extinction_var = find_variable(extinction, (
                {"variable_name": "light_extinction"},
                {"standard_name": "volume_extinction_coefficient_in_air_due_to_ambient_aerosol_particles"},
        ))

    if total_scattering_var:
        total_scattering_out = extensives.get_output(
            total_scattering_var, "scattering_coefficient",
            wavelength=True
        ).take()
        netcdf_var.variable_total_scattering(total_scattering_out.variable, is_stp=is_stp)
        total_scattering_out.variable.variable_id = "Bs"
        total_scattering_out.variable.coverage_content_type = "physicalMeasurement"
        setup_variable(total_scattering_out)
        copy_metadata(total_scattering_out, "scattering", total_scattering_var)
        apply_wavelengths(total_scattering_out, total_scattering_var)
        total_scattering_out.commit()
    else:
        total_scattering_out = None

    if back_scattering_var:
        back_scattering_out = extensives.get_output(
            back_scattering_var, "backscattering_coefficient",
            wavelength=True
        ).take()
        netcdf_var.variable_back_scattering(back_scattering_out.variable, is_stp=is_stp)
        back_scattering_out.variable.variable_id = "Bbs"
        back_scattering_out.variable.coverage_content_type = "physicalMeasurement"
        setup_variable(back_scattering_out)
        apply_wavelengths(back_scattering_out, back_scattering_var)
        back_scattering_out.commit()
    else:
        back_scattering_out = EmptySelectedVariable(
            extensives.times, (extensives.times.shape[0], len(wavelengths))
        )
        
    if absorption_var:
        absorption_out = extensives.get_output(
            absorption_var, "light_absorption",
            wavelength=True
        ).take()
        netcdf_var.variable_absorption(absorption_out.variable, is_stp=is_stp)
        absorption_out.variable.variable_id = "Ba"
        absorption_out.variable.coverage_content_type = "physicalMeasurement"
        setup_variable(absorption_out)
        copy_metadata(absorption_out, "absorption", absorption_var)
        apply_wavelengths(absorption_out, absorption_var)
        absorption_out.commit()
    else:
        absorption_out = None
        
    if extinction_var:
        extinction_out = extensives.get_output(
            extinction_var, "light_extinction",
            wavelength=True
        ).take()
        netcdf_var.variable_extinction(extinction_out.variable, is_stp=is_stp)
        extinction_out.variable.variable_id = "Ba"
        extinction_out.variable.coverage_content_type = "physicalMeasurement"
        setup_variable(extinction_out)
        copy_metadata(extinction_out, "extinction", extinction_var)
        apply_wavelengths(extinction_out, extinction_var)
        extinction_out.commit()
    else:
        extinction_out = None

    if cpc_var:
        cpc_out = extensives.get_output(cpc_var, "number_concentration").take()
        netcdf_var.variable_number_concentration(cpc_out.variable, is_stp=is_stp)
        cpc_out.variable.variable_id = "N"
        cpc_out.variable.coverage_content_type = "physicalMeasurement"
        setup_variable(cpc_out)
        copy_metadata(cpc_out, "cpc", cpc_var)
        cpc_out[:] = cpc_var[:]
        cpc_out.commit()
    else:
        cpc_out = None

    if not total_scattering_out and absorption_out and extinction_out:
        absorption_in, extinction_in = find_variable(extensives, {
            "variable_name": "light_absorption",
        }, {
            "variable_name": "light_extinction",
        })
        total_scattering_out = extensives.get_output(
            absorption_in, "scattering_coefficient",
            wavelength=True
        ).take()
        netcdf_var.variable_total_scattering(total_scattering_out.variable, is_stp=is_stp)
        total_scattering_out.variable.variable_id = "Bs"
        setup_variable(total_scattering_out)
        total_scattering_out[:] = calculate_scattering(absorption_in.values, extinction_in.values)
        total_scattering_out.commit()
    elif not absorption_out and total_scattering_out and extinction_out:
        scattering_in, extinction_in = find_variable(extensives, {
            "variable_name": "scattering_coefficient",
        }, {
            "variable_name": "light_extinction",
        })
        absorption_out = extensives.get_output(
            scattering_in, "light_absorption",
            wavelength=True
        ).take()
        netcdf_var.variable_absorption(absorption_out.variable, is_stp=is_stp)
        absorption_out.variable.variable_id = "Ba"
        setup_variable(absorption_out)
        absorption_out[:] = calculate_absorption(scattering_in.values, extinction_in.values)
        absorption_out.commit()
    elif not extinction_out and total_scattering_out and absorption_out:
        scattering_in, absorption_in = find_variable(extensives, {
            "variable_name": "scattering_coefficient",
        }, {
            "variable_name": "light_absorption",
        })
        extinction_out = extensives.get_output(
            scattering_in, "light_extinction",
            wavelength=True
        ).take()
        netcdf_var.variable_extinction(extinction_out.variable, is_stp=is_stp)
        extinction_out.variable.variable_id = "Ba"
        extinction_out.variable.coverage_content_type = "physicalMeasurement"
        setup_variable(extinction_out)
        extinction_out[:] = calculate_extinction(scattering_in.values, absorption_in.values)
        extinction_out.commit()

    def empty_wavelengths_out() -> SelectedVariable:
        return EmptySelectedVariable(
            extensives.times,
            shape=(extensives.times, len(wavelengths)),
            wavelengths=wavelengths,
        )

    system_flags_out = None

    if not total_scattering_out:
        total_scattering_out = empty_wavelengths_out()
    elif system_flags_out is None:
        system_flags_out = extensives.get_output(
            total_scattering_out, "system_flags",
            dtype=np.uint64,
        ).take()

    if not back_scattering_out:
        back_scattering_out = empty_wavelengths_out()
    elif system_flags_out is None:
        system_flags_out = extensives.get_output(
            back_scattering_out, "system_flags",
            dtype=np.uint64,
        ).take()

    if not absorption_out:
        absorption_out = empty_wavelengths_out()
    elif system_flags_out is None:
        system_flags_out = extensives.get_output(
            absorption_out, "system_flags",
            dtype=np.uint64,
        ).take()

    if not extinction_out:
        extinction_out = empty_wavelengths_out()
    elif system_flags_out is None:
        system_flags_out = extensives.get_output(
            extinction_out, "system_flags",
            dtype=np.uint64,
        ).take()

    if not cpc_out:
        cpc_out = EmptySelectedVariable(extensives.times)
    elif system_flags_out is None:
        system_flags_out = extensives.get_output(
            cpc_out, "system_flags",
            dtype=np.uint64,
        ).take()

    if system_flags_out:
        flags_bits: typing.Dict[int, str] = dict()
        existing_flags: typing.Dict[str, int] = dict()

        def propagate_flag(flag: str) -> bool:
            if flag.startswith("data_contamination_"):
                return True
            return False

        def apply_flags(source: typing.Optional[SelectedData]) -> None:
            if not source:
                return
            try:
                source_flags = source.get_input(system_flags_out, {
                    "variable_name": "system_flags",
                })
            except FileNotFoundError:
                return

            for flag_bits, flag_name in parse_flags(source_flags.variable).items():
                if not propagate_flag(flag_name):
                    continue

                hit_times = np.bitwise_and(source_flags.values, flag_bits) != 0
                if not np.any(hit_times):
                    continue

                destination_bits = existing_flags.get(flag_name)
                if destination_bits is None:
                    for b in range(64):
                        check_bit = 1 << b
                        if flags_bits.get(check_bit) is not None:
                            continue
                        destination_bits = check_bit
                        break
                    else:
                        _LOGGER.warning(f"No free bit for flag {flag_name}")
                        continue

                flags_bits[destination_bits] = flag_name
                existing_flags[flag_name] = destination_bits

                np.bitwise_or(system_flags_out.values, destination_bits, out=system_flags_out.values, where=hit_times)

        # Remove any cut size info, since it's implied with system flags and they span both
        try:
            system_flags_out.variable.delncattr("ancillary_variables")
        except (AttributeError, RuntimeError):
            pass
        system_flags_out[:] = 0

        apply_flags(scattering)
        apply_flags(absorption)
        apply_flags(extinction)
        apply_flags(cpc)

        netcdf_var.variable_flags(system_flags_out.variable, flags_bits)
        system_flags_out.variable.variable_id = "F1"
        system_flags_out.commit()

    return cpc_out, total_scattering_out, back_scattering_out, absorption_out, extinction_out
