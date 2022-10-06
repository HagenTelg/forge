import typing
from math import ceil
from numpy import uint64
from netCDF4 import Variable


def variable_flags(var: Variable, flags: typing.Dict[int, str] = None) -> None:
    var.standard_name = "status_flag"
    var.long_name = "bitwise OR of status condition flags"
    if not flags:
        return

    bits: typing.List[int] = list(flags.keys())
    bits.sort()
    all_bits = 0
    for b in bits:
        all_bits |= b
    var.valid_range = [uint64(bits[0]), uint64(all_bits)]
    var.flag_masks = [uint64(v) for v in bits]
    var.flag_meanings = " ".join([flags[b].replace(" ", "_") for b in bits])

    digits = int(ceil(all_bits.bit_length() / (4 * 4))) * 4
    var.C_format = f"%0{digits}llX"


def variable_cutsize(var: Variable) -> None:
    var.long_name = "maximum aerodynamic particle diameter"
    var.standard_name = "aerodynamic_particle_diameter"
    var.units = "um"
    var.C_format = "%.2g"


def variable_wavelength(var: Variable) -> None:
    var.long_name = "central measurement wavelength"
    var.standard_name = "radiation_wavelength"
    var.units = "nm"
    var.C_format = "%.0f"


def variable_number_concentration(var: Variable, is_stp: bool = False) -> None:
    if is_stp:
        var.long_name = "particle number concentration at STP"
        var.standard_name = "number_concentration_of_aerosol_particles_at_stp_in_air"
    else:
        var.long_name = "particle number concentration"
        var.standard_name = "number_concentration_of_ambient_aerosol_particles_in_air"
    var.units = "cm-3"
    var.C_format = "%7.1f"


def variable_total_scattering(var: Variable, is_stp: bool = False, is_dried: bool = True) -> None:
    if is_stp:
        var.long_name = "total light scattering coefficient"
    else:
        var.long_name = "total light scattering coefficient at STP"
    if is_dried:
        var.standard_name = "volume_scattering_coefficient_in_air_due_to_dried_aerosol_particles"
    elif not is_stp:
        var.standard_name = "volume_scattering_coefficient_in_air_due_to_ambient_aerosol_particles"
    var.units = "Mm-1"
    var.C_format = "%7.2f"


def variable_back_scattering(var: Variable, is_stp: bool = False, is_dried: bool = True) -> None:
    if is_stp:
        var.long_name = "backwards hemispheric light scattering coefficient"
    else:
        var.long_name = "backwards hemispheric light scattering coefficient at STP"
    if is_dried:
        var.standard_name = "volume_backwards_scattering_coefficient_in_air_due_to_dried_aerosol_particles"
    var.units = "Mm-1"
    var.C_format = "%7.2f"


def variable_absorption(var: Variable, is_stp: bool = False, is_dried: bool = True) -> None:
    if is_stp:
        var.long_name = "light absorption coefficient at STP"
    else:
        var.long_name = "light absorption coefficient"
    if is_dried:
        var.standard_name = "volume_absorption_coefficient_in_air_due_to_dried_aerosol_particles"
    var.units = "Mm-1"
    var.C_format = "%7.2f"


def variable_extinction(var: Variable, is_stp: bool = False, is_dried: bool = True) -> None:
    if is_stp:
        var.long_name = "light extinction coefficient at STP"
    else:
        var.long_name = "light extinction coefficient"
    if not is_stp and not is_dried:
        var.standard_name = "volume_extinction_coefficient_in_air_due_to_ambient_aerosol_particles"
    var.units = "Mm-1"
    var.C_format = "%7.2f"


def variable_ebc(var: Variable) -> None:
    var.long_name = "equivalent black carbon concentration derived from an optical measurement"
    var.units = "ug m-3"
    var.C_format = "%8.3f"


def variable_ozone(var: Variable) -> None:
    var.long_name = "mass fraction of ozone"
    var.standard_name = "mole_fraction_of_ozone_in_air"
    var.units = "1e-9"  # canonical ppb
    var.C_format = "%9.2f"


def variable_wind_speed(var: Variable) -> None:
    var.long_name = "wind speed"
    var.standard_name = "wind_speed"
    var.units = "m s-1"
    var.C_format = "%4.1f"


def variable_wind_direction(var: Variable) -> None:
    var.long_name = "wind direction from true north"
    var.standard_name = "wind_from_direction"
    var.units = "degree"
    var.C_format = "%5.1f"


def variable_temperature(var: Variable) -> None:
    var.units = "degC"
    var.C_format = "%5.1f"


def variable_air_temperature(var: Variable) -> None:
    var.long_name = "air temperature of the measurement"
    var.standard_name = "air_temperature"
    variable_temperature(var)


def variable_dewpoint(var: Variable) -> None:
    var.units = "degC"
    var.C_format = "%5.1f"


def variable_air_dewpoint(var: Variable) -> None:
    variable_dewpoint(var)


def variable_rh(var: Variable) -> None:
    var.units = "%"
    var.C_format = "%3.1f"


def variable_air_rh(var: Variable) -> None:
    var.long_name = "relative humidity of the measurement"
    var.standard_name = "relative_humidity"
    variable_rh(var)


def variable_pressure(var: Variable) -> None:
    var.units = "hPa"
    var.C_format = "%6.1f"


def variable_air_pressure(var: Variable) -> None:
    var.long_name = "absolute air pressure of the measurement"
    var.standard_name = "air_pressure"
    variable_pressure(var)


def variable_delta_pressure(var: Variable) -> None:
    var.units = "hPa"
    var.C_format = "%5.1f"


def variable_flow(var: Variable) -> None:
    var.units = "lpm"
    var.C_format = "%6.2f"


def variable_sample_flow(var: Variable) -> None:
    var.long_name = "sample flow rate"
    variable_flow(var)


def variable_size_distribution_Dp(var: Variable) -> None:
    var.long_name = "central diameter of particles in the bin (Dp)"
    var.units = "um"
    var.C_format = "%7.4f"


def variable_size_distribution_Dp_electrical_mobility(var: Variable) -> None:
    variable_size_distribution_Dp(var)
    var.standard_name = "electrical_mobility_diameter_of_ambient_aerosol_particles"


def variable_size_distribution_dN(var: Variable) -> None:
    var.long_name = "binned number concentration (dN)"
    var.units = "cm-3"
    var.C_format = "%7.1f"


def variable_size_distribution_dNdlogDp(var: Variable) -> None:
    var.long_name = "normalized number concentration (dN/dlogDp)"
    var.units = "cm-3"
    var.C_format = "%7.1f"
