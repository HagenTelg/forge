import typing
from netCDF4 import Variable
from forge.processing.average.contamination import StationContamination


class NoContamination(StationContamination):
    def is_contamination_flag(self, flag_bit: int, flag_name: str) -> bool:
        return False

    def variable_affected(self, variable: Variable) -> bool:
        return False


class MatchContamination(StationContamination):
    VARIABLE_NAMES: typing.Set[str] = set()
    STANDARD_NAMES: typing.Set[str] = set()
    UNITS: typing.Set[str] = set()

    def variable_affected(self, variable: Variable) -> bool:
        if not super().variable_affected(variable):
            return False
        if variable.name in self.VARIABLE_NAMES:
            return True
        try:
            if variable.units in self.UNITS:
                return True
        except AttributeError:
            pass
        try:
            if variable.standard_name in self.STANDARD_NAMES:
                return True
        except AttributeError:
            pass
        return False


class AerosolContamination(MatchContamination):
    VARIABLE_NAMES = {
        "number_concentration",
        "scattering_coefficient",
        "backscattering_coefficient",
        "light_absorption",
        "light_extinction",
        "equivalent_black_carbon",
        "number_distribution",
        "normalized_number_distribution",
        "polar_scattering_coefficient",
        "mass_concentration",
        "spot_one_light_absorption",
        "spot_two_light_absorption",
    }
    STANDARD_NAMES = {
        "number_concentration_of_ambient_aerosol_particles_in_air",
        "volume_scattering_coefficient_in_air_due_to_dried_aerosol_particles",
        "volume_backwards_scattering_coefficient_in_air_due_to_dried_aerosol_particles",
        "volume_absorption_coefficient_in_air_due_to_dried_aerosol_particles",
        "volume_extinction_coefficient_in_air_due_to_ambient_aerosol_particles",
    }


class RadiationContamination(MatchContamination):
    VARIABLE_NAMES = {
        "optical_depth",
    }
    UNITS = {
        "W/m2",
    }


def apply(station: str,
          tags: typing.Optional[typing.Set[str]] = None,
          start: typing.Optional[int] = None, end: typing.Optional[int] = None) -> StationContamination:
    if tags and 'met' in tags:
        return NoContamination()
    if tags and 'ozone' in tags:
        return NoContamination()
    if tags and 'radiation' in tags:
        return RadiationContamination()
    return AerosolContamination()
