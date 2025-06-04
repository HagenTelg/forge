import typing
import hashlib
import sys
import numpy as np
from netCDF4 import Dataset, Variable
from forge.timeparse import parse_iso8601_time
from forge.data.dimensions import find_dimension_values


def _qualitative_hash(file: Dataset) -> bytes:
    from forge.product.integrity.qualitative import qualitative_digest as digest_values

    measurement_variable_names = frozenset({
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
        "sample_flow",
        "wind_speed",
        "wind_direction",
        "ozone_mixing_ratio",
    })
    measurement_standard_names = frozenset({
        "number_concentration_of_ambient_aerosol_particles_in_air",
        "number_concentration_of_aerosol_particles_at_stp_in_air",
        "volume_scattering_coefficient_in_air_due_to_dried_aerosol_particles",
        "volume_scattering_coefficient_in_air_due_to_ambient_aerosol_particles",
        "volume_backwards_scattering_coefficient_in_air_due_to_dried_aerosol_particles",
        "volume_absorption_coefficient_in_air_due_to_dried_aerosol_particles",
        "volume_extinction_coefficient_in_air_due_to_ambient_aerosol_particles",
        "wind_speed",
        "wind_from_direction",
        "air_temperature",
        "air_pressure",
        "relative_humidity",
        "mole_fraction_of_ozone_in_air",
        "mole_fraction_of_nitrogen_monoxide_in_air",
        "mole_fraction_of_nitrogen_dioxide_in_air",
        "mole_fraction_of_carbon_monoxide_in_dry_air",
        "mole_fraction_of_carbon_monoxide_in_air",
        "mole_fraction_of_carbon_dioxide_in_dry_air",
        "mole_fraction_of_carbon_dioxide_in_air",
        "surface_downwelling_shortwave_flux_in_air",
        "surface_upwelling_shortwave_flux_in_air",
        "surface_direct_along_beam_shortwave_flux_in_air",
        "surface_diffuse_downwelling_shortwave_flux_in_air",
        "surface_downwelling_longwave_flux_in_air",
        "surface_upwelling_longwave_flux_in_air",
    })

    def digest_times(data: np.ndarray, digest) -> None:
        data = np.round(data / (5 * 1000))
        digest.update(data.astype(np.int64, casting='unsafe').tobytes())

    def is_measurement_variable(v: Variable) -> bool:
        if len(v.dimensions) < 1 or v.dimensions[0] != 'time':
            return False
        if v.name in ("averaged_time", "averaged_count", "time", "cut_size"):
            return False
        if not np.issubdtype(v.dtype, np.floating):
            return False
        return getattr(v, "coverage_content_type", None) == "physicalMeasurement"

    def is_important_variable(v: Variable) -> bool:
        if not is_measurement_variable(v):
            return False
        if v.name in measurement_variable_names:
            return True
        return getattr(v, "standard_name", "") in measurement_standard_names

    def digest_all(g: Dataset, check: typing.Callable[[Variable], bool], digest) -> bool:
        digest_variables = list()
        for var in g.variables.values():
            if not check(var):
                continue
            digest_variables.append(var)

        any_digest = False
        if digest_variables:
            any_digest = True

            _, times = find_dimension_values(g, 'time')
            digest_times(times[...].data, digest)

            digest_variables.sort(key=lambda v: v.name)
            for var in digest_variables:
                assert var.dimensions[0] == 'time'
                if len(var.dimensions) == 1:
                    digest_values(var[...].data, digest)
                else:
                    for didx in np.ndindex(var.shape[1:]):
                        digest_values(var[(slice(None), ) + didx].data, digest)

        digest_groups = list()
        for sub in g.groups.values():
            if sub.name == "statistics":
                continue
            digest_groups.append(sub)
        digest_groups.sort(key=lambda v: v.name)
        for sub in digest_groups:
            if digest_all(sub, check, digest):
                any_digest = True

        return any_digest

    h = hashlib.sha256()
    if not digest_all(file, is_important_variable, h):
        if not digest_all(file, is_measurement_variable, h):
            return bytes()
    return h.digest()


def _data_hash(file: Dataset) -> bytes:
    def is_measurement_variable(v: Variable) -> bool:
        if len(v.dimensions) < 1 or v.dimensions[0] != 'time':
            return False
        if v.name == "cut_size":
            return True
        if v.name in ("averaged_time", "averaged_count", "time"):
            return False
        return getattr(v, "coverage_content_type", None) in ("physicalMeasurement", "auxiliaryInformation")

    def digest_values(data: np.ndarray, digest) -> None:
        digest.update(data.tobytes())

    def digest_all(g: Dataset, digest) -> None:
        digest_variables = list()
        for var in g.variables.values():
            if not is_measurement_variable(var):
                continue
            digest_variables.append(var)

        if digest_variables:
            _, times = find_dimension_values(g, 'time')
            digest_values(times[...].data, digest)

            digest_variables.sort(key=lambda v: v.name)
            for var in digest_variables:
                digest_values(var[...].data, digest)

        for sub in sorted(g.groups.values(), key=lambda v: v.name):
            digest_all(sub, digest)

    h = hashlib.sha256()
    digest_all(file, h)
    return h.digest()


def _file_hash(file: str) -> bytes:
    with open(file, 'rb') as f:
        if sys.version_info[:2] >= (3, 11):
            return hashlib.file_digest(f, "sha256").digest()

        h = hashlib.sha256()
        while True:
            data = f.read(65536)
            if not data:
                break
            h.update(data)
        return h.digest()


def calculate_integrity(file: str) -> typing.Tuple[float, bytes, bytes, bytes]:
    file_hash = _file_hash(file)
    data = Dataset(file, "r")
    try:
        data_hash = _data_hash(data)
        qualitative_hash = _qualitative_hash(data)

        file_creation_time = getattr(data, 'date_created', None)
        if file_creation_time is not None:
            file_creation_time: float = parse_iso8601_time(str(file_creation_time)).timestamp()
    finally:
        data.close()
    return file_creation_time, file_hash, data_hash, qualitative_hash


def compare_integrity(file: str, hashes: typing.Union[typing.List[bytes], typing.Tuple[bytes, bytes, bytes]]) -> int:
    if len(hashes) < 1:
        return 1
    file_hash = _file_hash(file)
    if file_hash == hashes[0]:
        return 0
    if len(hashes) < 2:
        return len(hashes)

    data = Dataset(file, "r")
    try:
        data_hash = _data_hash(data)
        if data_hash == hashes[1]:
            return 1
        if len(hashes) < 3:
            return len(hashes)
        qualitative_hash = _qualitative_hash(data)
        if qualitative_hash == hashes[2]:
            return 2
    finally:
        data.close()
    return 3
