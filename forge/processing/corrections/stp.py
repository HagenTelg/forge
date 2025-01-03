import typing
import numpy as np
from math import nan
from forge.units import ZERO_C_IN_K, ONE_ATM_IN_HPA
from forge.data.structure.stp import standard_temperature, standard_pressure
from .selections import VOLUME_DEPENDENT_MEASUREMENTS
from ..context import SelectedData, VariableSelection, SelectedVariable


def _density(
    temperature: typing.Union[np.ndarray, float, int],
    pressure: typing.Union[np.ndarray, float, int],
) -> np.ndarray:
    temperature = np.array(temperature, copy=True)
    pressure = np.array(pressure, copy=True)

    selected = temperature[...] < 150.0
    temperature[selected] = temperature[selected] + ZERO_C_IN_K
    temperature[temperature < 100.0] = nan
    temperature[temperature > 350.0] = nan

    pressure[pressure < 10.0] = nan
    pressure[pressure > 2000.0] = nan

    return np.asarray((pressure / ONE_ATM_IN_HPA) * (ZERO_C_IN_K / temperature))


def correct_optical(
        values: np.ndarray,
        temperature: typing.Union[np.ndarray, float, int],
        pressure: typing.Union[np.ndarray, float, int],
) -> np.ndarray:
    return (values.T / _density(temperature, pressure).T).T


def correct_volume(
        values: np.ndarray,
        temperature: typing.Union[np.ndarray, float, int],
        pressure: typing.Union[np.ndarray, float, int],
) -> np.ndarray:
    return (values.T * _density(temperature, pressure).T).T


def to_stp(
        data,
        temperature: typing.Optional[typing.Union[float, int, typing.Dict[str, typing.Any], str, VariableSelection]] = None,
        pressure: typing.Optional[typing.Union[float, int, typing.Dict[str, typing.Any], str, VariableSelection]] = None,
) -> None:
    data = SelectedData.ensure_data(data)
    data.append_history("forge.correction.stp")

    select_params = []

    if temperature is None:
        temperature = {"standard_name": "air_temperature"}
    try:
        temperature = float(temperature)
        get_temperature = lambda p: temperature
    except (ValueError, TypeError):
        idx_temperature = len(select_params)
        select_params.append(temperature)
        get_temperature = lambda p: p[idx_temperature]

    if pressure is None:
        pressure = {"standard_name": "air_pressure"}
    try:
        pressure = float(pressure)
        get_pressure = lambda p: pressure
    except (ValueError, TypeError):
        idx_pressure = len(select_params)
        select_params.append(pressure)
        get_pressure = lambda p: p[idx_pressure]

    def attach_ancillary(destination: SelectedVariable) -> None:
        standard_temperature(destination.parent)
        standard_pressure(destination.parent)
        ancillary_variables = set(getattr(destination.variable, "ancillary_variables", "").split())
        ancillary_variables.add("standard_pressure")
        ancillary_variables.add("standard_temperature")
        destination.variable.ancillary_variables = " ".join(ancillary_variables)

    for optical, *params in data.select_variable(VOLUME_DEPENDENT_MEASUREMENTS, *select_params, always_tuple=True):
        optical.values = correct_optical(optical.values, get_temperature(params), get_pressure(params))
        attach_ancillary(optical)

        if optical.standard_name == "number_concentration_of_ambient_aerosol_particles_in_air":
            optical.standard_name = "number_concentration_of_aerosol_particles_at_stp_in_air"
        elif optical.standard_name == "volume_scattering_coefficient_in_air_due_to_ambient_aerosol_particles":
            optical.standard_name = None
        elif optical.standard_name == "volume_extinction_coefficient_in_air_due_to_ambient_aerosol_particles":
            optical.standard_name = None

    for volume, *params in data.select_variable((
            {"units": "lpm"},
            {"units": "m3"},
    ), *select_params, always_tuple=True):
        volume.values = correct_volume(volume.values, get_temperature(params), get_pressure(params))
        attach_ancillary(volume)
