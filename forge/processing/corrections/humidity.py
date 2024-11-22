import typing
import logging
import numpy as np
from forge.dewpoint import rh as calculate_rh, dewpoint as calculate_dewpoint
from forge.data.structure.variable import variable_rh, variable_air_rh, variable_dewpoint, variable_air_dewpoint
from ..context import SelectedData, SelectedVariable
from ..context.variable import EmptySelectedVariable

_LOGGER = logging.getLogger(__name__)


def _fill_missing(
        temperature: np.ndarray,
        rh: np.ndarray,
        dewpoint: np.ndarray,
        over_water: bool = True,
) -> typing.Tuple[np.ndarray, np.ndarray]:
    assert temperature.shape == rh.shape
    assert temperature.shape == dewpoint.shape

    rh = np.array(rh, copy=True)
    dewpoint = np.array(dewpoint, copy=True)

    temperature_valid = np.isfinite(temperature)
    rh_valid = np.isfinite(rh)
    dewpoint_valid = np.isfinite(dewpoint)
    fill_rh = np.logical_and(temperature_valid, np.logical_and(np.invert(rh_valid), dewpoint_valid))
    fill_dewpoint = np.logical_and(temperature_valid, np.logical_and(np.invert(dewpoint_valid), rh_valid))

    rh[fill_rh] = calculate_rh(temperature[fill_rh], dewpoint[fill_rh], over_water=over_water)
    dewpoint[fill_dewpoint] = calculate_dewpoint(temperature[fill_dewpoint], rh[fill_dewpoint], over_water=over_water)

    return rh, dewpoint


def populate_humidity(
        data,
        over_water: bool = True,
        populate: typing.Iterable[typing.Tuple[typing.Any, typing.Any, typing.Any]] = None,
) -> None:
    if populate is None:
        populate = [
            (
                [{"variable_id": "T1?"}, {"standard_name": "air_temperature"}],
                [{"variable_id": "U1?"}, {"standard_name": "relative_humidity"}],
                [{"variable_id": "TD1?"}],
            ),
            (
                [{"variable_id": "T2"}],
                [{"variable_id": "U2"}],
                [{"variable_id": "TD2"}],
            ),
            (
                [{"variable_id": "T3"}],
                [{"variable_id": "U3"}],
                [{"variable_id": "TD3"}],
            ),
        ]

    data = SelectedData.ensure_data(data)
    any_populated = False

    def assign_variable_id(source, dest, code: str):
        source_variable_id = getattr(source, "variable_id", None)
        if not source_variable_id:
            return
        source_variable_id = str(source_variable_id)
        suffix = source_variable_id[1:]
        dest.variable_id = code + suffix

    for selections in populate:
        for temperature, rh, dewpoint in data.select_variable(*selections, commit_variable=False,
                                                              always_tuple=True):
            if isinstance(rh, EmptySelectedVariable) and isinstance(dewpoint, EmptySelectedVariable):
                continue
            any_populated = True

            rh.values, dewpoint.values = _fill_missing(temperature.values, rh.values, dewpoint.values,
                                                       over_water=over_water)

            if isinstance(rh, EmptySelectedVariable):
                try:
                    with data.get_output(temperature, f"calculated_humidity_for_{temperature.variable.name}" ,
                                         error_when_duplicate=True) as rh_output:
                        if getattr(temperature.variable, "standard_name", None) == "air_temperature":
                            variable_air_rh(rh_output.variable)
                        else:
                            variable_rh(rh_output.variable)
                        rh_output.variable.cell_methods = "time: mean"
                        rh_output.variable.long_name = f"calculated relative humidity using {temperature.variable.name} and {dewpoint.variable.name}"
                        assign_variable_id(temperature.variable, rh_output.variable, "U")
                        rh_output[:] = rh[:]
                except FileExistsError:
                    pass
            else:
                rh.commit()

            if isinstance(dewpoint, EmptySelectedVariable):
                try:
                    with data.get_output(temperature, f"calculated_dewpoint_for_{temperature.variable.name}" ,
                                         error_when_duplicate=True) as dewpoint_output:
                        if getattr(temperature.variable, "standard_name", None) == "air_temperature":
                            variable_air_dewpoint(dewpoint_output.variable)
                        else:
                            variable_dewpoint(dewpoint_output.variable)
                        dewpoint_output.variable.cell_methods = "time: mean"
                        dewpoint_output.variable.long_name = f"calculated dewpoint using {temperature.variable.name} and {rh.variable.name}"
                        assign_variable_id(temperature.variable, dewpoint_output.variable, "TD")
                        dewpoint_output[:] = dewpoint[:]
                except FileExistsError:
                    pass
            else:
                dewpoint.commit()

    if any_populated:
        data.append_history("forge.correction.populatehumidity")
