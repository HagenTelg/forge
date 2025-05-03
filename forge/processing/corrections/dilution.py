import typing
import logging
import numpy as np
from math import nan
from .selections import VOLUME_DEPENDENT_MEASUREMENTS
from ..context import SelectedData, SelectedVariable

_LOGGER = logging.getLogger(__name__)


def dilution_factor(
        sample,
        dilution,
) -> np.ndarray:
    def make_sum(values):
        if not isinstance(values, tuple) and not isinstance(values, list):
            values = [np.asarray(values)]
        else:
            values = [np.asarray(v) for v in values]

        for v in values:
            if len(v.shape) == 0:
                continue
            result = np.full(v.shape, 0.0)
            break
        else:
            result = np.array(0.0)

        for v in values:
            result += v

        return result

    total_sample = make_sum(sample)
    total_dilution = make_sum(dilution)

    if len(total_sample.shape) > 0:
        result = np.full(total_sample.shape, nan)
        if len(total_dilution.shape) == 0:
            total_dilution = np.full(total_sample.shape, total_dilution)
    else:
        result = np.full(total_dilution.shape, nan)
        total_sample = np.full(total_dilution.shape, total_sample)

    dilution_valid = np.logical_and(
        total_sample > 0.0,
        total_sample > total_dilution,
    )
    result[dilution_valid] = total_sample[dilution_valid] / (total_sample[dilution_valid] - total_dilution[dilution_valid])
    return result


def correct_diluted(
        values: np.ndarray,
        sample_flow,
        dilution_flow,
) -> np.ndarray:
    factor = dilution_factor(sample_flow, dilution_flow)
    return (values.T * factor.T).T


def _assemble_flow(
        target: SelectedVariable,
        flow: typing.Iterable[typing.Union[float, int, typing.Dict[str, typing.Any]]]
) -> typing.Iterable:
    result = list()
    for raw_flow in flow:
        if isinstance(raw_flow, int) or isinstance(raw_flow, float):
            flow_value = float(raw_flow)
        else:
            try:
                source_data = SelectedData.ensure_data(raw_flow["data"])
                flow_value = source_data.get_input(target, raw_flow["flow"])

                fallback = raw_flow.get("fallback")
                if fallback is not None:
                    missing_values = np.invert(np.isnan(flow_value))
                    if np.any(missing_values):
                        flow_value = np.array(flow_value.values, copy=True)
                        flow_value[missing_values] = fallback
            except FileNotFoundError:
                _LOGGER.debug("Failed to find flow for %s", raw_flow)
                flow_value = raw_flow.get("fallback", nan)
        result.append(flow_value)
    return result


def dilution(
        diluted_data: typing.Iterable,
        sample_flow: typing.Iterable[typing.Union[float, int, typing.Dict[str, typing.Any]]],
        dilution_flow: typing.Iterable[typing.Union[float, int, typing.Dict[str, typing.Any]]],
) -> None:
    for data in diluted_data:
        data = SelectedData.ensure_data(data)
        data.append_history("forge.correction.dilution")

        for diluted_var in data.select_variable(VOLUME_DEPENDENT_MEASUREMENTS):
            sample = _assemble_flow(diluted_var, sample_flow)
            dilution = _assemble_flow(diluted_var, dilution_flow)
            factor = dilution_factor(sample, dilution)
            diluted_var.values = (diluted_var.values.T * factor.T).T

            ancillary_variables = set(getattr(diluted_var.variable, "ancillary_variables", "").split())
            ancillary_variables.add("dilution_factor")
            diluted_var.variable.ancillary_variables = " ".join(ancillary_variables)

            try:
                with data.get_output(diluted_var, "dilution_factor", error_when_duplicate=True) as factor_output:
                    factor_output.variable.coverage_content_type = "auxillaryInformation"
                    factor_output.variable.cell_methods = "time: mean"
                    factor_output.variable.long_name = "dilution factor (corrected = measured * factor)"
                    factor_output.variable.units = "1"
                    factor_output.variable.C_format = "%6.3f"
                    factor_output.variable.variable_id = "ZDILUTION"
                    factor_output[...] = factor[...]
            except FileExistsError:
                continue
