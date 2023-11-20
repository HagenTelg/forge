import typing
import numpy as np
from math import nan
from ..context import SelectedData, SelectedVariable
from ..derived.angstrom import angstrom_exponent_adjacent


class Coefficients:
    def __init__(
            self,
            total_coarse_angstrom: typing.Dict[float, typing.Tuple[float, ...]],
            total_coarse_no_angstrom: typing.Dict[float, float],
            total_fine_angstrom: typing.Dict[float, typing.Tuple[float, ...]],
            total_fine_no_angstrom: typing.Dict[float, float],
            back_coarse_no_angstrom: typing.Dict[float, float],
            back_fine_no_angstrom: typing.Dict[float, float],
    ):
        self.total_coarse_angstrom = {w: np.polynomial.Polynomial(c) for w, c in total_coarse_angstrom.items()}
        self.total_coarse_no_angstrom = total_coarse_no_angstrom
        self.total_fine_angstrom = {w: np.polynomial.Polynomial(c) for w, c in total_fine_angstrom.items()}
        self.total_fine_no_angstrom = total_fine_no_angstrom
        self.back_coarse_no_angstrom = back_coarse_no_angstrom
        self.back_fine_no_angstrom = back_fine_no_angstrom

    @staticmethod
    def _find_coefficient(c: typing.Dict[float, typing.Any], wl: float):
        best = None
        best_wl = None
        for cwl, cv in c.items():
            if best is None:
                best = cv
                best_wl = cwl
                continue
            if abs(cwl - wl) < abs(best_wl - wl):
                best = cv
                best_wl = cwl
        return best

    def _apply_total_inner(
            self,
            scattering: np.ndarray,
            angstrom: np.ndarray,
            wavelengths: typing.Union[float, typing.List[float], typing.Tuple[float, ...]],
            angstrom_fit: typing.Dict[float, np.polynomial.Polynomial],
            no_angstrom_coefficients: typing.Dict[float, float],
    ) -> np.ndarray:
        result = np.empty_like(scattering)

        if isinstance(wavelengths, (float, int)):
            wavelengths = float(wavelengths)
            fit = self._find_coefficient(angstrom_fit, wavelengths)(angstrom)
            fit_invalid = np.isnan(fit)

            np.multiply(scattering, fit,
                        out=result, where=np.invert(fit_invalid))
            np.multiply(scattering, self._find_coefficient(no_angstrom_coefficients, wavelengths),
                        out=result, where=fit_invalid)
            return result

        if isinstance(wavelengths, np.ndarray):
            wavelengths = wavelengths.tolist()

        for widx in range(len(wavelengths)):
            fit = self._find_coefficient(angstrom_fit, wavelengths[widx])(angstrom[..., widx])
            fit_invalid = np.isnan(fit)

            selected_scattering = scattering[..., widx]
            wavelength_result = np.empty_like(selected_scattering)
            np.multiply(selected_scattering, fit,
                        out=wavelength_result, where=np.invert(fit_invalid))
            np.multiply(selected_scattering, self._find_coefficient(no_angstrom_coefficients, wavelengths[widx]),
                        out=wavelength_result, where=fit_invalid)
            result[..., widx] = wavelength_result

        return result

    def apply_total_coarse(
            self,
            scattering: np.ndarray,
            angstrom: np.ndarray,
            wavelengths: typing.Union[float, typing.List[float], typing.Tuple[float, ...]],
    ) -> np.ndarray:
        return self._apply_total_inner(scattering, angstrom, wavelengths, self.total_coarse_angstrom,
                                       self.total_coarse_no_angstrom)

    def apply_total_fine(
            self,
            scattering: np.ndarray,
            angstrom: np.ndarray,
            wavelengths: typing.Union[float, typing.List[float], typing.Tuple[float, ...]],
    ) -> np.ndarray:
        return self._apply_total_inner(scattering, angstrom, wavelengths, self.total_fine_angstrom,
                                       self.total_fine_no_angstrom)

    def _apply_back_inner(
            self,
            scattering: np.ndarray,
            wavelengths: typing.Union[float, typing.List[float], typing.Tuple[float, ...]],
            no_angstrom_coefficients: typing.Dict[float, float],
    ) -> np.ndarray:
        if isinstance(wavelengths, (float, int)):
            return scattering * self._find_coefficient(no_angstrom_coefficients, float(wavelengths))
        if isinstance(wavelengths, np.ndarray):
            wavelengths = wavelengths.tolist()
        result = np.empty_like(scattering)
        for widx in range(len(wavelengths)):
            result[..., widx] = scattering[..., widx] * self._find_coefficient(no_angstrom_coefficients, wavelengths[widx])
        return result

    def apply_back_coarse(
            self,
            scattering: np.ndarray,
            wavelengths: typing.Union[typing.List[float], typing.Tuple[float, ...]],
    ) -> np.ndarray:
        return self._apply_back_inner(scattering, wavelengths, self.back_coarse_no_angstrom)

    def apply_back_fine(
            self,
            scattering: np.ndarray,
            wavelengths: typing.Union[typing.List[float], typing.Tuple[float, ...]],
    ) -> np.ndarray:
        return self._apply_back_inner(scattering, wavelengths, self.back_fine_no_angstrom)


ANDERSON_OGREN_1998_COEFFICIENTS = Coefficients(
    total_coarse_angstrom={
        450.0: (1.365, -0.156),
        550.0: (1.337, -0.138),
        700.0: (1.297, -0.113),
    },
    total_coarse_no_angstrom={
        450.0: 1.29,
        550.0: 1.29,
        700.0: 1.26,
    },
    total_fine_angstrom={
        450.0: (1.165, -0.046),
        550.0: (1.152, -0.044),
        700.0: (1.120, -0.035),
    },
    total_fine_no_angstrom={
        450.0: 1.094,
        550.0: 1.073,
        700.0: 1.049,
    },
    back_coarse_no_angstrom={
        450.0: 0.981,
        550.0: 0.982,
        700.0: 0.985,
    },
    back_fine_no_angstrom={
        450.0: 0.951,
        550.0: 0.947,
        700.0: 0.952,
    },
)
MUELLER_2011_TSI_COEFFICIENTS = Coefficients(
    total_coarse_angstrom={
        450.0: (1.345, -0.146),
        550.0: (1.319, -0.129),
        700.0: (1.279, -0.105),
    },
    total_coarse_no_angstrom={
        450.0: 1.30,
        550.0: 1.29,
        700.0: 1.26,
    },
    total_fine_angstrom={
        450.0: (1.148, -0.041),
        550.0: (1.137, -0.040),
        700.0: (1.109, -0.033),
    },
    total_fine_no_angstrom={
        450.0: 1.086,
        550.0: 1.066,
        700.0: 1.045,
    },
    back_coarse_no_angstrom={
        450.0: 0.983,
        550.0: 0.984,
        700.0: 0.988,
    },
    back_fine_no_angstrom={
        450.0: 0.950,
        550.0: 0.944,
        700.0: 0.954,
    }
)
MUELLER_2011_ECOTECH_COEFFICIENTS = Coefficients(
    total_coarse_angstrom={
        450.0: (1.455, -0.189),
        525.0: (1.434, -0.176),
        635.0: (1.403, -0.156),
    },
    total_coarse_no_angstrom={
        450.0: 1.37,
        525.0: 1.38,
        635.0: 1.36,
    },
    total_fine_angstrom={
        450.0: (1.213, -0.060),
        525.0: (1.207, -0.061),
        635.0: (1.176, -0.053),
    },
    total_fine_no_angstrom={
        450.0: 1.125,
        525.0: 1.103,
        635.0: 1.078,
    },
    back_coarse_no_angstrom={
        450.0: 0.963,
        525.0: 0.971,
        635.0: 0.968,
    },
    back_fine_no_angstrom={
        450.0: 0.932,
        525.0: 0.935,
        635.0: 0.935,
    }
)


def hourly_angstrom_exponent(scattering: SelectedVariable) -> np.ndarray:
    from forge.data.merge.timealign import incoming_before
    from ..average.calculate import fixed_interval_weighted_average

    smoothed_scattering = np.full_like(scattering.values, nan)
    for _, value_select, time_select in scattering.select_cut_size():
        selected_times = scattering.times[time_select]
        selected_values = scattering.values[value_select]
        smoothed_values, smoothed_start = fixed_interval_weighted_average(
            selected_times,
            selected_values,
            scattering.average_weights[time_select],
            60 * 60 * 1000,
        )

        smoothed_targets = incoming_before(selected_times, smoothed_start)
        smoothed_scattering[value_select] = smoothed_values[smoothed_targets]
    return angstrom_exponent_adjacent(scattering, smoothed_scattering)


def digital_filter_angstrom_exponent(scattering: SelectedVariable) -> np.ndarray:
    from ..average.digitalfilter import single_pole_low_pass

    smoothed_scattering = np.full_like(scattering.values, nan)
    for _, value_select, time_select in scattering.select_cut_size():
        smoothed_scattering[value_select] = single_pole_low_pass(
            scattering.times[value_select],
            scattering.values[time_select],
            3 * 60 * 1000,
            35 * 60 * 1000,
        )
    return angstrom_exponent_adjacent(scattering, smoothed_scattering)


def unsmoothed_angstrom_exponent(scattering: SelectedVariable) -> np.ndarray:
    return angstrom_exponent_adjacent(scattering)


def _correction_inner(
        data,
        coefficients: Coefficients,
        get_angstrom_exponent: typing.Callable[[SelectedVariable], np.ndarray] = hourly_angstrom_exponent,
) -> None:
    for total in data.select_variable((
            {"variable_name": "scattering_coefficient"},
            {"standard_name": "volume_scattering_coefficient_in_air_due_to_dried_aerosol_particles"},
    )):
        angstrom = get_angstrom_exponent(total)
        # Note the use of NaN (whole air) always being false, so invert(x <= 2.5) is not the same as x > 2.5
        fine_data = total.get_cut_size_index(lambda x: x <= 2.5)
        coarse_data = total.get_cut_size_index(lambda x: np.invert(x <= 2.5))
        if total.has_changing_wavelengths:
            for wavelengths, value_select, _ in total.select_wavelengths(tail_index_only=True):
                for widx in range(len(wavelengths)):
                    fine_index = (*fine_data, *value_select[widx])
                    total[fine_index] = coefficients.apply_total_fine(
                        total[fine_index], angstrom[fine_index], wavelengths[widx]
                    )

                    coarse_index = (*coarse_data, *value_select[widx])
                    total[coarse_index] = coefficients.apply_total_coarse(
                        total[coarse_index], angstrom[coarse_index], wavelengths[widx]
                    )
        else:
            total[fine_data] = coefficients.apply_total_fine(
                total[fine_data], angstrom[fine_data], total.wavelengths
            )
            total[coarse_data] = coefficients.apply_total_coarse(
                total[coarse_data], angstrom[coarse_data], total.wavelengths
            )

    for back in data.select_variable((
            {"variable_name": "backscattering_coefficient"},
            {"standard_name": "volume_backwards_scattering_coefficient_in_air_due_to_dried_aerosol_particles"},
    )):
        # Note the use of NaN (whole air) always being false, so invert(x <= 2.5) is not the same as x > 2.5
        fine_data = back.get_cut_size_index(lambda x: x <= 2.5)
        coarse_data = back.get_cut_size_index(lambda x: np.invert(x <= 2.5))
        if back.has_changing_wavelengths:
            for wavelengths, value_select, _ in back.select_wavelengths(tail_index_only=True):
                for widx in range(len(wavelengths)):
                    fine_index = (*fine_data, *value_select[widx])
                    back[fine_index] = coefficients.apply_back_fine(back[fine_index], wavelengths[widx])

                    coarse_index = (*coarse_data, *value_select[widx])
                    back[coarse_index] = coefficients.apply_back_coarse(back[coarse_index], wavelengths[widx])
        else:
            back[fine_data] = coefficients.apply_back_fine(back[fine_data], back.wavelengths)
            back[coarse_data] = coefficients.apply_back_coarse(back[coarse_data], back.wavelengths)


def anderson_ogren_1998(
        data,
        angstrom_exponent: typing.Callable[[SelectedVariable], np.ndarray] = hourly_angstrom_exponent
) -> None:
    data = SelectedData.ensure_data(data)
    data.append_history("forge.correction.andersonogren1998")
    _correction_inner(data, ANDERSON_OGREN_1998_COEFFICIENTS, angstrom_exponent)


def mueller_2011(
        data,
        angstrom_exponent: typing.Callable[[SelectedVariable], np.ndarray] = hourly_angstrom_exponent
) -> None:
    data = SelectedData.ensure_data(data)
    data.append_history("forge.correction.mueller2011")
    _correction_inner(data, MUELLER_2011_ECOTECH_COEFFICIENTS, angstrom_exponent)


def mueller_2011_tsi(
        data,
        angstrom_exponent: typing.Callable[[SelectedVariable], np.ndarray] = hourly_angstrom_exponent
) -> None:
    data = SelectedData.ensure_data(data)
    data.append_history("forge.correction.mueller2011")
    _correction_inner(data, MUELLER_2011_TSI_COEFFICIENTS, angstrom_exponent)
