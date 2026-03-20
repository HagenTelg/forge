#!/usr/bin/env python3
import typing
import numpy as np
from math import nan
from forge.processing.context import AvailableData
from forge.processing.station.default.editing import standard_scattering_corrections, standard_intensives, standard_meteorological, standard_stp_corrections
from forge.processing.corrections import *
from forge.processing.derived.average import hourly_median


def absorption_corrections(data: AvailableData) -> None:
    # PSAP data ingested uses instrument output with Weiss already applied
    for absorption, scattering in data.select_instrument((
            {"instrument": "psap1w"},
            {"instrument": "psap3w"},
    ), {"tags": "scattering -secondary"}):
        remove_low_transmittance(absorption)
        bond_1999(absorption, scattering)
    for absorption, scattering in data.select_instrument((
            {"instrument": "bmitap"},
            {"instrument": "clap"},
    ), {"tags": "scattering -secondary"}):
        remove_low_transmittance(absorption)
        weiss(absorption)
        bond_1999(absorption, scattering)


def aerosol_contamination(data: AvailableData) -> None:
    for aerosol, wind in data.select_instrument(
            {"tags": "aerosol -met"},
            {"instrument_id": "XM1"},
            always_tuple=True,
    ):
        wind_sector_contamination(
            aerosol, wind,
            contaminated_sector=((20, 120),),
            contaminated_minimum_speed=0.5,
            extend_before_ms=10 * 60 * 1000,
            extend_after_ms=10 * 60 * 1000,
        )


def spike_filter(data: AvailableData) -> None:
    for neph in data.select_instrument((
            {"instrument_id": "S11"}
    )):
        for var in neph.select_variable((
                {"variable_name": "backscattering_coefficient"},
                {"standard_name": "volume_backwards_scattering_coefficient_in_air_due_to_dried_aerosol_particles"},
        )):
            for wavelengths, value_select, _ in var.select_wavelengths():
                if len(wavelengths) < 2:
                    continue
                for widx in range(len(wavelengths)):
                    wavelength_selector = value_select[widx]
                    values = var[wavelength_selector]

                    threshold_values = np.full_like(values, np.nan)
                    for second_widx in range(len(wavelengths)):
                        if second_widx == widx:
                            continue
                        second_wavelength_selector = value_select[second_widx]
                        second_values = var[second_wavelength_selector]

                        update_threshold = np.logical_and(
                            np.isfinite(second_values),
                            np.logical_or(
                                np.invert(np.isfinite(threshold_values)),
                                threshold_values < second_values
                            )
                        )
                        threshold_values[update_threshold] = second_values[update_threshold]

                    threshold_values += 8

                    to_invalidate = values > threshold_values
                    values[to_invalidate] = nan
                    var[wavelength_selector] = values


        for var in neph.select_variable((
                {"variable_name": "scattering_coefficient"},
                {"standard_name": "volume_scattering_coefficient_in_air_due_to_dried_aerosol_particles"},
                {"variable_name": "backscattering_coefficient"},
                {"standard_name": "volume_backwards_scattering_coefficient_in_air_due_to_dried_aerosol_particles"},
        )):
            threshold_values = hourly_median(var)
            threshold_values *= 2.5
            for wavelengths, value_select, _ in var.select_wavelengths():
                for widx in range(len(wavelengths)):
                    wavelength_selector = value_select[widx]
                    values = var[wavelength_selector]
                    threshold = threshold_values[wavelength_selector]
                    to_invalidate = np.logical_and(
                        values > threshold,
                        values > 5.0,
                    )
                    values[to_invalidate] = nan
                    var[wavelength_selector] = values


def run(data: AvailableData) -> None:
    aerosol_contamination(data)
    spike_filter(data)

    standard_stp_corrections(data)
    absorption_corrections(data)
    standard_scattering_corrections(data)
    standard_intensives(data)
    standard_meteorological(data)


if __name__ == '__main__':
    from forge.processing.context import processing_main

    processing_main(run)
