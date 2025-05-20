#!/usr/bin/env python3
import typing
import numpy as np
from math import nan
from forge.processing.context import AvailableData
from forge.processing.corrections import *
from forge.processing.station.default.editing import standard_stp_corrections, standard_absorption_corrections, standard_scattering_corrections, standard_intensives, standard_meteorological
from forge.data.flags import parse_flags
from forge.processing.derived.wavelength import align_wavelengths


def absorption_corrections(data: AvailableData):
    # Extend the zero data removal so that the CLAP doesn't catch the zero filter still being
    # switched (since data will include the partial minute during the switch).
    for clap, neph in data.select_instrument((
            {"instrument": "clap"},
    ), {"instrument_id": "S11"}, start="2012-05-30"):
        for absorption in clap.select_variable((
                {"variable_name": "light_absorption"},
                {"standard_name": "volume_absorption_coefficient_in_air_due_to_dried_aerosol_particles"},
                {"standard_name": "volume_extinction_coefficient_in_air_due_to_ambient_aerosol_particles"},
        )):
            try:
                source_flags = neph.get_input(absorption, {
                    "variable_name": "system_flags",
                })
            except FileNotFoundError:
                continue
            if not np.issubdtype(source_flags.values.dtype, np.integer):
                continue
            flags = parse_flags(source_flags.variable)
            matched_bits = 0
            for bits, name in flags.items():
                if name not in ("zero", "blank", "spancheck"):
                    continue
                matched_bits |= bits
            if matched_bits == 0:
                continue
            is_in_zero = np.bitwise_and(source_flags.values, matched_bits) != 0
            absorption[is_in_zero, ...] = nan

    standard_absorption_corrections(data)


def bad_zero_correction(data: AvailableData) -> None:
    # Fix bad neph zeros by backing out the original one and replacing it with the last known good one.
    # This event is "due to strong biomass burning" per a Mar email.

    def apply(scattering, wall, zero_replacement: typing.Dict[float, float]) -> None:
        def find_zero(wl) -> float:
            best = None
            best_wl = None
            for cwl, cv in zero_replacement.items():
                if best is None:
                    best = cv
                    best_wl = cwl
                    continue
                if abs(cwl - wl) < abs(best_wl - wl):
                    best = cv
                    best_wl = cwl
            return best or 0

        original_wall = align_wavelengths(wall, scattering)

        for wavelengths, value_select, _ in scattering.select_wavelengths():
            for widx in range(len(wavelengths)):
                wavelength_selector = value_select[widx]
                scattering[wavelength_selector] = scattering[wavelength_selector] + \
                                                  original_wall[wavelength_selector] - \
                                                  find_zero(wavelengths[widx])

    for neph in data.select_instrument((
            {"instrument_id": "S11"}
    ), start="2016-09-08T09:00:58Z", end="2016-09-08T20:06:17Z"):
        for total in neph.select_variable((
                {"variable_name": "scattering_coefficient"},
                {"standard_name": "volume_scattering_coefficient_in_air_due_to_dried_aerosol_particles"},
        )):
            wall = neph.get_input(total, {"variable_name": "wall_scattering_coefficient"})
            apply(total, wall, {
                450.0: 3.3,
                550.0: 4.15,
                700.0: 14.53,
            })
        for back in neph.select_variable((
                {"variable_name": "backscattering_coefficient"},
                {"standard_name": "volume_backwards_scattering_coefficient_in_air_due_to_dried_aerosol_particles"},
        )):
            wall = neph.get_input(back, {"variable_name": "wall_backscattering_coefficient"})
            apply(back, wall, {
                450.0: 1.33,
                550.0: 2.02,
                700.0: 11.47,
            })


def run(data: AvailableData) -> None:
    bad_zero_correction(data)
    standard_stp_corrections(data)
    absorption_corrections(data)
    standard_scattering_corrections(data)

    for start, end in (
        ("2012-07-19T11:45:00Z", "2016-03-16T10:45:00Z"),
        ("2016-05-25T09:15:00Z", "2021-11-02T08:39:00Z"),
        ("2023-07-03T11:21:00Z", "2023-08-30T11:30:00Z"),
    ):
        for S11, A11, sample_flow, dilution_flow in data.select_multiple(
                {"instrument_id": "S11"},
                {"instrument_id": "A11"},
                {"instrument_id": "Q11"},
                {"instrument_id": "Q12"},
                start=start, end=end
        ):
            dilution(
                (S11, A11),
                (
                    {"data": sample_flow, "flow": {"variable_name": "sample_flow"}},
                    {"data": A11, "flow": {"variable_name": "sample_flow"}, "fallback": 0.0},
                ), (
                    {"data": dilution_flow, "flow": {"variable_name": "sample_flow"}},
                )
            )
    for start, end in (
        ("2023-08-30T11:30:00Z", "2024-01-10T03:24:00Z"),
        ("2024-01-12T10:38:00Z", "2024-11-21T13:00:00Z"),
        ("2024-11-25T13:00:00Z", None),
    ):
        for S11, A11, sample_flow, dilution_flow in data.select_multiple(
                {"instrument_id": "S11"},
                {"instrument_id": "A11"},
                {"instrument_id": "Q11"},
                {"instrument_id": "Q12"},
                start=start, end=end
        ):
            dilution(
                (S11, A11),
                (
                    {"data": sample_flow, "flow": {"variable_name": "sample_flow"}},
                    {"data": A11, "flow": {"variable_name": "sample_flow"}, "fallback": 0.0},
                    4.95,  # 4.95 extra sample flow, Mar email 2023-08-30
                ), (
                    {"data": dilution_flow, "flow": {"variable_name": "sample_flow"}},
                )
            )
    # Flow meters offline, use constants, Mar email 2024-01-15
    for S11, A11 in data.select_multiple(
            {"instrument_id": "S11"},
            {"instrument_id": "A11"},
            start="2024-01-10T03:24:00Z", end="2024-01-12T10:38:00Z"
    ):
        dilution(
            (S11, A11),
            (
                26.74,
                {"data": A11, "flow": {"variable_name": "sample_flow"}, "fallback": 0.0},
                4.95,  # 4.95 extra sample flow, Mar email 2023-08-30
            ), (
                8.8,
            )
        )
    # Dilution flow meter offline, Mar email 2024-11-25
    for S11, A11, sample_flow in data.select_multiple(
            {"instrument_id": "S11"},
            {"instrument_id": "A11"},
            {"instrument_id": "Q11"},
            start="2024-11-21T13:00:00Z", end="2024-11-25T13:00:00Z"
    ):
        dilution(
            (S11, A11),
            (
                {"data": sample_flow, "flow": {"variable_name": "sample_flow"}},
                {"data": A11, "flow": {"variable_name": "sample_flow"}, "fallback": 0.0},
                4.95,  # 4.95 extra sample flow, Mar email 2023-08-30
            ), (
                10.3,
            )
        )

    standard_intensives(data)
    standard_meteorological(data)


if __name__ == '__main__':
    from forge.processing.context import processing_main
    processing_main(run)
