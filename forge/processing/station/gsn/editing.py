#!/usr/bin/env python3
import typing
from forge.processing.context import AvailableData
from forge.processing.corrections import *
from forge.processing.station.default.editing import standard_absorption_corrections, standard_scattering_corrections, standard_intensives, standard_meteorological, standard_stp_corrections
from forge.processing.derived.intensives import generate_intensives, AdjustWavelengthParameters


def absorption_corrections(data: AvailableData) -> None:
    # CPD1/2 data: already has Weiss applied for PSAPs
    for absorption, scattering in data.select_instrument((
            {"instrument": "psap1w"},
            {"instrument": "psap3w"},
    ), {"tags": "scattering -secondary"}, end="2016-06-28"):
        remove_low_transmittance(absorption)
        bond_1999(absorption, scattering)
    for absorption, scattering in data.select_instrument((
            {"instrument": "bmitap"},
            {"instrument": "clap"},
    ), {"tags": "scattering -secondary"}, end="2016-06-28"):
        remove_low_transmittance(absorption)
        weiss(absorption)
        bond_1999(absorption, scattering)

    # Normal corrections now
    standard_absorption_corrections(data, start="2016-06-28")


def run(data: AvailableData) -> None:
    standard_stp_corrections(data)
    absorption_corrections(data)
    standard_scattering_corrections(data)

    for start, end in (
        ("2011-10-01", "2012-02-10T03:00:00Z"),
        ("2012-09-06T03:00:00Z", None),
    ):
        for S11, A11, pid, dilution_flow in data.select_multiple(
                {"instrument_id": "S11"},
                {"instrument_id": "A11"},
                {"instrument": "lovepid"},
                {"instrument_id": "Q12"},
                start=start, end=end,
        ):
            dilution(
                (S11, A11),
                (
                    {"data": pid, "flow": {"variable_id": "Q_Q11"}},
                    {"data": A11, "flow": {"variable_name": "sample_flow"}, "fallback": 0.0},
                ), (
                    {"data": dilution_flow, "flow": {"variable_name": "sample_flow"}},
                )
            )

    # PSAP-1W extrapolation
    for intensives, scattering, absorption, cpc in data.derive_output(
            "XI",
            {"tags": "scattering -secondary"},
            {"tags": "absorption -secondary -aethalometer -thermomaap"},
            {"tags": "cpc -secondary"},
            tags=("aerosol", "intensives"),
            end="2002-02-08",
    ):
        generate_intensives(intensives, cpc, scattering, absorption, wavelength_adjustment=AdjustWavelengthParameters(
            fallback_angstrom_exponent=1.0,
        ))
    standard_intensives(data, start="2002-02-08")

    standard_meteorological(data)


if __name__ == '__main__':
    from forge.processing.context import processing_main
    processing_main(run)
