#!/usr/bin/env python3
import typing
import numpy as np
from forge.processing.context import AvailableData
from forge.processing.corrections import *
from forge.processing.corrections.climatology import vaisala_hmp_limits
from forge.processing.station.default.editing import standard_absorption_corrections, standard_scattering_corrections, standard_intensives, standard_meteorological, standard_stp_corrections
from forge.data.flags import parse_flags


def absorption_corrections(data: AvailableData) -> None:
    # CPD1/2 data: already has Weiss applied for PSAPs
    for absorption, scattering in data.select_instrument((
            {"instrument": "psap1w"},
            {"instrument": "psap3w"},
    ), {"tags": "scattering -secondary"}, end="2015-08-17T17:57:00Z"):
        remove_low_transmittance(absorption)
        bond_1999(absorption, scattering)
    for absorption, scattering in data.select_instrument((
            {"instrument": "bmitap"},
            {"instrument": "clap"},
    ), {"tags": "scattering -secondary"}, end="2015-08-17T17:57:00Z"):
        remove_low_transmittance(absorption)
        weiss(absorption)
        bond_1999(absorption, scattering)

    # Normal corrections now
    standard_absorption_corrections(data, start="2015-08-17T17:57:00Z")


def aerosol_contamination(data: AvailableData) -> None:
    for aerosol, wind in data.select_instrument(
            {"tags": "aerosol -met"},
            {"instrument_id": "XM1"},
            always_tuple=True,
            start="2005-08-12",
            end="2023-01-01"
    ):
        wind_sector_contamination(
            aerosol, wind,
            contaminated_sector=((288, 55),),
            contaminated_minimum_speed=0.5,
            extend_before_ms=20*60*1000,
            extend_after_ms=20*60*1000,
        )

    # Updated to NSF wind sector for new data
    for aerosol, wind in data.select_instrument(
            {"tags": "aerosol -met"},
            {"instrument_id": "XM1"},
            always_tuple=True,
            start="2023-01-01",
    ):
        wind_sector_contamination(
            aerosol, wind,
            contaminated_sector=((345, 55),),
            contaminated_minimum_speed=0.5,
            extend_before_ms=20*60*1000,
            extend_after_ms=20*60*1000,
        )

    def remove_contamination(start, end):
        for aerosol in data.select_instrument((
                {"tags": "aerosol"},
        ), start=start, end=end):
            for system_flags in aerosol.system_flags():
                flags = parse_flags(system_flags.variable)
                matched_bits = 0
                for bits, name in flags.items():
                    if name != 'data_contamination_wind_sector':
                        continue
                    matched_bits |= bits
                if matched_bits == 0:
                    continue
                mask = np.array(matched_bits, dtype=np.uint64)
                mask = np.invert(mask)
                system_flags[:] = system_flags[:] & mask

    # Wind bird stuck events
    remove_contamination("2022-11-23T20:50:26Z", "2022-11-28T17:00:00Z")
    remove_contamination("2022-12-07T06:02:47Z", "2022-12-07T21:47:42Z")
    remove_contamination("2022-12-08T01:56:49Z", "2022-12-08T19:01:53Z")
    remove_contamination("2022-12-10T06:59:11Z", "2022-12-12T00:00:00Z")
    remove_contamination("2022-12-12T00:00:00Z", "2022-12-16T23:31:06Z")
    remove_contamination("2022-12-17T22:58:38Z", "2022-12-19T00:00:00Z")
    remove_contamination("2022-12-19T00:00:00Z", "2022-12-19T20:00:00Z")
    remove_contamination("2022-12-20T16:27:12Z", "2022-12-24T00:23:14Z")
    remove_contamination("2022-12-24T18:00:00Z", "2022-12-26T00:00:00Z")
    remove_contamination("2022-12-26T00:00:00Z", "2022-12-31T11:00:00Z")
    remove_contamination("2022-12-31T14:00:00Z", "2023-01-02T00:00:00Z")
    remove_contamination("2023-01-10T20:12:45Z", "2023-01-14T00:07:31Z")
    remove_contamination("2023-01-16T09:41:21Z", "2023-01-18T02:18:57Z")


def run(data: AvailableData) -> None:
    for met in data.select_instrument(
            {"instrument_id": "XM1"},
            start="2007-01-01",
            end="2012-08-16T12:00:00Z",
    ):
        meteorological_climatology_limits(
            met,
            normalized_temperature_rate_of_change=(-0.05, 0.05),
            normalized_humidity_rate_of_change=(-0.001667, 0.001667),
        )

    aerosol_contamination(data)

    standard_stp_corrections(data)
    absorption_corrections(data)
    standard_scattering_corrections(data)

    standard_intensives(data)
    standard_meteorological(data)

    for met in data.select_instrument({"instrument_id": "XM1"}, start="2016-07-05T17:16:00Z"):
        vaisala_hmp_limits(met)


if __name__ == '__main__':
    from forge.processing.context import processing_main
    processing_main(run)
