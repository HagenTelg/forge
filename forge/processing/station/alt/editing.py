#!/usr/bin/env python3
import typing
import numpy as np
from math import nan
from forge.processing.station.lookup import station_data
from forge.processing.context import AvailableData
from forge.processing.corrections import *
from forge.processing.station.default.editing import standard_absorption_corrections, standard_scattering_corrections, standard_intensives, standard_meteorological, standard_stp_corrections
from forge.processing.corrections.filter_absorption import spot_area_adjustment
from forge.processing.derived.wavelength import align_wavelengths
from forge.data.flags import parse_flags


def stp_corrections(data: AvailableData) -> None:
    for instrument in data.select_instrument((
            {"tags": "aerosol size -grimm110xopc"},
    )):
        to_stp(instrument, temperature=12.0,
               pressure=station_data(instrument.station, 'climatology',
                                     'surface_pressure')(instrument.station))

    standard_stp_corrections(data)


def absorption_corrections(data: AvailableData) -> None:
    # Spot size correction for 1W PSAP (S/N 0028) - Dan Veber
    for start, end in (
            ("2004-03-18", "2004-04-19"),
            ("2005-07-06", "2007-04-14"),
            ("2007-04-19", "2007-07-11"),
            ("2008-09-15T14:24:00Z", "2008-10-15T12:00:00Z"),
    ):
        for absorption in data.select_instrument((
                {"instrument_id": "A11"},
        ), start=start, end=end):
            spot_area_adjustment(absorption, 19.64, 21.6)
    for start, end in (
            ("2004-04-19", "2005-07-06"),
            ("2007-07-11T16:48:00Z", "2008-09-15T14:24:00Z"),
            ("2008-10-15T12:00:00Z", "2008-10-31T00:00:00Z"),
            ("2008-11-05", "2007-04-14"),
    ):
        for absorption in data.select_instrument((
                {"instrument_id": "A12"},
        ), start=start, end=end):
            spot_area_adjustment(absorption, 22.4, 21.6)

    # CPD1/2 data: already has Weiss applied for PSAPs
    for absorption, scattering in data.select_instrument((
            {"instrument": "psap1w"},
            {"instrument": "psap3w"},
    ), {"tags": "scattering -secondary"}, end="2017-07-17T12:44:00Z"):
        remove_low_transmittance(absorption)
        bond_1999(absorption, scattering)
    for absorption, scattering in data.select_instrument((
            {"instrument": "bmitap"},
            {"instrument": "clap"},
    ), {"tags": "scattering -secondary"}, end="2017-07-17T12:44:00Z"):
        remove_low_transmittance(absorption)
        weiss(absorption)
        bond_1999(absorption, scattering)

    standard_absorption_corrections(data, start="2017-07-17T12:44:00Z")


def leaky_zero_correction(data: AvailableData) -> None:
    # Fix the leaky background filter (SS emails on 2014-12-08).  This corrects them by backing out the orginal
    # one and replacing it with the backgrounds calculated from averages after the neph was fixed.

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
    ), start="2011-02-16", end="2012-01-01"):
        for total in neph.select_variable((
                {"variable_name": "scattering_coefficient"},
                {"standard_name": "volume_scattering_coefficient_in_air_due_to_dried_aerosol_particles"},
        )):
            wall = neph.get_input(total, {"variable_name": "wall_scattering_coefficient"})
            apply(total, wall, {
                450.0: 3.452,
                550.0: 2.883,
                700.0: 9.540,
            })
        for back in neph.select_variable((
                {"variable_name": "backscattering_coefficient"},
                {"standard_name": "volume_backwards_scattering_coefficient_in_air_due_to_dried_aerosol_particles"},
        )):
            wall = neph.get_input(back, {"variable_name": "wall_backscattering_coefficient"})
            apply(back, wall, {
                450.0: 1.841,
                550.0: 1.666,
                700.0: 6.240,
            })


def slew_edits(data: AvailableData):
    def windowed_slope(
            x: np.ndarray,
            y: np.ndarray,
            window: int = 2,
    ) -> np.ndarray:
        x = np.concatenate(([0], x))
        y = np.concatenate(([0], y))

        sx = np.cumsum(x)
        sy = np.cumsum(y)
        sxx = np.cumsum(x * x)
        syy = np.cumsum(x * y)

        sx = sx[window:] - sx[:-window]
        sy = sy[window:] - sy[:-window]
        sxx = sxx[window:] - sxx[:-window]
        syy = syy[window:] - syy[:-window]

        num = window * syy - sx * sy
        div = window * sxx - sx * sx
        valid = div != 0.0
        result = np.full(div.shape, nan, dtype=np.float64)
        result[valid] = num[valid] / div[valid]
        return result

    for neph in data.select_instrument((
            {"instrument_id": "S11"}
    ), start="2004-07-20T18:46:33Z ", end="2004-07-25T05:21:19Z"):
        for var in neph.select_variable((
                {"variable_name": "scattering_coefficient"},
                {"standard_name": "volume_scattering_coefficient_in_air_due_to_dried_aerosol_particles"},
                {"variable_name": "backscattering_coefficient"},
                {"standard_name": "volume_backwards_scattering_coefficient_in_air_due_to_dried_aerosol_particles"},
        )):
            for wavelengths, value_select, time_select in var.select_wavelengths():
                for widx in range(len(wavelengths)):
                    wavelength_selector = value_select[widx]
                    values = var[wavelength_selector]
                    slope = windowed_slope(var.times[time_select] / (60.0 * 1000.0), values)
                    slope = np.concatenate((slope, [slope[-1]]))
                    to_invalidate = np.logical_or(
                        slope < -0.008333,
                        slope > 0.066667,
                    )
                    values[to_invalidate] = nan
                    var[wavelength_selector] = values


def aerosol_contamination(data: AvailableData) -> None:
    for aerosol, wind in data.select_instrument(
            {"tags": "aerosol -met"},
            {"instrument_id": "XM1"},
            always_tuple=True,
    ):
        wind_sector_contamination(
            aerosol, wind,
            contaminated_sector=((340, 45),),
            contaminated_minimum_speed=0.1,
            extend_before_ms=5 * 60 * 1000,
            extend_after_ms=5 * 60 * 1000,
        )

    def remove_contamination(start, end, flag="data_contamination_"):
        for aerosol in data.select_instrument((
                {"tags": "aerosol"},
        ), start=start, end=end):
            for system_flags in aerosol.system_flags():
                flags = parse_flags(system_flags.variable)
                matched_bits = 0
                for bits, name in flags.items():
                    if not name.startswith(flag):
                        continue
                    matched_bits |= bits
                if matched_bits == 0:
                    continue
                mask = np.array(matched_bits, dtype=np.uint64)
                mask = np.invert(mask)
                system_flags[:] = system_flags[:] & mask

    # Various uncontaminate edits, with no comments provided
    for start, end in (
            ("2004-04-02T17:10:49Z", "2004-04-03T11:16:43Z"),
            ("2004-04-11T21:14:45Z", "2004-04-14T12:11:48Z"),
            ("2004-05-09T01:49:48Z", "2004-05-09T13:04:19Z"),
            ("2004-05-09T22:13:20Z", "2004-05-11T16:34:31Z"),
            ("2004-05-20T11:49:01Z", "2004-05-22T00:09:25Z"),
            ("2004-05-22T06:41:34Z", "2004-05-23T16:40:47Z"),
            ("2004-06-12T23:36:24Z", "2004-06-16T18:21:38Z"),
            ("2004-06-24T19:56:04Z", "2004-06-26T17:02:57Z"),
            ("2004-07-19T21:22:37Z", "2004-07-20T12:51:09Z"),
            ("2004-07-20T21:14:45Z", "2004-07-21T21:38:22Z"),
            ("2004-08-22T07:28:31Z", "2004-08-23T07:36:24Z"),
            ("2005-06-20T12:23:15Z", "2005-06-22T11:40:41Z"),
            ("2005-06-27T05:42:04Z", "2005-06-29T10:46:18Z"),
            ("2005-07-21T01:23:33Z", "2005-07-21T17:40:53Z"),
            ("2005-07-27T09:46:24Z", "2005-07-28T02:35:16Z"),
            ("2005-07-29T03:48:34Z", "2005-07-31T09:55:52Z"),
            ("2005-08-02T21:18:25Z", "2005-08-06T15:09:33Z"),
            ("2005-08-12T05:08:58Z", "2005-08-18T08:16:33Z"),
            ("2006-08-03T23:31:38Z", "2006-08-05T18:26:39Z"),
            ("2006-09-09T11:02:48Z", "2006-09-10T04:55:53Z"),
            ("2006-09-11T14:20:03Z", "2006-09-13T05:47:11Z"),
            ("2006-09-19T11:02:12Z", "2006-09-20T02:20:47Z"),
            ("2006-10-10T20:32:23Z", "2006-10-11T09:23:06Z"),
            ("2006-11-01T07:08:37Z", "2006-11-02T03:51:13Z"),
            ("2006-11-02T21:40:48Z", "2006-11-05T04:43:55Z"),
            ("2007-01-28T21:23:30Z", "2007-01-30T14:32:58Z"),
            ("2007-02-27T01:35:57Z", "2007-02-27T19:09:48Z"),
            ("2007-03-01T00:00:00Z", "2007-03-21T00:00:00Z"),
            ("2007-05-06T03:18:31Z", "2007-05-07T16:19:57Z"),
            ("2007-05-27T09:30:20Z", "2007-05-28T14:39:07Z"),
            ("2007-06-03T01:40:50Z", "2007-06-04T15:13:47Z"),
            ("2007-06-14T04:20:49Z", "2007-06-17T00:34:46Z"),
            ("2007-07-17T01:22:12Z", "2007-07-18T23:44:12Z"),
            ("2007-09-03T09:35:22Z", "2007-09-05T12:10:16Z"),
            ("2007-09-14T22:28:19Z", "2007-09-15T16:38:59Z"),
            ("2007-10-15T01:40:47Z", "2007-10-15T12:57:52Z"),
            ("2007-10-25T03:38:52Z", "2007-10-26T23:44:15Z"),
            ("2007-12-11T18:06:30Z", "2007-12-12T21:00:30Z"),
            ("2008-02-13T21:42:40Z", "2008-02-14T15:29:55Z"),
            ("2008-02-20T00:27:28Z", "2008-02-22T18:53:57Z"),
            ("2008-02-23T12:09:49Z", "2008-02-24T04:22:53Z"),
            ("2008-02-26T20:43:49Z", "2008-02-27T00:07:51Z"),
            ("2008-05-30T01:04:15Z", "2008-05-31T08:40:13Z"),
            ("2008-07-11T00:18:52Z", "2008-07-13T15:11:47Z"),
            ("2008-07-15T18:48:44Z", "2008-07-17T04:05:14Z"),
            ("2008-08-22T22:09:22Z", "2008-08-23T19:59:12Z"),
            ("2008-08-25T23:27:49Z", "2008-08-27T13:27:54Z"),
            ("2008-12-02T04:20:34Z", "2008-12-04T11:30:15Z"),
            ("2009-07-24T00:47:10Z", "2009-07-30T18:11:00Z"),
            ("2009-07-31T21:25:56Z", "2009-08-07T23:44:17Z"),
            ("2010-06-06T02:38:25Z", "2010-06-07T11:20:59Z"),
            ("2010-09-24T06:14:25Z", "2010-09-25T18:42:20Z"),
            ("2010-09-28T00:38:27Z", "2010-09-28T07:48:43Z"),
            ("2010-09-28T17:52:54Z", "2010-09-29T11:25:40Z"),
            ("2010-10-13T02:28:18Z", "2010-10-16T00:30:13Z"),
            ("2010-11-02T18:24:57Z", "2010-11-05T05:46:02Z"),
            ("2010-12-09T06:54:41Z", "2010-12-09T22:33:25Z"),
            ("2010-12-11T10:47:05Z", "2010-12-12T07:58:29Z"),
            ("2010-12-17T08:21:16Z", "2010-12-17T16:37:58Z"),
            ("2010-12-18T17:05:19Z", "2010-12-20T17:55:27Z"),
            ("2010-12-26T17:28:06Z", "2010-12-27T02:07:36Z"),
            ("2010-12-29T18:27:21Z", "2010-12-30T06:31:54Z"),
            ("2011-04-26T11:33:00Z", "2011-04-26T16:12:12Z"),
            ("2011-04-29T18:16:42Z", "2011-04-29T23:32:32Z"),
            ("2011-05-18T11:56:48Z", "2011-05-18T20:20:56Z"),
            ("2011-05-19T05:50:09Z", "2011-05-20T14:27:07Z"),
            ("2011-05-25T00:55:00Z", "2011-05-25T23:27:00Z"),
            ("2011-05-27T11:30:13Z", "2011-05-28T04:04:44Z"),
            ("2011-06-01T09:49:23Z", "2011-06-02T11:20:08Z"),
            ("2011-06-09T22:33:30Z", "2011-06-10T18:59:07Z"),
            ("2011-06-14T16:41:52Z", "2011-06-18T13:00:02Z"),
            ("2011-06-21T07:12:16Z", "2011-06-23T14:06:25Z"),
            ("2011-06-25T02:12:19Z", "2011-06-28T12:35:48Z"),
            ("2011-06-29T02:52:11Z", "2011-06-29T07:28:35Z"),
            ("2011-07-01T04:01:58Z", "2011-07-01T13:05:42Z"),
            ("2011-07-02T13:33:48Z", "2011-07-02T23:22:51Z"),
            ("2011-07-04T06:20:37Z", "2011-07-04T18:34:40Z"),
            ("2011-07-05T10:17:09Z", "2011-07-07T23:00:11Z"),
            ("2011-07-08T22:01:17Z", "2011-07-11T08:50:09Z"),
            ("2011-07-12T07:34:38Z", "2011-07-13T15:58:47Z"),
            ("2011-07-15T13:32:07Z", "2011-07-16T23:32:30Z"),
            ("2011-08-09T05:32:27Z", "2011-08-12T13:35:31Z"),
            ("2011-08-12T00:00:00Z", "2011-08-18T00:00:00Z"),
            ("2011-08-21T08:41:38Z", "2011-08-22T13:20:49Z"),
            ("2011-08-27T16:17:05Z", "2011-08-30T04:18:00Z"),
            ("2011-08-30T05:56:48Z", "2011-09-01T11:11:31Z"),
            ("2011-09-04T13:08:53Z", "2011-09-05T12:43:10Z"),
            ("2011-09-07T07:43:47Z", "2011-09-09T22:47:27Z"),
            ("2011-09-12T10:56:38Z", "2011-09-15T19:13:28Z"),
            ("2011-09-17T00:00:00Z", "2011-09-22T00:00:00Z"),
            ("2011-09-23T22:53:13Z", "2011-09-26T19:34:41Z"),
    ):
        remove_contamination(start, end)


def run(data: AvailableData) -> None:
    slew_edits(data)
    leaky_zero_correction(data)
    aerosol_contamination(data)

    stp_corrections(data)
    absorption_corrections(data)
    standard_scattering_corrections(data)

    standard_intensives(data)
    standard_meteorological(data)


if __name__ == '__main__':
    from forge.processing.context import processing_main
    processing_main(run)
