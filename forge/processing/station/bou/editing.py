#!/usr/bin/env python3
import typing
from forge.processing.context import AvailableData
from forge.processing.station.default.editing import standard_corrections, standard_intensives, standard_meteorological


def cpd2_ozone_zero_fix(data: AvailableData) -> None:
    # Reverse realtime zero subtraction.  The original versions of CPD2 subtracted the calculated zero value from the
    # reported ozone concentration.  This is not what was desired, so reverse that by adding the subtracted values
    # back in.  This subtraction needs to happen before calibration, so the calibration is backed out and re-applied
    calibration = [0.088496, 1.048]
    for instrument in data.select_instrument((
            {"instrument": "thermo49"},
    ), start="2015-10-01T22:12:08Z", end="2015-10-21T08:13:10Z"):
        for concentration, zero in instrument.select_variable((
                {"variable_name": "ozone_mixing_ratio"},
                {"standard_name": "mole_fraction_of_ozone_in_air"},
        ), {"variable_name": "zero_ozone_mixing_ratio"}):
            values = (concentration[:] - calibration[0]) / calibration[1]
            values += zero[:]
            concentration[:] = values * calibration[1] + calibration[0]


def run(data: AvailableData) -> None:
    cpd2_ozone_zero_fix(data)
    standard_corrections(data)
    standard_intensives(data)
    standard_meteorological(data)


if __name__ == '__main__':
    from forge.processing.context import processing_main
    processing_main(run)
