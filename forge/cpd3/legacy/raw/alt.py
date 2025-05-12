#!/usr/bin/env python3

import typing
import os
import numpy as np
from math import nan
from forge.data.structure.timeseries import cutsize_variable, variable_coordinates
from forge.const import STATIONS as VALID_STATIONS
from forge.cpd3.legacy.raw.write import InstrumentTimeConversion as C
from forge.cpd3.legacy.instrument.grimm110xopc import Converter as Grimm110xOPC
from forge.cpd3.legacy.instrument.tsi377xcpc import Converter as TSI377xCPC
from forge.cpd3.legacy.instrument.psap1w import Converter as PSAP1W
from forge.cpd3.legacy.instrument.psap3w import Converter as PSAP3W
from forge.cpd3.legacy.instrument.tsi3563nephelometer import Converter as TSI3563Nephelometer
from forge.cpd3.legacy.instrument.generic_size_distribution import Converter as BaseSizeDistribution

STATION = os.path.basename(__file__).split('.', 1)[0].lower()
assert STATION in VALID_STATIONS


class SMPS(BaseSizeDistribution):
    def run(self) -> bool:
        if not super().run():
            return False

        g = self.root.groups["data"]
        times = g.variables["time"][...].data
        self.declare_system_flags(g, times, flags_map={
            "ContaminateFault": "data_contamination_smps_not_ok"
        })

        return True


def force_cut(cls, cut_schedule: typing.Union[float, typing.Dict[typing.Tuple[float, float], float]] = 10.0):
    class Result(cls):
        def apply_cut_size(self, g, group_times, variables, wavelength_variables=None, **kwargs) -> None:
            if wavelength_variables:
                for var, data in wavelength_variables:
                    if var is None:
                        continue
                    selected_data = data[0]
                    for check_data in data:
                        if check_data.time.shape[0] > selected_data.time.shape[0]:
                            selected_data = check_data
                    variables.append((var, selected_data))

            var = cutsize_variable(g)
            variable_coordinates(g, var)
            if isinstance(cut_schedule, float) or isinstance(cut_schedule, int):
                var[:] = cut_schedule
            else:
                cut_data = np.full(group_times.shape, nan, dtype=np.float64)

                minute_of_hour = np.empty_like(group_times, dtype=np.int64)
                np.floor((group_times % (60 * 60 * 1000)) / (60 * 1000), out=minute_of_hour, casting='unsafe')

                for (start_minute, end_minute), cut_val in cut_schedule.items():
                    apply_values = np.logical_and(
                        minute_of_hour >= start_minute,
                        minute_of_hour < end_minute,
                    )
                    cut_data[apply_values] = cut_val

                var[:] = cut_data

            for var in variables:
                cut_var = var[0]
                if cut_var is None:
                    continue
                ancillary_variables = set(getattr(cut_var, 'ancillary_variables', "").split())
                ancillary_variables.add('cut_size')
                cut_var.ancillary_variables = " ".join(sorted(ancillary_variables))

    return Result


C.run(STATION, {
    "A11": [
        C(force_cut(PSAP1W, 10.0), start='2004-03-18', end='2005-03-31'),
        C(force_cut(PSAP1W, {
            (0, 15): 10.0,
            (15, 30): 1.0,
            (30, 45): 10.0,
            (45, 60): 1.0,
        }), start='2005-03-31', end='2005-05-30'),
        C(force_cut(PSAP1W, 10.0), start='2005-05-30', end='2005-07-29'),
        C(force_cut(PSAP1W, {
            (0, 15): 10.0,
            (15, 30): 1.0,
            (30, 45): 10.0,
            (45, 60): 1.0,
        }), start='2005-07-29', end='2005-12-06'),
        C('psap1w', start='2005-12-06', end='2006-03-01'),
        C(force_cut(PSAP1W, {
            (0, 15): 10.0,
            (15, 30): 1.0,
            (30, 45): 10.0,
            (45, 60): 1.0,
        }), start='2006-03-01', end='2006-03-30'),
        C('psap1w', start='2006-03-30', end='2007-07-11'),
        C('psap3w', start='2007-07-11', end='2008-10-30T17:00:00Z'),
        C(force_cut(PSAP3W, {
            (0, 15): 10.0,
            (15, 30): 1.0,
            (30, 45): 10.0,
            (45, 60): 1.0,
        }), start='2008-10-30T17:00:00Z', end='2008-11-04'),
        C('psap3w', start='2008-11-04', end='2014-08-31'),
        C('psap3w+secondary', start='2014-08-31', end='2017-03-28'),
        C('clap', start='2017-03-28'),
    ],
    "A12": [
        C('psap1w+secondary', start='2007-07-11', end='2010-03-30'),
        C('clap', start='2014-08-31', end='2017-03-28'),
        C('clap+secondary', start='2018-01-12'),
    ],
    "A13": [ C('psap3w+secondary', start='2018-01-16'), ],
    "A81": [
        C('mageeae31', start='2008-04-27', end='2019-03-24'),
        C('mageeae31+secondary', start='2019-03-24', end='2023-11-28'),
    ],
    "A82": [ C('mageeae33', start='2019-03-24'), ],
    "E81": [ C('dmtpax+secondary', start='2012-03-29', end='2014-10-17'), ],
    "N11": [
        C(Grimm110xOPC, start='2011-02-25', end='2017-01-18'),
        C(Grimm110xOPC.with_instrument_override(serial_number="11A15096", model="11-A"), start='2017-01-18', end='2024-03-28'),
        C(Grimm110xOPC, start='2024-03-28'),
    ],
    "N12": [ C(SMPS, start='2011-03-15'), ],
    "N61": [
        C('tsi3010cpc', start='2004-03-18', end='2011-01-23'),
        C(TSI377xCPC.with_instrument_override(serial_number="71033080", model="3772"), start='2011-01-23', end='2023-06-26'),
        C(TSI377xCPC, start='2023-06-26'),
    ],
    "N62": [
        C(TSI377xCPC.with_instrument_override(serial_number="71033080", model="3772").with_added_tag("secondary"),
          start='2015-11-30', end='2020-08-14'),
        C(TSI377xCPC.with_instrument_override(serial_number="3772150501", model="3772").with_added_tag("secondary"),
          start='2020-08-14', end='2023-11-29'),
        C(TSI377xCPC.with_added_tag("secondary"), start='2023-11-29'),
    ],
    "S11": [
        C(force_cut(TSI3563Nephelometer, 10.0), start='2004-03-18', end='2005-03-31'),
        C(force_cut(TSI3563Nephelometer, {
            (0, 15): 10.0,
            (15, 30): 1.0,
            (30, 45): 10.0,
            (45, 60): 1.0,
        }), start='2005-03-31', end='2005-05-30'),
        C(force_cut(TSI3563Nephelometer, 10.0), start='2005-05-30', end='2005-07-29'),
        C(force_cut(TSI3563Nephelometer, {
            (0, 15): 10.0,
            (15, 30): 1.0,
            (30, 45): 10.0,
            (45, 60): 1.0,
        }), start='2005-07-29', end='2005-12-06'),
        C('tsi3563nephelometer', start='2005-12-06', end='2006-03-01'),
        C(force_cut(TSI3563Nephelometer, {
            (0, 15): 10.0,
            (15, 30): 1.0,
            (30, 45): 10.0,
            (45, 60): 1.0,
        }), start='2006-03-01', end='2006-03-30'),
        C('tsi3563nephelometer', start='2006-03-30', end='2008-10-30T17:00:00Z'),
        C(force_cut(TSI3563Nephelometer, {
            (0, 15): 10.0,
            (15, 30): 1.0,
            (30, 45): 10.0,
            (45, 60): 1.0,
        }), start='2008-10-30T17:00:00Z', end='2008-11-04T12:00:00Z'),
        C('tsi3563nephelometer', start='2008-11-04T12:00:00Z'),
    ],
    "XM1": [ C('generic_met'), ],
})
