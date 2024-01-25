import pytest
import numpy as np
from math import nan
from netCDF4 import Dataset, Variable, Group
from forge.data.structure import instrument_timeseries
from forge.data.structure.timeseries import time_coordinate
from forge.data.structure.variable import variable_flags, variable_rh, variable_absorption
from forge.processing.average.contamination import invalidate_contamination


def test_basic_contamination(tmp_path):
    file = Dataset(str(tmp_path / "test.nc"), 'w', format='NETCDF4')

    times = np.array([1000, 2000, 3000, 4000])

    instrument_timeseries(
        file, "NIL", "X1",
        float(times[0]) / 1000.0, float(times[-1]) / 1000.0 + 1,
        1, {"aerosol", "testtag"}
    )

    data_group: Group = file.createGroup("data")

    time_var = time_coordinate(data_group)
    time_var[:] = times

    flags_var: Variable = data_group.createVariable("system_flags", "u8", ("time",), fill_value=False)
    variable_flags(flags_var, {1: "data_contamination_test"})
    flags_var[:] = [0, 1, 0, 0]

    var_contam = data_group.createVariable("light_absorption", "f8", ("time",), fill_value=False)
    variable_absorption(var_contam)
    var_contam[:] = [10, 11, 12, 13]

    var_no_contam = data_group.createVariable("sample_rh", "f8", ("time",), fill_value=False)
    variable_rh(var_no_contam)
    var_no_contam[:] = [20, 21, 22, 23]

    invalidate_contamination(file)

    assert time_var[:].tolist() == times.tolist()
    assert flags_var[:].tolist() == [0, 1, 0, 0]
    assert var_contam[:].tolist() == pytest.approx([10, nan, 12, 13], nan_ok=True)
    assert var_no_contam[:].tolist() == pytest.approx([20, 21, 22, 23], nan_ok=True)
