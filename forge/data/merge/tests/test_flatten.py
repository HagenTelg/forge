import pytest
import typing
import numpy as np
from math import nan
from netCDF4 import Dataset
from forge.data.merge.flatten import MergeFlatten
from forge.data.structure.timeseries import time_coordinate


def test_basic(tmp_path):
    data1 = Dataset(str(tmp_path / "input1.nc"), 'w', format='NETCDF4')
    data1.setncattr("attr1", "value1")
    instrument_group = data1.createGroup("instrument")
    instrument_group.setncattr("attr2", "value2")

    data_group = data1.createGroup("data")

    var = data_group.createVariable("constant1", "f8", ())
    assert var.dtype == np.float64
    var.setncattr("attr3", "value3")
    var[:] = 1.0

    var = time_coordinate(data_group)
    var.setncattr("attr2", "value4")
    var[:] = [1000, 2000, 3000, 4000]

    var = data_group.createVariable("value", "f8", ("time",), fill_value=nan)
    var[:] = [10.0, 11.0, 12.0, 13.0]

    data2 = Dataset(str(tmp_path / "input2.nc"), 'w', format='NETCDF4')
    data2.setncattr("attr1", "value12")
    instrument_group = data2.createGroup("instrument")
    instrument_group.setncattr("attr2", "value22")

    data_group = data2.createGroup("data")

    var = data_group.createVariable("constant1", "f8", ())
    assert var.dtype == np.float64
    var.setncattr("attr3", "value3")
    var[:] = 1.5

    var = time_coordinate(data_group)
    var.setncattr("attr2", "value4")
    var[:] = [999, 2000, 3000, 4000]

    var = data_group.createVariable("value", "f8", ("time",), fill_value=nan)
    var[:] = [10.5, 11.5, 12.5, 13.5]

    merge = MergeFlatten()
    merge.add_source(data1, "source1")
    merge.add_source(data2, "source2")
    output = merge.execute(tmp_path / "output.nc")
    merge = None
    data1.close()
    data1 = None
    data2.close()
    data2 = None

    output_group = output.groups['instrument'].groups['source1']
    assert output_group.getncattr("attr1") == "value1"
    assert output_group.getncattr("attr2") == "value2"
    output_group = output.groups['instrument'].groups['source2']
    assert output_group.getncattr("attr1") == "value12"
    assert output_group.getncattr("attr2") == "value22"

    output_group = output.groups['data']
    assert len(output_group.variables) == 1
    assert len(output_group.dimensions) == 1

    dim = output_group.dimensions["time"]
    assert dim.isunlimited()
    assert dim.size == 4

    var = output_group.variables["time"]
    assert var.dtype == np.int64
    assert list(var[:]) == [1000, 2000, 3000, 4000]

    output_group = output.groups['data'].groups['source1']
    assert len(output_group.variables) == 2
    assert len(output_group.dimensions) == 0

    var = output_group.variables["constant1"]
    assert var.getncattr("attr3") == "value3"
    assert var.size == 1
    assert var[0] == 1.0

    var = output_group.variables["value"]
    assert var.dtype == np.float64
    assert list(var[:]) == [10.0, 11.0, 12.0, 13.0]

    output_group = output.groups['data'].groups['source2']
    assert len(output_group.variables) == 2
    assert len(output_group.dimensions) == 0

    var = output_group.variables["constant1"]
    assert var.getncattr("attr3") == "value3"
    assert var.size == 1
    assert var[0] == 1.5

    var = output_group.variables["value"]
    assert var.dtype == np.float64
    assert list(var[:]) == [10.5, 11.5, 12.5, 13.5]
