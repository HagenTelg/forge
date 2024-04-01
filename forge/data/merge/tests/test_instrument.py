import pytest
import typing
import numpy as np
from math import nan, isnan
from netCDF4 import Dataset
from forge.data.merge.instrument import MergeInstrument
from forge.data.structure.timeseries import time_coordinate, state_change_coordinate
from forge.data.structure.variable import variable_flags


def test_noop(tmp_path):
    data = Dataset(str(tmp_path / "data.nc"), 'w', format='NETCDF4')
    data.setncattr("attr1", "value1")
    data.setncattr("forge_tags", "tag1 tag2")
    data.setncattr("instrument", "instr")
    data.setncattr("time_coverage_start", "2023-10-11T00:00:00Z")
    data.setncattr("time_coverage_end", "2023-10-12T00:00:00Z")

    var = data.createVariable("constant1", "f8", ())
    assert var.dtype == np.float64
    var.setncattr("attr2", "value2")
    var[:] = 1.0

    group = data.createGroup("data")
    group.setncattr("attr1", 3.0)
    group.setncattr("attr3", "value3")

    var = time_coordinate(group)
    var.setncattr("attr2", "value4")
    var[:] = [1696982400000, 1697004000000, 1697025600000, 1697047200000]

    var = group.createVariable("value", "f8", ("time",), fill_value=nan)
    var[:] = [10.0, 11.0, 12.0, 13.0]

    group = data.createGroup("state")
    var = state_change_coordinate(group)
    var[:] = [1696960800000, 1696982400001, 1697004000000]

    var = group.createVariable("value", str, ("time",), fill_value=nan)
    var.cell_methods = "time: point"
    var[0] = "A"
    var[1] = "B"
    var[2] = "C"

    merge = MergeInstrument()
    merge.overlay(data, not_before_ms=1696982400000, not_after_ms=1697068800000)
    output = merge.execute(tmp_path / "output.nc")
    merge = None
    data.close()
    data = None

    assert output.getncattr("attr1") == "value1"
    assert output.getncattr("forge_tags") == "tag1 tag2"
    assert output.getncattr("instrument") == "instr"
    assert getattr(output, "instrument_history", None) is None
    assert output.getncattr("time_coverage_start") == "2023-10-11T00:00:00Z"
    assert output.getncattr("time_coverage_end") == "2023-10-12T00:00:00Z"

    var = output.variables["constant1"]
    assert var.getncattr("attr2") == "value2"
    assert var.size == 1
    assert var[0] == 1.0

    assert len(output.groups) == 2

    group = output.groups["data"]
    assert group.getncattr("attr1") == 3.0
    assert group.getncattr("attr3") == "value3"

    assert len(group.dimensions) == 1
    dim = group.dimensions["time"]
    assert dim.isunlimited()
    assert dim.size == 4

    assert len(group.variables) == 2
    var = group.variables["time"]
    assert var.dtype == np.int64
    assert var.getncattr("attr2") == "value4"
    assert list(var[:]) == [1696982400000, 1697004000000, 1697025600000, 1697047200000]

    var = group.variables["value"]
    assert var.dtype == np.float64
    assert list(var[:]) == [10.0, 11.0, 12.0, 13.0]

    group = output.groups["state"]
    assert len(group.dimensions) == 1
    dim = group.dimensions["time"]
    assert dim.isunlimited()
    assert dim.size == 3

    assert len(group.variables) == 2
    var = group.variables["time"]
    assert var.dtype == np.int64
    assert list(var[:]) == [1696960800000, 1696982400001, 1697004000000]

    var = group.variables["value"]
    assert var.dtype == str
    assert var.getncattr("cell_methods") == "time: point"
    assert list(var[:]) == ["A", "B", "C"]


def test_start_overlay(tmp_path):
    under = Dataset(str(tmp_path / "under.nc"), 'w', format='NETCDF4')
    under.setncattr("forge_tags", "tag1")
    under.setncattr("instrument", "underinst")
    under.setncattr("attr1", "under1")
    under.setncattr("attr2", "under2")
    under.setncattr("time_coverage_start", "2023-10-11T00:00:00Z")
    under.setncattr("time_coverage_end", "2023-10-12T00:00:00Z")
    group = under.createGroup("data")
    group.setncattr("attr1", "under3")
    group.setncattr("attr2", "under4")
    var = time_coordinate(group)
    var.setncattr("attr2", "value4")
    var[:] = [1696982400000, 1697004000000, 1697025600000, 1697047200000]
    var = group.createVariable("value", "f8", ("time",), fill_value=nan)
    var[:] = [10.0, 11.0, 12.0, 13.0]
    group = under.createGroup("state")
    var = state_change_coordinate(group)
    var[:] = [1696960800000, 1696982400001, 1697004000000, 1697025600000]
    var = group.createVariable("value", "i8", ("time",), fill_value=False)
    var.cell_methods = "time: point"
    var[:] = [14, 15, 16, 17]

    over = Dataset(str(tmp_path / "over.nc"), 'w', format='NETCDF4')
    over.setncattr("forge_tags", "tag2")
    over.setncattr("instrument", "overinst")
    over.setncattr("attr1", "over1")
    over.setncattr("attr3", "over2")
    over.setncattr("time_coverage_start", "2023-10-10T18:00:00Z")
    over.setncattr("time_coverage_end", "2023-10-11T12:00:00Z")
    group = over.createGroup("data")
    group.setncattr("attr1", "over3")
    group.setncattr("attr3", "over4")
    var = time_coordinate(group)
    var.setncattr("attr2", "value4")
    var[:] = [1696960800000, 1696982400000, 1697004000000]
    var = group.createVariable("value", "f8", ("time",), fill_value=nan)
    var[:] = [20.0, 21.0, 22.0]
    var = group.createVariable("auxiliary", "f8", ("time",), fill_value=nan)
    var[:] = [30.0, 31.0, 32.0]
    group = over.createGroup("state")
    var = state_change_coordinate(group)
    var[:] = [1696960800000, 1696982400001, 1697004000000]
    var = group.createVariable("value", "i8", ("time",), fill_value=False)
    var.cell_methods = "time: point"
    var[:] = [23, 24, 25]

    merge = MergeInstrument()
    merge.overlay(under, not_before_ms=1696982400000, not_after_ms=1697068800000)
    merge.overlay(over, not_before_ms=1696982400000, not_after_ms=1697068800000)
    output = merge.execute(tmp_path / "output.nc")
    merge = None
    under.close()
    under = None
    over.close()
    over = None

    assert output.getncattr("forge_tags") == "tag1 tag2"
    assert output.getncattr("instrument") == "underinst"
    assert output.getncattr("instrument_history") == "2023-10-11T12:00:00Z,overinst"
    assert output.getncattr("attr1") == "over1"
    assert output.getncattr("attr2") == "under2"
    assert output.getncattr("attr3") == "over2"

    assert len(output.groups) == 2
    group = output.groups["data"]
    assert group.getncattr("attr1") == "over3"
    assert group.getncattr("attr2") == "under4"
    assert group.getncattr("attr3") == "over4"

    assert len(group.variables) == 3
    var = group.variables["time"]
    assert var.dtype == np.int64
    assert var.getncattr("attr2") == "value4"
    assert list(var[:]) == [1696982400000, 1697004000000, 1697025600000, 1697047200000]

    var = group.variables["value"]
    assert var.dtype == np.float64
    assert list(var[:]) == [21.0, 22.0, 12.0, 13.0]

    var = group.variables["auxiliary"]
    assert var.dtype == np.float64
    assert list(var[:2]) == [31.0, 32.0]
    assert var[2].mask or isnan(var[2])
    assert var[3].mask or isnan(var[3])

    group = output.groups["state"]
    assert len(group.dimensions) == 1
    dim = group.dimensions["time"]
    assert dim.isunlimited()

    assert len(group.variables) == 2
    var = group.variables["time"]
    assert var.dtype == np.int64
    assert list(var[:]) == [1696960800000, 1696982400001, 1697004000000, 1697025600000]

    var = group.variables["value"]
    assert var.dtype == np.int64
    assert list(var[:]) == [23, 24, 25, 17]


def test_end_overlay(tmp_path):
    under = Dataset(str(tmp_path / "under.nc"), 'w', format='NETCDF4')
    under.setncattr("forge_tags", "tag1 tag2")
    under.setncattr("instrument", "ui1")
    under.setncattr("instrument_history", "2023-10-11T06:00:00Z,ui2")
    under.setncattr("attr1", "under1")
    under.setncattr("attr2", "under2")
    under.setncattr("time_coverage_start", "2023-10-11T00:00:00Z")
    under.setncattr("time_coverage_end", "2023-10-12T00:00:00Z")
    group = under.createGroup("data")
    group.setncattr("attr1", "under3")
    group.setncattr("attr2", "under4")
    var = time_coordinate(group)
    var.setncattr("attr2", "value4")
    var[:] = [1696982400000, 1697004000000, 1697025600000, 1697047200000]
    var = group.createVariable("value", "f8", ("time",), fill_value=nan)
    var[:] = [10.0, 11.0, 12.0, 13.0]
    group = under.createGroup("state")
    var = state_change_coordinate(group)
    var[:] = [1696960800000, 1696982400001, 1697004000000, 1697025600000]
    var = group.createVariable("value", "i8", ("time",), fill_value=False)
    var.cell_methods = "time: point"
    var[:] = [14, 15, 16, 17]

    over = Dataset(str(tmp_path / "over.nc"), 'w', format='NETCDF4')
    over.setncattr("instrument", "oi1")
    over.setncattr("forge_tags", "tag2")
    over.setncattr("attr1", "over1")
    over.setncattr("attr3", "over2")
    over.setncattr("time_coverage_start", "2023-10-11T12:00:00Z")
    over.setncattr("time_coverage_end", "2023-10-12T06:00:00Z")
    group = over.createGroup("data")
    group.setncattr("attr1", "over3")
    group.setncattr("attr3", "over4")
    var = time_coordinate(group)
    var.setncattr("attr2", "value4")
    var[:] = [1697025600000, 1697047200000, 1697068800000, 1697090400000]
    var = group.createVariable("value", "f8", ("time",), fill_value=nan)
    var[:] = [20.0, 21.0, 22.0, 23.0]
    var = group.createVariable("auxiliary", "f8", ("time",), fill_value=nan)
    var[:] = [30.0, 31.0, 32.0, 33.0]
    group = over.createGroup("state")
    var = state_change_coordinate(group)
    var[:] = [1697025600000, 1697047200000]
    var = group.createVariable("value", "i8", ("time",), fill_value=False)
    var.cell_methods = "time: point"
    var[:] = [23, 24]

    merge = MergeInstrument()
    merge.overlay(under, not_before_ms=1696982400000, not_after_ms=1697068800000)
    merge.overlay(over, not_before_ms=1696982400000, not_after_ms=1697068800000)
    output = merge.execute(tmp_path / "output.nc")
    merge = None
    under.close()
    under = None
    over.close()
    over = None

    assert output.getncattr("forge_tags") == "tag1 tag2"
    assert output.getncattr("instrument") == "oi1"
    assert output.getncattr("instrument_history") == "2023-10-11T06:00:00Z,ui2\n2023-10-11T12:00:00Z,ui1"
    assert output.getncattr("attr1") == "over1"
    assert output.getncattr("attr2") == "under2"
    assert output.getncattr("attr3") == "over2"

    assert len(output.groups) == 2
    group = output.groups["data"]
    assert group.getncattr("attr1") == "over3"
    assert group.getncattr("attr2") == "under4"
    assert group.getncattr("attr3") == "over4"

    assert len(group.variables) == 3
    var = group.variables["time"]
    assert var.dtype == np.int64
    assert var.getncattr("attr2") == "value4"
    assert list(var[:]) == [1696982400000, 1697004000000, 1697025600000, 1697047200000]

    var = group.variables["value"]
    assert var.dtype == np.float64
    assert list(var[:]) == [10.0, 11.0, 20.0, 21.0]

    var = group.variables["auxiliary"]
    assert var.dtype == np.float64
    assert var[0].mask or isnan(var[0])
    assert var[1].mask or isnan(var[1])
    assert list(var[2:]) == [30.0, 31.0]

    group = output.groups["state"]
    assert len(group.dimensions) == 1
    dim = group.dimensions["time"]
    assert dim.isunlimited()

    assert len(group.variables) == 2
    var = group.variables["time"]
    assert var.dtype == np.int64
    assert list(var[:]) == [1696960800000, 1696982400001, 1697004000000, 1697025600000, 1697047200000]

    var = group.variables["value"]
    assert var.dtype == np.int64
    assert list(var[:]) == [14, 15, 16, 23, 24]


def test_middle_overlay(tmp_path):
    under = Dataset(str(tmp_path / "under.nc"), 'w', format='NETCDF4')
    under.setncattr("attr1", "under1")
    under.setncattr("attr2", "under2")
    under.setncattr("time_coverage_start", "2023-10-11T00:00:00Z")
    under.setncattr("time_coverage_end", "2023-10-12T00:00:00Z")
    group = under.createGroup("data")
    group.setncattr("attr1", "under3")
    group.setncattr("attr2", "under4")
    var = time_coordinate(group)
    var.setncattr("attr2", "value4")
    var[:] = [1696982400000, 1697004000000, 1697025600000, 1697047200000]
    var = group.createVariable("value", "f8", ("time",), fill_value=nan)
    var[:] = [10.0, 11.0, 12.0, 13.0]
    group = under.createGroup("state")
    var = state_change_coordinate(group)
    var[:] = [1696960800000, 1696982400001, 1697004000000, 1697047200000]
    var = group.createVariable("value", "i8", ("time",), fill_value=False)
    var.cell_methods = "time: point"
    var[:] = [14, 15, 16, 17]

    over = Dataset(str(tmp_path / "over.nc"), 'w', format='NETCDF4')
    over.setncattr("attr1", "over1")
    over.setncattr("attr3", "over2")
    over.setncattr("time_coverage_start", "2023-10-11T06:00:00Z")
    over.setncattr("time_coverage_end", "2023-10-11T18:00:00Z")
    group = over.createGroup("data")
    group.setncattr("attr1", "over3")
    group.setncattr("attr3", "over4")
    var = time_coordinate(group)
    var.setncattr("attr2", "value4")
    var[:] = [1697004000000, 1697025600000]
    var = group.createVariable("value", "f8", ("time",), fill_value=nan)
    var[:] = [20.0, 21.0]
    var = group.createVariable("auxiliary", "f8", ("time",), fill_value=nan)
    var[:] = [30.0, 31.0]
    group = over.createGroup("state")
    var = state_change_coordinate(group)
    var[:] = [1696982400001, 1697004000000]
    var = group.createVariable("value", "i8", ("time",), fill_value=False)
    var.cell_methods = "time: point"
    var[:] = [23, 24]

    merge = MergeInstrument()
    merge.overlay(under, not_before_ms=1696982400000, not_after_ms=1697068800000)
    merge.overlay(over, not_before_ms=1696982400000, not_after_ms=1697068800000)
    output = merge.execute(tmp_path / "output.nc")
    merge = None
    under.close()
    under = None
    over.close()
    over = None

    assert output.getncattr("attr1") == "over1"
    assert output.getncattr("attr2") == "under2"
    assert output.getncattr("attr3") == "over2"

    assert len(output.groups) == 2
    group = output.groups["data"]
    assert group.getncattr("attr1") == "over3"
    assert group.getncattr("attr2") == "under4"
    assert group.getncattr("attr3") == "over4"

    assert len(group.variables) == 3
    var = group.variables["time"]
    assert var.dtype == np.int64
    assert var.getncattr("attr2") == "value4"
    assert list(var[:]) == [1696982400000, 1697004000000, 1697025600000, 1697047200000]

    var = group.variables["value"]
    assert var.dtype == np.float64
    assert list(var[:]) == [10.0, 20.0, 21.0, 13.0]

    var = group.variables["auxiliary"]
    assert var.dtype == np.float64
    assert var[0].mask or isnan(var[0])
    assert var[3].mask or isnan(var[3])
    assert list(var[1:3]) == [30.0, 31.0]

    group = output.groups["state"]
    assert len(group.dimensions) == 1
    dim = group.dimensions["time"]
    assert dim.isunlimited()

    assert len(group.variables) == 2
    var = group.variables["time"]
    assert var.dtype == np.int64
    assert list(var[:]) == [1696960800000, 1696982400001, 1697004000000, 1697047200000]

    var = group.variables["value"]
    assert var.dtype == np.int64
    assert list(var[:]) == [14, 15, 24, 17]


def test_replace_overlay(tmp_path):
    under = Dataset(str(tmp_path / "under.nc"), 'w', format='NETCDF4')
    under.setncattr("attr1", "under1")
    under.setncattr("time_coverage_start", "2023-10-11T00:00:00Z")
    under.setncattr("time_coverage_end", "2023-10-12T00:00:00Z")
    group = under.createGroup("data")
    group.setncattr("attr1", "under2")
    var = time_coordinate(group)
    var.setncattr("attr2", "under3")
    var[:] = [1696982400000, 1697004000000, 1697025600000, 1697047200000]
    var = group.createVariable("value", "f8", ("time",), fill_value=nan)
    var[:] = [10.0, 11.0, 12.0, 13.0]
    group = under.createGroup("state")
    var = state_change_coordinate(group)
    var[:] = [1696960800000, 1696982400001, 1697004000000, 1697025600000]
    var = group.createVariable("value", "i8", ("time",), fill_value=False)
    var.cell_methods = "time: point"
    var[:] = [14, 15, 16, 17]

    over = Dataset(str(tmp_path / "over.nc"), 'w', format='NETCDF4')
    over.setncattr("attr1", "over1")
    over.setncattr("time_coverage_start", "2023-10-11T00:00:00Z")
    over.setncattr("time_coverage_end", "2023-10-12T00:00:00Z")
    group = over.createGroup("data")
    group.setncattr("attr1", "over2")
    var = time_coordinate(group)
    var.setncattr("attr2", "over3")
    var[:] = [1696982400000, 1697004000002, 1697025600000, 1697047200000]
    var = group.createVariable("value", "f8", ("time",), fill_value=nan)
    var[:] = [20.0, 21.0, 22.0, 23.0]
    group = over.createGroup("state")
    var = state_change_coordinate(group)
    var[:] = [1696960800000, 1696982400003, 1697004000000, 1697025600000]
    var = group.createVariable("value", "i8", ("time",), fill_value=False)
    var.cell_methods = "time: point"
    var[:] = [24, 25, 26, 27]

    merge = MergeInstrument()
    merge.overlay(under, not_before_ms=1696982400000, not_after_ms=1697068800000)
    merge.overlay(over, not_before_ms=1696982400000, not_after_ms=1697068800000)
    output = merge.execute(tmp_path / "output.nc")
    merge = None
    under.close()
    under = None
    over.close()
    over = None

    assert output.getncattr("attr1") == "over1"

    assert len(output.groups) == 2
    group = output.groups["data"]
    assert group.getncattr("attr1") == "over2"

    assert len(group.variables) == 2
    var = group.variables["time"]
    assert var.dtype == np.int64
    assert var.getncattr("attr2") == "over3"
    assert list(var[:]) == [1696982400000, 1697004000002, 1697025600000, 1697047200000]

    var = group.variables["value"]
    assert var.dtype == np.float64
    assert list(var[:]) == [20.0, 21.0, 22.0, 23.0]

    group = output.groups["state"]
    assert len(group.dimensions) == 1
    dim = group.dimensions["time"]
    assert dim.isunlimited()

    assert len(group.variables) == 2
    var = group.variables["time"]
    assert var.dtype == np.int64
    assert list(var[:]) == [1696960800000, 1696982400003, 1697004000000, 1697025600000]

    var = group.variables["value"]
    assert var.dtype == np.int64
    assert list(var[:]) == [24, 25, 26, 27]


def test_replace_exact_interior(tmp_path):
    under = Dataset(str(tmp_path / "under.nc"), 'w', format='NETCDF4')
    under.setncattr("time_coverage_start", "2023-10-11T00:00:00Z")
    under.setncattr("time_coverage_end", "2023-10-12T00:00:00Z")
    group = under.createGroup("data")
    var = time_coordinate(group)
    var[:] = [1697004000000, 1697025600000, 1697047200000]
    var = group.createVariable("value", "f8", ("time",), fill_value=nan)
    var[:] = [10.0, 11.0, 12.0]
    group = under.createGroup("state")
    var = state_change_coordinate(group)
    var[:] = [1697004000000, 1697025600000, 1697047200000]
    var = group.createVariable("value", "i8", ("time",), fill_value=False)
    var.cell_methods = "time: point"
    var[:] = [14, 15, 16]

    over = Dataset(str(tmp_path / "over.nc"), 'w', format='NETCDF4')
    over.setncattr("time_coverage_start", "2023-10-11T06:00:00Z")
    over.setncattr("time_coverage_end", "2023-10-11T19:00:00Z")
    group = over.createGroup("data")
    var = time_coordinate(group)
    var.setncattr("attr2", "over3")
    var[:] = [1697004000000, 1697025600000, 1697047200000]
    var = group.createVariable("value", "f8", ("time",), fill_value=nan)
    var[:] = [20.0, 21.0, 22.0]
    group = over.createGroup("state")
    var = state_change_coordinate(group)
    var[:] = [1697004000000, 1697025600000, 1697047200000]
    var = group.createVariable("value", "i8", ("time",), fill_value=False)
    var.cell_methods = "time: point"
    var[:] = [24, 25, 16]

    merge = MergeInstrument()
    merge.overlay(under, not_before_ms=1696982400000, not_after_ms=1697068800000)
    merge.overlay(over, not_before_ms=1696982400000, not_after_ms=1697068800000)
    output = merge.execute(tmp_path / "output.nc")
    merge = None
    under.close()
    under = None
    over.close()
    over = None

    group = output.groups["data"]
    var = group.variables["time"]
    assert list(var[:]) == [1697004000000, 1697025600000, 1697047200000]
    var = group.variables["value"]
    assert list(var[:]) == [20.0, 21.0, 22.0]

    group = output.groups["state"]
    var = group.variables["time"]
    assert list(var[:]) == [1697004000000, 1697025600000, 1697047200000]
    var = group.variables["value"]
    assert list(var[:]) == [24, 25, 16]


def test_preserve_single_state(tmp_path):
    data = Dataset(str(tmp_path / "data.nc"), 'w', format='NETCDF4')
    data.setncattr("time_coverage_start", "2023-10-11T00:00:00Z")
    data.setncattr("time_coverage_end", "2023-10-12T00:00:00Z")

    group = data.createGroup("state")
    var = state_change_coordinate(group)
    var[:] = [1696896000000]

    var = group.createVariable("value", str, ("time",))
    var.cell_methods = "time: point"
    var[0] = "A"

    merge = MergeInstrument()
    merge.overlay(data, not_before_ms=1696982400000, not_after_ms=1697068800000)
    output = merge.execute(tmp_path / "output.nc")
    merge = None
    data.close()
    data = None

    group = output.groups["state"]
    assert len(group.dimensions) == 1
    dim = group.dimensions["time"]
    assert dim.isunlimited()
    assert dim.size == 1

    assert len(group.variables) == 2
    var = group.variables["time"]
    assert var.dtype == np.int64
    assert list(var[:]) == [1696896000000]

    var = group.variables["value"]
    assert var.dtype == str
    assert var.getncattr("cell_methods") == "time: point"
    assert list(var[:]) == ["A"]


def test_fragmented_state(tmp_path):
    under = Dataset(str(tmp_path / "under.nc"), 'w', format='NETCDF4')
    under.setncattr("time_coverage_start", "2023-10-11T00:00:00Z")
    under.setncattr("time_coverage_end", "2023-10-12T00:00:00Z")

    group = under.createGroup("state")
    var = state_change_coordinate(group)
    var[:] = [1696978800000, 1696982400001]

    var = group.createVariable("value", "f8", ("time",), fill_value=nan)
    var.cell_methods = "time: point"
    var[:] = [1.0, 2.0]

    over = Dataset(str(tmp_path / "over.nc"), 'w', format='NETCDF4')
    over.setncattr("time_coverage_start", "2023-10-11T01:00:00Z")
    over.setncattr("time_coverage_end", "2023-10-11T02:00:00Z")

    group = over.createGroup("state")
    var = state_change_coordinate(group)
    var[:] = [1696982400001, 1696989000000]

    var = group.createVariable("value", "f8", ("time",), fill_value=nan)
    var.cell_methods = "time: point"
    var[:] = [2.0, 3.0]

    merge = MergeInstrument()
    merge.overlay(under, not_before_ms=1696982400000, not_after_ms=1697068800000)
    merge.overlay(over, not_before_ms=1696982400000, not_after_ms=1697068800000)
    output = merge.execute(tmp_path / "output.nc")
    merge = None
    under.close()
    under = None
    over.close()
    over = None

    group = output.groups["state"]
    assert len(group.dimensions) == 1
    dim = group.dimensions["time"]
    assert dim.isunlimited()
    #assert dim.size == 3

    assert len(group.variables) == 2
    var = group.variables["time"]
    assert var.dtype == np.int64
    assert list(var[:]) == [1696978800000, 1696982400001, 1696989000000]

    var = group.variables["value"]
    assert var.dtype == np.float64
    assert var.getncattr("cell_methods") == "time: point"
    assert list(var[:]) == [1.0, 2.0, 3.0]


def test_flags(tmp_path):
    under = Dataset(str(tmp_path / "under.nc"), 'w', format='NETCDF4')
    under.setncattr("time_coverage_start", "2023-10-11T00:00:00Z")
    under.setncattr("time_coverage_end", "2023-10-12T00:00:00Z")
    group = under.createGroup("data")
    var = time_coordinate(group)
    var[:] = [1696982400000, 1697004000000, 1697025600000, 1697047200000]
    var = group.createVariable("system_flags", "u8", ("time",), fill_value=False)
    variable_flags(var, {
        0x01: "under0",
        0x02: "under1",
        0x04: "under2",
    })
    var[:] = [0x01, 0x03, 0x00, 0x04]

    over = Dataset(str(tmp_path / "over.nc"), 'w', format='NETCDF4')
    over.setncattr("time_coverage_start", "2023-10-11T00:00:00Z")
    over.setncattr("time_coverage_end", "2023-10-11T12:00:00Z")
    group = over.createGroup("data")
    var = time_coordinate(group)
    var[:] = [1696982400000, 1697004000000]
    var = group.createVariable("system_flags", "u8", ("time",), fill_value=False)
    variable_flags(var, {
        0x01: "under0",
        0x04: "over1",
    })
    var[:] = [0x01, 0x05]

    merge = MergeInstrument()
    merge.overlay(under, not_before_ms=1696982400000, not_after_ms=1697068800000)
    merge.overlay(over, not_before_ms=1696982400000, not_after_ms=1697068800000)
    output = merge.execute(tmp_path / "output.nc")
    merge = None
    under.close()
    under = None
    over.close()
    over = None

    var = output.groups["data"].variables["system_flags"]
    assert var.getncattr("flag_meanings") == "under0 under1 over1 under2"
    assert list(var.getncattr("flag_masks")) == [0x01, 0x02, 0x04, 0x08]
    assert var.dtype == np.uint64
    assert list(var[:]) == [0x01, 0x05, 0x00, 0x08]


def test_enum(tmp_path):
    under = Dataset(str(tmp_path / "under.nc"), 'w', format='NETCDF4')
    under.setncattr("time_coverage_start", "2023-10-11T00:00:00Z")
    under.setncattr("time_coverage_end", "2023-10-12T00:00:00Z")
    group = under.createGroup("data")
    var = time_coordinate(group)
    var[:] = [1696982400000, 1697004000000, 1697025600000, 1697047200000]
    en = group.createEnumType(np.uint8, "value_t", {
        'under0': 0,
        'under1': 1,
        'under2': 2,
    })
    var = group.createVariable("value", en, ("time",), fill_value=False)
    var[:] = [0, 1, 2, 0]

    over = Dataset(str(tmp_path / "over.nc"), 'w', format='NETCDF4')
    over.setncattr("time_coverage_start", "2023-10-11T00:00:00Z")
    over.setncattr("time_coverage_end", "2023-10-11T12:00:00Z")
    group = over.createGroup("data")
    var = time_coordinate(group)
    var[:] = [1696982400000, 1697004000000]
    en = group.createEnumType(np.uint8, "value_t", {
        'over0': 0,
        'over3': 3,
    })
    var = group.createVariable("value", en, ("time",), fill_value=False)
    var[:] = [0, 3]

    merge = MergeInstrument()
    merge.overlay(under, not_before_ms=1696982400000, not_after_ms=1697068800000)
    merge.overlay(over, not_before_ms=1696982400000, not_after_ms=1697068800000)
    output = merge.execute(tmp_path / "output.nc")
    merge = None
    under.close()
    under = None
    over.close()
    over = None

    en = output.groups["data"].enumtypes["value_t"]
    assert en.enum_dict == {
        'under0': 4,
        'under1': 1,
        'under2': 2,
        'over0': 0,
        'over3': 3,
    }

    var = output.groups["data"].variables["value"]
    assert var.dtype == en
    assert list(var[:]) == [0, 3, 2, 4]


def test_simple_dimension(tmp_path):
    under = Dataset(str(tmp_path / "under.nc"), 'w', format='NETCDF4')
    under.setncattr("time_coverage_start", "2023-10-11T00:00:00Z")
    under.setncattr("time_coverage_end", "2023-10-12T00:00:00Z")
    group = under.createGroup("data")
    var = time_coordinate(group)
    var[:] = [1696982400000, 1697004000000, 1697025600000, 1697047200000]
    group.createDimension("dim1", 2)
    var = group.createVariable("dim1", "f8", ("dim1",), fill_value=nan)
    var[:] = [100.0, 101.0]
    var = group.createVariable("var1", "f8", ("time", "dim1"), fill_value=nan)
    var[:] = [
        [10.0, 11.0],
        [12.0, 13.0],
        [14.0, 15.0],
        [16.0, 17.0],
    ]

    over = Dataset(str(tmp_path / "over.nc"), 'w', format='NETCDF4')
    over.setncattr("time_coverage_start", "2023-10-11T00:00:00Z")
    over.setncattr("time_coverage_end", "2023-10-11T12:00:00Z")
    group = over.createGroup("data")
    var = time_coordinate(group)
    var[:] = [1696982400000, 1697004000000]
    group.createDimension("dim1", 2)
    var = group.createVariable("dim1", "f8", ("dim1",), fill_value=nan)
    var[:] = [200.0, 201.0]
    var = group.createVariable("var1", "f8", ("time", "dim1"), fill_value=nan)
    var[:] = [
        [20.0, 21.0],
        [22.0, 23.0],
    ]

    merge = MergeInstrument()
    merge.overlay(under, not_before_ms=1696982400000, not_after_ms=1697068800000)
    merge.overlay(over, not_before_ms=1696982400000, not_after_ms=1697068800000)
    output = merge.execute(tmp_path / "output.nc")
    merge = None
    under.close()
    under = None
    over.close()
    over = None

    assert output.groups["data"].dimensions["dim1"].size == 2
    var = output.groups["data"].variables["dim1"]
    assert list(var[:]) == [200.0, 201.0]

    var = output.groups["data"].variables["var1"]
    assert var.shape == (4, 2)
    assert list(var[0]) == [20.0, 21.0]
    assert list(var[1]) == [22.0, 23.0]
    assert list(var[2]) == [14.0, 15.0]
    assert list(var[3]) == [16.0, 17.0]


def test_dimension_reshape(tmp_path):
    under = Dataset(str(tmp_path / "under.nc"), 'w', format='NETCDF4')
    under.setncattr("time_coverage_start", "2023-10-11T00:00:00Z")
    under.setncattr("time_coverage_end", "2023-10-12T00:00:00Z")
    group = under.createGroup("data")
    var = time_coordinate(group)
    var[:] = [1696982400000, 1697004000000, 1697025600000, 1697047200000]
    group.createDimension("dim1", 2)
    var = group.createVariable("dim1", "f8", ("dim1",), fill_value=nan)
    var[:] = [100.0, 101.0]
    var = group.createVariable("var1", "f8", ("time", "dim1"), fill_value=nan)
    var[:] = [
        [10.0, 11.0],
        [12.0, 13.0],
        [14.0, 15.0],
        [16.0, 17.0],
    ]
    group.createDimension("dim2", 3)
    var = group.createVariable("dim2", "f8", ("dim2",), fill_value=nan)
    var[:] = [102.0, 103.0, 104.0]
    var = group.createVariable("var2", "f8", ("time", "dim2"), fill_value=nan)
    var[:] = [
        [10.0, 11.0, 12.0],
        [10.25, 11.25, 12.25],
        [10.5, 11.5, 12.5],
        [10.75, 11.75, 12.75],
    ]
    group.createDimension("dim3", 2)
    group.createDimension("dim4", 2)
    var = group.createVariable("var3", "f8", ("dim3", "dim4"), fill_value=nan)
    var[:] = [
        [1.0, 2.0],
        [3.0, 4.0],
    ]

    over = Dataset(str(tmp_path / "over.nc"), 'w', format='NETCDF4')
    over.setncattr("time_coverage_start", "2023-10-11T00:00:00Z")
    over.setncattr("time_coverage_end", "2023-10-11T12:00:00Z")
    group = over.createGroup("data")
    var = time_coordinate(group)
    var[:] = [1696982400000, 1697004000000]
    group.createDimension("dim1", 3)
    var = group.createVariable("dim1", "f8", ("dim1",), fill_value=nan)
    var[:] = [200.0, 201.0, 202.0]
    var = group.createVariable("var1", "f8", ("time", "dim1"), fill_value=nan)
    var[:] = [
        [20.0, 21.0, 22.0],
        [23.0, 24.0, 25.0],
    ]
    group.createDimension("dim2", 2)
    var = group.createVariable("dim2", "f8", ("dim2",), fill_value=nan)
    var[:] = [202.0, 203.0]
    var = group.createVariable("var2", "f8", ("time", "dim2"), fill_value=nan)
    var[:] = [
        [20.0, 21.0],
        [20.25, 21.25],
    ]
    group.createDimension("dim3", 3)
    group.createDimension("dim4", 4)
    var = group.createVariable("var3", "f8", ("dim3", "dim4"), fill_value=nan)
    var[:] = [
        [10.0, 11.0, 12.0, 13.0],
        [20.0, 21.0, 22.0, 23.0],
        [30.0, 31.0, 32.0, 33.0],
    ]

    merge = MergeInstrument()
    merge.overlay(under, not_before_ms=1696982400000, not_after_ms=1697068800000)
    merge.overlay(over, not_before_ms=1696982400000, not_after_ms=1697068800000)
    output = merge.execute(tmp_path / "output.nc")
    merge = None
    under.close()
    under = None
    over.close()
    over = None

    assert output.groups["data"].dimensions["dim1"].size == 3
    var = output.groups["data"].variables["dim1"]
    assert list(var[:]) == [200.0, 201.0, 202.0]

    var = output.groups["data"].variables["var1"]
    assert var.shape == (4, 3)
    assert list(var[0]) == [20.0, 21.0, 22.0]
    assert list(var[1]) == [23.0, 24.0, 25.0]
    assert list(var[2, :2]) == [14.0, 15.0]
    assert var[2, 2].mask or isnan(var[2, 2])
    assert list(var[3, :2]) == [16.0, 17.0]
    assert var[3, 2].mask or isnan(var[3, 2])

    assert output.groups["data"].dimensions["dim2"].size == 3
    var = output.groups["data"].variables["dim2"]
    assert list(var[:]) == [202.0, 203.0, 104.0]

    var = output.groups["data"].variables["var2"]
    assert var.shape == (4, 3)
    assert list(var[0, :2]) == [20.0, 21.0]
    assert var[0, 2].mask or isnan(var[0, 2])
    assert list(var[1, :2]) == [20.25, 21.25]
    assert var[1, 2].mask or isnan(var[1, 2])
    assert list(var[2]) == [10.5, 11.5, 12.5]
    assert list(var[3]) == [10.75, 11.75, 12.75]

    assert output.groups["data"].dimensions["dim3"].size == 3
    assert output.groups["data"].dimensions["dim4"].size == 4
    var = output.groups["data"].variables["var3"]
    assert var.shape == (3, 4)
    assert list(var[0]) == [10.0, 11.0, 12.0, 13.0]
    assert list(var[1]) == [20.0, 21.0, 22.0, 23.0]
    assert list(var[2]) == [30.0, 31.0, 32.0, 33.0]


def test_constant_change(tmp_path):
    under = Dataset(str(tmp_path / "under.nc"), 'w', format='NETCDF4')
    under.setncattr("time_coverage_start", "2023-10-11T00:00:00Z")
    under.setncattr("time_coverage_end", "2023-10-12T00:00:00Z")
    var = under.createVariable("top1", "f8", (), fill_value=False)
    var[0] = -1.0
    group = under.createGroup("instrument")
    var = group.createVariable("var1", str, (), fill_value=False)
    var.coverage_content_type = "referenceInformation"
    var[0] = "under1"
    var = group.createVariable("var2", "f8", (), fill_value=False)
    var.coverage_content_type = "referenceInformation"
    var.change_history = "2023-10-11T06:00:00Z,2.0"
    var.C_format = "%.1f"
    var[0] = 1.0

    over = Dataset(str(tmp_path / "over.nc"), 'w', format='NETCDF4')
    over.setncattr("time_coverage_start", "2023-10-11T12:00:00Z")
    over.setncattr("time_coverage_end", "2023-10-12T00:00:00Z")
    var = over.createVariable("top1", "f8", (), fill_value=False)
    var[0] = -10.0
    group = over.createGroup("instrument")
    var = group.createVariable("var1", str, (), fill_value=False)
    var.coverage_content_type = "referenceInformation"
    var[0] = "over1"
    var = group.createVariable("var2", "f8", (), fill_value=False)
    var.coverage_content_type = "referenceInformation"
    var.C_format = "%.1f"
    var[0] = 10.0

    merge = MergeInstrument()
    merge.overlay(under, not_before_ms=1696982400000, not_after_ms=1697068800000)
    merge.overlay(over, not_before_ms=1696982400000, not_after_ms=1697068800000)
    output = merge.execute(tmp_path / "output.nc")
    merge = None
    under.close()
    under = None
    over.close()
    over = None

    var = output.variables["top1"]
    assert len(var.dimensions) == 0
    assert var[0] == -10.0
    assert getattr(var, "change_history", None) is None

    group = output.groups["instrument"]

    var = group.variables["var1"]
    assert len(var.dimensions) == 0
    assert var[0] == "over1"
    assert var.change_history == "2023-10-11T12:00:00Z,under1"

    var = group.variables["var2"]
    assert len(var.dimensions) == 0
    assert var[0] == 10.0
    assert var.change_history == "2023-10-11T06:00:00Z,2.0\n2023-10-11T12:00:00Z,1.0"


def test_wavelength_basic(tmp_path):
    under = Dataset(str(tmp_path / "under.nc"), 'w', format='NETCDF4')
    under.setncattr("time_coverage_start", "2023-10-11T00:00:00Z")
    under.setncattr("time_coverage_end", "2023-10-12T00:00:00Z")
    group = under.createGroup("data")
    var = time_coordinate(group)
    var[:] = [1696982400000, 1697004000000, 1697025600000, 1697047200000]
    group.createDimension("wavelength", 3)
    var = group.createVariable("wavelength", "f8", ("wavelength",), fill_value=nan)
    var[:] = [450.0, 550.0, 700.0]
    var = group.createVariable("var1", "f8", ("time", "wavelength"), fill_value=nan)
    var[:] = [
        [10.00, 11.00, 12.00],
        [10.25, 11.25, 12.25],
        [10.50, 11.50, 12.50],
        [10.75, 11.75, 12.75],
    ]

    over = Dataset(str(tmp_path / "over.nc"), 'w', format='NETCDF4')
    over.setncattr("time_coverage_start", "2023-10-11T00:00:00Z")
    over.setncattr("time_coverage_end", "2023-10-11T12:00:00Z")
    group = over.createGroup("data")
    var = time_coordinate(group)
    var[:] = [1696982400000, 1697004000000]
    group.createDimension("wavelength", 3)
    var = group.createVariable("wavelength", "f8", ("wavelength",), fill_value=nan)
    var[:] = [450.0, 550.0, 700.0]
    var = group.createVariable("var1", "f8", ("time", "wavelength"), fill_value=nan)
    var[:] = [
        [20.00, 21.00, 22.00],
        [20.25, 21.25, 22.25],
    ]

    merge = MergeInstrument()
    merge.overlay(under, not_before_ms=1696982400000, not_after_ms=1697068800000)
    merge.overlay(over, not_before_ms=1696982400000, not_after_ms=1697068800000)
    output = merge.execute(tmp_path / "output.nc")
    merge = None
    under.close()
    under = None
    over.close()
    over = None

    assert output.groups["data"].dimensions["wavelength"].size == 3
    var = output.groups["data"].variables["wavelength"]
    assert list(var[:]) == [450.0, 550.0, 700.0]

    var = output.groups["data"].variables["var1"]
    assert var.shape == (4, 3)
    assert list(var[0]) == [20.00, 21.00, 22.00]
    assert list(var[1]) == [20.25, 21.25, 22.25]
    assert list(var[2]) == [10.50, 11.50, 12.50]
    assert list(var[3]) == [10.75, 11.75, 12.75]


def test_wavelength_change(tmp_path):
    under = Dataset(str(tmp_path / "under.nc"), 'w', format='NETCDF4')
    under.setncattr("time_coverage_start", "2023-10-11T00:00:00Z")
    under.setncattr("time_coverage_end", "2023-10-12T00:00:00Z")
    group = under.createGroup("data")
    var = time_coordinate(group)
    var[:] = [1696982400000, 1697004000000, 1697025600000, 1697047200000]
    group.createDimension("wavelength", 3)
    var = group.createVariable("wavelength", "f8", ("wavelength",), fill_value=nan)
    var.C_format = "%.0f"
    var[:] = [450.0, 550.0, 700.0]
    var = group.createVariable("var1", "f8", ("time", "wavelength"), fill_value=nan)
    var[:] = [
        [10.00, 11.00, 12.00],
        [10.25, 11.25, 12.25],
        [10.50, 11.50, 12.50],
        [10.75, 11.75, 12.75],
    ]

    over = Dataset(str(tmp_path / "over.nc"), 'w', format='NETCDF4')
    over.setncattr("time_coverage_start", "2023-10-11T00:00:00Z")
    over.setncattr("time_coverage_end", "2023-10-11T12:00:00Z")
    group = over.createGroup("data")
    var = time_coordinate(group)
    var[:] = [1696982400000, 1697004000000]
    group.createDimension("wavelength", 3)
    var = group.createVariable("wavelength", "f8", ("wavelength",), fill_value=nan)
    var.C_format = "%.0f"
    var[:] = [425.0, 535.0, 670.0]
    var = group.createVariable("var1", "f8", ("time", "wavelength"), fill_value=nan)
    var[:] = [
        [20.00, 21.00, 22.00],
        [20.25, 21.25, 22.25],
    ]

    merge = MergeInstrument()
    merge.overlay(under, not_before_ms=1696982400000, not_after_ms=1697068800000)
    merge.overlay(over, not_before_ms=1696982400000, not_after_ms=1697068800000)
    output = merge.execute(tmp_path / "output.nc")
    merge = None
    under.close()
    under = None
    over.close()
    over = None

    assert output.groups["data"].dimensions["wavelength"].size == 3
    var = output.groups["data"].variables["wavelength"]
    assert list(var[:]) == [450.0, 550.0, 700.0]
    assert var.change_history == "2023-10-11T12:00:00Z,425,535,670"

    var = output.groups["data"].variables["var1"]
    assert var.shape == (4, 3)
    assert list(var[0]) == [20.00, 21.00, 22.00]
    assert list(var[1]) == [20.25, 21.25, 22.25]
    assert list(var[2]) == [10.50, 11.50, 12.50]
    assert list(var[3]) == [10.75, 11.75, 12.75]


def test_wavelength_single_move(tmp_path):
    under = Dataset(str(tmp_path / "under.nc"), 'w', format='NETCDF4')
    under.setncattr("time_coverage_start", "2023-10-11T00:00:00Z")
    under.setncattr("time_coverage_end", "2023-10-12T00:00:00Z")
    group = under.createGroup("data")
    var = time_coordinate(group)
    var[:] = [1696982400000, 1697004000000, 1697025600000, 1697047200000]
    group.createDimension("wavelength", 3)
    var = group.createVariable("wavelength", "f8", ("wavelength",), fill_value=nan)
    var.C_format = "%.0f"
    var[:] = [450.0, 550.0, 700.0]
    var = group.createVariable("var1", "f8", ("time", "wavelength"), fill_value=nan)
    var[:] = [
        [10.00, 11.00, 12.00],
        [10.25, 11.25, 12.25],
        [10.50, 11.50, 12.50],
        [10.75, 11.75, 12.75],
    ]

    over = Dataset(str(tmp_path / "over.nc"), 'w', format='NETCDF4')
    over.setncattr("time_coverage_start", "2023-10-11T00:00:00Z")
    over.setncattr("time_coverage_end", "2023-10-11T12:00:00Z")
    group = over.createGroup("data")
    var = time_coordinate(group)
    var[:] = [1696982400000, 1697004000000]
    group.createDimension("wavelength", 1)
    var = group.createVariable("wavelength", "f8", ("wavelength",), fill_value=nan)
    var.C_format = "%.0f"
    var[:] = [535.0]
    var = group.createVariable("var1", "f8", ("time", "wavelength"), fill_value=nan)
    var[:] = [
        [20.0],
        [21.0],
    ]

    merge = MergeInstrument()
    merge.overlay(under, not_before_ms=1696982400000, not_after_ms=1697068800000)
    merge.overlay(over, not_before_ms=1696982400000, not_after_ms=1697068800000)
    output = merge.execute(tmp_path / "output.nc")
    merge = None
    under.close()
    under = None
    over.close()
    over = None

    assert output.groups["data"].dimensions["wavelength"].size == 3
    var = output.groups["data"].variables["wavelength"]
    assert list(var[:]) == [450.0, 550.0, 700.0]
    assert var.change_history == "2023-10-11T12:00:00Z,,535,"

    var = output.groups["data"].variables["var1"]
    assert var.shape == (4, 3)
    assert var[0, 0].mask or isnan(var[0, 0])
    assert var[0, 1] == 20.0
    assert var[0, 2].mask or isnan(var[0, 2])
    assert var[1, 0].mask or isnan(var[1, 0])
    assert var[1, 1] == 21.0
    assert var[1, 2].mask or isnan(var[1, 2])
    assert list(var[2]) == [10.50, 11.50, 12.50]
    assert list(var[3]) == [10.75, 11.75, 12.75]


def test_constant_cutsize(tmp_path):
    under = Dataset(str(tmp_path / "under.nc"), 'w', format='NETCDF4')
    under.setncattr("time_coverage_start", "2023-10-11T00:00:00Z")
    under.setncattr("time_coverage_end", "2023-10-12T00:00:00Z")
    group = under.createGroup("data")
    var = time_coordinate(group)
    var[:] = [1696982400000, 1697004000000, 1697025600000, 1697047200000]
    var = group.createVariable("cut_size", "f8", (), fill_value=nan)
    var[:] = 1.0
    var = group.createVariable("var1", "f8", ("time",), fill_value=nan)
    var.ancillary_variables = "cut_size"
    var[:] = [10.0, 11.0, 12.0, 13.0]

    over = Dataset(str(tmp_path / "over.nc"), 'w', format='NETCDF4')
    over.setncattr("time_coverage_start", "2023-10-11T00:00:00Z")
    over.setncattr("time_coverage_end", "2023-10-11T12:00:00Z")
    group = over.createGroup("data")
    var = time_coordinate(group)
    var[:] = [1696982400000, 1697004000000]
    var = group.createVariable("cut_size", "f8", (), fill_value=nan)
    var[:] = 1.0
    var = group.createVariable("var1", "f8", ("time",), fill_value=nan)
    var.ancillary_variables = "cut_size"
    var[:] = [20.0, 21.0]

    merge = MergeInstrument()
    merge.overlay(under, not_before_ms=1696982400000, not_after_ms=1697068800000)
    merge.overlay(over, not_before_ms=1696982400000, not_after_ms=1697068800000)
    output = merge.execute(tmp_path / "output.nc")
    merge = None
    under.close()
    under = None
    over.close()
    over = None

    var = output.groups["data"].variables["cut_size"]
    assert float(var[:]) == 1.0

    var = output.groups["data"].variables["var1"]
    assert var.ancillary_variables == "cut_size"
    assert var.shape == (4, )
    assert list(var[:]) == [20.0, 21.0, 12.0, 13.0]


def test_promote_cutsize(tmp_path):
    under = Dataset(str(tmp_path / "under.nc"), 'w', format='NETCDF4')
    under.setncattr("time_coverage_start", "2023-10-11T00:00:00Z")
    under.setncattr("time_coverage_end", "2023-10-12T00:00:00Z")
    group = under.createGroup("data")
    var = time_coordinate(group)
    var[:] = [1696982400000, 1697004000000, 1697025600000, 1697047200000]
    var = group.createVariable("cut_size", "f8", (), fill_value=nan)
    var[:] = 1.0
    var = group.createVariable("var1", "f8", ("time",), fill_value=nan)
    var.ancillary_variables = "cut_size"
    var[:] = [10.0, 11.0, 12.0, 13.0]

    over = Dataset(str(tmp_path / "over.nc"), 'w', format='NETCDF4')
    over.setncattr("time_coverage_start", "2023-10-11T00:00:00Z")
    over.setncattr("time_coverage_end", "2023-10-11T12:00:00Z")
    group = over.createGroup("data")
    var = time_coordinate(group)
    var[:] = [1696982400000, 1697004000000]
    var = group.createVariable("cut_size", "f8", (), fill_value=nan)
    var[:] = 10.0
    var = group.createVariable("var1", "f8", ("time",), fill_value=nan)
    var.ancillary_variables = "cut_size"
    var[:] = [20.0, 21.0]

    merge = MergeInstrument()
    merge.overlay(under, not_before_ms=1696982400000, not_after_ms=1697068800000)
    merge.overlay(over, not_before_ms=1696982400000, not_after_ms=1697068800000)
    output = merge.execute(tmp_path / "output.nc")
    merge = None
    under.close()
    under = None
    over.close()
    over = None

    var = output.groups["data"].variables["cut_size"]
    assert var.shape == (4,)
    assert list(var[:]) == [10.0, 10.0, 1.0, 1.0]

    var = output.groups["data"].variables["var1"]
    assert var.ancillary_variables == "cut_size"
    assert var.shape == (4,)
    assert list(var[:]) == [20.0, 21.0, 12.0, 13.0]


def test_dynamic_cutsize(tmp_path):
    under = Dataset(str(tmp_path / "under.nc"), 'w', format='NETCDF4')
    under.setncattr("time_coverage_start", "2023-10-11T00:00:00Z")
    under.setncattr("time_coverage_end", "2023-10-12T00:00:00Z")
    group = under.createGroup("data")
    var = time_coordinate(group)
    var[:] = [1696982400000, 1697004000000, 1697025600000, 1697047200000]
    var = group.createVariable("cut_size", "f8", ("time", ), fill_value=nan)
    var[:] = [1.0, 2.0, 3.0, 4.0]
    var = group.createVariable("var1", "f8", ("time",), fill_value=nan)
    var.ancillary_variables = "cut_size"
    var[:] = [10.0, 11.0, 12.0, 13.0]

    over = Dataset(str(tmp_path / "over.nc"), 'w', format='NETCDF4')
    over.setncattr("time_coverage_start", "2023-10-11T00:00:00Z")
    over.setncattr("time_coverage_end", "2023-10-11T12:00:00Z")
    group = over.createGroup("data")
    var = time_coordinate(group)
    var[:] = [1696982400000, 1697004000000]
    var = group.createVariable("cut_size", "f8", ("time",), fill_value=nan)
    var[:] = [15.0, 16.0]
    var = group.createVariable("var1", "f8", ("time",), fill_value=nan)
    var.ancillary_variables = "cut_size"
    var[:] = [20.0, 21.0]

    merge = MergeInstrument()
    merge.overlay(under, not_before_ms=1696982400000, not_after_ms=1697068800000)
    merge.overlay(over, not_before_ms=1696982400000, not_after_ms=1697068800000)
    output = merge.execute(tmp_path / "output.nc")
    merge = None
    under.close()
    under = None
    over.close()
    over = None

    var = output.groups["data"].variables["cut_size"]
    assert var.shape == (4,)
    assert list(var[:]) == [15.0, 16.0, 3.0, 4.0]

    var = output.groups["data"].variables["var1"]
    assert var.ancillary_variables == "cut_size"
    assert var.shape == (4,)
    assert list(var[:]) == [20.0, 21.0, 12.0, 13.0]


def test_dynamic_promote_cutsize(tmp_path):
    under = Dataset(str(tmp_path / "under.nc"), 'w', format='NETCDF4')
    under.setncattr("time_coverage_start", "2023-10-11T00:00:00Z")
    under.setncattr("time_coverage_end", "2023-10-12T00:00:00Z")
    group = under.createGroup("data")
    var = time_coordinate(group)
    var[:] = [1696982400000, 1697004000000, 1697025600000, 1697047200000]
    var = group.createVariable("cut_size", "f8", ("time",), fill_value=nan)
    var[:] = [1.0, 2.0, 3.0, 4.0]
    var = group.createVariable("var1", "f8", ("time",), fill_value=nan)
    var.ancillary_variables = "cut_size"
    var[:] = [10.0, 11.0, 12.0, 13.0]

    over = Dataset(str(tmp_path / "over.nc"), 'w', format='NETCDF4')
    over.setncattr("time_coverage_start", "2023-10-11T00:00:00Z")
    over.setncattr("time_coverage_end", "2023-10-11T12:00:00Z")
    group = over.createGroup("data")
    var = time_coordinate(group)
    var[:] = [1696982400000, 1697004000000]
    var = group.createVariable("cut_size", "f8", (), fill_value=nan)
    var[:] = 30.0
    var = group.createVariable("var1", "f8", ("time",), fill_value=nan)
    var.ancillary_variables = "cut_size"
    var[:] = [20.0, 21.0]

    merge = MergeInstrument()
    merge.overlay(under, not_before_ms=1696982400000, not_after_ms=1697068800000)
    merge.overlay(over, not_before_ms=1696982400000, not_after_ms=1697068800000)
    output = merge.execute(tmp_path / "output.nc")
    merge = None
    under.close()
    under = None
    over.close()
    over = None

    var = output.groups["data"].variables["cut_size"]
    assert var.shape == (4,)
    assert list(var[:]) == [30.0, 30.0, 3.0, 4.0]

    var = output.groups["data"].variables["var1"]
    assert var.ancillary_variables == "cut_size"
    assert var.shape == (4,)
    assert list(var[:]) == [20.0, 21.0, 12.0, 13.0]


def test_split_cutsize(tmp_path):
    under = Dataset(str(tmp_path / "under.nc"), 'w', format='NETCDF4')
    under.setncattr("time_coverage_start", "2023-10-11T00:00:00Z")
    under.setncattr("time_coverage_end", "2023-10-12T00:00:00Z")
    group = under.createGroup("data")
    var = time_coordinate(group)
    var[:] = [1696982400000, 1697004000000, 1697025600000, 1697047200000]
    group.createDimension("cut_size", 2)
    var = group.createVariable("cut_size", "f8", ("cut_size",), fill_value=nan)
    var[:] = [1.0, 2.0]
    var = group.createVariable("var1", "f8", ("time", "cut_size"), fill_value=nan)
    var.ancillary_variables = "cut_size"
    var[:] = [
        [10.0, 11.0],
        [12.0, 13.0],
        [14.0, 15.0],
        [16.0, 17.0],
    ]

    over = Dataset(str(tmp_path / "over.nc"), 'w', format='NETCDF4')
    over.setncattr("time_coverage_start", "2023-10-11T00:00:00Z")
    over.setncattr("time_coverage_end", "2023-10-11T12:00:00Z")
    group = over.createGroup("data")
    var = time_coordinate(group)
    var[:] = [1696982400000, 1697004000000]
    group.createDimension("cut_size", 3)
    var = group.createVariable("cut_size", "f8", ("cut_size",), fill_value=nan)
    var[:] = [3.0, 1.0, 2.0]
    var = group.createVariable("var1", "f8", ("time", "cut_size"), fill_value=nan)
    var.ancillary_variables = "cut_size"
    var[:] = [
        [20.0, 21.0, 22.0],
        [23.0, 24.0, 25.0],
    ]

    merge = MergeInstrument()
    merge.overlay(under, not_before_ms=1696982400000, not_after_ms=1697068800000)
    merge.overlay(over, not_before_ms=1696982400000, not_after_ms=1697068800000)
    output = merge.execute(tmp_path / "output.nc")
    merge = None
    under.close()
    under = None
    over.close()
    over = None

    var = output.groups["data"].variables["cut_size"]
    assert var.shape == (3,)
    assert list(var[:]) == [1.0, 2.0, 3.0]

    var = output.groups["data"].variables["var1"]
    assert var.ancillary_variables == "cut_size"
    assert var.shape == (4, 3)
    assert list(var[0]) == [21.0, 22.0, 20.0]
    assert list(var[1]) == [24.0, 25.0, 23.0]
    assert list(var[2, :2]) == [14.0, 15.0]
    assert var[2, 2].mask or isnan(var[2, 2])
    assert list(var[3, :2]) == [16.0, 17.0]
    assert var[3, 2].mask or isnan(var[3, 2])


def test_split_promote_cutsize(tmp_path):
    under = Dataset(str(tmp_path / "under.nc"), 'w', format='NETCDF4')
    under.setncattr("time_coverage_start", "2023-10-11T00:00:00Z")
    under.setncattr("time_coverage_end", "2023-10-12T00:00:00Z")
    group = under.createGroup("data")
    var = time_coordinate(group)
    var[:] = [1696982400000, 1697004000000, 1697025600000, 1697047200000]
    group.createDimension("cut_size", 2)
    var = group.createVariable("cut_size", "f8", ("cut_size",), fill_value=nan)
    var[:] = [1.0, 2.0]
    var = group.createVariable("var1", "f8", ("time", "cut_size"), fill_value=nan)
    var.ancillary_variables = "cut_size"
    var[:] = [
        [10.0, 11.0],
        [12.0, 13.0],
        [14.0, 15.0],
        [16.0, 17.0],
    ]

    over = Dataset(str(tmp_path / "over.nc"), 'w', format='NETCDF4')
    over.setncattr("time_coverage_start", "2023-10-11T00:00:00Z")
    over.setncattr("time_coverage_end", "2023-10-11T12:00:00Z")
    group = over.createGroup("data")
    var = time_coordinate(group)
    var[:] = [1696982400000, 1697004000000]
    var = group.createVariable("cut_size", "f8", (), fill_value=nan)
    var[:] = 1.0
    var = group.createVariable("var1", "f8", ("time",), fill_value=nan)
    var.ancillary_variables = "cut_size"
    var[:] = [20.0, 21.0]

    merge = MergeInstrument()
    merge.overlay(under, not_before_ms=1696982400000, not_after_ms=1697068800000)
    merge.overlay(over, not_before_ms=1696982400000, not_after_ms=1697068800000)
    output = merge.execute(tmp_path / "output.nc")
    merge = None
    under.close()
    under = None
    over.close()
    over = None

    var = output.groups["data"].variables["cut_size"]
    assert var.shape == (2,)
    assert list(var[:]) == [1.0, 2.0]

    var = output.groups["data"].variables["var1"]
    assert var.ancillary_variables == "cut_size"
    assert var.shape == (4, 2)
    assert list(var[0, :1]) == [20.0]
    assert var[0, 1].mask or isnan(var[0, 1])
    assert list(var[1, :1]) == [21.0]
    assert var[1, 1].mask or isnan(var[1, 1])
    assert list(var[2]) == [14.0, 15.0]
    assert list(var[3]) == [16.0, 17.0]

