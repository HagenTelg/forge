import pytest
import typing
import numpy as np
import forge.data.structure.eventlog as eventlog
from netCDF4 import Dataset, Variable
from forge.data.merge.eventlog import MergeEventLog


def _assign_str(var: Variable, values: typing.List[str]) -> None:
    for i in range(len(values)):
        var[i] = values[i]


def test_noop(tmp_path):
    data = Dataset(str(tmp_path / "data.nc"), 'w', format='NETCDF4')
    data.setncattr("attr1", "value1")

    var = data.createVariable("constant1", "f8", ())
    assert var.dtype == np.float64
    var.setncattr("attr2", "value2")
    var[:] = 1.0

    var = data.createVariable("constant2", str, ())
    var[0] = "1234"

    log = data.createGroup("log")
    event_t = log.createEnumType(np.uint8, "event_t", {
        'Info': 0,
        'Error': 1,
    })

    var = eventlog.event_time(log)
    var[:] = [1696982400000, 1697004000000, 1697025600000, 1697047200000]
    var = eventlog.event_type(log, event_t)
    var[:] = [0, 0, 1, 0]
    var = eventlog.event_source(log)
    _assign_str(var, ["", "S1", "S1", "S2"])
    var = eventlog.event_message(log)
    _assign_str(var, ["M1", "M2", "M1", "M3"])
    var = eventlog.event_auxiliary(log)
    _assign_str(var, ["", "", "", "AUX1"])

    merge = MergeEventLog()
    merge.overlay(data, not_before_ms=1696982400000, not_after_ms=1697068800000)
    output = merge.execute(tmp_path / "output.nc")
    merge = None
    data.close()
    data = None
    assert output is not None

    assert output.getncattr("attr1") == "value1"

    var = output.variables["constant1"]
    assert var.getncattr("attr2") == "value2"
    assert var.size == 1
    assert var[0] == 1.0

    var = output.variables["constant2"]
    assert var[0] == "1234"

    assert len(output.groups) == 1
    log = output.groups["log"]

    assert len(log.dimensions) == 1
    dim = log.dimensions["time"]
    assert dim.isunlimited()
    assert dim.size == 4

    event_t = log.enumtypes["event_t"]
    assert event_t.enum_dict == {
        'Info': 0,
        'Error': 1,
    }

    assert len(log.variables) == 5

    var = log.variables["time"]
    assert var.dtype == np.int64
    assert list(var[:]) == [1696982400000, 1697004000000, 1697025600000, 1697047200000]

    var = log.variables["type"]
    assert var.datatype == event_t
    assert list(var[:]) == [0, 0, 1, 0]

    var = log.variables["source"]
    assert var.dtype == str
    assert list(var[:]) == ["", "S1", "S1", "S2"]

    var = log.variables["message"]
    assert var.dtype == str
    assert list(var[:]) == ["M1", "M2", "M1", "M3"]

    var = log.variables["auxiliary_data"]
    assert var.dtype == str
    assert list(var[:]) == ["", "", "", "AUX1"]


def test_merge(tmp_path):
    under = Dataset(str(tmp_path / "under.nc"), 'w', format='NETCDF4')
    under.setncattr("attr1", "under1")
    under.setncattr("attr2", "under2")

    var = under.createVariable("constant1", "f8", ())
    assert var.dtype == np.float64
    var.setncattr("attr3", "under3")
    var.setncattr("attr4", "under4")
    var[:] = 1.0

    log = under.createGroup("log")
    event_t = log.createEnumType(np.uint8, "event_t", {
        'Info': 0,
        'Error': 1,
    })

    var = eventlog.event_time(log)
    var[:] = [1696982400000, 1697004000000, 1697025600000, 1697047200000]
    var = eventlog.event_type(log, event_t)
    var[:] = [0, 0, 1, 0]
    var = eventlog.event_source(log)
    _assign_str(var, ["", "S1", "S1", "S2"])
    var = eventlog.event_message(log)
    _assign_str(var, ["M1", "M2", "M1", "M3"])
    var = eventlog.event_auxiliary(log)
    _assign_str(var, ["", "", "", "AUX1"])

    over = Dataset(str(tmp_path / "over.nc"), 'w', format='NETCDF4')
    over.setncattr("attr1", "over1")
    over.setncattr("attr5", "over2")

    var = over.createVariable("constant1", "f8", ())
    assert var.dtype == np.float64
    var.setncattr("attr3", "over3")
    var.setncattr("attr6", "over4")
    var[:] = 2.0

    log = over.createGroup("log")
    event_t = log.createEnumType(np.uint8, "event_t", {
        'Info': 0,
        'Error': 1,
        'Other': 2,
    })

    var = eventlog.event_time(log)
    var[:] = [1696982400000, 1697004000000, 1697025600001, 1697068800001]
    var = eventlog.event_type(log, event_t)
    var[:] = [0, 0, 1, 0]
    var = eventlog.event_source(log)
    _assign_str(var, ["", "S9", "S2", "D1"])
    var = eventlog.event_message(log)
    _assign_str(var, ["M1", "M2", "M1", "D2"])
    var = eventlog.event_auxiliary(log)
    _assign_str(var, ["", "", "", "AUXD1"])


    merge = MergeEventLog()
    merge.overlay(under, not_before_ms=1696982400000, not_after_ms=1697068800000)
    merge.overlay(over, not_before_ms=1696982400000, not_after_ms=1697068800000)
    output = merge.execute(tmp_path / "output.nc")
    merge = None
    under.close()
    data = None
    over.close()
    over = None
    assert output is not None

    assert output.getncattr("attr1") == "over1"
    assert output.getncattr("attr2") == "under2"
    assert output.getncattr("attr5") == "over2"

    var = output.variables["constant1"]
    assert var.getncattr("attr3") == "over3"
    assert var.getncattr("attr4") == "under4"
    assert var.getncattr("attr6") == "over4"
    assert var.size == 1
    assert var[0] == 2.0

    assert len(output.groups) == 1
    log = output.groups["log"]

    assert len(log.dimensions) == 1
    dim = log.dimensions["time"]
    assert dim.isunlimited()
    assert dim.size == 6

    event_t = log.enumtypes["event_t"]
    assert event_t.enum_dict == {
        'Info': 0,
        'Error': 1,
        'Other': 2,
    }

    assert len(log.variables) == 5

    var = log.variables["time"]
    assert var.dtype == np.int64
    assert list(var[:]) == [1696982400000, 1697004000000, 1697004000000, 1697025600000, 1697025600001, 1697047200000]

    var = log.variables["type"]
    assert var.datatype == event_t
    assert list(var[:]) == [0, 0, 0, 1, 1, 0]

    var = log.variables["source"]
    assert var.dtype == str
    assert list(var[:]) == ["", "S1", "S9", "S1", "S2", "S2"]

    var = log.variables["message"]
    assert var.dtype == str
    assert list(var[:]) == ["M1", "M2", "M2", "M1", "M1", "M3"]

    var = log.variables["auxiliary_data"]
    assert var.dtype == str
    assert list(var[:]) == ["", "", "", "", "", "AUX1"]
