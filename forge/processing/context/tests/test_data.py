import pytest
import numpy as np
from math import nan, isfinite
from netCDF4 import Dataset, Group, Variable
from forge.data.structure import instrument_timeseries
from forge.data.structure.timeseries import time_coordinate, cutsize_variable, cutsize_coordinate, averaged_time_variable
from forge.data.structure.variable import variable_wavelength
from forge.processing.context.data import SelectedData


def _setup_file(file: Dataset, times):
    time_values = np.array(times, copy=False)

    instrument_timeseries(
        file, "NIL", "X1",
        float(times[0]) / 1000.0, float(times[-1]) / 1000.0 + 1,
        1, {"aerosol", "testtag"}
    )

    data_group: Group = file.createGroup("data")

    time_var = time_coordinate(data_group)
    time_var[:] = time_values

    return data_group, time_values


def _make_file(file: Dataset, times):
    data_group, time_values = _setup_file(file, times)

    var1: Variable = data_group.createVariable("var1", 'f8', ('time',))
    var1.variable_id = "Z1"
    var1.standard_name = "standard_1"
    var1[:] = np.arange(time_values.shape[0]) + 100.0

    var2: Variable = data_group.createVariable("var2", 'f8', ('time',))
    var2.variable_id = "Z2"
    var2.standard_name = "standard_2"
    var2[:] = np.arange(time_values.shape[0]) + 200.0

    return SelectedData.ensure_data(file)


@pytest.fixture
def first_data(tmp_path):
    file = Dataset(str(tmp_path / "first.nc"), 'w', format='NETCDF4')
    return _make_file(file, [1000, 2000, 3000, 4000])


@pytest.fixture
def second_data(tmp_path):
    file = Dataset(str(tmp_path / "second.nc"), 'w', format='NETCDF4')
    return _make_file(file, [1000, 2000, 3000, 4000])


@pytest.fixture
def third_data(tmp_path):
    file = Dataset(str(tmp_path / "third.nc"), 'w', format='NETCDF4')
    return _make_file(file, [999, 1999, 2999, 3000, 3001, 4000])


@pytest.fixture
def wavelength_data(tmp_path):
    file = Dataset(str(tmp_path / "wavelength.nc"), 'w', format='NETCDF4')
    data_group, time_values = _setup_file(file, [1000, 2000, 3000, 4000])

    data_group.createDimension("wavelength", 2)
    wavelength = data_group.createVariable("wavelength", 'f8', ("wavelength",))
    variable_wavelength(wavelength)
    wavelength[:] = [30.0, 40.0]

    var1: Variable = data_group.createVariable("var1", 'f8', ('time', 'wavelength'))
    var1.variable_id = "Z1"
    var1.standard_name = "standard_1"
    var1[:] = np.arange(time_values.shape[0] * 2).reshape(time_values.shape[0], 2) + 100.0

    var2: Variable = data_group.createVariable("var2", 'f8', ('time',))
    var2.variable_id = "Z2"
    var2.standard_name = "standard_2"
    var2[:] = np.arange(time_values.shape[0]) + 200.0

    return SelectedData.ensure_data(file)


@pytest.fixture
def cut_time_data(tmp_path):
    file = Dataset(str(tmp_path / "cut_time.nc"), 'w', format='NETCDF4')
    data_group, time_values = _setup_file(file, [1000, 2000, 3000, 4000])

    cut_size = cutsize_variable(data_group)
    cut_data = np.arange(time_values.shape[0], dtype=np.float64)
    cut_data[0] = nan
    cut_size[:] = cut_data

    var1: Variable = data_group.createVariable("var1", 'f8', ('time',))
    var1.variable_id = "Z1"
    var1.standard_name = "standard_1"
    var1.ancillary_variables = "cut_size"
    var1[:] = np.arange(time_values.shape[0]) + 100.0

    var2: Variable = data_group.createVariable("var2", 'f8', ('time',))
    var2.variable_id = "Z2"
    var2.standard_name = "standard_2"
    var2[:] = np.arange(time_values.shape[0]) + 200.0

    return SelectedData.ensure_data(file)


@pytest.fixture
def cut_dimension_data(tmp_path):
    file = Dataset(str(tmp_path / "cut_dimension.nc"), 'w', format='NETCDF4')
    data_group, time_values = _setup_file(file, [1000, 2000, 3000, 4000])

    cut_size = cutsize_coordinate(data_group, 2)
    cut_size[:] = [1.0, nan]

    var1: Variable = data_group.createVariable("var1", 'f8', ('time', 'cut_size'))
    var1.variable_id = "Z1"
    var1.standard_name = "standard_1"
    var1[:] = np.arange(time_values.shape[0] * 2).reshape(time_values.shape[0], 2) + 100.0

    var2: Variable = data_group.createVariable("var2", 'f8', ('time',))
    var2.variable_id = "Z2"
    var2.standard_name = "standard_2"
    var2[:] = np.arange(time_values.shape[0]) + 200.0

    return SelectedData.ensure_data(file)


def test_basic_select(first_data: SelectedData):
    assert not first_data.placeholder
    assert first_data.station.upper() == "NIL"
    assert first_data.times.tolist() == [1000, 2000, 3000, 4000]

    assert getattr(first_data.root, 'history', None) is None
    first_data.append_history("test")
    assert "test" in getattr(first_data.root, 'history', "")

    assert "wavelength" not in first_data.root.groups["data"].variables
    assert "wavelength" not in first_data.root.groups["data"].dimensions
    first_data.set_wavelengths([500.0, 600.0])
    assert first_data.root.groups["data"].variables["wavelength"][...].tolist() == [500.0, 600.0]
    assert first_data.root.groups["data"].dimensions["wavelength"].size == 2
    first_data.set_wavelengths([501.0, 601.0])
    assert first_data.root.groups["data"].variables["wavelength"][...].tolist() == [501.0, 601.0]

    empty_data = SelectedData.empty_placeholder()
    assert empty_data.placeholder

    count = 0
    for var1 in first_data.select_variable({"variable_name": "var1"}):
        assert var1.variable.name == "var1"
        assert var1[...].tolist() == [100.0, 101.0, 102.0, 103.0]
        assert var1.times.tolist() == [1000, 2000, 3000, 4000]
        var1[0] = 99.0
        count += 1
    assert count == 1
    for _ in empty_data.select_variable({"variable_name": "var1"}):
        assert False
    for _ in first_data.select_variable({"variable_name": "notfound"}):
        assert False

    count = 0
    for var1, var2 in first_data.select_variable({"variable_name": "var1"}, {"variable_name": "var2"}):
        assert var1.variable.name == "var1"
        assert var1[...].tolist() == [99.0, 101.0, 102.0, 103.0]
        assert var2.variable.name == "var2"
        count += 1
    assert count == 1

    count = 0
    for (var1,) in first_data.select_variable({"variable_name": "var1"}, always_tuple=True, commit_variable=False):
        assert var1.variable.name == "var1"
        assert var1[...].tolist() == [99.0, 101.0, 102.0, 103.0]
        var1[0] = 98.0
        count += 1
    assert count == 1

    count = 0
    for var1, var2 in first_data.select_variable({"variable_name": "var1"}, {"variable_name": "var2"}, commit_auxiliary=True):
        assert var1.variable.name == "var1"
        assert var1[...].tolist() == [99.0, 101.0, 102.0, 103.0]
        assert var2.variable.name == "var2"
        assert var2[...].tolist() == [200.0, 201.0, 202.0, 203.0]
        var1[0] = 98.0
        var2[0] = 199.0
        count += 1
    assert count == 1

    count = 0
    for var1, var2 in first_data.select_variable({"variable_name": "var1"}, {"variable_name": "notfound"}):
        assert var1.variable.name == "var1"
        assert var1[...].tolist() == [98.0, 101.0, 102.0, 103.0]
        assert np.all(np.isnan(var2.values))
        assert var2.times.tolist() == [1000, 2000, 3000, 4000]
        count += 1
    assert count == 1

    count = 0
    for var in first_data.select_variable({"variable_id": "Z."}):
        if var.variable.name == "var1":
            assert var[...].tolist() == [98.0, 101.0, 102.0, 103.0]
            assert (count & 0x01) == 0
            count |= 0x01
        elif var.variable.name == "var2":
            assert var[...].tolist() == [199.0, 201.0, 202.0, 203.0]
            assert (count & 0x02) == 0
            count |= 0x02
        else:
            assert False
    assert count == 0x03


def test_restrict_times(first_data: SelectedData):
    first_data.restrict_times(2000, 3001)
    assert first_data.times.tolist() == [2000, 3000]

    for var1 in first_data.select_variable({"variable_name": "var1"}):
        assert var1.variable.name == "var1"
        assert var1[...].tolist() == [101.0, 102.0]
        var1[0] = 99.0

    for var1 in first_data.select_variable({"variable_name": "var1"}):
        assert var1.variable.name == "var1"
        assert var1[...].tolist() == [99.0, 102.0]

    raw_var = first_data.root.groups["data"].variables["var1"]
    assert raw_var[...].tolist() == [100.0, 99.0, 102.0, 103.0]


def test_restrict_empty(first_data: SelectedData):
    first_data.restrict_times(5000, 6000)
    assert first_data.times.tolist() == []

    for var1 in first_data.select_variable({"variable_name": "var1"}):
        assert var1.variable.name == "var1"
        assert var1[...].tolist() == []


def test_basic_input(first_data: SelectedData):
    for var1 in first_data.select_variable({"variable_name": "var1"}):
        assert var1.times.tolist() == [1000, 2000, 3000, 4000]
        assert var1[...].tolist() == [100.0, 101.0, 102.0, 103.0]

        try:
            first_data.get_input(var1, {"variable_name": "notfound"})
            assert False
        except FileNotFoundError:
            pass
        empty_var = first_data.get_input(var1, {"variable_name": "notfound"}, error_when_missing=False)
        assert np.all(np.isnan(empty_var.values))
        assert empty_var.times.tolist() == [1000, 2000, 3000, 4000]

        var2 = first_data.get_input(var1, {"variable_name": "var2"})
        assert var2.times.tolist() == [1000, 2000, 3000, 4000]
        assert var2[...].tolist() == [200.0, 201.0, 202.0, 203.0]

    first_data.restrict_times(2000, 3001)
    for var1 in first_data.select_variable({"variable_name": "var1"}):
        assert var1.times.tolist() == [2000, 3000]
        assert var1[...].tolist() == [101.0, 102.0]

        empty_var = first_data.get_input(var1, {"variable_name": "notfound"}, error_when_missing=False)
        assert np.all(np.isnan(empty_var.values))
        assert empty_var.times.tolist() == [2000, 3000]

        var2 = first_data.get_input(var1, {"variable_name": "var2"})
        assert var2.times.tolist() == [2000, 3000]
        assert var2[...].tolist() == [201.0, 202.0]


def test_other_input(first_data: SelectedData, second_data: SelectedData):
    for var1 in first_data.select_variable({"variable_name": "var1"}, commit_variable=False):
        assert var1.times.tolist() == [1000, 2000, 3000, 4000]
        assert var1[...].tolist() == [100.0, 101.0, 102.0, 103.0]

        var2 = second_data.get_input(var1, {"variable_name": "var2"})
        assert var2.times.tolist() == [1000, 2000, 3000, 4000]
        assert var2[...].tolist() == [200.0, 201.0, 202.0, 203.0]

        var1a = second_data.get_input(var1, {"variable_name": "var1"})
        assert var1a.times.tolist() == [1000, 2000, 3000, 4000]
        assert var1a[...].tolist() == [100.0, 101.0, 102.0, 103.0]

        var1[0] = 99.0
        assert var1[...].tolist() == [99.0, 101.0, 102.0, 103.0]
        assert var1a[...].tolist() == [100.0, 101.0, 102.0, 103.0]

    first_data.restrict_times(2000, 3001)
    for var1 in first_data.select_variable({"variable_name": "var1"}, commit_variable=False):
        assert var1.times.tolist() == [2000, 3000]
        assert var1[...].tolist() == [101.0, 102.0]

        var2 = second_data.get_input(var1, {"variable_name": "var2"})
        assert var2.times.tolist() == [2000, 3000]
        assert var2[...].tolist() == [201.0, 202.0]

        var1a = second_data.get_input(var1, {"variable_name": "var1"})
        assert var1a.times.tolist() == [2000, 3000]
        assert var1a[...].tolist() == [101.0, 102.0]

        var1[0] = 99.0
        assert var1[...].tolist() == [99.0, 102.0]
        assert var1a[...].tolist() == [101.0, 102.0]


def test_align_input(first_data: SelectedData, third_data: SelectedData):
    for var1 in first_data.select_variable({"variable_name": "var1"}, commit_variable=False):
        assert var1.times.tolist() == [1000, 2000, 3000, 4000]
        assert var1[...].tolist() == [100.0, 101.0, 102.0, 103.0]

        var2 = third_data.get_input(var1, {"variable_name": "var2"})
        assert var2.times.tolist() == [999, 1999, 3000, 4000]
        assert var2[...].tolist() == [200.0, 201.0, 203.0, 205.0]

        var1a = third_data.get_input(var1, {"variable_name": "var1"})
        assert var1a.times.tolist() == [999, 1999, 3000, 4000]
        assert var1a[...].tolist() == [100.0, 101.0, 103.0, 105.0]

        var1[0] = 99.0
        assert var1[...].tolist() == [99.0, 101.0, 102.0, 103.0]
        assert var1a[...].tolist() == [100.0, 101.0, 103.0, 105.0]
        var1a[1] = 199.0
        var1a.commit()
        assert var1a[...].tolist() == [100.0, 199.0, 103.0, 105.0]

    for var1 in third_data.select_variable({"variable_name": "var1"}, commit_variable=False):
        assert var1.times.tolist() == [999, 1999, 2999, 3000, 3001, 4000]
        assert var1[...].tolist() == [100.0, 199.0, 199.0, 103.0, 103.0, 105.0]

    first_data.restrict_times(2000, 3001)
    for var1 in first_data.select_variable({"variable_name": "var1"}, commit_variable=False):
        assert var1.times.tolist() == [2000, 3000]
        assert var1[...].tolist() == [101.0, 102.0]

        var2 = third_data.get_input(var1, {"variable_name": "var2"})
        assert var2.times.tolist() == [1999, 3000]
        assert var2[...].tolist() == [201.0, 203.0]


def test_basic_output(first_data: SelectedData, tmp_path):
    var1 = next(first_data.select_variable({"variable_name": "var1"}, commit_variable=False))
    with first_data.get_output(var1, "outvar") as outvar:
        outvar[:] = [300.0, 301.0, 302.0, 303.0]
    assert first_data.root.groups["data"].variables["outvar"][:].tolist() == [300.0, 301.0, 302.0, 303.0]

    other_file = Dataset(str(tmp_path / "other.nc"), 'w', format='NETCDF4')
    instrument_timeseries(
        other_file, "NIL", "XO",
        1.0, 10.0,
        1, {"aerosol", "testtag"}
    )
    other_data = SelectedData.ensure_data(other_file)
    with other_data.get_output(var1, "outvar") as outvar:
        outvar[:] = [400.0, 401.0, 402.0, 403.0]
    assert other_file.groups["data"].variables["outvar"][:].tolist() == [400.0, 401.0, 402.0, 403.0]
    assert other_file.groups["data"].variables["time"][:].tolist() == [1000, 2000, 3000, 4000]


def test_wavelength_output(wavelength_data: SelectedData, tmp_path):
    var1 = next(wavelength_data.select_variable({"variable_name": "var1"}, commit_variable=False))
    assert var1.wavelengths == [30.0, 40.0]

    with wavelength_data.get_output(var1, "outvar", wavelength=True) as outvar:
        outvar[:] = np.array([
            [300.0, 301.0],
            [302.0, 303.0],
            [304.0, 305.0],
            [306.0, 307.0],
        ])
    assert wavelength_data.root.groups["data"].variables["outvar"][:].tolist() == [
        [300.0, 301.0],
        [302.0, 303.0],
        [304.0, 305.0],
        [306.0, 307.0],
    ]
    assert "wavelength" in wavelength_data.root.groups["data"].variables["outvar"].dimensions

    other_file = Dataset(str(tmp_path / "other.nc"), 'w', format='NETCDF4')
    instrument_timeseries(
        other_file, "NIL", "XO",
        1.0, 10.0,
        1, {"aerosol", "testtag"}
    )
    other_data = SelectedData.ensure_data(other_file)
    other_data.set_wavelengths([30.0, 40.0])
    with other_data.get_output(var1, "outvar", wavelength=True) as outvar:
        outvar[:] = np.array([
            [400.0, 401.0],
            [402.0, 403.0],
            [404.0, 405.0],
            [406.0, 407.0],
        ])
    assert other_file.groups["data"].variables["outvar"][:].tolist() == [
        [400.0, 401.0],
        [402.0, 403.0],
        [404.0, 405.0],
        [406.0, 407.0],
    ]
    assert "wavelength" in other_file.groups["data"].variables["outvar"].dimensions
    assert other_file.groups["data"].variables["time"][:].tolist() == [1000, 2000, 3000, 4000]
    assert other_file.groups["data"].variables["wavelength"][:].tolist() == [30.0, 40.0]


def test_cut_time_output(cut_time_data: SelectedData, tmp_path):
    var1 = next(cut_time_data.select_variable({"variable_name": "var1"}, commit_variable=False))
    assert var1.is_cut_split

    with cut_time_data.get_output(var1, "outvar") as outvar:
        outvar[:] = [300.0, 301.0, 302.0, 303.0]
    assert cut_time_data.root.groups["data"].variables["outvar"][:].tolist() == [300.0, 301.0, 302.0, 303.0]
    assert "cut_size" in cut_time_data.root.groups["data"].variables["outvar"].ancillary_variables

    other_file = Dataset(str(tmp_path / "other.nc"), 'w', format='NETCDF4')
    instrument_timeseries(
        other_file, "NIL", "XO",
        1.0, 10.0,
        1, {"aerosol", "testtag"}
    )
    other_data = SelectedData.ensure_data(other_file)
    with other_data.get_output(var1, "outvar") as outvar:
        outvar[:] = [400.0, 401.0, 402.0, 403.0]
    assert other_file.groups["data"].variables["outvar"][:].tolist() == [400.0, 401.0, 402.0, 403.0]
    assert "cut_size" in other_file.groups["data"].variables["outvar"].ancillary_variables
    assert other_file.groups["data"].variables["time"][:].tolist() == [1000, 2000, 3000, 4000]
    assert other_file.groups["data"].variables["cut_size"][:].data.tolist() == pytest.approx([nan, 1.0, 2.0, 3.0], nan_ok=True)


def test_cut_dimension_output(cut_dimension_data: SelectedData, tmp_path):
    var1 = next(cut_dimension_data.select_variable({"variable_name": "var1"}, commit_variable=False))
    assert var1.is_cut_split

    with cut_dimension_data.get_output(var1, "outvar") as outvar:
        outvar[:] = np.array([
            [300.0, 301.0],
            [302.0, 303.0],
            [304.0, 305.0],
            [306.0, 307.0],
        ])
    assert cut_dimension_data.root.groups["data"].variables["outvar"][:].tolist() == [
        [300.0, 301.0],
        [302.0, 303.0],
        [304.0, 305.0],
        [306.0, 307.0],
    ]
    assert "cut_size" in cut_dimension_data.root.groups["data"].variables["outvar"].dimensions

    other_file = Dataset(str(tmp_path / "other.nc"), 'w', format='NETCDF4')
    instrument_timeseries(
        other_file, "NIL", "XO",
        1.0, 10.0,
        1, {"aerosol", "testtag"}
    )
    other_data = SelectedData.ensure_data(other_file)
    with other_data.get_output(var1, "outvar") as outvar:
        outvar[:] = np.array([
            [400.0, 401.0],
            [402.0, 403.0],
            [404.0, 405.0],
            [406.0, 407.0],
        ])
    assert other_file.groups["data"].variables["outvar"][:].tolist() == [
        [400.0, 401.0],
        [402.0, 403.0],
        [404.0, 405.0],
        [406.0, 407.0],
    ]
    assert "cut_size" in other_file.groups["data"].variables["outvar"].dimensions
    assert other_file.groups["data"].variables["time"][:].tolist() == [1000, 2000, 3000, 4000]
    assert other_file.groups["data"].variables["cut_size"][:].data.tolist() == pytest.approx([1.0, nan], nan_ok=True)


def test_average_weight(first_data: SelectedData):
    averaged_time = averaged_time_variable(first_data.root.groups["data"])
    averaged_time[:] = [1000, 1000, 500, 1000]
    var1 = next(first_data.select_variable({"variable_name": "var1"}, commit_variable=False))
    assert var1.average_weights.tolist() == [1.0, 1.0, 0.5, 1.0]


def test_wavelength_fixed(wavelength_data: SelectedData):
    var1 = next(wavelength_data.select_variable({"variable_name": "var1"}, commit_variable=False))
    assert var1.has_multiple_wavelengths
    assert not var1.has_changing_wavelengths
    assert var1.wavelengths == [30.0, 40.0]

    assert var1[var1.get_wavelength_index(30.0)].tolist() == [100.0, 102.0, 104.0, 106.0]
    assert var1[var1.get_wavelength_index(40)].tolist() == [101.0, 103.0, 105.0, 107.0]
    assert var1[var1.get_wavelength_index(lambda x: x >= 30.0)].tolist() == [
        [100.0, 101.0],
        [102.0, 103.0],
        [104.0, 105.0],
        [106.0, 107.0],
    ]
    assert var1[var1.get_wavelength_index(lambda x: x < 10.0)].tolist() == [
        [],
        [],
        [],
        [],
    ]

    var2 = next(wavelength_data.select_variable({"variable_name": "var2"}, commit_variable=False))
    assert not var2.has_multiple_wavelengths
    assert not var2.has_changing_wavelengths
    assert len(var2.wavelengths) == 0
    assert var2[var2.get_wavelength_index(30.0)].tolist() == []
    assert var2[var2.get_wavelength_index(lambda x: np.invert(np.isfinite(x)))].tolist() == [200.0, 201.0, 202.0, 203.0]


def test_wavelength_changing(tmp_path):
    file = Dataset(str(tmp_path / "file.nc"), 'w', format='NETCDF4')
    #                                                  2023-01-01       2023-01-02       2023-01-03       2023-01-04
    data_group, time_values = _setup_file(file, [1672531200_000, 1672617600_000, 1672704000_000, 1672790400_000])

    data_group.createDimension("wavelength", 2)
    wavelength = data_group.createVariable("wavelength", 'f8', ("wavelength",))
    variable_wavelength(wavelength)
    wavelength.change_history = "2023-01-02T23:00:00Z,20,25"
    wavelength[:] = [30.0, 40.0]

    data_var1: Variable = data_group.createVariable("var1", 'f8', ('time', 'wavelength'))
    data_var1[:] = np.arange(time_values.shape[0] * 2).reshape(time_values.shape[0], 2) + 100.0

    data = SelectedData.ensure_data(file)
    var1 = next(data.select_variable({"variable_name": "var1"}, commit_variable=False))
    assert var1.has_multiple_wavelengths
    assert var1.has_changing_wavelengths

    hit = 0
    for wavelengths, value_index, time_index in var1.select_wavelengths():
        if wavelengths == [30.0, 40.0]:
            assert (hit & 0b01) == 0
            hit |= 0b01
            assert len(value_index) == 2
            assert var1[value_index[0]].tolist() == [104.0, 106.0]
            assert var1[value_index[1]].tolist() == [105.0, 107.0]
            assert var1.times[time_index].tolist() == [1672704000_000, 1672790400_000]
        elif wavelengths == [20.0, 25.0]:
            assert (hit & 0b10) == 0
            hit |= 0b10
            assert len(value_index) == 2
            assert var1[value_index[0]].tolist() == [100.0, 102.0]
            assert var1[value_index[1]].tolist() == [101.0, 103.0]
            assert var1.times[time_index].tolist() == [1672531200_000, 1672617600_000]
        else:
            assert False
    assert hit == 0b11

    hit = 0
    for wavelengths, value_index, time_index in var1.select_wavelengths(tail_index_only=True):
        if wavelengths == [30.0, 40.0]:
            assert (hit & 0b01) == 0
            hit |= 0b01
            assert len(value_index) == 2
            assert var1[value_index[0]].tolist() == [100.0, 102.0, 104.0, 106.0]
            assert var1[value_index[1]].tolist() == [101.0, 103.0, 105.0, 107.0]
            assert var1[(*time_index, *value_index[0])].tolist() == [104.0, 106.0]
            assert var1.times[time_index].tolist() == [1672704000_000, 1672790400_000]
        elif wavelengths == [20.0, 25.0]:
            assert (hit & 0b10) == 0
            hit |= 0b10
            assert len(value_index) == 2
            assert var1[value_index[0]].tolist() == [100.0, 102.0, 104.0, 106.0]
            assert var1[value_index[1]].tolist() == [101.0, 103.0, 105.0, 107.0]
            assert var1[(*time_index, *value_index[0])].tolist() == [100.0, 102.0]
            assert var1.times[time_index].tolist() == [1672531200_000, 1672617600_000]
        else:
            assert False
    assert hit == 0b11


def test_cut_time(cut_time_data: SelectedData):
    var1 = next(cut_time_data.select_variable({"variable_name": "var1"}, commit_variable=False))
    assert var1.is_cut_split
    var2 = next(cut_time_data.select_variable({"variable_name": "var2"}, commit_variable=False))
    assert not var2.is_cut_split

    assert var1[var1.get_cut_size_index(lambda x: x <= 2.0)].tolist() == [101.0, 102.0]
    assert var1[var1.get_cut_size_index(lambda x: np.invert(x <= 2.0))].tolist() == [100.0, 103.0]
    assert var2[var2.get_cut_size_index(lambda x: x <= 2.0)].tolist() == []
    assert var2[var2.get_cut_size_index(lambda x: np.invert(x <= 2.0))].tolist() == [200.0, 201.0, 202.0, 203.0]

    count = 0
    for cut_size, value_index, time_index in var1.select_cut_size():
        if not isfinite(cut_size):
            var1[value_index] = 99.0
            assert var1.times[time_index].tolist() == [1000]
        else:
            var1[value_index] = var1.times[time_index] + cut_size
        count += 1
    assert count == 4
    assert var1[:].tolist() == [99.0, 2001.0, 3002.0, 4003.0]

    count = 0
    for cut_size, value_index, time_index in var2.select_cut_size():
        assert not isfinite(cut_size)
        count += 1
        assert var2[value_index].tolist() == [200.0, 201.0, 202.0, 203.0]
        assert var2.times[time_index].tolist() == [1000, 2000, 3000, 4000]
    assert count == 1


def test_cut_dimension(cut_dimension_data: SelectedData):
    var1 = next(cut_dimension_data.select_variable({"variable_name": "var1"}, commit_variable=False))
    assert var1.is_cut_split
    var2 = next(cut_dimension_data.select_variable({"variable_name": "var2"}, commit_variable=False))
    assert not var2.is_cut_split

    assert var1[var1.get_cut_size_index(lambda x: x <= 1.0, preserve_dimensions=False)].tolist() == [100.0, 102.0, 104.0, 106.0]
    assert var1[var1.get_cut_size_index(lambda x: np.invert(x <= 1.0), preserve_dimensions=False)].tolist() == [101.0, 103.0, 105.0, 107.0]
    assert var2[var2.get_cut_size_index(lambda x: x <= 1.0)].tolist() == []
    assert var2[var2.get_cut_size_index(lambda x: np.invert(x <= 1.0))].tolist() == [200.0, 201.0, 202.0, 203.0]

    count = 0
    for cut_size, value_index, time_index in var1.select_cut_size():
        assert var1.times[time_index].tolist() == [1000, 2000, 3000, 4000]
        if not isfinite(cut_size):
            var1[value_index] = var1.times[time_index] + 99.0
        else:
            var1[value_index] = var1.times[time_index] + cut_size
        count += 1
    assert count == 2
    assert var1[:].tolist() == [
        [1001.0, 1099.0],
        [2001.0, 2099.0],
        [3001.0, 3099.0],
        [4001.0, 4099.0],
    ]

    count = 0
    for cut_size, value_index, time_index in var2.select_cut_size():
        assert not isfinite(cut_size)
        count += 1
        assert var2[value_index].tolist() == [200.0, 201.0, 202.0, 203.0]
        assert var2.times[time_index].tolist() == [1000, 2000, 3000, 4000]
    assert count == 1
