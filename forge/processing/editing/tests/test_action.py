import pytest
import numpy as np
from json import dumps as to_json
from math import nan
from netCDF4 import Dataset
from forge.processing.editing.action import Action
import forge.data.structure.timeseries as data_structure
import forge.data.structure.variable as data_variable


@pytest.fixture
def data_file(tmp_path):
    file = Dataset(str(tmp_path / "data.nc"), 'w', format='NETCDF4')
    file.instrument_id = "X1"
    file.forge_tags = "tag1 tag2"
    file.instrument = "testint"

    g = file.createGroup("data")
    times = data_structure.time_coordinate(g)

    times[:] = [100, 200, 300, 400]

    var = data_structure.measured_variable(g, "first_var")
    var.variable_id = "N"
    var[:] = [10, 11, 12, 13]

    var = data_structure.measured_variable(g, "second_var")
    var[:] = [20, 21, 22, 23]

    g.createDimension("wavelength", 3)
    wl = g.createVariable("wavelength", "f8", ("wavelength",))
    data_variable.variable_wavelength(wl)
    wl[:] = [100, 200, 300]

    var = data_structure.measured_variable(g, "scattering_coefficient", dimensions=["wavelength"])
    data_variable.variable_total_scattering(var)
    var.variable_id = "Bs"
    var[:, 0] = [1000, 1001, 1002, 1003]
    var[:, 1] = [2000, 2001, 2002, 2003]
    var[:, 2] = [3000, 3001, 3002, 3003]

    flags = g.createVariable("system_flags", np.uint64, ("time",), fill_value=False)
    data_variable.variable_flags(flags, {0x01: "flag1"})
    flags[:] = [0x01, 0, 0, 0x01]

    return file


def test_invalidate(data_file: Dataset):
    from forge.processing.editing.action import Invalidate

    assert Action.from_code("Invalidate") == Invalidate

    a = Invalidate(to_json({
        "selection": [{"variable_id": "N"}]
    }))
    assert not a.needs_prepare
    assert not a.filter_data(data_file, data_file)
    assert a.filter_data(data_file, data_file.groups["data"])
    a.apply(data_file, data_file.groups["data"], slice(0, 2))
    assert data_file.groups["data"].variables["first_var"][:].data.tolist() == pytest.approx([nan, nan, 12, 13], nan_ok=True)
    assert data_file.groups["data"].variables["second_var"][:].data.tolist() == pytest.approx([20, 21, 22, 23], nan_ok=True)

    a = Invalidate(to_json({
        "selection": [{"variable_name": "scattering_coefficient", "wavelength": 200}]
    }))
    assert not a.needs_prepare
    assert not a.filter_data(data_file, data_file)
    assert a.filter_data(data_file, data_file.groups["data"])
    a.apply(data_file, data_file.groups["data"], slice(3, 4))
    assert data_file.groups["data"].variables["first_var"][:].data.tolist() == pytest.approx([nan, nan, 12, 13],
                                                                                             nan_ok=True)
    assert data_file.groups["data"].variables["scattering_coefficient"][:].data[:, 0].tolist() == pytest.approx([1000, 1001, 1002, 1003], nan_ok=True)
    assert data_file.groups["data"].variables["scattering_coefficient"][:].data[:, 1].tolist() == pytest.approx([2000, 2001, 2002, nan], nan_ok=True)
    assert data_file.groups["data"].variables["scattering_coefficient"][:].data[:, 2].tolist() == pytest.approx([3000, 3001, 3002, 3003], nan_ok=True)


def test_contaminate(data_file: Dataset):
    from forge.processing.editing.action import Contaminate

    assert Action.from_code("Contaminate") == Contaminate

    a = Contaminate("")
    assert not a.needs_prepare
    assert not a.filter_data(data_file, data_file)
    assert a.filter_data(data_file, data_file.groups["data"])
    a.apply(data_file, data_file.groups["data"], slice(0, 2))
    assert data_file.groups["data"].variables["system_flags"][:].data.tolist() == [0x03, 0x02, 0, 0x01]

    from forge.data.flags import parse_flags
    flags = parse_flags(data_file.groups["data"].variables["system_flags"])
    assert flags == {0x01: "flag1", 0x02: "data_contamination_mentor_edit"}


def test_abnormal_data(data_file: Dataset):
    from forge.processing.editing.action import AbnormalData

    assert Action.from_code("AbnormalData") == AbnormalData

    a = AbnormalData(to_json({'episode_type': 'wild_fire'}))
    assert not a.needs_prepare
    assert not a.filter_data(data_file, data_file)
    assert a.filter_data(data_file, data_file.groups["data"])
    a.apply(data_file, data_file.groups["data"], slice(0, 2))
    assert data_file.groups["data"].variables["system_flags"][:].data.tolist() == [0x03, 0x02, 0, 0x01]

    from forge.data.flags import parse_flags
    flags = parse_flags(data_file.groups["data"].variables["system_flags"])
    assert flags == {0x01: "flag1", 0x02: "abnormal_data_wild_fire"}


def test_calibration(data_file: Dataset):
    from forge.processing.editing.action import Calibration

    assert Action.from_code("Calibration") == Calibration

    a = Calibration(to_json({
        "selection": [{"variable_id": "N"}],
        "calibration": [5.0, 1.0],
    }))
    assert not a.needs_prepare
    assert not a.filter_data(data_file, data_file)
    assert a.filter_data(data_file, data_file.groups["data"])
    a.apply(data_file, data_file.groups["data"], slice(0, 2))
    assert data_file.groups["data"].variables["first_var"][:].data.tolist() == [15, 16, 12, 13]
    assert data_file.groups["data"].variables["second_var"][:].data.tolist() == [20, 21, 22, 23]

    a = Calibration(to_json({
        "selection": [{"variable_name": "scattering_coefficient", "wavelength": 200}],
        "calibration": [6.0, 1.0],
    }))
    assert not a.needs_prepare
    assert not a.filter_data(data_file, data_file)
    assert a.filter_data(data_file, data_file.groups["data"])
    a.apply(data_file, data_file.groups["data"], slice(3, 4))
    assert data_file.groups["data"].variables["first_var"][:].data.tolist() == [15, 16, 12, 13]
    assert data_file.groups["data"].variables["scattering_coefficient"][:].data[:, 0].tolist() == pytest.approx([1000, 1001, 1002, 1003], nan_ok=True)
    assert data_file.groups["data"].variables["scattering_coefficient"][:].data[:, 1].tolist() == pytest.approx([2000, 2001, 2002, 2009], nan_ok=True)
    assert data_file.groups["data"].variables["scattering_coefficient"][:].data[:, 2].tolist() == pytest.approx([3000, 3001, 3002, 3003], nan_ok=True)


def test_recalibrate(data_file: Dataset):
    from forge.processing.editing.action import Recalibrate

    assert Action.from_code("Recalibrate") == Recalibrate

    a = Recalibrate(to_json({
        "selection": [{"variable_id": "N"}],
        "calibration": [5.0, 1.0],
        "reverse_calibration": [-1.0, 1.0],
    }))
    assert not a.needs_prepare
    assert not a.filter_data(data_file, data_file)
    assert a.filter_data(data_file, data_file.groups["data"])
    a.apply(data_file, data_file.groups["data"], slice(0, 2))
    assert data_file.groups["data"].variables["first_var"][:].data.tolist() == [16, 17, 12, 13]
    assert data_file.groups["data"].variables["second_var"][:].data.tolist() == [20, 21, 22, 23]

    a = Recalibrate(to_json({
        "selection": [{"variable_name": "scattering_coefficient", "wavelength": 200}],
        "calibration": [6.0, 1.0],
        "reverse_calibration": [-1.0, 1.0],
    }))
    assert not a.needs_prepare
    assert not a.filter_data(data_file, data_file)
    assert a.filter_data(data_file, data_file.groups["data"])
    a.apply(data_file, data_file.groups["data"], slice(3, 4))
    assert data_file.groups["data"].variables["first_var"][:].data.tolist() == [16, 17, 12, 13]
    assert data_file.groups["data"].variables["scattering_coefficient"][:].data[:, 0].tolist() == pytest.approx([1000, 1001, 1002, 1003], nan_ok=True)
    assert data_file.groups["data"].variables["scattering_coefficient"][:].data[:, 1].tolist() == pytest.approx([2000, 2001, 2002, 2010], nan_ok=True)
    assert data_file.groups["data"].variables["scattering_coefficient"][:].data[:, 2].tolist() == pytest.approx([3000, 3001, 3002, 3003], nan_ok=True)


def test_flow_correction(tmp_path):
    file = Dataset(str(tmp_path / "data.nc"), 'w', format='NETCDF4')
    file.instrument_id = "X1"

    g = file.createGroup("data")
    times = data_structure.time_coordinate(g)
    times[:] = [100, 200, 300, 400]

    var = data_structure.measured_variable(g, "scattering_coefficient")
    data_variable.variable_total_scattering(var)
    var.variable_id = "Bs"
    var[:] = [12, 16, 98, 99]

    var = data_structure.measured_variable(g, "sample_flow")
    data_variable.variable_sample_flow(var)
    var.variable_id = "Q"
    var[:] = [1, 2, 88, 89]

    from forge.processing.editing.action import FlowCorrection

    assert Action.from_code("FlowCorrection") == FlowCorrection

    a = FlowCorrection(to_json({
        "instrument": "X1",
        "calibration": [0.0, 2.0],
        "reverse_calibration": [0.0, 0.5],
    }))
    assert a.needs_prepare
    assert a.filter_data(file, file.groups["data"])
    a.prepare(file, file.groups["data"], file.groups["data"].variables["time"][:].data)
    a.apply(file, file.groups["data"], slice(0, 2))
    assert file.groups["data"].variables["sample_flow"][:].data.tolist() == [4, 8, 88, 89]
    assert file.groups["data"].variables["scattering_coefficient"][:].data.tolist() == [3, 4, 98, 99]


def test_flow_correction(tmp_path):
    file = Dataset(str(tmp_path / "data.nc"), 'w', format='NETCDF4')
    file.instrument_id = "X1"

    g = file.createGroup("data")
    times = data_structure.time_coordinate(g)
    times[:] = [100, 200, 300, 400]

    cut_size = data_structure.cutsize_variable(g)
    cut_size[:] = [1, 10, 2.5, 10]

    var = data_structure.measured_variable(g, "scattering_coefficient")
    data_variable.variable_total_scattering(var)
    var.ancillary_variables = "cut_size"
    var.variable_id = "Bs"
    var[:] = [10, 11, 12, 13]

    var = data_structure.measured_variable(g, "other_var")
    var[:] = [20, 21, 22, 23]

    from forge.processing.editing.action import SizeCutFix

    assert Action.from_code("SizeCutFix") == SizeCutFix

    a = SizeCutFix(to_json({
        "cutsize": 10,
        "modified_cutsize": 1,
    }))
    assert a.needs_prepare
    assert a.filter_data(file, file.groups["data"])
    a.prepare(file, file.groups["data"], file.groups["data"].variables["time"][:].data)
    a.apply(file, file.groups["data"], slice(0, 3))
    assert file.groups["data"].variables["cut_size"][:].data.tolist() == [1, 1, 2.5, 10]
    assert file.groups["data"].variables["scattering_coefficient"][:].data.tolist() == [10, 11, 12, 13]

    a = SizeCutFix(to_json({
        "cutsize": 1,
        "modified_cutsize": "invalidate",
    }))
    assert a.needs_prepare
    assert a.filter_data(file, file.groups["data"])
    a.prepare(file, file.groups["data"], file.groups["data"].variables["time"][:].data)
    a.apply(file, file.groups["data"], slice(1, 4))
    assert file.groups["data"].variables["cut_size"][:].data.tolist() == [1, 1, 2.5, 10]
    assert file.groups["data"].variables["scattering_coefficient"][:].data.tolist() == pytest.approx([10, nan, 12, 13], nan_ok=True)
    assert file.groups["data"].variables["other_var"][:].data.tolist() == [20, 21, 22, 23]
