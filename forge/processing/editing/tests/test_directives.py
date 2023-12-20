import pytest
import numpy as np
from json import dumps as to_json
from math import nan
from netCDF4 import Dataset
from forge.processing.editing.directives import apply_edit_directives
import forge.data.structure.timeseries as data_structure
import forge.data.structure.variable as data_variable
import forge.data.structure.editdirectives as edits


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

    var = data_structure.measured_variable(g, "scattering_coefficient")
    var[:] = [20, 21, 22, 23]

    var = data_structure.measured_variable(g, "sample_flow")
    data_variable.variable_sample_flow(var)
    var.variable_id = "Q"
    var[:] = [1, 2, 3, 4]

    flags = g.createVariable("system_flags", np.uint64, ("time",), fill_value=False)
    data_variable.variable_flags(flags, {0x01: "flag1"})
    flags[:] = [0x01, 0, 0, 0x01]

    return file


def test_basic(data_file: Dataset, tmp_path):
    edit_file = Dataset(str(tmp_path / "edits.nc"), 'w', format='NETCDF4')
    g = edit_file.createGroup("edits")

    edit_start, edit_end = edits.edit_bounds(g)
    edit_start[:] = [100, 50]
    edit_end[:] = [210, 500]
    edit_deleted = edits.edit_deleted(g)
    edit_deleted[:] = [0, 1]
    edit_profile = edits.edit_profile(g, ["aerosol"])
    edit_profile[:] = [0, 0]
    edit_action = edits.edit_action_type(g)
    edit_action[:] = [0, 0]
    edit_action_parameters = edits.edit_action_parameters(g)
    edit_action_parameters[0] = to_json({
        "selection": [{"variable_name": "first_var"}]
    })
    edit_action_parameters[1] = to_json({
        "selection": [{"variable_name": "scattering_coefficient"}]
    })
    edit_condition = edits.edit_condition_type(g)
    edit_condition[:] = [0, 0]
    edit_condition_parameters = edits.edit_condition_parameters(g)
    edit_condition_parameters[0] = ""
    edit_condition_parameters[1] = ""

    apply_edit_directives(edit_file, [data_file])

    assert data_file.groups["data"].variables["first_var"][:].data.tolist() == pytest.approx([nan, nan, 12, 13], nan_ok=True)
    assert data_file.groups["data"].variables["scattering_coefficient"][:].data.tolist() == [20, 21, 22, 23]


def test_condition(data_file: Dataset, tmp_path):
    edit_file = Dataset(str(tmp_path / "edits.nc"), 'w', format='NETCDF4')
    g = edit_file.createGroup("edits")

    edit_start, edit_end = edits.edit_bounds(g)
    edit_start[:] = [100, ]
    edit_end[:] = [500, ]
    edit_deleted = edits.edit_deleted(g)
    edit_deleted[:] = [0, ]
    edit_profile = edits.edit_profile(g, ["aerosol"])
    edit_profile[:] = [0, ]
    edit_action = edits.edit_action_type(g)
    edit_action[:] = [1, ]
    edit_action_parameters = edits.edit_action_parameters(g)
    edit_action_parameters[0] = ""
    edit_condition = edits.edit_condition_type(g)
    edit_condition[:] = [1, ]
    edit_condition_parameters = edits.edit_condition_parameters(g)
    edit_condition_parameters[0] = to_json({
        "selection": [{"variable_name": "first_var"}],
        "lower": 10.5,
        "upper": 12.5,
    })

    apply_edit_directives(edit_file, [data_file])

    assert data_file.groups["data"].variables["first_var"][:].data.tolist() == pytest.approx([10, 11, 12, 13], nan_ok=True)
    assert data_file.groups["data"].variables["system_flags"][:].data.tolist() == [0x01, 0x02, 0x02, 0x01]


def test_action_prepare(data_file: Dataset, tmp_path):
    edit_file = Dataset(str(tmp_path / "edits.nc"), 'w', format='NETCDF4')
    g = edit_file.createGroup("edits")

    edit_start, edit_end = edits.edit_bounds(g)
    edit_start[:] = [100, ]
    edit_end[:] = [500, ]
    edit_deleted = edits.edit_deleted(g)
    edit_deleted[:] = [0, ]
    edit_profile = edits.edit_profile(g, ["aerosol"])
    edit_profile[:] = [0, ]
    edit_action = edits.edit_action_type(g)
    edit_action[:] = [4, ]
    edit_action_parameters = edits.edit_action_parameters(g)
    edit_action_parameters[0] = to_json({
        "instrument": "X1",
        "calibration": [0, 1],
        "reverse_calibration": [0, 2],
    })
    edit_condition = edits.edit_condition_type(g)
    edit_condition[:] = [0, ]
    edit_condition_parameters = edits.edit_condition_parameters(g)
    edit_condition_parameters[0] = ""

    apply_edit_directives(edit_file, [data_file])

    assert data_file.groups["data"].variables["first_var"][:].data.tolist() == pytest.approx([10, 11, 12, 13], nan_ok=True)
    assert data_file.groups["data"].variables["sample_flow"][:].data.tolist() == [1/2, 2/2, 3/2, 4/2]
    assert data_file.groups["data"].variables["scattering_coefficient"][:].data.tolist() == [20*2, 21*2, 22*2, 23*2]
