import pytest
from netCDF4 import Dataset
import forge.data.structure.timeseries as data_structure
import forge.data.structure.variable as data_variable
from forge.processing.editing.selection import Selection


@pytest.fixture
def data_file(tmp_path):
    file = Dataset(str(tmp_path / "data.nc"), 'w', format='NETCDF4')
    file.instrument_id = "X1"
    file.forge_tags = "tag1 tag2"
    file.instrument = "testint"

    g = file.createGroup("data")
    times = data_structure.time_coordinate(g)
    data_structure.averaged_time_variable(g)
    data_structure.averaged_count_variable(g)
    data_structure.cutsize_variable(g)

    times[:] = [100, 200, 300, 400]

    var = data_structure.measured_variable(g, "first_var")
    var.variable_id = "N"
    var[:] = [10, 11, 12, 13]

    var = data_structure.measured_variable(g, "second_var", standard_name="standard_name")
    var.variable_id = "Q_X1"
    var[:] = [20, 21, 22, 23]

    g.createDimension("wavelength", 3)
    wl = g.createVariable("wavelength", "f8", ("wavelength", ))
    data_variable.variable_wavelength(wl)
    wl[:] = [100, 200, 300]

    var = data_structure.measured_variable(g, "scattering_coefficient", dimensions=["wavelength"])
    data_variable.variable_total_scattering(var)
    var.variable_id = "Bs"
    var[:, 0] = [1000, 1001, 1002, 1003]
    var[:, 1] = [2000, 2001, 2002, 2003]
    var[:, 2] = [3000, 3001, 3002, 3003]

    g.createDimension("angle", 2)
    var = data_structure.measured_variable(g, "polar_scattering_coefficient", dimensions=["angle", "wavelength"])
    var[:, 0, 0] = [10000, 10001, 10002, 10003]
    var[:, 1, 0] = [10010, 10011, 10012, 10013]
    var[:, 0, 1] = [20000, 20001, 20002, 20003]
    var[:, 1, 1] = [20010, 20011, 20012, 20013]
    var[:, 0, 2] = [30000, 30001, 30002, 30003]
    var[:, 1, 2] = [30010, 30011, 30012, 30013]

    return file


def test_filter_data(data_file: Dataset):
    sel = Selection([])
    assert not sel.filter_data(data_file, data_file)
    assert not sel.filter_data(data_file, data_file.groups["data"])

    sel = Selection([{"variable_id": "N"}])
    assert not sel.filter_data(data_file, data_file)
    assert sel.filter_data(data_file, data_file.groups["data"])
    sel = Selection([{"variable_id": "Q"}])
    assert not sel.filter_data(data_file, data_file)
    assert sel.filter_data(data_file, data_file.groups["data"])
    sel = Selection([{"variable_id": "Q"}, {"variable_id": "N"}])
    assert not sel.filter_data(data_file, data_file)
    assert sel.filter_data(data_file, data_file.groups["data"])
    sel = Selection([{"variable_id": "Q1"}, {"variable_id": "N"}])
    assert not sel.filter_data(data_file, data_file)
    assert sel.filter_data(data_file, data_file.groups["data"])
    sel = Selection([{"variable_id": "N_X1"}])
    assert not sel.filter_data(data_file, data_file)
    assert sel.filter_data(data_file, data_file.groups["data"])
    sel = Selection([{"variable_id": "Q1_X1"}])
    assert not sel.filter_data(data_file, data_file)
    assert not sel.filter_data(data_file, data_file.groups["data"])
    sel = Selection([{"variable_id": "N1"}])
    assert not sel.filter_data(data_file, data_file)
    assert not sel.filter_data(data_file, data_file.groups["data"])
    sel = Selection([{"variable_id": "N1_X1"}])
    assert not sel.filter_data(data_file, data_file)
    assert not sel.filter_data(data_file, data_file.groups["data"])

    sel = Selection([{"variable_name": "second_var"}])
    assert not sel.filter_data(data_file, data_file)
    assert sel.filter_data(data_file, data_file.groups["data"])
    sel = Selection([{"variable_name": "missing_var"}])
    assert not sel.filter_data(data_file, data_file)
    assert not sel.filter_data(data_file, data_file.groups["data"])

    sel = Selection([{"standard_name": "standard_name"}])
    assert not sel.filter_data(data_file, data_file)
    assert sel.filter_data(data_file, data_file.groups["data"])
    sel = Selection([{"standard_name": "missing_name"}])
    assert not sel.filter_data(data_file, data_file)
    assert not sel.filter_data(data_file, data_file.groups["data"])

    sel = Selection([{"variable_name": "scattering_coefficient", "wavelength": 100}])
    assert not sel.filter_data(data_file, data_file)
    assert sel.filter_data(data_file, data_file.groups["data"])
    sel = Selection([{"variable_name": "scattering_coefficient", "wavelength": 110}])
    assert not sel.filter_data(data_file, data_file)
    assert not sel.filter_data(data_file, data_file.groups["data"])

    sel = Selection([{"instrument_id": "X1", "variable_id": "N"}])
    assert not sel.filter_data(data_file, data_file)
    assert sel.filter_data(data_file, data_file.groups["data"])
    sel = Selection([{"instrument_id": "X2", "variable_id": "N"}])
    assert not sel.filter_data(data_file, data_file)
    assert not sel.filter_data(data_file, data_file.groups["data"])

    sel = Selection([{"instrument": "testint", "variable_id": "N"}])
    assert not sel.filter_data(data_file, data_file)
    assert sel.filter_data(data_file, data_file.groups["data"])
    sel = Selection([{"instrument": "missinginst", "variable_id": "N"}])
    assert not sel.filter_data(data_file, data_file)
    assert not sel.filter_data(data_file, data_file.groups["data"])

    sel = Selection([{"require_tags": ["tag1"], "exclude_tags": ["tag3"], "variable_id": "N"}])
    assert not sel.filter_data(data_file, data_file)
    assert sel.filter_data(data_file, data_file.groups["data"])
    sel = Selection([{"require_tags": ["tag3"], "exclude_tags": ["tag1"], "variable_id": "N"}])
    assert not sel.filter_data(data_file, data_file)
    assert not sel.filter_data(data_file, data_file.groups["data"])


def _empty_iterator(i):
    for _ in i:
        return False
    return True


def test_select_data(data_file: Dataset):
    sel = Selection([])
    assert _empty_iterator(sel.select_data(data_file, data_file))
    assert _empty_iterator(sel.select_data(data_file, data_file.groups["data"]))

    sel = Selection([{"variable_id": "N"}])
    assert _empty_iterator(sel.select_data(data_file, data_file))
    count = 0
    for var, hit in sel.select_data(data_file, data_file.groups["data"]):
        assert count == 0
        count += 1
        assert var.name == "first_var"
        assert len(hit) == 0
        assert var[(slice(None), *hit)].data.tolist() == [10, 11, 12, 13]
    assert count == 1

    sel = Selection([{"variable_id": "Bs"}])
    assert _empty_iterator(sel.select_data(data_file, data_file))
    count = 0
    for var, hit in sel.select_data(data_file, data_file.groups["data"]):
        assert count == 0
        count += 1
        assert var.name == "scattering_coefficient"
        assert var[(slice(None), *hit)].data.tolist() == [
            [1000, 2000, 3000],
            [1001, 2001, 3001],
            [1002, 2002, 3002],
            [1003, 2003, 3003],
        ]
    assert count == 1

    sel = Selection([
        {"variable_name": "scattering_coefficient", "wavelength": 100},
        {"variable_name": "scattering_coefficient", "wavelength": 200},
    ])
    assert _empty_iterator(sel.select_data(data_file, data_file))
    count = 0
    for var, hit in sel.select_data(data_file, data_file.groups["data"]):
        assert count == 0
        count += 1
        assert var.name == "scattering_coefficient"
        assert var[(slice(None), *hit)].data.tolist() == [
            [1000, 2000],
            [1001, 2001],
            [1002, 2002],
            [1003, 2003],
        ]
    assert count == 1

    sel = Selection([
        {"variable_name": "polar_scattering_coefficient", "wavelength": 100},
        {"variable_name": "polar_scattering_coefficient", "wavelength": 300},
    ])
    assert _empty_iterator(sel.select_data(data_file, data_file))
    count = 0
    for var, hit in sel.select_data(data_file, data_file.groups["data"]):
        assert count == 0
        count += 1
        assert var.name == "polar_scattering_coefficient"
        assert var[(slice(None), *hit)].data.tolist() == [
            [[10000, 30000], [10010, 30010]],
            [[10001, 30001], [10011, 30011]],
            [[10002, 30002], [10012, 30012]],
            [[10003, 30003], [10013, 30013]],
        ]
    assert count == 1


def test_select_single(data_file: Dataset):
    sel = Selection([])
    assert _empty_iterator(sel.select_single(data_file, data_file))
    assert _empty_iterator(sel.select_single(data_file, data_file.groups["data"]))

    sel = Selection([{"variable_id": "N"}])
    assert _empty_iterator(sel.select_single(data_file, data_file))
    count = 0
    for var, hit in sel.select_single(data_file, data_file.groups["data"]):
        assert count == 0
        count += 1
        assert var.name == "first_var"
        assert len(hit) == 0
        assert var[(slice(None), *hit)].data.tolist() == [10, 11, 12, 13]
    assert count == 1

    sel = Selection([{"variable_id": "Bs"}])
    assert _empty_iterator(sel.select_single(data_file, data_file))
    count = 0
    for var, hit in sel.select_single(data_file, data_file.groups["data"]):
        assert var.name == "scattering_coefficient"
        values = var[(slice(None), *hit)].data.tolist()
        if values == [1000, 1001, 1002, 1003]:
            assert count & 0b001 == 0
            count |= 0b001
        elif values == [2000, 2001, 2002, 2003]:
            assert count & 0b010 == 0
            count |= 0b010
        elif values == [3000, 3001, 3002, 3003]:
            assert count & 0b100 == 0
            count |= 0b100
        else:
            assert False
    assert count == 0b111

    sel = Selection([
        {"variable_name": "scattering_coefficient", "wavelength": 100},
        {"variable_name": "scattering_coefficient", "wavelength": 200},
    ])
    assert _empty_iterator(sel.select_single(data_file, data_file))
    count = 0
    for var, hit in sel.select_single(data_file, data_file.groups["data"]):
        assert var.name == "scattering_coefficient"
        values = var[(slice(None), *hit)].data.tolist()
        if values == [1000, 1001, 1002, 1003]:
            assert count & 0b01 == 0
            count |= 0b01
        elif values == [2000, 2001, 2002, 2003]:
            assert count & 0b10 == 0
            count |= 0b10
        else:
            assert False
    assert count == 0b11

    sel = Selection([
        {"variable_name": "polar_scattering_coefficient", "wavelength": 100},
        {"variable_name": "polar_scattering_coefficient", "wavelength": 300},
    ])
    assert _empty_iterator(sel.select_single(data_file, data_file))
    count = 0
    for var, hit in sel.select_single(data_file, data_file.groups["data"]):
        assert var.name == "polar_scattering_coefficient"
        values = var[(slice(None), *hit)].data.tolist()
        if values == [10000, 10001, 10002, 10003]:
            assert count & 0b0001 == 0
            count |= 0b0001
        elif values == [10010, 10011, 10012, 10013]:
            assert count & 0b0010 == 0
            count |= 0b0010
        elif values == [30000, 30001, 30002, 30003]:
            assert count & 0b0100 == 0
            count |= 0b0100
        elif values == [30010, 30011, 30012, 30013]:
            assert count & 0b1000 == 0
            count |= 0b1000
        else:
            assert False
    assert count == 0b1111
