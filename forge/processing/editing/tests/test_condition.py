import pytest
import numpy as np
from json import dumps as to_json
from netCDF4 import Dataset
from forge.processing.editing.condition import Condition


@pytest.fixture
def data_file(tmp_path):
    import forge.data.structure.timeseries as data_structure

    file = Dataset(str(tmp_path / "data.nc"), 'w', format='NETCDF4')
    file.instrument_id = "X1"
    file.forge_tags = "tag1 tag2"
    file.instrument = "testint"

    g = file.createGroup("data")
    times = data_structure.time_coordinate(g)

    times[:] = [100, 200, 300, 401]

    var = data_structure.measured_variable(g, "first_var")
    var.variable_id = "N"
    var[:] = [10, 11, 12, 13]

    var = data_structure.measured_variable(g, "second_var")
    var[:] = [10, 11, 16, 10]

    return file


def test_always():
    from forge.processing.editing.condition import Always

    assert Condition.from_code("None") == Always

    c = Always("")
    assert not c.needs_prepare

    times = np.array([100, 200, 300, 400, 500])
    assert times[c.evaluate(times, 100, 600)].tolist() == [100, 200, 300, 400, 500]
    assert c.evaluate(times, 50, 60) is None
    assert c.evaluate(times, 510, 600) is None
    assert c.evaluate(times, 210, 220) is None
    assert c.evaluate(times, 380, 400) is None
    assert times[c.evaluate(times, 100, 200)].tolist() == [100]
    assert times[c.evaluate(times, 500, 600)].tolist() == [500]
    assert times[c.evaluate(times, 100, 210)].tolist() == [100, 200]
    assert times[c.evaluate(times, 110, 210)].tolist() == [200]
    assert times[c.evaluate(times, 110, 499)].tolist() == [200, 300, 400]


def test_periodic():
    from forge.processing.editing.condition import Periodic

    assert Condition.from_code("Periodic") == Periodic

    c = Periodic(to_json({
        "interval": "hour",
        "division": "minute",
        "moments": [0, 1, 2, 58, 59]
    }))
    assert not c.needs_prepare
    times = np.arange(2 * 60) * 60 * 1000
    assert c.evaluate(times, 3 * 60 * 60 * 1000, 4 * 60 * 60 * 1000) is None
    assert times[c.evaluate(times, 0, 1 * 60 * 60 * 1000)].tolist() == [
        0 * 60 * 1000,
        1 * 60 * 1000,
        2 * 60 * 1000,
        58 * 60 * 1000,
        59 * 60 * 1000,
    ]
    assert times[c.evaluate(times, 0, 2 * 60 * 60 * 1000)].tolist() == [
        0 * 60 * 1000,
        1 * 60 * 1000,
        2 * 60 * 1000,
        58 * 60 * 1000,
        59 * 60 * 1000,

        0 * 60 * 1000 + 60 * 60 * 1000,
        1 * 60 * 1000 + 60 * 60 * 1000,
        2 * 60 * 1000 + 60 * 60 * 1000,
        58 * 60 * 1000 + 60 * 60 * 1000,
        59 * 60 * 1000 + 60 * 60 * 1000,
    ]

    c = Periodic(to_json({
        "interval": "day",
        "division": "hour",
        "moments": [2, 3, 4]
    }))
    assert not c.needs_prepare
    times = np.arange(2 * 24) * 60 * 60 * 1000
    assert times[c.evaluate(times, 0, 1 * 24 * 60 * 60 * 1000)].tolist() == [
        2 * 60 * 60 * 1000,
        3 * 60 * 60 * 1000,
        4 * 60 * 60 * 1000,
    ]
    assert times[c.evaluate(times, 0, 2 * 24 * 60 * 60 * 1000)].tolist() == [
        2 * 60 * 60 * 1000,
        3 * 60 * 60 * 1000,
        4 * 60 * 60 * 1000,

        2 * 60 * 60 * 1000 + 24 * 60 * 60 * 1000,
        3 * 60 * 60 * 1000 + 24 * 60 * 60 * 1000,
        4 * 60 * 60 * 1000 + 24 * 60 * 60 * 1000,
    ]


def test_threshold(data_file: Dataset):
    from forge.processing.editing.condition import Threshold

    assert Condition.from_code("Threshold") == Threshold

    c = Threshold(to_json({
        "selection": [{"variable_id": "N"}],
        "lower": 9.0,
        "upper": 14.0,
    }))
    assert c.needs_prepare
    c.prepare(data_file, data_file.groups["data"])
    times = np.array([100, 200, 300, 400, 500])
    assert c.evaluate(times, 90, 100) is None
    assert c.evaluate(times, 600, 700) is None
    assert times[c.evaluate(times, 100, 600)].tolist() == [100, 200, 300, 400, 500]
    assert times[c.evaluate(times, 100, 400)].tolist() == [100, 200, 300]
    assert times[c.evaluate(times, 10, 400)].tolist() == [100, 200, 300]
    times = np.array([100, 200, 300])
    assert times[c.evaluate(times, 100, 400)].tolist() == [100, 200, 300]
    times = np.array([200, 300, 401])
    assert times[c.evaluate(times, 200, 500)].tolist() == [200, 300, 401]

    c = Threshold(to_json({
        "selection": [{"variable_id": "N"}],
        "lower": 10.5,
    }))
    assert c.needs_prepare
    c.prepare(data_file, data_file.groups["data"])
    times = np.array([100, 200, 300, 400, 500])
    assert times[c.evaluate(times, 100, 600)].tolist() == [200, 300, 400, 500]
    assert c.evaluate(times, 100, 200) is None

    c = Threshold(to_json({
        "selection": [{"variable_id": "N"}],
        "upper": 12.5,
    }))
    assert c.needs_prepare
    c.prepare(data_file, data_file.groups["data"])
    times = np.array([100, 200, 300, 400, 500])
    assert times[c.evaluate(times, 100, 600)].tolist() == [100, 200, 300, 400]

    c = Threshold(to_json({
        "selection": [{"variable_name": "second_var"}],
        "lower": 10.5,
        "upper": 11.5,
    }))
    assert c.needs_prepare
    c.prepare(data_file, data_file.groups["data"])
    times = np.array([100, 200, 300, 401, 500])
    assert times[c.evaluate(times, 100, 600)].tolist() == [200]

    c = Threshold(to_json({
        "selection": [{"variable_name": "second_var"}],
        "upper": 11.5,
    }))
    assert c.needs_prepare
    c.prepare(data_file, data_file.groups["data"])
    times = np.array([100, 200, 300, 400, 500])
    assert times[c.evaluate(times, 100, 600)].tolist() == [100, 200, 500]
    times = np.array([100, 200, 300, 401, 500])
    assert times[c.evaluate(times, 100, 600)].tolist() == [100, 200, 401, 500]

    c = Threshold(to_json({
        "selection": [{"variable_id": "N"}, {"variable_name": "second_var"}],
        "upper": 12.5,
    }))
    assert c.needs_prepare
    c.prepare(data_file, data_file.groups["data"])
    times = np.array([100, 200, 300, 400, 500])
    assert times[c.evaluate(times, 100, 600)].tolist() == [100, 200, 300, 400, 500]
