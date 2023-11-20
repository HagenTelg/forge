import pytest
import numpy as np
from pathlib import Path
from netCDF4 import Dataset, Group
from forge.data.structure import instrument_timeseries
from forge.data.structure.timeseries import time_coordinate
from forge.processing.editing.run import EditingAvailableDay

_TIME_20230501 = 1682899200
_TIME_20230502 = 1682985600


@pytest.fixture
def first_data(tmp_path):
    file = Dataset(str(tmp_path / "first.nc"), 'w', format='NETCDF4')
    instrument_timeseries(
        file, "NIL", "X1",
        _TIME_20230501, _TIME_20230502,
        60, {"tag1", "tag2"}
    )
    data_group: Group = file.createGroup("data")
    time_var = time_coordinate(data_group)
    time_var[:] = [_TIME_20230501 * 1000, (_TIME_20230501+60) * 1000, (_TIME_20230501+120) * 1000]
    return file


@pytest.fixture
def second_data(tmp_path):
    file = Dataset(str(tmp_path / "second.nc"), 'w', format='NETCDF4')
    instrument_timeseries(
        file, "NIL", "X2",
        _TIME_20230501, _TIME_20230502,
        60, {"tag2", "tag3"}
    )
    data_group: Group = file.createGroup("data")
    time_var = time_coordinate(data_group)
    time_var[:] = [_TIME_20230501 * 1000, (_TIME_20230501 + 60) * 1000, (_TIME_20230501 + 120) * 1000]
    return file


@pytest.fixture
def third_data(tmp_path):
    file = Dataset(str(tmp_path / "third.nc"), 'w', format='NETCDF4')
    instrument_timeseries(
        file, "NIL", "X3",
        _TIME_20230501, _TIME_20230502,
        60, {"tag2", "tag4"}
    )
    data_group: Group = file.createGroup("data")
    time_var = time_coordinate(data_group)
    time_var[:] = [_TIME_20230501 * 1000, (_TIME_20230501 + 60) * 1000, (_TIME_20230501 + 120) * 1000]
    return file


@pytest.fixture
def available(first_data, second_data, third_data, tmp_path):
    return EditingAvailableDay("NIL", tmp_path, _TIME_20230501, [first_data, second_data, third_data])


def test_basic_select(available: EditingAvailableDay):
    count = 0
    for check in available.select_instrument({"instrument_id": "X1"}):
        assert not check.placeholder
        assert check.root.instrument_id == "X1"
        count += 1
    assert count == 1

    count = 0
    for (check,) in available.select_instrument({"instrument_id": "X1"}, always_tuple=True):
        assert not check.placeholder
        assert check.root.instrument_id == "X1"
        count += 1
    assert count == 1

    for _ in available.select_instrument({"instrument_id": "X5"}):
        assert False

    count = 0
    for check in available.select_instrument([
        {"instrument_id": "X1"},
        {"instrument_id": "X2"},
    ]):
        assert not check.placeholder
        if check.root.instrument_id == "X1":
            assert (count & 0b01) == 0
            count |= 0b01
        elif check.root.instrument_id == "X2":
            assert (count & 0b10) == 0
            count |= 0b10
        else:
            assert False
    assert count == 0b11

    count = 0
    for check in available.select_instrument({"instrument_id": "X3"}, start=_TIME_20230501):
        assert not check.placeholder
        assert check.root.instrument_id == "X3"
        count += 1
    assert count == 1

    for _ in available.select_instrument({"tags": "tag2"}, end="2023-05-01"):
        assert False


def test_aux_select(available: EditingAvailableDay):
    count = 0
    for first, second in available.select_instrument(
            {"instrument_id": "X1"},
            {"instrument_id": "X2"},
    ):
        assert not first.placeholder
        assert first.root.instrument_id == "X1"
        assert not second.placeholder
        assert second.root.instrument_id == "X2"
        count += 1
    assert count == 1

    count = 0
    for first, second in available.select_instrument(
            {"instrument_id": "X1"},
            {"instrument_id": "X5"},
    ):
        assert not first.placeholder
        assert first.root.instrument_id == "X1"
        assert second.placeholder
        count += 1
    assert count == 1

    count = 0
    for first, second1, second2 in available.select_instrument(
            {"instrument_id": "X1"},
            {"instrument_id": "X2"},
            {"instrument_id": "X2"},
    ):
        assert not first.placeholder
        assert first.root.instrument_id == "X1"
        assert not second1.placeholder
        assert second1.root.instrument_id == "X2"
        assert not second2.placeholder
        assert second2.root.instrument_id == "X2"
        count += 1
    assert count == 1

    count = 0
    for first, second, third in available.select_instrument(
            {"instrument_id": "X1"},
            {"instrument_id": "X2"},
            {"tags": "tag4"},
    ):
        assert not first.placeholder
        assert first.root.instrument_id == "X1"
        assert not second.placeholder
        assert second.root.instrument_id == "X2"
        assert not third.placeholder
        assert third.root.instrument_id == "X3"
        count += 1
    assert count == 1


def test_multiple_select(available: EditingAvailableDay):
    count = 0
    for check in available.select_multiple({"instrument_id": "X1"}):
        assert not check.placeholder
        assert check.root.instrument_id == "X1"
        count += 1
    assert count == 1

    count = 0
    for (check,) in available.select_multiple({"instrument_id": "X1"}, always_tuple=True):
        assert not check.placeholder
        assert check.root.instrument_id == "X1"
        count += 1
    assert count == 1

    count = 0
    for check in available.select_multiple({"instrument_id": "X5"}):
        assert check.placeholder
        count += 1
    assert count == 1

    count = 0
    for check in available.select_multiple({"instrument_id": "X1"}, start="2023-05-01", end="2023-05-02"):
        assert not check.placeholder
        assert check.root.instrument_id == "X1"
        count += 1
    assert count == 1

    for _ in available.select_multiple({"instrument_id": "X1"}, end=_TIME_20230501):
        assert False

    count = 0
    for first, second in available.select_multiple({"instrument_id": "X2"}, {"instrument_id": "X1"}):
        assert not first.placeholder
        assert first.root.instrument_id == "X2"
        assert not second.placeholder
        assert second.root.instrument_id == "X1"
        count += 1
    assert count == 1

    count = 0
    for first, second in available.select_multiple({"instrument_id": "X1"}, {"instrument_id": "X1"}):
        assert not first.placeholder
        assert first.root.instrument_id == "X1"
        assert not second.placeholder
        assert second.root.instrument_id == "X1"
        count += 1
    assert count == 1

    count = 0
    for first, second in available.select_multiple({"instrument_id": "X2"}, {"instrument_id": "X5"}):
        assert not first.placeholder
        assert first.root.instrument_id == "X2"
        assert second.placeholder
        count += 1
    assert count == 1

    count = 0
    for first, second in available.select_multiple({"instrument_id": "X5"}, {"instrument_id": "X3"}):
        assert first.placeholder
        assert not second.placeholder
        assert second.root.instrument_id == "X3"
        count += 1
    assert count == 1

    count = 0
    for first, second in available.select_multiple([
        {"instrument_id": "X1"},
        {"instrument_id": "X3"},
    ], {"instrument_id": "X2"}):
        assert not first.placeholder
        assert first.root.instrument_id in ("X1", "X3")
        assert not second.placeholder
        assert second.root.instrument_id == "X2"
        count += 1
    assert count == 1


def test_output(available: EditingAvailableDay, tmp_path):
    count = 0
    for out, first in available.derive_output("X4", {"instrument_id": "X1"}):
        assert not first.placeholder
        assert first.root.instrument_id == "X1"

        assert out.root.instrument_id == "X4"
        assert Path(out.root.filepath()) == (tmp_path / "NIL-X4_s20230501.nc")

        data_group = out.root.groups["data"]
        times = data_group.variables["time"]
        assert np.all(times[:] == np.arange(_TIME_20230501 * 1000, _TIME_20230502 * 1000, 60 * 1000, dtype=np.int64))

        count += 1
    assert count == 1

    check = Dataset(str(tmp_path / "NIL-X4_s20230501.nc"), 'r')
    assert check.instrument_id == "X4"
    count = 0
    for check in available.select_instrument({"instrument_id": "X4"}):
        assert not check.placeholder
        assert check.root.instrument_id == "X4"
        count += 1
    assert count == 1

    for _, _ in available.derive_output("X99", {"instrument_id": "X5"}):
        assert False

    for out, second in available.derive_output("X6", {"instrument_id": "X2"}, peer_times=True):
        assert not second.placeholder
        assert second.root.instrument_id == "X2"

        assert out.root.instrument_id == "X6"
        assert Path(out.root.filepath()) == (tmp_path / "NIL-X6_s20230501.nc")

        data_group = out.root.groups["data"]
        times = data_group.variables["time"]
        assert np.all(times[:] == second.times)

    check = Dataset(str(tmp_path / "NIL-X6_s20230501.nc"), 'r')
    assert check.instrument_id == "X6"
    count = 0
    for check in available.select_instrument({"instrument_id": "X6"}):
        assert not check.placeholder
        assert check.root.instrument_id == "X6"
        count += 1
    assert count == 1
