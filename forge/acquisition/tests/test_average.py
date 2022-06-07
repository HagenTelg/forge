import typing
import pytest
from forge.acquisition import LayeredConfiguration
from forge.acquisition.average import AverageRecord


def test_basic():
    a = AverageRecord(LayeredConfiguration({'AVERAGE': 100}))
    v1 = a.variable()
    f1 = a.flag()

    v1(1.0)
    f1(False)
    result = a(100.0)
    assert result is None

    v1(2.0)
    result = a(150.0)
    assert result is None

    v1(3.0)
    result = a(200.0)
    assert result.start_time == 100.0
    assert result.end_time == 200.0
    assert result.total_seconds == 100.0
    assert result.sample_count == 2
    assert float(v1) == 2.5
    assert not bool(f1)

    v1(4.0)
    f1(True)
    result = a(275.0)
    assert result is None

    v1(5.0)
    f1(False)
    result = a(300.0)
    assert result.start_time == 200.0
    assert result.end_time == 300.0
    assert result.total_seconds == 100.0
    assert result.sample_count == 2
    assert float(v1) == 4.25
    assert bool(f1)

    v1(6.0)
    result = a(310.0)
    assert result is None

    result = a.complete(320.0)
    assert result.start_time == 300.0
    assert result.end_time == 400.0
    assert result.total_seconds == 20.0
    assert result.sample_count == 2
    assert v1.value == 6.0
    assert not f1.value


def test_long():
    a = AverageRecord(LayeredConfiguration({'AVERAGE': 100}))
    v1 = a.variable()

    result = a(100.0)
    assert result is None

    v1(2.0)
    result = a(250.0)
    assert result.start_time == 100.0
    assert result.end_time == 200.0
    assert result.total_seconds == 100.0
    assert float(v1) == 2.0

    v1(3.0)
    result = a(400.0)
    assert result.start_time == 200.0
    assert result.end_time == 400.0
    assert result.total_seconds == 200.0
    assert float(v1) == 2.75

    v1(4.0)
    result = a(720.0)
    assert result.start_time == 400.0
    assert result.end_time == 700.0
    assert result.total_seconds == 300.0
    assert float(v1) == 4.0


def test_disable():
    a = AverageRecord(LayeredConfiguration({'AVERAGE': 100}))
    v1 = a.variable()
    f1 = a.flag()

    result = a(100.0)
    assert result is None

    v1(2.0)
    f1(False)
    result = a(150.0)
    assert result is None

    a.set_averaging(False)
    v1(3.0)
    f1(True)
    result = a(200.0)
    assert result.start_time == 100.0
    assert result.end_time == 200.0
    assert result.total_seconds == 50.0
    assert float(v1) == 2.0
    assert not bool(f1)

    a.set_averaging(True)
    a.start_flush(49.0, now=200.0)
    v1(4.0)
    f1(True)
    result = a(230.0)
    assert result is None

    result = a(250.0)
    assert result is None

    v1(5.0)
    result = a(300.0)
    assert result.start_time == 200.0
    assert result.end_time == 300.0
    assert result.total_seconds == 50.0
    assert float(v1) == 5.0
    assert not bool(f1)

    v1(6.0)
    f1(True)
    result = a(325.0)
    assert result is None

    a.reset()
    v1(7.0)
    result = a(350.0)
    assert result is None

    v1(8.0)
    result = a(400.0)
    assert result.start_time == 300.0
    assert result.end_time == 400.0
    assert result.total_seconds == 50.0
    assert float(v1) == 8.0
    assert not bool(f1)


def test_types():
    a = AverageRecord(LayeredConfiguration({'AVERAGE': 100}))
    first_valid = a.first_valid()
    last_valid = a.last_valid()
    vector = a.vector()
    arr = a.array()

    result = a(100.0)
    assert result is None

    first_valid(1.0)
    last_valid(2.0)
    vector(20.0, 30.0)
    arr([3.0, 4.0, 5.0])
    result = a(150.0)
    assert result is None

    first_valid(6.0)
    last_valid(7.0)
    vector(30.0, 30.0)
    arr([4.0, 5.0, 6.0])
    result = a(200.0)
    assert result.start_time == 100.0
    assert result.end_time == 200.0
    assert result.total_seconds == 100.0
    assert float(first_valid) == 1.0
    assert float(last_valid) == 7.0
    assert abs(vector.magnitude - 25.0) < 1e-6
    assert abs(vector.direction - 30.0) < 1e-6
    assert arr.value == [3.5, 4.5, 5.5]
