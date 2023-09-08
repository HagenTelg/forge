import typing
import pytest
from forge.range import intersects, subtract_tuple


def test_intersects():
    assert intersects(100, 200, 125, 175)
    assert intersects(125, 175, 100, 200)
    assert intersects(100, 200, 50, 150)
    assert intersects(50, 150, 100, 200)
    assert intersects(100, 200, 150, 250)
    assert intersects(150, 250, 100, 200)
    assert intersects(100, 200, 125, 200)
    assert intersects(125, 200, 100, 200)
    assert intersects(100, 200, 100, 125)
    assert intersects(100, 125, 100, 200)
    assert not intersects(100, 200, 200, 300)
    assert not intersects(200, 300, 100, 200)
    assert not intersects(100, 200, 300, 400)
    assert not intersects(300, 400, 100, 200)
    assert not intersects(100, 200, 50, 100)
    assert not intersects(50, 100, 100, 200)
    assert not intersects(100, 200, 50, 75)
    assert not intersects(50, 75, 100, 200)


def test_subtract():
    def sub(existing, start, end):
        subtract_tuple(existing, start, end)
        return existing

    assert sub([], 100, 200) == []
    assert sub([(100, 200)], 100, 200) == []
    assert sub([(100, 200)], 50, 300) == []
    assert sub([(100, 200)], 200, 300) == [(100, 200)]
    assert sub([(100, 200)], 300, 400) == [(100, 200)]
    assert sub([(100, 200)], 50, 75) == [(100, 200)]
    assert sub([(100, 200)], 50, 100) == [(100, 200)]
    assert sub([(100, 200), (200, 300)], 300, 400) == [(100, 200), (200, 300)]
    assert sub([(100, 200)], 150, 200) == [(100, 150)]
    assert sub([(100, 200)], 100, 150) == [(150, 200)]
    assert sub([(100, 200)], 150, 175) == [(100, 150), (175, 200)]

    assert sub([(100, 200), (200, 300)], 100, 300) == []
    assert sub([(100, 200), (200, 300)], 50, 400) == []
    assert sub([(100, 200), (225, 300)], 50, 400) == []

    assert sub([(100, 200), (200, 300)], 100, 200) == [(200, 300)]
    assert sub([(100, 200), (200, 300)], 50, 250) == [(250, 300)]
    assert sub([(100, 200), (225, 300)], 100, 200) == [(225, 300)]
    assert sub([(100, 200), (225, 300)], 50, 250) == [(250, 300)]

    assert sub([(100, 200), (200, 300)], 200, 300) == [(100, 200)]
    assert sub([(100, 200), (200, 300)], 150, 400) == [(100, 150)]
    assert sub([(100, 175), (200, 300)], 200, 300) == [(100, 175)]
    assert sub([(100, 175), (200, 300)], 150, 300) == [(100, 150)]

    assert sub([(100, 200), (200, 300)], 250, 300) == [(100, 200), (200, 250)]
    assert sub([(100, 200), (200, 300)], 50, 150) == [(150, 200), (200, 300)]
    assert sub([(100, 200), (200, 300)], 175, 200) == [(100, 175), (200, 300)]
    assert sub([(100, 200), (200, 300)], 200, 225) == [(100, 200), (225, 300)]
    assert sub([(100, 200), (200, 300)], 175, 225) == [(100, 175), (225, 300)]
    assert sub([(100, 200), (200, 300)], 150, 175) == [(100, 150), (175, 200), (200, 300)]
    assert sub([(100, 200), (200, 300)], 250, 275) == [(100, 200), (200, 250), (275, 300)]
