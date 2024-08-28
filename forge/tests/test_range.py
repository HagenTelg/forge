import typing
import pytest
from forge.range import intersects, contains, subtract_tuple, intersecting_tuple, insertion_tuple, merge_tuple


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


def test_contains():
    assert contains(100, 200, 125, 175)
    assert contains(100, 200, 125, 200)
    assert contains(100, 200, 100, 175)
    assert contains(100, 200, 100, 200)
    assert not contains(100, 200, 100, 250)
    assert not contains(100, 200, 50, 150)
    assert not contains(100, 200, 50, 250)
    assert not contains(100, 200, 50, 100)
    assert not contains(100, 200, 200, 250)


def test_subtract():
    def sub(existing, start, end):
        saved = list(existing)
        subtract_tuple(existing, start, end)
        subtract_tuple(saved, start, end, canonical=False)
        assert existing == saved
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


def test_find_intersecting():
    def find(existing, start, end):
        first = intersecting_tuple(existing, start, end)
        second = intersecting_tuple(existing, start, end, canonical=False)
        first = list(first)
        assert first == list(second)
        return first

    assert find([], 100, 200) == []
    assert find([(100, 200)], 100, 200) == [0]
    assert find([(100, 200)], 250, 300) == []
    assert find([(100, 200)], 50, 75) == []
    assert find([(100, 200)], 200, 300) == []
    assert find([(100, 200)], 50, 100) == []
    assert find([(100, 200)], 50, 300) == [0]
    assert find([(100, 200)], 50, 150) == [0]
    assert find([(100, 200)], 150, 300) == [0]

    assert find([(100, 200), (200, 300)], 100, 200) == [0]
    assert find([(100, 200), (200, 300)], 200, 300) == [1]
    assert find([(100, 200), (200, 300)], 100, 300) == [0, 1]
    assert find([(100, 200), (200, 300)], 50, 75) == []
    assert find([(100, 200), (200, 300)], 350, 400) == []
    assert find([(100, 200), (200, 300)], 50, 150) == [0]
    assert find([(100, 200), (200, 300)], 125, 150) == [0]
    assert find([(100, 200), (200, 300)], 150, 250) == [0, 1]
    assert find([(100, 200), (200, 300)], 250, 300) == [1]
    assert find([(100, 200), (200, 300)], 250, 350) == [1]


def test_insertion():
    assert insertion_tuple([], 100) == 0
    assert insertion_tuple([(100, 200)], 50) == 0
    assert insertion_tuple([(100, 200)], 100) == 0
    assert insertion_tuple([(100, 200)], 150) == 1
    assert insertion_tuple([(100, 200)], 200) == 1
    assert insertion_tuple([(100, 200)], 250) == 1
    assert insertion_tuple([(100, 200), (200, 300)], 50) == 0
    assert insertion_tuple([(100, 200), (200, 300)], 100) == 0
    assert insertion_tuple([(100, 200), (200, 300)], 150) == 1
    assert insertion_tuple([(100, 200), (200, 300)], 200) == 1
    assert insertion_tuple([(100, 200), (200, 300)], 250) == 2
    assert insertion_tuple([(100, 200), (200, 300)], 300) == 2
    assert insertion_tuple([(100, 200), (200, 300)], 350) == 2
    assert insertion_tuple([(100, 200), (300, 400)], 200) == 1
    assert insertion_tuple([(100, 200), (300, 400)], 250) == 1
    assert insertion_tuple([(100, 200), (300, 400)], 300) == 1
    assert insertion_tuple([(100, 200), (300, 400)], 350) == 2
    assert insertion_tuple([(100, 200), (300, 400)], 400) == 2
    assert insertion_tuple([(100, 200), (300, 400)], 450) == 2
    assert insertion_tuple([(100, 200), (200, 300), (300, 400)], 200) == 1
    assert insertion_tuple([(100, 200), (200, 300), (300, 400)], 250) == 2
    assert insertion_tuple([(100, 200), (200, 300), (300, 400)], 300) == 2
    assert insertion_tuple([(100, 200), (200, 300), (300, 400)], 350) == 3
    assert insertion_tuple([(100, 200), (200, 300), (300, 400)], 400) == 3
    assert insertion_tuple([(100, 200), (200, 300), (300, 400)], 450) == 3


def test_merge():
    def merge(existing, start, end):
        first = list(existing)
        merge_tuple(first, start, end)
        second = list(existing)
        merge_tuple(second, start, end, canonical=False)
        assert sorted(first) == sorted(second)
        return first

    assert merge([], 100, 200) == [(100, 200)]
    assert merge([(100, 200)], 110, 190) == [(100, 200)]
    assert merge([(100, 200)], 100, 190) == [(100, 200)]
    assert merge([(100, 200)], 110, 200) == [(100, 200)]
    assert merge([(200, 300)], 100, 200) == [(100, 300)]
    assert merge([(100, 200)], 200, 300) == [(100, 300)]
    assert merge([(100, 200), (300, 400)], 200, 300) == [(100, 400)]
    assert merge([(100, 200), (300, 400)], 210, 290) == [(100, 200), (210, 290), (300, 400)]
    assert merge([(100, 200), (300, 400)], 210, 300) == [(100, 200), (210, 400)]
    assert merge([(100, 200), (300, 400)], 200, 290) == [(100, 290), (300, 400)]
    assert merge([(100, 200), (300, 400)], 190, 310) == [(100, 400)]
    assert merge([(100, 200), (300, 400)], 50, 90) == [(50, 90), (100, 200), (300, 400)]
    assert merge([(100, 200), (300, 400)], 50, 100) == [(50, 200), (300, 400)]
    assert merge([(100, 200), (300, 400)], 50, 110) == [(50, 200), (300, 400)]
    assert merge([(100, 200), (300, 400)], 50, 210) == [(50, 210), (300, 400)]
    assert merge([(100, 200), (300, 400)], 50, 410) == [(50, 410)]
    assert merge([(100, 200), (300, 400)], 500, 600) == [(100, 200), (300, 400), (500, 600)]
    assert merge([(100, 200), (300, 400)], 400, 600) == [(100, 200), (300, 600)]
    assert merge([(100, 200), (300, 400)], 390, 600) == [(100, 200), (300, 600)]
    assert merge([(100, 200), (300, 400)], 290, 600) == [(100, 200), (290, 600)]
    assert merge([(100, 200), (300, 400)], 200, 600) == [(100, 600)]
    assert merge([(100, 200), (300, 400)], 190, 600) == [(100, 600)]
    assert merge([(100, 200), (300, 400), (500, 600)], 410, 490) == [(100, 200), (300, 400), (410, 490), (500, 600)]
    assert merge([(100, 200), (300, 400), (500, 600)], 400, 490) == [(100, 200), (300, 490), (500, 600)]
    assert merge([(100, 200), (300, 400), (500, 600)], 410, 500) == [(100, 200), (300, 400), (410, 600)]
    assert merge([(100, 200), (300, 400), (500, 600)], 400, 500) == [(100, 200), (300, 600)]
