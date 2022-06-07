import asyncio
import typing
import pytest
from forge.acquisition import LayeredConfiguration
from forge.acquisition.schedule import Schedule


def test_basic():
    s = Schedule(LayeredConfiguration({
        'CYCLE_TIME': 100,
        'SCHEDULE': [
            {'TIME': 0, 'id': 1},
            {'TIME': 30, 'id': 2},
        ]
    }))

    current, next = s.advance(100.0)
    assert current.config['id'] == 1
    assert current.next_time == 200.0
    assert current.scheduled_time == 100.0
    assert current.activate(100.0)
    assert current.last_time == 100.0
    assert next.config['id'] == 2
    assert next.next_time == 130.0
    assert next.scheduled_time == 130.0
    assert next.last_time is None

    current, next = s.advance(120.0)
    assert current.config['id'] == 1
    assert current.next_time == 200.0
    assert current.scheduled_time == 100.0
    assert not current.activate(120.0)
    assert current.last_time == 100.0
    assert next.config['id'] == 2
    assert next.next_time == 130.0
    assert next.scheduled_time == 130.0
    assert next.last_time is None

    current, next = s.advance(130.0)
    assert current.config['id'] == 2
    assert current.next_time == 230.0
    assert current.scheduled_time == 130.0
    assert current.activate(130.0)
    assert current.last_time == 130.0
    assert next.config['id'] == 1
    assert next.next_time == 200.0
    assert next.last_time == 100.0
    assert next.scheduled_time == 200.0

    current, next = s.advance(199.0)
    assert current.config['id'] == 2
    assert current.next_time == 230.0
    assert current.scheduled_time == 130.0
    assert not current.activate(130.0)
    assert current.last_time == 130.0
    assert next.config['id'] == 1
    assert next.next_time == 200.0
    assert next.last_time == 100.0
    assert next.scheduled_time == 200.0

    current, next = s.advance(201.0)
    assert current.config['id'] == 1
    assert current.next_time == 300.0
    assert current.scheduled_time == 200.0
    assert current.activate(201.0)
    assert current.last_time == 201.0
    assert next.config['id'] == 2
    assert next.next_time == 230.0
    assert next.last_time == 130.0
    assert next.scheduled_time == 230.0

    current, next = s.advance(300.0)
    assert current.config['id'] == 1
    assert current.next_time == 400.0
    assert current.scheduled_time == 300.0
    assert current.activate(300.0)
    assert current.last_time == 300.0
    assert next.config['id'] == 2
    assert next.next_time == 330.0
    assert next.last_time == 130.0
    assert next.scheduled_time == 330.0


def test_missed_first():
    s = Schedule(LayeredConfiguration({
        'CYCLE_TIME': 100,
        'SCHEDULE': [
            {'TIME': 0, 'id': 1},
            {'TIME': 30, 'id': 2},
        ]
    }))

    current, next = s.advance(131.0)
    assert current.config['id'] == 2
    assert current.next_time == 230.0
    assert current.activate(131.0)
    assert current.last_time == 131.0
    assert next.config['id'] == 1
    assert next.next_time == 200.0
    assert next.last_time is None


def test_alternation():
    s = Schedule(LayeredConfiguration({
        'CYCLE_TIME': 100,
        'SCHEDULE': [{'id': 1}],
        'ALTERNATE': {'id': 2, 'INTERVAL': 20.0}
    }))

    current, next = s.advance(100.0)
    assert current.config['id'] == 1
    assert current.next_time == 200.0
    assert next.config['id'] == 2
    assert next.next_time == 120.0

    current, next = s.advance(120.0)
    assert current.config['id'] == 2
    assert current.next_time == 220.0
    assert next.config['id'] == 1
    assert next.next_time == 140.0

    current, next = s.advance(140.0)
    assert current.config['id'] == 1
    assert current.next_time == 240.0
    assert next.config['id'] == 2
    assert next.next_time == 160.0

    current, next = s.advance(161.0)
    assert current.config['id'] == 2
    assert current.next_time == 260.0
    assert next.config['id'] == 1
    assert next.next_time == 180.0

    current, next = s.advance(200.0)
    assert current.config['id'] == 1
    assert current.next_time == 300.0
    assert next.config['id'] == 2
    assert next.next_time == 220.0
