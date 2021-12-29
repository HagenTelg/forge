import asyncio
import typing
import pytest
from math import isfinite
from forge.vis.realtime.controller.block import DataBlock


@pytest.mark.asyncio
async def test_basic(tmp_path):
    block = DataBlock()

    assert len(block.records) == 0

    block.add_record(5000, {'foo': 1.0}, rounding_ms=1)
    assert len(block.records) == 1
    assert block.records[0].epoch_ms == 5000
    assert block.records[0].fields == {'foo': 1.0}

    block.add_record(5000, {'foo': 2.0, 'bar': [3.0, 4.0]}, rounding_ms=1)
    assert len(block.records) == 1
    assert block.records[0].epoch_ms == 5000
    assert block.records[0].fields == {'foo': 2.0, 'bar': [3.0, 4.0]}

    block.add_record(6000, {'foo': 5.0}, rounding_ms=1)
    assert len(block.records) == 2
    assert block.records[0].epoch_ms == 5000
    assert block.records[0].fields == {'foo': 2.0, 'bar': [3.0, 4.0]}
    assert block.records[1].epoch_ms == 6000
    assert block.records[1].fields == {'foo': 5.0}

    block_file = tmp_path / 'block'
    with open(str(block_file), mode='wb') as f:
        await block.save(f)

    assert block_file.stat().st_size > 0

    block = DataBlock()
    with open(str(block_file), mode='rb') as f:
        await block.load(f)
    assert len(block.records) == 2
    assert block.records[0].epoch_ms == 5000
    assert block.records[0].fields == {'foo': 2.0, 'bar': [3.0, 4.0]}
    assert block.records[1].epoch_ms == 6000
    assert block.records[1].fields == {'foo': 5.0}


@pytest.mark.asyncio
async def test_trim(tmp_path):
    block = DataBlock()

    block.add_record(5000, {'foo': 1.0, 'bar': 0.5}, rounding_ms=1)
    block.add_record(6000, {'foo': 2.0}, rounding_ms=1)
    block.add_record(7000, {'foo': 3.0}, rounding_ms=1)
    block.add_record(8000, {'foo': 4.0}, rounding_ms=1)
    assert len(block.records) == 4

    block_file = tmp_path / 'block'
    with open(str(block_file), mode='wb') as f:
        await block.save(f)

    def trim_1(times: typing.List[int]) -> int:
        assert times == [5000, 6000, 7000, 8000]
        return 2
    with open(str(block_file), mode='r+b') as f:
        remove_file = await DataBlock.trim(f, trim_1)
    assert not remove_file

    block = DataBlock()
    with open(str(block_file), mode='rb') as f:
        await block.load(f)
    assert len(block.records) == 2
    assert block.records[0].epoch_ms == 7000
    assert block.records[0].fields == {'foo': 3.0}
    assert block.records[1].epoch_ms == 8000
    assert block.records[1].fields == {'foo': 4.0}

    def trim_2(times: typing.List[int]) -> int:
        assert times == [7000, 8000]
        return 0
    with open(str(block_file), mode='r+b') as f:
        remove_file = await DataBlock.trim(f, trim_2)
    assert not remove_file

    block = DataBlock()
    with open(str(block_file), mode='rb') as f:
        await block.load(f)
    assert len(block.records) == 2
    assert block.records[0].epoch_ms == 7000
    assert block.records[0].fields == {'foo': 3.0}
    assert block.records[1].epoch_ms == 8000
    assert block.records[1].fields == {'foo': 4.0}

    def trim_3(times: typing.List[int]) -> int:
        assert times == [7000, 8000]
        return 1
    with open(str(block_file), mode='r+b') as f:
        remove_file = await DataBlock.trim(f, trim_3)
    assert not remove_file

    block = DataBlock()
    with open(str(block_file), mode='rb') as f:
        await block.load(f)
    assert len(block.records) == 1
    assert block.records[0].epoch_ms == 8000
    assert block.records[0].fields == {'foo': 4.0}

    def trim_4(times: typing.List[int]) -> int:
        assert times == [8000]
        return 1
    with open(str(block_file), mode='r+b') as f:
        remove_file = await DataBlock.trim(f, trim_4)
    assert remove_file

