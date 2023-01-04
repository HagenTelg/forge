import asyncio
import typing
import pytest
from forge.tasks import wait_cancelable
from forge.acquisition.instrument.testing import create_streaming_instrument, BusInterface
from forge.acquisition.instrument.thermo49.simulator import Simulator
from forge.acquisition.instrument.thermo49.instrument import Instrument



@pytest.mark.asyncio
async def test_communications():
    simulator: Simulator = None
    instrument: Instrument = None
    simulator, instrument = await create_streaming_instrument(Instrument, Simulator)
    bus: BusInterface = instrument.context.bus

    simulator_run = asyncio.ensure_future(simulator.run())
    instrument_run = asyncio.ensure_future(instrument.run())

    await wait_cancelable(bus.wait_for_communicating(), 30)

    assert await bus.value('X') == simulator.data_X
    assert await bus.value('Qa') == simulator.data_Qa
    assert await bus.value('Qb') == simulator.data_Qb
    assert await bus.value('Ca') == simulator.data_Ca
    assert await bus.value('Cb') == simulator.data_Cb
    assert await bus.value('Psample') == pytest.approx(simulator.data_Psample, abs=1)
    assert await bus.value('Tsample') == simulator.data_Tsample
    assert await bus.value('Tlamp') == simulator.data_Tlamp
    assert await bus.value('bitflags') == simulator.flags

    assert await bus.value('Qozonator') == simulator.data_Qozonator

    instrument_run.cancel()
    simulator_run.cancel()
    try:
        await instrument_run
    except asyncio.CancelledError:
        pass
    try:
        await simulator_run
    except asyncio.CancelledError:
        pass


@pytest.mark.asyncio
async def test_49i():
    simulator: Simulator = None
    instrument: Instrument = None
    simulator, instrument = await create_streaming_instrument(Instrument, Simulator)
    bus: BusInterface = instrument.context.bus
    simulator.mode = simulator.InstrumentMode.MODE_49i

    simulator_run = asyncio.ensure_future(simulator.run())
    instrument_run = asyncio.ensure_future(instrument.run())

    await wait_cancelable(bus.wait_for_communicating(), 30)

    assert await bus.value('X') == simulator.data_X
    assert await bus.value('Qa') == simulator.data_Qa
    assert await bus.value('Qb') == simulator.data_Qb
    assert await bus.value('Ca') == simulator.data_Ca
    assert await bus.value('Cb') == simulator.data_Cb
    assert await bus.value('Psample') == pytest.approx(simulator.data_Psample, abs=1)
    assert await bus.value('Tsample') == simulator.data_Tsample
    assert await bus.value('Tlamp') == simulator.data_Tlamp
    assert await bus.value('bitflags') == simulator.flags

    assert await bus.value('Tozonator') == simulator.data_Tozonator
    assert await bus.value('Vlamp') == simulator.data_Vlamp
    assert await bus.value('Vozonator') == simulator.data_Vozonator

    instrument_run.cancel()
    simulator_run.cancel()
    try:
        await instrument_run
    except asyncio.CancelledError:
        pass
    try:
        await simulator_run
    except asyncio.CancelledError:
        pass


@pytest.mark.asyncio
async def test_49c_legacy1():
    simulator: Simulator = None
    instrument: Instrument = None
    simulator, instrument = await create_streaming_instrument(Instrument, Simulator)
    bus: BusInterface = instrument.context.bus
    simulator.mode = simulator.InstrumentMode.MODE_49c_Legacy1
    simulator.sum_delimiter = b'\x80'

    simulator_run = asyncio.ensure_future(simulator.run())
    instrument_run = asyncio.ensure_future(instrument.run())

    await wait_cancelable(bus.wait_for_communicating(), 30)

    assert await bus.value('X') == simulator.data_X
    assert await bus.value('Qa') == simulator.data_Qa
    assert await bus.value('Qb') == simulator.data_Qb
    assert await bus.value('Ca') == simulator.data_Ca
    assert await bus.value('Cb') == simulator.data_Cb
    assert await bus.value('Psample') == pytest.approx(simulator.data_Psample, abs=1)
    assert await bus.value('Tsample') == simulator.data_Tsample
    assert await bus.value('Tlamp') == simulator.data_Tlamp
    assert await bus.value('bitflags') == simulator.flags

    instrument_run.cancel()
    simulator_run.cancel()
    try:
        await instrument_run
    except asyncio.CancelledError:
        pass
    try:
        await simulator_run
    except asyncio.CancelledError:
        pass


@pytest.mark.asyncio
async def test_49c_legacy2():
    simulator: Simulator = None
    instrument: Instrument = None
    simulator, instrument = await create_streaming_instrument(Instrument, Simulator)
    bus: BusInterface = instrument.context.bus
    simulator.mode = simulator.InstrumentMode.MODE_49c_Legacy2
    simulator.sum_delimiter = b'*\n'

    simulator_run = asyncio.ensure_future(simulator.run())
    instrument_run = asyncio.ensure_future(instrument.run())

    await wait_cancelable(bus.wait_for_communicating(), 30)

    assert await bus.value('X') == simulator.data_X
    assert await bus.value('Qa') == simulator.data_Qa
    assert await bus.value('Qb') == simulator.data_Qb
    assert await bus.value('Ca') == simulator.data_Ca
    assert await bus.value('Cb') == simulator.data_Cb
    assert await bus.value('Psample') == pytest.approx(simulator.data_Psample, abs=1)
    assert await bus.value('Tsample') == simulator.data_Tsample
    assert await bus.value('Tlamp') == simulator.data_Tlamp
    assert await bus.value('bitflags') == simulator.flags

    instrument_run.cancel()
    simulator_run.cancel()
    try:
        await instrument_run
    except asyncio.CancelledError:
        pass
    try:
        await simulator_run
    except asyncio.CancelledError:
        pass
