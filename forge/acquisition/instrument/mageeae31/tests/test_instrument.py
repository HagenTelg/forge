import asyncio
import typing
import pytest
from forge.tasks import wait_cancelable
from forge.acquisition.instrument.testing import create_streaming_instrument, BusInterface
from forge.acquisition.instrument.mageeae31.simulator import Simulator
from forge.acquisition.instrument.mageeae31.instrument import Instrument



@pytest.mark.asyncio
async def test_communications():
    simulator: Simulator = None
    instrument: Instrument = None
    simulator, instrument = await create_streaming_instrument(Instrument, Simulator, config={
        'SAMPLE_TEMPERATURE': 0.0,
        'SAMPLE_PRESSURE': 1013.25,
    })
    bus: BusInterface = instrument.context.bus
    simulator.unpolled_interval = 2.0

    simulator_run = asyncio.ensure_future(simulator.run())
    instrument_run = asyncio.ensure_future(instrument.run())

    await wait_cancelable(bus.wait_for_communicating(), 30)

    assert await bus.value('Q') == simulator.data_Q
    assert await bus.value('PCTbypass') == simulator.data_PCTbypass

    assert await bus.value('X1') == simulator.data_X1
    assert await bus.value('If1') == pytest.approx(simulator.data_If1)
    assert await bus.value('Ip1') == pytest.approx(simulator.data_Ip1)
    assert await bus.value('Ifz1') == pytest.approx(simulator.data_Ifz1)
    assert await bus.value('Ipz1') == pytest.approx(simulator.data_Ipz1)
    assert await bus.value('Ir1') == pytest.approx(simulator.data_Ir1)

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
async def test_compressed():
    simulator: Simulator = None
    instrument: Instrument = None
    simulator, instrument = await create_streaming_instrument(Instrument, Simulator, config={
        'SAMPLE_TEMPERATURE': 0.0,
        'SAMPLE_PRESSURE': 1013.25,
    })
    bus: BusInterface = instrument.context.bus
    simulator.unpolled_interval = 2.0
    simulator.serial_number = 123
    simulator.compressed_output = True

    simulator_run = asyncio.ensure_future(simulator.run())
    instrument_run = asyncio.ensure_future(instrument.run())

    await wait_cancelable(bus.wait_for_communicating(), 30)

    assert await bus.value('Q') == simulator.data_Q
    assert await bus.value('PCTbypass') == simulator.data_PCTbypass

    assert await bus.value('X1') == simulator.data_X1
    assert await bus.value('If1') == pytest.approx(simulator.data_If1)
    assert await bus.value('Ip1') == pytest.approx(simulator.data_Ip1)
    assert await bus.value('Ifz1') == pytest.approx(simulator.data_Ifz1)
    assert await bus.value('Ipz1') == pytest.approx(simulator.data_Ipz1)
    assert await bus.value('Ir1') == pytest.approx(simulator.data_Ir1)

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



