import asyncio
import typing
import pytest
from forge.tasks import wait_cancelable
from forge.acquisition.instrument.testing import create_streaming_instrument, cleanup_streaming_instrument, BusInterface
from forge.acquisition.instrument.grimm110xopc.simulator import Simulator
from forge.acquisition.instrument.grimm110xopc.instrument import Instrument



@pytest.mark.asyncio
async def test_communications():
    simulator: Simulator = None
    instrument: Instrument = None
    simulator, instrument = await create_streaming_instrument(Instrument, Simulator, config={
        'DATA': {
            'Q': 1.2,
        },
    })
    bus: BusInterface = instrument.context.bus

    simulator_run = asyncio.ensure_future(simulator.run())
    instrument_run = asyncio.ensure_future(instrument.run())

    await wait_cancelable(bus.wait_for_communicating(), 90)

    await bus.value('Q')
    del bus.data_values['Q']
    assert await bus.value('Q') == simulator.data_Q
    assert await bus.value('N') == pytest.approx(simulator.data_N)
    assert await bus.value('PCTpump') == simulator.data_PCTpump
    assert await bus.value('PCTbattery') == simulator.data_PCTbattery
    assert await bus.value('dN') == pytest.approx(simulator.data_dN)
    assert bus.state_records['Dp'] == simulator.data_Dp

    await cleanup_streaming_instrument(simulator, instrument, instrument_run, simulator_run)


@pytest.mark.asyncio
async def test_version180():
    simulator: Simulator = None
    instrument: Instrument = None
    simulator, instrument = await create_streaming_instrument(Instrument, Simulator, config={
        'DATA': {
            'Q': 1.2,
        },
    })
    bus: BusInterface = instrument.context.bus
    simulator.mass_concentrations = True
    simulator.model_number = "180MC"
    simulator.firmware_version = "7.80 US"

    simulator_run = asyncio.ensure_future(simulator.run())
    instrument_run = asyncio.ensure_future(instrument.run())

    await wait_cancelable(bus.wait_for_communicating(), 90)

    await bus.value('Q')
    del bus.data_values['Q']
    assert await bus.value('Q') == simulator.data_Q
    assert await bus.value('N') == pytest.approx(simulator.data_N)
    assert await bus.value('PCTpump') == simulator.data_PCTpump
    assert await bus.value('PCTbattery') == simulator.data_PCTbattery
    assert await bus.value('X1') == simulator.data_X1
    assert await bus.value('X25') == simulator.data_X25
    assert await bus.value('X10') == simulator.data_X10
    assert await bus.value('dN') == pytest.approx(simulator.data_dN)
    assert bus.state_records['Dp'] == simulator.data_Dp

    await cleanup_streaming_instrument(simulator, instrument, instrument_run, simulator_run)


@pytest.mark.asyncio
async def test_version5():
    simulator: Simulator = None
    instrument: Instrument = None
    simulator, instrument = await create_streaming_instrument(Instrument, Simulator, config={
        'DATA': {
            'Q': 1.2,
        },
    })
    bus: BusInterface = instrument.context.bus
    simulator.model_number = "1.105"
    del simulator.data_Dlower[8:]
    del simulator.data_Csum[8:]

    simulator_run = asyncio.ensure_future(simulator.run())
    instrument_run = asyncio.ensure_future(instrument.run())

    await wait_cancelable(bus.wait_for_communicating(), 90)

    await bus.value('Q')
    del bus.data_values['Q']
    assert await bus.value('Q') == simulator.data_Q
    assert await bus.value('N') == pytest.approx(simulator.data_N)
    assert await bus.value('PCTpump') == simulator.data_PCTpump
    assert await bus.value('PCTbattery') == simulator.data_PCTbattery
    assert await bus.value('dN') == pytest.approx(simulator.data_dN)
    assert bus.state_records['Dp'] == simulator.data_Dp

    await cleanup_streaming_instrument(simulator, instrument, instrument_run, simulator_run)


@pytest.mark.asyncio
async def test_version8():
    simulator: Simulator = None
    instrument: Instrument = None
    simulator, instrument = await create_streaming_instrument(Instrument, Simulator, config={
        'DATA': {
            'Q': 1.2,
        },
    })
    bus: BusInterface = instrument.context.bus
    simulator.model_number = "1.108"
    del simulator.data_Dlower[15:]
    del simulator.data_Csum[15:]

    simulator_run = asyncio.ensure_future(simulator.run())
    instrument_run = asyncio.ensure_future(instrument.run())

    await wait_cancelable(bus.wait_for_communicating(), 90)

    await bus.value('Q')
    del bus.data_values['Q']
    assert await bus.value('Q') == simulator.data_Q
    assert await bus.value('N') == pytest.approx(simulator.data_N)
    assert await bus.value('PCTpump') == simulator.data_PCTpump
    assert await bus.value('PCTbattery') == simulator.data_PCTbattery
    assert await bus.value('dN') == pytest.approx(simulator.data_dN)
    assert bus.state_records['Dp'] == simulator.data_Dp

    await cleanup_streaming_instrument(simulator, instrument, instrument_run, simulator_run)


@pytest.mark.asyncio
async def test_flow_calculate():
    simulator: Simulator = None
    instrument: Instrument = None
    simulator, instrument = await create_streaming_instrument(Instrument, Simulator)
    bus: BusInterface = instrument.context.bus

    simulator_run = asyncio.ensure_future(simulator.run())
    instrument_run = asyncio.ensure_future(instrument.run())

    await wait_cancelable(bus.wait_for_communicating(), 90)

    await bus.value('Q')
    del bus.data_values['Q']
    assert await bus.value('Q') == pytest.approx(simulator.data_Q, abs=0.1)

    await cleanup_streaming_instrument(simulator, instrument, instrument_run, simulator_run)
