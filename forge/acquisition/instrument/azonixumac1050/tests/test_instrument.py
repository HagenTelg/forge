import asyncio
import typing
import pytest
from forge.tasks import wait_cancelable
from forge.acquisition.instrument.testing import create_streaming_instrument, BusInterface
from forge.acquisition.instrument.azonixumac1050.simulator import Simulator
from forge.acquisition.instrument.azonixumac1050.instrument import Instrument



@pytest.mark.asyncio
async def test_communications():
    simulator: Simulator = None
    instrument: Instrument = None
    simulator, instrument = await create_streaming_instrument(Instrument, Simulator, config={
        'DATA': {
            'T_V11': {
                'CHANNEL': 2,
                'CALIBRATION': [0.5, 10.0]
            },
        },
    })
    bus: BusInterface = instrument.context.bus

    simulator_run = asyncio.ensure_future(simulator.run())
    instrument_run = asyncio.ensure_future(instrument.run())

    await wait_cancelable(bus.wait_for_communicating(), 30)

    assert await bus.value('T') == simulator.data_T
    assert await bus.value('V') == simulator.data_V
    assert await bus.value('raw') == simulator.ain
    assert await bus.value('T_V11') == 20.5
    assert (await bus.value('value'))[2] == 20.5

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
async def test_outputs():
    simulator: Simulator = None
    instrument: Instrument = None
    simulator, instrument = await create_streaming_instrument(Instrument, Simulator, config={
        'ANALOG_OUTPUT': {
            'AOT1': {
                'CHANNEL': 1,
            },
        },
        'DIGITAL': {
            'DOT1': {
                'CHANNEL': 0,
            },
        },
    })
    bus: BusInterface = instrument.context.bus

    bus.command('set_digital_output', 0x0001)
    bus.command('set_analog_channel', {
        'channel': 1,
        'value': 2.0,
    })

    simulator_run = asyncio.ensure_future(simulator.run())
    instrument_run = asyncio.ensure_future(instrument.run())

    await wait_cancelable(bus.wait_for_communicating(), 30)

    assert await bus.value('T') == simulator.data_T
    assert await bus.value('V') == simulator.data_V
    assert (await bus.state('output'))[1] == 2.0
    assert await bus.state('digital') == 0x0001

    assert bus.instrument_info['output'][1] == 'AOT1'
    assert bus.instrument_info['digital'][0] == 'DOT1'

    assert simulator.dot == 0x0001
    assert simulator.aot[1] == 2.0

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

