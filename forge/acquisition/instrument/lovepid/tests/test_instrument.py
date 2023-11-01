import asyncio
import typing
import pytest
from forge.tasks import wait_cancelable
from forge.acquisition.instrument.testing import create_streaming_instrument, cleanup_streaming_instrument, BusInterface
from forge.acquisition.instrument.lovepid.simulator import Simulator
from forge.acquisition.instrument.lovepid.instrument import Instrument



@pytest.mark.asyncio
async def test_communications():
    simulator: Simulator = None
    instrument: Instrument = None
    simulator, instrument = await create_streaming_instrument(Instrument, Simulator, config={
        'DATA': {
            'T_V11': {
                'ADDRESS': 0x33,
                'CALIBRATION': [0.0, 10.0]
            },
        },
    })
    bus: BusInterface = instrument.context.bus

    simulator_run = asyncio.ensure_future(simulator.run())
    instrument_run = asyncio.ensure_future(instrument.run())

    await wait_cancelable(bus.wait_for_communicating(), 30)

    assert await bus.value('raw') == simulator.value
    assert await bus.state('setpoint') == simulator.setpoint
    assert await bus.value('control') == simulator.output
    assert await bus.value('T_V11') == 25.0
    assert (await bus.value('value'))[2] == 25.0

    await cleanup_streaming_instrument(simulator, instrument, instrument_run, simulator_run)


@pytest.mark.asyncio
async def test_outputs():
    simulator: Simulator = None
    instrument: Instrument = None
    simulator, instrument = await create_streaming_instrument(Instrument, Simulator, config={
        'DATA': {
            'T_V11': {
                'ADDRESS': 0x32,
                'CALIBRATION': [0.0, 10.0]
            },
        },
    })
    bus: BusInterface = instrument.context.bus

    bus.command('set_analog_channel', {
        'channel': 0x32,
        'value': 10.0,
    })

    simulator_run = asyncio.ensure_future(simulator.run())
    instrument_run = asyncio.ensure_future(instrument.run())

    await wait_cancelable(bus.wait_for_communicating(), 30)

    assert (await bus.state('setpoint'))[1] == 10.0

    assert bus.instrument_info['variable'][1] == 'T_V11'
    assert bus.instrument_info['address'][1] == 0x32

    assert simulator.setpoint[1] == 10.0

    await cleanup_streaming_instrument(simulator, instrument, instrument_run, simulator_run)

