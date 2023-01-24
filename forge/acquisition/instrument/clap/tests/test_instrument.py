import asyncio
import typing
import pytest
from forge.tasks import wait_cancelable
from forge.acquisition.instrument.testing import create_streaming_instrument, BusInterface
from forge.acquisition.instrument.clap.simulator import Simulator
from forge.acquisition.instrument.clap.instrument import Instrument


@pytest.mark.asyncio
async def test_communications():
    simulator: Simulator = None
    instrument: Instrument = None
    simulator, instrument = await create_streaming_instrument(Instrument, Simulator)
    bus: BusInterface = instrument.context.bus

    simulator_run = asyncio.ensure_future(simulator.run())
    instrument_run = asyncio.ensure_future(instrument.run())

    await wait_cancelable(bus.wait_for_communicating(), 30)

    assert await bus.value('Q') == simulator.data_Q
    assert await bus.value('Tcase') == simulator.data_Tcase
    assert await bus.value('Tsample') == simulator.data_Tsample
    await bus.value('Vflow')
    assert await bus.state('Ff') == simulator.data_Ff
    assert await bus.state('Fn') == simulator.data_Fn

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
async def test_filter_change():
    simulator: Simulator = None
    instrument: Instrument = None
    simulator, instrument = await create_streaming_instrument(Instrument, Simulator, config={
        'AUTODETECT': False,
        'INTENSITY_DETAILS': True,
        'CHANGE': {
            'FILTER': {
                'TIME': 5.0,
            },
            'SPOT': {
                'TIME': 5.0,
                'DISCARD': 1.0,
            },
        },
    })
    bus: BusInterface = instrument.context.bus

    simulator_run = asyncio.ensure_future(simulator.run())
    instrument_run = asyncio.ensure_future(instrument.run())

    await wait_cancelable(bus.wait_for_communicating(), 30)

    assert await bus.value('Q') == simulator.data_Q
    assert await bus.value('Tcase') == simulator.data_Tcase
    assert await bus.value('Tsample') == simulator.data_Tsample
    await bus.value('Vflow')
    assert await bus.state('Ff') == simulator.data_Ff
    assert await bus.state('Fn') == simulator.data_Fn

    bus.command('filter_change_start')
    await bus.wait_for_notification('need_filter_change', is_set=False)
    bus.command('filter_change_end')
    await bus.wait_for_notification('filter_baseline')
    await bus.wait_for_notification('filter_change', is_set=False)
    await bus.wait_for_notification('wait_spot_stability')
    await bus.wait_for_notification('filter_baseline', is_set=False)
    await bus.wait_for_notification('wait_spot_stability', is_set=False)

    assert await bus.state('Ff') == simulator.data_Ff
    assert await bus.state('Fn') == 1
    assert simulator.data_Fn == 1

    assert await bus.value('ID') == simulator.data_ID
    assert await bus.value('IB') == simulator.data_IB
    assert await bus.value('IG') == simulator.data_IG
    assert await bus.value('IR') == simulator.data_IR
    assert await bus.value('IpB') == simulator.data_IpB
    assert await bus.value('IpG') == simulator.data_IpG
    assert await bus.value('IpR') == simulator.data_IpR
    assert await bus.value('IfB') == simulator.data_IfB
    assert await bus.value('IfG') == simulator.data_IfG
    assert await bus.value('IfR') == simulator.data_IfR
    await bus.state('In0')

    assert await bus.value('IrB') == pytest.approx(1.0)
    assert await bus.value('IrG') == pytest.approx(1.0)
    assert await bus.value('IrR') == pytest.approx(1.0)

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
