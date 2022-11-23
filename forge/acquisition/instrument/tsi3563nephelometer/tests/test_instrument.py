import asyncio
import typing
import pytest
from forge.tasks import wait_cancelable
from forge.units import ZERO_C_IN_K
from forge.acquisition.instrument.testing import create_streaming_instrument, BusInterface, PersistentInterface
from forge.acquisition.instrument.tsi3563nephelometer.simulator import Simulator
from forge.acquisition.instrument.tsi3563nephelometer.instrument import Instrument



@pytest.mark.asyncio
async def test_communications():
    simulator: Simulator = None
    instrument: Instrument = None
    simulator, instrument = await create_streaming_instrument(Instrument, Simulator, config={
        'ZERO': False,
    })
    bus: BusInterface = instrument.context.bus
    persistent: PersistentInterface = instrument.context.persistent

    simulator_run = asyncio.ensure_future(simulator.run())
    instrument_run = asyncio.ensure_future(instrument.run())

    await wait_cancelable(bus.wait_for_communicating(), 30)

    assert await bus.value('Tsample') == pytest.approx(simulator.data_Tsample)
    assert await bus.value('Usample') == simulator.data_Usample
    assert await bus.value('Psample') == simulator.data_Psample
    assert await bus.value('Tinlet') == pytest.approx(simulator.data_Tinlet)
    assert await bus.value('Uinlet') == pytest.approx(simulator.data_Uinlet)
    assert await bus.value('Vl') == simulator.data_Vl
    assert await bus.value('Al') == simulator.data_Al
    assert await bus.value('modetime') == simulator.remaining_time
    assert await bus.state('modestring') == simulator.data_modestring

    assert await bus.value('BsB') == simulator.data_Bs[0]
    assert await bus.value('BsG') == simulator.data_Bs[1]
    assert await bus.value('BsR') == simulator.data_Bs[2]
    assert await bus.value('BbsB') == simulator.data_Bbs[0]
    assert await bus.value('BbsG') == simulator.data_Bbs[1]
    assert await bus.value('BbsR') == simulator.data_Bbs[2]

    assert await bus.state('BswB') == pytest.approx(simulator.data_Bsw[0], abs=1E-2)
    assert await bus.state('BswG') == pytest.approx(simulator.data_Bsw[1], abs=1E-2)
    assert await bus.state('BswR') == pytest.approx(simulator.data_Bsw[2], abs=1E-2)
    assert await bus.state('BbswB') == pytest.approx(simulator.data_Bbsw[0], abs=1E-2)
    assert await bus.state('BbswG') == pytest.approx(simulator.data_Bbsw[1], abs=1E-2)
    assert await bus.state('BbswR') == pytest.approx(simulator.data_Bbsw[2], abs=1E-2)

    assert await bus.value('CsB') == pytest.approx(simulator.data_Cs[0])
    assert await bus.value('CsG') == pytest.approx(simulator.data_Cs[1])
    assert await bus.value('CsR') == pytest.approx(simulator.data_Cs[2])
    assert await bus.value('CbsB') == pytest.approx(simulator.data_Cbs[0])
    assert await bus.value('CbsG') == pytest.approx(simulator.data_Cbs[1])
    assert await bus.value('CbsR') == pytest.approx(simulator.data_Cbs[2])

    assert await bus.value('CdB') == pytest.approx(simulator.data_Cd[0])
    assert await bus.value('CdG') == pytest.approx(simulator.data_Cd[1])
    assert await bus.value('CdR') == pytest.approx(simulator.data_Cd[2])
    assert await bus.value('CbdB') == pytest.approx(simulator.data_Cbd[0])
    assert await bus.value('CbdG') == pytest.approx(simulator.data_Cbd[1])
    assert await bus.value('CbdR') == pytest.approx(simulator.data_Cbd[2])

    assert await bus.value('CfB') == pytest.approx(simulator.data_Cf[0])
    assert await bus.value('CfG') == pytest.approx(simulator.data_Cf[1])
    assert await bus.value('CfR') == pytest.approx(simulator.data_Cf[2])

    assert persistent.values['BswB'].data == pytest.approx(simulator.data_Bsw[0], abs=1E-2)
    assert persistent.values['BswG'].data == pytest.approx(simulator.data_Bsw[1], abs=1E-2)
    assert persistent.values['BswR'].data == pytest.approx(simulator.data_Bsw[2], abs=1E-2)
    assert persistent.values['BbswB'].data == pytest.approx(simulator.data_Bbsw[0], abs=1E-2)
    assert persistent.values['BbswG'].data == pytest.approx(simulator.data_Bbsw[1], abs=1E-2)
    assert persistent.values['BbswR'].data == pytest.approx(simulator.data_Bbsw[2], abs=1E-2)
    assert persistent.values['Bsw'].data[0] == pytest.approx(simulator.data_Bsw[0], abs=1E-2)
    assert persistent.values['Bsw'].data[1] == pytest.approx(simulator.data_Bsw[1], abs=1E-2)
    assert persistent.values['Bsw'].data[2] == pytest.approx(simulator.data_Bsw[2], abs=1E-2)
    assert persistent.values['Bbsw'].data[0] == pytest.approx(simulator.data_Bbsw[0], abs=1E-2)
    assert persistent.values['Bbsw'].data[1] == pytest.approx(simulator.data_Bbsw[1], abs=1E-2)
    assert persistent.values['Bbsw'].data[2] == pytest.approx(simulator.data_Bbsw[2], abs=1E-2)

    assert persistent.values['modestring'].data == simulator.data_modestring
    assert persistent.values['sampling'].data == 0

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
async def test_start_zero():
    simulator: Simulator = None
    instrument: Instrument = None
    simulator, instrument = await create_streaming_instrument(Instrument, Simulator, config={
        'ZERO': False,
        'PARAMETERS': {
            'STB': 10,
            'STZ': 5,
        },
    })
    bus: BusInterface = instrument.context.bus

    simulator_run = asyncio.ensure_future(simulator.run())
    instrument_run = asyncio.ensure_future(instrument.run())

    await wait_cancelable(bus.wait_for_communicating(), 30)

    await bus.state('BswB')
    bus.state_records.pop('BswB', None)
    bus.state_records.pop('BswdB', None)

    await bus.wait_for_notification('zero', is_set=False)
    assert not simulator.is_in_zero

    bus.command('start_zero')
    await wait_cancelable(bus.wait_for_notification('zero'), 30)
    for i in range(30):
        if simulator.is_in_zero:
            break
        await asyncio.sleep(1)
    assert simulator.is_in_zero

    await wait_cancelable(bus.wait_for_notification('zero', is_set=False), 60)
    assert await bus.state('BswB') == pytest.approx(simulator.data_Bsw[0], abs=1E-2)
    assert await bus.state('BswdB') == 0.0

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
async def test_set_parameters():
    simulator: Simulator = None
    instrument: Instrument = None
    simulator, instrument = await create_streaming_instrument(Instrument, Simulator, config={
        'ZERO': False,
    })
    bus: BusInterface = instrument.context.bus

    simulator_run = asyncio.ensure_future(simulator.run())
    instrument_run = asyncio.ensure_future(instrument.run())

    await wait_cancelable(bus.wait_for_communicating(), 30)

    bus.state_records.pop('parameters', None)

    bus.command('set_parameters', {
        'STZ': 42
    })

    assert (await bus.state('parameters'))['STZ'] == 42

    for i in range(30):
        if simulator.parameters.STZ == 42:
            break
        await asyncio.sleep(1)
    assert simulator.parameters.STZ == 42

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
async def test_spancheck():
    simulator: Simulator = None
    instrument: Instrument = None
    simulator, instrument = await create_streaming_instrument(Instrument, Simulator, config={
        'ZERO': False,
        'PARAMETERS': {
            'STB': 10,
            'STZ': 5,
        },
    })
    bus: BusInterface = instrument.context.bus
    persistent: PersistentInterface = instrument.context.persistent

    simulator_run = asyncio.ensure_future(simulator.run())
    instrument_run = asyncio.ensure_future(instrument.run())

    await wait_cancelable(bus.wait_for_communicating(), 30)

    await bus.wait_for_notification('spancheck', is_set=False)

    bus.command('_spancheck_control', 'initialize')
    bus.command('_spancheck_control', 'air_flush')
    await wait_cancelable(bus.wait_for_notification('spancheck'), 30)
    for i in range(30):
        if simulator.data_Vx == 0:
            break
        await asyncio.sleep(1)
    assert simulator.data_Vx == 0

    bus.command('_spancheck_control', 'gas_flush')
    for i in range(30):
        await asyncio.sleep(1)
        if simulator.data_Vx == 5000:
            break
    assert simulator.data_Vx == 5000

    bus.command('_spancheck_control', 'gas_sample')
    bus.data_values.pop('BsG', None)
    bus.data_values.pop('Psample', None)
    await bus.value('BsG')
    await bus.value('Psample')

    bus.command('_spancheck_control', 'air_flush')
    for i in range(30):
        if simulator.data_Vx == 0:
            break
        await asyncio.sleep(1)
    assert simulator.data_Vx == 0

    bus.command('_spancheck_control', 'air_sample')
    bus.data_values.pop('BsG', None)
    bus.data_values.pop('Psample', None)
    await bus.value('BsG')
    await bus.value('Psample')

    bus.command('_spancheck_control', 'complete')
    bus.command('_spancheck_calculate', {'gas_factor': 2.61})

    await bus.wait_for_notification('spancheck', is_set=False)
    await bus.wait_for_notification('zero')

    def to_stp(value: float) -> float:
        density = (simulator.data_Psample / 1013.25) * (ZERO_C_IN_K / (simulator.data_Tsample + ZERO_C_IN_K))
        return value / density

    result = await bus.state('spancheck_result')
    assert result['pressure']['air'] == simulator.data_Psample
    assert result['scattering']['air']['total']['G'] == pytest.approx(to_stp(simulator.data_Bs[1]), abs=1E-2)
    assert result['scattering']['air']['back']['G'] == pytest.approx(to_stp(simulator.data_Bbs[1]), abs=1E-2)
    result = persistent.values['spancheck_result'].data
    assert result['pressure']['air'] == simulator.data_Psample
    assert result['scattering']['air']['total']['G'] == pytest.approx(to_stp(simulator.data_Bs[1]), abs=1E-2)
    assert result['scattering']['air']['back']['G'] == pytest.approx(to_stp(simulator.data_Bbs[1]), abs=1E-2)

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