import asyncio
import typing
import pytest
from forge.units import ZERO_C_IN_K
from forge.tasks import wait_cancelable
from forge.acquisition.instrument.testing import create_streaming_instrument, cleanup_streaming_instrument, BusInterface, PersistentInterface
from forge.acquisition.instrument.acoemnex00nephelometer.simulator import Simulator
from forge.acquisition.instrument.acoemnex00nephelometer.instrument import Instrument



@pytest.mark.asyncio
async def test_communications():
    simulator: Simulator = None
    instrument: Instrument = None
    simulator, instrument = await create_streaming_instrument(Instrument, Simulator)
    bus: BusInterface = instrument.context.bus
    persistent: PersistentInterface = instrument.context.persistent

    simulator_run = asyncio.ensure_future(simulator.run())
    instrument_run = asyncio.ensure_future(instrument.run())

    await wait_cancelable(bus.wait_for_communicating(), 30)

    assert await bus.value('Tsample') == pytest.approx(simulator.data_Tsample)
    assert await bus.value('Usample') == pytest.approx(simulator.data_Usample)
    assert await bus.value('Psample') == pytest.approx(simulator.data_Psample)
    assert await bus.value('Tchassis') == pytest.approx(simulator.data_Tchassis)
    assert await bus.value('Uchassis') == pytest.approx(simulator.data_Uchassis)
    assert await bus.value('Pchassis') == pytest.approx(simulator.data_Pchassis)
    assert await bus.value('Cd') == pytest.approx(simulator.data_Cd)

    assert await bus.value('BsB') == pytest.approx(simulator.data_Bs[0])
    assert await bus.value('BsG') == pytest.approx(simulator.data_Bs[1])
    assert await bus.value('BsR') == pytest.approx(simulator.data_Bs[2])
    assert await bus.value('BbsB') == pytest.approx(simulator.data_Bbs[0])
    assert await bus.value('BbsG') == pytest.approx(simulator.data_Bbs[1])
    assert await bus.value('BbsR') == pytest.approx(simulator.data_Bbs[2])

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

    assert persistent.values['sampling'].data == 0

    await cleanup_streaming_instrument(simulator, instrument, instrument_run, simulator_run)


@pytest.mark.asyncio
async def test_zero():
    simulator: Simulator = None
    instrument: Instrument = None
    simulator, instrument = await create_streaming_instrument(Instrument, Simulator)
    bus: BusInterface = instrument.context.bus

    simulator_run = asyncio.ensure_future(simulator.run())
    instrument_run = asyncio.ensure_future(instrument.run())

    await wait_cancelable(bus.wait_for_communicating(), 30)

    await bus.state('BswB')
    bus.state_records.pop('BswB', None)
    bus.state_records.pop('BswdB', None)

    await bus.wait_for_notification('zero', is_set=False)
    assert not simulator.is_in_zero

    await simulator.start_zero()

    await wait_cancelable(bus.wait_for_notification('zero'), 30)
    for i in range(30):
        if simulator.is_in_zero:
            break
        await asyncio.sleep(1)
    else:
        assert False

    await wait_cancelable(bus.wait_for_notification('zero', is_set=False), 60)
    for i in range(10):
        if bus.state_records.pop('BswB', None) is not None:
            break
        await asyncio.sleep(1)
    else:
        assert False
    for i in range(10):
        if bus.state_records.pop('BswdB', None) is not None:
            break
        await asyncio.sleep(1)
    else:
        assert False

    await cleanup_streaming_instrument(simulator, instrument, instrument_run, simulator_run)


@pytest.mark.asyncio
async def test_polar():
    simulator: Simulator = None
    instrument: Instrument = None
    simulator, instrument = await create_streaming_instrument(Instrument, Simulator)
    bus: BusInterface = instrument.context.bus
    persistent: PersistentInterface = instrument.context.persistent
    simulator.make_polar()

    simulator_run = asyncio.ensure_future(simulator.run())
    instrument_run = asyncio.ensure_future(instrument.run())

    await wait_cancelable(bus.wait_for_communicating(), 30)

    assert await bus.value('BsB') == simulator.data_Bs[0]

    await cleanup_streaming_instrument(simulator, instrument, instrument_run, simulator_run)
