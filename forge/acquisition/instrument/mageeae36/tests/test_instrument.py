import asyncio
import typing
import pytest
from forge.tasks import wait_cancelable
from forge.acquisition.instrument.testing import create_http_instrument, cleanup_http_instrument, BusInterface
from forge.acquisition.instrument.testing import create_streaming_instrument, cleanup_streaming_instrument
from forge.acquisition.instrument.mageeae36.simulator_uidep import Simulator as SimulatorUIDEP
from forge.acquisition.instrument.mageeae36.simulator_adp import Simulator as SimulatorADP
from forge.acquisition.instrument.mageeae36.instrument import InstrumentUIDEP, InstrumentADP


@pytest.mark.asyncio
async def test_uidep():
    simulator: SimulatorUIDEP = None
    instrument: InstrumentUIDEP = None
    simulator, instrument = await create_http_instrument(InstrumentUIDEP, SimulatorUIDEP)
    bus: BusInterface = instrument.context.bus

    simulator_run = asyncio.ensure_future(simulator.run())
    instrument_run = asyncio.ensure_future(instrument.run())

    await wait_cancelable(bus.wait_for_communicating(), 30)

    assert await bus.value('Q1') == simulator.data_Q1
    assert await bus.value('Q2') == simulator.data_Q2
    assert await bus.value('Tinlet') == simulator.data_Tinlet
    assert await bus.value('Uinlet') == simulator.data_Uinlet
    assert await bus.value('Tcontroller') == simulator.data_Tcontroller
    assert await bus.value('Tled') == simulator.data_Tled
    assert await bus.value('Tsource') == simulator.data_Tsource
    assert await bus.value('Ttape') == simulator.data_Ttape
    assert await bus.value('Utape') == simulator.data_Utape
    assert await bus.value('PCT') == simulator.data_PCT
    assert await bus.state('Fn') == simulator.data_Fn

    assert await bus.value('X1') == simulator.data_X1
    assert await bus.value('Xa1') == simulator.data_Xa1
    assert await bus.value('Xb1') == simulator.data_Xb1
    assert await bus.value('k1') == simulator.data_k1
    assert await bus.value('If1') == simulator.data_If1
    assert await bus.value('Ip1') == simulator.data_Ip1
    assert await bus.value('Ips1') == simulator.data_Ips1

    await cleanup_http_instrument(simulator, instrument, instrument_run, simulator_run)


@pytest.mark.asyncio
async def test_adp():
    simulator: SimulatorADP = None
    instrument: InstrumentADP = None
    simulator, instrument = await create_streaming_instrument(InstrumentADP, SimulatorADP)
    bus: BusInterface = instrument.context.bus

    simulator_run = asyncio.ensure_future(simulator.run())
    instrument_run = asyncio.ensure_future(instrument.run())

    await wait_cancelable(bus.wait_for_communicating(), 30)

    assert await bus.value('Q1') == simulator.data_Q1
    assert await bus.value('Q2') == simulator.data_Q2
    assert await bus.value('Tinlet') == simulator.data_Tinlet
    assert await bus.value('Uinlet') == simulator.data_Uinlet
    assert await bus.value('Tcontroller') == simulator.data_Tcontroller
    assert await bus.value('Tled') == simulator.data_Tled
    assert await bus.value('Tsource') == simulator.data_Tsource
    assert await bus.value('Ttape') == simulator.data_Ttape
    assert await bus.value('Utape') == simulator.data_Utape
    assert await bus.value('PCT') == simulator.data_PCT
    assert await bus.state('Fn') == simulator.data_Fn

    assert await bus.value('X1') == simulator.data_X1
    assert await bus.value('Xa1') == simulator.data_Xa1
    assert await bus.value('Xb1') == simulator.data_Xb1
    assert await bus.value('k1') == simulator.data_k1
    assert await bus.value('If1') == simulator.data_If1
    assert await bus.value('Ip1') == simulator.data_Ip1
    assert await bus.value('Ips1') == simulator.data_Ips1

    await cleanup_streaming_instrument(simulator, instrument, instrument_run, simulator_run)
