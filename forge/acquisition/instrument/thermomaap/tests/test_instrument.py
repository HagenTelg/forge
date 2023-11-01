import asyncio
import typing
import pytest
from forge.tasks import wait_cancelable
from forge.acquisition.instrument.testing import create_streaming_instrument, cleanup_streaming_instrument, BusInterface
from forge.acquisition.instrument.thermomaap.simulator import Simulator
from forge.acquisition.instrument.thermomaap.instrument import Instrument


@pytest.mark.asyncio
async def test_communications():
    simulator: Simulator = None
    instrument: Instrument = None
    simulator, instrument = await create_streaming_instrument(Instrument, Simulator)
    simulator.model_run_time = 6.0
    simulator.unpolled_delay = 2.0
    bus: BusInterface = instrument.context.bus

    simulator_run = asyncio.ensure_future(simulator.run())
    instrument_run = asyncio.ensure_future(instrument.run())

    await wait_cancelable(bus.wait_for_communicating(), 120)

    # assert await bus.value('Ba') == pytest.approx(simulator.data_Ba)
    assert await bus.value('Ir') == pytest.approx(simulator.data_Ir)
    assert await bus.value('Bac') == simulator.data_Bac
    assert await bus.value('If') == simulator.data_If
    assert await bus.value('Ip') == simulator.data_Ip
    assert await bus.value('Is135') == simulator.data_Is135
    assert await bus.value('Is165') == simulator.data_Is165
    assert await bus.value('SSA') == simulator.data_SSA
    assert await bus.value('Tsample') == simulator.data_Tsample
    assert await bus.value('Thead') == simulator.data_Thead
    assert await bus.value('Tsystem') == simulator.data_Tsystem
    assert await bus.value('P') == simulator.data_P
    assert await bus.value('PDorifice') == simulator.data_PDorifice
    assert await bus.value('PDvacuum') == simulator.data_PDvacuum
    assert await bus.value('Q') == simulator.data_Q
    assert await bus.value('PCT') == simulator.data_PCT

    await cleanup_streaming_instrument(simulator, instrument, instrument_run, simulator_run)


