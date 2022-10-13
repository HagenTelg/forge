import asyncio
import typing
import pytest
import enum
from forge.acquisition.instrument.testing import BusInterface
from forge.acquisition.instrument.spancheck import Spancheck as BaseSpancheck


class Spancheck(BaseSpancheck):
    class ValveSetting(enum.Enum):
        AIR = 0
        GAS = 1

    class FinalState(enum.Enum):
        COMPLETE = 0
        ABORT = 1

    def __init__(self, bus: BusInterface):
        super().__init__(bus)
        self.valve: typing.Optional[Spancheck.ValveSetting] = None
        self.final_state: typing.Optional[Spancheck.FinalState] = None

    @property
    def wavelengths(self) -> typing.Iterable[float]:
        return [550.0]

    @property
    def angles(self) -> typing.Iterable[float]:
        return [0.0]

    async def set_filtered_air(self) -> None:
        self.valve = self.ValveSetting.AIR

    async def set_span_gas(self) -> None:
        self.valve = self.ValveSetting.GAS

    async def abort(self) -> None:
        self.final_state = self.FinalState.ABORT

    async def complete(self) -> None:
        self.final_state = self.FinalState.COMPLETE


@pytest.mark.asyncio
async def test_basic():
    bus = BusInterface()
    spancheck = Spancheck(bus)

    assert not spancheck.is_running
    assert spancheck.active_phase is None

    bus.command('_spancheck_control', 'initialize')
    await spancheck()
    assert spancheck.is_running
    assert spancheck.active_phase is None

    bus.command('_spancheck_control', 'air_flush')
    await spancheck()
    assert spancheck.is_running
    assert spancheck.active_phase is None
    assert spancheck.valve == Spancheck.ValveSetting.AIR

    bus.command('_spancheck_control', 'gas_flush')
    await spancheck()
    assert spancheck.is_running
    assert spancheck.active_phase is None
    assert spancheck.valve == Spancheck.ValveSetting.GAS

    expected_air_rayleigh = 12.267
    expected_CO2_rayleigh = expected_air_rayleigh * 2.61
    zero_offset = 0.5
    error_factor = 1.05
    measured_CO2 = expected_CO2_rayleigh * error_factor + zero_offset - expected_air_rayleigh
    measured_air = zero_offset

    bus.command('_spancheck_control', 'gas_sample')
    await spancheck()
    assert spancheck.is_running
    phase = spancheck.active_phase
    assert phase is not None
    phase.temperature(0.0)
    phase.pressure(1013.25)
    phase.wavelengths[550.0].angles[0.0].scattering(measured_CO2)
    assert spancheck.valve == Spancheck.ValveSetting.GAS

    bus.command('_spancheck_control', 'air_flush')
    await spancheck()
    assert spancheck.is_running
    assert spancheck.active_phase is None
    assert spancheck.valve == Spancheck.ValveSetting.AIR

    bus.command('_spancheck_control', 'air_sample')
    await spancheck()
    assert spancheck.is_running
    phase = spancheck.active_phase
    assert phase is not None
    phase.temperature(0.0)
    phase.pressure(1013.25)
    phase.wavelengths[550.0].angles[0.0].scattering(measured_air)
    assert spancheck.valve == Spancheck.ValveSetting.AIR

    bus.command('_spancheck_control', 'complete')
    await spancheck()
    assert not spancheck.is_running
    assert spancheck.active_phase is None
    assert spancheck.final_state == Spancheck.FinalState.COMPLETE

    bus.command('_spancheck_calculate', {'gas_factor': 2.61})
    assert not spancheck.is_running
    assert spancheck.active_phase is None
    result = spancheck.last_result
    assert result is not None
    data = result.output_data()
    assert data['temperature'] == {'air': 0.0, 'gas': 0.0}
    assert data['pressure'] == {'air': 1013.25, 'gas': 1013.25}
    assert data['scattering']['air']['0']['550'] == pytest.approx(measured_air, abs=0.1)
    assert data['scattering']['gas']['0']['550'] == pytest.approx(measured_CO2, abs=0.1)
    assert data['percent_error']['0']['550'] == pytest.approx((error_factor - 1.0) * 100.0, abs=0.1)
    assert result.average_percent_error() == pytest.approx((error_factor - 1.0) * 100.0, abs=0.1)


@pytest.mark.asyncio
async def test_abort():
    bus = BusInterface()
    spancheck = Spancheck(bus)

    assert not spancheck.is_running
    assert spancheck.active_phase is None

    bus.command('_spancheck_control', 'initialize')
    await spancheck()
    assert spancheck.is_running
    assert spancheck.active_phase is None

    bus.command('_spancheck_control', 'air_flush')
    await spancheck()
    assert spancheck.is_running
    assert spancheck.active_phase is None
    assert spancheck.valve == Spancheck.ValveSetting.AIR

    bus.command('_spancheck_control', 'abort')
    await spancheck()
    assert not spancheck.is_running
    assert spancheck.active_phase is None
    assert spancheck.final_state == Spancheck.FinalState.ABORT
