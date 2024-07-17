import typing
from forge.vis.view.timeseries import TimeSeries
from ..default.aerosol.ecotechnephelometer import NephelometerZero, NephelometerStatus


class NephelometerZeroSecondary(TimeSeries):
    ThreeWavelength = NephelometerZero.ThreeWavelength

    def __init__(self, mode: str, **kwargs):
        super().__init__(**kwargs)
        self.title = "Ecotech Nephelometer Zero Results"

        total_scattering = self.ThreeWavelength(f'{mode}-nephzero2', 'Bsw')
        total_scattering.title = "Wall Scattering"
        self.graphs.append(total_scattering)

        back_scattering = self.ThreeWavelength(f'{mode}-nephzero2', 'Bbsw')
        back_scattering.title = "Backwards-hemispheric Wall Scattering"
        self.graphs.append(back_scattering)


class NephelometerStatusSecondary(NephelometerStatus):
    def __init__(self, mode: str, **kwargs):
        super().__init__(mode, **kwargs)
        self.title = "Ecotech Nephelometer Status"

        for g in self.graphs:
            for t in g.traces:
                t.data_record += '2'
