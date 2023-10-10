import typing
from forge.vis.view.timeseries import TimeSeries
from ..default.aerosol.ecotechnephelometer import NephelometerZero, NephelometerStatus


class NephelometerZero3(TimeSeries):
    ThreeWavelength = NephelometerZero.ThreeWavelength

    def __init__(self, mode: str, **kwargs):
        super().__init__(**kwargs)
        self.title = "S13 Nephelometer Zero Results"

        total_scattering = self.ThreeWavelength(f'{mode}-nephzero3', 'Bsw')
        total_scattering.title = "Wall Scattering"
        self.graphs.append(total_scattering)

        back_scattering = self.ThreeWavelength(f'{mode}-nephzero3', 'Bbsw')
        back_scattering.title = "Backwards-hemispheric Wall Scattering"
        self.graphs.append(back_scattering)


class NephelometerStatus3(NephelometerStatus):
    def __init__(self, mode: str, **kwargs):
        super().__init__(mode, **kwargs)
        self.title = "S13 Nephelometer Status"

        for g in self.graphs:
            for t in g.traces:
                t.data_record += '3'


class NephelometerZero4(TimeSeries):
    ThreeWavelength = NephelometerZero.ThreeWavelength

    def __init__(self, mode: str, **kwargs):
        super().__init__(**kwargs)
        self.title = "S14 Nephelometer Zero Results"

        total_scattering = self.ThreeWavelength(f'{mode}-nephzero4', 'Bsw')
        total_scattering.title = "Wall Scattering"
        self.graphs.append(total_scattering)

        back_scattering = self.ThreeWavelength(f'{mode}-nephzero4', 'Bbsw')
        back_scattering.title = "Backwards-hemispheric Wall Scattering"
        self.graphs.append(back_scattering)


class NephelometerStatus4(NephelometerStatus):
    def __init__(self, mode: str, **kwargs):
        super().__init__(mode, **kwargs)
        self.title = "S14 Nephelometer Status"

        for g in self.graphs:
            for t in g.traces:
                t.data_record += '4'
