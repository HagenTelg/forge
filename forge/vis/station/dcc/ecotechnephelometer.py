import typing
from forge.vis.view.timeseries import TimeSeries
from ..default.aerosol.ecotechnephelometer import NephelometerZero as BaseNephelometerZero


class NephelometerZero(TimeSeries):
    ThreeWavelength = BaseNephelometerZero.ThreeWavelength

    def __init__(self, mode: str, **kwargs):
        super().__init__(**kwargs)
        self.title = "Nephelometer Zero Results"

        total_scattering = self.ThreeWavelength(f'{mode}-nephzero', 'Bsz')
        total_scattering.title = "Total Scattering Offset"
        self.graphs.append(total_scattering)

        back_scattering = self.ThreeWavelength(f'{mode}-nephzero', 'Bbsz')
        back_scattering.title = "Backwards-hemispheric Scattering Offset"
        self.graphs.append(back_scattering)

