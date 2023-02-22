import typing
from forge.vis.view.timeseries import TimeSeries
from ..default.aerosol.optical import Optical


class OpticalScatteringSecondary(TimeSeries):
    ThreeWavelength = Optical.ThreeWavelength

    def __init__(self, mode: str, **kwargs):
        super().__init__(**kwargs)
        self.title = "Ecotech Optical Properties"

        total_scattering = self.ThreeWavelength(f'{mode}-scattering2', 'Bs')
        total_scattering.title = "Total Light Scattering"
        total_scattering.contamination = f'{mode}-contamination'
        self.graphs.append(total_scattering)

        back_scattering = self.ThreeWavelength(f'{mode}-scattering2', 'Bbs')
        back_scattering.title = "Backwards-hemispheric Light Scattering"
        back_scattering.contamination = f'{mode}-contamination'
        self.graphs.append(back_scattering)

        absorption = self.ThreeWavelength(f'{mode}-absorption', 'Ba')
        absorption.title = "Light Absorption"
        absorption.contamination = f'{mode}-contamination'
        self.graphs.append(absorption)
