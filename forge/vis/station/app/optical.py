import typing
from forge.vis.view.timeseries import TimeSeries
from ..default.aerosol.optical import Optical


class OpticalScatteringSecondary(TimeSeries):
    ThreeWavelength = Optical.ThreeWavelength

    def __init__(self, mode: str):
        super().__init__()
        self.title = "Humidified Optical Properties"

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


class EditingScatteringSecondary(TimeSeries):
    ThreeWavelength = Optical.ThreeWavelength

    def __init__(self, profile: str = 'aerosol'):
        super().__init__()
        self.title = "Humidified Total Light Scattering"

        raw = self.ThreeWavelength(f'{profile}-raw-scattering2', 'Bs', 'Raw {code} ({size})')
        raw.title = "Raw"
        raw.contamination = f'{profile}-raw-contamination'
        self.graphs.append(raw)

        edited = self.ThreeWavelength(f'{profile}-editing-scattering2', 'Bs', 'Edited {code} ({size})')
        edited.title = "Edited"
        edited.contamination = f'{profile}-editing-contamination'
        self.graphs.append(edited)


class EditingBackScatteringSecondary(TimeSeries):
    ThreeWavelength = Optical.ThreeWavelength

    def __init__(self, profile: str = 'aerosol'):
        super().__init__()
        self.title = "Humidified Backwards-hemispheric Light Scattering"

        raw = self.ThreeWavelength(f'{profile}-raw-scattering2', 'Bbs', 'Raw {code} ({size})')
        raw.title = "Raw"
        raw.contamination = f'{profile}-raw-contamination'
        self.graphs.append(raw)

        edited = self.ThreeWavelength(f'{profile}-editing-scattering2', 'Bbs', 'Edited {code} ({size})')
        edited.title = "Edited"
        edited.contamination = f'{profile}-editing-contamination'
        self.graphs.append(edited)
