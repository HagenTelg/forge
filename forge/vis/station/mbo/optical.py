import typing
from forge.vis.view.timeseries import TimeSeries
from ..default.aerosol.optical import Optical


class OpticalTAP(TimeSeries):
    ThreeWavelength = Optical.ThreeWavelength

    def __init__(self, mode: str):
        super().__init__()
        self.title = "TAP Optical Properties"

        total_scattering = self.ThreeWavelength(f'{mode}-scattering', 'Bs')
        total_scattering.title = "Total Light Scattering"
        total_scattering.contamination = f'{mode}-contamination'
        self.graphs.append(total_scattering)

        back_scattering = self.ThreeWavelength(f'{mode}-scattering', 'Bbs')
        back_scattering.title = "Backwards-hemispheric Light Scattering"
        back_scattering.contamination = f'{mode}-contamination'
        self.graphs.append(back_scattering)

        absorption = self.ThreeWavelength(f'{mode}-tap', 'Ba')
        absorption.title = "Light Absorption"
        absorption.contamination = f'{mode}-contamination'
        self.graphs.append(absorption)


class EditingTAP(TimeSeries):
    ThreeWavelength = Optical.ThreeWavelength

    def __init__(self, profile: str = 'aerosol'):
        super().__init__()
        self.title = "TAP Light Absorption"

        raw = self.ThreeWavelength(f'{profile}-raw-tap', 'Ba', 'Raw {code} ({size})')
        raw.title = "Raw"
        raw.contamination = f'{profile}-raw-contamination'
        self.graphs.append(raw)

        edited = self.ThreeWavelength(f'{profile}-editing-tap', 'Ba', 'Edited {code} ({size})')
        edited.title = "Edited"
        edited.contamination = f'{profile}-editing-contamination'
        self.graphs.append(edited)

