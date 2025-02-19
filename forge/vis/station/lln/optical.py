import typing
from forge.vis.view.timeseries import TimeSeries
from ..default.aerosol.optical import Optical


class OpticalCOSMOS(TimeSeries):
    ThreeWavelength = Optical.ThreeWavelength

    def __init__(self, mode: str, **kwargs):
        super().__init__(**kwargs)
        self.title = "COSMOS Absorption Comparison"

        absorption = self.ThreeWavelength(f'{mode}-absorption', 'Ba')
        absorption.title = "Aerosol CLAP"
        absorption.contamination = f'{mode}-contamination'
        self.graphs.append(absorption)

        absorption = self.ThreeWavelength(f'{mode}-clap2', 'Ba')
        absorption.title = "COSMOS CLAP"
        absorption.contamination = f'{mode}-contamination'
        self.graphs.append(absorption)


class EditingCOSMOS(TimeSeries):
    ThreeWavelength = Optical.ThreeWavelength

    def __init__(self, profile: str = 'aerosol', **kwargs):
        super().__init__(**kwargs)
        self.title = "CLAP Light Absorption"

        raw = self.ThreeWavelength(f'{profile}-raw-clap2', 'Ba', 'Raw {code} ({size})')
        raw.title = "Raw"
        raw.contamination = f'{profile}-raw-contamination'
        self.graphs.append(raw)

        edited = self.ThreeWavelength(f'{profile}-editing-clap2', 'Ba', 'Edited {code} ({size})')
        edited.title = "Edited"
        edited.contamination = f'{profile}-editing-contamination'
        self.graphs.append(edited)
