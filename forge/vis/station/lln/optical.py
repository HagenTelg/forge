import typing
from forge.vis.view.timeseries import TimeSeries
from ..default.aerosol.optical import Optical


class OpticalCLAP(TimeSeries):
    ThreeWavelength = Optical.ThreeWavelength

    def __init__(self, mode: str, **kwargs):
        super().__init__(**kwargs)
        self.title = "CLAP Optical Properties"

        total_scattering = self.ThreeWavelength(f'{mode}-scattering', 'Bs')
        total_scattering.title = "Total Light Scattering"
        total_scattering.contamination = f'{mode}-contamination'
        self.graphs.append(total_scattering)

        back_scattering = self.ThreeWavelength(f'{mode}-scattering', 'Bbs')
        back_scattering.title = "Backwards-hemispheric Light Scattering"
        back_scattering.contamination = f'{mode}-contamination'
        self.graphs.append(back_scattering)

        absorption = self.ThreeWavelength(f'{mode}-clap', 'Ba')
        absorption.title = "CLAP Light Absorption"
        absorption.contamination = f'{mode}-contamination'
        self.graphs.append(absorption)


class OpticalPSAP(TimeSeries):
    ThreeWavelength = Optical.ThreeWavelength

    def __init__(self, mode: str, **kwargs):
        super().__init__(**kwargs)
        self.title = "PSAP Optical Properties"

        total_scattering = self.ThreeWavelength(f'{mode}-scattering', 'Bs')
        total_scattering.title = "Total Light Scattering"
        total_scattering.contamination = f'{mode}-contamination'
        self.graphs.append(total_scattering)

        back_scattering = self.ThreeWavelength(f'{mode}-scattering', 'Bbs')
        back_scattering.title = "Backwards-hemispheric Light Scattering"
        back_scattering.contamination = f'{mode}-contamination'
        self.graphs.append(back_scattering)

        absorption = self.ThreeWavelength(f'{mode}-absorption', 'Ba')
        absorption.title = "PSAP Light Absorption"
        absorption.contamination = f'{mode}-contamination'
        self.graphs.append(absorption)


class OpticalCOSMOS(TimeSeries):
    ThreeWavelength = Optical.ThreeWavelength

    def __init__(self, mode: str, **kwargs):
        super().__init__(**kwargs)
        self.title = "COSMOS Absorption Comparison"

        absorption = self.ThreeWavelength(f'{mode}-clap', 'Ba')
        absorption.title = "Aerosol CLAP"
        absorption.contamination = f'{mode}-contamination'
        self.graphs.append(absorption)

        absorption = self.ThreeWavelength(f'{mode}-clap2', 'Ba')
        absorption.title = "COSMOS CLAP"
        absorption.contamination = f'{mode}-contamination'
        self.graphs.append(absorption)


class EditingCLAP(TimeSeries):
    ThreeWavelength = Optical.ThreeWavelength

    def __init__(self, profile: str = 'aerosol', **kwargs):
        super().__init__(**kwargs)
        self.title = "CLAP Light Absorption"

        raw = self.ThreeWavelength(f'{profile}-raw-clap', 'Ba', 'Raw {code} ({size})')
        raw.title = "Raw"
        raw.contamination = f'{profile}-raw-contamination'
        self.graphs.append(raw)

        edited = self.ThreeWavelength(f'{profile}-editing-clap', 'Ba', 'Edited {code} ({size})')
        edited.title = "Edited"
        edited.contamination = f'{profile}-editing-contamination'
        self.graphs.append(edited)


class EditingPSAP(TimeSeries):
    ThreeWavelength = Optical.ThreeWavelength

    def __init__(self, profile: str = 'aerosol', **kwargs):
        super().__init__(**kwargs)
        self.title = "PSAP Light Absorption"

        raw = self.ThreeWavelength(f'{profile}-raw-absorption', 'Ba', 'Raw {code} ({size})')
        raw.title = "Raw"
        raw.contamination = f'{profile}-raw-contamination'
        self.graphs.append(raw)

        edited = self.ThreeWavelength(f'{profile}-editing-absorption', 'Ba', 'Edited {code} ({size})')
        edited.title = "Edited"
        edited.contamination = f'{profile}-editing-contamination'
        self.graphs.append(edited)


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
