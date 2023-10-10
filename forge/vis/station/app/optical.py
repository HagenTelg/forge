import typing
from forge.vis.view.timeseries import TimeSeries
from ..default.aerosol.optical import Optical


class AllScattering(TimeSeries):
    ThreeWavelength = Optical.ThreeWavelength

    def __init__(self, mode: str, **kwargs):
        super().__init__(**kwargs)
        self.title = "Nephelometer Scattering"

        neph = self.ThreeWavelength(f'{mode}-scattering', 'Bs')
        neph.title = "Total Dry Nephelometer (S11)"
        self.graphs.append(neph)

        neph = self.ThreeWavelength(f'{mode}-scattering2', 'Bs')
        neph.title = "Total Wet Nephelometer (S12)"
        self.graphs.append(neph)

        neph = self.ThreeWavelength(f'{mode}-scattering3', 'Bs')
        neph.title = "Total Ecotech Nephelometer (S13)"
        self.graphs.append(neph)

        neph = self.ThreeWavelength(f'{mode}-scattering4', 'Bs')
        neph.title = "Total Ecotech Nephelometer (S14)"
        self.graphs.append(neph)

        neph = self.ThreeWavelength(f'{mode}-scattering', 'Bbs')
        neph.title = "Backwards-hemispheric Dry Nephelometer (S11)"
        self.graphs.append(neph)

        neph = self.ThreeWavelength(f'{mode}-scattering2', 'Bbs')
        neph.title = "Backwards-hemispheric Wet Nephelometer (S12)"
        self.graphs.append(neph)

        neph = self.ThreeWavelength(f'{mode}-scattering3', 'Bbs')
        neph.title = "Backwards-hemispheric Ecotech Nephelometer (S13)"
        self.graphs.append(neph)

        neph = self.ThreeWavelength(f'{mode}-scattering4', 'Bbs')
        neph.title = "Backwards-hemispheric Ecotech Nephelometer (S14)"
        self.graphs.append(neph)


class OpticalScatteringSecondary(TimeSeries):
    ThreeWavelength = Optical.ThreeWavelength

    def __init__(self, mode: str, **kwargs):
        super().__init__(**kwargs)
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

    def __init__(self, profile: str = 'aerosol', **kwargs):
        super().__init__(**kwargs)
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

    def __init__(self, profile: str = 'aerosol', **kwargs):
        super().__init__(**kwargs)
        self.title = "Humidified Backwards-hemispheric Light Scattering"

        raw = self.ThreeWavelength(f'{profile}-raw-scattering2', 'Bbs', 'Raw {code} ({size})')
        raw.title = "Raw"
        raw.contamination = f'{profile}-raw-contamination'
        self.graphs.append(raw)

        edited = self.ThreeWavelength(f'{profile}-editing-scattering2', 'Bbs', 'Edited {code} ({size})')
        edited.title = "Edited"
        edited.contamination = f'{profile}-editing-contamination'
        self.graphs.append(edited)


class EditingScattering3(TimeSeries):
    ThreeWavelength = Optical.ThreeWavelength

    def __init__(self, profile: str = 'aerosol', **kwargs):
        super().__init__(**kwargs)
        self.title = "S13 Total Light Scattering"

        raw = self.ThreeWavelength(f'{profile}-raw-scattering3', 'Bs', 'Raw {code} ({size})')
        raw.title = "Raw"
        raw.contamination = f'{profile}-raw-contamination'
        self.graphs.append(raw)

        edited = self.ThreeWavelength(f'{profile}-editing-scattering3', 'Bs', 'Edited {code} ({size})')
        edited.title = "Edited"
        edited.contamination = f'{profile}-editing-contamination'
        self.graphs.append(edited)


class EditingBackScattering3(TimeSeries):
    ThreeWavelength = Optical.ThreeWavelength

    def __init__(self, profile: str = 'aerosol', **kwargs):
        super().__init__(**kwargs)
        self.title = "S13 Backwards-hemispheric Light Scattering"

        raw = self.ThreeWavelength(f'{profile}-raw-scattering3', 'Bbs', 'Raw {code} ({size})')
        raw.title = "Raw"
        raw.contamination = f'{profile}-raw-contamination'
        self.graphs.append(raw)

        edited = self.ThreeWavelength(f'{profile}-editing-scattering3', 'Bbs', 'Edited {code} ({size})')
        edited.title = "Edited"
        edited.contamination = f'{profile}-editing-contamination'
        self.graphs.append(edited)


class EditingScattering4(TimeSeries):
    ThreeWavelength = Optical.ThreeWavelength

    def __init__(self, profile: str = 'aerosol', **kwargs):
        super().__init__(**kwargs)
        self.title = "S14 Total Light Scattering"

        raw = self.ThreeWavelength(f'{profile}-raw-scattering4', 'Bs', 'Raw {code} ({size})')
        raw.title = "Raw"
        raw.contamination = f'{profile}-raw-contamination'
        self.graphs.append(raw)

        edited = self.ThreeWavelength(f'{profile}-editing-scattering4', 'Bs', 'Edited {code} ({size})')
        edited.title = "Edited"
        edited.contamination = f'{profile}-editing-contamination'
        self.graphs.append(edited)


class EditingBackScattering4(TimeSeries):
    ThreeWavelength = Optical.ThreeWavelength

    def __init__(self, profile: str = 'aerosol', **kwargs):
        super().__init__(**kwargs)
        self.title = "S14 Backwards-hemispheric Light Scattering"

        raw = self.ThreeWavelength(f'{profile}-raw-scattering4', 'Bbs', 'Raw {code} ({size})')
        raw.title = "Raw"
        raw.contamination = f'{profile}-raw-contamination'
        self.graphs.append(raw)

        edited = self.ThreeWavelength(f'{profile}-editing-scattering4', 'Bbs', 'Edited {code} ({size})')
        edited.title = "Edited"
        edited.contamination = f'{profile}-editing-contamination'
        self.graphs.append(edited)
