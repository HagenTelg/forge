import typing
from forge.vis.view.timeseries import TimeSeries
from ..optical import Optical


class EditingScattering(TimeSeries):
    ThreeWavelength = Optical.ThreeWavelength

    def __init__(self, profile: str = 'aerosol'):
        super().__init__()
        self.title = "Total Light Scattering"

        raw = self.ThreeWavelength(f'{profile}-raw-scattering', 'Bs', 'Raw {code} ({size})')
        raw.title = "Raw"
        self.graphs.append(raw)

        edited = self.ThreeWavelength(f'{profile}-editing-scattering', 'Bs', 'Edited {code} ({size})')
        edited.title = "Edited"
        self.graphs.append(edited)


class EditingBackScattering(TimeSeries):
    ThreeWavelength = Optical.ThreeWavelength

    def __init__(self, profile: str = 'aerosol'):
        super().__init__()
        self.title = "Backwards-hemispheric Light Scattering"

        raw = self.ThreeWavelength(f'{profile}-raw-scattering', 'Bbs', 'Raw {code} ({size})')
        raw.title = "Raw"
        self.graphs.append(raw)

        edited = self.ThreeWavelength(f'{profile}-editing-scattering', 'Bbs', 'Edited {code} ({size})')
        edited.title = "Edited"
        self.graphs.append(edited)


class EditingAbsorption(TimeSeries):
    ThreeWavelength = Optical.ThreeWavelength

    def __init__(self, profile: str = 'aerosol'):
        super().__init__()
        self.title = "Light Absorption"

        raw = self.ThreeWavelength(f'{profile}-raw-absorption', 'Ba', 'Raw {code} ({size})')
        raw.title = "Raw"
        self.graphs.append(raw)

        edited = self.ThreeWavelength(f'{profile}-editing-absorption', 'Ba', 'Edited {code} ({size})')
        edited.title = "Edited"
        self.graphs.append(edited)
