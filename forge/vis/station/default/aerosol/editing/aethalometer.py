import typing
from forge.vis.view.timeseries import TimeSeries
from ..aethalometer import AethalometerOptical


class EditingAethalometer(TimeSeries):
    SevenWavelength = AethalometerOptical.SevenWavelength

    def __init__(self, profile: str = 'aerosol'):
        super().__init__()
        self.title = "Absorption Coefficient"

        raw = self.SevenWavelength("Mm⁻¹", '.2f', "Raw ({wavelength} nm)",
                                   f'{profile}-raw-aethalometer', 'Ba{index}')
        raw.title = "Raw"
        raw.contamination = f'{profile}-raw-contamination'
        self.graphs.append(raw)

        edited = self.SevenWavelength("Mm⁻¹", '.2f', "Edited ({wavelength} nm)",
                                      f'{profile}-editing-aethalometer', 'Ba{index}')
        edited.title = "Edited"
        edited.contamination = f'{profile}-editing-contamination'
        self.graphs.append(edited)
