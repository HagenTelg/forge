import typing
from forge.vis.view.timeseries import TimeSeries
from ..aethalometer import AethalometerOptical


class EditingAethalometer(TimeSeries):
    SevenWavelength = AethalometerOptical.SevenWavelength

    def __init__(self, profile: str = 'aerosol', **kwargs):
        super().__init__(**kwargs)
        self.title = "Equivalent Black Carbon"

        raw = self.SevenWavelength("μg/m³", '.2f', "Raw ({wavelength} nm)",
                                   f'{profile}-raw-aethalometer', 'X{index}')
        raw.title = "Raw"
        raw.contamination = f'{profile}-raw-contamination'
        self.graphs.append(raw)


        edited = self.SevenWavelength("μg/m³", '.2f', "Edited ({wavelength} nm)",
                                      f'{profile}-editing-aethalometer', 'X{index}')
        edited.title = "Edited"
        edited.contamination = f'{profile}-editing-contamination'
        self.graphs.append(edited)


        status = TimeSeries.Graph()
        status.title = "Transmittance"
        self.graphs.append(status)

        transmittance = TimeSeries.Axis()
        transmittance.format_code = '.7f'
        status.axes.append(transmittance)

        IrG = TimeSeries.Trace(transmittance)
        IrG.legend = "Transmittance (590 nm)"
        IrG.data_record = f'{profile}-raw-aethalometer'
        IrG.data_field = 'Ir4'
        status.traces.append(IrG)

