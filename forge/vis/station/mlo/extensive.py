import typing
from ..default.aerosol.extensive import TimeSeries, Optical
from ..default.aerosol.aethalometer import AethalometerOptical


class ExtensiveSecondary(TimeSeries):
    ThreeWavelength = Optical.ThreeWavelength
    SevenWavelength = AethalometerOptical.SevenWavelength

    def __init__(self, mode: str, **kwargs):
        super().__init__(**kwargs)
        self.title = "Secondary Extensive Parameters"

        cnc = TimeSeries.Graph()
        cnc.contamination = f'{mode}-contamination'
        self.graphs.append(cnc)

        cm_3 = TimeSeries.Axis()
        cm_3.title = "cm⁻³"
        cm_3.range = 0
        cm_3.format_code = '.1f'
        cnc.axes.append(cm_3)

        n_cnc = TimeSeries.Trace(cm_3)
        n_cnc.legend = "CNC2 (MAGIC)"
        n_cnc.data_record = f'{mode}-cnc'
        n_cnc.data_field = 'cnc2'
        cnc.traces.append(n_cnc)

        ebc = self.SevenWavelength("μg/m³", '.3f', "EBC ({wavelength} nm)", f'{mode}-aethalometer', 'X{index}')
        ebc.title = "Equivalent Black Carbon"
        ebc.contamination = f'{mode}-contamination'
        self.graphs.append(ebc)

        total_scattering = self.ThreeWavelength(f'{mode}-scattering', 'Bs')
        total_scattering.title = "Total Light Scattering"
        total_scattering.contamination = f'{mode}-contamination'
        self.graphs.append(total_scattering)
