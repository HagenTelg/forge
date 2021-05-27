import typing
from forge.vis.view.timeseries import TimeSeries
from .optical import Optical


class Extensive(TimeSeries):
    ThreeWavelength = Optical.ThreeWavelength

    def __init__(self, mode: str):
        super().__init__()
        self.title = "Extensive Parameters"

        cnc = TimeSeries.Graph()
        self.graphs.append(cnc)

        cm_3 = TimeSeries.Axis()
        cm_3.title = "cm⁻³"
        cm_3.range = 0
        cm_3.format_code = '.1f'
        cnc.axes.append(cm_3)

        n_cnc = TimeSeries.Trace(cm_3)
        n_cnc.legend = "CNC"
        n_cnc.data_record = f'{mode}-cnc'
        n_cnc.data_field = 'cnc'
        cnc.traces.append(n_cnc)

        absorption = self.ThreeWavelength(f'{mode}-absorption', 'Ba')
        absorption.title = "Light Absorption"
        self.graphs.append(absorption)

        total_scattering = self.ThreeWavelength(f'{mode}-scattering', 'Bs')
        total_scattering.title = "Total Light Scattering"
        self.graphs.append(total_scattering)
