import typing
from forge.vis.view.timeseries import TimeSeries
from .optical import Optical


class NephelometerZero(TimeSeries):
    class ThreeWavelength(TimeSeries.Graph):
        def __init__(self, record: str, name: str, field: typing.Optional[str] = None):
            super().__init__()

            if not field:
                field = name

            Mm_1 = TimeSeries.Axis()
            Mm_1.title = "Mm⁻¹"
            Mm_1.format_code = '.2f'
            self.axes.append(Mm_1)

            B = TimeSeries.Trace(Mm_1)
            B.legend = f"{name}B"
            B.data_record = record
            B.data_field = f'{field}B'
            B.color = '#00f'
            self.traces.append(B)

            G = TimeSeries.Trace(Mm_1)
            G.legend = f"{name}G"
            G.data_record = record
            G.data_field = f'{field}G'
            G.color = '#0f0'
            self.traces.append(G)

            R = TimeSeries.Trace(Mm_1)
            R.legend = f"{name}R"
            R.data_record = record
            R.data_field = f'{field}R'
            R.color = '#f00'
            self.traces.append(R)

    def __init__(self, mode: str, **kwargs):
        super().__init__(**kwargs)
        self.title = "Nephelometer Zero Results"

        total_scattering = self.ThreeWavelength(f'{mode}-nephzero', 'Bsw')
        total_scattering.title = "Wall Scattering"
        self.graphs.append(total_scattering)

        back_scattering = self.ThreeWavelength(f'{mode}-nephzero', 'Bbsw')
        back_scattering.title = "Backwards-hemispheric Wall Scattering"
        self.graphs.append(back_scattering)


class NephelometerStatus(TimeSeries):
    def __init__(self, mode: str, **kwargs):
        super().__init__(**kwargs)
        self.title = "Nephelometer Status"

        reference = TimeSeries.Graph()
        reference.title = "Reference Count Rate"
        self.graphs.append(reference)

        Hz = TimeSeries.Axis()
        Hz.title = "Hz"
        Hz.format_code = '.0f'
        reference.axes.append(Hz)

        CfG = TimeSeries.Trace(Hz)
        CfG.legend = "Green Reference"
        CfG.data_record = f'{mode}-nephstatus'
        CfG.data_field = 'CfG'
        reference.traces.append(CfG)


        lamp = TimeSeries.Graph()
        lamp.title = "Lamp"
        self.graphs.append(lamp)

        A = TimeSeries.Axis()
        A.title = "A"
        A.format_code = '.1f'
        lamp.axes.append(A)

        V = TimeSeries.Axis()
        V.title = "V"
        V.format_code = '.1f'
        lamp.axes.append(V)

        lamp_current = TimeSeries.Trace(A)
        lamp_current.legend = "Current"
        lamp_current.data_record = f'{mode}-nephstatus'
        lamp_current.data_field = 'Al'
        lamp.traces.append(lamp_current)

        lamp_voltage = TimeSeries.Trace(V)
        lamp_voltage.legend = "Voltage"
        lamp_voltage.data_record = f'{mode}-nephstatus'
        lamp_voltage.data_field = 'Vl'
        lamp.traces.append(lamp_voltage)

