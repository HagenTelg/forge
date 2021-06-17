import typing
from collections import OrderedDict
from forge.vis.view.timeseries import TimeSeries
from ..met.wind import Wind as BaseWind


class Wind(BaseWind):
    def __init__(self, mode: str, measurements: typing.Optional[typing.Dict[str, str]] = None):
        if measurements is None:
            measurements = OrderedDict([
                ('{code}', '{type}'),
            ])

        super().__init__(f'{mode}-wind', measurements)

        concentration = TimeSeries.Graph()
        concentration.contamination = f'{mode}-contamination'
        self.graphs.insert(0, concentration)

        ppb = TimeSeries.Axis()
        ppb.title = "ppb"
        ppb.format_code = '.2f'
        concentration.axes.append(ppb)

        ozone = TimeSeries.Trace(ppb)
        ozone.legend = "Ozone"
        ozone.data_record = f'{mode}-ozone'
        ozone.data_field = 'ozone'
        concentration.traces.append(ozone)
