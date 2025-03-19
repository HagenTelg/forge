import typing
from forge.vis.view.timeseries import PublicTimeSeries


class PublicConcentrationShort(PublicTimeSeries):
    def __init__(self, mode: str = 'public-ozoneweb', **kwargs):
        super().__init__(**kwargs)

        concentration = PublicConcentrationShort.Graph()
        concentration.contamination = f'{mode}-contamination'
        self.graphs.append(concentration)

        ppb = PublicConcentrationShort.Axis()
        ppb.title = "ppb"
        ppb.format_code = '.2f'
        ppb.range = 0
        concentration.axes.append(ppb)

        ozone = PublicConcentrationShort.Trace(ppb)
        ozone.legend = "Ozone"
        ozone.data_record = f'{mode}-ozone'
        ozone.data_field = 'ozone'
        concentration.traces.append(ozone)

    @property
    def height(self) -> typing.Optional[int]:
        return 800


class PublicConcentrationLong(PublicConcentrationShort):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.average = self.Averaging.HOUR

