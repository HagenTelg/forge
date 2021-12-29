import typing
from collections import OrderedDict
from ..met.wind import Wind as BaseWind


class Wind(BaseWind):
    def __init__(self, mode: str, measurements: typing.Optional[typing.Dict[str, str]] = None, **kwargs):
        if measurements is None:
            measurements = OrderedDict([
                ('{code}', '{type}'),
            ])

        super().__init__(f'{mode}-wind', measurements, **kwargs)
        for graph in self.graphs:
            graph.contamination = f'{mode}-contamination'
