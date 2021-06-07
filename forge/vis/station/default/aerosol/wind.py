import typing
from collections import OrderedDict
from ..met.wind import Wind as BaseWind


class Wind(BaseWind):
    def __init__(self, mode: str, measurements: typing.Optional[typing.Dict[str, str]] = None):
        super().__init__(f'{mode}-wind', measurements)
        for graph in self.graphs:
            graph.contamination = f'{mode}-contamination'
