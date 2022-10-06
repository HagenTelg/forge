import typing
from .base import BaseInstrument, BaseDataOutput
from .state import Persistent


class Dimension(BaseInstrument.Dimension):
    class Field(BaseDataOutput.ArrayFloat):
        def __init__(self, name: str):
            super().__init__(name)
            self.dim: typing.Optional[Dimension] = None
            self.template = BaseDataOutput.Field.Template.DIMENSION

        @property
        def value(self) -> typing.List[float]:
            v = self.dim.source.value
            if not isinstance(v, list):
                return []
            return v

    def __init__(self, instrument: BaseInstrument, source: Persistent,
                 name: str, code: typing.Optional[str], attributes: typing.Dict[str, typing.Any]):
        super().__init__(instrument, name, code, attributes)
        self.data.dim = self
        self.source = source
        if self.source.value is None:
            self.source.value = list()

    def __getitem__(self, key):
        return self.source.value[key]



