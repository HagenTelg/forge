import typing
from forge.vis.view.statistics import Statistics


class StatisticsScattering(Statistics):
    def __init__(self, record: str, **kwargs):
        super().__init__(record=record, **kwargs)
        self.title = "Light Scattering"
        self.units = "Mm⁻¹"

    @classmethod
    def with_title(cls, record: str, title: str):
        p = cls(record)
        p.title = title
        return p


class StatisticsAbsorption(Statistics):
    def __init__(self, record: str, **kwargs):
        super().__init__(record=record, **kwargs)
        self.title = "Light Absorption"
        self.units = "Mm⁻¹"

    @classmethod
    def with_title(cls, record: str, title: str):
        p = cls(record)
        p.title = title
        return p