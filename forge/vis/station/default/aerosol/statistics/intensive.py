import typing
from forge.vis.view.statistics import Statistics


class StatisticsBackscatterFraction(Statistics):
    def __init__(self, record: str, **kwargs):
        super().__init__(record=record, **kwargs)
        self.title = "Backscatter Fraction (σbsp/σsp)"
        self.range = 0

    @classmethod
    def with_title(cls, record: str, title: str):
        p = cls(record)
        p.title = title
        return p


class StatisticsAngstromExponent(Statistics):
    def __init__(self, record: str, **kwargs):
        super().__init__(record=record, **kwargs)
        self.title = "Ångström Exponent"

    @classmethod
    def with_title(cls, record: str, title: str):
        p = cls(record)
        p.title = title
        return p


class StatisticsSingleScatteringAlbedo(Statistics):
    def __init__(self, record: str, **kwargs):
        super().__init__(record=record, **kwargs)
        self.title = "Single Scattering Albedo"

    @classmethod
    def with_title(cls, record: str, title: str):
        p = cls(record)
        p.title = title
        return p


class StatisticsSubumFraction(Statistics):
    def __init__(self, record: str, **kwargs):
        super().__init__(record=record, **kwargs)
        self.title = "Sub-μm Fraction"
        self.sub_um_fraction = True

    @classmethod
    def with_title(cls, record: str, title: str):
        p = cls(record)
        p.title = title
        return p

