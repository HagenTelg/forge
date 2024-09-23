import typing
from forge.vis.view.statistics import Statistics


class StatisticsParticleConcentration(Statistics):
    def __init__(self, record: str = 'cnc', **kwargs):
        super().__init__(record=record, **kwargs)
        self.title = "Particle Concentration"
        self.units = "cm⁻³"
        self.range = 0