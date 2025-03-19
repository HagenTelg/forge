import typing
from forge.vis.view.statistics import Statistics


class StatisticsOzoneConcentration(Statistics):
    def __init__(self, record: str = 'ozone', profile: str = 'ozonestats', **kwargs):
        super().__init__(record=record, profile=profile, **kwargs)
        self.title = "Ozone Concentration"
        self.units = "ppb"
        self.range = 0