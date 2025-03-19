import typing
from forge.vis.view.statistics import Statistics


class StatisticsOzoneConcentration(Statistics):
    def __init__(self, record: str = 'ozone', **kwargs):
        super().__init__(record=record, **kwargs)
        self.title = "Ozone Concentration"
        self.units = "ppb"
        self.range = 0