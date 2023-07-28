import typing
from ..default.aerosol.clap import CLAPStatus


class CLAPStatusSecondary(CLAPStatus):
    def __init__(self, mode: str, **kwargs):
        super().__init__(mode, **kwargs)
        self.title = "TAP Status"
        for g in self.graphs:
            for t in g.traces:
                t.data_record += '2'
