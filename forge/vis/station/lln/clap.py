import typing
from ..default.aerosol.clap import CLAPStatus


class CLAPStatusCOSMOS(CLAPStatus):
    def __init__(self, mode: str):
        super().__init__(mode)
        for g in self.graphs:
            for t in g.traces:
                t.data_record += '2'

