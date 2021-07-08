import typing
from ..default.aerosol.aethalometer import AethalometerOptical
from ..default.aerosol.aethalometer import AE33 as AE33Base
from ..default.aerosol.aethalometer import AE33Status as AE33StatusBase
from ..default.aerosol.aethalometer import AE33OpticalStatus as AE33OpticalStatusBase
from ..default.aerosol.aethalometer import AE31 as AE31Base
from ..default.aerosol.aethalometer import AE31Status as AE31StatusBase
from ..default.aerosol.aethalometer import AE31OpticalStatus as AE31OpticalStatusBase
from ..default.aerosol.editing.aethalometer import EditingAethalometer


class AE33Optical(AethalometerOptical):
    def __init__(self, mode: str):
        super().__init__(mode)
        for g in self.graphs:
            for t in g.traces:
                t.data_record = t.data_record.replace('-aethalometer', '-ae33')


class AE33(AE33Base):
    def __init__(self, mode: str):
        super().__init__(mode)
        for g in self.graphs:
            for t in g.traces:
                t.data_record = t.data_record.replace('-aethalometer', '-ae33')


class AE33Status(AE33StatusBase):
    def __init__(self, mode: str):
        super().__init__(mode)
        for g in self.graphs:
            for t in g.traces:
                t.data_record = t.data_record.replace('-aethalometer', '-ae33')


class AE33OpticalStatus(AE33OpticalStatusBase):
    def __init__(self, mode: str):
        super().__init__(mode)
        for g in self.graphs:
            for t in g.traces:
                t.data_record = t.data_record.replace('-aethalometer', '-ae33')


class EditingAE33(EditingAethalometer):
    def __init__(self, profile: str = 'aerosol'):
        super().__init__(profile)
        for g in self.graphs:
            for t in g.traces:
                t.data_record = t.data_record.replace('-aethalometer', '-ae33')


class AE31Optical(AethalometerOptical):
    def __init__(self, mode: str):
        super().__init__(mode)
        for g in self.graphs:
            for t in g.traces:
                t.data_record = t.data_record.replace('-aethalometer', '-ae31')


class AE31(AE31Base):
    def __init__(self, mode: str):
        super().__init__(mode)
        for g in self.graphs:
            for t in g.traces:
                t.data_record = t.data_record.replace('-aethalometer', '-ae31')


class AE31Status(AE31StatusBase):
    def __init__(self, mode: str):
        super().__init__(mode)
        for g in self.graphs:
            for t in g.traces:
                t.data_record = t.data_record.replace('-aethalometer', '-ae31')


class AE31OpticalStatus(AE31OpticalStatusBase):
    def __init__(self, mode: str):
        super().__init__(mode)
        for g in self.graphs:
            for t in g.traces:
                t.data_record = t.data_record.replace('-aethalometer', '-ae31')


class EditingAE31(EditingAethalometer):
    def __init__(self, profile: str = 'aerosol'):
        super().__init__(profile)
        for g in self.graphs:
            for t in g.traces:
                t.data_record = t.data_record.replace('-aethalometer', '-ae31')
