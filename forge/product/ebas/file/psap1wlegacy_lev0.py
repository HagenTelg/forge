import typing
from .psap1w_lev0 import File as Level0File


class File(Level0File):
    @property
    def file_metadata(self) -> typing.Dict[str, str]:
        r = super().file_metadata
        r.update({
            'std_method': 'Single-angle_Correction=Weiss',
        })
        return r
