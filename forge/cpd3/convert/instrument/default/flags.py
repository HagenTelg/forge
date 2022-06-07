import typing


class CPD3Flag:
    def __init__(self, code: str, description: typing.Optional[str] = None, bit: typing.Optional[int] = None):
        self.code = code
        self.description = description
        self.bit = bit


lookup: typing.Dict[str, CPD3Flag] = dict()