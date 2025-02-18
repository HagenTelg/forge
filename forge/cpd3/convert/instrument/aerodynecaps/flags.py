import typing
from ..default.flags import CPD3Flag


lookup: typing.Dict[str, CPD3Flag] = {
    'blank': CPD3Flag("Blank", "Data removed in blank period", 0x40000000),
    'zero': CPD3Flag("Zero", "Zero in progress", 0x20000000),
}
