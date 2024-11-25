import typing
from ..default.flags import CPD3Flag


lookup: typing.Dict[str, CPD3Flag] = {
    'zero': CPD3Flag("Zero", "Zero in progress", 0x20000000),
    'spancheck': CPD3Flag("Spancheck", "Spancheck in progress"),
}
