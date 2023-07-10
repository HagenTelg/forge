import typing
from ..default.flags import CPD3Flag


lookup: typing.Dict[str, CPD3Flag] = {
    'heater_on': CPD3Flag("HeaterOn", "Heater turned on", 0x00010000),
}
