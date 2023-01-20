import typing
from ..default.flags import CPD3Flag


lookup: typing.Dict[str, CPD3Flag] = {
    'spot_advancing': CPD3Flag("SpotAdvanced", "The spot has just advanced and data may be suspect"),
}
