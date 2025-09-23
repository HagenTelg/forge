import typing

if typing.TYPE_CHECKING:
    from forge.product.selection import InstrumentSelection


def updates(station: str) -> typing.Dict[str, typing.Tuple[str, typing.List["InstrumentSelection"]]]:
    from ..default.sqldb import standard_updates
    u = standard_updates(station)
    for k in list(u.keys()):
        if k.startswith("met_"):
            del u[k]
    return u
