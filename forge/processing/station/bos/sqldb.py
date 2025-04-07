import typing

if typing.TYPE_CHECKING:
    from forge.product.selection import InstrumentSelection


def updates(station: str) -> typing.Dict[str, typing.Tuple[str, typing.List["InstrumentSelection"]]]:
    from forge.product.selection import InstrumentSelection
    return {
        "aerosol_hour": ("avgh", [InstrumentSelection(instrument_id=["XI"])]),
        "aerosol_day": ("avgd", [InstrumentSelection(instrument_id=["XI"])]),
        "aerosol_month": ("avgm", [InstrumentSelection(instrument_id=["XI"])]),
    }