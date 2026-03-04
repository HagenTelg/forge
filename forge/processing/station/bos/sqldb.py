import typing

if typing.TYPE_CHECKING:
    from forge.product.selection import InstrumentSelection


def updates(station: str) -> typing.Dict[str, typing.Tuple[str, typing.List["InstrumentSelection"]]]:
    from forge.product.selection import InstrumentSelection
    return {
        "aerosol_hour": ("clean", [InstrumentSelection(instrument_id=["XI"])]),
        "aerosol_day": ("clean", [InstrumentSelection(instrument_id=["XI"])]),
        "aerosol_month": ("clean", [InstrumentSelection(instrument_id=["XI"])]),
    }