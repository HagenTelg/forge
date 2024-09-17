import typing
from forge.product.selection import InstrumentSelection


def updates(station: str) -> typing.Dict[str, typing.Tuple[str, typing.List["InstrumentSelection"]]]:
    return {
        "aerosol_hour": ("avgh", [InstrumentSelection(instrument_id=["XI"])]),
        "aerosol_day": ("avgd", [InstrumentSelection(instrument_id=["XI"])]),
        "aerosol_month": ("avgm", [InstrumentSelection(instrument_id=["XI"])]),
    }