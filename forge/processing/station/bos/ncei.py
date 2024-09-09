import typing
from forge.product.selection import InstrumentSelection
from forge.product.update.ncei import Destination


# def submit(station: str) -> typing.Dict[str, typing.Tuple[str, typing.List["InstrumentSelection"], typing.List[Destination]]]:
#     from ..default.ncei import standard_submit
#     return standard_submit(station)


def submit(station: str) -> typing.Dict[str, typing.Tuple[str, typing.List["InstrumentSelection"], typing.List[Destination]]]:
    from forge.product.update.ncei import SFTP, Local
    return {
        "aerosol": ("avgh", [InstrumentSelection(
            require_tags=["absorption"],
            exclude_tags=["secondary", "aethalometer", "thermomaap"],
        ), InstrumentSelection(
            require_tags=["scattering"],
            exclude_tags=["secondary"],
        ), InstrumentSelection(
            require_tags=["cpc"],
            exclude_tags=["secondary"],
        )], [SFTP("hageman"), Local("/tmp/ncei/{station}")]),
    }