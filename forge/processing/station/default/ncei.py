import typing

if typing.TYPE_CHECKING:
    from forge.product.ncei.file import NCEIFile
    from forge.product.selection import InstrumentSelection
    from forge.product.update.ncei import Destination


def file(station: str, type_code: str, start_epoch_ms: int, end_epoch_ms: int) -> typing.Type["NCEIFile"]:
    from forge.product.ncei.file import NCEIFile
    return NCEIFile.from_type_code(type_code)


def submit(station: str) -> typing.Dict[str, typing.Tuple[str, typing.List["InstrumentSelection"], typing.List["Destination"]]]:
    return dict()


def standard_submit(station: str) -> typing.Dict[str, typing.Tuple[str, typing.List["InstrumentSelection"], typing.List["Destination"]]]:
    from forge.product.selection import InstrumentSelection
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
        )], [SFTP("gmdaerosols"), Local("/outgoing_ftp/data/bedi/Aerosols/Data/v01/{station}")]),
    }
