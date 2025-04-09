import typing

if typing.TYPE_CHECKING:
    from nilutility.datatypes import DataObject
    from forge.product.ebas.file import EBASFile
    from forge.product.selection import InstrumentSelection


def file(gaw_station: str, type_code: str, start_epoch_ms: int, end_epoch_ms: int) -> typing.Type["EBASFile"]:
    from ..default.ebas import file
    from forge.product.ebas.file.dmtccn_lev0 import File as CCNLevel0
    from forge.product.ebas.file.dmtccn_lev1 import File as CCNLevel1

    result = file(gaw_station, type_code, start_epoch_ms, end_epoch_ms)

    if isinstance(result, CCNLevel0) or isinstance(result, CCNLevel1):
        class WithCCN200(result):
            @property
            def instrument_selection(self) -> typing.Iterable[InstrumentSelection]:
                return [InstrumentSelection(
                    instrument_type=["dmtccn", "dmtccn200"],
                    exclude_tags=["secondary"],
                )]
        return WithCCN200

    return result



