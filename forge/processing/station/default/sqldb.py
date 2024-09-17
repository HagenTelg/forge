import typing
from forge.product.selection import InstrumentSelection

if typing.TYPE_CHECKING:
    from forge.product.sqldb import TableUpdate


def table_update(station: str, table_code: str, start_epoch_ms: int, end_epoch_ms: int) -> "TableUpdate":
    from forge.product.sqldb import TableUpdate
    from forge.product.update import CONFIGURATION

    if table_code.startswith("aerosol_"):
        uri = CONFIGURATION["SQLDB.UPDATE.AEROSOL.DATABASE"]
        password_file = CONFIGURATION.get("SQLDB.UPDATE.AEROSOL.PASSWORD_FILE")
    elif table_code.startswith("met_"):
        uri = CONFIGURATION["SQLDB.UPDATE.MET.DATABASE"]
        password_file = CONFIGURATION.get("SQLDB.UPDATE.MET.PASSWORD_FILE")
    else:
        raise FileNotFoundError

    return TableUpdate.from_type_code(table_code)(station, start_epoch_ms, end_epoch_ms, uri, password_file)


def updates(station: str) -> typing.Dict[str, typing.Tuple[str, typing.List["InstrumentSelection"]]]:
    return dict()


def standard_updates(station: str) -> typing.Dict[str, typing.Tuple[str, typing.List["InstrumentSelection"]]]:
    return {
        "aerosol_hour": ("avgh", [InstrumentSelection(instrument_id=["XI"])]),
        "aerosol_day": ("avgd", [InstrumentSelection(instrument_id=["XI"])]),
        "aerosol_month": ("avgm", [InstrumentSelection(instrument_id=["XI"])]),

        "met_minute": ("clean", [InstrumentSelection(instrument_id=["XM1"])]),
        "met_hour": ("avgh", [InstrumentSelection(instrument_id=["XM1"])]),
    }
