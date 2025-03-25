import typing
import time


def data_lock_key(station: str, archive: str) -> str:
    return f"data/{station.lower()}/{archive.lower()}/file"


def data_notification_key(station: str, archive: str) -> str:
    return f"data/{station.lower()}/{archive.lower()}"


def data_file_name(station: str, archive: str, instrument_id: str, file_start: float) -> str:
    ts = time.gmtime(int(file_start))
    return f"data/{station.lower()}/{archive.lower()}/{ts.tm_year:04}/{station.upper()}-{instrument_id}_s{ts.tm_year:04}{ts.tm_mon:02}{ts.tm_mday:02}.nc"


def index_lock_key(station: str, archive: str) -> str:
    return f"data/{station.lower()}/{archive.lower()}/index"


def index_file_name(station: str, archive: str, year_start: float) -> str:
    ts = time.gmtime(int(year_start))
    return f"data/{station.lower()}/{archive.lower()}/{ts.tm_year:04}/_index.json"


def index_instrument_history_file_name(station: str, archive: str, year_start: float) -> str:
    assert archive == "raw"
    ts = time.gmtime(int(year_start))
    return f"data/{station.lower()}/raw/{ts.tm_year:04}/_history.json"


def event_log_lock_key(station: str) -> str:
    return f"eventlog/{station.lower()}"


def event_log_file_name(station: str, file_start: float) -> str:
    ts = time.gmtime(int(file_start))
    return f"eventlog/{station.lower()}/{ts.tm_year:04}/{station.upper()}-LOG_s{ts.tm_year:04}{ts.tm_mon:02}{ts.tm_mday:02}.nc"


def edit_directives_lock_key(station: str) -> str:
    return f"edits/{station.lower()}"


def edit_directives_notification_key(station: str) -> str:
    return f"edits/{station.lower()}"


def edit_directives_file_name(station: str, file_start: typing.Optional[float]) -> str:
    if file_start is None:
        return f"edits/{station.lower()}/{station.upper()}-EDITS_UNBOUNDED.nc"
    ts = time.gmtime(int(file_start))
    return f"edits/{station.lower()}/{station.upper()}-EDITS_s{ts.tm_year:04}0101.nc"


def passed_lock_key(station: str) -> str:
    return f"passed/{station.lower()}"


def passed_notification_key(station: str) -> str:
    return f"passed/{station.lower()}"


def passed_file_name(station: str, file_start: float) -> str:
    ts = time.gmtime(int(file_start))
    return f"passed/{station.lower()}/{station.upper()}-PASSED_s{ts.tm_year:04}0101.nc"
