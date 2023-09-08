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


def event_log_lock_key(station: str) -> str:
    return f"eventlog/{station.lower()}"


def event_log_file_name(station: str, file_start: float) -> str:
    ts = time.gmtime(int(file_start))
    return f"eventlog/{station.lower()}/{ts.tm_year:04}/{station.upper()}-LOG_s{ts.tm_year:04}{ts.tm_mon:02}{ts.tm_mday:02}.nc"
