import typing
from .basic import BasicEntry, BasicRecord, Severity
from forge.formattime import format_simple_duration
from forge.formatsize import format_bytes
from html import escape as html_escape


class FileIngestEntry(BasicEntry):
    class FileProcessed(BasicEntry.Event):
        @property
        def is_processed_file(self) -> bool:
            return True

        @property
        def affects_status(self) -> bool:
            if self.severity == Severity.INFO:
                return False
            return super().affects_status

        @property
        def file_name(self) -> str:
            if not self.data:
                return "UNKNOWN"
            parts = self.data.split(',', 2)
            return parts[0]

        @property
        def html_file_name(self) -> str:
            name = self.file_name
            name = html_escape(name)
            return name.replace('.', '<span>.</span>')

        @property
        def file_size(self) -> typing.Optional[int]:
            if not self.data:
                return None
            try:
                name, size, elapsed = self.data.split(',', 2)
                return int(size)
            except (ValueError, TypeError):
                pass
            return None

        @property
        def file_size_display(self) -> str:
            size = self.file_size
            if size is None:
                return ""
            return format_bytes(size)

        @property
        def elapsed_time_ms(self) -> typing.Optional[int]:
            try:
                name, size, elapsed = self.data.split(',', 2)
                return int(elapsed)
            except (ValueError, TypeError):
                pass
            return None

        @property
        def elapsed_time(self) -> typing.Optional[float]:
            elapsed = self.elapsed_time_ms
            if elapsed is None:
                return None
            return elapsed / 1000.0

        @property
        def elapsed_time_display(self) -> str:
            elapsed = self.elapsed_time
            if elapsed is None:
                return ""
            return format_simple_duration(elapsed, milliseconds=True)

        @property
        def file_state_title(self) -> typing.Optional[str]:
            return None

    class FileCorrupted(FileProcessed):
        @property
        def file_state_title(self) -> typing.Optional[str]:
            return "File corrupted"

    class FileError(FileProcessed):
        @property
        def file_state_title(self) -> typing.Optional[str]:
            return "Error processing file"


    EVENT_CODES = {
        'file-processed': FileProcessed,
        'file-corrupted': FileCorrupted,
        'file-error': FileError,
    }


class FileIngestRecord(BasicRecord):
    DETAILS_TEMPLATE = 'ingest.html'
    EMAIL_TEXT_TEMPLATE = 'ingest.txt'
    EMAIL_HTML_TEMPLATE = 'ingest.html'
    ENTRY: typing.Type[FileIngestEntry] = FileIngestEntry
