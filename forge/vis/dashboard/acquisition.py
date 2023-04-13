import typing
from math import isfinite
from starlette.requests import Request
from starlette.responses import Response
from forge.vis.station.lookup import station_data
from forge.vis.mode.permissions import is_available as mode_available
from forge.processing.station.lookup import station_data as processing_data
from forge.processing.instrument.lookup import instrument_data as processing_instrument
from forge.processing.instrument.default.flags import DashboardFlag
from forge.dashboard import CONFIGURATION
from .basic import DisplayInterface, BasicEntry, DatabaseCondition, EmailContents, Status, Severity
from .fileingest import FileIngestEntry, FileIngestRecord
from .telemetry import get_station_time_offset, TelemetryInterface, ProcessingInterface


def _get_dashboard_flag(station: typing.Optional[str], condition_code: str) -> typing.Optional[DashboardFlag]:
    try:
        instrument_id, instrument_type, flag = condition_code.split('-', 2)
    except ValueError:
        return None
    if station:
        return processing_data(station, 'instrument', 'dashboard_flag')(
            station, instrument_id, instrument_type, flag)
    else:
        return processing_instrument(instrument_type, 'flags', 'dashboard_flags').get(flag)


class AcquisitionIngestEntry(FileIngestEntry):
    TIME_UNSYNC_THRESHOLD = 2 * 60

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._time_synchronization_error_cached: typing.Optional[int] = None
        self._time_synchronization_error_checked = False
        if self.station:
            self._lookup_dashboard_flag = processing_data(self.station, 'instrument', 'dashboard_flag')
        else:
            from forge.processing.station.default.instrument import dashboard_flag
            self._lookup_dashboard_flag = dashboard_flag

    class CLAPFinalSpot(FileIngestEntry.Notification):
        @property
        def display(self) -> str:
            try:
                instrument, _ = self.code.split('-', 1)
            except ValueError:
                return "The CLAP is on the final spot"
            return f"CLAP {instrument} is on the final spot"

        @property
        def detail(self) -> typing.Optional[str]:
            return "Please change the filter."

    class DataWatchdog(FileIngestEntry.Watchdog):
        @property
        def display(self) -> str:
            try:
                instrument, _ = self.code.split('-', 1)
            except ValueError:
                return super().display
            return f"{instrument} has stopped reporting data"

        @property
        def detail(self) -> typing.Optional[str]:
            return "Please verify instrument status and communications."

    class InstrumentCondition(FileIngestEntry.Condition):
        DISPLAY_TEXT: str = "{instrument} {flag}"
        DETAILS_TEXT: typing.Optional[str] = None

        @property
        def is_instrument_condition(self) -> bool:
            return True

        @classmethod
        def from_db(cls, entry: "BasicEntry", last: DatabaseCondition,
                    begin_present: float, end_present: float,
                    total_seconds: float) -> typing.Optional["BasicEntry.Condition"]:
            if total_seconds < 1800.0:
                return None
            return cls(entry, last.code, last.severity, begin_present, end_present, total_seconds, last.data)

        @property
        def instrument(self) -> str:
            try:
                instrument, _, flag = self.code.split('-', 2)
                return instrument
            except ValueError:
                pass
            return ""

        @property
        def display(self) -> str:
            try:
                instrument, _, flag = self.code.split('-', 2)
            except ValueError:
                return super().display
            return self.DISPLAY_TEXT.format(instrument=instrument, flag=flag)

        @property
        def detail(self) -> typing.Optional[str]:
            return self.DETAILS_TEXT

        @classmethod
        def flag_override(cls, display: str,
                          details: typing.Optional[str] = None) -> typing.Type["AcquisitionIngestEntry.Condition"]:
            class Override(cls):
                DISPLAY_TEXT = display
                DETAILS_TEXT = details
            return Override

    class FileProcessed(FileIngestEntry.FileProcessed):
        pass

    class FileUnauthorized(FileIngestEntry.FileProcessed):
        @property
        def file_state_title(self) -> typing.Optional[str]:
            return "File not authorized for processing"

    class FileCorrupted(FileIngestEntry.FileProcessed):
        @property
        def file_state_title(self) -> typing.Optional[str]:
            return "File corrupted"

    class FileError(FileIngestEntry.FileProcessed):
        @property
        def file_state_title(self) -> typing.Optional[str]:
            return "Error processing file"

    class Spancheck(BasicEntry.Event):
        @property
        def percent_error(self) -> float:
            if not self.data:
                return 0
            try:
                average_percent_error, _ = self.data.split(',', 1)
                return float(average_percent_error)
            except (ValueError, TypeError):
                return 0

        @property
        def display(self) -> str:
            try:
                instrument, _ = self.code.split('-', 1)
            except ValueError:
                return f"Spancheck - {self.percent_error:.1f}% average error"
            return f"{instrument} Spancheck - {self.percent_error:.1f}% average error"

        @property
        def detail(self) -> typing.Optional[str]:
            if not self.data:
                return None
            try:
                _, *percent_error = self.data.split(',')
            except ValueError:
                return None
            if len(percent_error) != 6:
                return None
            for i in range(len(percent_error)):
                try:
                    percent_error[i] = float(i)
                    if not isfinite(percent_error[i]):
                        raise ValueError
                except (ValueError, TypeError):
                    percent_error[i] = None

            def percent_errors(errs):
                return ", ".join([f"{e:4.1f}" if e is not None else "    " for e in errs])
            return (
                    "Total scattering errors: " + percent_errors(percent_error[0:3]) + "\n" +
                    "Back scattering errors:  " + percent_errors(percent_error[0:3])
            )

    class CommunicationsLost(BasicEntry.Event):
        @property
        def is_communications_lost(self) -> bool:
            return True

        @property
        def display(self) -> str:
            try:
                instrument, _ = self.code.split('-', 1)
            except ValueError:
                return super().display
            return instrument

    class MessageLog(BasicEntry.Event):
        @property
        def is_message_log(self) -> bool:
            return True

        @property
        def message_source(self) -> str:
            if not self.data:
                return ""
            try:
                source, message = self.data.split(',', 1)
            except (ValueError, TypeError):
                return ""
            return source

        @property
        def display(self) -> str:
            if not self.data:
                return ""
            try:
                source, message = self.data.split(',', 1)
            except (ValueError, TypeError):
                return self.data
            return message

    EVENT_CODES = {
        'file-processed': FileProcessed,
        'file-unauthorized': FileUnauthorized,
        'file-corrupted': FileCorrupted,
        'file-error': FileError,
        'message-log': MessageLog,
    }

    def notification_for_code(self, code: str) -> typing.Type["BasicEntry.Notification"]:
        if code.endswith('-clap-finalspot'):
            return self.CLAPFinalSpot
        return super().notification_for_code(code)

    def watchdog_for_code(self, code: str) -> typing.Type["BasicEntry.Watchdog"]:
        if code.endswith('-data'):
            return self.DataWatchdog
        return super().watchdog_for_code(code)

    def event_for_code(self, code: str) -> typing.Type["BasicEntry.Event"]:
        if code.endswith('-tsi3563nephelometer-spancheck'):
            return self.Spancheck
        elif code.endswith('-ecotechnephelometer-spancheck'):
            return self.Spancheck
        elif code.endswith("-communications-lost"):
            return self.CommunicationsLost
        return super().event_for_code(code)

    def condition_for_code(self, code: str) -> typing.Type["BasicEntry.Condition"]:
        def lookup_flag() -> typing.Optional[DashboardFlag]:
            try:
                instrument_id, instrument_type, flag = code.split('-', 2)
            except ValueError:
                return None
            return self._lookup_dashboard_flag(self.station, instrument_id, instrument_type, flag)

        dashboard_flag = lookup_flag()
        if dashboard_flag:
            return self.InstrumentCondition.flag_override(dashboard_flag.title, dashboard_flag.text)
        return super().condition_for_code(code)

    async def get_time_synchronization_error(self,
                                             telemetry: typing.Optional[TelemetryInterface],
                                             processing: typing.Optional[ProcessingInterface]) -> typing.Optional[int]:
        if self._time_synchronization_error_checked:
            return self._time_synchronization_error_cached
        self._time_synchronization_error_checked = True

        if not self.station:
            return None
        if not self.TIME_UNSYNC_THRESHOLD:
            return None
        time_offset = await get_station_time_offset(telemetry, processing, self.station)
        if not time_offset:
            return None
        if abs(time_offset) <= self.TIME_UNSYNC_THRESHOLD:
            return None
        self._time_synchronization_error_cached = time_offset
        return time_offset

    @classmethod
    async def get_status(cls, station: typing.Optional[str],
                         telemetry: typing.Optional[TelemetryInterface],
                         processing: typing.Optional[ProcessingInterface],
                         **kwargs) -> typing.Optional["Status"]:
        status = await super(FileIngestEntry, cls).get_status(
            station=station,
            telemetry=telemetry,
            processing=processing,
            **kwargs
        )
        if not status or status.information == Severity.ERROR:
            return status
        if not station:
            return status
        if not cls.TIME_UNSYNC_THRESHOLD:
            return status
        time_offset = await get_station_time_offset(telemetry, processing, station)
        if not time_offset:
            return status
        if abs(time_offset) <= cls.TIME_UNSYNC_THRESHOLD:
            return status
        status.information = Severity.ERROR
        return status

    async def base_email_severity(self,
                                  telemetry: typing.Optional[TelemetryInterface],
                                  processing: typing.Optional[ProcessingInterface],
                                  **kwargs) -> typing.Optional[Severity]:
        time_offset = await self.get_time_synchronization_error(telemetry, processing)
        if time_offset:
            return Severity.ERROR
        return None


class AcquisitionEmail(EmailContents):
    @property
    def reply_to(self) -> typing.Set[str]:
        emails = CONFIGURATION.get(
            'DASHBOARD.EMAIL.ACQUISITION.REPLY',
            CONFIGURATION.get('DASHBOARD.EMAIL.REPLY', [])
        )
        return set([r.lower() for r in emails])

    @property
    def expose_all_recipients(self) -> bool:
        return True


class AcquisitionIngestRecord(FileIngestRecord):
    DETAILS_TEMPLATE = 'acquisition.html'
    EMAIL_TEXT_TEMPLATE = 'acquisition.txt'
    EMAIL_HTML_TEMPLATE = 'acquisition.html'
    ENTRY = AcquisitionIngestEntry
    EMAIL_CONTENTS = AcquisitionEmail
    PLOT_MODE = 'aerosol-raw'
    ACQUISITION_MODE = 'acquisition'
    REALTIME_MODE = 'aerosol-realtime'

    @classmethod
    def simple_override(cls, *args,
                        acquisition_mode: typing.Optional[str] = None,
                        realtime_mode: typing.Optional[str] = None,
                        **kwargs) -> "AcquisitionIngestRecord":
        class Override(AcquisitionIngestRecord):
            ACQUISITION_MODE = acquisition_mode if acquisition_mode is not None else cls.ACQUISITION_MODE
            REALTIME_MODE = realtime_mode if realtime_mode is not None else cls.REALTIME_MODE
            ENTRY = cls.ENTRY.simple_override(*args, **kwargs)

        return Override()

    async def details(self, request: Request, station: typing.Optional[str], entry_code: str, **kwargs) -> Response:
        if station:
            link_to_acquisition = station_data(station, 'realtime', 'visible')(station, self.ACQUISITION_MODE)
            if link_to_acquisition and not mode_available(request, station, self.ACQUISITION_MODE):
                link_to_acquisition = False
            link_to_realtime = station_data(station, 'acquisition', 'visible')(station, self.REALTIME_MODE)
            if link_to_acquisition and not mode_available(request, station, self.REALTIME_MODE):
                link_to_realtime = False
        else:
            link_to_acquisition = False
            link_to_realtime = False

        return await super().details(
            request=request, station=station, entry_code=entry_code,
            link_to_acquisition=link_to_acquisition,
            link_to_realtime=link_to_realtime,
            **kwargs
        )

    async def email(self,
                    telemetry: typing.Optional[TelemetryInterface],
                    processing: typing.Optional[ProcessingInterface],
                    station: typing.Optional[str], entry_code: str, **kwargs) -> typing.Optional[EmailContents]:
        if station:
            link_to_acquisition = station_data(station, 'realtime', 'visible')(station, self.ACQUISITION_MODE)
            link_to_realtime = station_data(station, 'acquisition', 'visible')(station, self.REALTIME_MODE)
        else:
            link_to_acquisition = False
            link_to_realtime = False

        result = await super().email(
            telemetry=telemetry, processing=processing,
            station=station, entry_code=entry_code,
            link_to_acquisition=link_to_acquisition,
            link_to_realtime=link_to_realtime,
            **kwargs
        )

        time_error = await result.entry.get_time_synchronization_error(telemetry, processing)
        if time_error:
            result.severity = Severity.ERROR
        return result
