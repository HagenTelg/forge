import typing
import time
import datetime
import starlette.status
from starlette.requests import Request
from starlette.responses import Response, HTMLResponse, JSONResponse
from starlette.exceptions import HTTPException
from forge.formattime import format_iso8601_time, format_time_of_day, format_date, format_simple_duration
from forge.vis import CONFIGURATION
from forge.vis.util import package_template
from forge.telemetry.display import DisplayInterface as TelemetryInterface
from forge.processing.control.display import DisplayInterface as ProcessingInterface
from forge.dashboard.database import Severity
from . import Record as BaseRecord
from .status import Status
from .entry import Entry as BaseEntry
from .email import EmailContents
from .badge import assemble_badge_json, assemble_badge_svg


class TelemetryEntry(BaseEntry):
    OFFLINE_THRESHOLD = 26 * 60 * 60
    UNSYNC_THRESHOLD = 2 * 60

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.time_offset: typing.Optional[int] = None
        self.have_authorization: bool = False

    @property
    def time_offset_ms(self) -> typing.Optional[int]:
        if self.time_offset is None:
            return None
        return int(round(self.time_offset * 1000.0))

    @property
    def display(self) -> typing.Optional[str]:
        return "Acquisition computer telemetry"

    @classmethod
    def _convert_db(cls, station: str, code: str, accepted_keys: typing.Optional[typing.Set],
                    last_seen: typing.Optional[datetime.datetime]):
        status = cls.Status.OK
        if last_seen:
            last_seen = last_seen.replace(tzinfo=datetime.timezone.utc)
            if accepted_keys and cls.OFFLINE_THRESHOLD:
                age = (datetime.datetime.now(tz=datetime.timezone.utc) - last_seen).total_seconds()
                if age > cls.OFFLINE_THRESHOLD:
                    status = cls.Status.OFFLINE
            return cls(station, code, status, last_seen.timestamp())

        if accepted_keys:
            return cls(station, code, cls.Status.FAILED, time.time())
        else:
            # Do not
            return cls(station, code, cls.Status.OK, time.time())

    @classmethod
    async def from_db(cls, station: str, code: str,
                      telemetry: TelemetryInterface,
                      processing: typing.Optional[ProcessingInterface]) -> typing.Optional["TelemetryEntry"]:
        accepted_keys = None
        if processing:
            accepted_keys = await processing.get_data_processing_keys(station)
        last_seen = await telemetry.get_last_seen(station, accepted_keys)
        return cls._convert_db(station, code, accepted_keys, last_seen)

    @classmethod
    async def get_status(cls, station: str, email: Status.Email,
                         telemetry: TelemetryInterface,
                         processing: typing.Optional[ProcessingInterface]) -> typing.Optional["Status"]:
        accepted_keys = None
        if processing:
            accepted_keys = await processing.get_data_processing_keys(station)
        if not accepted_keys:
            return Status(None, email)
        last_seen, time_offset = await telemetry.get_status(station, accepted_keys)

        if last_seen is None:
            return Status(None, email)
        if time_offset and cls.UNSYNC_THRESHOLD:
            if abs(time_offset) >= cls.UNSYNC_THRESHOLD:
                return Status(Severity.ERROR, email)
        return Status(None, email)

    @classmethod
    async def detailed(cls, station: str, code: str,
                       telemetry: TelemetryInterface,
                       processing: typing.Optional[ProcessingInterface]) -> typing.Tuple[typing.Optional["TelemetryEntry"], typing.Optional[Severity]]:
        accepted_keys = None
        if processing:
            accepted_keys = await processing.get_data_processing_keys(station)
        last_seen, time_offset = await telemetry.get_status(station, accepted_keys)

        entry = cls._convert_db(station, code, accepted_keys, last_seen)
        entry.have_authorization = bool(accepted_keys)

        severity = None
        if time_offset and cls.UNSYNC_THRESHOLD:
            if abs(time_offset) >= cls.UNSYNC_THRESHOLD:
                severity = Severity.ERROR
                entry.time_offset = time_offset

        return entry, severity


class TelemetryRecord(BaseRecord):
    DATABASE_LINKED = False
    CODE = "acquisition-telemetry"

    DETAILS_TEMPLATE = 'telemetry.html'
    EMAIL_TEXT_TEMPLATE = 'telemetry.txt'
    EMAIL_HTML_TEMPLATE = 'telemetry.html'
    BADGE_TEMPLATE = 'basic.svg'
    ENTRY: typing.Type[TelemetryEntry] = TelemetryEntry
    EMAIL_CONTENTS: typing.Type[EmailContents] = EmailContents

    async def entry(self, station: typing.Optional[str], code: str,
                    telemetry: typing.Optional[TelemetryInterface],
                    processing: typing.Optional[ProcessingInterface],
                    **kwargs) -> typing.Optional["TelemetryEntry"]:
        if not station:
            return None
        if not telemetry:
            return None
        return await self.ENTRY.from_db(station, code, telemetry, processing)

    async def status(self, station: typing.Optional[str], email: Status.Email,
                     telemetry: typing.Optional[TelemetryInterface],
                     processing: typing.Optional[ProcessingInterface],
                     **kwargs) -> typing.Optional["Status"]:
        if not station:
            return None
        if not telemetry:
            return None
        return await self.ENTRY.get_status(station, email, telemetry, processing)

    async def details(self, request: Request,
                      station: typing.Optional[str], entry_code: str,
                      telemetry: typing.Optional[TelemetryInterface],
                      processing: typing.Optional[ProcessingInterface], **kwargs) -> Response:
        entry, _ = await self.ENTRY.detailed(station, entry_code, telemetry, processing)
        if not entry:
            raise HTTPException(starlette.status.HTTP_404_NOT_FOUND, detail="Entry not found")

        return HTMLResponse(await package_template('dashboard', 'details', self.DETAILS_TEMPLATE).render_async(
            request=request,
            telemetry=telemetry,
            processing=processing,
            record=self,
            station=station,
            code=entry_code,
            entry=entry,
            format_interval=format_simple_duration,
            format_datetime=format_iso8601_time,
            format_date=format_date,
            format_time=format_time_of_day,
            **kwargs
        ))

    async def email(self, station: typing.Optional[str], entry_code: str,
                    telemetry: typing.Optional[TelemetryInterface],
                    processing: typing.Optional[ProcessingInterface], **kwargs) -> typing.Optional["EmailContents"]:
        entry, contents_severity = await self.ENTRY.detailed(station, entry_code, telemetry, processing)
        if not entry:
            return None

        async def template_file(template) -> str:
            return await template.render_async(
                telemetry=telemetry,
                processing=processing,
                record=self,
                station=station,
                code=entry_code,
                entry=entry,
                Severity=Severity,
                URL=CONFIGURATION.get("DASHBOARD.EMAIL.URL"),
                format_interval=format_simple_duration,
                format_datetime=format_iso8601_time,
                format_date=format_date,
                format_time=format_time_of_day,
                **kwargs
            )

        text = await template_file(package_template('dashboard', 'email', self.EMAIL_TEXT_TEMPLATE))
        html = await template_file(package_template('dashboard', 'email', self.EMAIL_HTML_TEMPLATE))
        return self.EMAIL_CONTENTS(entry, contents_severity, text, html)

    async def badge_data(self, telemetry: typing.Optional[TelemetryInterface],
                         processing: typing.Optional[ProcessingInterface],
                         station: typing.Optional[str], code: str) -> typing.Optional["TelemetryEntry"]:
        return await self.entry(station, code, telemetry, processing)

    async def badge_json(self, request: Request,
                         telemetry: typing.Optional[TelemetryInterface],
                         processing: typing.Optional[ProcessingInterface],
                         station: typing.Optional[str], entry_code: str, **kwargs) -> Response:
        entry = await self.badge_data(telemetry, processing, station, entry_code)
        if not entry:
            entry = TelemetryEntry(station, entry_code, TelemetryEntry.Status.OFFLINE, time.time())
        return assemble_badge_json(entry)

    async def badge_svg(self, request: Request,
                        telemetry: typing.Optional[TelemetryInterface],
                        processing: typing.Optional[ProcessingInterface],
                        station: typing.Optional[str], entry_code: str, **kwargs) -> Response:
        entry = await self.badge_data(telemetry, processing, station, entry_code)
        if not entry:
            entry = TelemetryEntry(station, entry_code, TelemetryEntry.Status.OFFLINE, time.time())
        return await assemble_badge_svg(
            request, entry, self.BADGE_TEMPLATE,
            telemetry=telemetry,
            processing=processing,
            record=self,
            station=station,
            code=entry_code,
            **kwargs,
        )


async def get_station_time_offset(telemetry: typing.Optional[TelemetryInterface],
                                  processing: typing.Optional[ProcessingInterface],
                                  station: str) -> typing.Optional[int]:
    if not telemetry:
        return None
    accepted_keys = None
    if processing:
        accepted_keys = await processing.get_data_processing_keys(station)
    return await telemetry.get_time_offset(station, accepted_keys)
