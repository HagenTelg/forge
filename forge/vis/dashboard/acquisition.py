import typing
from starlette.requests import Request
from starlette.responses import Response
from forge.vis.station.lookup import station_data
from forge.vis.mode.permissions import is_available as mode_available
from .basic import BasicEntry, DatabaseCondition, EmailContents
from .fileingest import FileIngestEntry, FileIngestRecord


class AcquisitionIngestEntry(FileIngestEntry):
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

    INSTRUMENT_CONDITIONS = {
        '-tsi3563nephelometer-lamp_power_error': InstrumentCondition.flag_override("Lamp power not within 10 percent of the setpoint"),
        '-tsi3563nephelometer-heater_unstable': InstrumentCondition.flag_override("Heater instability"),
        '-tsi3563nephelometer-valve_fault': InstrumentCondition.flag_override("Valve fault"),
        '-tsi3563nephelometer-pressure_out_of_range': InstrumentCondition.flag_override("Pressure out of range", "Check the sensor and connections"),
        '-tsi3563nephelometer-sample_temperature_out_of_range': InstrumentCondition.flag_override("Internal temperature out of range", "Check the sensor and connections"),
        '-tsi3563nephelometer-inlet_temperature_out_of_range': InstrumentCondition.flag_override("Inlet temperature out of range", "Check the sensor and connections"),
        '-tsi3563nephelometer-rh_suspect': InstrumentCondition.flag_override("Internal relative humidity suspect", "Check for condensation and sensor integrity"),
        '-tsi3563nephelometer-lamp_voltage_high': InstrumentCondition.flag_override("Lamp voltage too high", "Lamp is likely burned out"),
        '-tsi3563nephelometer-shutter_fault': InstrumentCondition.flag_override("Shutter fault", "Scatterings suspect, motor service may be required"),
        '-tsi3563nephelometer-chopper_fault': InstrumentCondition.flag_override("Chopper fault", "Scatterings suspect, motor service may be required"),

        '-ecotechnephelometer-backscatter_fault': InstrumentCondition.flag_override("Backscatter fault", "Scatterings suspect"),
        '-ecotechnephelometer-backscatter_digital_fault': InstrumentCondition.flag_override("Backscatter digital IO fault", "Scatterings suspect"),
        '-ecotechnephelometer-shutter_fault': InstrumentCondition.flag_override("Shutter fault", "Scatterings suspect"),
        '-ecotechnephelometer-light_source_fault': InstrumentCondition.flag_override("Light source fault", "Scatterings suspect"),
        '-ecotechnephelometer-pmt_fault': InstrumentCondition.flag_override("PMT fault", "Scatterings suspect"),
        '-ecotechnephelometer-system_fault': InstrumentCondition.flag_override("Unclassified system fault"),
        '-ecotechnephelometer-pressure_sensor_fault': InstrumentCondition.flag_override("Pressure sensor fault"),
        '-ecotechnephelometer-enclosure_temperature_fault': InstrumentCondition.flag_override("Enclosure temperature sensor fault"),
        '-ecotechnephelometer-sample_temperature_fault': InstrumentCondition.flag_override("Sample temperature sensor fault"),
        '-ecotechnephelometer-rh_fault': InstrumentCondition.flag_override("RH sensor fault"),
        '-ecotechnephelometer-warmup_fault': InstrumentCondition.flag_override("Warm up fault"),
        '-ecotechnephelometer-backscatter_high_warning': InstrumentCondition.flag_override("Backscatter high warning"),
        '-ecotechnephelometer-inconsistent_zero': InstrumentCondition.flag_override("Inconsistent zero and data filter settings selected"),
        '-ecotechnephelometer-rh_suspect': InstrumentCondition.flag_override("Internal relative humidity suspect", "Check for condensation and sensor integrity"),

        '-clap-flow_error': InstrumentCondition.flag_override("Flow error"),
        '-clap-led_error': InstrumentCondition.flag_override("LED error"),
        '-clap-filter_was_not_white': InstrumentCondition.flag_override("Initial filter parameters where not close to white", "Change the filter and verify normal sampling if ongoing"),
        '-clap-temperature_out_of_range': InstrumentCondition.flag_override("Temperature out of range"),

        '-thermo49-alarm_sample_temperature_low': InstrumentCondition.flag_override("Sample temperature too low"),
        '-thermo49-alarm_sample_temperature_high': InstrumentCondition.flag_override("Sample temperature too high"),
        '-thermo49-alarm_lamp_temperature_low': InstrumentCondition.flag_override("Lamp temperature too low"),
        '-thermo49-alarm_lamp_temperature_high': InstrumentCondition.flag_override("Lamp temperature too high"),
        '-thermo49-alarm_ozonator_temperature_low': InstrumentCondition.flag_override("Ozonator temperature too low"),
        '-thermo49-alarm_ozonator_temperature_high': InstrumentCondition.flag_override("Ozonator temperature too high"),
        '-thermo49-alarm_pressure_low': InstrumentCondition.flag_override("Pressure too low"),
        '-thermo49-alarm_pressure_high': InstrumentCondition.flag_override("Pressure too high"),
        '-thermo49-alarm_flow_a_low': InstrumentCondition.flag_override("Cell A flow too low"),
        '-thermo49-alarm_flow_a_high': InstrumentCondition.flag_override("Cell A flow too high"),
        '-thermo49-alarm_flow_b_low': InstrumentCondition.flag_override("Cell B flow too low"),
        '-thermo49-alarm_flow_b_high': InstrumentCondition.flag_override("Cell B flow too high"),
        '-thermo49-alarm_intensity_a_low': InstrumentCondition.flag_override("Cell A count rate too low"),
        '-thermo49-alarm_intensity_a_high': InstrumentCondition.flag_override("Cell A count rate too high"),
        '-thermo49-alarm_intensity_b_low': InstrumentCondition.flag_override("Cell B count rate too low"),
        '-thermo49-alarm_intensity_b_high': InstrumentCondition.flag_override("Cell B count rate too high"),
        '-thermo49-alarm_ozone_low': InstrumentCondition.flag_override("Ozone concentration too low"),
        '-thermo49-alarm_ozone_high': InstrumentCondition.flag_override("Ozone concentration too high"),

        '-thermo49iq-alarm_intensity_a_high': InstrumentCondition.flag_override("Cell A count rate too high"),
        '-thermo49iq-alarm_intensity_b_high': InstrumentCondition.flag_override("Cell B count rate too high"),
        '-thermo49iq-lamp_temperature_short': InstrumentCondition.flag_override("Lamp temperature sensor short circuit"),
        '-thermo49iq-lamp_temperature_open': InstrumentCondition.flag_override("Lamp temperature sensor open circuit"),
        '-thermo49iq-sample_temperature_short': InstrumentCondition.flag_override("Sample temperature sensor short circuit"),
        '-thermo49iq-sample_temperature_open': InstrumentCondition.flag_override("Sample temperature sensor open circuit"),
        '-thermo49iq-lamp_connection_alarm': InstrumentCondition.flag_override("Lamp connection alarm"),
        '-thermo49iq-lamp_short': InstrumentCondition.flag_override("Lamp short circuit"),
        '-thermo49iq-communications_alarm': InstrumentCondition.flag_override("Communications alarm"),
        '-thermo49iq-power_supply_alarm': InstrumentCondition.flag_override("Power supply alarm"),
        '-thermo49iq-lamp_current_alarm': InstrumentCondition.flag_override("Lamp current out of range"),
        '-thermo49iq-lamp_temperature_alarm': InstrumentCondition.flag_override("Lamp temperature out of range"),
        '-thermo49iq-sample_temperature_alarm': InstrumentCondition.flag_override("Sample temperature out of range"),
    }

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
        for check, cond in self.INSTRUMENT_CONDITIONS.items():
            if code.endswith(check):
                return cond
        return super().condition_for_code(code)


class AcquisitionIngestRecord(FileIngestRecord):
    DETAILS_TEMPLATE = 'acquisition.html'
    EMAIL_TEXT_TEMPLATE = 'acquisition.txt'
    EMAIL_HTML_TEMPLATE = 'acquisition.html'
    ENTRY: typing.Type[FileIngestEntry] = AcquisitionIngestEntry
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

    async def email(self, station: typing.Optional[str], entry_code: str, **kwargs) -> typing.Optional[EmailContents]:
        if station:
            link_to_acquisition = station_data(station, 'realtime', 'visible')(station, self.ACQUISITION_MODE)
            link_to_realtime = station_data(station, 'acquisition', 'visible')(station, self.REALTIME_MODE)
        else:
            link_to_acquisition = False
            link_to_realtime = False

        return await super().email(
            station=station, entry_code=entry_code,
            link_to_acquisition=link_to_acquisition,
            link_to_realtime=link_to_realtime,
            **kwargs
        )
