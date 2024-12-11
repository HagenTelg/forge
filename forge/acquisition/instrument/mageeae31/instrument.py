import typing
import asyncio
import datetime
import logging
import time
import enum
import re
from math import isfinite, nan, exp, log
from forge.tasks import wait_cancelable
from forge.units import mass_ng_to_ug, flow_lpm_to_m3s, ONE_ATM_IN_HPA, ZERO_C_IN_K
from ..streaming import StreamingInstrument, StreamingContext, CommunicationsError
from ..parse import parse_number
from ..variable import Input

_LOGGER = logging.getLogger(__name__)
_INSTRUMENT_TYPE = __name__.split('.')[-2]
_FIELD_SPLIT = re.compile(rb"(?:\s*,\s*)|\s+")


def _unquote(raw: bytes) -> bytes:
    raw = raw.strip()
    if len(raw) < 2:
        return raw
    if raw[:1] == b'"' and raw[-1:] == b'"':
        return raw[1:-1].strip()
    elif raw[:1] == b"'" and raw[-1:] == b"'":
        return raw[1:-1].strip()
    return raw


def _parse_time(raw: bytes) -> datetime.time:
    raw = _unquote(raw)
    try:
        fields = raw.split(b':')
        if len(fields) != 2:
            raise CommunicationsError("invalid number of time fields")
        hour = int(fields[0].strip())
        minute = int(fields[1].strip())
        return datetime.time(hour, minute, 0, tzinfo=datetime.timezone.utc)
    except ValueError as e:
        raise CommunicationsError from e


_MONTH_NAMES = {
    b"jan": 1,
    b"feb": 2,
    b"mar": 3,
    b"apr": 4,
    b"may": 5,
    b"jun": 6,
    b"jul": 7,
    b"aug": 8,
    b"sep": 9,
    b"oct": 10,
    b"nov": 11,
    b"dec": 12,
}


def _parse_date(raw: bytes) -> datetime.date:
    raw = _unquote(raw)
    try:
        fields = raw.split(b'-')
        if len(fields) != 3:
            fields = raw.split(b'/')
        if len(fields) != 3:
            raise CommunicationsError("invalid number of date fields")

        day = int(fields[0].strip())

        raw_month = fields[1].strip()
        month = _MONTH_NAMES.get(raw_month.lower())
        if not month:
            month = int(raw_month)

        year = int(fields[2].strip())
        td = time.gmtime()
        current_century = td.tm_year - (td.tm_year % 100)
        year += current_century
        if year > td.tm_year + 50:
            year -= 100
        if year < 1900 or year > 2999:
            raise CommunicationsError(f"invalid year {year}")

        return datetime.date(year, month, day)
    except ValueError as e:
        raise CommunicationsError from e


def _parse_datetime(date_field: bytes, time_field: bytes) -> datetime.datetime:
    try:
        d = _parse_date(date_field)
        t = _parse_time(time_field)
        return datetime.datetime(d.year, d.month, d.day, t.hour, t.minute, t.second, tzinfo=t.tzinfo)
    except ValueError as e:
        raise CommunicationsError from e


def _parse_number_mvc(value: bytes) -> float:
    value = _unquote(value)
    if len(value) == 0:
        return nan
    try:
        v = float(value.strip())
    except (ValueError, OverflowError):
        raise CommunicationsError(f"invalid number {value}")
    if not isfinite(v):
        raise CommunicationsError("converted number is not finite")
    return v


def _decompress_field(data: bytes) -> float:
    if len(data) == 0:
        raise CommunicationsError(f"empty compressed value")
    result = 0.0
    for d in data:
        result *= 50.0
        if ord(b'a') <= d <= ord(b'z'):
            result += d - ord(b'a')
        elif ord(b'B') <= d <= ord(b'Y'):
            result += (d - ord(b'B')) + 26.0
        else:
            raise CommunicationsError(f"invalid compressed value {data}")
    return result


def _atn_to_transmittance(value: float) -> float:
    if not isfinite(value) or value <= 0.0:
        return nan
    return exp(value / -100.0)


class Instrument(StreamingInstrument):
    INSTRUMENT_TYPE = _INSTRUMENT_TYPE
    MANUFACTURER = "Magee"
    MODEL = "AE31"
    DISPLAY_LETTER = "E"
    TAGS = frozenset({"aerosol", "aethalometer", "absorption", _INSTRUMENT_TYPE})
    SERIAL_PORT = {'baudrate': 9600}

    WAVELENGTHS = (
        (370.0, "1"),
        (470.0, "2"),
        (520.0, "3"),
        (590.0, "4"),
        (660.0, "5"),
        (880.0, "6"),
        (950.0, "7"),
    )

    DEFAULT_SPOT_SIZE = 50.0

    class _EBCUnits(enum.Enum):
        Unknown = 0
        ug = 1
        ng = 2

        @staticmethod
        def parse(s) -> "Instrument._EBCUnits":
            if isinstance(s, str):
                s = s.lower()
                if s == "u" or s == "ug" or s == "ug/m3" or s == "ugm3":
                    return Instrument._EBCUnits.ug
                elif s == "n" or s == "ng" or s == "ng/m3" or s == "ngm3":
                    return Instrument._EBCUnits.ng
            return Instrument._EBCUnits.Unknown

    def __init__(self, context: StreamingContext):
        super().__init__(context)

        self._report_interval: float = float(context.config.get('REPORT_INTERVAL', default=300.0))
        self._spot_advance_detect_transmittance: float = float(context.config.get('CHANGE.DETECT_TRANSMITTANCE', default=0.1))

        ebc_efficiency: typing.Union[typing.List[float], float] = context.config.get('EBC_EFFICIENCY')
        self._ebc_efficiency: typing.List[float] = list()
        for i in range(len(self.WAVELENGTHS)):
            wl = self.WAVELENGTHS[i][0]
            if isinstance(ebc_efficiency, list) and i < len(ebc_efficiency):
                e = float(ebc_efficiency[i])
            elif ebc_efficiency is not None and ebc_efficiency > 0.0:
                e = ebc_efficiency / wl
            else:
                e = 14625.0 / wl
            self._ebc_efficiency.append(e)

        config_spot_size = context.config.get('SPOT')
        if config_spot_size is not None:
            self.spot_size: float = float(config_spot_size)
        else:
            self.spot_size: float = self.DEFAULT_SPOT_SIZE
        self.sample_temperature: float = float(context.config.get('SAMPLE_TEMPERATURE', default=20.0))
        self.sample_pressure: float = float(context.config.get('SAMPLE_PRESSURE', default=1013.0))
        self.mean_ratio: float = float(context.config.get('MEAN_RATIO', default=1.0))

        self._ebc_units: "Instrument._EBCUnits" = self._EBCUnits.Unknown
        units = context.config.get('EBC_UNITS')
        if units:
            self._ebc_units = self._EBCUnits.parse(units)
        self._identified_ebc_units: "Instrument._EBCUnits" = self._ebc_units
        self._identified_unit_uncertain: bool = False

        self.data_Q = self.input("Q")
        self.data_PCTbypass = self.input("PCTbypass")
        self.data_Ld = self.input("Ld", send_to_bus=False)

        self.data_X_wavelength: typing.List[Input] = list()
        self.data_Ba_wavelength: typing.List[Input] = list()
        self.data_Ir_wavelength: typing.List[Input] = list()
        self.data_If_wavelength: typing.List[Input] = list()
        self.data_Ifz_wavelength: typing.List[Input] = list()
        self.data_Ip_wavelength: typing.List[Input] = list()
        self.data_Ipz_wavelength: typing.List[Input] = list()
        for _, code in self.WAVELENGTHS:
            self.data_X_wavelength.append(self.input("X" + code))
            self.data_Ba_wavelength.append(self.input("Ba" + code))
            self.data_Ir_wavelength.append(self.input("Ir" + code))
            self.data_If_wavelength.append(self.input("If" + code))
            self.data_Ifz_wavelength.append(self.input("Ifz" + code))
            self.data_Ip_wavelength.append(self.input("Ip" + code))
            self.data_Ipz_wavelength.append(self.input("Ipz" + code))

        self.data_wavelength = self.persistent("wavelength", save_value=False, send_to_bus=False)
        self.data_wavelength([wl for wl, _ in self.WAVELENGTHS])
        self.data_X = self.input_array("X", send_to_bus=False)
        self.data_Ba = self.input_array("Ba", send_to_bus=False)
        self.data_Ir = self.input_array("Ir", send_to_bus=False)
        self.data_If = self.input_array("If", send_to_bus=False)
        self.data_Ip = self.input_array("Ip", send_to_bus=False)

        self._prior_report_time: typing.Optional[float] = None
        self._Ir0: typing.Optional[typing.List[float]] = None

        self.notify_spot_advancing = self.notification('spot_advancing')

        dimension_wavelength = self.dimension_wavelength(self.data_wavelength)
        self.instrument_report = self.report(
            self.variable_ebc(self.data_X, dimension_wavelength, code="X").st_stp(),
            self.variable_absorption(self.data_Ba, dimension_wavelength, code="Ba").st_stp(),

            self.variable_transmittance(self.data_Ir, dimension_wavelength, code="Ir"),
            self.variable_array(self.data_If, dimension_wavelength, "reference_intensity", code="If", attributes={
                'long_name': "sensing beam signal",
                'C_format': "%7.4f",
            }),
            self.variable_array(self.data_Ip, dimension_wavelength, "sample_intensity", code="Ip",
                                attributes={
                'long_name': "reference beam signal",
                'C_format': "%7.4f",
            }),

            self.variable_sample_flow(self.data_Q, code="Q", attributes={'C_format': "%6.3f"}).at_stp(),

            flags=[
                self.flag(self.notify_spot_advancing),
            ],

            auxiliary_variables=(
                [self.variable(w) for w in self.data_Ba_wavelength] +
                [self.variable(w) for w in self.data_X_wavelength] +
                [self.variable_last_valid(w) for w in self.data_Ir_wavelength]
            ),
        )
        self.instrument_report.record.data_record.report_interval = self._report_interval

        self.parameters_record = self.context.data.constant_record("parameters")
        self.parameters_record.array_float_attr("mass_absorption_efficiency", self, '_ebc_efficiency', attributes={
            'long_name': "the efficiency factor used to convert absorption coefficients into an equivalent black carbon",
            'units': "m2 g",
        })
        self.parameters_record.float_attr("spot_area", self, 'spot_size', attributes={
            'long_name': "sampling spot area",
            'C_format': "%5.2f",
            'units': "mm2",
        })
        self.parameters_record.float_attr("instrument_standard_temperature", self, 'sample_temperature', attributes={
            'long_name': "standard temperature of instrument data",
            'C_format': "%5.2f",
            'units': "degC",
        })
        self.parameters_record.float_attr("instrument_standard_pressure", self, 'sample_pressure', attributes={
            'long_name': "standard pressure of instrument data",
            'C_format': "%7.2f",
            'units': "hPa",
        })
        self.parameters_record.float_attr("mean_ratio", self, 'mean_ratio', attributes={
            'long_name': "instrument mean ratio correction factor",
            'C_format': "%5.3f",
        })

    async def start_communications(self) -> None:
        # This is less reliable than a normal record flush, but the slow reporting makes that
        # undesirable
        try:
            await wait_cancelable(self.read_line(), 5.0)
        except asyncio.TimeoutError:
            pass
        # Flush the first record
        await self.drain_reader(0.5)

        self._prior_report_time = None
        self._Ir0 = None
        self.instrument_report.record.data_record.report_interval = self._report_interval
        self._identified_ebc_units = self._ebc_units
        self._identified_unit_uncertain = False

        # Process a valid record
        await self.communicate()

        self._prior_report_time = None
        self._Ir0 = None
        self.instrument_report.record.data_record.report_interval = self._report_interval

    def _calculate_path_length(self, elapsed_seconds: float = 1.0) -> None:
        Q = float(self.data_Q)
        fraction = float(self.data_PCTbypass)
        if isfinite(fraction) and fraction > 0.0:
            fraction /= 100.0
        else:
            fraction = 1.0
        if isfinite(self.spot_size) and isfinite(Q) and self.spot_size > 0.0:
            dQt = flow_lpm_to_m3s(Q) * elapsed_seconds * fraction
            self.data_Ld(dQt / (self.spot_size * 1E-6))
        else:
            self.data_Ld(nan)

    def _calculate_absorption(self) -> typing.List[float]:
        Ld = float(self.data_Ld)
        result: typing.List[float] = list()
        for widx in range(len(self.WAVELENGTHS)):
            Ir = float(self.data_Ir_wavelength[widx])
            if self._Ir0 and widx < len(self._Ir0):
                Ir0 = self._Ir0[widx]
            else:
                Ir0 = nan

            if isfinite(Ld) and isfinite(Ir) and isfinite(Ir0) and Ld > 0.0 and Ir > 0.0 and Ir0 > 0.0:
                result.append((log(Ir0 / Ir) / Ld) * 1E6)
            else:
                result.append(nan)
        return result

    def _identify_ebc_units(self, ebc_raw: typing.List[bytes],
                            ebc_converted: typing.List[float]) -> "Instrument._EBCUnits":
        units = self._ebc_units
        ng_probable = False
        ug_probable = False
        units_uncertain = False
        if units == self._EBCUnits.Unknown:
            calculated_Ba = self._calculate_absorption()
            for widx in range(len(self.WAVELENGTHS)):
                a = float(calculated_Ba[widx])
                if not isfinite(a):
                    continue
                bc = ebc_converted[widx]
                bc_a = abs(bc)

                calc_ebc_ug = a / self._ebc_efficiency[widx]
                calc_ebc_ng = calc_ebc_ug * 1E3
                calc_ebc_ug_a = abs(calc_ebc_ug)
                calc_ebc_ng_a = abs(calc_ebc_ng)

                # When they're too small to work out, try comparing ratios as a fallback
                if calc_ebc_ug_a < 10.0 or calc_ebc_ng_a < 10.0:
                    if calc_ebc_ug_a < 1.0:
                        continue
                    ug_ratio = (calc_ebc_ug_a / bc_a) - 1.0
                    ng_ratio = (calc_ebc_ng_a / bc_a) - 1.0

                    ng_check = abs(ng_ratio) < 0.3
                    ug_check = abs(ug_ratio) < 0.3
                    if ng_check and not ug_check:
                        ng_probable = True
                    elif ug_check and not ng_check:
                        ug_probable = True
                    continue

                if abs(bc - calc_ebc_ug) < abs(bc - calc_ebc_ng):
                    units = self._EBCUnits.ug
                else:
                    units = self._EBCUnits.ng
                break
        if units == self._EBCUnits.Unknown:
            units = self._identified_ebc_units
            units_uncertain = self._identified_unit_uncertain
        if units == self._EBCUnits.Unknown or units_uncertain:
            if ng_probable:
                units = self._EBCUnits.ng
                units_uncertain = True
            elif ug_probable:
                units = self._EBCUnits.ug
                units_uncertain = True
        if units == self._EBCUnits.Unknown:
            have_any_valid = False
            decimal_present = False
            decimal_absent = False
            for f in ebc_raw:
                if len(f) == 0:
                    continue
                have_any_valid = True
                if b'.' in f:
                    decimal_present = True
                else:
                    decimal_absent = True
            if have_any_valid:
                if decimal_present and not decimal_absent:
                    units = self._EBCUnits.ug
                    units_uncertain = True
                elif decimal_absent and not decimal_present:
                    units = self._EBCUnits.ng
                    units_uncertain = True
        self._identified_ebc_units = units
        self._identified_unit_uncertain = units_uncertain
        return units

    def _calculate_ebc(self, ebc_raw: typing.List[bytes], ebc_factor: float) -> None:
        ebc_converted: typing.List[float] = list()
        for v in ebc_raw:
            if len(v) == 0:
                ebc_converted.append(nan)
                continue
            v = parse_number(v)
            ebc_converted.append(v * ebc_factor)

        units = self._identify_ebc_units(ebc_raw, ebc_converted)
        if units == self._EBCUnits.Unknown:
            for widx in range(len(self.data_X_wavelength)):
                self.data_X_wavelength[widx](nan)
        elif units == self._EBCUnits.ng:
            for widx in range(len(self.data_X_wavelength)):
                self.data_X_wavelength[widx](mass_ng_to_ug(ebc_converted[widx]))
        else:
            for widx in range(len(self.data_X_wavelength)):
                self.data_X_wavelength[widx](ebc_converted[widx])

        for widx in range(len(self.data_Ba_wavelength)):
            self.data_Ba_wavelength[widx](self.data_X_wavelength[widx].value * self._ebc_efficiency[widx])

    async def communicate(self) -> None:
        line: bytes = await wait_cancelable(self.read_line(), self._report_interval * 2.0 + 1.0)
        if len(line) < 10:
            raise CommunicationsError
        now = time.monotonic()
        if self._prior_report_time is not None:
            elapsed_seconds = now - self._prior_report_time
            report_minutes = round(elapsed_seconds / 60.0)
            if 2 <= report_minutes <= 60:
                self.instrument_report.record.data_record.report_interval = report_minutes * 60.0
        else:
            elapsed_seconds = None
        self._prior_report_time = now

        fields = _FIELD_SPLIT.split(line.strip())
        if len(fields) < 3:
            raise CommunicationsError(f"invalid number of fields in {line}")
        sn = _unquote(fields[0])
        try:
            sn = int(sn)
            self.set_serial_number(sn)
            fields = fields[1:]
        except (ValueError, OverflowError):
            pass

        try:
            (raw_date, raw_time, *fields) = fields
        except ValueError:
            raise CommunicationsError(f"invalid number of fields in {line}")
        _parse_datetime(raw_date, raw_time)

        if len(fields) == len(self.WAVELENGTHS)*2:  # == 14
            ebc_raw = fields[:len(self.WAVELENGTHS)]
            fields = fields[len(self.WAVELENGTHS):]

            first_compressed = _unquote(fields[0])
            if len(first_compressed) != 16:
                raise CommunicationsError(f"invalid first compressed field in {line}")
            Qinstrument = _decompress_field(first_compressed[14:16]) / 10.0
            self.data_PCTbypass(((_decompress_field(first_compressed[13:14]) + 1.0) / 50.0) * 100.0)

            for i in range(len(self.WAVELENGTHS)):
                compressed = _unquote(fields[i])
                if len(compressed) != 16:
                    raise CommunicationsError(f"invalid compressed field {i} in {line}")
                self.data_Ir_wavelength[i](_atn_to_transmittance(_decompress_field(compressed[0:3]) / 1000.0 - 10.0))
                self.data_Ipz_wavelength[i](_decompress_field(compressed[3:5]) / 10000.0)
                self.data_Ifz_wavelength[i](_decompress_field(compressed[5:7]) / 10000.0)
                self.data_Ip_wavelength[i](_decompress_field(compressed[7:10]) / 10000.0 - 1.0)
                self.data_If_wavelength[i](_decompress_field(compressed[10:13]) / 10000.0 - 1.0)
        elif len(fields) == len(self.WAVELENGTHS) + 1 + len(self.WAVELENGTHS) * 6:  # == 50
            ebc_raw = fields[:len(self.WAVELENGTHS)]
            fields = fields[len(self.WAVELENGTHS):]

            Qinstrument = parse_number(fields[0])
            fields = fields[1:]

            self.data_PCTbypass(parse_number(fields[4]) * 100.0)

            for i in range(len(self.WAVELENGTHS)):
                base = i * 6
                self.data_Ipz_wavelength[i](parse_number(fields[base]))
                self.data_Ip_wavelength[i](parse_number(fields[base+1]))
                self.data_Ifz_wavelength[i](parse_number(fields[base+2]))
                self.data_If_wavelength[i](parse_number(fields[base+3]))
                # Bypass fraction handled above
                self.data_Ir_wavelength[i](_atn_to_transmittance(parse_number(fields[base+5])))
        else:
            raise CommunicationsError(f"invalid number of fields in {line}")

        stp_factor = (self.sample_pressure / ONE_ATM_IN_HPA) * (ZERO_C_IN_K / (self.sample_temperature + ZERO_C_IN_K))
        Q = self.data_Q(Qinstrument * stp_factor)
        ebc_factor = 1.0 / self.mean_ratio
        if isfinite(Q) and isfinite(Qinstrument) and Q != 0.0:
            ebc_factor *= Qinstrument / Q

        in_spot_advance: bool = False
        if self._spot_advance_detect_transmittance > 0.0 and self._Ir0:
            for i in range(min(len(self.data_Ir_wavelength), len(self._Ir0))):
                Ir = self.data_Ir_wavelength[i].value
                Ir0 = self._Ir0[i]
                if not isfinite(Ir) or not isfinite(Ir0):
                    continue
                if Ir - Ir0 > self._spot_advance_detect_transmittance:
                    in_spot_advance = True
                    _LOGGER.debug(f"Detected spot advance ({Ir} > {Ir0}) in channel {i+1}")
                    break
        self.notify_spot_advancing(in_spot_advance)

        if elapsed_seconds and not in_spot_advance:
            self._calculate_path_length(elapsed_seconds)
        else:
            self.data_Ld(nan)

        if not in_spot_advance:
            self._calculate_ebc(ebc_raw, ebc_factor)
        else:
            for c in self.data_X_wavelength:
                c(nan)

        self.data_Ip([float(c) for c in self.data_Ip_wavelength])
        self.data_If([float(c) for c in self.data_If_wavelength])
        if not in_spot_advance:
            self.data_Ba([float(c) for c in self.data_Ba_wavelength])
            self.data_X([float(c) for c in self.data_X_wavelength])
            self.data_Ir([float(c) for c in self.data_Ir_wavelength])
            self._Ir0 = [float(c) for c in self.data_Ir_wavelength]
        else:
            self.data_Ba([nan for _ in self.data_Ba_wavelength])
            self.data_X([nan for _ in self.data_X_wavelength])
            self.data_Ir([nan for _ in self.data_Ir_wavelength])
            self._Ir0 = None

        self.instrument_report()
