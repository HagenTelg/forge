import typing
import asyncio
import enum
import datetime
import time
import re
from math import nan, isfinite
from forge.tasks import wait_cancelable
from forge.units import flow_m3s_to_lpm, flow_lpm_to_ccs
from ..streaming import StreamingInstrument, StreamingContext, CommunicationsError
from ..parse import parse_number
from ..variable import Input
from ..array import ArrayInput
from ..record import Report

_INSTRUMENT_TYPE = __name__.split('.')[-2]
_MODEL_NUMBER = re.compile(br"(?:OPC\s+)?Model\s*:?\s*([^\s:]+)\s*Version\s*:?\s*([^\s:].*)\s*(?:[a-z]{2}\s*)?",
                           flags=re.IGNORECASE)
_FIRMWARE_VERSION = re.compile(br"\s*Ver(?:sion)?\s*:?\s*([^\s:].*)\s*(?:[a-z]{2}\s*)?",
                               flags=re.IGNORECASE)
_SERIAL_NUMBER = re.compile(br"Ser(?:ial)?\s*\.\s*(?:(?:Number)|(?:No))\s*\.\s*:?\s*([^\s:]+)\s*(?:Sensor\s*:\s*([^\s:]+)\s*)?",
                            flags=re.IGNORECASE)


class Instrument(StreamingInstrument):
    INSTRUMENT_TYPE = _INSTRUMENT_TYPE
    MANUFACTURER = "Grimm"
    MODEL = "1.10x"
    TAGS = frozenset({"aerosol", "size", "opc", _INSTRUMENT_TYPE})
    SERIAL_PORT = {'baudrate': 9600}

    class _VolumeQueryState(enum.Enum):
        WAIT_FIRST_MINUTE = enum.auto()
        WAIT_DATA = enum.auto()
        ISSUE_FIRST = enum.auto()
        RESPOND_FIRST = enum.auto()
        ISSUE_SECOND = enum.auto()
        RESPOND_SECOND = enum.auto()

    def __init__(self, context: StreamingContext):
        super().__init__(context)

        self.data_N = self.input("N")
        self.data_Q = self.input("Q")
        self.data_PCTpump = self.input("PCTpump")
        self.data_PCTbattery = self.input("PCTbattery")

        self.data_dN = self.input_array("dN")
        self.data_Dp = self.persistent("Dp", save_value=False)

        self._flow_correction = 1.2
        self._gf_scale = 1.0 / 20.0
        self._gf: float = nan
        self._volume_query = self._VolumeQueryState.WAIT_FIRST_MINUTE
        self._last_volume_time: typing.Optional[float] = None
        self._last_volume_total: typing.Optional[float] = None

        dimension_Dp = self.dimension_size_distribution_diameter(self.data_Dp, code="Ns", attributes={
            'cell_methods': "time: mean",
        })

        self.integer_flags: typing.Dict[int, Instrument.Notification] = {
            1: self.notification("pump_high", is_warning=True),
            2: self.notification("pump_low", is_warning=True),
            4: self.notification("pump_current_high", is_warning=True),
            8: self.notification("battery_low"),
            16: self.notification("battery_drained"),
            32: self.notification("nozzle_fault", is_warning=True),
            64: self.notification("memory_card_fault"),
            128: self.notification("self_test_failure", is_warning=True),
            3: self.notification("flow_error", is_warning=True),
        }
        self.count_report = self.report(
            self.variable_number_concentration(self.data_N, code="N"),
            self.variable_size_distribution_dN(self.data_dN, dimension_Dp, code="Nb"),

            self.variable_sample_flow(self.data_Q, code="Q",
                                      attributes={'C_format': "%6.3f"}),

            flags=[
                self.flag(n, b) for b, n in self.integer_flags.items()
            ],
        )

        self.data_X1: typing.Optional[Input] = None
        self.data_X25: typing.Optional[Input] = None
        self.data_X10: typing.Optional[Input] = None
        self.data_X: typing.Optional[ArrayInput] = None
        self.mass_report: typing.Optional[Report] = None

    def _declare_mass(self) -> None:
        if self.mass_report:
            return
        self.data_X1 = self.input("X1")
        self.data_X25 = self.input("X25")
        self.data_X10 = self.input("X10")

        self.data_X = self.input_array("X", send_to_bus=False)
        if self.data_X.field.use_cut_size is None:
            self.data_X.field.use_cut_size = False

        data_mass_diameter = self.persistent("mass_diameter", save_value=False, send_to_bus=False)
        data_mass_diameter([1.0, 2.5, 10.0])

        dimension_diameter = self.dimension_size_distribution_diameter(data_mass_diameter, "mass_diameter", attributes={
            'long_name': "particle mass upper particle diameter threshold",
            'units': 'um',
            'C_format': "%4.1f"
        })

        self.mass_report = self.report(
            self.variable_array(self.data_X, dimension_diameter, "mass_concentration", code="X", attributes={
                'long_name': "calculated mass concentration of particles",
                'units': "ug m-3",
                'C_format': "%6.1f"
            }),
        )

    def _apply_model_defaults(self, model: bytes) -> None:
        if model.endswith(b"1"):
            self._flow_correction = 0.6
            if not self.data_Dp.value:
                self.data_Dp([nan] * 14)
        elif model.endswith(b"4"):
            self._flow_correction = 1.2
            if not self.data_Dp.value:
                self.data_Dp([nan] * 14)
        elif model.endswith(b"5"):
            self._flow_correction = 1.2
            if not self.data_Dp.value:
                self.data_Dp(self._calculate_sizes([0.75, 1.0, 2.0, 3.5, 5.0, 7.5, 10.0, 15.0]))
        elif model.endswith(b"7") or model.endswith(b"9") or model.endswith(b"11-a"):
            self._flow_correction = 1.2
            if not self.data_Dp.value:
                self.data_Dp(self._calculate_sizes([
                    0.25, 0.28, 0.30, 0.35, 0.40, 0.45, 0.50, 0.58,
                    0.65, 0.70, 0.80, 1.0,  1.3,  1.6,  2.0,  2.5,
                    3.0,  3.5,  4.0,  5.0,  6.5,  7.5,  8.5, 10.0,
                    12.5, 15.0, 17.5, 20.0, 25.0, 30.0, 32.0,
                ]))
        elif model.endswith(b"180mc"):
            self._flow_correction = 1.2
            self._declare_mass()
            if not self.data_Dp.value:
                self.data_Dp(self._calculate_sizes([
                    0.25, 0.28, 0.30, 0.35, 0.40, 0.45, 0.50, 0.58,
                    0.65, 0.70, 0.80, 1.0,  1.3,  1.6,  2.0,  2.5,
                    3.0,  3.5,  4.0,  5.0,  6.5,  7.5,  8.5, 10.0,
                    12.5, 15.0, 17.5, 20.0, 25.0, 30.0, 32.0,
                ]))
        elif model.endswith(b"8"):
            self._flow_correction = 1.2
            self._declare_mass()
            if not self.data_Dp.value:
                self.data_Dp(self._calculate_sizes([
                    0.30, 0.40, 0.50, 0.65, 0.80, 1.0, 1.6, 2.0,
                    3.0,  4.0,  5.0,  7.5,  10.0, 15.0, 20.0,
                ]))

    def _apply_firmware_defaults(self, firmware) -> None:
        if isinstance(firmware, bytes):
            firmware = firmware.split(b" ", 1)[0]
        elif isinstance(firmware, str):
            firmware = firmware.split(" ", 1)[0]
        try:
            ver = float(firmware)
            if ver >= 7.0:
                self._gf_scale = 1.0 / 100.0
            else:
                self._gf_scale = 1.0 / 20.0
        except (ValueError, TypeError):
            pass

    @staticmethod
    def _calculate_sizes(raw: typing.List[float]) -> typing.List[float]:
        if not raw:
            return raw
        result: typing.List[float] = list()
        for i in range(len(raw) - 1):
            result.append((raw[i] + raw[i+1]) / 2.0)
        if len(raw) <= 1:
            result.append(raw[0])
        else:
            result.append(raw[-1] + (raw[-1] + raw[-2]) / 2.0)
        return result

    @staticmethod
    def _split_fields(line: bytes) -> typing.List[bytes]:
        if len(line) < 3:
            raise CommunicationsError(f"invalid fields in {line}")
        fields = line.split()

        del fields[0]
        # 180 can have a space before the ":"
        if len(fields) > 1 and (fields[0] == b":" or fields[0] == b";"):
            del fields[0]

        return fields

    @staticmethod
    def _minute_index(line: bytes) -> int:
        try:
            minute_index = int(line[1:2])
        except (ValueError, TypeError):
            raise CommunicationsError(f"invalid minute {line}")
        return minute_index

    def _response_bins(self, lines: typing.List[bytes]) -> typing.List[float]:
        if len(lines[0]) == 1:  # Ignore echo
            del lines[0]
        if len(lines) < 1:
            raise CommunicationsError

        bins: typing.List[float] = list()
        for l in lines:
            fields = self._split_fields(l)

            # 180 can have trailing zero fields
            del fields[8:]

            bins.extend([parse_number(f) for f in fields])

        # The midpoint duplicates a value at the half transition, so that each line is 8 bins long
        if len(bins) > 8 and len(bins) % 8 == 0 and bins[len(bins)//2] == bins[len(bins)//2 - 1]:
            del bins[len(bins)//2]
        return bins

    def _process_m_response(self, lines: typing.List[bytes]) -> None:
        pass

    def _process_j_response(self, lines: typing.List[bytes]) -> None:
        if len(lines[0]) == 1:  # Ignore echo
            del lines[0]

        # 180 can return this on the first line
        if len(lines) > 0 and lines[0].startswith(b"J:") and b"PM10" in lines[0] and b"PM2.5" in lines[0] and b"PM1.0" in lines[0]:
            del lines[0]

        bins = self._response_bins(lines)
        self.data_Dp(self._calculate_sizes(bins), oneshot=True, deduplicate=True)

    def _calculate_concentration(self, Nb: typing.Iterable[float]) -> None:
        N = None
        for v in Nb:
            if not isfinite(v):
                continue
            if N is None:
                N = v
            else:
                N += v
        if N is None:
            self.data_N(nan)
            return
        self.data_N(N)

    def _process_n_response(self, lines: typing.List[bytes]) -> int:
        if len(lines[0]) == 1:  # Ignore echo
            del lines[0]
        if len(lines) < 1:
            raise CommunicationsError

        bins: typing.List[float] = list()
        for l in lines:
            fields = self._split_fields(l)
            if l.startswith(b"N") and l[2:3] == b"," and len(fields) == 3:
                self._declare_mass()

                try:
                    (X10, X25, X1) = fields
                except ValueError:
                    raise CommunicationsError(f"invalid number of fields in {fields}")

                self.data_X1(parse_number(X1) / 10.0)
                self.data_X25(parse_number(X25) / 10.0)
                self.data_X10(parse_number(X10) / 10.0)

                self.data_X([float(self.data_X1), float(self.data_X25), float(self.data_X10)])
                self.mass_report()
                continue

            # 180 can have trailing zero fields
            del fields[8:]

            bins.extend([parse_number(f) for f in fields])

        # 180 mass line
        if len(bins) == 0:
            return self._minute_index(lines[0])

        # The midpoint duplicates a value at the half transition, so that each line is 8 bins long
        if len(bins) > 8 and len(bins) % 8 == 0 and bins[len(bins) // 2] == bins[len(bins) // 2 - 1]:
            del bins[len(bins) // 2]

        # Mass concentrations means no binned masses
        if self.mass_report:
            self.data_X1(nan)
            self.data_X25(nan)
            self.data_X10(nan)
            self.data_X([nan, nan, nan])
            self.mass_report()

        def _calculate(mass: float) -> float:
            # Manufacturer does not define the mapping of mass and gf to concentration
            return nan

        Nb = [_calculate(m) for m in bins]
        self.data_dN(Nb)
        self._calculate_concentration(Nb)
        self.count_report()

        return self._minute_index(lines[0])

    def _process_c_response(self, lines: typing.List[bytes]) -> int:
        bins = self._response_bins(lines)

        elapsed = 6.0

        Nb: typing.List[float] = list()
        for i in range(len(bins)):
            if i == len(bins) - 1:
                N = bins[i]
            else:
                N = bins[i] - bins[i+1]

            N /= flow_lpm_to_ccs(self.data_Q.value) * elapsed
            N *= self._flow_correction
            Nb.append(N)

        self.data_dN(Nb)
        self._calculate_concentration(Nb)
        self.count_report()

        return self._minute_index(lines[0])

    def _process_p_record(self, line: bytes) -> None:
        fields = self._split_fields(line)
        try:
            (
                year, month, day, hour, minute,
                _,  # Location
                gf, err, PCTbattery, PCTpump,
                *fields,  # Calibrations and inputs
            ) = fields
        except ValueError:
            raise CommunicationsError(f"invalid number of fields in {line}")

        try:
            year = int(year)
            if 0 <= year <= 99:
                td = time.gmtime()
                current_century = td.tm_year - (td.tm_year % 100)
                year += current_century
                if year > td.tm_year + 50:
                    year -= 100
            if year < 1900 or year > 2999:
                raise CommunicationsError(f"invalid year {year}")
            month = int(month)
            day = int(day)
            hour = int(hour)
            minute = int(minute)
            datetime.datetime(year, month, day, hour, minute, 0, tzinfo=datetime.timezone.utc)
        except ValueError as e:
            raise CommunicationsError from e

        self._gf = parse_number(gf) * self._gf_scale
        self.data_PCTbattery(parse_number(PCTbattery))
        self.data_PCTpump(parse_number(PCTpump))
        try:
            err = int(err)
        except (ValueError, OverflowError):
            raise CommunicationsError(f"invalid flags {err}")
        for value, check in self.integer_flags.items():
            if err == value:
                check(True)
            elif value & 3 == 0:
                check((err & value) != 0)
            else:
                check(False)

    def _process_k_record(self, line: bytes) -> None:
        pass

    def _process_v_record(self, line: bytes) -> None:
        fields = self._split_fields(line)
        if len(fields) < 1:
            raise CommunicationsError
        volume = fields[0]
        if b"m" in volume:
            idx_units = volume.index(b"m")
            volume = volume[:idx_units].strip()

        volume = parse_number(volume)
        if volume < 0.0:
            raise CommunicationsError(f"invalid volume {line}")

        now = time.monotonic()
        if self._last_volume_time is not None and self._last_volume_total is not None:
            elapsed = now - self._last_volume_time
            volume_change = volume - self._last_volume_total
            if volume_change > 0.0:
                self.data_Q(flow_m3s_to_lpm(volume_change / elapsed))

        self._last_volume_time = now
        self._last_volume_total = volume

    async def start_communications(self) -> None:
        if self.writer:
            self.writer.write(b"\x1B\r\rS\r\x1B\r\rS\rS\r")
            await self.writer.drain()
            await self.drain_reader(1.0)
            self.writer.write(b"S\r")
            await self.writer.drain()
            await self.drain_reader(7.0)

            self.writer.write(b"!\r")
            data: bytes = await wait_cancelable(self.read_line(), 5.0)
            if len(data) == 1:  # Ignore echo
                data: bytes = await wait_cancelable(self.read_line(), 5.0)
            match = _MODEL_NUMBER.fullmatch(data)
            if match:
                self.set_instrument_info('model', match.group(1).decode('ascii', 'ignore').strip())
                self.set_firmware_version(match.group(2))
                self._apply_model_defaults(match.group(1).strip())
                self._apply_firmware_defaults(match.group(2).strip())
            else:
                self.set_instrument_info('model', data.decode('utf-8'))
                self._apply_model_defaults(data)

                self.writer.write(b"V\r")
                data: bytes = await wait_cancelable(self.read_line(), 5.0)
                if len(data) == 1:  # Ignore echo
                    data: bytes = await wait_cancelable(self.read_line(), 5.0)

                match = _FIRMWARE_VERSION.fullmatch(data)
                if match:
                    self.set_firmware_version(match.group(1).strip())
                    self._apply_firmware_defaults(match.group(1).strip())

            self.writer.write(b"@\r")
            data: bytes = await wait_cancelable(self.read_line(), 5.0)
            if len(data) == 1:  # Ignore echo
                data: bytes = await wait_cancelable(self.read_line(), 5.0)
            match = _SERIAL_NUMBER.fullmatch(data)
            if match:
                self.set_serial_number(match.group(1))
                # sensor = match.group(2).decode('ascii', 'ignore').strip()
                # if match.group(2):
                #     self.set_instrument_info('calibration', sensor)

            # Switch to measurement mode
            self.writer.write(b"F\r")
            await self.writer.drain()
            await self.drain_reader(1.0)

            # Count mode on
            self.writer.write(b"C\r")
            await self.writer.drain()
            await self.drain_reader(1.0)

            # Process median record (for volume)
            self._last_volume_time = None
            self._last_volume_total = None
            self.writer.write(b"M\r")
            response = await self.read_multiple_lines(total=4.0, first=2.0, tail=3.0)
            while len(response) > 0:
                if response[0].startswith(b"V"):
                    self._process_v_record(response[0])
                    del response[0]
                    continue
                if response[-1].startswith(b"V"):
                    self._process_v_record(response[-1])
                    del response[-1]
                    continue
                break
            if len(response) != 0:
                self._process_m_response(response)

            self.writer.write(b"J\r")
            response = await self.read_multiple_lines(total=4.0, first=2.0, tail=3.0)
            self._process_j_response(response)

            # Switch to run mode
            self.writer.write(b"R\r")
            await self.writer.drain()
            await self.drain_reader(30.0)

            # Discard a record
            await self.read_multiple_lines(total=12.0, first=8.0, tail=3.0)

        # Flush the first record
        await self.read_multiple_lines(total=12.0, first=8.0, tail=3.0)

        # Process a valid record
        self._volume_query = self._VolumeQueryState.WAIT_FIRST_MINUTE
        await self.communicate()

    async def communicate(self) -> None:
        lines = await self.read_multiple_lines(total=12.0, first=8.0, tail=3.0)

        def separate_record() -> typing.Optional[typing.List[bytes]]:
            if len(lines) == 0:
                return None
            identifier = lines[0][:1].lower()
            result: typing.List[bytes] = [lines[0]]
            del lines[0]
            while len(lines) > 0:
                if lines[0][:1].lower() != identifier:
                    break
                result.append(lines[0])
                del lines[0]
            return result

        first_minute: bool = False
        volume_response: bool = False
        record_index: typing.Optional[int] = None
        while True:
            record = separate_record()
            if not record:
                break
            if record[0].startswith(b"N") or record[0].startswith(b"n"):
                record_index = self._process_n_response(record)
            elif record[0].startswith(b"C") or record[0].startswith(b"c"):
                record_index = self._process_c_response(record)
            elif record[0].startswith(b"M") or record[0].startswith(b"m"):
                self._process_m_response(record)
            elif record[0].startswith(b"P") and len(record) == 1:
                self._process_p_record(record[0])
                first_minute = True
            elif record[0].startswith(b"K") and len(record) == 1:
                self._process_k_record(record[0])
                first_minute = True
            elif record[0].startswith(b"V") and len(record) == 1:
                self._process_v_record(record[0])
            else:
                raise CommunicationsError(f"unknown record response {record}")

        if first_minute:
            self._volume_query = self._VolumeQueryState.WAIT_DATA

        if self._volume_query == self._VolumeQueryState.WAIT_DATA and record_index == 1:
            self._volume_query = self._VolumeQueryState.ISSUE_FIRST
        elif self._volume_query in (self._VolumeQueryState.ISSUE_FIRST, self._VolumeQueryState.RESPOND_FIRST) and record == 2:
            self._volume_query = self._VolumeQueryState.ISSUE_SECOND
        elif record_index == 0:
            self._volume_query = self._VolumeQueryState.WAIT_DATA

        if volume_response:
            self._volume_query = self._VolumeQueryState.WAIT_DATA

        if self.writer:
            # Don't need to sleep before the write because the tail read has already waited
            if self._volume_query == self._VolumeQueryState.ISSUE_FIRST:
                self.writer.write(b"M\r")
                self._volume_query = self._VolumeQueryState.RESPOND_FIRST
            elif self._volume_query == self._VolumeQueryState.ISSUE_SECOND:
                self.writer.write(b"M\r")
                self._volume_query = self._VolumeQueryState.RESPOND_SECOND
