import time
import typing
import asyncio
from forge.tasks import wait_cancelable
from forge.units import speed_knots_to_ms, speed_mph_to_ms, speed_kph_to_ms, temperature_f_to_c, pressure_Pa_to_hPa, pressure_bar_to_hPa, pressure_mmHg_to_hPa, pressure_inHg_to_hPa, distance_in_to_mm
from ..streaming import StreamingInstrument, StreamingContext, CommunicationsError
from ..parse import parse_number

_INSTRUMENT_TYPE = __name__.split('.')[-2]


class Instrument(StreamingInstrument):
    INSTRUMENT_TYPE = _INSTRUMENT_TYPE
    MANUFACTURER = "Vaisala"
    MODEL = "WXT5xx"
    DISPLAY_LETTER = "I"
    TAGS = frozenset({"met", "aerosol", _INSTRUMENT_TYPE})
    SERIAL_PORT = {'baudrate': 19200}

    def __init__(self, context: StreamingContext):
        super().__init__(context)

        self._report_interval: float = float(context.config.get('REPORT_INTERVAL', default=1))
        self._address: bytes = str(context.config.get('ADDRESS', default="")).encode('ascii')
        self._reported_address: typing.Optional[bytes] = None
        self._enable_solar_radiation: typing.Optional[bool] = context.config.get('SENSOR.RADIATION')
        self._enable_level_sensor: typing.Optional[bool] = context.config.get('SENSOR.LEVEL')
        self._enable_aux_temperature: typing.Optional[bool] = context.config.get('SENSOR.AUX_TEMPERATURE')
        self._enable_aux_rain: typing.Optional[bool] = context.config.get('SENSOR.RAIN')
        self._set_heater: typing.Optional[bool] = context.config.get('HEATER')

        self.data_WS = self.input("WS")
        self.data_WD = self.input("WD")
        self.data_WSgust = self.input("WSgust")
        self.data_WI = self.input("WI")
        self.data_P = self.input("P")
        self.data_Uambient = self.input("Uambient")
        self.data_Tambient = self.input("Tambient")
        self.data_Tinternal = self.input("Tinternal")
        self.data_Theater = self.input("Theater")
        self.data_Vsupply = self.input("Vsupply")
        self.data_Vreference = self.input("Vreference")
        self.data_Vheater = self.input("Vheater")

        self.notify_heater_on = self.notification("heater_on")

        self.report_winds = self.report(
            *self.variable_winds(self.data_WS, self.data_WD, code=""),

            self.variable(self.data_WSgust, "wind_gust_speed", code="ZWSGust", attributes={
                'long_name': "averaged wind gust speed",
                'standard_name': "wind_speed_of_gust",
                'units': "m s-1",
                'C_format': "%4.1f"
            }),
        )

        self.report_conditions = self.report(
            self.variable_air_pressure(self.data_P, "ambient_pressure", code="P",
                                       attributes={'long_name': "ambient pressure"}),
            self.variable_air_rh(self.data_Uambient, "ambient_humidity", code="U1",
                                 attributes={'long_name': "ambient relative humidity"}),
            self.variable_air_temperature(self.data_Tambient, "ambient_temperature", code="T1",
                                          attributes={'long_name': "ambient air temperature"}),
            self.variable_temperature(self.data_Tinternal, "internal_temperature", code="T2",
                                      attributes={'long_name': "instrument internal temperature"}),
        )

        self.report_precipitation = self.report(
            self.variable(self.data_WI, "precipitation_rate", code="WI", attributes={
                'long_name': "precipitation rate",
                'units': "mm h-1",
                'C_format': "%7.3f"
            }),
        )

        self.report_monitor = self.report(
            self.variable_temperature(self.data_Theater, "heater_temperature", code="T3",
                                      attributes={'long_name': "heater temperature"}),

            flags=[
                self.flag(self.notify_heater_on)
            ],
        )

        self._accumulated_rain: typing.Optional[float] = None
        self._accumulated_rain_time: typing.Optional[float] = None

        self._data_R: typing.Optional[Instrument.Input] = None
        self._report_solar_radiation: typing.Optional[Instrument.Report] = None

        self._data_Ld: typing.Optional[Instrument.Input] = None
        self._report_level_sensor: typing.Optional[Instrument.Report] = None

        self._data_Taux: typing.Optional[Instrument.Input] = None
        self._report_auxiliary_temperature: typing.Optional[Instrument.Report] = None

    @property
    def data_R(self) -> "Instrument.Input":
        if self._data_R:
            return self._data_R
        self._data_R = self.input("R")
        return self._data_R

    @property
    def report_solar_radiation(self) -> "Instrument.Report":
        if self._report_solar_radiation:
            return self._report_solar_radiation

        self._report_solar_radiation = self.report(
            self.variable(self.data_R, "solar_radiation", code="VA", attributes={
                'long_name': "solar radiation intensity",
                'standard_name': 'solar_irradiance',
                'units': "W m-2",
                'C_format': "%7.2f"
            }),
        )

        return self._report_solar_radiation

    @property
    def data_Ld(self) -> "Instrument.Input":
        if self._data_Ld:
            return self._data_Ld
        self._data_Ld = self.input("Ld")
        return self._data_Ld

    @property
    def report_level_sensor(self) -> "Instrument.Report":
        if self._report_level_sensor:
            return self._report_level_sensor

        self._report_level_sensor = self.report(
            self.variable(self.data_Ld, "level_sensor", code="Ld", attributes={
                'long_name': "measured distance from level sensor",
                'units': "m",
                'C_format': "%5.2f"
            }),
        )

        return self._report_level_sensor

    @property
    def data_Taux(self) -> "Instrument.Input":
        if self._data_Taux:
            return self._data_Taux
        self._data_Taux = self.input("Taux")
        return self._data_Taux

    @property
    def report_auxiliary_temperature(self) -> "Instrument.Report":
        if self._report_auxiliary_temperature:
            return self._report_auxiliary_temperature

        self._report_auxiliary_temperature = self.report(
            self.variable_temperature(self.data_Taux, "auxiliary_temperature", code="T4",
                                      attributes={'long_name': "auxiliary temperature sensor"}),
        )

        return self._report_auxiliary_temperature

    @property
    def _target_address(self) -> bytes:
        if self._reported_address:
            return self._reported_address
        return self._address or b"0"

    async def start_communications(self) -> None:
        if self.writer:
            # Stop reports
            self.writer.write(b"\r\n")
            self.writer.write((self._address or b"0") + b"XU,I=0,M=P\r\n")
            self.writer.write(b"!\r\n")
            self.writer.write((self._address or b"0") + b"XXU,I=0!")
            self.writer.write(b"!\r\n")
            await self.writer.drain()
            await self.drain_reader(self._report_interval + 1.0)

            async def get_address():
                self.writer.write(b"?\r\n")
                data: bytes = await wait_cancelable(self.read_line(), 2.0)
                if len(data) == 1 and data.isalnum():
                    self._reported_address = data
                    return True

                self.writer.write(b"?!")
                data: bytes = await wait_cancelable(self.read_line(), 2.0)
                if len(data) == 1 and data.isalnum():
                    self._reported_address = data
                    return True

                return False

            if not await get_address():
                # Try a full reset
                if self._address:
                    self.writer.write(self._address + b"XZ\r\n")
                    self.writer.write(self._address + b"!")
                else:
                    for addr in range(0, 10+26+26):
                        if addr < 10:
                            addr = bytes([addr+ord('0')])
                        elif addr < 10+26:
                            addr = bytes([addr-10+ord('A')])
                        else:
                            addr = bytes([addr-10-26+ord('a')])
                        self.writer.write(addr + b"XZ\r\n")
                        self.writer.write(addr + b"!")

                async def process_response():
                    while True:
                        line: bytes = await wait_cancelable(self.read_line(), 2.0)
                        if data.startswith(b"$"):
                            checksum = line[-3:]
                            if checksum[:1] != b'*':
                                continue
                            checksum = checksum[1:]
                            try:
                                checksum = int(checksum.strip(), 16)
                                if checksum < 0 or checksum > 0xFF:
                                    continue
                            except ValueError:
                                continue
                            frame = line[1:-3]
                            v = 0
                            for b in frame:
                                v ^= b
                            if v != checksum:
                                continue

                            break
                        if len(line) == 1 and line.isalnum():
                            self._reported_address = line
                            break

                await wait_cancelable(process_response(), 3.0)

            def split_settings(line: bytes) -> typing.Dict[bytes, bytes]:
                result: typing.Dict[bytes, bytes] = dict()
                fields = line.split(b',')
                if len(fields) < 2:
                    raise CommunicationsError
                if not fields[0].startswith(self._target_address):
                    raise CommunicationsError
                fields = fields[1:]
                for setting in fields:
                    setting = setting.strip()
                    try:
                        (key, value) = setting.split(b'=', 1)
                    except ValueError:
                        raise CommunicationsError
                    key = key.strip()
                    if len(key) != 1:
                        raise CommunicationsError
                    value = value.strip()
                    if not value:
                        continue
                    result[key] = value
                return result

            # Communications parameters
            self.writer.write(self._target_address + b"XU\r\n")
            data: bytes = await wait_cancelable(self.read_line(), 5.0)
            settings = split_settings(data)
            param = settings.get(b"V")
            if param:
                self.set_firmware_version(param)
            param = settings.get(b"N")
            maybe_has_auxiliary = True
            if param and param.startswith(b"WXT"):
                model = param.decode('ascii')
                self.set_instrument_info('model', model)
                if model.upper().startswith("WXT52"):
                    maybe_has_auxiliary = False

            command = self._target_address + b"XU,I=0"
            param = settings.get(b"M")
            if param == b"A" or param == b"a" or param == b"N":
                command += b",M=Q"
            command += b"\r\n"
            self.writer.write(command)
            await self.writer.drain()
            await self.drain_reader(1.0)

            report_seconds = round(self._report_interval)
            if report_seconds < 1:
                report_seconds = 1
            elif report_seconds > 3600:
                report_seconds = 3600

            # Conditions
            self.writer.write(self._target_address + b"TU\r\n")
            data: bytes = await wait_cancelable(self.read_line(), 5.0)
            split_settings(data)
            self.writer.write(self._target_address + b"TU,R=11110000&11110000,I=%d,P=H,T=C\r\n" % report_seconds)
            await self.writer.drain()
            await self.drain_reader(1.0)

            # Winds
            self.writer.write(self._target_address + b"WU\r\n")
            data: bytes = await wait_cancelable(self.read_line(), 5.0)
            split_settings(data)
            self.writer.write(self._target_address + b"WU,R=01001100&01001100,I=%d,A=%d,G=3,U=M,N=T,F=4\r\n" %
                              (report_seconds, report_seconds))
            await self.writer.drain()
            await self.drain_reader(1.0)

            # Precipitation
            self.writer.write(self._target_address + b"RU\r\n")
            data: bytes = await wait_cancelable(self.read_line(), 5.0)
            split_settings(data)
            self.writer.write(self._target_address + b"RU,R=00100100&00100100,I=%d,U=M,S=M,M=T\r\n" % report_seconds)
            await self.writer.drain()
            await self.drain_reader(1.0)

            # Monitor
            self.writer.write(self._target_address + b"SU\r\n")
            data: bytes = await wait_cancelable(self.read_line(), 5.0)
            split_settings(data)
            command = self._target_address + b"SU,R=11111000&11111000,I=%d,S=N" % report_seconds
            if self._set_heater is not None:
                if self._set_heater:
                    command += b",H=Y"
                else:
                    command += b",H=N"
            command += b"\r\n"
            self.writer.write(command)
            await self.writer.drain()
            await self.drain_reader(1.0)

            async def set_auxiliary():
                if not maybe_has_auxiliary:
                    return
                self.writer.write(self._target_address + b"IU\r\n")
                try:
                    data: bytes = await wait_cancelable(self.read_line(), 5.0)
                except (TimeoutError, asyncio.TimeoutError):
                    return
                try:
                    settings = split_settings(data)
                except CommunicationsError:
                    await self.drain_reader(1.0)
                    return

                try:
                    (polled, unpolled) = settings.get(b"R", b"11111111&11111111").split(b'&', 1)
                except ValueError:
                    polled = b"11111111"
                    unpolled = b"11111111"

                polled = bytearray(polled.ljust(8, b"0"))
                unpolled = bytearray(unpolled.ljust(8, b"0"))

                if self._enable_aux_temperature is not None:
                    if self._enable_aux_temperature:
                        polled[0] = ord('1')
                        unpolled[0] = ord('1')
                    else:
                        polled[0] = ord('0')
                        unpolled[0] = ord('0')
                if self._enable_aux_rain is not None:
                    if self._enable_aux_rain:
                        polled[1] = ord('1')
                        unpolled[1] = ord('1')
                    else:
                        polled[1] = ord('0')
                        unpolled[1] = ord('0')
                if self._enable_level_sensor is not None:
                    if self._enable_level_sensor:
                        polled[2] = ord('1')
                        unpolled[2] = ord('1')
                    else:
                        polled[2] = ord('0')
                        unpolled[2] = ord('0')
                if self._enable_solar_radiation is not None:
                    if self._enable_solar_radiation:
                        polled[3] = ord('1')
                        unpolled[3] = ord('1')
                    else:
                        polled[3] = ord('0')
                        unpolled[3] = ord('0')

                self.writer.write(self._target_address + b"RU,R=" + bytes(polled) + b"&" + bytes(unpolled) +
                                  b",I=%d,A=%d\r\n" % (report_seconds, report_seconds))
                await self.writer.drain()
                await self.drain_reader(1.0)

            await set_auxiliary()

            self.writer.write(self._target_address +b"XU,M=N\r\n")
            await self.writer.drain()

        # Flush the first record
        await self.drain_reader(2.0)
        await wait_cancelable(self.read_line(), self._report_interval * 3 + 3)

        self._accumulated_rain = None
        self._accumulated_rain_time = None

        # Process a valid record
        await self.communicate()

    async def communicate(self) -> None:
        line: bytes = await wait_cancelable(self.read_line(), self._report_interval * 2 + 1)
        if len(line) < 4:
            raise CommunicationsError

        checksum = line[-3:]
        if checksum[:1] != b"*":
            raise CommunicationsError(f"invalid checksum in {line}")
        checksum = checksum[1:]
        try:
            checksum = int(checksum.strip(), 16)
            if checksum < 0 or checksum > 0xFF:
                raise ValueError
        except ValueError:
            raise CommunicationsError(f"invalid checksum in {line}")

        frame = line[1:-3]
        v = 0
        for b in frame:
            v ^= b
        if v != checksum:
            raise CommunicationsError(f"checksum mismatch on {line} (got {v:02X})")

        fields = frame.strip().split(b",")
        if fields[0] != b"WIXDR":
            raise CommunicationsError(f"invalid record in {line}")
        fields = fields[1:]

        if len(fields) < 4:
            raise CommunicationsError(f"invalid number of fields in {fields}")

        try:
            id_offset = ord(self._target_address)
            if ord(b"0") <= id_offset <= ord(b"9"):
                id_offset = id_offset - ord(b"0")
            elif ord(b"A") <= id_offset <= ord(b"Z"):
                id_offset = id_offset - ord(b"A") + 10
            elif ord(b"a") <= id_offset <= ord(b"z"):
                id_offset = id_offset - ord(b"a") + 10 + 26
            else:
                raise TypeError
        except TypeError:
            id_offset = 0

        rain_accumulation: typing.Optional[float] = None
        aux_rain_accumulation: typing.Optional[float] = None
        seen_reports: typing.Set["Instrument.Report"] = set()
        for i in range(0, len(fields), 4):
            try:
                (transducer_type, value, units, transducer_id) = fields[i:(i+4)]
            except ValueError:
                raise CommunicationsError(f"invalid number of fields at {i} in {fields}")
            try:
                transducer_id = int(transducer_id.strip())
            except(ValueError, OverflowError):
                raise CommunicationsError(f"invalid transducer id {transducer_id} at {i} in {fields}")

            transducer_type = transducer_type.strip().upper()
            units = units.upper().strip()

            if transducer_id < id_offset:
                continue
            transducer_id -= id_offset

            if transducer_type == b"G":
                # if transducer_id == 4:
                #     instrument_id = value.decode('utf-8')
                continue

            if units == b"#":
                continue

            value = parse_number(value)

            if transducer_type == b"C":
                if units == b"C":
                    pass
                elif units == b"F":
                    value = temperature_f_to_c(value)
                else:
                    raise CommunicationsError(f"unrecognized units {units} at {i} in {fields}")

                if transducer_id == 0:
                    self.data_Tambient(value)
                    seen_reports.add(self.report_conditions)
                elif transducer_id == 1:
                    self.data_Tinternal(value)
                    seen_reports.add(self.report_conditions)
                elif transducer_id == 2:
                    self.data_Theater(value)
                    seen_reports.add(self.report_monitor)
                elif transducer_id == 4:
                    self.data_Taux(value)
                    self.report_auxiliary_temperature()
                else:
                    raise CommunicationsError(f"unrecognized transducer {transducer_id} at {i} in {fields}")
            elif transducer_type == b"A":
                if units == b"D" or units == b"R" or units == b"":
                    pass
                else:
                    raise CommunicationsError(f"unrecognized units {units} at {i} in {fields}")

                if transducer_id == 0:
                    # Wind direction minimum
                    pass
                elif transducer_id == 1:
                    self.data_WD(value)
                    seen_reports.add(self.report_winds)
                elif transducer_id == 2:
                    # Wind direction maximum
                    pass
                else:
                    raise CommunicationsError(f"unrecognized transducer {transducer_id} at {i} in {fields}")
            elif transducer_type == b"S":
                if units == b"M" or units == b'':
                    pass
                elif units == b"K":
                    value = speed_kph_to_ms(value)
                elif units == b"S":
                    value = speed_mph_to_ms(value)
                elif units == b"N":
                    value = speed_knots_to_ms(value)
                else:
                    raise CommunicationsError(f"unrecognized units {units} at {i} in {fields}")

                if transducer_id == 0:
                    # Wind speed minimum
                    pass
                elif transducer_id == 1:
                    self.data_WS(value)
                    seen_reports.add(self.report_winds)
                elif transducer_id == 2:
                    self.data_WSgust(value)
                    seen_reports.add(self.report_winds)
                else:
                    raise CommunicationsError(f"unrecognized transducer {transducer_id} at {i} in {fields}")
            elif transducer_type == b"P":
                if units == b"H" or units == b'':
                    pass
                elif units == b"P":
                    value = pressure_Pa_to_hPa(value)
                elif units == b"B":
                    value = pressure_bar_to_hPa(value)
                elif units == b"M":
                    value = pressure_mmHg_to_hPa(value)
                elif units == b"I":
                    value = pressure_inHg_to_hPa(value)
                else:
                    raise CommunicationsError(f"unrecognized units {units} at {i} in {fields}")

                if transducer_id == 0:
                    self.data_P(value)
                    seen_reports.add(self.report_conditions)
                else:
                    raise CommunicationsError(f"unrecognized transducer {transducer_id} at {i} in {fields}")
            elif transducer_type == b"H":
                if units == b"P" or units == b"":
                    pass
                else:
                    raise CommunicationsError(f"unrecognized units {units} at {i} in {fields}")

                if transducer_id == 0:
                    self.data_Uambient(value)
                    seen_reports.add(self.report_conditions)
                else:
                    raise CommunicationsError(f"unrecognized transducer {transducer_id} at {i} in {fields}")
            elif transducer_type == b"V":
                if units == b"M" or units == b'':
                    pass
                elif units == b"I":
                    value = distance_in_to_mm(value)
                elif units == b"H":
                    # hits
                    continue
                else:
                    raise CommunicationsError(f"unrecognized units {units} at {i} in {fields}")

                if transducer_id == 0:
                    rain_accumulation = value
                elif transducer_id == 1:
                    # Hail accumulation shared with auxiliary rain accumulation
                    if self._enable_aux_rain is None or self._enable_aux_rain:
                        aux_rain_accumulation = value
                else:
                    raise CommunicationsError(f"unrecognized transducer {transducer_id} at {i} in {fields}")
            elif transducer_type == b"Z":
                if transducer_id == 0:
                    # Rain duration
                    pass
                elif transducer_id == 1:
                    # Hail duration
                    pass
                else:
                    raise CommunicationsError(f"unrecognized transducer {transducer_id} at {i} in {fields}")
            elif transducer_type == b"R":
                if units == b"M" or units == b'':
                    pass
                elif units == b"I":
                    value = distance_in_to_mm(value)
                elif units == b"H":
                    # hits
                    continue
                else:
                    raise CommunicationsError(f"unrecognized units {units} at {i} in {fields}")

                if transducer_id == 0:
                    self.data_WI(value)
                    seen_reports.add(self.report_precipitation)
                elif transducer_id == 1:
                    # Hail intensity
                    pass
                elif transducer_id == 2:
                    # Rain peak intensity
                    pass
                elif transducer_id == 3:
                    # Hail peak intensity
                    pass
                else:
                    raise CommunicationsError(f"unrecognized transducer {transducer_id} at {i} in {fields}")
            elif transducer_type == b"U":
                if transducer_id == 0:
                    self.data_Vsupply(value)
                elif transducer_id == 1:
                    self.data_Vheater(value)
                    self.notify_heater_on(units == b"V" or units == b"W" or units == b"F")
                    seen_reports.add(self.report_monitor)
                elif transducer_id == 2:
                    self.data_Vreference(value)
                elif transducer_id == 3:
                    self.data_R(value)
                    self.report_solar_radiation()
                elif transducer_id == 4:
                    self.data_Ld(value)
                    self.report_level_sensor()
                else:
                    raise CommunicationsError(f"unrecognized transducer {transducer_id} at {i} in {fields}")
            else:
                raise CommunicationsError(f"unrecognized type {transducer_type} at {i} in {fields}")

        now = time.monotonic()
        if self.report_conditions not in seen_reports:
            if rain_accumulation is not None and self._accumulated_rain is not None:
                if rain_accumulation >= self._accumulated_rain:
                    self.data_WI((rain_accumulation - self._accumulated_rain) / (60 * 60))
                    seen_reports.add(self.report_precipitation)
            elif aux_rain_accumulation is not None and self._accumulated_rain is not None:
                if aux_rain_accumulation >= self._accumulated_rain:
                    self.data_WI((aux_rain_accumulation - self._accumulated_rain) / (60 * 60))
                    seen_reports.add(self.report_precipitation)

        if rain_accumulation is not None:
            self._accumulated_rain = rain_accumulation
        elif aux_rain_accumulation is not None:
            self._accumulated_rain = aux_rain_accumulation
        else:
            self._accumulated_rain = None
        self._accumulated_rain_time = now

        for r in seen_reports:
            r()
