import typing
import asyncio
from math import nan
from forge.tasks import wait_cancelable
from forge.units import temperature_f_to_c
from ..streaming import StreamingInstrument, StreamingContext, CommunicationsError
from ..parse import parse_number, parse_datetime_field


class Instrument(StreamingInstrument):
    INSTRUMENT_TYPE = "purpleair"
    MANUFACTURER = "Purple Air"
    MODEL = "PA-II"
    DISPLAY_LETTER = "A"
    TAGS = frozenset({"aerosol", "purpleair"})
    SERIAL_PORT = {'baudrate': 115200}
    INSTRUMENT_INFO_METADATA = {
        **StreamingInstrument.INSTRUMENT_INFO_METADATA,
        **{
            'mac_address': "instrument Wi-Fi MAC address",
            'hardware': "instrument hardware description",
        }
    }

    def __init__(self, context: StreamingContext):
        super().__init__(context)

        self._report_interval: float = float(context.config.get('REPORT_INTERVAL', default=120))

        self.data_Xa = self.input("Xa")
        self.data_Xb = self.input("Xb")
        self.data_IBsa = self.input("IBsa")
        self.data_IBsb = self.input("IBsb")
        self.data_T = self.input("T")
        self.data_U = self.input("U")
        self.data_P = self.input("P")

        self.instrument_report = self.report(
            self.variable(self.data_Xa, "detector_a_mass_concentration", code="ZXa", attributes={
                'long_name': "detector A reported PM2.5 mass concentration",
                'units': "ug m-3",
                'C_format': "%6.1f"
            }),
            self.variable(self.data_Xb, "detector_b_mass_concentration", code="ZXb", attributes={
                'long_name': "detector B reported PM2.5 mass concentration",
                'units': "ug m-3",
                'C_format': "%6.1f"
            }),

            self.variable(self.data_IBsa, "detector_a_intensity", code="Ipa", attributes={
                'long_name': "detector A raw intensity",
                'C_format': "%7.2f"
            }),
            self.variable(self.data_IBsb, "detector_b_intensity", code="Ipb", attributes={
                'long_name': "detector B raw intensity",
                'C_format': "%7.2f"
            }),

            self.variable_air_temperature(self.data_T, "ambient_temperature", code="T"),
            self.variable_air_rh(self.data_U, "ambient_humidity", code="U"),
            self.variable_air_pressure(self.data_P, "ambient_pressure", code="P"),
        )

    async def _read_frame(self) -> bytes:
        while True:
            line: bytes = await self.read_line()
            if line.startswith(b"disk"):
                continue
            if line.startswith(b"ls"):
                continue
            if line.startswith(b"append "):
                continue
            return line

    async def start_communications(self) -> None:
        # This is less reliable than a normal record flush, but the slow reporting makes that
        # undesirable
        try:
            await wait_cancelable(self._read_frame(), 5.0)
        except asyncio.TimeoutError:
            pass
        # Flush the first record
        await self.drain_reader(0.5)

        # Process a valid record
        await self.communicate()

    async def communicate(self) -> None:
        line: bytes = await wait_cancelable(self._read_frame(), self._report_interval * 2 + 1)
        if len(line) < 3:
            raise CommunicationsError

        # UTCDateTime,mac_address,firmware_ver,hardware,current_temp_f,current_humidity,
        # current_dewpoint_f,pressure,adc,mem,rssi,uptime,pm1_0_cf_1,pm2_5_cf_1,pm10_0_cf_1,
        # pm1_0_atm,pm2_5_atm,pm10_0_atm,pm2.5_aqi_cf_1,pm2.5_aqi_atm,p_0_3_um,p_0_5_um,
        # p_1_0_um,p_2_5_um,p_5_0_um,p_10_0_um,pm1_0_cf_1_b,pm2_5_cf_1_b,pm10_0_cf_1_b,
        # pm1_0_atm_b,pm2_5_atm_b,pm10_0_atm_b,pm2.5_aqi_cf_1_b,pm2.5_aqi_atm_b,p_0_3_um_b,
        # p_0_5_um_b,p_1_0_um_b,p_2_5_um_b,p_5_0_um_b,p_10_0_um_b,gas

        fields = line.split(b',')
        try:
            (
                date_time, mac_address, firmware_version, hardware,
                *fields
            ) = fields
        except ValueError:
            raise CommunicationsError(f"invalid number of fields in {line}")

        date_time = date_time.upper()
        if date_time.endswith(b'Z'):
            date_time = date_time[:-1]
        parse_datetime_field(date_time, datetime_seperator=b'T', date_separator=b'/')

        if mac_address.count(b':') != 5:
            raise CommunicationsError(f"invalid MAC address in {line}")
        if b'+' not in hardware:
            raise CommunicationsError(f"invalid hardware description {line}")

        # Handle record corruption due to instrument onboard debug output
        for i in range(len(fields)):
            if b"append" not in fields[i]:
                continue
            fields = fields[:i]
            break

        if len(fields) > 0:
            self.data_T(temperature_f_to_c(parse_number(fields[0])))
        else:
            self.data_T(nan)

        if len(fields) > 1:
            self.data_U(parse_number(fields[1]))
        else:
            self.data_U(nan)

        if len(fields) > 3:
            self.data_P(parse_number(fields[3]))
        else:
            self.data_P(nan)

        if len(fields) > 9:
            self.data_Xa(parse_number(fields[9]))
        else:
            self.data_Xa(nan)

        if len(fields) > 16:
            self.data_IBsa(parse_number(fields[16]))
        else:
            self.data_IBsa(nan)

        if len(fields) > 23:
            self.data_Xb(parse_number(fields[23]))
        else:
            self.data_Xb(nan)

        if len(fields) > 30:
            self.data_IBsb(parse_number(fields[30]))
        else:
            self.data_IBsb(nan)

        self.set_firmware_version(firmware_version)
        self.set_instrument_info('mac_address', mac_address.decode('utf-8', 'backslashreplace'))
        self.set_instrument_info('hardware', hardware.decode('utf-8', 'backslashreplace'))

        self.instrument_report()
