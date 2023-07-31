import typing
import asyncio
import aiohttp
import logging
import traceback
import time
from json import JSONDecodeError
from forge.units import temperature_f_to_c
from ..standard import IterativeCommunicationsInstrument
from ..base import BaseContext, BaseBusInterface, CommunicationsError
from ..parse import parse_datetime_field

_LOGGER = logging.getLogger(__name__)


class Instrument(IterativeCommunicationsInstrument):
    INSTRUMENT_TYPE = "purpleair"
    MANUFACTURER = "Purple Air"
    MODEL = "PA-II"
    DISPLAY_LETTER = "A"
    TAGS = frozenset({"aerosol", "purpleair"})
    INSTRUMENT_INFO_METADATA = {
        **IterativeCommunicationsInstrument.INSTRUMENT_INFO_METADATA,
        **{
            'mac_address': "instrument WiFi MAC address",
            'hardware': "instrument hardware description",
        }
    }

    def __init__(self, context: BaseContext):
        super().__init__(context)

        self._report_interval: float = float(context.config.get('REPORT_INTERVAL', default=1.0))
        self._url: str = str(context.config['URL'])
        self._sleep_time: float = 0

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

    def _process_report(self, fields: typing.Dict[str, typing.Any]) -> None:
        # http://192.168.0.98/json?live=true
        # {"SensorId":"84:f3:eb:d8:e0:48","DateTime":"2020/10/24T16:40:13z",
        # "Geo":"PurpleAir-e048","Mem":19120,"memfrag":22,"memfb":16064,"memcs":1104,"Id":42,
        # "lat":38.426300,"lon":-122.581001,"Adc":0.02,"loggingrate":15,"place":"inside",
        # "version":"6.03","uptime":1993,"rssi":-57,"period":119,"httpsuccess":98,"httpsends":98,
        # "hardwareversion":"3.0",
        # "hardwarediscovered":"3.0+OPENLOG+15802 MB+DS3231+BME280+BME680+PMSX003-A+PMSX003-B",
        # "current_temp_f":73,"current_humidity":21,"current_dewpoint_f":31,"pressure":825.33,
        # "current_temp_f_680":74,"current_humidity_680":21,"current_dewpoint_f_680":32,
        # "pressure_680":825.04,"gas_680":0.00,"p25aqic_b":"rgb(4,228,0)","pm2.5_aqi_b":13,
        # "pm1_0_cf_1_b":2.00,"p_0_3_um_b":456.00,"pm2_5_cf_1_b":3.00,"p_0_5_um_b":138.00,
        # "pm10_0_cf_1_b":3.00,"p_1_0_um_b":24.00,"pm1_0_atm_b":2.00,"p_2_5_um_b":2.00,
        # "pm2_5_atm_b":3.00,"p_5_0_um_b":0.00,"pm10_0_atm_b":3.00,"p_10_0_um_b":0.00,
        # "p25aqic":"rgb(19,230,0)","pm2.5_aqi":21,"pm1_0_cf_1":2.00,"p_0_3_um":522.00,
        # "pm2_5_cf_1":5.00,"p_0_5_um":152.00,"pm10_0_cf_1":6.00,"p_1_0_um":48.00,"pm1_0_atm":2.00,
        # "p_2_5_um":6.00,"pm2_5_atm":5.00,"p_5_0_um":2.00,"pm10_0_atm":6.00,"p_10_0_um":2.00,
        # "pa_latency":183,"response":201,"response_date":1603557533,"latency":379,
        # "key1_response":200,"key1_response_date":1603557527,"key1_count":701,"ts_latency":417,
        # "key2_response":200,"key2_response_date":1603557529,"key2_count":701,"ts_s_latency":405,
        # "key1_response_b":200,"key1_response_date_b":1603557530,"key1_count_b":702,
        # "ts_latency_b":399,"key2_response_b":200,"key2_response_date_b":1603557531,
        # "key2_count_b":701,"ts_s_latency_b":411,"wlstate":"Connected","status_0":2,
        # "status_1":2,"status_2":2,"status_3":2,"status_4":2,"status_5":2,"status_6":2,"status_7":0,
        # "status_8":2,"status_9":2,"ssid":"Purple"}

        date_time = fields["DateTime"].encode('ascii')
        date_time = date_time.upper()
        if date_time.endswith(b'Z'):
            date_time = date_time[:-1]
        parse_datetime_field(date_time, datetime_seperator=b'T', date_separator=b'/')

        self.data_T(temperature_f_to_c(float(fields["current_temp_f"])))
        self.data_U(float(fields["current_humidity"]))
        self.data_P(float(fields["pressure"]))
        self.data_Xa(float(fields["pm2_5_cf_1"]))
        self.data_Xb(float(fields["pm2_5_cf_1_b"]))
        self.data_IBsa(float(fields["p_0_3_um"]))
        self.data_IBsb(float(fields["p_0_3_um_b"]))

        mac_address = fields.get("SensorId")
        if mac_address:
            self.set_instrument_info('mac_address', str(mac_address))
        firmware_version = fields.get("version")
        if firmware_version:
            self.set_firmware_version(str(firmware_version))
        hardware = fields.get("hardwarediscovered")
        if not hardware:
            hardware = fields.get("hardwareversion")
        if hardware:
            self.set_instrument_info('hardware', str(hardware))

        self.instrument_report()

    async def _poll_report(self) -> None:
        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(self._url) as resp:
                if resp.status != 200:
                    data = (await resp.read()).decode('utf-8')
                    raise CommunicationsError(f"invalid response status: {resp.reason} - {data}")
                try:
                    fields = await resp.json()
                    if not isinstance(fields, dict):
                        raise CommunicationsError
                except JSONDecodeError as e:
                    raise CommunicationsError("invalid response") from e
                self._process_report(fields)

    async def initialize_communications(self) -> bool:
        await self._poll_report()
        self._sleep_time = 0
        return True

    async def step_communications(self) -> bool:
        if self._sleep_time > 0.0:
            await asyncio.sleep(self._sleep_time)
            self._sleep_time = 0.0
        begin_read = time.monotonic()
        await self._poll_report()
        end_read = time.monotonic()
        self._sleep_time = self._report_interval - (end_read - begin_read)
        return True
