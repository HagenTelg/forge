import typing
import asyncio
import time
from forge.acquisition.instrument.streaming import StreamingSimulator


class Simulator(StreamingSimulator):
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        super().__init__(reader, writer)

        self.unpolled_interval = 120.0

        self.data_Xa = 12.0
        self.data_Xb = 13.0
        self.data_IBsa = 200.0
        self.data_IBsb = 300.0
        self.data_T = 21.1
        self.data_U = 40.0
        self.data_P = 880.0

    async def run(self) -> typing.NoReturn:
        while True:
            ts = time.gmtime()
            self.writer.write((
                f"{ts.tm_year:04}/{ts.tm_mon:02}/{ts.tm_mday:02}T{ts.tm_hour:02}:{ts.tm_min:02}:{ts.tm_sec:02}z,"  # UTCDateTime
                "84:f3:eb:d8:e0:48,"  # mac_address
                "6.03,"  # firmware_ver
                "3.0+OPENLOG+15802 MB+DS3231+BME280+BME680+PMSX003-A+PMSX003-B,"  # hardware
                f"{self.data_T * (9.0/5.0) + 32.0:.2f},"  # current_temp_f
                f"{self.data_U:.2f},"  # current_humidity
                "34.12,"  # current_dewpoint_f
                f"{self.data_P:.2f},"  # pressure
                "0.02,"  # adc
                "36456,"  # mem
                "0,"  # rssi
                "820,"  # uptime
                "0.00,"  # pm1_0_cf_1
                f"{self.data_Xa:.2f},"  # pm2_5_cf_1
                "0.00,"  # pm10_0_cf_1
                "0.00,"  # pm1_0_atm
                "0.00,"  # pm2_5_atm
                "0.00,"  # pm10_0_atm
                "0.00,"  # pm2.5_aqi_cf_1
                "0.00,"  # pm2.5_aqi_atm
                f"{self.data_IBsa:.2f},"  # p_0_3_um
                "0.00,"  # p_0_5_um
                "0.00,"  # p_1_0_um
                "0.00,"  # p_2_5_um
                "0.00,"  # p_5_0_um
                "0.00,"  # p_10_0_um
                "0.00,"  # pm1_0_cf_1_b
                f"{self.data_Xb:.2f},"  # pm2_5_cf_1_b
                "0.00,"  # pm10_0_cf_1_b
                "0.00,"  # pm1_0_atm_b
                "0.00,"  # pm2_5_atm_b
                "0.00,"  # pm10_0_atm_b
                "0.00,"  # pm2.5_aqi_cf_1_b
                "0.00,"  # pm2.5_aqi_atm_b
                f"{self.data_IBsb:.2f},"  # p_0_3_um_b
                "0.00,"  # p_0_5_um_b
                "0.00,"  # p_1_0_um_b
                "0.00,"  # p_2_5_um_b
                "0.00,"  # p_5_0_um_b
                "0.00,"  # p_10_0_um_b
                "0.00\n"  # gas
            ).encode('ascii'))
            await asyncio.sleep(self.unpolled_interval)


if __name__ == '__main__':
    from forge.acquisition.serial.simulator import parse_arguments, run
    run(parse_arguments(), Simulator)
