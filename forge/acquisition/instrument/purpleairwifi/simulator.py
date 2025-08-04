import typing
import asyncio
import time
from starlette.routing import Route
from starlette.requests import Request
from starlette.responses import JSONResponse
from forge.acquisition.instrument.http import HttpSimulator


class Simulator(HttpSimulator):
    def __init__(self):
        super().__init__()
        self.data_Xa = 12.0
        self.data_Xb = 13.0
        self.data_IBsa = 200.0
        self.data_IBsb = 300.0
        self.data_T = 21.1
        self.data_U = 40.0
        self.data_P = 880.0

    def record(self, _request: Request) -> JSONResponse:
        result: typing.Dict[str, typing.Any] = dict()

        ts = time.gmtime()
        result["SensorId"] = "84:f3:eb:d8:e0:48"
        result["DateTime"] = f"{ts.tm_year:04}/{ts.tm_mon:02}/{ts.tm_mday:02}T{ts.tm_hour:02}:{ts.tm_min:02}:{ts.tm_sec:02}z"

        result["version"] = "6.03"
        result["hardwareversion"] = "3.0"
        result["hardwarediscovered"] = "3.0+OPENLOG+15802 MB+DS3231+BME280+BME680+PMSX003-A+PMSX003-B"

        result["current_temp_f"] = self.data_T * (9.0/5.0) + 32.0
        result["current_humidity"] = self.data_U
        result["pressure"] = self.data_P
        result["p_0_3_um"] = self.data_IBsa
        result["p_0_3_um_b"] = self.data_IBsb
        result["pm2_5_cf_1"] = self.data_Xa
        result["pm2_5_cf_1_b"] = self.data_Xb

        return JSONResponse(result)

    @property
    def routes(self) -> typing.List[Route]:
        return [
            Route('/json', endpoint=self.record),
        ]


if __name__ == '__main__':
    Simulator.run_server()
