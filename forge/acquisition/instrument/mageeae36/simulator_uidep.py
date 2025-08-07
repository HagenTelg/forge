import typing
import asyncio
import time
from math import exp
from starlette.routing import Route
from starlette.requests import Request
from starlette.responses import JSONResponse
from forge.acquisition.instrument.http import HttpSimulator
from forge.units import flow_lpm_to_ccm


class Simulator(HttpSimulator):
    WAVELENGTHS = [370, 470, 520, 590, 630, 880, 950]

    def __init__(self):
        super().__init__()

        self.data_PCT = 15.0
        self.data_Q1 = 1.0
        self.data_Q2 = 0.5
        self.data_Tinlet = 18.0
        self.data_Uinlet = 35.0
        self.data_Tcontroller = 21.0
        self.data_Tled = 23.0
        self.data_Tsource = 24.0
        self.data_Ttape = 25.0
        self.data_Utape = 34.0

        self.data_If = [float(i + 100) for i in range(len(self.WAVELENGTHS))]
        self.data_Ip = [float(i + 200) for i in range(len(self.WAVELENGTHS))]
        self.data_Ips = [float(i + 300) for i in range(len(self.WAVELENGTHS))]
        self.data_Xa = [float(i + 30) for i in range(len(self.WAVELENGTHS))]
        self.data_Xb = [float(i + 40) for i in range(len(self.WAVELENGTHS))]
        self.data_correction_factor = [float(0.15 + i / 100) for i in range(len(self.WAVELENGTHS))]
        self.data_ATNa = [float(i + 1.0) for i in range(len(self.WAVELENGTHS))]
        self.data_ATNb = [float(i + 1.5) for i in range(len(self.WAVELENGTHS))]
        self.data_X = [float((self.data_Xa[i] / (1.0 - self.data_correction_factor[i] * self.data_ATNa[i])) / 1.57) for
                       i in range(len(self.WAVELENGTHS))]

        self.data_Fn = 1

    def record(self, _request: Request) -> JSONResponse:
        result: typing.Dict[str, typing.Any] = dict()

        now = time.time()
        ts = time.gmtime(now)

        ts_micros = int(round(now * 1E6))
        ts_str = f"{ts.tm_year:04}-{ts.tm_mon:02}-{ts.tm_mday:02} {ts.tm_hour:02}:{ts.tm_min:02}:{ts.tm_sec:02}"

        result["Device"] = "AE36"
        result["SN"] = "AE36-00-00107"

        components = list()
        result["Components"] = components

        components.append({"Component": "ID", "Value": 353721})
        components.append({"Component": "TimestampUTC", "Value": ts_micros})
        components.append({"Component": "StartTime", "Value": ts_str})
        components.append({"Component": "EndTime", "Value": ts_str})
        components.append({"Component": "StartTimeUTC", "Value": ts_str})
        components.append({"Component": "EndTimeUTC", "Value": ts_str})
        components.append({"Component": "SetupID", "Value": 0})
        components.append({"Component": "SetupTimestamp", "Value": ts_str})
        for i in range(7):
            components.append({"Component": f"G{i}_Status", "Value": 0})
        components.append({"Component": "ControllerStatus", "Value": 0})
        components.append({"Component": "DetectorStatus", "Value": 0})
        components.append({"Component": "LedStatus", "Value": 0})
        components.append({"Component": "ValveStatus", "Value": 0})
        components.append({"Component": "PumpStatus", "Value": 0})
        for i in range(len(self.WAVELENGTHS)):
            wl = self.WAVELENGTHS[i]
            components.append({"Component": f"Ref{wl}", "Value": self.data_If[i]})
            components.append({"Component": f"Sens{wl}_1", "Value": self.data_Ip[i]})
            components.append({"Component": f"Sens{wl}_2", "Value": self.data_Ips[i]})
        for i in range(len(self.WAVELENGTHS)):
            wl = self.WAVELENGTHS[i]
            components.append({"Component": f"Atn{wl}_1", "Value": self.data_ATNa[i]})
            components.append({"Component": f"Atn{wl}_2", "Value": self.data_ATNb[i]})
        for i in range(len(self.WAVELENGTHS)):
            wl = self.WAVELENGTHS[i]
            components.append({"Component": f"BC{wl}_1", "Value": self.data_Xa[i] * 1000.0})
            components.append({"Component": f"BC{wl}_2", "Value": self.data_Xb[i] * 1000.0})
            components.append({"Component": f"BC{wl}", "Value": self.data_X[i] * 1000.0})
        for i in range(len(self.WAVELENGTHS)):
            wl = self.WAVELENGTHS[i]
            components.append({"Component": f"K{wl}", "Value": self.data_correction_factor[i]})
        components.append({"Component": "BB", "Value": self.data_PCT})
        for i in range(len(self.WAVELENGTHS)):
            wl = self.WAVELENGTHS[i]
            e = 6833.0 / wl
            components.append({"Component": f"Babs{wl}", "Value": self.data_X[i] * e})
        components.append({"Component": "Pressure", "Value": 1013.25 * 100})
        components.append({"Component": "Temp", "Value": 0.0})
        components.append({"Component": "Flow1", "Value": flow_lpm_to_ccm(self.data_Q1)})
        components.append({"Component": "Flow2", "Value": flow_lpm_to_ccm(self.data_Q2)})
        components.append({"Component": "FlowC", "Value": flow_lpm_to_ccm(self.data_Q1 + self.data_Q2)})
        components.append({"Component": "PumpDriver", "Value": 80})
        components.append({"Component": "PumpSpeed", "Value": 81})
        components.append({"Component": "ControllerTemp", "Value": self.data_Tcontroller})
        components.append({"Component": "InletHumidity", "Value": self.data_Uinlet})
        components.append({"Component": "InletTemp", "Value": self.data_Tinlet})
        components.append({"Component": "TapeHumidity", "Value": self.data_Utape})
        components.append({"Component": "TapeTemp", "Value": self.data_Ttape})
        components.append({"Component": "LedTemp", "Value": self.data_Tled})
        components.append({"Component": "LedSourceTemp", "Value": self.data_Tsource})
        components.append({"Component": "TapeAdvanceCount", "Value": self.data_Fn})
        components.append({"Component": "TapeAdvanceLeft", "Value": 500})
        components.append({"Component": "CPU", "Value": 75})

        return JSONResponse(result)

    @property
    def data_Ir(self) -> typing.List[float]:
        return [exp(v / -100.0) for v in self.data_ATNa]

    @property
    def data_Irs(self) -> typing.List[float]:
        return [exp(v / -100.0) for v in self.data_ATNb]

    @property
    def data_X1(self) -> float:
        return self.data_X[0]

    @property
    def data_Xa1(self) -> float:
        return self.data_Xa[0]

    @property
    def data_Xb1(self) -> float:
        return self.data_Xb[0]

    @property
    def data_k1(self) -> float:
        return self.data_correction_factor[0]

    @property
    def data_If1(self) -> float:
        return self.data_If[0]

    @property
    def data_Ip1(self) -> float:
        return self.data_Ip[0]

    @property
    def data_Ips1(self) -> float:
        return self.data_Ips[0]

    @property
    def routes(self) -> typing.List[Route]:
        return [
            Route('/values/simple', endpoint=self.record),
            Route('/values/complex', endpoint=self.record),
        ]


if __name__ == '__main__':
    Simulator.run_server()
