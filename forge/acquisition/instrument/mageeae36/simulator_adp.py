import typing
import asyncio
import time
from math import exp
from forge.acquisition.instrument.streaming import StreamingSimulator
from forge.units import flow_lpm_to_ccm


class Simulator(StreamingSimulator):
    WAVELENGTHS = [370, 470, 520, 590, 630, 880, 950]
    REPORTED_WAVELENGTHS = [340, 370, 400, 470, 520, 590, 630, 880, 950]

    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        super().__init__(reader, writer)

        self._unpolled_task: typing.Optional[asyncio.Task] = None

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

        self.timebase = 1

        def ts_micros():
            return str(round(time.time() * 1E6))

        def ts_str():
            ts = time.gmtime()
            return f"{ts.tm_year:04}-{ts.tm_mon:02}-{ts.tm_mday:02} {ts.tm_hour:02}:{ts.tm_min:02}:{ts.tm_sec:02}"
        
        def wl_lookup(source, index, scale=1.0):
            def r():
                return str(source[index] * scale)
            return r

        def wrap_constant(value):
            value = str(value)
            def r():
                return value
            return r

        self._data_columns: typing.List[typing.Tuple[str, typing.Callable[[], typing.Any]]] = []
        self._data_columns.append(("ID", lambda: "353721"))
        self._data_columns.append(("TimestampUTC", ts_micros))
        self._data_columns.append(("SetupTimestamp", ts_micros))
        self._data_columns.append(("SetupID", lambda : "1"))
        for i in range(7):
            self._data_columns.append((f"G{i}_Status", lambda : "0"))
        self._data_columns.append(("ControllerStatus", lambda : "0"))
        self._data_columns.append(("DetectorStatus", lambda : "0"))
        self._data_columns.append(("LedStatus", lambda : "0"))
        self._data_columns.append(("ValveStatus", lambda : "0"))
        self._data_columns.append(("PumpStatus", lambda : "0"))
        for wl in self.REPORTED_WAVELENGTHS:
            try:
                i = self.WAVELENGTHS.index(wl)
            except ValueError:
                i = 0
            self._data_columns.append((f"Ref{wl}", wl_lookup(self.data_If, i)))
            self._data_columns.append((f"Sens{wl}_1", wl_lookup(self.data_Ip, i)))
            self._data_columns.append((f"Sens{wl}_2", wl_lookup(self.data_Ips, i)))
        for wl in self.REPORTED_WAVELENGTHS:
            try:
                i = self.WAVELENGTHS.index(wl)
            except ValueError:
                i = 0
            self._data_columns.append((f"Atn{wl}_1", wl_lookup(self.data_ATNa, i)))
            self._data_columns.append((f"Atn{wl}_2", wl_lookup(self.data_ATNb, i)))
        for wl in self.REPORTED_WAVELENGTHS:
            try:
                i = self.WAVELENGTHS.index(wl)
            except ValueError:
                i = 0
            self._data_columns.append((f"BC{wl}_1", wl_lookup(self.data_Xa, i, 1000.0)))
            self._data_columns.append((f"BC{wl}_2", wl_lookup(self.data_Xb, i, 1000.0)))
            self._data_columns.append((f"BC{wl}", wl_lookup(self.data_X, i, 1000.0)))
        for wl in self.REPORTED_WAVELENGTHS:
            try:
                i = self.WAVELENGTHS.index(wl)
            except ValueError:
                i = 0
            self._data_columns.append((f"K{wl}", wl_lookup(self.data_correction_factor, i)))
        self._data_columns.append(("BB", lambda : str(self.data_PCT)))
        for wl in self.REPORTED_WAVELENGTHS:
            try:
                i = self.WAVELENGTHS.index(wl)
            except ValueError:
                i = 0
            e = 6833.0 / wl
            self._data_columns.append((f"Babs{wl}", wl_lookup(self.data_X, i, e)))
        for wl in self.REPORTED_WAVELENGTHS:
            self._data_columns.append((f"BabsBrC{wl}", lambda : "0"))
        self._data_columns.append(("BrC", lambda: str(self.data_PCT)))
        self._data_columns.append(("Pressure", lambda: str(1013.25 * 100)))
        self._data_columns.append(("Temp", lambda: str(0.0)))
        self._data_columns.append(("Flow1", lambda: str(flow_lpm_to_ccm(self.data_Q1))))
        self._data_columns.append(("Flow2", lambda: str(flow_lpm_to_ccm(self.data_Q2))))
        self._data_columns.append(("FlowC", lambda: str(flow_lpm_to_ccm(self.data_Q1 + self.data_Q2))))
        self._data_columns.append(("PumpDriver", lambda: "80"))
        self._data_columns.append(("PumpSpeed", lambda: "81"))
        self._data_columns.append(("ControllerTemp", lambda: str(self.data_Tcontroller)))
        self._data_columns.append(("InletHumidity", lambda: str(self.data_Uinlet)))
        self._data_columns.append(("InletTemp", lambda: str(self.data_Tinlet)))
        self._data_columns.append(("TapeHumidity", lambda: str(self.data_Utape)))
        self._data_columns.append(("TapeTemp", lambda: str(self.data_Ttape)))
        self._data_columns.append(("LedTemp", lambda: str(self.data_Tled)))
        self._data_columns.append(("LedSourceTemp", lambda: str(self.data_Tsource)))
        self._data_columns.append(("TapeAdvanceCount", lambda: str(self.data_Fn)))
        self._data_columns.append(("TapeAdvanceLeft", lambda: str(500)))
        self._data_columns.append(("CPU", lambda: "75"))

        self._setup_columns: typing.List[typing.Tuple[str, typing.Callable[[], typing.Any]]] = []
        self._setup_columns.append(("ID", lambda: "353722"))
        self._setup_columns.append(("TimestampUTC", ts_micros))
        self._setup_columns.append(("FirmwareVer", lambda : "1.0.0"))
        self._setup_columns.append(("SoftwareVer", lambda : "1.0.1"))
        self._setup_columns.append(("NetworkEnabled", lambda : "1"))
        self._setup_columns.append(("Timebase", lambda : str(self.timebase)))
        for wl in self.REPORTED_WAVELENGTHS:
            try:
                i = self.WAVELENGTHS.index(wl)
            except ValueError:
                i = 0
            e = 6833.0 / wl
            self._setup_columns.append((f"MAC{wl}", wrap_constant(e)))
        self._setup_columns.append(("C", lambda: "1.57"))
        self._setup_columns.append(("Area", lambda: "50.0"))
        self._setup_columns.append(("Zeta", lambda: "1.0"))
        self._setup_columns.append(("ZetaCompensation", lambda: "0"))
        self._setup_columns.append(("Aff", lambda: "0.98"))
        self._setup_columns.append(("Abb", lambda: "0.97"))
        self._setup_columns.append(("Abc", lambda: "0.96"))
        self._setup_columns.append(("Pressure", lambda: str(1013.25 * 100)))
        self._setup_columns.append(("Temp", lambda: str(0.0)))
        self._setup_columns.append(("ATNf1", lambda: "0"))
        self._setup_columns.append(("ATNf2", lambda: "5"))
        self._setup_columns.append(("Kmax", lambda: "0.0015"))
        self._setup_columns.append(("Kmin", lambda: "-0.0005"))
        self._setup_columns.append(("Flow", lambda: "1500"))
        self._setup_columns.append(("FlowRepStd", lambda: "3"))
        self._setup_columns.append(("FlowCa0", lambda: "0.1"))
        self._setup_columns.append(("FlowCa1", lambda: "0.2"))
        self._setup_columns.append(("FlowCa2", lambda: "0.3"))
        self._setup_columns.append(("Flow1a0", lambda: "0.4"))
        self._setup_columns.append(("Flow1a1", lambda: "0.5"))
        self._setup_columns.append(("Flow1a2", lambda: "0.6"))
        self._setup_columns.append(("TAtype", lambda: "1"))
        self._setup_columns.append(("TAatnMax", lambda: "70"))
        self._setup_columns.append(("TAinterval", lambda: "24"))
        self._setup_columns.append(("TAtime", lambda: "12:00:00"))
        self._setup_columns.append(("TapeRighta0", lambda: "5.0"))
        self._setup_columns.append(("TapeRighta1", lambda: "5.1"))
        self._setup_columns.append(("TapeRighta2", lambda: "5.2"))
        self._setup_columns.append(("TapeRighta3", lambda: "5.3"))
        self._setup_columns.append(("TapeLefta0", lambda: "5.4"))
        self._setup_columns.append(("TapeLefta1", lambda: "5.5"))
        self._setup_columns.append(("TapeLefta2", lambda: "5.6"))
        self._setup_columns.append(("TapeLefta3", lambda: "5.7"))
        self._setup_columns.append(("WarmUpInterval", lambda: "12"))
        self._setup_columns.append(("AutoTestEnabled", lambda: "0"))
        self._setup_columns.append(("AutoTestType", lambda: "0"))
        self._setup_columns.append(("AutoTestDay", lambda: "4"))
        self._setup_columns.append(("AutoTestTime", lambda: "12:00:00"))
        self._setup_columns.append(("MeasureTimeStamp", lambda: "0"))
        self._setup_columns.append(("HomeInfo", lambda: "0"))
        self._setup_columns.append(("Display", lambda: "0"))
        self._setup_columns.append(("DST", lambda: "0"))
        self._setup_columns.append(("TimeZone", lambda: "UTC"))
        self._setup_columns.append(("TapeAdvanceAdjust", lambda: "0"))
        self._setup_columns.append(("ExternalID", lambda: "123"))
        self._setup_columns.append(("BHparamID", lambda: "123"))
        self._setup_columns.append(("TimeSync", lambda: "1"))
        self._setup_columns.append(("DHCP", lambda: "1"))
        self._setup_columns.append(("InstrumentIP", lambda: "127.0.0.1"))
        self._setup_columns.append(("SubnetMask", lambda: "255.0.0.0"))
        self._setup_columns.append(("Gateway", lambda: "127.0.0.127"))
        self._setup_columns.append(("dRH_Inlet", lambda: "70"))
        self._setup_columns.append(("dRH_Tape", lambda: "71"))
        self._setup_columns.append(("RH_Warning", lambda: "95"))
        self._setup_columns.append(("Average", lambda: "1"))
        self._setup_columns.append(("AeroBaudRate", lambda: "115200"))
        self._setup_columns.append(("AeroDelimiter", lambda: "2"))
        self._setup_columns.append(("AdpEnabled", lambda: "1"))
        self._setup_columns.append(("RasEnabled", lambda: "1"))
        self._setup_columns.append(("UidepEnabled", lambda: "1"))
        self._setup_columns.append(("Language", lambda: "en"))
        self._setup_columns.append(("IndexBClimit0", lambda: "5000"))
        self._setup_columns.append(("IndexBClimit1", lambda: "6000"))
        self._setup_columns.append(("IndexBClimit2", lambda: "7000"))
        self._setup_columns.append(("IndexBClimit3", lambda: "8000"))
        self._setup_columns.append(("HomeChartPeriod", lambda: "0"))

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

    def _output_fetch(self, table: typing.List[typing.Tuple[str, typing.Callable[[], typing.Any]]]) -> None:
        fields: typing.List[str] = list()
        for _, value in table:
            fields.append(value())
        self.writer.write((",".join(fields)).encode('utf-8'))
        self.writer.write(b"\r")

    def _output_export(self, table: typing.List[typing.Tuple[str, typing.Callable[[], typing.Any]]]):
        fields: typing.List[str] = list()
        for column, _ in table:
            fields.append(column)
        self.writer.write((",".join(fields)).encode('utf-8'))
        self.writer.write(b"\r")
        self._output_fetch(table)

    async def _unpolled(self) -> typing.NoReturn:
        while True:
            await asyncio.sleep(self.timebase)
            self._output_fetch(self._data_columns)

    async def _stop_unpolled(self) -> None:
        t = self._unpolled_task
        self._unpolled_task = None
        if not t:
            return
        try:
            t.cancel()
        except:
            pass
        try:
            await t
        except:
            pass

    async def _start_unpolled(self) -> None:
        await self._stop_unpolled()
        self._unpolled_task = asyncio.ensure_future(self._unpolled())

    async def run(self) -> typing.NoReturn:
        try:
            await self._start_unpolled()

            while True:
                line = await self.reader.readuntil(b'\r')
                line = line.strip()

                try:
                    if line == b'$AERO:END':
                        await self._stop_unpolled()
                    elif line.startswith(b'$AERO:MAXID'):
                        self.writer.write(b"1\r")
                    elif line.startswith(b'$AERO:EXPORT'):
                        fields = line[12:].strip().split()
                        table = fields[0].lower()
                        if table == b'setup':
                            self._output_export(self._setup_columns)
                        else:
                            raise ValueError
                    elif line.startswith(b'$AERO:FETCH'):
                        fields = line[11:].strip().split()
                        table = fields[0].lower()
                        if table == b'data':
                            self._output_fetch(self._data_columns)
                            if b'C' in fields:
                                await self._start_unpolled()
                        elif table == b'setup':
                            self._output_fetch(self._setup_columns)
                        else:
                            raise ValueError
                    else:
                        raise ValueError
                except (ValueError, IndexError):
                    self.writer.write(b'ERROR\r')
                await self.writer.drain()
        finally:
            await self._stop_unpolled()


if __name__ == '__main__':
    from forge.acquisition.serial.simulator import parse_arguments, run
    run(parse_arguments(), Simulator)
