import typing
import asyncio
import time
from forge.units import flow_lpm_to_ccm, flow_lpm_to_ccs
from forge.acquisition.instrument.streaming import StreamingSimulator
from forge.acquisition.instrument.admagic250cpc.parameters import Parameters


class Simulator(StreamingSimulator):
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        super().__init__(reader, writer)

        self.unpolled_interval = 1.0
        self._unpolled_task: typing.Optional[asyncio.Task] = None
        self._unpolled_long: bool = False

        self.data_N = 1234.0
        self.data_Q = 0.2
        self.data_C = int(self.data_N * flow_lpm_to_ccs(self.data_Q))
        self.data_P = 980.0
        self.data_Vpulse = 2123.0
        self.data_Vpwr = 12.0
        self.data_Cwick = 100
        self.data_PDflow = 20

        self.data_Tinlet = 21.0
        self.data_Tconditioner = 25.0
        self.data_Tinitiator = 30.0
        self.data_Tmoderator = 23.0
        self.data_Toptics = 35.5
        self.data_Theatsink = 55.0
        self.data_Tcase = 27.0
        self.data_Tboard = 28.0
        self.data_Uinlet = 45.0
        self.data_TDinlet = 85.0
        self.data_TDgrowth = 85.0

        self.parameters = Parameters(
            lset=1000,
            doslope=171, doint=44, doff=216,
            dvlt=60,
            dthr=250,
            pht=50,
            dthr2=766,
            qcf=140,
            qtrg=300, qset=72.6,
            heff=0.75,
            hmax=92.0,
            wtrg=93,
            wdry=60,
            wwet=93,
            wgn=40,
            wmax=188,
            wmin=39,
            tcon=Parameters.Temperature(Parameters.Temperature.Mode.RELATIVE, -18.0),
            tini=Parameters.Temperature(Parameters.Temperature.Mode.RELATIVE, -17.0),
            tmod=Parameters.Temperature(Parameters.Temperature.Mode.RELATIVE, 0.0),
            topt=Parameters.Temperature(Parameters.Temperature.Mode.ABSOLUTE, 35.0),
        )
        self.parameters.lcur = 1031
        self.parameters.tcon.current = 14.6
        self.parameters.tcon.reference = 32.6
        self.parameters.tini.current = 49.5
        self.parameters.tini.reference = 32.5
        self.parameters.tmod.current = 24.6
        self.parameters.tmod.reference = 24.6

        self.flags = 0

    @property
    def data_PCTwick(self) -> float:
        return (self.data_Cwick - self.parameters.wmax) * (100 / (self.parameters.wmin - self.parameters.wmax))

    async def _unpolled(self) -> typing.NoReturn:
        while True:
            ts = time.gmtime()
            if self._unpolled_long:
                self.writer.write((
                    f"{ts.tm_year:04}/{ts.tm_mon:02}/{ts.tm_mday:02} {ts.tm_hour:02}:{ts.tm_min:02}:{ts.tm_sec:02},"
                    f"{self.data_N:7.0f},"
                    f"{self.data_TDinlet:4.1f},"
                    f"{self.data_Tinlet:4.1f},"
                    f"{self.data_Uinlet:4.1f},"
                    f"{self.data_Tconditioner:4.1f},"
                    f"{self.data_Tinitiator:4.1f},"
                    f"{self.data_Tmoderator:4.1f},"
                    f"{self.data_Toptics:4.1f},"
                    f"{self.data_Theatsink:4.1f},"
                    f"{self.data_Tcase:4.1f},"
                    f"{self.data_PCTwick:6.0f},"
                    "21.5,"
                    f"{self.data_TDgrowth:4.1f},"
                    f"{self.data_Cwick:4d},"
                    f"{self.data_Tboard:5.1f},"
                    f"{self.data_Vpwr:4.1f}V,"
                    f"{self.data_PDflow:5.1f},"
                    f"{self.data_P:4.0f},"
                    f"{flow_lpm_to_ccm(self.data_Q):3.0f},"
                    "1,10000,    0,"
                    f"{self.data_C:7.0f},"
                    f"{self.data_Vpulse:5.0f}.{self.parameters.pht},"
                    f"{self.flags:04X},"
                    " ...... ,123\r"
                ).encode('ascii'))
            else:
                self.writer.write((
                    f"{ts.tm_year:04}/{ts.tm_mon:02}/{ts.tm_mday:02} {ts.tm_hour:02}:{ts.tm_min:02}:{ts.tm_sec:02},"
                    f"{self.data_N:7.0f},"
                    f"{self.data_TDinlet:4.1f},"
                    f"{self.data_Tinlet:4.1f},"
                    f"{self.data_Uinlet:4.1f},"
                    f"{self.data_Tconditioner:4.1f},"
                    f"{self.data_Tinitiator:4.1f},"
                    f"{self.data_Tmoderator:4.1f},"
                    f"{self.data_Toptics:4.1f},"
                    f"{self.data_Theatsink:4.1f},"
                    f"{self.data_Tcase:4.1f},"
                    f"{self.data_PCTwick:6.0f},"
                    "21.5,"
                    f"{self.data_TDgrowth:4.1f},"
                    f"{self.data_P:4.0f},"
                    f"{flow_lpm_to_ccm(self.data_Q):3.0f},"
                    "1,10000,    0,"
                    f"{self.data_C:7.0f},"
                    f"{self.data_Vpulse:5.0f}.{self.parameters.pht},"
                    f"{self.flags:04X},"
                    " ...... ,123\r"
                ).encode('ascii'))

            await asyncio.sleep(self.unpolled_interval)

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

    def _output_parameter_temperature(self, name: str) -> None:
        temperature: Parameters.Temperature = getattr(self.parameters, name)
        if temperature.mode == Parameters.Temperature.Mode.RELATIVE:
            self.writer.write(f"{name},r,{temperature.setpoint:.1f}\r\n".encode('ascii'))
            self.writer.write(f"setTemp={temperature.current:.1f}, refTemp={temperature.reference:.1f}\r\n".encode('ascii'))
        elif temperature.mode == Parameters.Temperature.Mode.ABSOLUTE:
            self.writer.write(f"{name},a,{temperature.setpoint:.1f}\r\n".encode('ascii'))

    async def run(self) -> typing.NoReturn:
        try:
            await self._start_unpolled()
            while True:
                line = await self.reader.readuntil(b'\r')
                line = line.strip()

                # Echo
                self.writer.write(line)
                self.writer.write(b'\r\n')

                try:
                    if line.startswith(b'log,'):
                        if line[4:] == b"off":
                            await self._stop_unpolled()
                        elif line[4:] == b"on":
                            self._unpolled_long = False
                            await self._start_unpolled()
                        else:
                            interval = int(line[4:])
                            self._unpolled_long = True
                            self.unpolled_interval = interval
                        self.writer.write(b'OK\r\n')
                    elif line.startswith(b'logl,'):
                        if line[5:] == b"off":
                            await self._stop_unpolled()
                        elif line[5:] == b"on":
                            self._unpolled_long = True
                            await self._start_unpolled()
                        else:
                            interval = int(line[5:])
                            self._unpolled_long = True
                            self.unpolled_interval = interval
                        self.writer.write(b'OK\r\n')
                    elif line == b'ver' or line == b'rv':
                        self.writer.write(b' Serial Number: 123\r\n')
                        self.writer.write(b' FW Ver: 3.03a\r\n')
                        self.writer.write(b' 2022 Aug 31\r\n')
                    elif line.startswith(b'rtc,'):
                        self.writer.write(b'\r\nOK\r\n')
                    elif line == b'wadc':
                        self.writer.write(b'59\r\n')
                    elif line == b'svs':
                        self.writer.write(b'OK\r\n')
                    elif line == b'sus':
                        self.writer.write((
                            f"lset  {self.parameters.lset}uW at 1000mBar;  current={self.parameters.lcur}uW; lawp: 1\r\n"
                            f"doslope {self.parameters.doslope}mV/mW   doint {self.parameters.doint} 44mV (doff {self.parameters.doff} 216mV)\r\n"
                            f"dvlt  {self.parameters.dvlt}V\r\n"
                            f"dthr  {self.parameters.dthr}mV\r\n"
                            f"pht {self.parameters.pht}%, (dthr2 {self.parameters.dthr2}mV)\r\n"
                            f"qcf   {self.parameters.qcf}\r\n"
                            f"qtrg  {self.parameters.qtrg} cc/min   (qset  {self.parameters.qset:.1f}%)\r\n"
                            f"heff {self.parameters.heff:.2f}  hmax {self.parameters.hmax:.1f}\r\n"
                            f"wTrg {self.parameters.wtrg}\r\n"
                            f"wDry {self.parameters.wdry}\r\n"
                            f"wWet {self.parameters.wwet}\r\n"
                            f"wgn {self.parameters.wgn}\r\n"
                            f"wmax {self.parameters.wmax}\r\n"
                            f"wmin {self.parameters.wmin}\r\n"
                            f"(lcur {self.parameters.lcur}mA --reading only)\r\n"
                        ).encode('ascii'))
                    elif line == b'tcon':
                        self._output_parameter_temperature('tcon')
                    elif line == b'tini':
                        self._output_parameter_temperature('tini')
                    elif line == b'tmod':
                        self._output_parameter_temperature('tmod')
                    elif line == b'topt':
                        self._output_parameter_temperature('topt')
                    elif line == b'hdr':
                        if self._unpolled_long:
                            self.writer.write(b'year time, Concentration, DewPoint,Input T, Input RH\r\n')
                            self.writer.write(b'Cond T, Init T,Mod T, Opt T, HeatSink T,  Case T,\r\n')
                            self.writer.write(b'wickSensor, ModSet, Humidifer Exit DP,\r\n')
                            self.writer.write(b'Wadc, Board Temperature, Power Supply Voltage, Diff. Press,\r\n')
                            self.writer.write(b'Abs. Press., flow (cc/min)\r\n')
                            self.writer.write(b'log interval, corrected live time, measured dead time, raw counts, PulseHeight.Thres2\r\n')
                            self.writer.write(b'Status(hex code), Status(ascii), Serial Number\r\n')
                        else:
                            self.writer.write(b'year time, Concentration, DewPoint,Input T, Input RH \r\n')
                            self.writer.write(b'Cond T, Init T,Mod T, Opt T, HeatSink T,  Case T,\r\n')
                            self.writer.write(b'wickSensor, ModSet, Humidifer Exit DP,\r\n')
                            self.writer.write(b'Abs. Press., flow (cc/min)\r\n')
                            self.writer.write(b'log interval, corrected live time, measured dead time, raw counts, PulseHeight.Thres2\r\n')
                            self.writer.write(b'Status(hex code), Status(ascii), Serial Number\r\n')
                    elif line == b'tspid':
                        self.writer.write(b' (r, deltaT, refT, setTemp) or (a, setTemp), OnTime, Func, Ctrl, errRange,iGain, pGain, RefPW\r\n')
                        self.writer.write(b'r, -18.0, 33.0,  15.0, 113, cool,  on,   1.2,   	 0.02 1.20 128\r\n')
                        self.writer.write(b'r,  17.0, 33.0,  50.0,  96, heat,  on,   1.2,   	 0.02 1.20 128\r\n')
                        self.writer.write(b'r,   0.0, 23.8,  23.8,  83, cool,  on,   1.2,   	 0.02 1.20 128\r\n')
                        self.writer.write(b'a,  35.0,               43, heat,  on,   1.2,   	 0.02 1.20 128\r\n')
                    elif getattr(self.parameters, line.decode('ascii', 'ignore').lower().split(',', 1)[0], None):
                        fields = line.decode('ascii', 'ignore').split(',')
                        name = fields[0].lower()
                        if name in self.parameters.INTEGER_PARAMETERS:
                            value = int(fields[1])
                            setattr(self.parameters, name, value)
                        elif name in self.parameters.FLOAT_PARAMETERS:
                            value = float(fields[1])
                            setattr(self.parameters, name, value)
                        elif name in self.parameters.TEMPERATURE_PARAMETERS:
                            mode = fields[1].upper()
                            setpoint = float(fields[2])
                            target: Parameters.Temperature = getattr(self.parameters, name)
                            target.mode = Parameters.Temperature.Mode(mode)
                            target.setpoint = setpoint
                        else:
                            raise ValueError

                        self.writer.write(b'OK\r\n')
                    else:
                        raise ValueError
                except (ValueError, IndexError):
                    self.writer.write(b'ERROR in cmd\r')
                await self.writer.drain()
        finally:
            await self._stop_unpolled()


if __name__ == '__main__':
    from forge.acquisition.serial.simulator import parse_arguments, run
    run(parse_arguments(), Simulator)
