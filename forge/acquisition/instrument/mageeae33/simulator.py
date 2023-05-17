import typing
import asyncio
import time
from forge.units import flow_lpm_to_ccm
from forge.acquisition.instrument.streaming import StreamingSimulator


class Simulator(StreamingSimulator):
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        super().__init__(reader, writer)

        self.data_Q1 = 1.0
        self.data_Q2 = 0.5
        self.data_Tcontroller = 21.0
        self.data_Tsupply = 22.0
        self.data_Tled = 23.0

        self.data_If = [float(i + 100) for i in range(7)]
        self.data_Ip = [float(i + 200) for i in range(7)]
        self.data_Ips = [float(i + 300) for i in range(7)]
        self.data_Xa = [float(i + 30) for i in range(7)]
        self.data_Xb = [float(i + 40) for i in range(7)]
        self.data_correction_factor = [float(0.15 + i/100) for i in range(7)]
        self.data_ATNa = [float(i + 1.0) for i in range(7)]
        self.data_X = [float((self.data_Xa[i] / (1.0 - self.data_correction_factor[i] * self.data_ATNa[i])) / 1.57) for i in range(7)]

        self.data_Fn = 1

        self.status_main = 0
        self.status_controller = 0
        self.status_detector = 10
        self.status_led = 10
        self.status_valve = 0

        self.external_sensor_types = [0, 0, 0]
        self.external_sensor_fields = []

        self.parameters_data = (
            b"19 AE33-S10-01257 4/26/2022 8:21:19 AM 540 1.7.0.0 172.16.0.2:8001 "
            b"1 0 1 18.47 14.54 13.14 11.58 10.35 7.77 7.19 1.39 0.785 0.01 1 2 "
            b"101325 0.00 10 30 0.015 -0.005 5000 3 585 -2071.90478515625 -2414.56079101563 "
            b"11.8877620697021 12.4278383255005 0.00019356407574378 -0.000190374892554246 "
            b"173.804489135742 0.0836383700370789 -1.10416010556946E-07 1 120 12 1/1/2003 "
            b"12:02:47 AM 1.08437502384186 -12.0875015258789 1.10903429985046 "
            b"-11.8504695892334 3 1 1 1 1/1/2014 12:00:00 AM 0 0 1 0 0 "
            b"Coordinated Universal Time 5 1 1 0 1 192.168.0.2 255.255.255.0 192.168.0.1\r"
        )

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

    def _emit_record(self) -> None:
        ts = time.gmtime()
        self.writer.write((
            f"{ts.tm_year:04}/{ts.tm_mon:02}/{ts.tm_mday:02} {ts.tm_hour:02}:{ts.tm_min:02}:{ts.tm_sec:02} 1 "
        ).encode('ascii'))
        for i in range(7):
            self.writer.write((
                f"{self.data_If[i]} "
                f"{self.data_Ip[i]} "
                f"{self.data_Ips[i]} "
            ).encode('ascii'))
        self.writer.write((
            f"{flow_lpm_to_ccm(self.data_Q1)} "
            f"{flow_lpm_to_ccm(self.data_Q2)} "
            f"{flow_lpm_to_ccm(self.data_Q1 + self.data_Q2)} "
            f"101325 "  # Flow standard pressure
            f"0 "  # Flow standard temperature
            f"0 "  # Biomass Burning %
            f"{self.data_Tcontroller} "
            f"{self.data_Tsupply} "
            f"{self.status_main} "
            f"{self.status_controller} "
            f"{self.status_detector} "
            f"{self.status_led} "
            f"{self.status_valve} "
            f"{self.data_Tled} "
        ).encode('ascii'))
        for i in range(7):
            self.writer.write((
                f"{self.data_Xa[i] * 1000.0} "
                f"{self.data_Xb[i] * 1000.0} "
                f"{self.data_X[i] * 1000.0} "
            ).encode('ascii'))
        for i in range(7):
            self.writer.write((
                f"{self.data_correction_factor[i]} "
            ).encode('ascii'))
        self.writer.write((
            f"{self.data_Fn} "
        ).encode('ascii'))
        self.writer.write((" ".join([
            str(s) for s in (self.external_sensor_types + self.external_sensor_fields)
        ]).encode('ascii')))
        self.writer.write(b'\r')

    async def run(self) -> typing.NoReturn:
        while True:
            line = await self.reader.readuntil(b'\r')
            line = line.strip()

            try:
                if line == b'$AE33:D1':
                    self._emit_record()
                elif line == b'$AE33:SG':
                    self.writer.write(self.parameters_data)
                elif line == b'$AE33:X0':
                    pass
                elif line == b'$AE33:X1':
                    self.data_Fn += 1
                else:
                    raise ValueError
            except (ValueError, IndexError):
                self.writer.write(b'ERROR\r')
            await self.writer.drain()


if __name__ == '__main__':
    from forge.acquisition.serial.simulator import parse_arguments, run
    run(parse_arguments(), Simulator)
