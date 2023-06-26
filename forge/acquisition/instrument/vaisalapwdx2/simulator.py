import typing
import asyncio
from forge.units import distance_km_to_m
from forge.acquisition.instrument.streaming import StreamingSimulator


class Simulator(StreamingSimulator):
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        super().__init__(reader, writer)

        self.data_WZ = 10.0
        self.data_WZ10Min = 10.5
        self.data_WI = 0.5

        self.data_WX = 20
        self.data_WX15Min = 24
        self.data_WX1Hour = 26
        self.data_nws_code = "--R"

        self.data_Tambient = 21.0
        self.data_Tinternal = 21.5
        self.data_Tdrd = 19.0

        self.data_Csignal = 30.0
        self.data_Coffset = 10.0
        self.data_Cdrift = 1.0
        self.data_Cdrd = 40.0
        self.data_I = 15.0

        self.data_BsTx = 2.0
        self.data_BsTxChange = 1.0
        self.data_BsRx = 4.0
        self.data_BsRxChange = 3.0

        self.data_Vsupply = 12.0
        self.data_Vpositive = 12.5
        self.data_Vnegative = -11.5
        self.data_Vled = 2.5
        self.data_Vambient = -1.0

        self.alarms = "00"

    def _output_message_2(self) -> None:
        self.writer.write((
            f"{self.alarms} "
            f"{distance_km_to_m(self.data_WZ):5.0f} "
            f"{distance_km_to_m(self.data_WZ10Min):5.0f} "
            f"{self.data_nws_code} "
            f"{self.data_WX:2d} "
            f"{self.data_WX15Min:2d} "
            f"{self.data_WX1Hour:2d} "
            f"{self.data_WI:.2f} "
            " 12.16   0"
        ).encode('ascii'))

    def _output_status(self) -> None:
        def labeled_field(label: bytes, value, total: int, decimals: int = 0):
            self.writer.write(label)
            self.writer.write(b" ")
            self.writer.write(("{:.%df}" % decimals).format(value).encode('ascii').rjust(total - len(label) - 1, b" "))

        self.writer.write(b"VAISALA PWD22 V 1.00 2003-12-15 SN:Y46101\r\n\r\n")

        labeled_field(b"SIGNAL", self.data_Csignal, 16, 2)
        self.writer.write(b" ")
        labeled_field(b"OFFSET", self.data_Coffset, 16, 2)
        self.writer.write(b" ")
        labeled_field(b"DRIFT", self.data_Cdrift, 16, 2)
        self.writer.write(b"\r\n")

        labeled_field(b"REC. BACKSCATTER", self.data_BsRx, 25, 1)
        self.writer.write(b"  ")
        labeled_field(b"CHANGE", self.data_BsRxChange, 13, 1)
        self.writer.write(b"\r\n")

        labeled_field(b"TR. BACKSCATTER", self.data_BsTx, 25, 1)
        self.writer.write(b"  ")
        labeled_field(b"CHANGE", self.data_BsTxChange, 13, 1)
        self.writer.write(b"\r\n")

        labeled_field(b"LEDI", self.data_Vled, 11, 1)
        self.writer.write(b"  ")
        labeled_field(b"AMBL", self.data_Vambient, 12, 1)
        self.writer.write(b"\r\n")

        labeled_field(b"VBB", self.data_Vsupply, 11, 1)
        self.writer.write(b"  ")
        labeled_field(b"P12", self.data_Vpositive, 12, 1)
        self.writer.write(b"  ")
        labeled_field(b"M12", self.data_Vnegative, 12, 1)
        self.writer.write(b"\r\n")

        labeled_field(b"TS", self.data_Tambient, 11, 1)
        self.writer.write(b"  ")
        labeled_field(b"TB", self.data_Tinternal, 12, 1)
        self.writer.write(b"\r\n")

        labeled_field(b"TDRD", self.data_Tdrd, 11, 1)
        self.writer.write(b"    25  ")
        labeled_field(b"DRD", self.data_Cdrd, 12, 0)
        self.writer.write(b"  843  ")
        labeled_field(b"DRY", 857.5, 12, 1)
        self.writer.write(b"\r\n")

        labeled_field(b"BL", self.data_I, 11, 0)
        self.writer.write(b"\r\n")

        self.writer.write(b"RELAYS  OFF OFF OFF\r\n\r\n")
        self.writer.write(b"HOOD HEATERS OFF\r\n")
        self.writer.write(b"HARDWARE :\r\n")
        self.writer.write(b" OK\r\n")

    async def line_open(self) -> None:
        while True:
            line = await self.reader.readuntil(b'\r')
            line = line.strip()
            if not line:
                continue

            try:
                if line == b"CLOSE":
                    return
                elif line.startswith(b"AMES "):
                    self.writer.write(b"UNPOLLED SET\r\n")
                elif line.startswith(b"TIME "):
                    self.writer.write(b"TIME SET\r\n")
                elif line.startswith(b"DATE "):
                    self.writer.write(b"TIME SET\r\n")
                elif line == b"STA":
                    self.writer.write(b"PWD STATUS\r\n")
                    self._output_status()
                elif line == b"PAR":
                    self.writer.write(
                        b"SYSTEM PARAMETERS\r\n"
                        b"VAISALA PWD22 v 1.00 2003-04-09 SN:X1234567 ID STRING:\r\n"
                        b"AUTOMATIC MESSAGE 0 INTERVAL 0\r\n"
                        b"BAUD RATE: 9600 N81\r\n"
                        b"ALARM LIMIT 1 0\r\n"
                        b"ALARM LIMIT 2 0\r\n"
                        b"ALARM LIMIT 3 0\r\n"
                        b"RELAY ON DELAY 10 OFF DELAY 11\r\n"
                        b"OFFSET REF 152.38\r\n"
                        b"CLEAN REFERENCES\r\n"
                        b"TRANSMITTER 5.0 RECEIVER 1200\r\n"
                        b"CONTAMINATION WARNING LIMITS\r\n"
                        b"TRANSMITTER 0.5 RECEIVER 300\r\n"
                        b"CONTAMINATION ALARM LIMITS\r\n"
                        b"TRANSMITTER 3.0 RECEIVER 600\r\n"
                        b"SIGN SIGNAL 1 1.000\r\n"
                        b"DAC MODE: EXT1\r\n"
                        b"MAX VIS 20000, 20.0 mA\r\n"
                        b"MIN VIS 180, 4.5 mA\r\n"
                        b"20 mA SCALE_1 184.6, SC_0 -2.8\r\n"
                        b"1 mA SCALE_1 184.8, SC_0 -1.4\r\n"
                    )
                elif line == b"WPAR":
                    self.writer.write(
                        b"WEATHER PARAMETERS\r\n\r\n"
                        b"PRECIPITATION LIMIT     40\r\n"
                        b"WEATHER UPDATE DELAY     6\r\n"
                        b"RAIN INTENSITY SCALE  1.00\r\n"
                        b"HEAVY RAIN LIMIT       8.0\r\n"
                        b"LIGHT RAIN LIMIT       2.0\r\n"
                        b"SNOW LIMIT             5.0\r\n"
                        b"HEAVY SNOW LIMIT       600\r\n"
                        b"LIGHT SNOW LIMIT      1200\r\n"
                        b"DRD SCALE              1.0\r\n"
                        b"DRD DRY OFFSET       809.5\r\n"
                        b"DRD WET SCALE       0.0017\r\n"
                    )
                else:
                    raise ValueError
            except (ValueError, IndexError):
                self.writer.write(b'ERROR\r')
            await self.writer.drain()

    async def run(self) -> typing.NoReturn:
        while True:
            line = await self.reader.readuntil(b'\r')
            line = line.strip()
            if not line:
                continue

            try:
                if line == b"OPEN":
                    self.writer.write(b"LINE OPENED FOR OPERATOR COMMANDS\r\n")
                    await self.line_open()
                    self.writer.write(b"LINE CLOSED\r\n")
                elif line.startswith(b"\x05PW  1 2"):
                    await asyncio.sleep(0.1)
                    self.writer.write(b"\x01PW  1\x02")
                    self._output_message_2()
                    self.writer.write(b"\x03\r\n")
                elif line.startswith(b"\x05PW  1 3"):
                    await asyncio.sleep(0.1)
                    self.writer.write(b"\x01PW  1\x02")
                    self._output_status()
                    self.writer.write(b"\x03\r\n")
                else:
                    raise ValueError
            except (ValueError, IndexError):
                self.writer.write(b'ERROR\r')
            await self.writer.drain()


if __name__ == '__main__':
    from forge.acquisition.serial.simulator import parse_arguments, run
    run(parse_arguments(), Simulator)
