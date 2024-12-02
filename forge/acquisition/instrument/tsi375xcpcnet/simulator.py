import typing
import asyncio
import logging
from json import dumps as to_json, loads as from_json, JSONDecodeError
from forge.acquisition.instrument.streaming import StreamingSimulator

_LOGGER = logging.getLogger(__name__)


class Simulator(StreamingSimulator):
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        super().__init__(reader, writer)

        self.data_N = 1234.0
        self.data_C = 20000
        self.data_Q = 1.0
        self.data_Qinlet = 1.5
        self.data_P = 830.0
        self.data_PDinlet = 2.0
        self.data_PDnozzle = 28.0
        self.data_PDorifice = 300.0
        self.data_Alaser = 30.0
        self.data_PCT = 90.0
        self.data_liquid_level = 2345
        self.data_Tsaturator = 45.0
        self.data_Tcondenser = 15.0
        self.data_Toptics = 25.0
        self.data_Tcabinet = 21.0
        self.data_Twatertrap = 16.0

        self.model_number: str = "3756"
        self.device_status: typing.Set[str] = set()

        self._unpolled_subscription_id: int = 0
        self._unpolled_task: typing.Optional[asyncio.Task] = None

    async def _unpolled(self) -> typing.NoReturn:
        while True:
            self.writer.write(to_json({
                'command': 'PUBLISH',
                'subscriptionID': self._unpolled_subscription_id,
                'element': "Concentration",
                'status': "OK",
                'value': {
                    "time": "2018-05-25T15:22:07-0500",
                    "timeUs": "1527279727169174",
                    "deviceStatus": list(self.device_status),
                    "inletFlow": [self.data_Qinlet],
                    "aerosolFlow": [0.049],
                    "condenserTemp": [self.data_Tcondenser],
                    "saturatorTemp": [self.data_Tsaturator],
                    "cabinetTemp": [self.data_Tcabinet],
                    "inletPressure": [self.data_PDinlet / 10.0],
                    "orificePressure": [self.data_PDorifice / 10.0],
                    "sampleFlowRate": [self.data_Q],
                    "liquidLevel": [self.data_liquid_level],
                    "waterTrapTemp": [self.data_Twatertrap],
                    "enablesStatus": 450,
                    "loggingStatus":0,
                    "nanoEnhPresent":0,
                    "concentration": self.data_N,
                    "pulseHeight": self.data_PCT,
                    "opticsTemp": self.data_Toptics,
                    "ambientPressure": self.data_P / 10.0,
                    "nozzlePressure": self.data_PDnozzle / 10.0,
                    "laserCurrent": self.data_Alaser,
                    "counts1Second": self.data_C,
                    "availableMemory": 113385.1094,
                    "aerosolRH": 31.3999996,
                    "aerosolTemp": 25.399996,
                },
            }).encode('ascii'))
            await self.writer.drain()
            await asyncio.sleep(1.0)

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
            while True:
                transaction_id = None
                try:
                    async def read_json():
                        contents = bytearray()
                        while len(contents) < 65536:
                            d = await self.reader.read(1)
                            if not d:
                                break
                            contents += d
                            if d != b'}':
                                continue
                            try:
                                return from_json(contents)
                            except JSONDecodeError:
                                continue
                        raise ValueError

                    data = await read_json()
                    transaction_id = data['transactionID']
                    command = data['command']
                    element = data['element']

                    if command == 'READ' and element == 'deviceRecord':
                        self.writer.write(to_json({
                            'command': 'RESPONSE',
                            'status': 'OK',
                            'transactionID': transaction_id,
                            'element': element,
                            'value': {
                                'name': "tsi-3750173901",
                                'modelName': "CPC",
                                'manufactureDate': "2017-10-18",
                                'calibrationDate': "2017-10-18",
                                'totalMemory': "119884.03125",
                                'timezone': "America/Chicago",
                                'serialNumber': "3750173901",
                                'modelNumber': self.model_number,
                                'firmwareVersion': "1.0.4",
                            },
                        }).encode('ascii'))
                    elif command == 'SUBSCRIBE' and element == 'Concentration':
                        self._unpolled_subscription_id = data['subscriptionID']
                        self.writer.write(to_json({
                            'command': 'RESPONSE',
                            'status': 'OK',
                            'transactionID': transaction_id,
                            'subscriptionID': self._unpolled_subscription_id,
                            'element': element,
                        }).encode('ascii'))
                        _LOGGER.debug(f"Subscription started for ID {self._unpolled_subscription_id}")
                        await self._start_unpolled()
                    else:
                        raise ValueError

                except (ValueError, IndexError, KeyError):
                    self.writer.write(to_json({
                        'command': 'RESPONSE',
                        'status': 'ERROR',
                        'transactionID': transaction_id or 0,
                    }).encode('ascii'))

                await self.writer.drain()
        finally:
            await self._stop_unpolled()


if __name__ == '__main__':
    from forge.acquisition.serial.simulator import parse_arguments, run
    run(parse_arguments(), Simulator)
