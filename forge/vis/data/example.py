import typing
import asyncio
import random
from .stream import DataStream, RecordStream
from forge.vis.util import package_data


class ExampleTimeSeries(RecordStream):
    def __init__(self, start_epoch_ms: int, send: typing.Callable[[typing.Dict], typing.Awaitable[None]]):
        super().__init__(send, ['BsG', 'BaG', 'Tsample', 'Psample', 'Tambient'])
        self.filename = package_data('static', 'example', 'timeseries.csv')
        self.start_epoch_ms = start_epoch_ms
        self.last_epoch_ms = self.start_epoch_ms
        self.last_record: typing.Dict[str, float] = dict()

    async def run(self) -> None:
        first_time = None
        with open(self.filename, mode='r') as csv:
            header = csv.readline()
            for line in csv:
                text_fields = line.split(',')
                epoch_ms = int(text_fields[1]) * 1000
                if first_time is None:
                    first_time = epoch_ms
                number_fields = dict()
                for i in range(2, len(text_fields)):
                    field = text_fields[i]
                    if len(field) == 0:
                        field = None
                    else:
                        field = float(field)
                    number_fields[self.fields[i-2]] = field

                record_time = epoch_ms - first_time + self.start_epoch_ms
                await self.send_record(record_time, number_fields)
                self.last_epoch_ms = record_time
                self.last_record = number_fields
        await self.flush()


class ExampleRealtime(ExampleTimeSeries):
    async def run(self) -> None:
        await super().run()
        await self.flush()
        while True:
            await asyncio.sleep(10)
            self.last_epoch_ms += 10 * 1000
            record: typing.Dict[str, float] = dict()
            for field in self.fields:
                value = self.last_record.get(field)
                if value is None:
                    value = 0.0
                value += random.uniform(-5, 5)
                record[field] = value

            await self.send_record(self.last_epoch_ms, record)
            await self.flush()
            self.last_record = record


class ExampleEditDirectives(DataStream):
    def __init__(self, start_epoch_ms: int, send: typing.Callable[[typing.Dict], typing.Awaitable[None]]):
        super().__init__(send)
        self.start_epoch_ms = start_epoch_ms

    async def run(self) -> None:
        for i in range(5):
            await self.send({
                'start_epoch_ms': self.start_epoch_ms + 86400000 * i,
                'end_epoch_ms': self.start_epoch_ms + 86400000 * (i+1),
                'type': "Aerosol",
                'author': "DCH",
                'modified_epoch_ms': 1612137600000,
                'action': "invalidate",
                'comment': 'Example Edit ' + str(i+1),
                'history': [
                    {'time_epoch_ms': 1618963200000, 'user': "Derek Hageman", 'operation': "Created"},
                ],
                'selection': [
                    {'variable_id': 'Bs', 'instrument_id': 'S11'},
                ],
                'condition': {'type': 'none'},
                '_id': i,
            })
        await self.send({
            'start_epoch_ms': self.start_epoch_ms,
            'end_epoch_ms': self.start_epoch_ms + 86400000,
            'type': "Aerosol",
            'author': "EJA",
            'modified_epoch_ms': 1612137600000,
            'action': "invalidate",
            'comment': 'Deleted directive',
            'history': [
                {'time_epoch_ms': 1618963200000, 'user': "Derek Hageman", 'operation': "Created"},
                {'time_epoch_ms': 1619049600000, 'user': "Derek Hageman", 'operation': "Removed"},
            ],
            'selection': [
                {'variable_id': 'Bs', 'instrument_id': 'S11'},
                {'variable_id': 'Ba', 'instrument_id': 'A11'},
            ],
            'condition': {'type': 'none'},
            'deleted': True,
            '_id': 100,
        })
        await self.send({
            'start_epoch_ms': self.start_epoch_ms,
            'end_epoch_ms': self.start_epoch_ms + 86400000,
            'type': "Met",
            'author': "PJS",
            'modified_epoch_ms': 1612137600000,
            'action': "contaminate",
            'comment': 'External directive',
            'other_type': True,
            'condition': {'type': 'none'},
            '_id': 101,
        })


class ExampleEditAvailable(DataStream):
    async def run(self) -> None:
        await self.send({
            'type': 'variable_id',
            'variable_id': "Bs",
            'instrument_id': "S11",
            'wavelengths': [450, 550, 700],
        })
        await self.send({
            'type': 'variable_id',
            'variable_id': "Ba",
            'instrument_id': "A11",
            'wavelengths': [467, 528, 660],
        })
        await self.send({
            'type': 'variable_id',
            'variable_id': "N",
            'instrument_id': "N71",
        })
        await self.send({
            'type': 'variable_id',
            'variable_id': "Q",
            'instrument_id': "A11",
        })
        for index in range(20):
            await self.send({
                'type': 'variable_id',
                'variable_id': f'N{index+1}',
                'instrument_id': 'N11',
            })


class ExampleEventLog(DataStream):
    def __init__(self, start_epoch_ms: int, send: typing.Callable[[typing.Dict], typing.Awaitable[None]]):
        super().__init__(send)
        self.start_epoch_ms = start_epoch_ms

    async def run(self) -> None:
        for i in range(5):
            await self.send({
                'epoch_ms': self.start_epoch_ms + 3600000 * i,
                'type': "User",
                'author': "DCH",
                'message': 'Example Event ' + str(i+1),
            })
        await self.send({
            'epoch_ms': self.start_epoch_ms,
            'type': "Instrument",
            'author': "S11",
            'message': "Example Instrument Error",
            'acquisition': True,
            'error': True,
        })
        await self.send({
            'epoch_ms': self.start_epoch_ms + 60 * 1000,
            'type': "Communications",
            'author': "S11",
            'message': "Example Communications Established",
            'acquisition': True,
        })


class ExamplePassed(DataStream):
    def __init__(self, send: typing.Callable[[typing.Dict], typing.Awaitable[None]]):
        super().__init__(send)

    async def run(self) -> None:
        from forge.logicaltime import year_bounds_ms
        for year in range(2018, 2025):
            if year == 2021:
                continue
            start, end = year_bounds_ms(year)
            await self.send({
                'start_epoch_ms': start,
                'end_epoch_ms': end,
                'pass_time_epoch_ms': 1746057600000,
                'comment': "A comment" if year == 2020 else "",
            })
        await self.send({
            'start_epoch_ms': 1736035200000,
            'end_epoch_ms': 1736467200000,
            'pass_time_epoch_ms': 1736467200000,
            'comment': "After a gap",
        })


class ExampleInstruments(DataStream):
    def __init__(self, send: typing.Callable[[typing.Dict], typing.Awaitable[None]]):
        super().__init__(send)

    async def run(self) -> None:
        await self.send({
            'start_epoch_ms': 1420070400000,
            'end_epoch_ms': 1735689600000,
            'instrument_id': "S11",
            'instrument_code': "tsi3563nephelometer",
            'manufacturer': "TSI",
            'model': "3563",
            'serial_number': "1001",
        })
        await self.send({
            'start_epoch_ms': 1420070400000,
            'end_epoch_ms': 1577836800000,
            'instrument_id': "A11",
            'instrument_code': "clap",
            'manufacturer': "GML",
            'model': "CLAP",
            'serial_number': "1",
        })
        await self.send({
            'start_epoch_ms': 1577836800000,
            'end_epoch_ms': 1735689600000,
            'instrument_id': "A11",
            'instrument_code': "clap",
            'manufacturer': "GML",
            'model': "CLAP",
            'serial_number': "2",
        })
        await self.send({
            'start_epoch_ms': 1525132800000,
            'end_epoch_ms': 1588291200000,
            'instrument_id': "N61",
            'instrument_code': "tsi3760",
            'manufacturer': "TSI",
            'model': "3760",
            'serial_number': "123",
        })
        await self.send({
            'start_epoch_ms': 1651363200000,
            'end_epoch_ms': 1704067200000,
            'instrument_id': "N61",
            'instrument_code': "tsi3760",
            'manufacturer': "TSI",
            'model': "3760",
            'serial_number': "123",
        })
