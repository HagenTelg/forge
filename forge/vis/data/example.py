import typing
from .stream import DataStream, RecordStream
from aiofile import AIOFile, LineReader
from forge.vis.util import package_data


class ExampleTimeSeries(RecordStream):
    def __init__(self, start_epoch_ms: int, send: typing.Callable[[typing.Dict], typing.Awaitable[None]]):
        super().__init__(send, ['BsG', 'BaG', 'Tsample', 'Psample', 'Tambient'])
        self.filename = package_data('static', 'example', 'timeseries.csv')
        self.start_epoch_ms = start_epoch_ms

    async def run(self) -> None:
        first_time = None
        async with AIOFile(self.filename, mode='r') as csv:
            lines = LineReader(csv)
            header = await lines.readline()
            async for line in lines:
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

                await self.send_record(epoch_ms - first_time + self.start_epoch_ms, number_fields)
        await self.flush()


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
                    {'type': 'variable', 'variable': 'BsG_S11'},
                ],
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
                {'type': 'variable', 'variable': 'BsG_S11'},
                {'type': 'variable', 'variable': 'BaG_A11'},
            ],
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
            '_id': 101,
        })


class ExampleEditAvailable(DataStream):
    async def run(self) -> None:
        for name in ["BsG_S11", "BaG_A11", "N_N71", "Q_A11"]:
            await self.send({
                'type': 'variable',
                'variable': name,
            })
        for index in range(20):
            await self.send({
                'type': 'variable',
                'variable': f'N{index+1}_N11',
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
            'type': "Acquisition",
            'author': "S11",
            'message': "Example Communications Loss",
            'acquisition': True,
        })

