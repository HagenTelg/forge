import typing
import asyncio
import psutil


async def add_memory_utilization(telemetry: typing.Dict[str, typing.Any]) -> None:
    telemetry['memory_utilization'] = {}

    vm = psutil.virtual_memory()
    telemetry['memory_utilization']['total_bytes'] = vm.total
    telemetry['memory_utilization']['usage_percent'] = ((vm.total - vm.available) / vm.total) * 100.0

    swap = psutil.swap_memory()
    telemetry['memory_utilization']['swap_bytes'] = swap.total
    telemetry['memory_utilization']['swap_percent'] = swap.percent
