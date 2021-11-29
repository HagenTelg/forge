import typing
import asyncio
import psutil


_need_cpu_percent_wait = True


async def add_cpu_utilization(telemetry: typing.Dict[str, typing.Any]) -> None:
    telemetry['load_average'] = psutil.getloadavg()[1]

    global _need_cpu_percent_wait
    utilization = psutil.cpu_percent(percpu=True)
    if _need_cpu_percent_wait:
        await asyncio.sleep(0.5)
        utilization = psutil.cpu_percent(percpu=True)
        _need_cpu_percent_wait = False
    if len(utilization) == 0:
        return

    telemetry['cpu_core_utilization'] = max(utilization)
    telemetry['cpu_total_utilization'] = sum(utilization) / len(utilization)
