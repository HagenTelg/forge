import typing
import asyncio
import logging


_LOGGER = logging.getLogger(__name__)


async def complete() -> typing.Dict[str, typing.Any]:
    result: typing.Dict[str, typing.Any] = dict()
    _tasks: typing.List[asyncio.Task] = list()

    from .time import add_time
    _tasks.append(asyncio.ensure_future(add_time(result)))

    from .system import add_system_info, add_battery, add_temperature_sensors, add_lsb_info, add_uname
    _tasks.append(asyncio.ensure_future(add_system_info(result)))
    _tasks.append(asyncio.ensure_future(add_battery(result)))
    _tasks.append(asyncio.ensure_future(add_temperature_sensors(result)))
    _tasks.append(asyncio.ensure_future(add_lsb_info(result)))
    _tasks.append(asyncio.ensure_future(add_uname(result)))

    from .login import add_login_user
    _tasks.append(asyncio.ensure_future(add_login_user(result)))

    from .network import add_external_address, add_local_addresses, add_network_rate, add_network_configuration
    _tasks.append(asyncio.ensure_future(add_external_address(result)))
    _tasks.append(asyncio.ensure_future(add_local_addresses(result)))
    _tasks.append(asyncio.ensure_future(add_network_rate(result)))
    _tasks.append(asyncio.ensure_future(add_network_configuration(result)))

    from .memory import add_memory_utilization
    _tasks.append(asyncio.ensure_future(add_memory_utilization(result)))

    from .disk import add_disk_space, add_disk_rate
    _tasks.append(asyncio.ensure_future(add_disk_space(result)))
    _tasks.append(asyncio.ensure_future(add_disk_rate(result)))

    from .cpu import add_cpu_utilization
    _tasks.append(asyncio.ensure_future(add_cpu_utilization(result)))

    from .logs import add_kernel_log, add_system_log, add_acquisition_log
    _tasks.append(asyncio.ensure_future(add_kernel_log(result)))
    # _tasks.append(asyncio.ensure_future(add_system_log(result)))
    _tasks.append(asyncio.ensure_future(add_acquisition_log(result)))

    from .ntp import add_ntp_info
    _tasks.append(asyncio.ensure_future(add_ntp_info(result)))

    from .serial import add_serial_ports
    _tasks.append(asyncio.ensure_future(add_serial_ports(result)))

    if len(_tasks) > 0:
        try:
            await asyncio.wait(_tasks)
        except:
            _LOGGER.debug("Error running telemetry task", exc_info=True)
    return result
