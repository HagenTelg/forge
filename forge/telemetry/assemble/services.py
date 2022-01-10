import typing
import asyncio


async def add_failed_services(telemetry: typing.Dict[str, typing.Any]) -> None:
    try:
        import dbus
    except ImportError:
        return

    try:
        bus = dbus.SystemBus()
        proxy = bus.get_object('org.freedesktop.systemd1', '/org/freedesktop/systemd1')
        failed_services = proxy.ListUnitsFiltered(['failed'], dbus_interface='org.freedesktop.systemd1.Manager')
    except:
        return

    telemetry['failed_services'] = []
    for service in failed_services:
        # snapd update process breaks systemd mount status by deleting the unit files while still lazy mounted
        if service[2] == 'not-found' and service[0].startswith('var-lib-snapd-snap-') and service[0].endswith('.mount'):
            continue
        telemetry['failed_services'].append({
            'name': service[0],
            'description': service[1],
            'sub_state': service[4],
        })
