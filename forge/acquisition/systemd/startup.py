import typing
import logging
import time
import dbus
import os
import sys
import argparse
from pathlib import Path
from forge.acquisition import CONFIGURATION, LayeredConfiguration
from forge.acquisition.cutsize import CutSize


_LOGGER = logging.getLogger(__name__)
_SYSTEMD: dbus.Interface = None
_ACQUISITION_SYSTEM_GROUP: str = None
_DATA_OUTPUT_USER: str = None
_COMPLETED_DATA_DIRECTORY: str = None
_STATE_LOCATION_DIRECTORY: str = None
_SERIAL_ACCESS_GROUPS: typing.List[str] = list()
_EXECUTABLE_LOCATION: str = None
_ENABLE_DEBUG: bool = False


def assemble_exec(*command: str, control: str = ""):
    return [(command[0], [control + command[0]] + list(command[1:]), False)]


def assemble_forge_exec(program: str, *args: str):
    if _EXECUTABLE_LOCATION:
        program = Path(_EXECUTABLE_LOCATION) / program
        program = str(program)
    return assemble_exec(program, *args)


def basic_service(description: str,
                  restart: bool = True,
                  user: typing.Optional[str] = None) -> typing.List[typing.Tuple[str, typing.Any]]:
    properties: typing.List[typing.Tuple[str, typing.Any]] = [
        ("Description", description),
        ("Group", _ACQUISITION_SYSTEM_GROUP),
        ("ProtectSystem", "strict"),
        ("ProtectHome", "true"),
        ("WorkingDirectory", "/"),
        ("WatchdogUSec", dbus.types.UInt64(30_000_000)),
        ("TimeoutStopUSec", dbus.types.UInt64(30_000_000)),
        ("Type", "notify"),
    ]
    if not user:
        properties.append(("DynamicUser", True))
    else:
        properties.append(("User", user))
    if restart:
        properties.append(("Restart", "on-failure"))
        properties.append(("RestartUSec", dbus.types.UInt64(5_000_000)))

    dynaconf_env = os.environ.get("ROOT_PATH_FOR_DYNACONF")
    if dynaconf_env:
        properties.append(("Environment", [f"ROOT_PATH_FOR_DYNACONF={dynaconf_env}"]))

    return properties


def set_dependencies(properties: typing.List[typing.Tuple[str, typing.Any]],
                     before: typing.List[str] = None,
                     after: typing.List[str] = None,
                     binds_to: typing.List[str] = None,
                     part_of: typing.List[str] = None,
                     conflicts: typing.List[str] = None) -> None:
    if before:
        properties.append(("Before", before))
    if after:
        properties.append(("After", after))
    if binds_to:
        properties.append(("BindsTo", binds_to))
    if part_of:
        properties.append(("PartOf", part_of))
    if conflicts:
        properties.append(("Conflicts", conflicts))


def release_transient_unit(name: str) -> None:
    # Have to stop it so we can re-use the name
    try:
        _SYSTEMD.StopUnit(name, "replace")
    except dbus.exceptions.DBusException:
        pass

    # Remove any status about it
    try:
        _SYSTEMD.ResetFailedUnit(name)
    except dbus.exceptions.DBusException:
        pass


def start_transient_unit(name: str, properties: typing.List[typing.Tuple[str, typing.Any]]) -> None:
    _SYSTEMD.StartTransientUnit(name, "replace", [
        (key, value) for key, value in properties
    ], [])


def start_unit(name: str) -> None:
    try:
        _SYSTEMD.StartUnit(name, "replace")
    except dbus.exceptions.DBusException:
        _LOGGER.warning(f"Unit {name} start failed", exc_info=True)


def stop_unit(name: str) -> None:
    try:
        _SYSTEMD.StopUnit(name, "replace")
    except dbus.exceptions.DBusException:
        _LOGGER.debug(f"Unit {name} stop failed", exc_info=True)


def start_control(control_type: str) -> None:
    _LOGGER.debug(f"Starting control {control_type}")

    properties = basic_service(f"Forge acquisition control {control_type.upper()}")
    set_dependencies(
        properties,
        before=["forge-acquisition-start.target", "forge-acquisition-control.target"],
        after=["forge-acquisition-bus.socket", "forge-acquisition-initialize.target"],
        binds_to=["forge-acquisition-bus.socket"],
        conflicts=["forge-acquisition-stop.target"],
    )
    properties.append(("ExecStart", assemble_forge_exec(
        "forge-acquisition-control", "--systemd", control_type
    )))

    unit_name = f"forge-control-{control_type}.service"
    release_transient_unit(unit_name)
    start_transient_unit(unit_name, properties)


def start_instrument_serial(source: str, instrument_unit_name: str) -> typing.Optional[str]:
    physical_port = CONFIGURATION.get(f"INSTRUMENT.{source}.SERIAL_PORT")
    if not physical_port:
        return None
    if isinstance(physical_port, dict):
        if CONFIGURATION.get(f"INSTRUMENT.{source}.SERIAL_PORT.DIRECT"):
            return None
        physical_port = CONFIGURATION.get(f"INSTRUMENT.{source}.SERIAL_PORT.PORT")
    if not physical_port:
        return None

    _LOGGER.debug(f"Starting serial port interface for {source} on {physical_port}")

    serial_unit_name = f"forge-serial-{source}.service"

    properties = basic_service(f"Forge acquisition serial interface for {source}")
    set_dependencies(
        properties,
        before=["forge-acquisition-start.target", instrument_unit_name],
        part_of=[instrument_unit_name],
        conflicts=["forge-acquisition-stop.target"],
    )
    properties.append(("RuntimeDirectory", [f"forge-serial-{source}"]))
    properties.append(("UMask", dbus.types.UInt32(0o0007)))
    if _SERIAL_ACCESS_GROUPS:
        properties.append(("SupplementaryGroups", _SERIAL_ACCESS_GROUPS))
    properties.append(("ExecStart", assemble_forge_exec(
        "forge-acquisition-serial-multiplexer", "--systemd", *(["--debug"] if _ENABLE_DEBUG else []),
        "--eavesdropper", "${RUNTIME_DIRECTORY}/eavesdropper.sock",
        "--raw", "${RUNTIME_DIRECTORY}/raw.sock",
        "--control", "${RUNTIME_DIRECTORY}/control.dgram",
        "--",
        physical_port, "${RUNTIME_DIRECTORY}/instrument.tty", "${RUNTIME_DIRECTORY}/eavesdropper.tty"
    )))

    release_transient_unit(serial_unit_name)
    start_transient_unit(serial_unit_name, properties)
    return serial_unit_name


def start_instrument(source: str) -> None:
    instrument_type = CONFIGURATION.get(f"INSTRUMENT.{source}.TYPE")
    if not instrument_type:
        _LOGGER.warning(f"Instrument {source} has no type, skipping")
        return

    instrument_unit_name = f"forge-instrument-{source}.service"
    release_transient_unit(instrument_unit_name)
    serial_controller = start_instrument_serial(source, instrument_unit_name)
    serial_args: typing.List[str] = []
    if serial_controller:
        serial_args.append(f"--serial=/run/forge-serial-{source}/instrument.tty")
        serial_args.append(f"--control=/run/forge-serial-{source}/control.dgram")

    _LOGGER.debug(f"Starting instrument {source}")

    after: typing.List[str] = [
        "forge-acquisition-bus.socket",
        "forge-acquisition-control.target",
        "forge-acquisition-initialize.target",
    ]
    if serial_controller:
        after.append(serial_controller)

    properties = basic_service(f"Forge acquisition instrument {source}", user=_DATA_OUTPUT_USER)
    set_dependencies(
        properties,
        before=["forge-acquisition-start.target"],
        after=after,
        binds_to=["forge-acquisition-bus.socket"],
        conflicts=["forge-acquisition-stop.target"],
    )
    properties.append(("RuntimeDirectory", [f"forge-instrument-{source}"]))
    properties.append(("UMask", dbus.types.UInt32(0o0007)))
    properties.append(("ReadWritePaths", [_COMPLETED_DATA_DIRECTORY, _STATE_LOCATION_DIRECTORY]))
    properties.append(("ExecStart", assemble_forge_exec(
        "forge-acquisition-instrument", "--systemd", *(["--debug"] if _ENABLE_DEBUG else []),
        "--data-working", "${RUNTIME_DIRECTORY}",
        "--data-completed", _COMPLETED_DATA_DIRECTORY,
        "--state-location", _STATE_LOCATION_DIRECTORY,
        *serial_args,
        "--",
        instrument_type, source
    )))
    properties.append(("ExecStopPost", assemble_exec(
        "find", "${RUNTIME_DIRECTORY}", "-mindepth", "1", "-maxdepth", "1", "-type", "f",
        "-name", "*.nc",
        "-exec", "mv", "--", "{}", f"{_COMPLETED_DATA_DIRECTORY}/", ";"
    )))

    start_transient_unit(instrument_unit_name, properties)


def need_spancheck_control() -> bool:
    spancheck_instruments = frozenset({
        'ecotechnephelometer',
        'tsi3563nephelometer',
    })
    instrument_root = CONFIGURATION.get("INSTRUMENT")
    if not instrument_root:
        return False
    for source in instrument_root.keys():
        instrument_type = CONFIGURATION.get(f"INSTRUMENT.{source}.TYPE")
        if instrument_type in spancheck_instruments:
            return True
    return False


def need_pressure_bypass_control() -> bool:
    pressure_bypass = CONFIGURATION.get("ACQUISITION.PRESSURE_BYPASS")
    if pressure_bypass is not None and not pressure_bypass:
        return False
    return CONFIGURATION.get("ACQUISITION.PRESSURE_BYPASS.ENABLE", True)


def start_all_control() -> None:
    try:
        start_control("restart")
    except dbus.exceptions.DBusException:
        _LOGGER.debug("Retrying initial unit startup", exc_info=True)
        unit_name = f"forge-control-restart.service"
        release_transient_unit(unit_name)
        time.sleep(0.5)
        release_transient_unit(unit_name)
        time.sleep(0.5)
        start_control("restart")

    cutsize = CONFIGURATION.get("ACQUISITION.CUTSIZE")
    if cutsize and not CutSize(LayeredConfiguration(cutsize)).constant_size:
        start_control("impactorcycle")

    if need_spancheck_control():
        start_control("spancheck")

    if need_pressure_bypass_control():
        start_control("pressurebypass")


def start_all_instruments() -> None:
    instrument_root = CONFIGURATION.get("INSTRUMENT")
    if not instrument_root:
        _LOGGER.warning("No instruments configured")
        return
    for source in instrument_root.keys():
        start_instrument(source)


def main():
    parser = argparse.ArgumentParser(description="Forge acquisition systemd startup sequencer.")

    parser.add_argument('--debug',
                        dest='debug', action='store_true',
                        help="enable debug output")

    parser.add_argument('--group',
                        dest='group', default='forge',                        
                        help="shared group for system execution")
    parser.add_argument('--data-user',
                        dest='data_user', default='forge-data',
                        help="owning user of output data")
    parser.add_argument('--data-directory',
                        dest='data_directory', default="/var/lib/forge/incoming",
                        help="directory to move completed data files to")
    parser.add_argument('--state-location',
                        dest='state_location', default="/var/lib/forge/state",
                        help="directory that state is stored in")

    parser.add_argument('--executable-path',
                        dest='executable_path', default=sys.path[0],
                        help="absolute path to search for Forge executables in")

    parser.add_argument('--serial-group',
                        dest='serial_groups', action='append',
                        help="additional groups to add for serial port access")

    args = parser.parse_args()

    if args.debug:
        root_logger = logging.getLogger()
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(name)-40s %(message)s')
        handler.setFormatter(formatter)
        root_logger.setLevel(logging.DEBUG)
        root_logger.addHandler(handler)
        global _ENABLE_DEBUG
        _ENABLE_DEBUG = True
        
    global _ACQUISITION_SYSTEM_GROUP
    _ACQUISITION_SYSTEM_GROUP = args.group
    global _DATA_OUTPUT_USER
    _DATA_OUTPUT_USER = args.data_user
    global _SERIAL_ACCESS_GROUPS
    if args.serial_groups:
        _SERIAL_ACCESS_GROUPS = args.serial_groups
    global _COMPLETED_DATA_DIRECTORY
    _COMPLETED_DATA_DIRECTORY = args.data_directory
    global _STATE_LOCATION_DIRECTORY
    _STATE_LOCATION_DIRECTORY = args.state_location
    global _EXECUTABLE_LOCATION
    _EXECUTABLE_LOCATION = args.executable_path

    bus = dbus.SystemBus()
    pid1 = bus.get_object("org.freedesktop.systemd1", "/org/freedesktop/systemd1")
    global _SYSTEMD
    _SYSTEMD = dbus.Interface(pid1, dbus_interface="org.freedesktop.systemd1.Manager")

    stop_unit("forge-acquisition-stop.target")
    start_unit("forge-acquisition-initialize.target")
    start_all_control()
    start_unit("forge-acquisition-control.target")
    start_all_instruments()
    start_unit("forge-acquisition-start.target")


if __name__ == '__main__':
    main()
