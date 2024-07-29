#!/usr/bin/env python3

import typing
import logging
import argparse
import sys
from pathlib import Path
from tomlkit.items import Item as TOMLItem, Table as TOMLTable, Key as TOMLKey
from forge.acquisition import LayeredConfiguration

_LOGGER = logging.getLogger(__name__)


class Instrument:
    def __init__(self, name: str, acquisition_type: str,
                 serial_parent: TOMLTable, serial_key: TOMLKey,
                 current_port: Path):
        self.name = name
        self.acquisition_type = acquisition_type
        self.serial_parent = serial_parent
        self.serial_key = serial_key
        self.current_port = current_port

        self.numbered_port: typing.Optional[Path] = None
        comment = serial_parent[serial_key].trivia.comment.strip()
        if comment.startswith("#"):
            comment = comment[1:].strip()
        if comment.startswith("/dev/"):
            comment = comment[5:]
        if comment.startswith("tty") and "/" not in comment:
            try:
                self.numbered_port = Path("/dev/" + comment).resolve(strict=False)
            except (IOError, FileNotFoundError):
                pass
        self.renumbered: bool = False

        self.suggested_port: typing.Optional[Path] = None
        self._find_suggested_port()

    def _find_suggested_port(self) -> None:
        if self.current_port.is_relative_to("/dev/serial/by-id/") or self.current_port.is_relative_to("/dev/serial/by-path/"):
            if self.current_port.is_char_device():
                try:
                    renumber = str(self.current_port.resolve(strict=True))
                except (IOError, FileNotFoundError):
                    return
                if renumber.startswith("/dev/"):
                    renumber = renumber[5:]
                    if "/" not in renumber:
                        target = Path("/dev/" + renumber)
                        if target != self.numbered_port:
                            self.numbered_port = target
                            self.renumbered = True
                return

        target_port = None
        if self.current_port.is_char_device():
            try:
                renumber = str(self.current_port.resolve(strict=True))
                if renumber.startswith("/dev/"):
                    renumber = renumber[5:]
                    if "/" not in renumber:
                        target_port = "/dev/" + renumber
            except (IOError, FileNotFoundError):
                pass
        if target_port is None and self.numbered_port is not None:
            target_port = self.numbered_port
        if target_port is None:
            return

        for check in Path("/dev/serial/by-id").iterdir():
            try:
                resolved = check.resolve(strict=True)
            except (IOError, FileNotFoundError):
                continue
            if resolved == target_port:
                if check == self.current_port:
                    return
                self.suggested_port = check
                return

    def set_port(self, device: Path, target: typing.Optional[Path] = None) -> None:
        self.serial_parent[self.serial_key] = str(device)
        if target:
            self.serial_parent[self.serial_key].comment(str(target.name))
        else:
            self.serial_parent[self.serial_key].trivia.comment = ""
            self.serial_parent[self.serial_key].trivia.comment_ws = ""


def main():
    parser = argparse.ArgumentParser(description="Forge serial port setup.")

    parser.add_argument('--debug',
                        dest='debug', action='store_true',
                        help="enable debug output")
    parser.add_argument('--config',
                        dest='config', default="/etc/forge/acquisition/settings.local.toml",
                        help="location of configuration file to manage")
    parser.add_argument('--require-local-ports',
                        dest='require_local_ports', action='store_true',
                        help="require local USB serial ports before doing anything")

    args, other_args = parser.parse_known_args()
    if args.debug:
        from forge.log import set_debug_logger
        set_debug_logger()

    if args.require_local_ports:
        for dev in Path("/dev").iterdir():
            name = dev.name
            if not name.startswith("ttyUSB") and not name.startswith("ttyACM"):
                continue
            if not dev.is_char_device():
                continue
            break
        else:
            _LOGGER.debug("No USB serial ports found, exiting")
            exit(0)

    from tomlkit import load, dump
    with open(args.config, "rt") as f:
        root = load(f)

    instrument_root = LayeredConfiguration.toml_path(root, "instrument")
    if instrument_root is None:
        print("No instrument configuration defined")
        exit(1)

    instruments: typing.List[Instrument] = list()
    for name in instrument_root:
        inst = instrument_root[name]
        if not isinstance(inst, TOMLTable):
            continue
        inst = inst.value

        acquisition_type, _ = LayeredConfiguration.toml_resolve(inst, "type")
        if acquisition_type is None:
            continue
        try:
            acquisition_type = str(acquisition_type.value)
        except:
            continue

        serial_parent = inst
        serial_value, serial_key = LayeredConfiguration.toml_resolve(inst, "serial_port")
        if serial_value is None:
            continue

        if isinstance(serial_value.value, TOMLTable):
            serial_parent = serial_value.value
            serial_value, serial_key = LayeredConfiguration.toml_resolve(serial_parent, "port")
            if serial_value is None:
                continue

        try:
            port_path = Path(str(serial_value.value))
        except (TypeError, ValueError):
            continue
        if not port_path.is_relative_to("/dev/"):
            continue

        instruments.append(Instrument(name, acquisition_type, serial_parent, serial_key, port_path))
        _LOGGER.debug(f"Found instrument {name}/{acquisition_type} on {port_path}")

    if len(instruments) == 0:
        print("No serial port instruments defined, nothing to display")
        exit(1)
    instruments.sort(key=lambda x: x.name)

    from PyQt5 import QtWidgets
    from forge.acquisition.serial.setup.ui import Main

    app = QtWidgets.QApplication(sys.argv[:1] + other_args)
    window = Main()
    for i in instruments:
        window.add_instrument(i)
    window.table.adjustSize()
    window.adjustSize()
    window.show()
    rc = app.exec_()
    if window.save_changes:
        with open(args.config, "wt") as f:
            dump(root, f)
    sys.exit(rc)


if __name__ == '__main__':
    main()
