import typing
from pathlib import Path
from PyQt5 import QtCore, QtGui, QtWidgets

if typing.TYPE_CHECKING:
    from .__main__ import Instrument


def _get_serial_prefix() -> typing.Set[str]:
    result: typing.Set[str] = {"ttyS", "ttyUSB", "ttyACM"}
    try:
        drivers = open("/proc/tty/drivers", "r")
    except FileNotFoundError:
        return result
    with drivers:
        for l in drivers:
            fields = l.split()
            if len(fields) < 5:
                continue
            prefix = fields[1]
            driver_type = fields[4].lower()
            if driver_type != "serial":
                continue
            if prefix == "/dev/tty":
                continue
            prefix = prefix.split("/")[-1]
            result.add(prefix)
    return result


class Main(QtWidgets.QMainWindow):
    def __init__(self):
        QtWidgets.QMainWindow.__init__(self)
        self.setObjectName("SETUP")
        self.setWindowTitle("Forge Serial Port Setup")

        self.have_changes = False
        self.save_changes = False

        central_widget = QtWidgets.QWidget(self)
        self.setCentralWidget(central_widget)
        central_layout = QtWidgets.QVBoxLayout(central_widget)
        central_widget.setLayout(central_layout)

        self.table = QtWidgets.QTableWidget(central_widget)
        central_layout.addWidget(self.table, 1)
        self.table.setColumnCount(4)
        self.table.horizontalHeader().setSectionResizeMode(QtWidgets.QHeaderView.ResizeToContents)
        self.table.horizontalHeader().setStretchLastSection(True)
        self.table.verticalHeader().setVisible(False)
        self.table.setSelectionMode(QtWidgets.QAbstractItemView.SelectionMode.NoSelection)
        self.table.setFocusPolicy(QtCore.Qt.NoFocus)
        self.table.setSizeAdjustPolicy(QtWidgets.QAbstractScrollArea.SizeAdjustPolicy.AdjustToContents)
        self.table.setHorizontalHeaderLabels([
            "ID",
            "Type",
            "Current",
            "Changed",
        ])

        control_buttons = QtWidgets.QDialogButtonBox(
            QtWidgets.QDialogButtonBox.Cancel |
            QtWidgets.QDialogButtonBox.Save,
            central_widget
        )
        central_layout.addWidget(control_buttons)
        control_buttons.rejected.connect(self.close)
        control_buttons.accepted.connect(self.save_and_quit)

        prefixes = _get_serial_prefix()
        self._numbered_ports: typing.Set[Path] = set()
        for dev in Path("/dev").iterdir():
            for check in prefixes:
                if dev.name.startswith(check):
                    break
            else:
                continue
            if dev.name.startswith("ttyS"):
                try:
                    index = int(dev.name[4:])
                    if index >= 4:
                        continue
                except (ValueError, TypeError):
                    pass
            if not dev.is_char_device():
                continue
            self._numbered_ports.add(dev)

        self._id_ports: typing.Dict[Path, Path] = dict()
        try:
            for dev in Path("/dev/serial/by-id").iterdir():
                try:
                    target = dev.resolve(strict=True)
                except (IOError, FileNotFoundError):
                    continue
                if not target.is_char_device():
                    continue
                self._id_ports[dev] = target
        except FileNotFoundError:
            pass

        self._path_ports: typing.Dict[Path, Path] = dict()
        try:
            for dev in Path("/dev/serial/by-path").iterdir():
                try:
                    target = dev.resolve(strict=True)
                except (IOError, FileNotFoundError):
                    continue
                if not target.is_char_device():
                    continue
                self._path_ports[dev] = target
        except FileNotFoundError:
            pass

        self._all_ports: typing.List[typing.Tuple[str, Path, Path]] = list()

        sorted_ports = list(self._id_ports.keys())
        sorted_ports.sort(key=lambda x: x.name)
        for dev in sorted_ports:
            target = self._id_ports[dev]
            self._all_ports.append((f"{dev.name} @ {target.name}", dev, target))
            self._numbered_ports.discard(target)

        sorted_ports = list(self._path_ports.keys())
        sorted_ports.sort(key=lambda x: x.name)
        if sorted_ports and self._all_ports:
            self._all_ports.append(("", Path(), Path()))
        for dev in sorted_ports:
            target = self._path_ports[dev]
            self._all_ports.append((f"{dev.name} @ {target.name}", dev, target))
            self._numbered_ports.discard(target)

        sorted_ports = sorted(self._numbered_ports, key=lambda x: x.name)
        if sorted_ports and self._all_ports:
            self._all_ports.append(("", Path(), Path()))
        for dev in sorted_ports:
            self._all_ports.append((f"{dev.name}", dev, dev))

    def closeEvent(self, event):
        if not self.have_changes:
            event.accept()
            return

        close = QtWidgets.QMessageBox.question(self,
                                               "Confirm Exit",
                                               "Changes have been made.  Are you sure you want to quit and discard them?",
                                               QtWidgets.QMessageBox.Save | QtWidgets.QMessageBox.Cancel | QtWidgets.QMessageBox.Close)
        if close == QtWidgets.QMessageBox.Cancel:
            event.ignore()
            return
        if close == QtWidgets.QMessageBox.Save:
            self.save_changes = True

        event.accept()

    def save_and_quit(self) -> None:
        self.have_changes = False
        self.save_changes = True
        self.close()

    def add_instrument(self, instrument: "Instrument") -> None:
        row = self.table.rowCount()
        self.table.insertRow(row)

        def populate_text_cells():
            name = QtWidgets.QTableWidgetItem(instrument.name)
            self.table.setItem(row, 0, name)

            acquisition_type = QtWidgets.QTableWidgetItem(instrument.acquisition_type)
            self.table.setItem(row, 1, acquisition_type)

            current_port = QtWidgets.QTableWidgetItem(str(instrument.current_port.name))
            self.table.setItem(row, 2, current_port)

        select_item = QtWidgets.QTableWidgetItem()
        self.table.setItem(row, 3, select_item)

        select = QtWidgets.QComboBox()
        select.setSizeAdjustPolicy(QtWidgets.QComboBox.SizeAdjustPolicy.AdjustToContents)
        self.table.setCellWidget(row, 3, select)

        select.setCurrentIndex(-1)
        hit_suggested = False
        for (title, dev, target) in self._all_ports:
            if not title:
                select.insertSeparator(select.count())
                continue
            select.addItem(title, (dev, target))
            if dev == instrument.suggested_port:
                select.setCurrentIndex(select.count() - 1)
                hit_suggested = True
            elif not hit_suggested and dev == instrument.current_port:
                select.setCurrentIndex(select.count() - 1)

        if instrument.current_port and select.currentIndex() < 0:
            select.insertItem(0, f"{instrument.current_port}", (instrument.current_port, instrument.current_port))
            select.setCurrentIndex(0)

        def update_selection():
            (dev, target) = select.itemData(select.currentIndex())
            instrument.set_port(dev, target if target != dev else None)

            populate_text_cells()
            if dev == instrument.current_port:
                if instrument.renumbered and not instrument.suggested_port:
                    for col in range(3):
                        self.table.item(row, col).setForeground(QtGui.QColor(0, 0xaa, 0))
                    self.have_changes = True
            elif dev == instrument.suggested_port:
                for col in range(3):
                    self.table.item(row, col).setForeground(QtGui.QColor(0, 0xaa, 0))
                self.have_changes = True
            else:
                for col in range(3):
                    self.table.item(row, col).setForeground(QtGui.QColor(0x8c, 0x67, 0))
                self.have_changes = True

        select.currentIndexChanged.connect(update_selection)
        update_selection()
        self.table.resizeColumnToContents(3)


if __name__ == '__main__':
    import sys
    app = QtWidgets.QApplication(sys.argv)
    window = Main()
    window.show()
    sys.exit(app.exec_())
