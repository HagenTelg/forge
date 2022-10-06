import typing
import curses
import time
from forge.acquisition.console.ui import UserInterface
from .data import DataDisplay
from ..lookup import instrument_data


class InstrumentWindow:
    def __init__(self, ui: UserInterface, source: str, instrument_info: typing.Dict[str, typing.Any]):
        self.ui = ui
        self.source = source
        self.instrument_info = instrument_info
        self.state: typing.Dict[str, typing.Any] = dict()
        self.data: typing.Dict[str, typing.Union[float, int, str, typing.List[float]]] = dict()

        self.data_display: typing.Dict[str, typing.Optional[DataDisplay]] = dict()

        self.instrument = instrument_info.get('type')
        if self.instrument:
            data_display = instrument_data(self.instrument, 'data', 'display')
            if data_display:
                self.data_display.update(data_display)

        self.window = self.ui.stdscr.subwin(0, 0, 3, 3)
        self._center_window: bool = True
        self._x = 0
        self._y = 0

    def instrument_message(self, record: str, message: typing.Any) -> bool:
        if record == 'instrument':
            self.instrument_info = message
            return False
        elif record == 'state':
            self.state = message
            return True

        if record == 'data':
            self.data.update(message)
            return True

    def global_message(self, source: str, record: str, message: typing.Any) -> bool:
        pass

    @property
    def display_letter(self) -> typing.Optional[str]:
        return self.instrument_info.get('display_letter')

    @property
    def window_title(self) -> str:
        manufacturer = self.instrument_info.get('manufacturer')
        model = self.instrument_info.get('model')
        serial_number = self.instrument_info.get('serial_number')

        result = self.source

        if manufacturer:
            result = result + " " + str(manufacturer)
        if model:
            result = result + " " + str(model)
        if serial_number:
            result = result + " #" + str(serial_number)

        return result

    @property
    def detailed_status(self) -> str:
        communicating = self.state.get('communicating')
        bypassed = self.state.get('bypassed')
        notifications = self.state.get('notifications')
        if communicating is not None and not communicating:
            return "NO COMMUNICATIONS"
        elif notifications:
            if len(notifications) == 1:
                return notifications[0]
            max_y, max_x = self.ui.stdscr.getmaxyx()
            elide = 2
            max_length = max_y / 2
            while True:
                elided_notifications: typing.List[str] = list()
                any_elided = False
                for n in notifications:
                    if len(n) < 8:
                        elided_notifications.append(n)
                        continue

                    if elide >= len(n)-4:
                        n = n[0:2] + "..." + n[-2:]
                    else:
                        n = n[0:elide] + "..." + n[-elide:]
                        any_elided = True

                    elided_notifications.append(n)

                check = " ".join(elided_notifications)
                if len(check) < max_length:
                    return check
                if not any_elided:
                    break
                elide += 1
            return " ".join(notifications)
        elif bypassed:
            return "BYPASSED"

        return ""

    def _unroll_formatted(self, value: typing.Union[float, int, str, typing.List[float]],
                          formatter: typing.Callable[[float], str]) -> str:
        if isinstance(value, str):
            return value
        if isinstance(value, list):
            if len(value) <= 4:
                return ", ".join([self._unroll_formatted(v, formatter) for v in value])

            try:
                center_max = max(value[1:-1])
            except TypeError:
                center_max = None
            return (
                self._unroll_formatted(value[0], formatter) +
                (" .. " + self._unroll_formatted(center_max, formatter) + " .. " if center_max else " ... ") +
                self._unroll_formatted(value[-1], formatter)
            )
        return formatter(value)

    def data_lines(self) -> typing.List[typing.Tuple[str, str]]:
        result: typing.List[typing.Tuple[str, str]] = list()
        for name, value in self.data.items():
            if name not in self.data_display:
                result.append((name, self._unroll_formatted(value, DataDisplay.apply_default_format)))
                continue

            display = self.data_display[name]
            if not display:
                continue

            value = self._unroll_formatted(value, display.apply_format)
            if display.name:
                name = display.name

            result.append((name, value))
        result.sort(key=lambda x: x[0])
        return result

    def data_columns(self) -> typing.List[typing.List[str]]:
        max_y, max_x = self.ui.stdscr.getmaxyx()
        max_y -= 1

        data_lines = self.data_lines()

        max_rows = max_y - 5
        n_cols = 1
        n_rows = len(data_lines)
        while n_rows > max_rows:
            n_cols += 1
            n_rows = len(data_lines) // n_cols
            if len(data_lines) % n_cols != 0:
                n_rows += 1

        result_columns: typing.List[typing.List[str]] = list()
        add_index = 0
        for c in range(n_cols):
            label_width = 0
            value_width = 0
            display_index = add_index
            for r in range(n_rows):
                if add_index >= len(data_lines):
                    break
                line = data_lines[add_index]
                add_index += 1

                label_width = max(label_width, len(line[0]))
                value_width = max(value_width, len(line[1]))

            column_data: typing.List[str] = list()
            result_columns.append(column_data)
            for r in range(n_rows):
                if display_index >= len(data_lines):
                    break
                line = data_lines[display_index]
                display_index += 1

                column_data.append(line[0].ljust(label_width) + " " + line[1].rjust(value_width))

        return result_columns

    def content_size(self, columns: typing.List[typing.List[str]]) -> typing.Tuple[int, int]:
        max_x = len(self.window_title)
        max_y = 0

        if columns:
            max_rows = 0
            total_width = 0
            for c in columns:
                max_rows = max(max_rows, len(c))
                row_width = 0
                for r in c:
                    row_width = max(row_width, len(r))
                if total_width == 0:
                    total_width += 2
                total_width += row_width

            max_y += max_rows
            max_x = max(max_x, total_width)

        status_line = self.detailed_status
        if status_line:
            max_y += 1
            max_x = max(max_x, len(status_line))

        return max(max_x, 1), max(max_y, 1)

    def draw(self) -> None:
        max_y, max_x = self.ui.stdscr.getmaxyx()
        max_y -= 1
        
        win_y, win_x = self.window.getmaxyx()

        columns = self.data_columns()
        content_x, content_y = self.content_size(columns)
        title = self.window_title
        if content_x < len(title):
            content_x = len(title)
        content_x += 2
        content_y += 2
        if content_x > max_x or content_y > max_y:
            return

        apply_resize = False
        if content_x != win_x or content_y != win_y:
            apply_resize = True
            win_x = content_x
            win_y = content_y

        if self._center_window:
            self._center_window = False
            self._x = (max_x - win_x) // 2
            self._y = (max_y - win_y) // 2

        if self._x + win_x >= max_x:
            self._x = max_x - win_x - 1
        if self._x < 0:
            self._x = 0
        if self._y + win_y >= max_y:
            self._y = max_y - win_y - 1
        if self._y < 0:
            self._y = 0

        if self._x + win_x >= max_x:
            return
        if self._y + win_y >= max_y:
            return

        if apply_resize:
            try:
                self.window.mvwin(0, 0)
                self.window.resize(win_y, win_x)
            except curses:
                return

        self.window.mvwin(self._y, self._x)

        self.window.clear()
        self.window.box()
        self.window.addstr(0, (win_x - len(title))//2, title)

        status_line = self.detailed_status
        if status_line:
            communicating = self.state.get('communicating')

            attr = 0
            if communicating is not None and not communicating:
                attr = curses.color_pair(curses.COLOR_RED)
                if int(time.time()) % 2 == 0:
                    attr |= curses.A_BOLD

            self.window.addstr(win_y-2, (win_x - len(status_line)) // 2, status_line, attr)

        column_x = 1
        for c in columns:
            row_width = 0
            column_y = 1
            for r in c:
                row_width = max(row_width, len(r))
                self.window.addstr(column_y, column_x, r, 0)
                column_y += 1
            column_x += row_width + 2

        self.window.noutrefresh()

    def status_line(self) -> typing.Tuple[typing.Optional[str], typing.Optional[int]]:
        display_letter = self.display_letter
        if not display_letter:
            return None, None

        communicating = self.state.get('communicating')
        bypassed = self.state.get('bypassed')
        warning = self.state.get('warning')
        if communicating is not None and not communicating:
            attr = curses.color_pair(curses.COLOR_RED)
            if int(time.time()) % 2 == 0:
                attr |= curses.A_BOLD
        elif warning:
            attr = curses.color_pair(curses.COLOR_CYAN)
        elif bypassed:
            attr = curses.A_DIM | curses.color_pair(curses.COLOR_WHITE)
        else:
            attr = curses.color_pair(curses.COLOR_GREEN)

        return display_letter, attr

    def handle_key(self, key: int) -> bool:
        if key == curses.KEY_LEFT:
            self._x -= 1
            return True
        elif key == curses.KEY_RIGHT:
            self._x += 1
            return True
        elif key == curses.KEY_UP:
            self._y -= 1
            return True
        elif key == curses.KEY_DOWN:
            self._y += 1
            return True
        return False


def create(ui: UserInterface, source: str,
           instrument_info: typing.Dict[str, typing.Any]) -> typing.Optional[InstrumentWindow]:
    return InstrumentWindow(ui, source, instrument_info)
