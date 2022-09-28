import typing
import asyncio
import curses
import curses.ascii
import sys
import time
from collections import deque
from forge.formattime import format_iso8601_time, format_year_doy
from forge.acquisition.bus.client import AcquisitionBusClient, PersistenceLevel
from .instrument.lookup import instrument_data

if typing.TYPE_CHECKING:
    from .instrument.default.window import InstrumentWindow


_message_log_author: str = ""


class Menu:
    class Entry:
        def __init__(self, text: str, selected: typing.Callable[[], None]):
            self.text = text
            self.selected = selected

    def __init__(self, ui: "UserInterface"):
        self.ui = ui
        self.window = self.ui.stdscr.subwin(10, 10, 5, 5)
        self.window.attrset(curses.A_BOLD | curses.color_pair(curses.COLOR_YELLOW))

        self._entries: typing.List[Menu.Entry] = list()
        self._selected_index: int = 0
        self._column_pitch = 0
        self._hotkeys: typing.Dict[int, Menu.Entry] = dict()

    def add_entry(self, text: str, selected: typing.Callable[[], None],
                  hotkey: typing.Optional[int] = None) -> "Menu.Entry":
        e = self.Entry(text, selected)
        self._entries.append(e)
        if hotkey is not None:
            self._hotkeys[hotkey] = e
        return e

    def draw(self) -> None:
        if not self._entries:
            return

        max_y, max_x = self.ui.stdscr.getmaxyx()
        max_y -= 1

        max_rows = max_y - 5
        n_cols = 1
        n_rows = len(self._entries)
        while n_rows > max_rows:
            n_cols += 1
            n_rows = len(self._entries) // n_cols
            if len(self._entries) % n_cols != 0:
                n_rows += 1

        if n_cols > 1:
            self._column_pitch = n_rows
        else:
            self._column_pitch = 0

        column_widths: typing.List[int] = list()
        add_index = 0
        for c in range(n_cols):
            col_width = 0
            for r in range(n_rows):
                if add_index >= len(self._entries):
                    break
                entry = self._entries[add_index]
                col_width = max(col_width, len(entry.text))
                add_index += 1
            column_widths.append(col_width)

        content_y = n_rows
        content_x = sum(column_widths) + (len(column_widths)-1)*2

        win_y, win_x = self.window.getmaxyx()
        if content_x + 2 != win_x or content_y + 2 != win_y:
            try:
                self.window.mvwin(0, 0)
                self.window.resize(content_y + 2, content_x + 2)
            except:
                return
            win_y, win_x = self.window.getmaxyx()

        self.window.mvwin((max_y - win_y) // 2, (max_x - win_x) // 2)
        self.window.clear()
        self.window.box()

        entry_x = 1
        add_index = 0
        for c in range(n_cols):
            for r in range(n_rows):
                if add_index >= len(self._entries):
                    break
                entry = self._entries[add_index]
                is_selected = (add_index == self._selected_index)
                add_index += 1

                if is_selected:
                    attr = curses.A_REVERSE | curses.A_BOLD
                else:
                    attr = 0

                self.window.addstr(r+1, entry_x, entry.text.ljust(column_widths[c]), attr)
            entry_x += column_widths[c] + 2

        self.window.noutrefresh()

    def hide(self) -> None:
        self.ui._menu = None
        self.ui._changed.set()

    def handle_key(self, key: int) -> None:
        if key == ord('\n') or key == ord('\r') or key == curses.KEY_ENTER:
            self.hide()
            if self._selected_index < len(self._entries):
                self._entries[self._selected_index].selected()
            return
        elif key == 0x1B or key == 0x17:
            self.hide()
            return

        if key == curses.KEY_LEFT:
            pos = self._selected_index - self._column_pitch
            if pos >= 0:
                self._selected_index = pos
        elif key == curses.KEY_RIGHT:
            pos = self._selected_index + self._column_pitch
            if pos < len(self._entries):
                self._selected_index = pos
        elif key == curses.KEY_UP:
            self._selected_index -= 1
        elif key == curses.KEY_DOWN:
            self._selected_index += 1
        elif key == curses.KEY_HOME:
            self._selected_index = 0
        elif key == curses.KEY_END:
            self._selected_index = len(self._entries) - 1
        elif key == curses.KEY_PPAGE:
            if self._column_pitch:
                self._selected_index = (self._selected_index // self._column_pitch) * self._column_pitch
            else:
                self._selected_index = 0
        elif key == curses.KEY_NPAGE:
            if self._column_pitch:
                self._selected_index = (self._selected_index // self._column_pitch) * self._column_pitch
                self._selected_index += self._column_pitch - 1
            else:
                self._selected_index = len(self._entries) - 1
        else:
            hk = self._hotkeys.get(key)
            if hk:
                self.hide()
                hk.selected()
                return

        if self._selected_index >= len(self._entries):
            self._selected_index = len(self._entries) - 1
        if self._selected_index < 0:
            self._selected_index = 0


class Dialog:
    class _Item:
        def __init__(self, dialog: "Dialog"):
            self.dialog = dialog
            if self.dialog._focus_index == -1 and self.focusable:
                self.dialog._focus_index = len(self.dialog._items)
            self.dialog._items.append(self)
            self.y = len(self.dialog._items)
            self.window = self.dialog.window

        @property
        def width(self) -> int:
            return 0

        @property
        def focusable(self) -> bool:
            return True

        def draw(self, is_focused: bool) -> None:
            pass

        def handle_key(self, key: int) -> bool:
            return False

    def label(self, text: str):
        class Label(Dialog._Item):
            def __init__(self, dialog: Dialog, text: str):
                super().__init__(dialog)
                self.text = text

            @property
            def width(self) -> int:
                return len(self.text)

            @property
            def focusable(self) -> bool:
                return False

            def draw(self, is_focused: bool) -> None:
                if is_focused:
                    attr = curses.A_REVERSE | curses.A_BOLD
                else:
                    attr = 0

                self.dialog.window.addstr(self.y, 1, self.text, attr)
        return Label(self, text)

    def button(self, text: str, pressed: typing.Callable[[], None] = None):
        class Button(Dialog._Item):
            def __init__(self, dialog: Dialog, text: str, pressed: typing.Callable[[], None] = None):
                super().__init__(dialog)
                self.text = text
                self.pressed = pressed or self.dialog.hide

            @property
            def width(self) -> int:
                return len(self.text)

            def draw(self, is_focused: bool) -> None:
                _, win_x = self.window.getmaxyx()

                draw_text = self.text.center(win_x - 2)
                if is_focused:
                    attr = curses.A_REVERSE | curses.A_BOLD
                else:
                    attr = 0
                self.dialog.window.addstr(self.y, 1, draw_text, attr)

            def handle_key(self, key: int) -> bool:
                if key == ord('\n') or key == ord('\r') or key == curses.KEY_ENTER:
                    self.pressed()
                    return True
                return False
        return Button(self, text, pressed)

    def yes_no(self, on_yes: typing.Callable[[], None] = None,
                     on_no: typing.Callable[[], None] = None):
        class YesNo(Dialog._Item):
            def __init__(self, dialog: Dialog, on_yes: typing.Callable[[], None] = None,
                         on_no: typing.Callable[[], None] = None):
                super().__init__(dialog)
                self.yes_text = "Yes"
                self.no_text = "No"
                self.on_yes = on_yes
                self.on_no = on_no
                self.yes_selected: bool = False
                self.yes_left: bool = True

            @property
            def width(self) -> int:
                return len(self.yes_text) + 4 + len(self.no_text)

            def draw(self, is_focused: bool) -> None:
                _, win_x = self.window.getmaxyx()
                half_width = (win_x-2) // 2

                left_width = half_width
                right_width = half_width + (win_x-2) % 2
                if self.yes_left:
                    yes_text = self.yes_text.center(left_width)
                    yes_x = 1
                    no_text = self.no_text.center(right_width)
                    no_x = half_width + 1
                else:
                    no_text = self.no_text.center(left_width)
                    no_x = 1
                    yes_text = self.yes_text.center(right_width)
                    yes_x = half_width + 1

                if self.yes_selected and is_focused:
                    attr = curses.A_REVERSE | curses.A_BOLD
                else:
                    attr = 0
                self.dialog.window.addstr(self.y, yes_x, yes_text, attr)

                if not self.yes_selected and is_focused:
                    attr = curses.A_REVERSE | curses.A_BOLD
                else:
                    attr = 0
                self.dialog.window.addstr(self.y, no_x, no_text, attr)

            def handle_key(self, key: int) -> bool:
                if key == curses.KEY_LEFT or key == curses.KEY_RIGHT:
                    self.yes_selected = not self.yes_selected
                    return True
                elif key == ord('\n') or key == ord('\r') or key == curses.KEY_ENTER:
                    if self.yes_selected:
                        if self.on_yes:
                            self.on_yes()
                        self.dialog.hide()
                    else:
                        if self.on_no:
                            self.on_no()
                        self.dialog.hide()
                    return True
                return False
        return YesNo(self, on_yes, on_no)

    def text_entry(self, label: str = ""):
        class TextEntry(Dialog._Item):
            def __init__(self, dialog: Dialog, label: str = ""):
                super().__init__(dialog)
                self.text = ""
                self.label = label
                _, max_x = self.dialog.ui.stdscr.getmaxyx()
                self.min_width = max_x - 10

            @property
            def width(self) -> int:
                _, max_x = self.dialog.ui.stdscr.getmaxyx()

                required_width = len(self.label) + 10
                desired_width = max(len(self.text) + len(self.label), self.min_width)
                return max(min(max_x - 10, desired_width), required_width)

            def draw(self, is_focused: bool) -> None:
                _, win_x = self.window.getmaxyx()
                available_length = win_x - 2

                x = 1
                if self.label:
                    available_length -= len(self.label)
                    x += len(self.label)

                    if is_focused:
                        attr = curses.A_REVERSE | curses.A_BOLD
                    else:
                        attr = 0
                    self.dialog.window.addstr(self.y, 1, self.label, attr)

                if is_focused:
                    attr = curses.A_BOLD
                    display_text = self.text[-(available_length-1):]
                else:
                    attr = 0
                    display_text = self.text[:available_length]
                self.dialog.window.addstr(self.y, x, display_text, attr)
                x += len(display_text)
                available_length -= len(display_text)

                if is_focused and available_length > 0:
                    self.dialog.window.addstr(self.y, x, " ", curses.A_BOLD | curses.A_REVERSE)

            def handle_key(self, key: int) -> bool:
                if key == curses.KEY_BACKSPACE:
                    if len(self.text) > 0:
                        self.text = self.text[:-1]
                    return True
                elif curses.ascii.isprint(key):
                    self.text = self.text + chr(key)
                    return True
        return TextEntry(self, label)

    def __init__(self, ui: "UserInterface"):
        self.ui = ui
        self.window = self.ui.stdscr.subwin(10, 10, 5, 5)
        self.window.attrset(curses.A_BOLD | curses.color_pair(curses.COLOR_YELLOW))
        self._items: typing.List[Dialog._Item] = list()
        self._focus_index: int = -1

        self.title: str = ""

    def draw(self) -> None:
        if not self._items:
            return

        max_y, max_x = self.ui.stdscr.getmaxyx()
        max_y -= 1

        content_y = len(self._items)
        content_x = len(self.title)
        for item in self._items:
            content_x = max(content_x, item.width)

        win_y, win_x = self.window.getmaxyx()
        if content_x + 2 != win_x or content_y + 2 != win_y:
            try:
                self.window.mvwin(0, 0)
                self.window.resize(content_y + 2, content_x + 2)
            except:
                return
            win_y, win_x = self.window.getmaxyx()

        self.window.mvwin((max_y - win_y) // 2, (max_x - win_x) // 2)
        self.window.clear()
        self.window.box()

        if self.title:
            self.window.addstr(0, (win_x - len(self.title)) // 2, self.title)

        for i in range(len(self._items)):
            self._items[i].draw(i == self._focus_index)

        self.window.noutrefresh()

    def hide(self) -> None:
        self.ui._dialog = None
        self.ui._changed.set()

    def _focus_advance(self, direction: int) -> None:
        self._focus_index = self._focus_index % len(self._items)
        if self._items[self._focus_index].focusable:
            return
        for _ in range(1, len(self._items)):
            self._focus_index += direction
            self._focus_index = self._focus_index % len(self._items)
            if self._items[self._focus_index].focusable:
                return
        self._focus_index = -1

    def handle_key(self, key: int) -> None:
        if key == 0x1B or key == 0x17:
            self.hide()
            return
        if not self._items:
            return

        if self._focus_index >= 0 and self._focus_index < len(self._items):
            if self._items[self._focus_index].handle_key(key):
                return

        if key == curses.KEY_UP or key == curses.KEY_PPAGE:
            self._focus_index -= 1
            self._focus_advance(-1)
        elif key == curses.KEY_DOWN or key == curses.KEY_NPAGE:
            self._focus_index += 1
            self._focus_advance(1)
        elif key == curses.KEY_HOME:
            self._focus_index = 0
            self._focus_advance(1)
        elif key == curses.KEY_END:
            self._focus_index = len(self._items) - 1
            self._focus_advance(-1)


class UserInterface:
    class Client(AcquisitionBusClient):
        def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, ui: "UserInterface"):
            super().__init__("_CONSOLE", reader, writer)
            self.ui = ui

        async def incoming_message(self, source: str, record: str, message: typing.Any) -> None:
            source_window = self.ui._sources.get(source)

            if record == 'instrument':
                if not message:
                    if source_window:
                        self.ui._windows.remove(source_window)
                        del self.ui._sources[source]
                        source_window = None
                elif isinstance(message, dict):
                    if not source_window:
                        instrument_type = message.get('type')
                        if instrument_type:
                            source_window = instrument_data(instrument_type, 'window', 'create')(self.ui, source, message)

                        if source_window:
                            self.ui._windows.append(source_window)
                            self.ui._sources[source] = source_window

            if source_window:
                source_window.instrument_message(record, message)

            for win in self.ui._windows:
                if win.global_message(source, record, message):
                    self.ui._changed.set()

    def __init__(self):
        self.client: typing.Optional[UserInterface.Client] = None
        self.stdscr: typing.Optional[curses.window] = None
        self._changed = asyncio.Event()

        self._sources: typing.Dict[str, "InstrumentWindow"] = dict()
        self._windows: typing.Deque["InstrumentWindow"] = deque()
        self._menu: typing.Optional[Menu] = None
        self._dialog: typing.Optional[Dialog] = None

    def _draw_screen(self) -> None:
        now = time.time()
        iso8601 = format_iso8601_time(now)
        doy = format_year_doy(now)
        y, x = self.stdscr.getmaxyx()
        self.stdscr.addstr(y-1, 0, f"UTC: {iso8601} {doy}")

        status_characters: typing.List[typing.Tuple[str, int]] = list()
        for win in self._windows:
            status, attr = win.status_line()
            if not status:
                continue
            status_characters.append((status, attr))
        status_characters.sort(key=lambda s: s[0])
        status_x = x-1
        for status, attr in status_characters:
            status_x -= len(status)
            self.stdscr.addstr(y - 1, status_x, status, attr)

        self.stdscr.noutrefresh()

        for win in reversed(self._windows):
            if not self._menu and win == self._windows[0]:
                win.window.attrset(curses.A_BOLD | curses.color_pair(curses.COLOR_YELLOW))
            else:
                win.window.attrset(curses.color_pair(curses.COLOR_WHITE))
            win.draw()

        if self._dialog:
            self._dialog.draw()

        if self._menu:
            self._menu.draw()

    def _show_message_log_entry(self) -> None:
        dialog = self.show_dialog()
        dialog.title = "Message Log Entry"

        global _message_log_author

        message = dialog.text_entry("Message: ")
        author = dialog.text_entry("Author: ")
        author.text = _message_log_author

        def output_log():
            global _message_log_author

            message_text = message.text.strip()
            if message_text:
                message_author = author.text.strip()

                event = {
                    'type': 'user',
                    'message': message_text,
                }
                if message_author:
                    _message_log_author = message_author
                    event['author'] = message_author

                self.client.send_message(PersistenceLevel.DATA, 'event_log', event)

            dialog.hide()

        dialog.button("OK", output_log)

    def _show_restart_confirm(self) -> None:
        dialog = self.show_dialog()
        dialog.title = "RESTART ACQUISITION SYSTEM"
        dialog.label("System restart requested.")
        dialog.label("If you continue the acquisition system will restart.")
        dialog.label("This will cause a temporary interruption of data collection.")
        dialog.label("Any changes to the configuration will be loaded.")
        confirm = dialog.yes_no(
            on_yes=lambda: self.client.send_message(PersistenceLevel.SYSTEM, 'restart_acquisition', 1)
        )
        confirm.yes_text = "RESTART SYSTEM"
        confirm.no_text = "ABORT"

    def _show_main_menu(self) -> None:
        menu = self.show_menu()

        menu.add_entry("  MESSAGE LOG", self._show_message_log_entry)
        menu.add_entry("  RESTART ACQUISITION", self._show_restart_confirm)

        def window_selected(win: "InstrumentWindow") -> typing.Callable[[], None]:
            def select():
                try:
                    self._windows.remove(win)
                except ValueError:
                    return
                self._windows.insert(0, win)
            return select

        win_entries: typing.List[typing.Tuple[str, typing.Callable[[], None]]] = list()
        for win in self._windows:
            hotkey = win.display_letter
            if not hotkey:
                hotkey = " "
            title = hotkey + " " + win.window_title

            win_entries.append((title, window_selected(win)))
        win_entries.sort(key=lambda x: x[0])
        for e in win_entries:
            menu.add_entry(e[0], e[1], ord(e[0]) if e[0] != " " else None)

    def _handle_key(self, key: int) -> None:
        if self._menu:
            self._menu.handle_key(key)
            return
        if self._dialog:
            self._dialog.handle_key(key)
            return

        if key == ord('\t'):
            win = self._windows[0]
            del self._windows[0]
            self._windows.append(win)
            return
        elif key == curses.KEY_BTAB:
            win = self._windows[-1]
            del self._windows[-1]
            self._windows.insert(0, win)
            return

        for win in self._windows:
            if win.handle_key(key):
                return

        if key == ord('\n') or key == ord('\r') or key == curses.KEY_ENTER:
            self._show_main_menu()

    def show_menu(self) -> Menu:
        if self._menu:
            raise RuntimeError("cannot show two menus at once")
        self._menu = Menu(self)
        self._changed.set()
        return self._menu

    def show_dialog(self) -> Dialog:
        if self._dialog:
            raise RuntimeError("cannot show two dialogs at once")
        self._dialog = Dialog(self)
        self._changed.set()
        return self._dialog

    async def run(self) -> None:
        curses.curs_set(0)
        curses.start_color()
        curses.use_default_colors()
        for c in range(1, curses.COLORS):
            curses.init_pair(c, c, -1)

        self.stdscr.clear()
        self.stdscr.nodelay(True)

        while True:
            self.stdscr.clear()
            self._draw_screen()
            curses.doupdate()

            key = self.stdscr.getch()
            self._changed.clear()

            if key == curses.ERR:
                asyncio.get_event_loop().add_reader(sys.stdin.fileno(), self._changed.set)
                try:
                    await asyncio.wait_for(self._changed.wait(), 0.25)
                except asyncio.TimeoutError:
                    pass
                asyncio.get_event_loop().remove_reader(sys.stdin.fileno())
            elif key == curses.KEY_RESIZE:
                curses.update_lines_cols()
                y, x = self.stdscr.getmaxyx()
                curses.resizeterm(y, x)
            elif key == curses.KEY_EXIT:
                break
            else:
                self._handle_key(key)
