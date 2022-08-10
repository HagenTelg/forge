import typing
import asyncio
import logging
import argparse
import curses
import signal
from forge.acquisition.bus.client import AcquisitionBusClient
from forge.acquisition import CONFIGURATION
from .ui import UserInterface



def main():
    parser = argparse.ArgumentParser(description="Acquisition bus test client.")
    parser.add_argument('--socket',
                        dest='socket', type=str,
                        default=CONFIGURATION.get('ACQUISITION.BUS', '/run/forge-acquisition-bus.socket'),
                        help="server socket")
    args = parser.parse_args()

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    user_interface = UserInterface()

    def run(stdscr) -> None:
        user_interface.stdscr = stdscr

        async def start():
            reader, writer = await asyncio.open_unix_connection(args.socket)
            user_interface.client = UserInterface.Client(reader, writer, user_interface)
            await user_interface.client.start()

        loop.run_until_complete(start())

        user_interface_run = loop.create_task(user_interface.run())
        loop.add_signal_handler(signal.SIGINT, user_interface_run.cancel)
        loop.add_signal_handler(signal.SIGTERM, user_interface_run.cancel)

        try:
            loop.run_until_complete(user_interface_run)
        except asyncio.CancelledError:
            pass

    curses.wrapper(run)
    if user_interface.client:
        loop.run_until_complete(user_interface.client.shutdown())


if __name__ == '__main__':
    main()
