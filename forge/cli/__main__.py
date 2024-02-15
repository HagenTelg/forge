#!/usr/bin/env python3
import typing
import asyncio
import sys
from forge.cli.arguments import ParseArguments


def main():
    parse = ParseArguments(sys.argv)
    execute = parse()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(execute())
    loop.close()


if __name__ == '__main__':
    main()
