#!/usr/bin/env python3

import argparse
import logging
import uvicorn
import os
from . import CONFIGURATION
from .server import app


def parse_arguments():
    parser = argparse.ArgumentParser(description="Forge dashboard backend web server.")

    group = parser.add_mutually_exclusive_group()
    group.add_argument('--bind',
                       dest="bind_address",
                       help="the IP address to listen for connections on")
    parser.add_argument('--port',
                        dest="port", type=int, default=8000,
                        help="TCP port to listen for connections on")
    group.add_argument('--unix',
                       dest="bind_unix",
                       help="Unix socket to listen for connections on")

    parser.add_argument('--workers',
                        dest="workers", type=int, default=1,
                        help="number of server workers")

    parser.add_argument('--debug',
                        dest='debug', action='store_true',
                        help="enable debug output")

    return parser.parse_args()


def main():
    args = parse_arguments()
    log_level = 'info'
    log_config = uvicorn.config.LOGGING_CONFIG
    if args.debug:
        log_config['loggers'][''] = {"handlers": ["default"], "level": "DEBUG"}
        log_config['loggers']['uvicorn']['propagate'] = False
        app.debug = True
        CONFIGURATION.DEBUG = True
        log_level = 'debug'

    if args.bind_unix is not None:
        try:
            os.unlink(args.bind_unix)
        except OSError:
            pass
        uvicorn.run('forge.dashboard.server:app', workers=args.workers, access_log=args.debug,
                    uds=args.bind_unix, log_level=log_level, log_config=log_config)
    elif args.bind_address is not None:
        uvicorn.run('forge.dashboard.server:app', workers=args.workers, access_log=args.debug,
                    host=args.bind_address, port=args.port, log_level=log_level, log_config=log_config)
    else:
        uvicorn.run('forge.dashboard.server:app', workers=args.workers, access_log=args.debug,
                    port=args.port, log_level=log_level, log_config=log_config)


if __name__ == '__main__':
    main()
