import typing
import argparse
import logging
import socket
import struct
from forge.acquisition.serial.multiplexer.protocol import ControlOperation, Parity


def main():
    parser = argparse.ArgumentParser(description="Acquisition serial control client.")

    parser.add_argument('--debug',
                        dest='debug', action='store_true',
                        help="enable debug output")

    parser.add_argument('socket',
                        help="control socket path")

    subparsers = parser.add_subparsers(dest='command')

    command_parser = subparsers.add_parser('baud',
                                           help="set the serial port baud rate")
    command_parser.add_argument('baud',
                                type=int,
                                help="the baud rate to set")

    command_parser = subparsers.add_parser('bits',
                                           help="set the serial port data bits")
    command_parser.add_argument('databits',
                                type=int,
                                help="the number of bits in a data byte")

    command_parser = subparsers.add_parser('parity',
                                           help="set the serial port parity")
    command_parser.add_argument('parity',
                                choices=['none', 'even', 'odd', 'mark', 'space'],
                                help="the data byte parity bit")

    command_parser = subparsers.add_parser('stop',
                                           help="set the serial port stop bits")
    command_parser.add_argument('stopbits',
                                type=int,
                                help="the number of bits after a data byte")

    command_parser = subparsers.add_parser('rs485',
                                           help="set or disable RS-485 mode")
    modeparser = command_parser.add_subparsers(dest='mode')
    modeparser.add_parser('disable',
                          help="disable RS485 mode")
    command_parser = modeparser.add_parser('enable',
                                           help="enable RS485 mode")
    command_parser.add_argument('--loopback',
                                dest='loopback', action='store_true',
                                help="enable loopback (transmitted data also received)")
    command_parser.add_argument('--no-rts-for-tx',
                                dest='rts_for_tx', action='store_false',
                                help="disable RTS when transmitting data")
    command_parser.set_defaults(rts_for_tx=True)
    command_parser.add_argument('--rts-for-rx',
                                dest='rts_for_rx', action='store_true',
                                help="enable RTS when receiving data")
    command_parser.set_defaults(rts_for_rx=False)
    command_parser.add_argument('--before-tx',
                                dest='before_tx', type=float, default=0.0,
                                help="delay in seconds before transmitting")
    command_parser.add_argument('--before-rx',
                                dest='before_rx', type=float, default=0.0,
                                help="delay in seconds before receiving")

    command_parser = subparsers.add_parser('rts',
                                           help="set the serial port RTS line")
    command_parser.add_argument('state',
                                choices=['assert', 'clear'],
                                help="the RTS line state")

    command_parser = subparsers.add_parser('dtr',
                                           help="set the serial port DTR line")
    command_parser.add_argument('state',
                                choices=['assert', 'clear'],
                                help="the DTR line state")

    subparsers.add_parser('flush',
                          help="flush the serial port")
    subparsers.add_parser('break',
                          help="send a break to the serial port")
    subparsers.add_parser('reopen',
                          help="close and re-open the serial port")

    args = parser.parse_args()
    if args.debug:
        root_logger = logging.getLogger()
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(name)-40s %(message)s')
        handler.setFormatter(formatter)
        root_logger.setLevel(logging.DEBUG)
        root_logger.addHandler(handler)

    control = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)

    if args.command == 'baud':
        control.sendto(struct.pack('<BI', ControlOperation.SET_BAUD.value, args.baud), args.socket)
    elif args.command == 'bits':
        control.sendto(struct.pack('<BB', ControlOperation.SET_DATA_BITS.value, args.databits), args.socket)
    elif args.command == 'parity':
        if args.parity == 'none':
            parity = Parity.NONE
        elif args.parity == 'even':
            parity = Parity.EVEN
        elif args.parity == 'odd':
            parity = Parity.ODD
        elif args.parity == 'mark':
            parity = Parity.MARK
        elif args.parity == 'space':
            parity = Parity.SPACE
        else:
            raise ValueError
        control.sendto(struct.pack('<BB', ControlOperation.SET_PARITY.value, parity.value), args.socket)
    elif args.command == 'stop':
        control.sendto(struct.pack('<BB', ControlOperation.SET_STOP_BITS.value, args.stopbits), args.socket)
    elif args.command == 'rs485':
        if args.mode == 'disable':
            control.sendto(struct.pack('<BB', ControlOperation.SET_RS485.value, 0), args.socket)
        else:
            control.sendto(struct.pack('<BBBBBff', ControlOperation.SET_RS485.value, 1,
                                       args.rts_for_tx and 1 or 0,
                                       args.rts_for_rx and 1 or 0,
                                       args.loopback and 1 or 0,
                                       args.before_tx and args.before_tx or 0,
                                       args.before_rx and args.before_rx or 0,
                                       ), args.socket)
    elif args.command == 'rts':
        control.sendto(struct.pack('<BB', ControlOperation.SET_RTS.value, args.state == 'assert'), args.socket)
    elif args.command == 'dtr':
        control.sendto(struct.pack('<BB', ControlOperation.SET_DTR.value, args.state == 'assert'), args.socket)
    elif args.command == 'flush':
        control.sendto(struct.pack('<B', ControlOperation.FLUSH.value), args.socket)
    elif args.command == 'break':
        control.sendto(struct.pack('<B', ControlOperation.BREAK.value), args.socket)
    elif args.command == 'reopen':
        control.sendto(struct.pack('<B', ControlOperation.REOPEN.value), args.socket)

    control.close()


if __name__ == '__main__':
    main()