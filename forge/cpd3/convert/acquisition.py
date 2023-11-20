import typing
import argparse
import logging
import sys
from netCDF4 import Dataset
from forge.cpd3.identity import Identity
from forge.cpd3.datawriter import StandardDataOutput
from forge.cpd3.convert.station.lookup import station_data


_LOGGER = logging.getLogger(__name__)


class OutputDataWriter(StandardDataOutput):
    def __init__(self, target):
        super().__init__()
        self.target = target
        self.target.write(self.DIRECT_HEADER)

    def output_ready(self, packet: bytes) -> None:
        self.target.write(self.direct_encode(packet))


def main():
    parser = argparse.ArgumentParser(description="Forge acquisition to CPD3 data converter.")

    parser.add_argument('--debug',
                        dest='debug', action='store_true',
                        help="enable debug output")
    parser.add_argument('--output',
                        help="output file")
    parser.add_argument('--station',
                        dest='station',
                        help="allowed station",
                        action='append')
    parser.add_argument('file',
                        help="input NetCDF file")

    args = parser.parse_args()
    if args.debug:
        from forge.log import set_debug_logger
        set_debug_logger()

    _LOGGER.debug(f"Loading {args.file}")
    root = Dataset(args.file, 'r')

    station = root.variables["station_name"][0]
    if not isinstance(station, str) or len(station) < 3:
        raise ValueError(f"input file contains an invalid station: {station}")
    station = station.lower()
    if args.station:
        for check in args.station:
            check = check.lower()
            if station == check:
                break
        else:
            parser.error(f"input station {station} not specified with --station")
            exit(1)

    _LOGGER.debug(f"Converting data")
    converted: typing.List[typing.Tuple[Identity, typing.Any]] = station_data(station, 'acquisition', 'convert')(
        station, root
    )

    _LOGGER.debug(f"Sorting data")
    converted.sort(key=lambda x: x[0].start if x[0].start is not None else sys.float_info.min)

    output_file = sys.stdout.buffer
    if args.output and args.output != '-':
        output_file = open(args.output, 'wb')
        _LOGGER.debug(f"Writing output to {args.output}")
    else:
        _LOGGER.debug("Writing output to stdout")

    output = OutputDataWriter(output_file)
    for identity, value in converted:
        output.incoming_value(identity, value)
    output.finish()
