
def main():
    from .instrument import Instrument
    from ..run import run, arguments, average_config, instrument_config, cutsize_config, \
        data_output, bus_interface, persistent_interface
    from ..base import BaseContext

    args = arguments()
    args = args.parse_args()
    bus = bus_interface(args)
    data = data_output(args)
    persistent = persistent_interface(args)
    instrument_config = instrument_config(args)
    ctx = BaseContext(instrument_config, data, bus, persistent)
    ctx.average_config = average_config(args)
    ctx.cutsize_config = cutsize_config(args)
    run(Instrument(ctx), args.systemd)
