def main():
    from ..run import run, instrument_config, data_output, bus_interface, persistent_interface
    from ..streaming import arguments

    args = arguments()
    args = args.parse_args()
    bus = bus_interface(args)
    data = data_output(args)
    persistent = persistent_interface(args)
    instrument_config = instrument_config(args)

    if bool(instrument_config.get("URL", default=False)):
        from ..http import HttpContext, configure_context as configure_http_context
        from .instrument import InstrumentUIDEP

        ctx = HttpContext(instrument_config, data, bus, persistent)
        configure_http_context(args, ctx, InstrumentUIDEP, instrument_config)

        instrument = InstrumentUIDEP(ctx)
    else:
        from ..streaming import create_context as create_streaming_context
        from ..run import configure_context as configure_streaming_context
        from .instrument import InstrumentADP

        ctx = create_streaming_context(args, InstrumentADP, instrument_config, data, bus, persistent)
        configure_streaming_context(args, ctx)

        instrument = InstrumentADP(ctx)

    ctx.persistent.version = instrument.PERSISTENT_VERSION
    run(instrument, args.systemd)
