from ..clap.instrument import Instrument as CLAP

_INSTRUMENT_TYPE = __name__.split('.')[-2]


class Instrument(CLAP):
    INSTRUMENT_TYPE = _INSTRUMENT_TYPE
    MANUFACTURER = "BMI"
    MODEL = "TAP"

    DEFAULT_SPOT_SIZE = 20.1
