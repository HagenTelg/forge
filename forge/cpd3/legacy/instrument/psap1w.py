import typing
from .psap3w import Converter as PSAP3Converter


class Converter(PSAP3Converter):
    WAVELENGTHS = [
        (574.0, "G"),
    ]

    @property
    def tags(self) -> typing.Optional[typing.Set[str]]:
        return {"aerosol", "absorption", "psap1w"}

    @property
    def instrument_type(self) -> typing.Optional[str]:
        return "psap1w"

    def apply_instrument_metadata(self, *args,
                                  model: typing.Optional[str] = None, **kwargs):
        if model:
            model = "PSAP-1W"
        return super().apply_instrument_metadata(*args, model=model, **kwargs)