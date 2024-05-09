import typing
from .clap import Converter as CLAPConverter


class Converter(CLAPConverter):
    @property
    def tags(self) -> typing.Optional[typing.Set[str]]:
        return {"aerosol", "absorption", "bmitap"}

    @property
    def instrument_type(self) -> typing.Optional[str]:
        return "bmitap"

    def apply_instrument_metadata(self, *args,
                                  manufacturer: typing.Optional[str] = None,
                                  model: typing.Optional[str] = None, **kwargs):
        if manufacturer:
            manufacturer = "BMI"
        if model:
            model = "TAP"
        return super().apply_instrument_metadata(*args, manufacturer=manufacturer, model=model, **kwargs)