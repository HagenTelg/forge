import typing
import asyncio
from forge.product.selection import InstrumentSelection

if typing.TYPE_CHECKING:
    from ebas.io.file import nasa_ames
    from .spectral import EBASFile


class AerosolInstrument:
    @property
    def instrument_selection(self) -> typing.Iterable[InstrumentSelection]:
        raise NotImplementedError

    @classmethod
    def with_instrument_selection(cls, selection: typing.Iterable[InstrumentSelection]) -> typing.Type["EBASFile"]:
        class Result(cls):
            @property
            def instrument_selection(self) -> typing.Iterable[InstrumentSelection]:
                return selection

        return Result

    @property
    def matrix_to_inlet(self) -> typing.Dict[str, typing.Tuple[str, typing.Optional[str]]]:
        return {
            'pm1': ('Impactor--direct', 'Switched impactor at 1 um'),
            'pm10': ('Impactor--direct', 'Switched impactor at 10 um'),
            'pm25': ('Cyclone', 'Cyclone size selection at 2.5um aerodynamic diameter'),
            'aerosol': ('Hat or hood', None),
        }

    @classmethod
    def with_inlet(
            cls,
            inlet: typing.Union[str, typing.Tuple[str, typing.Optional[str]], typing.Dict[str, typing.Tuple[str, typing.Optional[str]]]]
    ) -> typing.Type["EBASFile"]:
        if isinstance(inlet, str):
            inlet = (inlet, None)
        if isinstance(inlet, tuple):
            inlet = {
                'pm1': inlet,
                'pm10': inlet,
                'pm25': inlet,
                'aerosol': inlet,
            }

        class Result(cls):
            @property
            def matrix_to_inlet(self) -> typing.Dict[str, typing.Tuple[str, typing.Optional[str]]]:
                return inlet

        return Result

    def apply_inlet(self, nas: "nasa_ames.EbasNasaAmes") -> None:
        inlet_type, inlet_desc = self.matrix_to_inlet.get(nas.metadata.matrix)
        if inlet_type is None:
            return
        if getattr(nas.metadata, 'inlet_type', None) is None:
            nas.metadata.inlet_type = inlet_type
        if inlet_desc is not None and getattr(nas.metadata, 'inlet_desc', None) is None:
            if not inlet_desc:
                nas.metadata.inlet_desc = None
            else:
                nas.metadata.inlet_desc = inlet_desc
