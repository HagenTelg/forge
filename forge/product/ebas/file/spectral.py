import typing
import logging
import netCDF4
import numpy as np
from math import inf, isfinite
from forge.data.dimensions import find_dimension_values
from ebas.domain.masterdata.dc import DCError
from . import EBASFile, DatasetCharacteristicList

_LOGGER = logging.getLogger(__name__)


class SpectralFile(EBASFile):
    WAVELENGTH_BANDS: typing.List[typing.Tuple[float, float]] = [
        (-inf, 500.0),
        (500.0, 600.0),
        (600.0, 750.0),
        (750.0, inf),
    ]

    class SpectralVariable:
        def __init__(self, file: "SpectralFile"):
            self.file = file
            self._band_variables: typing.List[typing.Optional["SpectralFile.SpectralVariable.Variable"]] = [None] * len(file.WAVELENGTH_BANDS)

        class Variable(EBASFile.Variable):
            def __init__(self, file: "SpectralFile"):
                super().__init__(file)
                self.wavelength: float = None

            def set_wavelength(self, wavelength: typing.Union[int, float],
                               instrument_type: str, component: str) -> None:
                try:
                    self.add_characteristic('Wavelength', f'{int(round(wavelength))} nm',
                                            instrument_type, component)
                except DCError:
                    # Notably happens on neph zero components
                    _LOGGER.warning("Error in wavelength parse (EBAS-IO database possibly incorrect), overriding", exc_info=True)
                    if getattr(self.metadata, 'characteristics', None) is None:
                        self.metadata.characteristics = DatasetCharacteristicList()
                    charac = self.metadata.characteristics.__class__.CLIENT_CLASS_ELEMENTS(
                        self.metadata.characteristics.__class__.CLIENT_CLASS_ELEMENTS.setup_dict(
                            'Wavelength', round(wavelength), instrument_type, component))
                    self.metadata.characteristics.append(charac)

            def apply_metadata(
                    self,
                    title: typing.Optional[str] = None,
                    instrument_type: typing.Optional[str] = None,
                    component: typing.Optional[str] = None,
                    **kwargs
            ) -> None:
                if title:
                    self.metadata.title = title.format(wavelength=int(round(self.wavelength)))
                if not instrument_type:
                    instrument_type = self.file.instrument_type
                if not component:
                    component = kwargs['comp_name']
                self.set_wavelength(self.wavelength, instrument_type, component)
                for key, value in kwargs.items():
                    setattr(self.metadata, key, value)

        def __iter__(self) -> typing.Iterator["SpectralFile.SpectralVariable.Variable"]:
            for var in self._band_variables:
                if var is None:
                    continue
                yield var

        def iter_spectral(self) -> typing.Iterator[typing.Tuple["SpectralFile.SpectralVariable.Variable", float, int]]:
            for band_idx in range(len(self._band_variables)):
                var = self._band_variables[band_idx]
                if var is None:
                    continue
                yield var, var.wavelength, band_idx

        def _to_wavelength_band_index(self, wavelength: float) -> typing.Optional[int]:
            for band_idx in range(len(self.file.WAVELENGTH_BANDS)):
                band = self.file.WAVELENGTH_BANDS[band_idx]
                if band[0] <= wavelength <= band[1]:
                    return band_idx
            return None

        def _to_band_variable(self, band_idx: int) -> "SpectralFile.SpectralVariable.Variable":
            existing = self._band_variables[band_idx]
            if existing is None:
                existing = self.Variable(self.file)
                self._band_variables[band_idx] = existing
            return existing

        def integrate_variable(
                self,
                var: netCDF4.Variable,
                selector: typing.Optional[typing.Dict[str, typing.Union[slice, int, np.ndarray]]] = None,
                converter: typing.Callable[[np.ndarray], np.ndarray] = None,
                wavelength_converter: typing.Callable[[float], typing.Optional[typing.Callable[[np.ndarray], np.ndarray]]] = None,
                allow_constant: bool = False,
                extra_vars: typing.List[typing.Union[netCDF4.Variable, np.ndarray]] = None,
        ) -> None:
            if 'wavelength' not in var.dimensions:
                if 'wavelength' not in getattr(var, 'ancillary_variables', "").split():
                    return
                wavelength_var = var.group().variables.get('wavelength')
                if wavelength_var is None:
                    return
                if len(wavelength_var.shape) != 0:
                    return
                wavelength = float(wavelength_var[0])
                if not isfinite(wavelength):
                    return
                band_idx = self._to_wavelength_band_index(wavelength)
                if band_idx is None:
                    return

                if wavelength_converter is not None:
                    converter = wavelength_converter(wavelength)

                band_var = self._to_band_variable(band_idx)
                band_var.integrate_variable(
                    var,
                    selector=selector,
                    converter=converter,
                    allow_constant=allow_constant,
                    extra_vars=extra_vars,
                )
                band_var.wavelength = wavelength
                return

            _, wavelength_var = find_dimension_values(var.group(), 'wavelength')
            wavelength_values = wavelength_var[:].data

            assert len(wavelength_values.shape) == 1
            hit_bands: typing.Set[int] = set()
            for wlidx in range(wavelength_values.shape[0]):
                wavelength = float(wavelength_values[wlidx])
                if not isfinite(wavelength):
                    continue
                band_idx = self._to_wavelength_band_index(wavelength)
                if band_idx is None:
                    continue
                if band_idx in hit_bands:
                    continue
                hit_bands.add(band_idx)

                wl_selector = dict()
                if selector:
                    wl_selector.update(selector)
                wl_selector['wavelength'] = wlidx

                if wavelength_converter is not None:
                    converter = wavelength_converter(wavelength)

                band_var = self._to_band_variable(band_idx)
                band_var.integrate_variable(
                    var,
                    selector=wl_selector,
                    converter=converter,
                    allow_constant=allow_constant,
                    extra_vars=extra_vars,
                )
                band_var.wavelength = wavelength

    class MatrixData(EBASFile.MatrixData):
        class Context(EBASFile.MatrixData.Context):
            def iter_spectral(self) -> typing.Iterator[typing.Tuple["SpectralFile.SpectralVariable.Variable", float, int]]:
                for spectral in self:
                    yield from spectral.iter_spectral()

            def __iter__(self):
                for v in super().__iter__():
                    if isinstance(v, SpectralFile.SpectralVariable):
                        yield from v.__iter__()
                    else:
                        yield v

        def __init__(self, files: "SpectralFile"):
            super().__init__(files)
            self.files = files

        def spectral_variable(self) -> "SpectralFile.MatrixData.Context":
            return self.context(self.files.spectral_variable)

    def spectral_variable(self) -> "SpectralFile.SpectralVariable":
        return self.SpectralVariable(self)

    def to_wavelength_band(self, wavelength: typing.Union[int, float]) -> typing.Optional[int]:
        for i in range(len(self.WAVELENGTH_BANDS)):
            start, end = self.WAVELENGTH_BANDS[i]
            if not isfinite(wavelength):
                if not isfinite(end):
                    return i
                continue
            if start <= wavelength < end:
                return i
        return None