import typing
import asyncio
import logging
from math import isfinite, floor, log10, pow, nan
from forge.acquisition import LayeredConfiguration
from forge.units import ONE_ATM_IN_HPA
from ..streaming import CommunicationsError
from ..base import BaseDataOutput
from ..dimension import Dimension

_LOGGER = logging.getLogger(__name__)


def _round_exponent(d: typing.Optional[float], digits: int = 3) -> typing.Optional[float]:
    if d is None or not isfinite(d):
        return d
    m = abs(d)
    if m == 0.0:
        return 0.0
    base = floor(log10(m))
    base -= digits
    m = pow(10.0, base)
    return round(d / m) * m


def _format_exponent(value: float, decimals: int = 3, exponent: int = 1) -> bytes:
    raw = (b'%.' + (b'%d' % decimals) + b'e') % value
    (before, after) = raw.split(b'e')
    evalue = int(after[1:])
    return before + b'e' + after[:1] + (b'%d' % evalue).rjust(exponent, b'0')


class Parameters:
    class SK:
        def __init__(self, K1: typing.Optional[int] = None, K2: typing.Optional[float] = None,
                     K3: typing.Optional[float] = None, K4: typing.Optional[float] = None):
            self.K1 = K1
            self.K2 = K2
            self.K3 = K3
            self.K4 = K4

        @property
        def K1(self) -> typing.Optional[int]:
            return self._K1

        @K1.setter
        def K1(self, value: typing.Optional[int]) -> None:
            if value is not None and (value < 0 or value > 65535):
                value = None
            self._K1 = value

        @property
        def K2(self) -> typing.Optional[float]:
            return self._K2

        @K2.setter
        def K2(self, value: typing.Optional[float]) -> None:
            value = _round_exponent(value, 3)
            if value is not None and value < 0.0:
                value = None
            self._K2 = value

        @property
        def K3(self) -> typing.Optional[float]:
            return self._K3

        @K3.setter
        def K3(self, value: typing.Optional[float]) -> None:
            value = _round_exponent(value, 3)
            if value is not None and value < 0.0:
                value = None
            self._K3 = value

        @property
        def K4(self) -> typing.Optional[float]:
            return self._K4

        @K4.setter
        def K4(self, value: typing.Optional[float]) -> None:
            if value is None or not isfinite(value):
                self._K4 = None
                return
            value = round(value, 3)
            if value < 0.0 or value > 1.0:
                value = None
            self._K4 = value

        def persistent(self) -> typing.Dict[str, typing.Any]:
            result: typing.Dict[str, typing.Any] = dict()
            result['K1'] = self.K1
            result['K2'] = self.K2
            result['K3'] = self.K3
            result['K4'] = self.K4
            return result

        def load(self, config: typing.Optional[typing.Union[typing.List, typing.Dict[str, typing.Any]]]) -> bool:
            if config is None:
                return False
            if isinstance(config, list):
                changed = False

                if len(config) > 0:
                    K1 = config[0]
                    if K1 is not None:
                        try:
                            self.K1 = int(K1)
                            changed = True
                        except (TypeError, ValueError):
                            pass

                if len(config) > 1:
                    K2 = config[1]
                    if K2 is not None:
                        try:
                            K2 = float(K2)
                            if isfinite(K2):
                                self.K2 = K2
                                changed = True
                        except (TypeError, ValueError):
                            pass

                if len(config) > 2:
                    K3 = config[2]
                    if K3 is not None:
                        try:
                            K3 = float(K3)
                            if isfinite(K3):
                                self.K3 = K3
                                changed = True
                        except (TypeError, ValueError):
                            pass

                if len(config) > 3:
                    K4 = config[2]
                    if K4 is not None:
                        try:
                            K4 = float(K4)
                            if isfinite(K4):
                                self.K4 = K4
                                changed = True
                        except (TypeError, ValueError):
                            pass

                return changed
            if not isinstance(config, dict):
                return False

            changed = False

            K1 = config.get("K1")
            if K1 is not None:
                try:
                    self.K1 = int(K1)
                    changed = True
                except (TypeError, ValueError):
                    pass

            K2 = config.get("K2")
            if K2 is not None:
                try:
                    K2 = float(K2)
                    if isfinite(K2):
                        self.K2 = K2
                        changed = True
                except (TypeError, ValueError):
                    pass

            K3 = config.get("K3")
            if K3 is not None:
                try:
                    K3 = float(K3)
                    if isfinite(K3):
                        self.K3 = K3
                        changed = True
                except (TypeError, ValueError):
                    pass

            K4 = config.get("K4")
            if K4 is not None:
                try:
                    K4 = float(K4)
                    if isfinite(K4):
                        self.K4 = K4
                        changed = True
                except (TypeError, ValueError):
                    pass

            return changed

        def overlay(self, other: "Parameters.SK") -> None:
            for name in ("_K1", "_K2", "_K3", "_K4"):
                top = getattr(other, name, None)
                if top is None:
                    continue
                setattr(self, name, top)

        @classmethod
        def default(cls, K3: float = 1.226e-5) -> "Parameters.SK":
            return cls(20000, 4.000e-3, K3, 0.5)

        @classmethod
        def read(cls, value: bytes) -> "Parameters.SK":
            fields = value.split(b',')
            try:
                (K1, K2, K3, K4) = fields
            except ValueError:
                raise CommunicationsError("invalid SK fields")

            try:
                K1 = int(K1)
                K2 = float(K2)
                K3 = float(K3)
                K4 = float(K4)
                return cls(K1, K2, K3, K4)
            except (ValueError, OverflowError):
                raise CommunicationsError("invalid number in SK values")

        @classmethod
        def write(cls, value: "Parameters.SK", default_K3: float = 1.226e-5) -> bytes:
            K1 = value.K1
            if K1 is None:
                K1 = 20000
            K1 = b'%d' % K1

            K2 = value.K2
            if K2 is None:
                K2 = 4.000e-3
            K2 = _format_exponent(K2)

            K3 = value.K3
            if K3 is None:
                K3 = default_K3
            K3 = _format_exponent(K3)

            K4 = value.K4
            if K4 is None:
                K4 = 0.5
            K4 = b'%.3f' % K4

            return b','.join([K1, K2, K3, K4])

        def __str__(self):
            return ",".join([
                str(self.K1) if self.K1 else '',
                f"{self.K2:.3e}" if self.K2 else '',
                f"{self.K3:.3e}" if self.K3 else '',
                f"{self.K4:.3f}" if self.K4 else '',
            ])

        def __eq__(self, other):
            if not isinstance(other, Parameters.SK):
                return False
            return self.K1 == other.K1 and self.K2 == other.K2 and self.K3 == other.K3 and self.K4 == other.K4

        def __ne__(self, other):
            return not self.__eq__(other)

    def __init__(self, **kwargs):
        self.SKB: typing.Optional[Parameters.SK] = kwargs.get("SKB")
        self.SKG: typing.Optional[Parameters.SK] = kwargs.get("SKG")
        self.SKR: typing.Optional[Parameters.SK] = kwargs.get("SKR")
        self.SMZ: typing.Optional[int] = kwargs.get("SMZ")
        self.SP: typing.Optional[int] = kwargs.get("SP")
        self.STA: typing.Optional[int] = kwargs.get("STA")
        self.STB: typing.Optional[int] = kwargs.get("STB")
        self.STP: typing.Optional[int] = kwargs.get("STP")
        self.STZ: typing.Optional[int] = kwargs.get("STZ")
        self.SVB: typing.Optional[int] = kwargs.get("SVB")
        self.SVG: typing.Optional[int] = kwargs.get("SVG")
        self.SVR: typing.Optional[int] = kwargs.get("SVR")
        self.B: typing.Optional[int] = kwargs.get("B")
        self.H: typing.Optional[bool] = kwargs.get("H")
        self.SMB: typing.Optional[bool] = kwargs.get("SMB")
        self.SL: typing.Optional[str] = kwargs.get("SL")

    @classmethod
    def default(cls) -> "Parameters":
        return cls(
            SMZ=1,
            STA=1,
            STB=62,
            STP=32000,
            STZ=300,
            SMB=True,
            SP=75,
            B=255,
        )

    def overlay(self, other: "Parameters") -> None:
        for name in ("SMZ", "SP", "STA", "STB", "STP", "STZ", "SVB", "SVR", "B", "H", "SMB", "SL"):
            top = getattr(other, name, None)
            if top is None:
                continue
            setattr(self, name, top)
        for name, K3 in (("SKB", 2.789e-5), ("SKG", 1.226e-5), ("SKR", 4.605e-6)):
            top = getattr(other, name, None)
            if top is None:
                continue
            bottom = getattr(self, name, None)
            if bottom is None:
                bottom = self.SK.default(K3)
                setattr(self, name, bottom)
            bottom.overlay(top)

    def K1(self, wavelength: int) -> typing.Optional[int]:
        if wavelength == 0:
            if self.SKB is None:
                return None
            return self.SKB.K1
        elif wavelength == 1:
            if self.SKG is None:
                return None
            return self.SKG.K1
        elif wavelength == 2:
            if self.SKR is None:
                return None
            return self.SKR.K1
        raise ValueError

    def record(self, target: BaseDataOutput.ConstantRecord, wavelengths: Dimension) -> None:
        class _Integer(BaseDataOutput.UnsignedInteger):
            def __init__(self, parameters: Parameters, name: str, attributes: typing.Dict[str, typing.Any] = None):
                super().__init__(name)
                self.template = BaseDataOutput.Field.Template.METADATA
                self.parameters = parameters
                if attributes:
                    self.attributes.update(attributes)

            @property
            def value(self) -> int:
                return getattr(self.parameters, self.name, None) or 0

        class _Boolean(BaseDataOutput.UnsignedInteger):
            def __init__(self, parameters: Parameters, name: str, attributes: typing.Dict[str, typing.Any] = None):
                super().__init__(name)
                self.template = BaseDataOutput.Field.Template.METADATA
                self.parameters = parameters
                self.attributes['C_format'] = "%1llu"
                if attributes:
                    self.attributes.update(attributes)

            @property
            def value(self) -> int:
                return (getattr(self.parameters, self.name, None) and 1) or 0

        class _Wavelengths(BaseDataOutput.ArrayFloat):
            def __init__(self, parameters: Parameters, wavelengths: Dimension,
                         fetch: typing.Callable[["Parameters"], typing.List[float]],
                         name: str, attributes: typing.Dict[str, typing.Any] = None):
                super().__init__(name)
                self.template = BaseDataOutput.Field.Template.METADATA
                self.parameters = parameters
                self.wavelengths = wavelengths
                self.fetch = fetch
                if attributes:
                    self.attributes.update(attributes)

            @property
            def value(self) -> typing.List[float]:
                v = self.fetch(self.parameters)
                for i in range(len(v)):
                    if v[i] is None:
                        v[i] = nan
                return v 

            @property
            def dimensions(self) -> typing.Optional[typing.List[BaseDataOutput.ArrayFloat]]:
                return [self.wavelengths.data]

        target.constants.append(_Integer(self, "SMZ", {
            'long_name': "zero mode: 0=manual only, 1-24=autozero with average of last N zeros",
            'C_format': "%2llu",
        }))
        target.constants.append(_Integer(self, "SP", {
            'long_name': "lamp power",
            'units': "W",
            'C_format': "%2llu",
        }))
        target.constants.append(_Integer(self, "STA", {
            'long_name': "averaging time",
            'units': "s",
            'C_format': "%4llu",
        }))
        target.constants.append(_Integer(self, "STB", {
            'long_name': "blanking time",
            'units': "s",
            'C_format': "%3llu",
        }))
        target.constants.append(_Integer(self, "STP", {
            'long_name': "autozero interval",
            'units': "s",
            'C_format': "%5llu",
        }))
        target.constants.append(_Integer(self, "STZ", {
            'long_name': "zero measurement length",
            'units': "s",
            'C_format': "%4llu",
        }))
        target.constants.append(_Wavelengths(self, wavelengths, lambda p: [p.SVB, p.SVG, p.SVR], "SV", {
            'long_name': "photomultiplier tube voltage",
            'units': "V",
            'C_format': "%4.0f",
        }))
        target.constants.append(_Integer(self, "B", {
            'long_name': "blower power (0-255)",
            'C_format': "%3llu",
        }))
        target.constants.append(_Boolean(self, "H", {
            'long_name': "heater enable",
        }))
        target.constants.append(_Boolean(self, "SMB", {
            'long_name': "backscatter shutter enable",
        }))
        target.constants.append(_Wavelengths(self, wavelengths, lambda p: [
            p.SKB.K1 if p.SKB else None, p.SKG.K1 if p.SKG else None, p.SKR.K1 if p.SKR else None
        ], "K1", {
            'long_name': "photomultiplier tube dead time",
            'units': "ps",
            'C_format': "%5.0f",
        }))
        target.constants.append(_Wavelengths(self, wavelengths, lambda p: [
            p.SKB.K2 if p.SKB else None, p.SKG.K2 if p.SKG else None, p.SKR.K2 if p.SKR else None
        ], "K2", {
            'long_name': "total scattering calibration",
            'units': "m-1",
            'C_format': "%.3e",
        }))
        target.constants.append(_Wavelengths(self, wavelengths, lambda p: [
            p.SKB.K3 if p.SKB else None, p.SKG.K3 if p.SKG else None, p.SKR.K3 if p.SKR else None
        ], "K3", {
            'long_name': "air Rayleigh scattering",
            'units': "m-1",
            'C_format': "%.3e",
            'ancillary_variables': "standard_temperature standard_pressure",
        }))
        target.constants.append(_Wavelengths(self, wavelengths, lambda p: [
            p.SKB.K4 if p.SKB else None, p.SKG.K4 if p.SKG else None, p.SKR.K4 if p.SKR else None
        ], "K4", {
            'long_name': "backscattering Rayleigh contribution fraction",
            'C_format': "%5.3f",
        }))

        target.standard_temperature = 0.0
        target.standard_pressure = ONE_ATM_IN_HPA

    def persistent(self) -> typing.Dict[str, typing.Any]:
        result: typing.Dict[str, typing.Any] = dict()
        for name in ("SMZ", "SP", "STA", "STB", "STP", "STZ", "SVB", "SVG", "SVR", "B", "H", "SMB", "SL"):
            value = getattr(self, name, None)
            if value is None:
                continue
            result[name] = value
        for name in ("SKB", "SKG", "SKR"):
            value = getattr(self, name, None)
            if value is None:
                continue
            result[name] = value.persistent()
        return result

    def load(self, config: typing.Union[LayeredConfiguration, typing.Dict[str, typing.Any]]) -> None:
        def set_if_valid(name, converter):
            v = config.get(name)
            if v is None:
                return
            try:
                v = converter(v)
            except (ValueError, TypeError):
                return
            setattr(self, name, v)

        def set_in_range(name, converter, minimum, maximum):
            def limited(v):
                v = converter(v)

                if minimum is not None and v < minimum:
                    return None
                if maximum is not None and v > maximum:
                    return None
                return v

            set_if_valid(name, limited)

        set_in_range("SMZ", int, 0, 24)
        set_in_range("SP", int, 0, 150)
        set_in_range("STA", int, 1, 9960)
        set_in_range("STB", int, 15, 999)
        set_in_range("STP", int, 10, 32767)  # Manual says 9999, but it actually goes higher
        set_in_range("STZ", int, 1, 9999)
        set_in_range("SVB", int, 800, 1200)
        set_in_range("SVG", int, 800, 1200)
        set_in_range("SVR", int, 800, 1200)
        set_in_range("B", int, 0, 255)
        set_if_valid("H", bool)
        set_if_valid("SMB", bool)
        set_if_valid("SL", str)

        def set_calibration(name):
            c = config.get(name)
            value = getattr(self, name, None)
            if value is None:
                value = self.SK()
            if not value.load(c):
                return
            setattr(self, name, value)

        set_calibration("SKB")
        set_calibration("SKG")
        set_calibration("SKR")

    async def read(self, reader: typing.Callable[[bytes], typing.Awaitable[bytes]]) -> None:
        async def read_value(name, converter):
            v = await reader(name.encode('ascii'))
            try:
                v = converter(v)
            except (ValueError, TypeError, UnicodeError):
                raise CommunicationsError(f"error reading {name}")
            setattr(self, name, v)

        await read_value("SMZ", int)
        await read_value("SP", int)
        await read_value("STA", int)
        await read_value("STB", int)
        await read_value("STP", int)
        await read_value("STZ", int)
        await read_value("SVB", int)
        await read_value("SVG", int)
        await read_value("SVR", int)
        await read_value("B", int)

        def is_true(value):
            return int(value) != 0
        await read_value("H", is_true)
        await read_value("SMB", is_true)

        def convert_label(value):
            try:
                return value.decode("utf-8")
            except UnicodeError:
                return value.decode("ascii")
        await read_value("SL", convert_label)

        await read_value("SKB", self.SK.read)
        await read_value("SKG", self.SK.read)
        await read_value("SKR", self.SK.read)

    async def apply_changes(self, changes: "Parameters",
                            reader: typing.Callable[[bytes], typing.Awaitable[bytes]],
                            writer: typing.Callable[[bytes], typing.Awaitable]) -> None:
        async def retry_change(name, change, convert_to, convert_from):
            _LOGGER.debug(f"Setting parameter {name} to {change}")
            query_name = name.encode('ascii')
            for t in range(5):
                send: bytes = query_name + convert_to(change)
                await writer(send)
                current: bytes = await reader(query_name)
                current = convert_from(current)
                if current == change:
                    setattr(self, name, current)
                    return
            raise CommunicationsError(f"error setting parameter {name} to {change}")

        async def write_value(name, convert_to, convert_from):
            change = getattr(changes, name, None)
            if change is None:
                return
            current = getattr(self, name, None)
            if current == change:
                return
            return await retry_change(name, change, convert_to, convert_from)


        def int_to_bytes(i: int) -> bytes:
            return b'%d' % i

        await write_value("SMZ", int_to_bytes, int)
        await write_value("SP", int_to_bytes, int)
        await write_value("STA", int_to_bytes, int)
        await write_value("STB", int_to_bytes, int)
        await write_value("STP", int_to_bytes, int)
        await write_value("STZ", int_to_bytes, int)
        await write_value("SVB", int_to_bytes, int)
        await write_value("SVG", int_to_bytes, int)
        await write_value("SVR", int_to_bytes, int)
        await write_value("B", int_to_bytes, int)

        def bool_to_bytes(b: bool) -> bytes:
            return b'1' if b else b'0'

        def is_true(value: bytes) -> bool:
            return int(value) != 0
        await write_value("H", bool_to_bytes, is_true)
        await write_value("SMB", bool_to_bytes, is_true)

        def from_label(value: bytes):
            try:
                return value.decode("utf-8")
            except UnicodeError:
                return value.decode("ascii")

        def to_label(value: str):
            return value.encode('utf-8')
        await write_value("SL", to_label, from_label)

        async def write_SK(name):
            change = getattr(changes, name, None)
            if change is None:
                return

            new_values = self.SK()
            current = getattr(self, name, None)
            if current:
                new_values.overlay(current)
            new_values.overlay(change)
            if current == new_values:
                return

            return await retry_change(name, new_values, self.SK.write, self.SK.read)

        await write_SK("SKB")
        await write_SK("SKG")
        await write_SK("SKR")
