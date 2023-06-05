import typing
import asyncio
import logging
import enum
import re
from math import isfinite, nan
from forge.acquisition import LayeredConfiguration
from ..streaming import CommunicationsError
from ..base import BaseDataOutput

_LOGGER = logging.getLogger(__name__)


class Parameters:
    class Temperature:
        class Mode(enum.Enum):
            ABSOLUTE = "A"
            RELATIVE = "R"

        def __init__(self, mode: typing.Optional["Parameters.Temperature.Mode"] = None, setpoint: typing.Optional[float] = None):
            self.mode = mode
            self.setpoint = setpoint
            # Read only from the instrument
            self.current: typing.Optional[float] = None
            self.reference: typing.Optional[float] = None

        def persistent(self) -> typing.Dict[str, typing.Any]:
            result: typing.Dict[str, typing.Any] = dict()
            result['mode'] = self.mode.value
            result['setpoint'] = self.setpoint
            return result

        def load(self, config: typing.Optional[typing.Union[float, typing.List, typing.Dict[str, typing.Any]]]) -> bool:
            if config is None:
                return False
            if isinstance(config, float):
                try:
                    setpoint = float(config)
                    if not isfinite(setpoint):
                        raise ValueError
                    self.setpoint = setpoint
                    return True
                except (TypeError, ValueError):
                    pass
                return False

            if isinstance(config, list):
                changed = False

                if len(config) > 0:
                    mode = config[0]
                    if mode is not None:
                        try:
                            self.mode = self.Mode(str(mode).upper())
                            changed = True
                        except (TypeError, ValueError):
                            pass

                if len(config) > 1:
                    setpoint = config[1]
                    if setpoint is not None:
                        try:
                            setpoint = float(setpoint)
                            if not isfinite(setpoint):
                                raise ValueError
                            self.setpoint = setpoint
                            changed = True
                        except (TypeError, ValueError):
                            pass
                return changed
            if not isinstance(config, dict):
                return False

            changed = False

            mode = config.get("mode")
            if mode is not None:
                try:
                    self.mode = self.Mode(str(mode).upper())
                    changed = True
                except (TypeError, ValueError):
                    pass

            setpoint = config.get("setpoint")
            if setpoint is not None:
                try:
                    setpoint = float(setpoint)
                    if not isfinite(setpoint):
                        raise ValueError
                    self.setpoint = setpoint
                    changed = True
                except (TypeError, ValueError):
                    pass

            return changed

        def overlay(self, other: "Parameters.Temperature") -> None:
            for name in ("setpoint", "value"):
                top = getattr(other, name, None)
                if top is None:
                    continue
                setattr(self, name, top)

        PARSE_ABSOLUTE = re.compile(rb"^([^,]+),a,(-?\d+(?:\.\d*)?)", flags=re.IGNORECASE)
        PARSE_RELATIVE = re.compile(rb"^([^,]+),r,(-?\d+(?:\.\d*)?)", flags=re.IGNORECASE)
        PARSE_RELATIVE_CURRENT = re.compile(rb"setTemp\s*=\s*(-?\d+(?:\.\d*)?)", flags=re.IGNORECASE)
        PARSE_RELATIVE_REFERENCE = re.compile(rb"refTemp\s*=\s*(-?\d+(?:\.\d*)?)", flags=re.IGNORECASE)

        def parse(self, name: bytes, lines: typing.List[bytes]) -> None:
            if not lines:
                raise CommunicationsError

            self.current: typing.Optional[float] = None
            self.reference: typing.Optional[float] = None

            match = self.PARSE_ABSOLUTE.search(lines[0])
            if match:
                if match.group(1) != name:
                    raise CommunicationsError(f"invalid response for {name}: {lines}")
                self.mode = self.Mode.ABSOLUTE
                self.setpoint = float(match.group(2))
                return

            match = self.PARSE_RELATIVE.search(lines[0])
            if match:
                if match.group(1) != name:
                    raise CommunicationsError(f"invalid response for {name}: {lines}")
                self.mode = self.Mode.RELATIVE
                self.setpoint = float(match.group(2))
                if len(lines) > 1:
                    match = self.PARSE_RELATIVE_CURRENT.search(lines[1])
                    if match:
                        self.current = float(match.group(1))
                    match = self.PARSE_RELATIVE_REFERENCE.search(lines[1])
                    if match:
                        self.reference = float(match.group(1))
                return

            raise CommunicationsError(f"invalid response for {name}: {lines}")

        def set_command(self, name: str) -> bytes:
            if self.mode == self.Mode.ABSOLUTE:
                return f"{name},a,{self.setpoint or 0}".encode('ascii')
            return f"{name},r,{self.setpoint or 0}".encode('ascii')

        def __str__(self):
            return f"{self.mode.value},{self.setpoint:.1f}"

        def __repr__(self):
            return f"Temperature({repr(self.mode)},{self.setpoint})"

        def __eq__(self, other):
            if not isinstance(other, Parameters.Temperature):
                return False
            return self.Mode == other.Mode and self.setpoint == other.setpoint

        def __ne__(self, other):
            return not self.__eq__(other)

    INTEGER_PARAMETERS = frozenset({
        "lset", "doslope", "doint", "doff", "dvlt", "dthr", "pht", "dthr2", "qcf", "qtrg",
        "wtrg", "wdry", "wwet", "wgn", "wmax", "wmin"
    })
    FLOAT_PARAMETERS = frozenset({
        "qset", "heff", "hmax", "mrefint", "mrefslope"
    })
    TEMPERATURE_PARAMETERS = frozenset({
        "tcon", "tini", "tmod", "topt"
    })

    def __init__(self, **kwargs):
        for name in (self.INTEGER_PARAMETERS | self.FLOAT_PARAMETERS | self.TEMPERATURE_PARAMETERS):
            setattr(self, name, kwargs.get(name))

        # Read only from the instrument
        self.lcur: typing.Optional[float] = None

    def overlay(self, other: "Parameters") -> None:
        for name in (self.INTEGER_PARAMETERS | self.FLOAT_PARAMETERS):
            top = getattr(other, name, None)
            if top is None:
                continue
            setattr(self, name, top)
        for name in self.TEMPERATURE_PARAMETERS:
            top = getattr(other, name, None)
            if top is None:
                continue
            bottom = getattr(self, name, None)
            if bottom is None:
                bottom = self.Temperature()
                setattr(self, name, bottom)
            bottom.overlay(top)

    def record(self, target: BaseDataOutput.ConstantRecord) -> None:
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

        target.constants.append(_Integer(self, "lset", {
            'long_name': "laser diode power setpoint",
            'C_format': "%4llu",
            'units': "uW",
        }))
        target.constants.append(_Integer(self, "doslope", {
            'long_name': "slope of the detector offset to laser power",
            'C_format': "%3llu",
            'units': "mV/mW",
        }))
        target.constants.append(_Integer(self, "doint", {
            'long_name': "intercept of the detector offset to laser power",
            'C_format': "%4llu",
            'units': "mV",
        }))
        target.constants.append(_Integer(self, "doff", {
            'long_name': "detector offset (no effect if doslope is non-zero)",
            'C_format': "%3llu",
            'units': "mV",
        }))
        target.constants.append(_Integer(self, "dvlt", {
            'long_name': "detector voltage",
            'C_format': "%3llu",
            'units': "V",
        }))
        target.constants.append(_Integer(self, "dthr", {
            'long_name': "detector particle counting threshold",
            'C_format': "%3llu",
            'units': "mV",
        }))
        target.constants.append(_Integer(self, "pht", {
            'long_name': "target percentage of particles above upper detector threshold (dthr2) for auto-adjustment",
            'C_format': "%3llu",
            'units': "%",
        }))
        target.constants.append(_Integer(self, "dthr2", {
            'long_name': "upper detector threshold (no effect if pht is non-zero)",
            'C_format': "%4llu",
            'units': "mV",
        }))
        target.constants.append(_Integer(self, "qcf", {
            'long_name': "flow calibration factor",
            'C_format': "%3llu",
        }))
        target.constants.append(_Integer(self, "qtrg", {
            'long_name': "target volumetric flow rate",
            'C_format': "%3llu",
            'units': "cm3/min",
        }))
        target.constants.append(_Integer(self, "wtrg", {
            'long_name': "target wick saturation percentage",
            'C_format': "%3llu",
            'units': "%",
        }))
        target.constants.append(_Integer(self, "wdry", {
            'long_name': "wick saturation percentage that will trigger wick recovery mode",
            'C_format': "%3llu",
            'units': "%",
        }))
        target.constants.append(_Integer(self, "wwet", {
            'long_name': "wick saturation percentage that will exit wick recovery mode",
            'C_format': "%3llu",
            'units': "%",
        }))
        target.constants.append(_Integer(self, "wgn", {
            'long_name': "feedback gain used in the moderator setpoint calculation",
            'C_format': "%3llu",
            'units': "cdegC/%",
        }))
        target.constants.append(_Integer(self, "wmax", {
            'long_name': "raw wick sensor reading corresponding to 0% saturation",
            'C_format': "%4llu",
        }))
        target.constants.append(_Integer(self, "wmin", {
            'long_name': "raw wick sensor reading corresponding to 100% saturation",
            'C_format': "%4llu",
        }))

        class _Float(BaseDataOutput.Float):
            def __init__(self, parameters: Parameters, name: str, attributes: typing.Dict[str, typing.Any] = None):
                super().__init__(name)
                self.template = BaseDataOutput.Field.Template.METADATA
                self.parameters = parameters
                if attributes:
                    self.attributes.update(attributes)

            @property
            def value(self) -> float:
                return getattr(self.parameters, self.name, None) or nan

        target.constants.append(_Float(self, "lcur", {
            'long_name': "laser diode current",
            'C_format': "%5.1f",
            'units': "mA",
        }))
        target.constants.append(_Float(self, "qset", {
            'long_name': "pump power",
            'C_format': "%5.1f",
            'units': "%",
        }))
        target.constants.append(_Float(self, "heff", {
            'long_name': "humidifier effectiveness parameter in dewpoint estimator",
            'C_format': "%4.2f",
        }))
        target.constants.append(_Float(self, "hmax", {
            'long_name': "maximum expected RH from humidifier parameter in dewpoint estimator",
            'C_format': "%4.2f",
            'units': "%",
        }))
        target.constants.append(_Float(self, "mrefint", {
            'long_name': "moderator reference intercept parameter in setpoint calculation",
            'C_format': "%4.1f",
            'units': "degC",
        }))
        target.constants.append(_Float(self, "mrefslope", {
            'long_name': "moderator reference slope parameter in setpoint calculation",
            'C_format': "%5.2f",
            'units': "1",   # degC/degC
        }))

        class _TemperatureSetpoint(BaseDataOutput.Float):
            def __init__(self, parameters: Parameters, name: str,
                         attributes: typing.Dict[str, typing.Any] = None):
                super().__init__(name)
                self.template = BaseDataOutput.Field.Template.METADATA
                self.parameters = parameters
                if attributes:
                    self.attributes.update(attributes)

            @property
            def value(self) -> float:
                temperature: Parameters.Temperature = getattr(self.parameters, self.name, None)
                if temperature is None or temperature.setpoint is None:
                    return nan
                return temperature.setpoint

        class _TemperatureMode(BaseDataOutput.String):
            def __init__(self, parameters: Parameters, temperature: str, name: str,
                         attributes: typing.Dict[str, typing.Any] = None):
                super().__init__(name)
                self.template = BaseDataOutput.Field.Template.METADATA
                self.parameters = parameters
                if attributes:
                    self.attributes.update(attributes)
                self.temperature = temperature

            @property
            def value(self) -> str:
                temperature: Parameters.Temperature = getattr(self.parameters, self.temperature, None)
                if temperature is None or temperature.mode is None:
                    return ""
                if temperature.mode == Parameters.Temperature.Mode.ABSOLUTE:
                    return "Absolute"
                result = "Relative"
                if temperature.current is not None:
                    result += f",setTemp={temperature.current:.1f} degC"
                if temperature.reference is not None:
                    result += f",refTemp={temperature.reference:.1f} degC"
                return result

        def declare_temperature(name: str, description: str):
            target.constants.append(_TemperatureSetpoint(self, name, {
                'long_name': f"{description} temperature setpoint",
                'C_format': "%5.1f",
                'units': "degC",
            }))
            target.constants.append(_TemperatureMode(self, name, f"{name}_mode", {
                'long_name': f"{description} temperature control mode",
            }))

        declare_temperature("tcon", "conditioner")
        declare_temperature("tini", "initiator")
        declare_temperature("tmod", "moderator")
        declare_temperature("topt", "optics")

    def persistent(self) -> typing.Dict[str, typing.Any]:
        result: typing.Dict[str, typing.Any] = dict()
        for name in (self.INTEGER_PARAMETERS | self.FLOAT_PARAMETERS):
            value = getattr(self, name, None)
            if value is None:
                continue
            result[name] = value
        for name in self.TEMPERATURE_PARAMETERS:
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

        set_in_range("lset", int, 0, None)
        set_if_valid("doslope", int)
        set_in_range("doint", int, 0, 800)
        set_in_range("doff", int, 0, 800)
        set_in_range("dvlt", int, 0, 100)
        set_in_range("dthr", int, 0, 799)
        set_in_range("pht", int, 0, 100)
        set_in_range("dthr2", int, 0, 4000)
        set_in_range("qcf", int, 1, 255)
        set_in_range("qtrg", int, 0, None)
        set_in_range("wtrg", int, 0, 100)
        set_in_range("wdry", int, 0, 100)
        set_in_range("wwet", int, 0, 100)
        set_in_range("wgn", int, 0, 255)
        set_in_range("wmax", int, 0, 1023)
        set_in_range("wmin", int, 0, 1023)
        set_in_range("qset", float, 0, 100)
        set_in_range("heff", float, 0, 1)
        set_in_range("hmax", float, 0, 100)
        set_if_valid("mrefint", float)
        set_if_valid("mrefslope", float)

        def set_temperature(name):
            c = config.get(name)
            value = getattr(self, name, None)
            if value is None:
                value = self.Temperature()
            if not value.load(c):
                return
            setattr(self, name, value)

        set_temperature("tcon")
        set_temperature("tini")
        set_temperature("tmod")
        set_temperature("topt")

    INTEGER_PARSE: typing.List[typing.Tuple[str, re.Pattern]] = [
        ("lset",    re.compile(rb"^lset\s+(\d+)", flags=re.IGNORECASE)),
        ("lcur",    re.compile(rb"^lset\s+\d+\s*[^;]*;\s*current\s*=\s*(\d+)", flags=re.IGNORECASE)),
        ("doslope", re.compile(rb"doslope\s+(\d+)", flags=re.IGNORECASE)),
        ("doint",   re.compile(rb"doint\s+(\d+)", flags=re.IGNORECASE)),
        ("doff",    re.compile(rb"doint\s+\d+[^(]*\(\s*doff\s+(\d+)", flags=re.IGNORECASE)),
        ("doff",    re.compile(rb"^doff\s+(\d+)", flags=re.IGNORECASE)),
        ("dvlt",    re.compile(rb"^dvlt\s+(\d+)", flags=re.IGNORECASE)),
        ("dthr",    re.compile(rb"^dthr\s+(\d+)", flags=re.IGNORECASE)),
        ("pht",     re.compile(rb"^pht\s+(\d+)", flags=re.IGNORECASE)),
        ("dthr2",   re.compile(rb"^pht\s+\d+[^(]*\(\s*dthr2\s+(\d+)", flags=re.IGNORECASE)),
        ("dthr2",   re.compile(rb"^dthr2\s+(\d+)", flags=re.IGNORECASE)),
        ("qcf",     re.compile(rb"^qcf\s+(\d+)", flags=re.IGNORECASE)),
        ("qtrg",    re.compile(rb"^qtrg\s+(\d+)", flags=re.IGNORECASE)),
        ("wtrg",    re.compile(rb"^wTrg\s+(\d+)", flags=re.IGNORECASE)),
        ("wwet",    re.compile(rb"^wWet\s+(\d+)", flags=re.IGNORECASE)),
        ("wdry",    re.compile(rb"^wDry\s+(\d+)", flags=re.IGNORECASE)),
        ("wgn",     re.compile(rb"^wgn\s+(\d+)", flags=re.IGNORECASE)),
        ("wmax",    re.compile(rb"^wmax\s+(\d+)", flags=re.IGNORECASE)),
        ("wmin",    re.compile(rb"^wmin\s+(\d+)", flags=re.IGNORECASE)),
    ]

    FLOAT_PARSE: typing.List[typing.Tuple[str, re.Pattern]] = [
        ("qset",    re.compile(rb"^qtrg\s+\d+[^(]*\(\s*qset\s*(-?\d+(?:\.\d*)?)", flags=re.IGNORECASE)),
        ("qset",    re.compile(rb"^qset\s*(-?\d+(?:\.\d*)?)", flags=re.IGNORECASE)),
        ("heff",    re.compile(rb"^heff\s*(-?\d+(?:\.\d*)?)", flags=re.IGNORECASE)),
        ("hmax",    re.compile(rb"^heff\s*-?\d+(?:\.\d*)?\S*\s+hmax\s*(-?\d+(?:\.\d*)?)", flags=re.IGNORECASE)),
        ("hmax",    re.compile(rb"^hmax\s*(-?\d+(?:\.\d*)?)", flags=re.IGNORECASE)),
        ("mrefint", re.compile(rb"^mrefint\s*(-?\d+(?:\.\d*)?)", flags=re.IGNORECASE)),
        ("mrefslope", re.compile(rb"^mrefslope\s*(-?\d+(?:\.\d*)?)", flags=re.IGNORECASE)),
        ("lcur",    re.compile(rb"^\(?\s*lcur\s*(\d+(?:\.\d*)?)", flags=re.IGNORECASE)),
    ]

    def parse_sus(self, lines: typing.List[bytes]) -> None:
        for name in (self.INTEGER_PARAMETERS | self.FLOAT_PARAMETERS | self.TEMPERATURE_PARAMETERS):
            setattr(self, name, None)
        self.lcur: typing.Optional[float] = None

        any_hit = False
        for l in lines:
            hit = False
            for (name, e) in self.INTEGER_PARSE:
                match = e.search(l)
                if not match:
                    continue
                setattr(self, name, int(match.group(1)))
                hit = True
            for (name, e) in self.FLOAT_PARSE:
                match = e.search(l)
                if not match:
                    continue
                setattr(self, name, float(match.group(1)))
                hit = True
            if not hit:
                _LOGGER.debug(f"No match for sus line {l}")
            else:
                any_hit = True

        if not any_hit:
            raise CommunicationsError(f"invalid sus response {lines}")

    def set_commands(self) -> typing.List[bytes]:
        result: typing.List[bytes] = list()

        for name in self.INTEGER_PARAMETERS:
            value = getattr(self, name, None)
            if value is None:
                continue
            result.append(f"{name},{int(value)}".encode('ascii'))
        for name in self.FLOAT_PARAMETERS:
            value = getattr(self, name, None)
            if value is None:
                continue
            result.append(f"{name},{float(value):.3f}".encode('ascii'))
        for name in self.TEMPERATURE_PARAMETERS:
            value = getattr(self, name, None)
            if value is None:
                continue
            if value.mode is None or value.setpoint is None:
                continue
            result.append(value.set_command(name))
        return result
