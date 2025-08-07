import typing
import asyncio
import logging
import time
import re
import csv
from math import isfinite, exp, nan
from forge.units import mass_ng_to_ug, pressure_Pa_to_hPa, flow_ccm_to_lpm
from forge.tasks import wait_cancelable
from ..standard import StandardInstrument, CommunicationsError, BaseContext
from ..http import HttpInstrument, HttpContext
from ..streaming import StreamingInstrument, StreamingContext
from ..variable import Input

_LOGGER = logging.getLogger(__name__)
_INSTRUMENT_TYPE = __name__.split('.')[-2]
_SERIAL_NUMBER = re.compile(r"AE36[sS]?-S?\d+-(\d+)")


if typing.TYPE_CHECKING:
    class _InstrumentMixin(StandardInstrument):
        pass
else:
    class _InstrumentMixin:
        def __init__(self, *args, **kwargs):
            pass


class _BaseInstrument(_InstrumentMixin):
    def __init__(self, context: BaseContext):
        super().__init__(context)

        self._report_interval: float = float(context.config.get('REPORT_INTERVAL', default=1.0))
        self._instrument_timebase: float = self._report_interval

        if context.config.get('NINE_WAVELENGTH'):
            # AE36s
            self._wavelengths = [
                (340.0, "1"),
                (370.0, "2"),
                (400.0, "3"),
                (470.0, "4"),
                (520.0, "5"),
                (590.0, "6"),
                (630.0, "7"),
                (880.0, "8"),
                (950.0, "9"),
            ]
        else:
            self._wavelengths = [
                (370.0, "1"),
                (470.0, "2"),
                (520.0, "3"),
                (590.0, "4"),
                (630.0, "5"),
                (880.0, "6"),
                (950.0, "7"),
            ]

        ebc_efficiency: typing.Union[typing.List[float], float] = context.config.get('EBC_EFFICIENCY')
        self._ebc_efficiency: typing.List[float] = list()
        ebc_efficiency_explicit: typing.Set[int] = set()
        for i in range(len(self._wavelengths)):
            wl = self._wavelengths[i][0]
            if isinstance(ebc_efficiency, list) and i < len(ebc_efficiency):
                e = float(ebc_efficiency[i])
                ebc_efficiency_explicit.add(i)
            elif ebc_efficiency is not None and ebc_efficiency > 0.0:
                e = ebc_efficiency / wl
                ebc_efficiency_explicit.add(i)
            else:
                e = 6833.0 / wl
            self._ebc_efficiency.append(e)

        self._weingartner_constant = float(context.config.get('WEINGARTNER_CONSTANT', default=1.57))

        self.data_PCT = self.input("PCT")
        self.data_Q1 = self.input("Q1")
        self.data_Q2 = self.input("Q2")
        self.data_Tinlet = self.input("Tinlet")
        self.data_Uinlet = self.input("Uinlet")
        self.data_Tcontroller = self.input("Tcontroller")
        self.data_Tsupply = self.input("Tsupply")
        self.data_Tled = self.input("Tled")
        self.data_Tsource = self.input("Tsource")
        self.data_Ttape = self.input("Ttape")
        self.data_Utape = self.input("Utape")
        self.data_Fn = self.persistent('Fn')

        self.data_X_wavelength: typing.List[Input] = list()
        self.data_Xa_wavelength: typing.List[Input] = list()
        self.data_Xb_wavelength: typing.List[Input] = list()
        self.data_Bac_wavelength: typing.List[Input] = list()
        self.data_Ba_wavelength: typing.List[Input] = list()
        self.data_Bas_wavelength: typing.List[Input] = list()
        self.data_Ir_wavelength: typing.List[Input] = list()
        self.data_Irs_wavelength: typing.List[Input] = list()
        self.data_If_wavelength: typing.List[Input] = list()
        self.data_Ip_wavelength: typing.List[Input] = list()
        self.data_Ips_wavelength: typing.List[Input] = list()
        self.data_correction_factor_wavelength: typing.List[Input] = list()
        for _, code in self._wavelengths:
            self.data_X_wavelength.append(self.input("X" + code))
            self.data_Xa_wavelength.append(self.input("Xa" + code))
            self.data_Xb_wavelength.append(self.input("Xb" + code))
            self.data_Bac_wavelength.append(self.input("Bac" + code))
            self.data_Ba_wavelength.append(self.input("Ba" + code))
            self.data_Bas_wavelength.append(self.input("Bas" + code))
            self.data_Ir_wavelength.append(self.input("Ir" + code))
            self.data_Irs_wavelength.append(self.input("Irs" + code))
            self.data_If_wavelength.append(self.input("If" + code))
            self.data_Ip_wavelength.append(self.input("Ip" + code))
            self.data_Ips_wavelength.append(self.input("Ips" + code))
            self.data_correction_factor_wavelength.append(self.input("k" + code))

        self.data_wavelength = self.persistent("wavelength", save_value=False, send_to_bus=False)
        self.data_wavelength([wl for wl, _ in self._wavelengths])
        self.data_X = self.input_array("X", send_to_bus=False)
        self.data_Bac = self.input_array("Bac", send_to_bus=False)
        self.data_Ba = self.input_array("Ba", send_to_bus=False)
        self.data_Bas = self.input_array("Bas", send_to_bus=False)
        self.data_Ir = self.input_array("Ir", send_to_bus=False)
        self.data_Irs = self.input_array("Irs", send_to_bus=False)
        self.data_If = self.input_array("If", send_to_bus=False)
        self.data_Ip = self.input_array("Ip", send_to_bus=False)
        self.data_Ips = self.input_array("Ips", send_to_bus=False)
        self.data_correction_factor = self.input_array("k", send_to_bus=False)

        self.data_In0 = self.persistent("In0", send_to_bus=False)
        self.data_Ins0 = self.persistent("Ins0", send_to_bus=False)
        self._normalization_changed: bool = False
        self._spot_change_observed: bool = False

        self.notify_spot_advancing = self.notification('spot_advancing')

        self._have_direct_Ir: typing.List[bool] = [False] * len(self._wavelengths)
        self._have_direct_Irs: typing.List[bool] = [False] * len(self._wavelengths)
        self._have_direct_Bac: typing.List[bool] = [False] * len(self._wavelengths)

        def report_P(v: float) -> None:
            self.instrument_report.record.data_record.standard_pressure = pressure_Pa_to_hPa(v)

        def report_T(v: float) -> None:
            self.instrument_report.record.data_record.standard_temperature = v

        def timebase(v: float) -> None:
            self._instrument_timebase = v

        def weingartner_constant(v: float) -> None:
            self._weingartner_constant = v

        def serial_number(v: str) -> None:
            matched = _SERIAL_NUMBER.search(v)
            if matched:
                self.set_serial_number(matched.group(1))
            else:
                self.set_serial_number(v)

        self.float_columns: typing.Dict[str, typing.Callable[[float], typing.Any]] = {
            "bb": self.data_PCT,
            "pressure": report_P,
            "temp": report_T,
            "flow1": lambda v: self.data_Q1(flow_ccm_to_lpm(v)),
            "flow2": lambda v: self.data_Q2(flow_ccm_to_lpm(v)),
            "controllertemp": self.data_Tcontroller,
            "inlethumidity": self.data_Uinlet,
            "inlettemp": self.data_Tinlet,
            "tapehumidity": self.data_Utape,
            "tapetemp": self.data_Ttape,
            "ledtemp": self.data_Tled,
            "ledsourcetemp": self.data_Tsource,
            "timebase": timebase,
            "c": weingartner_constant,
        }
        self.integer_columns: typing.Dict[str, typing.Callable[[int], typing.Any]] = {
            "tapeadvancecount": self._handle_tape_advance,
        }
        self.string_columns: typing.Dict[str, typing.Callable[[str], typing.Any]] = {
            "serialnumber": serial_number,
            "firmwarever": self.set_firmware_version,
            "softwarever": lambda v: self.set_instrument_info('software_version', v),
        }

        def set_wavelength_value(target: Input) -> typing.Callable[[float], None]:
            def apply(v: float):
                target(v)
            return apply

        def set_wavelength_value_direct(target: Input,
                                        index: int, direct: typing.List[bool]) -> typing.Callable[[float], None]:
            def apply(v: float):
                if isfinite(v):
                    direct[index] = True
                target(v)
            return apply

        def set_atn_value(target: Input, index: int, direct: typing.List[bool]) -> typing.Callable[[float], None]:
            def apply(v: float):
                if not isfinite(v) or v <= -10.0:
                    target(nan)
                else:
                    direct[index] = True
                    target(exp(v / -100.0))
            return apply

        def set_bc_value(target: Input) -> typing.Callable[[float], None]:
            def apply(v: float):
                target(mass_ng_to_ug(v))
            return apply

        def set_ebc_efficiency(index: int) -> typing.Callable[[float], None]:
            def apply(v: float):
                self._ebc_efficiency[index] = v
            return apply

        for index in range(len(self._wavelengths)):
            wl_name = f"{self._wavelengths[index][0]:.0f}"
            self.float_columns[f"ref{wl_name}"] = set_wavelength_value(self.data_If_wavelength[index])
            self.float_columns[f"sens{wl_name}_1"] = set_wavelength_value(self.data_Ip_wavelength[index])
            self.float_columns[f"sens{wl_name}_2"] = set_wavelength_value(self.data_Ips_wavelength[index])
            self.float_columns[f"atn{wl_name}_1"] = set_atn_value(self.data_Ir_wavelength[index], index, self._have_direct_Ir)
            self.float_columns[f"atn{wl_name}_2"] = set_atn_value(self.data_Irs_wavelength[index], index, self._have_direct_Irs)
            self.float_columns[f"bc{wl_name}_1"] = set_bc_value(self.data_Xa_wavelength[index])
            self.float_columns[f"bc{wl_name}_2"] = set_bc_value(self.data_Xb_wavelength[index])
            self.float_columns[f"bc{wl_name}"] = set_bc_value(self.data_X_wavelength[index])
            self.float_columns[f"k{wl_name}"] = set_wavelength_value(self.data_correction_factor_wavelength[index])
            self.float_columns[f"babs{wl_name}"] = set_wavelength_value_direct(self.data_Bac_wavelength[index], index, self._have_direct_Bac)

            if index not in ebc_efficiency_explicit:
                self.float_columns[f"mac{wl_name}"] = set_ebc_efficiency(index)

        dimension_wavelength = self.dimension_wavelength(self.data_wavelength)
        self.instrument_report = self.report(
            self.variable_ebc(self.data_X, dimension_wavelength, code="X").at_stp(),
            self.variable_absorption(self.data_Bac, dimension_wavelength, code="Bac").at_stp(),

            self.variable_absorption(self.data_Ba, dimension_wavelength, "spot_one_light_absorption",
                                            code="Ba", attributes={
                'long_name': "uncorrected light absorption coefficient at STP on spot one",
                'standard_name': None,
            }).at_stp(),
            self.variable_absorption(self.data_Bas, dimension_wavelength, "spot_two_light_absorption",
                                            code="Bas", attributes={
                'long_name': "uncorrected light absorption coefficient at STP on spot two",
                'standard_name': None,
            }).at_stp(),
            self.variable_transmittance(self.data_Ir, dimension_wavelength, "spot_one_transmittance", code="Ir",
                                        attributes={
                'long_name': "transmittance fraction of light through the filter relative to the amount before sampling on spot one",
            }),
            self.variable_transmittance(self.data_Irs, dimension_wavelength, "spot_two_transmittance", code="Irs",
                                        attributes={
                'long_name': "transmittance fraction of light through the filter relative to the amount before sampling on spot two",
            }),
            self.variable_array(self.data_If, dimension_wavelength, "reference_intensity", code="If", attributes={
                'long_name': "reference detector signal",
                'C_format': "%6.0f",
            }),
            self.variable_array(self.data_Ip, dimension_wavelength, "spot_one_sample_intensity", code="Ip",
                                attributes={
                'long_name': "sample detector signal on spot one",
                'C_format': "%6.0f",
            }),
            self.variable_array(self.data_Ips, dimension_wavelength, "spot_two_sample_intensity", code="Ips",
                                attributes={
                'long_name': "sample detector signal on spot two",
                'C_format': "%6.0f",
            }),
            self.variable_array(self.data_correction_factor, dimension_wavelength, "correction_factor", code="ZFACTOR",
                                attributes={
                'long_name': "correction factor applied to calculate the final EBC",
                'C_format': "%9.6f",
            }),

            self.variable(self.data_PCT, "biomass_burning", code="PCT", attributes={
                'long_name': "percentage of low efficiency combustion as obtained from the Aethalometer apportionment model",
                'units': "%",
                'C_format': "%3.0f"
            }),

            self.variable_flow(self.data_Q1, "spot_one_flow", code="Q1", attributes={
                'long_name': "sample flow through spot one",
                'C_format': "%7.3f",
            }).at_stp(),
            self.variable_flow(self.data_Q2, "spot_two_flow", code="Q2", attributes={
                'long_name': "sample flow through spot two",
                'C_format': "%7.3f",
            }).at_stp(),
            self.variable_temperature(self.data_Tcontroller, "controller_temperature", code="T1",
                                      attributes={'long_name': "controller board temperature"}),
            self.variable_temperature(self.data_Tled, "led_temperature", code="T2",
                                      attributes={'long_name': "LED board temperature"}),
            self.variable_temperature(self.data_Tsource, "source_temperature", code="T3",
                                      attributes={'long_name': "LED light source temperature"}),
            self.variable_air_temperature(self.data_Ttape, "tape_temperature", code="T4",
                                          attributes={'long_name': "temperature measured in the tape compartment"}),
            self.variable_temperature(self.data_Tinlet, "inlet_temperature", code="Tu",
                                      attributes={'long_name': "temperature measured at the instrument inlet"}),
            self.variable_air_rh(self.data_Uinlet, "inlet_humidity", code="Uu",
                                 attributes={'long_name': "relative measured at the instrument inlet"}),
            self.variable_rh(self.data_Utape, "tape_humidity", code="U4",
                             attributes={'long_name': "relative measured in the tape compartment"}),

            flags=[
                self.flag(self.notify_spot_advancing),
            ],

            auxiliary_variables=(
                [self.variable(w) for w in self.data_Ba_wavelength] +
                [self.variable(w) for w in self.data_X_wavelength] +
                [self.variable(w) for w in self.data_correction_factor_wavelength] +
                [self.variable_last_valid(w) for w in self.data_Ir_wavelength]
            ),
        )

        self.filter_state = self.change_event(
            self.state_unsigned_integer(self.data_Fn, "tape_advance", code="Fn", attributes={
                'long_name': "tape advance count",
            }),
            self.state_measurement_array(self.data_In0, dimension_wavelength, "spot_one_normalization", code="In", attributes={
                'long_name': "sample/reference intensity at spot one sampling start",
                'units': "1",
                'C_format': "%9.7f",
            }),
            self.state_measurement_array(self.data_Ins0, dimension_wavelength, "spot_two_normalization", code="Ins", attributes={
                'long_name': "sample/reference intensity at spot two sampling start",
                'units': "1",
                'C_format': "%9.7f",
            }),
        )

        self.parameters_record = self.context.data.constant_record("parameters")
        self.parameters_record.array_float_attr("mass_absorption_efficiency", self, '_ebc_efficiency', attributes={
            'long_name': "the efficiency factor used to convert absorption coefficients into an equivalent black carbon",
            'units': "m2 g",
            'C_format': "%5.2f",
        })

    def _lookup_target(self, name: str) -> typing.Callable[[typing.Union[bytes, str]], None]:
        name = name.lower()

        target = self.string_columns.get(name)
        if target:
            def result(value: typing.Union[bytes, str]):
                if isinstance(value, bytes) or isinstance(value, bytearray):
                    try:
                        value = value.decode('utf-8')
                    except UnicodeDecodeError:
                        return
                try:
                    target(str(value))
                except (ValueError, TypeError) as e:
                    raise CommunicationsError(f"invalid field value {name}") from e
            return result

        target = self.integer_columns.get(name)
        if target:
            def result(value: typing.Union[bytes, str]):
                try:
                    target(int(value))
                except (ValueError, TypeError, OverflowError) as e:
                    raise CommunicationsError(f"invalid field value {name}") from e
            return result

        target = self.float_columns.get(name)
        if target:
            def result(value: typing.Union[bytes, str]):
                try:
                    if value is None or value == b"" or value == "":
                        target(nan)
                    else:
                        target(float(value))
                except (ValueError, TypeError, OverflowError) as e:
                    raise CommunicationsError(f"invalid field value {name}") from e
            return result

        return lambda _: None

    def _handle_tape_advance(self, Fn: int) -> None:
        if self.data_Fn.value is not None and self.data_Fn.value != Fn:
            self._normalization_changed = True
            self._spot_change_observed = True
        self.data_Fn(Fn)

    def _normalization_ready(self) -> bool:
        def _all_valid(check: typing.Iterable[Input]) -> bool:
            for w in check:
                if w.value is None or not isfinite(w.value):
                    return False
            return True

        if not _all_valid(self.data_X_wavelength):
            return False
        if not _all_valid(self.data_Xa_wavelength):
            return False
        if not _all_valid(self.data_Xb_wavelength):
            return False
        if not _all_valid(self.data_If_wavelength):
            return False
        if not _all_valid(self.data_Ip_wavelength):
            return False
        if not _all_valid(self.data_Ips_wavelength):
            return False

        return True

    def _normalized_intensities(self) -> typing.Tuple[typing.List[float], typing.List[float]]:
        In: typing.List[float] = list()
        Ins: typing.List[float] = list()
        for i in range(len(self.data_If_wavelength)):
            If = float(self.data_If_wavelength[i])
            if not isfinite(If) or If == 0.0:
                In.append(nan)
                Ins.append(nan)
                continue
            In.append(float(self.data_Ip_wavelength[i]) / If)
            Ins.append(float(self.data_Ips_wavelength[i]) / If)
        return In, Ins

    def _extrapolate_normalization(self) -> None:
        def _calculate(data_In0, Ip_wavelength, Ba_wavelength):
            In0: typing.Optional[typing.List[float]] = None
            for i in range(len(self._wavelengths)):
                if data_In0.value and i < len(data_In0.value) and isfinite(data_In0.value[i]):
                    continue

                Bac = float(self.data_Bac_wavelength[i])
                if not isfinite(Bac) or abs(Bac) < 0.5:
                    continue
                Ba = float(Ba_wavelength[i])
                if not isfinite(Ba) or abs(Ba) < 0.5:
                    continue
                If = float(self.data_If_wavelength[i])
                if not isfinite(If) or If == 0.0:
                    continue
                In = float(Ip_wavelength[i]) / If
                if not isfinite(In) or In == 0.0:
                    continue
                k = float(self.data_correction_factor_wavelength[i])
                if not isfinite(k) or abs(k) < 0.001:
                    continue

                # Back out Weingartner
                Bac = Bac * self._weingartner_constant
                # Bac = Ba / (1.0 - k * ATN)
                ATN = (1.0 - Ba / Bac) / k
                # ATN = ln(Ir) * -100.0
                Ir = exp(ATN / -100.0)
                if not isfinite(Ir) or Ir < 0.3 or Ir > 1.01:
                    continue

                if In0 is None:
                    In0 = data_In0.value
                    if In0 is None:
                        In0 = list()
                    else:
                        In0 = list(In0)
                while len(In0) <= i:
                    In0.append(nan)

                # Ir = In / In0
                In0[i] = In / Ir

            if In0 is not None:
                _LOGGER.debug(f"Applying recovered normalization {In0}")
                data_In0(In0, oneshot=True)

        _calculate(self.data_In0, self.data_Ip_wavelength, self.data_Ba_wavelength)
        # This makes the assumption that the second spot will correct to the same value with a different attenuation,
        # which isn't really true.  However, since this is a recovery path anyway, it's probably close enough.
        _calculate(self.data_Ins0, self.data_Ips_wavelength, self.data_Bas_wavelength)

    def _calculate_transmittance(self) -> None:
        In0 = self.data_In0.value
        Ins0 = self.data_Ins0.value
        In, Ins = self._normalized_intensities()
        for i in range(len(self.data_If_wavelength)):
            if not self._have_direct_Ir[i]:
                if In0 and i < len(In0) and isfinite(In0[i]) and In0[i] != 0.0:
                    self.data_Ir_wavelength[i](In[i] / In0[i])
                else:
                    self.data_Ir_wavelength[i](nan)
            if not self._have_direct_Irs[i]:
                if Ins0 and i < len(Ins0) and isfinite(Ins0[i]) and Ins0[i] != 0.0:
                    self.data_Irs_wavelength[i](Ins[i] / Ins0[i])
                else:
                    self.data_Irs_wavelength[i](nan)

    def _begin_data_report(self) -> None:
        for index in range(len(self._wavelengths)):
            self._have_direct_Ir[index] = False
            self._have_direct_Irs[index] = False
            self._have_direct_Bac[index] = False

    def _complete_data_report(self) -> None:
        self.notify_spot_advancing(self._normalization_changed)
        if self._normalization_changed and self._normalization_ready():
            self._normalization_changed = False
            if self._spot_change_observed:
                In, Ins = self._normalized_intensities()
                _LOGGER.debug("Spot change observed, capturing normalized intensities")
                self.data_In0(In, oneshot=True)
                self.data_Ins0(Ins, oneshot=True)
            else:
                self.data_In0([nan] * len(self._wavelengths), oneshot=True)
                self.data_Ins0([nan] * len(self._wavelengths), oneshot=True)
            self._spot_change_observed = False

        for i in range(len(self._wavelengths)):
            if not self._have_direct_Bac[i]:
                self.data_Bac_wavelength[i](self.data_X_wavelength[i].value * self._ebc_efficiency[i])
            self.data_Ba_wavelength[i](self.data_Xa_wavelength[i].value * self._ebc_efficiency[i])
            self.data_Bas_wavelength[i](self.data_Xb_wavelength[i].value * self._ebc_efficiency[i])

        if not bool(self.notify_spot_advancing):
            self._extrapolate_normalization()
            self._calculate_transmittance()

            self.data_X([float(c) for c in self.data_X_wavelength])
            self.data_Bac([float(c) for c in self.data_Bac_wavelength])
            self.data_Ba([float(c) for c in self.data_Ba_wavelength])
            self.data_Bas([float(c) for c in self.data_Bas_wavelength])
        else:
            for w in self.data_Ir_wavelength:
                w(nan)
            for w in self.data_Irs_wavelength:
                w(nan)

            self.data_X([nan] * len(self._wavelengths))
            self.data_Bac([nan] * len(self._wavelengths))
            self.data_Ba([nan] * len(self._wavelengths))
            self.data_Bas([nan] * len(self._wavelengths))

        self.data_Ir([float(c) for c in self.data_Ir_wavelength])
        self.data_Irs([float(c) for c in self.data_Irs_wavelength])
        self.data_If([float(c) for c in self.data_If_wavelength])
        self.data_Ip([float(c) for c in self.data_Ip_wavelength])
        self.data_Ips([float(c) for c in self.data_Ips_wavelength])
        self.data_correction_factor([float(c) for c in self.data_correction_factor_wavelength])

        self.instrument_report.record.data_record.report_interval = self._instrument_timebase
        self.instrument_report()


class InstrumentADP(StreamingInstrument, _BaseInstrument):
    INSTRUMENT_TYPE = _INSTRUMENT_TYPE
    MANUFACTURER = "Magee"
    MODEL = "AE36"
    DISPLAY_LETTER = "A"
    TAGS = frozenset({"aerosol", "aethalometer", "absorption", _INSTRUMENT_TYPE})
    SERIAL_PORT = {'baudrate': 115200}
    INSTRUMENT_INFO_METADATA = {
        **StandardInstrument.INSTRUMENT_INFO_METADATA,
        **{
            'software_version': "instrument software version information",
        }
    }

    _CHECK_CSV = re.compile(rb'^(?:\s*")?\s*(?:\d+|ID)\s*(?:"\s*)?,',
                            flags=re.IGNORECASE)

    _DATA_COLUMNS = [
        "ID",
        "TimestampUTC",
        "SetupTimestamp",
        "SetupID",
        "G0_Status",
        "G1_Status",
        "G2_Status",
        "G3_Status",
        "G4_Status",
        "G5_Status",
        "G6_Status",
        "ControllerStatus",
        "DetectorStatus",
        "LedStatus",
        "ValveStatus",
        "PumpStatus",
        "Ref340",
        "Sens340_1",
        "Sens340_2",
        "Ref370",
        "Sens370_1",
        "Sens370_2",
        "Ref400",
        "Sens400_1",
        "Sens400_2",
        "Ref470",
        "Sens470_1",
        "Sens470_2",
        "Ref520",
        "Sens520_1",
        "Sens520_2",
        "Ref590",
        "Sens590_1",
        "Sens590_2",
        "Ref630",
        "Sens630_1",
        "Sens630_2",
        "Ref880",
        "Sens880_1",
        "Sens880_2",
        "Ref950",
        "Sens950_1",
        "Sens950_2",
        "Atn340_1",
        "Atn340_2",
        "Atn370_1",
        "Atn370_2",
        "Atn400_1",
        "Atn400_2",
        "Atn470_1",
        "Atn470_2",
        "Atn520_1",
        "Atn520_2",
        "Atn590_1",
        "Atn590_2",
        "Atn630_1",
        "Atn630_2",
        "Atn880_1",
        "Atn880_2",
        "Atn950_1",
        "Atn950_2",
        "BC340_1",
        "BC340_2",
        "BC340",
        "BC370_1",
        "BC370_2",
        "BC370",
        "BC400_1",
        "BC400_2",
        "BC400",
        "BC470_1",
        "BC470_2",
        "BC470",
        "BC520_1",
        "BC520_2",
        "BC520",
        "BC590_1",
        "BC590_2",
        "BC590",
        "BC630_1",
        "BC630_2",
        "BC630",
        "BC880_1",
        "BC880_2",
        "BC880",
        "BC950_1",
        "BC950_2",
        "BC950",
        "K340",
        "K370",
        "K400",
        "K470",
        "K520",
        "K590",
        "K630",
        "K880",
        "K950",
        "BB",
        "Babs340",
        "Babs370",
        "Babs400",
        "Babs470",
        "Babs520",
        "Babs590",
        "Babs630",
        "Babs880",
        "Babs950",
        "BabsBrC340",
        "BabsBrC370",
        "BabsBrC400",
        "BabsBrC470",
        "BabsBrC520",
        "BabsBrC590",
        "BabsBrC630",
        "BabsBrC880",
        "BabsBrC950",
        "BrC",
        "Pressure",
        "Temp",
        "Flow1",
        "Flow2",
        "FlowC",
        "PumpDriver",
        "PumpSpeed",
        "ControllerTemp",
        "InletHumidity",
        "InletTemp",
        "TapeHumidity",
        "TapeTemp",
        "LedTemp",
        "LedSourceTemp",
        "TapeAdvanceCount",
        "TapeAdvanceLeft",
        "CPU",
    ]

    def __init__(self, context: StreamingContext):
        StreamingInstrument.__init__(self, context)
        _BaseInstrument.__init__(self, context)

        self.parameter_raw = self.parameters_record.string("instrument_parameters", attributes={
            'long_name': "instrument parameters table read response",
        })

        self._data_columns: typing.List[typing.Callable[[typing.Union[str, bytes]], None]] = [
            self._lookup_target(name) for name in self._DATA_COLUMNS
        ]

    def _field_split(self, line: bytes) -> typing.List[typing.Union[str, bytes]]:
        if self._CHECK_CSV.match(line):
            line = line.decode('utf-8', 'ignore')
            r = csv.reader((line,))
            try:
                return next(iter(r))
            except StopIteration:
                raise CommunicationsError("finvalid CSV line")
        return line.strip().split()

    async def start_communications(self) -> None:
        if not self.writer:
            raise CommunicationsError

        if self._instrument_timebase < self._report_interval:
            self._instrument_timebase = self._report_interval

        self.writer.write(b"$AERO:END\r")
        await self.writer.drain()
        await self.drain_reader(1.0)

        ts = time.gmtime()
        self.writer.write(f'$AERO:EXPORT SETUP "{ts.tm_year:04}-{ts.tm_mon:02}-{ts.tm_mday:02}" "{ts.tm_year:04}-{ts.tm_mon:02}-{ts.tm_mday:02}"\r'.encode('ascii'))
        resp = await self.read_multiple_lines(total=10.0, first=5.0, tail=1.0)
        if len(resp) < 2:
            raise CommunicationsError("invalid setup table export response")
        self.writer.write(b"$AERO:END\r")
        await self.writer.drain()
        await self.drain_reader(1.0)

        header = resp[0]
        fields = resp[-1]
        self.parameter_raw("\n".join([l.decode('utf-8', 'backslashreplace') for l in (header, fields)]))
        header = self._field_split(header.strip())
        fields = self._field_split(fields.strip())
        for index in range(min(len(header), len(fields))):
            name = header[index]
            if isinstance(name, bytes) or isinstance(name, bytearray):
                name = name.decode('ascii')
            if not name:
                continue
            if len(name) > 1 and name[0] == '"' and name[-1] == '"':
                name = name[1:-1]
            value = fields[index]
            self._lookup_target(name)(value)

        self.writer.write(b"$AERO:MAXID DATA\r")
        line: bytes = await wait_cancelable(self.read_line(), 2.0)
        try:
            read_id = int(line)
        except ValueError as e:
            raise CommunicationsError("invalid data table max ID") from e
        await self.drain_reader(0.5)

        self.writer.write(f"$AERO:FETCH DATA {read_id} C\r".encode('ascii'))
        await self.drain_reader(0.5)
        line: bytes = await wait_cancelable(self.read_line(), self._instrument_timebase + 2.0)
        self._process_record(line)
        await self.drain_reader(min(max(self._instrument_timebase / 2.0, 0.5), 2.0))

        self._normalization_changed = False
        self._spot_change_observed = False

    def _process_record(self, line: bytes) -> None:
        self._begin_data_report()

        if len(line) < 3:
            raise CommunicationsError
        fields = self._field_split(line.strip())
        if len(fields) != len(self._data_columns):
            raise CommunicationsError(f"invalid number of fields ({len(fields)} vs {len(self._data_columns)}) in {line}")
        for index in range(len(fields)):
            self._data_columns[index](fields[index])

    async def communicate(self) -> None:
        line: bytes = await wait_cancelable(self.read_line(), self._instrument_timebase + 2.0)
        self._process_record(line)
        self._complete_data_report()


class InstrumentUIDEP(HttpInstrument, _BaseInstrument):
    INSTRUMENT_TYPE = _INSTRUMENT_TYPE
    MANUFACTURER = "Magee"
    MODEL = "AE36"
    DISPLAY_LETTER = "A"
    TAGS = frozenset({"aerosol", "aethalometer", "absorption", _INSTRUMENT_TYPE})
    SERIAL_PORT = {'baudrate': 115200}
    INSTRUMENT_INFO_METADATA = {
        **StandardInstrument.INSTRUMENT_INFO_METADATA,
        **{
            'software_version': "instrument software version information",
        }
    }

    def __init__(self, context: HttpContext):
        HttpInstrument.__init__(self, context)
        _BaseInstrument.__init__(self, context)

        self._sleep_time: float = 0.0

    def _process_report(self, fields: typing.Dict[str, typing.Any]) -> None:
        model = fields.get("Device")
        if model:
            self.set_instrument_info('model', model)
        sn = fields.get("SN")
        if sn:
            if isinstance(sn, str):
                matched = _SERIAL_NUMBER.search(sn)
                if matched:
                    self.set_serial_number(matched.group(1))
                else:
                    self.set_serial_number(sn)
            else:
                self.set_serial_number(sn)

        components: typing.List[typing.Dict[str, typing.Any]] = fields.get("Components")
        if not components:
            raise CommunicationsError("no data components reported")

        for c in components:
            name = str(c.get("Component"))
            value = c.get("Value")
            self._lookup_target(name)(value)

    async def _poll_report(self) -> None:
        self._begin_data_report()

        fields = await self.get("values/complex/", json=True)
        if not isinstance(fields, dict):
            raise CommunicationsError
        self._process_report(fields)

    async def initialize_communications(self) -> bool:
        if self._instrument_timebase < self._report_interval:
            self._instrument_timebase = self._report_interval

        await self._poll_report()
        self._sleep_time = 0
        self._normalization_changed = False
        self._spot_change_observed = False
        return True

    async def step_communications(self) -> bool:
        if self._sleep_time > 0.0:
            await asyncio.sleep(self._sleep_time)
            self._sleep_time = 0.0
        begin_read = time.monotonic()
        await self._poll_report()
        self._complete_data_report()
        end_read = time.monotonic()

        self._sleep_time = self._instrument_timebase - (end_read - begin_read)
        return True
