import typing
import asyncio
import time
import logging
import re
from math import isfinite, nan, exp
from forge.tasks import wait_cancelable
from forge.units import mass_ng_to_ug, flow_ccm_to_lpm, pressure_Pa_to_hPa, ONE_ATM_IN_HPA
from ..streaming import StreamingInstrument, StreamingContext, CommunicationsError
from ..parse import parse_number, parse_date_and_time
from ..variable import Input

_LOGGER = logging.getLogger(__name__)
_INSTRUMENT_TYPE = __name__.split('.')[-2]
_FIELD_SPLIT = re.compile(rb"\s+")
_SERIAL_NUMBER = re.compile(r"AE33-S\d+-(\d+)")


class _ExternalSensor:
    def __init__(self, instrument: "Instrument"):
        self.instrument = instrument

    def process_fields(self, fields: typing.List[bytes]) -> typing.List[bytes]:
        return fields

    def report_complete(self) -> None:
        pass


class _IgnoredExternalSensor(_ExternalSensor):
    pass


class _AMES_TPR159(_ExternalSensor):
    def __init__(self, instrument: "Instrument"):
        super().__init__(instrument)
        self.data_T = self.instrument.input("T_AMES_TPR159")
        self.data_U = self.instrument.input("U_AMES_TPR159")
        self.data_P = self.instrument.input("P_AMES_TPR159")

        self.report = self.instrument.report(
            self.instrument.variable_air_temperature(self.data_T, "ames_tpr159_temperature", code="Tx",
                                                     attributes={'long_name': "AMES TPR159 ambient temperature"}),
            self.instrument.variable_air_rh(self.data_U, "ames_tpr159_humidity", code="Ux",
                                            attributes={'long_name': "AMES TPR159 ambient humidity"}),
            self.instrument.variable_air_pressure(self.data_P, "ames_tpr159_pressure", code="Px",
                                                  attributes={'long_name': "AMES TPR159 ambient pressure"}),
        )

    def process_fields(self, fields: typing.List[bytes]) -> typing.List[bytes]:
        try:
            (
                T, U, P,
                *fields
            ) = fields
        except ValueError:
            raise CommunicationsError(f"invalid external sensor fields")

        self.data_T(parse_number(T))
        self.data_U(parse_number(U))
        self.data_P(parse_number(P))

        return fields

    def report_complete(self) -> None:
        self.report()


class _Comet_T0310(_ExternalSensor):
    def __init__(self, instrument: "Instrument"):
        super().__init__(instrument)
        self.data_T = self.instrument.input("T_Comet_T0310")

        self.report = self.instrument.report(
            self.instrument.variable_air_temperature(self.data_T, "comet_t0310_temperature", code="Tx",
                                                     attributes={'long_name': "Comet T0310 ambient temperature"}),
        )

    def process_fields(self, fields: typing.List[bytes]) -> typing.List[bytes]:
        try:
            (
                T,
                *fields
            ) = fields
        except ValueError:
            raise CommunicationsError(f"invalid external sensor fields")

        self.data_T(parse_number(T))

        return fields

    def report_complete(self) -> None:
        self.report()


class _Vaisala_GMP343(_ExternalSensor):
    def __init__(self, instrument: "Instrument"):
        super().__init__(instrument)
        self.data_T = self.instrument.input("T_Vaisala_GMP343")
        self.data_X = self.instrument.input("X_Vaisala_GMP343")

        self.report = self.instrument.report(
            self.instrument.variable_temperature(self.data_T, "vaisala_gmp343_temperature", code="Tx",
                                                 attributes={'long_name': "Vaisala GMP343 temperature"}),
            self.instrument.variable_co2(self.data_X, "vaisala_gmp343_carbon_dioxide_mixing_ratio", code="Xx",
                                         attributes={'long_name': "Vaisala GMP343 fractional concentration of carbon dioxide"}),
        )

    def process_fields(self, fields: typing.List[bytes]) -> typing.List[bytes]:
        try:
            (
                X,
                _,  # CO2Raw
                T,
                *fields
            ) = fields
        except ValueError:
            raise CommunicationsError(f"invalid external sensor fields")

        self.data_T(parse_number(T))
        self.data_X(parse_number(X))

        return fields

    def report_complete(self) -> None:
        self.report()


class _TSI_4100(_ExternalSensor):
    def __init__(self, instrument: "Instrument"):
        super().__init__(instrument)
        self.data_Q = self.instrument.input("Q_TSI_4100")

        v = self.instrument.variable_flow(self.data_Q, "tsi_4100_flow", code="Qx",
                                          attributes={'long_name': "TSI 4100 flow"})
        v.data.use_standard_temperature = True
        v.data.use_standard_pressure = True
        self.report = self.instrument.report(v)

    def process_fields(self, fields: typing.List[bytes]) -> typing.List[bytes]:
        try:
            (
                Q,
                *fields
            ) = fields
        except ValueError:
            raise CommunicationsError(f"invalid external sensor fields")

        self.data_Q(parse_number(Q))

        return fields

    def report_complete(self) -> None:
        self.report()


class _InletDrier(_ExternalSensor):
    def __init__(self, instrument: "Instrument"):
        super().__init__(instrument)
        self.data_Tu = self.instrument.input("Tu_InletDrier")
        self.data_Uu = self.instrument.input("Uu_InletDrier")
        self.data_TDu = self.instrument.input("TDu_InletDrier")
        self.data_T = self.instrument.input("T_InletDrier")
        self.data_U = self.instrument.input("U_InletDrier")
        self.data_TD = self.instrument.input("TD_InletDrier")

        self.report = self.instrument.report(
            self.instrument.variable_air_temperature(self.data_Tu, "inlet_drier_inlet_temperature", code="Tu",
                                                     attributes={'long_name': "air temperature at the drier inlet"}),
            self.instrument.variable_air_rh(self.data_Uu, "inlet_drier_inlet_humidity", code="Uu",
                                            attributes={'long_name': "air humidity at the drier inlet"}),
            self.instrument.variable_air_dewpoint(self.data_TDu, "inlet_drier_inlet_humidity", code="TDu",
                                                  attributes={'long_name': "air dewpoint at the drier inlet"}),
            self.instrument.variable_temperature(self.data_Tu, "inlet_drier_inlet_temperature", code="Tx",
                                                 attributes={'long_name': "air temperature at the drier outlet"}),
            self.instrument.variable_rh(self.data_Uu, "inlet_drier_inlet_humidity", code="Ux",
                                        attributes={'long_name': "air humidity at the drier outlet"}),
            self.instrument.variable_dewpoint(self.data_TDu, "inlet_drier_inlet_humidity", code="TDx",
                                              attributes={'long_name': "air dewpoint at the drier outlet"}),
        )

    def process_fields(self, fields: typing.List[bytes]) -> typing.List[bytes]:
        try:
            (
                Tu, Uu, TDu,
                T, U, TD,
                *fields
            ) = fields
        except ValueError:
            raise CommunicationsError(f"invalid external sensor fields")

        self.data_Tu(parse_number(Tu))
        self.data_Uu(parse_number(Uu))
        self.data_TDu(parse_number(TDu))
        self.data_T(parse_number(T))
        self.data_U(parse_number(U))
        self.data_TD(parse_number(TD))

        return fields

    def report_complete(self) -> None:
        self.report()


class _AMES_VMT107(_ExternalSensor):
    def __init__(self, instrument: "Instrument"):
        super().__init__(instrument)
        self.data_WS = self.instrument.input("WS_AMES_VMT107")
        self.data_WD = self.instrument.input("WD_AMES_VMT107")

        self.report = self.instrument.report(
            *self.instrument.variable_winds(self.data_WS, self.data_WD, name_suffix="_ames_vmt107", code=""),
        )

    def process_fields(self, fields: typing.List[bytes]) -> typing.List[bytes]:
        try:
            (
                WS, WD,
                *fields
            ) = fields
        except ValueError:
            raise CommunicationsError(f"invalid external sensor fields")

        self.data_WS(parse_number(WS))
        self.data_WD(parse_number(WD))

        return fields

    def report_complete(self) -> None:
        self.report()


class _GMX(_ExternalSensor):
    def __init__(self, instrument: "Instrument"):
        super().__init__(instrument)
        self.data_T = self.instrument.input("T_GMX")
        self.data_U = self.instrument.input("U_GMX")
        self.data_P = self.instrument.input("P_GMX")
        self.data_WS = self.instrument.input("WS_GMX")
        self.data_WD = self.instrument.input("WD_GMX")

        self.report = self.instrument.report(
            self.instrument.variable_air_temperature(self.data_T, "gmx_temperature", code="Tx",
                                                     attributes={'long_name': "GMX ambient temperature"}),
            self.instrument.variable_air_rh(self.data_U, "gmx_humidity", code="Ux",
                                            attributes={'long_name': "GMX ambient humidity"}),
            self.instrument.variable_air_pressure(self.data_P, "gmx_pressure", code="Px",
                                                  attributes={'long_name': "GMX ambient pressure"}),
            *self.instrument.variable_winds(self.data_WS, self.data_WD, name_suffix="_gmx", code=""),
        )

    def process_fields(self, fields: typing.List[bytes]) -> typing.List[bytes]:
        try:
            (
                T, U, P, WS, WD,
                *fields
            ) = fields
        except ValueError:
            raise CommunicationsError(f"invalid external sensor fields")

        self.data_T(parse_number(T))
        self.data_U(parse_number(U))
        self.data_P(parse_number(P))
        self.data_WS(parse_number(WS))
        self.data_WD(parse_number(WD))

        return fields

    def report_complete(self) -> None:
        self.report()


_EXTERNAL_SENSORS: typing.Dict[int, typing.Type[_ExternalSensor]] = {
    1: _AMES_TPR159,
    2: _Comet_T0310,
    3: _Vaisala_GMP343,
    4: _TSI_4100,
    6: _InletDrier,
    11: _AMES_VMT107,
    18: _GMX,

    5: _IgnoredExternalSensor,  # Datalogger_AE33_protocol - port waiting
    7: _IgnoredExternalSensor,  # Datalogger_BH_protocol
    8: _IgnoredExternalSensor,  # Datalogger_Qair_protocol
    9: _IgnoredExternalSensor,  # AE33_DataStreaming
    12: _IgnoredExternalSensor,  # Datalogger_BH_protocol2
    13: _IgnoredExternalSensor,  # Datalogger_BH_protocol3
}


class Instrument(StreamingInstrument):
    INSTRUMENT_TYPE = _INSTRUMENT_TYPE
    MANUFACTURER = "Magee"
    MODEL = "AE33"
    DISPLAY_LETTER = "E"
    TAGS = frozenset({"aerosol", "aethalometer", "absorption", _INSTRUMENT_TYPE})
    SERIAL_PORT = {'baudrate': 115200}

    WAVELENGTHS = (
        (370.0, "1"),
        (470.0, "2"),
        (520.0, "3"),
        (590.0, "4"),
        (660.0, "5"),
        (880.0, "6"),
        (950.0, "7"),
    )

    def __init__(self, context: StreamingContext):
        super().__init__(context)

        self._report_interval: float = float(context.config.get('REPORT_INTERVAL', default=1.0))
        self._instrument_timebase: float = self._report_interval
        self._sleep_time: float = 0.0

        self._ebc_zero_is_mvc: bool = bool(context.config.get('ZERO_EBC_IS_MVC', default=True))

        ebc_efficiency: typing.Union[typing.List[float], float] = context.config.get('EBC_EFFICIENCY')
        self._ebc_efficiency: typing.List[float] = list()
        for i in range(len(self.WAVELENGTHS)):
            wl = self.WAVELENGTHS[i][0]
            if isinstance(ebc_efficiency, list) and i < len(ebc_efficiency):
                e = float(ebc_efficiency[i])
            elif ebc_efficiency is not None and ebc_efficiency > 0.0:
                e = ebc_efficiency / wl
            else:
                e = 6833.0 / wl
            self._ebc_efficiency.append(e)

        self._weingartner_constant = float(context.config.get('WEINGARTNER_CONSTANT', default=1.57))

        self.data_Q1 = self.input("Q1")
        self.data_Q2 = self.input("Q2")
        self.data_Tcontroller = self.input("Tcontroller")
        self.data_Tsupply = self.input("Tsupply")
        self.data_Tled = self.input("Tled")
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
        for _, code in self.WAVELENGTHS:
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
        self.data_wavelength([wl for wl, _ in self.WAVELENGTHS])
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

        def at_stp(s: Instrument.Variable):
            s.data.use_standard_pressure = True
            s.data.use_standard_temperature = True
            return s

        self.notify_spot_advancing = self.notification('spot_advancing')
        self.notify_flow_check_history = self.notification('flow_check_history')
        self.notify_stability_test = self.notification('stability_test')
        self.notify_clear_air_test = self.notification('clear_air_test')
        self.notify_change_tape_test = self.notification('change_tape_test')
        self.notify_tape_low = self.notification('tape_low')
        self.notify_tape_critical = self.notification('tape_critical')
        self.notify_tape_error = self.notification('tape_error')
        self.notify_not_measuring = self.notification('not_measuring')
        self.notify_calibrating = self.notification('calibrating')
        self.notify_stopped = self.notification('stopped')
        self.notify_flow_out_of_range = self.notification('flow_out_of_range')
        self.notify_led_calibration = self.notification('led_calibration')
        self.notify_led_calibration_error = self.notification('led_calibration_error')
        self.notify_led_error = self.notification('led_error')
        self.notify_chamber_error = self.notification('chamber_error')
        self.notify_controller_not_ready = self.notification('controller_not_ready')
        self.notify_controller_busy = self.notification('controller_busy')
        self.notify_detector_initialization_error = self.notification('detector_initialization_error')
        self.notify_detector_stopped = self.notification('detector_stopped')
        self.notify_detector_led_calibration = self.notification('detector_led_calibration')
        self.notify_detector_fast_led_calibration = self.notification('detector_fast_led_calibration')
        self.notify_detector_read_ndf0 = self.notification('detector_read_ndf0')
        self.notify_detector_read_ndf1 = self.notification('detector_read_ndf1')
        self.notify_detector_read_ndf2 = self.notification('detector_read_ndf2')
        self.notify_detector_read_ndf3 = self.notification('detector_read_ndf3')
        self.notify_detector_read_ndf_error = self.notification('detector_read_ndf_error')

        dimension_wavelength = self.dimension_wavelength(self.data_wavelength)
        self.instrument_report = self.report(
            at_stp(self.variable_ebc(self.data_X, dimension_wavelength, code="X")),
            at_stp(self.variable_absorption(self.data_Bac, dimension_wavelength, code="Bac")),

            at_stp(self.variable_absorption(self.data_Ba, dimension_wavelength, "spot_one_light_absorption",
                                            code="Ba", attributes={
                'long_name': "uncorrected light absorption coefficient at STP on spot one",
                'standard_name': None,
            })),
            at_stp(self.variable_absorption(self.data_Bas, dimension_wavelength, "spot_two_light_absorption",
                                            code="Bas", attributes={
                'long_name': "uncorrected light absorption coefficient at STP on spot two",
                'standard_name': None,
            })),
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

            self.variable_flow(self.data_Q1, "spot_one_flow", code="Q1",
                               attributes={'C_format': "%7.3f"}),
            self.variable_flow(self.data_Q2, "spot_two_flow", code="Q2",
                               attributes={'C_format': "%7.3f"}),
            self.variable_temperature(self.data_Tcontroller, "controller_temperature", code="T1",
                                      attributes={'long_name': "controller board temperature"}),
            self.variable_temperature(self.data_Tsupply, "supply_temperature", code="T2",
                                      attributes={'long_name': "power supply board temperature"}),
            self.variable_temperature(self.data_Tled, "led_temperature", code="T2",
                                      attributes={'long_name': "LED board temperature"}),

            flags=[
                self.flag(self.notify_spot_advancing),
                self.flag(self.notify_flow_check_history),
                self.flag(self.notify_stability_test),
                self.flag(self.notify_clear_air_test),
                self.flag(self.notify_change_tape_test),
                self.flag(self.notify_tape_low),
                self.flag(self.notify_tape_critical),
                self.flag(self.notify_tape_error),
                self.flag(self.notify_not_measuring),
                self.flag(self.notify_calibrating),
                self.flag(self.notify_stopped),
                self.flag(self.notify_flow_out_of_range),
                self.flag(self.notify_led_calibration),
                self.flag(self.notify_led_calibration_error),
                self.flag(self.notify_led_error),
                self.flag(self.notify_chamber_error),
                self.flag(self.notify_controller_not_ready),
                self.flag(self.notify_controller_busy),
                self.flag(self.notify_detector_initialization_error),
                self.flag(self.notify_detector_stopped),
                self.flag(self.notify_detector_led_calibration),
                self.flag(self.notify_detector_fast_led_calibration),
                self.flag(self.notify_detector_read_ndf0),
                self.flag(self.notify_detector_read_ndf1),
                self.flag(self.notify_detector_read_ndf2),
                self.flag(self.notify_detector_read_ndf3),
                self.flag(self.notify_detector_read_ndf_error),
            ],

            auxiliary_variables=(
                [self.variable(w) for w in self.data_Ba_wavelength] +
                [self.variable(w) for w in self.data_X_wavelength] +
                [self.variable(w) for w in self.data_correction_factor_wavelength] +
                [self.variable_last_valid(w) for w in self.data_Ir_wavelength]
            ),
        )
        self.instrument_report.record.data_record.standard_temperature = 0.0
        self.instrument_report.record.data_record.standard_pressure = ONE_ATM_IN_HPA

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

        self._active_external_sensors: typing.Dict[int, _ExternalSensor] = dict()

        self.context.bus.connect_command('spot_advance', self._command_spot_advance)
        self._spot_advanced_queued: bool = False

        self.parameters_record = self.context.data.constant_record("parameters")
        self.parameter_sg = self.parameters_record.string("instrument_parameters", attributes={
            'long_name': "instrument response to the $AE33:SG command, representing the raw parameters values from the instrument configuration without formatting context",
        })
        self.parameters_record.array_float_attr("mass_absorption_efficiency", self, '_ebc_efficiency', attributes={
            'long_name': "the efficiency factor used to convert absorption coefficients into an equivalent black carbon",
            'units': "m2 g",
        })

    def _command_spot_advance(self, _) -> None:
        _LOGGER.debug("Received spot advance command")
        self._spot_advanced_queued = True

    async def _read_parameters_line(self) -> bytes:
        line = bytearray()

        async def _append_line():
            nonlocal line
            while len(line) < 65536:
                d = await self.reader.read(1)
                if not d:
                    break
                if d == b'\r' or d == b'\n':
                    line = line.strip()
                    if line:
                        break
                    line.clear()
                    continue
                line += d

        self.writer.write(b"$AE33:SG\r")
        try:
            await wait_cancelable(_append_line(), 2.0)
        except asyncio.TimeoutError:
            # Sometimes the instrument will not actually flush its send buffer until a record read is issued
            self.writer.write(b"$AE33:D1\r")
            await wait_cancelable(_append_line(), 2.0)

        return bytes(line)

    async def start_communications(self) -> None:
        if self._instrument_timebase < self._report_interval:
            self._instrument_timebase = self._report_interval

        if self.writer:
            # Clear anything in the send buffer on the instrument
            self.writer.write(b"$AE33:D1\r")
            await self.writer.drain()
            await self.drain_reader(1.0)

            data: bytes = await self._read_parameters_line()
            try:
                decoded = data.decode('utf-8')
            except UnicodeDecodeError:
                decoded = None

            self.parameter_sg(decoded)
            if decoded:
                matched = _SERIAL_NUMBER.search(decoded)
                if matched:
                    self.set_serial_number(matched.group(1))

        await self.drain_reader(0.5)
        if self.writer:
            self.writer.write(b"$AE33:D1\r")
            await self.writer.drain()
            await wait_cancelable(self.read_line(), 2.0)

            self.writer.write(b"$AE33:D1\r")
            line: bytes = await wait_cancelable(self.read_line(), 2.0)
            self._process_record(line)
        else:
            await wait_cancelable(self.read_line(), self._instrument_timebase + 2.0)
            line: bytes = await wait_cancelable(self.read_line(), self._instrument_timebase + 2.0)
            self._process_record(line)

        self._sleep_time = 0.0
        self._normalization_changed = False
        self._spot_change_observed = False

    def _process_record(self, line: bytes) -> None:
        if len(line) < 3:
            raise CommunicationsError
        fields = _FIELD_SPLIT.split(line.strip())
        try:
            (raw_data, raw_time, time_base, *fields) = fields
        except ValueError:
            raise CommunicationsError(f"invalid number of fields in {line}")

        parse_date_and_time(raw_data, raw_time, date_separator=b'/')
        try:
            time_base = int(time_base)
            if time_base <= 0:
                raise ValueError
        except (ValueError, OverflowError):
            raise CommunicationsError(f"invalid time base {time_base}")

        if len(fields) < len(self.WAVELENGTHS) * 3:
            raise CommunicationsError(f"invalid number of fields in {line}")
        for i in range(len(self.WAVELENGTHS)):
            base = i * 3
            self.data_If_wavelength[i](parse_number(fields[base]))
            self.data_Ip_wavelength[i](parse_number(fields[base + 1]))
            self.data_Ips_wavelength[i](parse_number(fields[base + 2]))
        fields = fields[len(self.WAVELENGTHS)*3:]

        try:
            (
                Q1, Q2,
                _,  # Total flow
                report_P, report_T,
                _,  # Sample RH (not present)
                Tcontroller, Tsupply,
                status_main, status_controller, status_detector, status_led, status_valve,
                Tled,
                *fields
            ) = fields
        except ValueError:
            raise CommunicationsError(f"invalid number of fields in {line}")

        if len(fields) < len(self.WAVELENGTHS) * 3:
            raise CommunicationsError(f"invalid number of fields in {line}")
        all_ebc_zero = True
        for i in range(len(self.WAVELENGTHS)):
            base = i * 3
            Xa = parse_number(fields[base])
            Xb = parse_number(fields[base+1])
            X = parse_number(fields[base+2])
            all_ebc_zero = all_ebc_zero and Xa == 0.0 and Xb == 0.0 and X == 0.0
            self.data_Xa_wavelength[i](mass_ng_to_ug(Xa))
            self.data_Xb_wavelength[i](mass_ng_to_ug(Xb))
            self.data_X_wavelength[i](mass_ng_to_ug(X))
        fields = fields[len(self.WAVELENGTHS)*3:]

        if len(fields) < len(self.WAVELENGTHS):
            raise CommunicationsError(f"invalid number of fields in {line}")
        for i in range(len(self.WAVELENGTHS)):
            self.data_correction_factor_wavelength[i](parse_number(fields[i]))
        fields = fields[len(self.WAVELENGTHS):]

        if len(fields) == 1:
            # Allow omitted sensor data
            Fn = fields[0]
        else:
            try:
                (Fn, sensor_type_1, sensor_type_2, sensor_type_3, *sensor_fields) = fields
            except ValueError:
                raise CommunicationsError(f"invalid number of fields in {line}")

            for sensor_type in (sensor_type_1, sensor_type_2, sensor_type_3):
                try:
                    sensor_type = int(sensor_type.strip())
                except (ValueError, OverflowError):
                    raise CommunicationsError(f"invalid sensor type {sensor_type}")
                if sensor_type == 0:
                    continue

                active_sensor = self._active_external_sensors.get(sensor_type)
                if active_sensor is None:
                    sensor_class = _EXTERNAL_SENSORS.get(sensor_type)
                    if sensor_class is None:
                        _LOGGER.warning(f"Unknown external sensor type {sensor_type}, assuming zero fields")
                        sensor_class = _IgnoredExternalSensor
                    active_sensor = sensor_class(self)
                    self._active_external_sensors[sensor_type] = active_sensor

                fields = active_sensor.process_fields(fields)

        report_P = pressure_Pa_to_hPa(parse_number(report_P))
        report_T = parse_number(report_T)

        self.data_Q1(flow_ccm_to_lpm(parse_number(Q1)))
        self.data_Q2(flow_ccm_to_lpm(parse_number(Q2)))
        self.data_Tcontroller(parse_number(Tcontroller))
        self.data_Tsupply(parse_number(Tsupply))
        self.data_Tled(parse_number(Tled))
        try:
            Fn = int(Fn)
        except (ValueError, OverflowError):
            raise CommunicationsError(f"invalid tape advance count {Fn}")
        if self.data_Fn.value is not None and self.data_Fn.value != Fn:
            self._normalization_changed = True
            self._spot_change_observed = True
        self.data_Fn(Fn)

        try:
            status_main = int(status_main)
            status_controller = int(status_controller)
            status_detector = int(status_detector)
            status_led = int(status_led)
            status_valve = int(status_valve)
        except (ValueError, OverflowError) as e:
            raise CommunicationsError(f"invalid status in {line}") from e

        bits = status_main & 0x3
        self.notify_not_measuring((bits == 1))
        self.notify_calibrating((bits == 2))
        self.notify_stopped((bits == 3))
        bits = (status_main >> 2) & 0x3
        self.notify_flow_out_of_range((bits & 0x1) != 0)
        self.notify_flow_check_history((bits & 0x2) != 0)
        bits = (status_main >> 4) & 0x3
        self.notify_led_calibration((bits == 1))
        self.notify_led_calibration_error((bits == 2))
        have_led_error = (bits == 3)
        bits = (status_main >> 5) & 0x1
        self.notify_chamber_error((bits == 1))
        bits = (status_main >> 8) & 0x3
        self.notify_tape_low((bits == 1) or (bits == 2))
        self.notify_tape_critical((bits == 2))
        self.notify_tape_error((bits == 3))
        bits = (status_main >> 10) & 0x3
        self.notify_stability_test((bits == 1))
        self.notify_clear_air_test((bits == 2))
        self.notify_change_tape_test((bits == 3))

        self.notify_controller_not_ready((status_controller == 100))
        self.notify_controller_busy((status_controller == 255))

        self.notify_detector_initialization_error((status_detector == 0))
        # 10 = normal measurement
        self.notify_detector_stopped((status_detector == 20))
        self.notify_detector_led_calibration((status_detector == 30))
        self.notify_detector_fast_led_calibration((status_detector == 40))
        self.notify_detector_read_ndf0((status_detector == 55))
        self.notify_detector_read_ndf1((status_detector == 56))
        self.notify_detector_read_ndf2((status_detector == 57))
        self.notify_detector_read_ndf3((status_detector == 58))
        self.notify_detector_read_ndf_error((status_detector == 59))

        have_led_error = have_led_error or (status_led == 0)
        # 10 = normal measurement
        self.notify_led_error(have_led_error)

        if self._ebc_zero_is_mvc and all_ebc_zero:
            for w in self.data_X_wavelength:
                w(nan)
            for w in self.data_Xa_wavelength:
                w(nan)
            for w in self.data_Xb_wavelength:
                w(nan)
            self._normalization_changed = True

        for i in range(len(self.WAVELENGTHS)):
            self.data_Bac_wavelength[i](self.data_X_wavelength[i].value * self._ebc_efficiency[i])
            self.data_Ba_wavelength[i](self.data_Xa_wavelength[i].value * self._ebc_efficiency[i])
            self.data_Bas_wavelength[i](self.data_Xb_wavelength[i].value * self._ebc_efficiency[i])

        self.instrument_report.record.data_record.standard_temperature = report_T
        self.instrument_report.record.data_record.standard_pressure = report_P
        self._instrument_timebase = float(time_base)

    def _normalization_ready(self) -> bool:
        if self.notify_not_measuring.value:
            return False
        if self.notify_calibrating.value:
            return False
        if self.notify_stopped.value:
            return False

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
        if self.notify_not_measuring.value:
            return
        if self.notify_calibrating.value:
            return
        if self.notify_stopped.value:
            return

        def _calculate(data_In0, Ip_wavelength, Ba_wavelength):
            In0: typing.Optional[typing.List[float]] = None
            for i in range(len(self.WAVELENGTHS)):
                if data_In0.value and isfinite(data_In0.value[i]):
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
            if In0 and isfinite(In0[i]) and In0[i] != 0.0:
                self.data_Ir_wavelength[i](In[i] / In0[i])
            else:
                self.data_Ir_wavelength[i](nan)
            if Ins0 and isfinite(Ins0[i]) and Ins0[i] != 0.0:
                self.data_Irs_wavelength[i](Ins[i] / Ins0[i])
            else:
                self.data_Irs_wavelength[i](nan)

    async def communicate(self) -> None:
        if self.writer and self._sleep_time > 0.0:
            await asyncio.sleep(self._sleep_time)
            self._sleep_time = 0.0
        begin_read = time.monotonic()

        if self._spot_advanced_queued and self.writer:
            _LOGGER.debug("Cycling instrument run mode to advance the spot")
            self._spot_advanced_queued = False
            self.writer.write(b"$AE33:X0\r")
            await asyncio.sleep(5.0)
            self.writer.write(b"$AE33:X1\r")
            await asyncio.sleep(5.0)
            _LOGGER.debug("Instrument run resumed")

        if self.writer:
            self.writer.write(b"$AE33:D1\r")
            line: bytes = await wait_cancelable(self.read_line(), 2.0)
        else:
            line: bytes = await wait_cancelable(self.read_line(), self._instrument_timebase + 2.0)
        self._process_record(line)

        self.notify_spot_advancing(self._normalization_changed)
        if self._normalization_changed and self._normalization_ready():
            self._normalization_changed = False
            if self._spot_change_observed:
                In, Ins = self._normalized_intensities()
                _LOGGER.debug("Spot change observed, capturing normalized intensities")
                self.data_In0(In, oneshot=True)
                self.data_Ins0(Ins, oneshot=True)
            else:
                self.data_In0([nan] * len(self.WAVELENGTHS), oneshot=True)
                self.data_Ins0([nan] * len(self.WAVELENGTHS), oneshot=True)
            self._spot_change_observed = False

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

            self.data_X([nan] * len(self.WAVELENGTHS))
            self.data_Bac([nan] * len(self.WAVELENGTHS))
            self.data_Ba([nan] * len(self.WAVELENGTHS))
            self.data_Bas([nan] * len(self.WAVELENGTHS))

        self.data_Ir([float(c) for c in self.data_Ir_wavelength])
        self.data_Irs([float(c) for c in self.data_Irs_wavelength])
        self.data_If([float(c) for c in self.data_If_wavelength])
        self.data_Ip([float(c) for c in self.data_Ip_wavelength])
        self.data_Ips([float(c) for c in self.data_Ips_wavelength])
        self.data_correction_factor([float(c) for c in self.data_correction_factor_wavelength])

        self.instrument_report.record.data_record.report_interval = self._instrument_timebase
        self.instrument_report()
        for sensor in self._active_external_sensors.values():
            sensor.report_complete()

        end_read = time.monotonic()
        self._sleep_time = self._instrument_timebase - (end_read - begin_read)
