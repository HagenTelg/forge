import typing
import asyncio
import random
from math import isfinite
from json import dumps as to_json, loads as from_json, JSONDecodeError
from forge.tasks import wait_cancelable
from forge.units import pressure_kPa_to_hPa
from ..streaming import StreamingInstrument, StreamingContext, CommunicationsError


class Instrument(StreamingInstrument):
    INSTRUMENT_TYPE = "tsi375xcpc"
    MANUFACTURER = "TSI"
    MODEL = "375x"
    DISPLAY_LETTER = "C"
    TAGS = frozenset({"aerosol", "cpc", "tsi375xcpc"})

    def __init__(self, context: StreamingContext):
        super().__init__(context)

        self._transaction_id: int = 100
        self._concentration_subscription_id: typing.Optional[int] = None
        self._reported_model: typing.Optional[str] = None

        self.data_N = self.input("N")
        self.data_C = self.input("C")
        self.data_P = self.input("P")
        self.data_PDinlet = self.input("PDinlet")
        self.data_PDnozzle = self.input("PDnozzle")
        self.data_PDorifice = self.input("PDorifice")
        self.data_Alaser = self.input("Alaser")
        self.data_PCT = self.input("PCT")
        self.data_liquid_level = self.input("liquid_level")

        self.data_Q = self.input("Q")
        self.data_Qinlet = self.input("Qinlet")
        self.data_Qinstrument = self.input("Qinstrument")

        self.data_Tsaturator = self.input("Tsaturator")
        self.data_Tcondenser = self.input("Tcondenser")
        self.data_Toptics = self.input("Toptics")
        self.data_Tcabinet = self.input("Tcabinet")
        self.data_Twatertrap = self.input("Twatertrap")

        if not self.data_N.field.comment and self.data_Q.field.comment:
            self.data_N.field.comment = self.data_Q.field.comment
        if not self.data_N.field.comment and self.data_Qinstrument.field.comment:
            self.data_N.field.comment = self.data_Qinstrument.field.comment

        self.variable_saturator = self.variable_temperature(
            self.data_Tsaturator, "saturator_temperature", code="T1",
            attributes={'long_name': "saturator block temperature"}
        )
        self.variable_condenser = self.variable_temperature(
            self.data_Tcondenser, "condenser_temperature", code="T2",
            attributes={'long_name': "condenser temperature"}
        )
        self.variable_water_trap = self.variable_temperature(
            self.data_Twatertrap, "water_trap_temperature", code="T5",
            attributes={'long_name': "water trap temperature"}
        )

        self.device_status_notify = {
            'SaturatorError': self.notification("saturator_temperature_out_of_range", is_warning=True),
            'SaturatorWarn': self.notification("saturator_temperature_warning"),
            'CondenserError': self.notification("condenser_temperature_out_of_range", is_warning=True),
            'CondenserWarn': self.notification("condenser_temperature_warning"),
            'OpticsError': self.notification("optics_temperature_error", is_warning=True),
            'OpticsWarn': self.notification("optics_temperature_warning"),
            'WaterTrapError': self.notification("water_trap_temperature_error", is_warning=True),
            'WaterTrapWarn': self.notification("water_trap_temperature_warning"),
            'OrificeError': self.notification("orifice_pressure_drop_error", is_warning=True),
            'OrificeWarn': self.notification("orifice_pressure_drop_warning"),
            'NozzleError': self.notification("nozzle_pressure_drop_error", is_warning=True),
            'NozzleWarn': self.notification("nozzle_pressure_drop_warning"),
            'InletPresError': self.notification("inlet_pressure_drop_error", is_warning=True),
            'InletPresWarn': self.notification("inlet_pressure_drop_warning"),
            'PulseHeightError': self.notification("pulse_height_error", is_warning=True),
            'PulseHeightWarn': self.notification("pulse_height_warning"),
            'CaseTemp': self.notification("case_temperature_error", is_warning=True),
            'AbsolutePressure': self.notification("absolute_pressure_error", is_warning=True),
            'SampleFlowRateError': self.notification("sample_flow_error", is_warning=True),
            'SampleFlowRateWarn': self.notification("sample_flow_warning"),
            'LiquidLevel': self.notification("liquid_low", is_warning=True),
            'LaserCurrentError': self.notification("laser_power_error", is_warning=True),
            'TiltAngleError': self.notification("instrument_tilt_detected", is_warning=True),
            'OverConcentration': self.notification("concentration_out_of_range"),
        }
        self.device_status_remap = {
            "InitiatorError": "SaturatorError",
            "InitiatorWarn": "SaturatorWarn",
            "ConditionerWarn": "CondenserWarn",
            "CondenserError": "CondenserError",
            "ModeratorWarn": "WaterTrapWarn",
            "ModeratorError": "WaterTrapError",
        }

        self.instrument_report = self.report(
            self.variable_number_concentration(self.data_N, code="N"),

            self.variable_sample_flow(self.data_Q, code="Q",
                                      attributes={'C_format': "%5.3f"}),
            self.variable_flow(self.data_Qinlet, "inlet_flow", code="Qu", attributes={
                'long_name': "inlet flow rate",
                'C_format': "%5.3f",
            }),

            self.variable_air_pressure(self.data_P, "pressure", code="P",
                                       attributes={'long_name': "absolute pressure"}),

            self.variable_delta_pressure(self.data_PDorifice, "orifice_pressure_drop", code="Pd2", attributes={
                'long_name': "orifice pressure drop",
                'C_format': "%4.0f",
            }),
            self.variable_delta_pressure(self.data_PDinlet, "inlet_pressure_drop", code="Pdu", attributes={
                'long_name': "inlet pressure drop to ambient",
                'C_format': "%6.2f",
            }),

            self.variable_saturator,
            self.variable_condenser,
            self.variable_temperature(self.data_Tcabinet, "cabinet_temperature", code="T4",
                                      attributes={'long_name': "internal cabinet temperature"}),


            flags=[
            ],
        )

        self.report_not_3757: typing.Optional[StreamingInstrument.Report] = None
        self.report_3757_3789: typing.Optional[StreamingInstrument.Report] = None

    async def _read_json(self) -> typing.Dict[str, typing.Any]:
        contents = bytearray()
        while len(contents) < 65536:
            d = await self.reader.read(1)
            if not d:
                break
            contents += d
            if d != b'}':
                continue
            try:
                return from_json(contents)
            except JSONDecodeError:
                continue
        raise CommunicationsError("response too long")

    async def _command_response(self, command: typing.Dict[str, typing.Any]) -> typing.Dict[str, typing.Any]:
        self.writer.write(to_json(command).encode('ascii') + b"\n")
        response = await wait_cancelable(self._read_json(), 5.0)
        if not isinstance(response, dict):
            raise CommunicationsError("invalid response type")
        return response

    async def _read(self, element: str, parameter: str = None) -> typing.Any:
        self._transaction_id += 1
        if self._transaction_id > 10000:
            self._transaction_id = 100

        command = {
            'command': 'READ',
            'element': element,
            'transactionID': self._transaction_id,
        }
        if parameter:
            command['parameter'] = {'parameter': None}
        response = await self._command_response(command)

        response_type = response.get('command')
        if response_type != 'RESPONSE':
            raise CommunicationsError(f"invalid command response {response_type}")
        response_status = response.get('status')
        if response_status != 'OK':
            raise CommunicationsError(f"invalid command status {response_status}")
        received_transaction_id = response.get('transactionID')
        if received_transaction_id != self._transaction_id:
            raise CommunicationsError(f"unexpected transaction ID {received_transaction_id} instead of {self._transaction_id}")
        return response.get('value')

    async def _subscribe(self, element: str, subscription_id: int) -> typing.Any:
        self._transaction_id += 1
        if self._transaction_id > 10000:
            self._transaction_id = 100

        command = {
            'command': 'SUBSCRIBE',
            'element': element,
            'transactionID': self._transaction_id,
            'subscriptionID': subscription_id,
        }
        response = await self._command_response(command)

        response_type = response.get('command')
        if response_type != 'RESPONSE':
            raise CommunicationsError(f"invalid subscription response {response_type}")
        response_status = response.get('status')
        if response_status != 'OK':
            raise CommunicationsError(f"invalid subscription status {response_status}")
        received_element = response.get('element')
        if received_element != element:
            raise CommunicationsError(f"invalid subscription element {received_element}")
        received_transaction_id = response.get('transactionID')
        if received_transaction_id != self._transaction_id:
            raise CommunicationsError(f"unexpected transaction ID {received_transaction_id} instead of {self._transaction_id}")
        received_subscription_id = response.get('subscriptionID')
        if received_subscription_id != subscription_id:
            raise CommunicationsError(f"unexpected subscription ID {received_subscription_id} instead of {subscription_id}")
        return response.get('value')

    def _update_model_report(self, model: str) -> None:
        self._reported_model = model

        if model != "3757" and not self.report_not_3757:
            self.report_not_3757 = self.report(
                self.variable_delta_pressure(self.data_PDnozzle, "nozzle_pressure_drop", code="Pd1", attributes={
                    'long_name': "nozzle pressure drop",
                    'C_format': "%6.2f",
                }),

                self.variable_temperature(self.data_Toptics, "optics_temperature", code="T3",
                                          attributes={'long_name': "optics block temperature"}),

                self.variable(self.data_Alaser, "laser_current", code="A", attributes={
                    'long_name': "laser current",
                    'units': "mA",
                    'C_format': "%3.0f"
                }),
                self.variable(self.data_PCT, "pulse_height", code="PCT", attributes={
                    'long_name': "pulse height indicator",
                    'units': "%",
                    'C_format': "%3.0f"
                }),

                automatic=False,
            )
        if model in ("3757", "3789") and not self.report_3757_3789:
            self.report_not_3757 = self.report(
                self.variable_water_trap,

                automatic=False,
            )

        if model == "3789":
            self.variable_saturator.data.name = "initiator_temperature"
            self.variable_saturator.data.attributes["long_name"] = "initiator temperature"

            self.variable_condenser.data.name = "conditioner_temperature"
            self.variable_condenser.data.attributes["long_name"] = "conditioner temperature"

            self.variable_water_trap.data.name = "moderator_temperature"
            self.variable_water_trap.data.attributes["long_name"] = "moderator temperature"
        else:
            self.variable_saturator.data.name = "saturator_temperature"
            self.variable_saturator.data.attributes["long_name"] = "saturator temperature"

            self.variable_condenser.data.name = "condenser_temperature"
            self.variable_condenser.data.attributes["long_name"] = "condenser temperature"

            self.variable_water_trap.data.name = "water_trap_temperature"
            self.variable_water_trap.data.attributes["long_name"] = "water trap temperature"

    async def start_communications(self) -> None:
        if self.writer:
            await self.writer.drain()
            await self.drain_reader(0.25)

            device_record = await self._read('deviceRecord')
            model = device_record.get('modelNumber')
            if model:
                model = str(model).strip()
                if not model.startswith("375") and model != "3789":
                    raise CommunicationsError(f"unsupported model {model}")
                self.set_instrument_info('model', model)
                self._update_model_report(model)

            serial_number = device_record.get('serialNumber')
            if serial_number:
                self.set_serial_number(serial_number)
            firmware_version = device_record.get('firmwareVersion')
            if firmware_version:
                self.set_firmware_version(firmware_version)
            calibration = device_record.get('calibrationDate')
            if calibration:
                self.set_instrument_info('calibration', calibration)

            if not self._concentration_subscription_id:
                self._concentration_subscription_id = random.randint(100, 10000)
            await self._subscribe("Concentration", self._concentration_subscription_id)
        else:
            self._concentration_subscription_id = None

        await self.communicate()

    async def communicate(self) -> None:
        async def _read_subscribed():
            while True:
                data = await self._read_json()
                if data.get('status') != 'OK':
                    raise CommunicationsError("invalid instrument status")
                if self._concentration_subscription_id:
                    if data.get('subscriptionID') != self._concentration_subscription_id:
                        continue
                if data.get('command') != 'PUBLISH':
                    continue
                if data.get('element') != 'Concentration':
                    continue
                value = data.get('value')
                if not isinstance(value, dict):
                    raise CommunicationsError("invalid subscription data")
                return value

        values = await wait_cancelable(_read_subscribed(), 5.0)

        def field_value(field: str) -> float:
            v = values.get(field)
            if v is None:
                raise ValueError
            if isinstance(v, list):
                v = v[0]
            v = float(v)
            if not isfinite(v):
                raise ValueError
            return v

        def optional_field_value(field: str) -> typing.Optional[float]:
            try:
                return field_value(field)
            except ValueError:
                return None

        Qinlet = field_value('inletFlow')
        Tcabinet = field_value('cabinetTemp')
        PDinlet = pressure_kPa_to_hPa(field_value('inletPressure'))
        PDorifice = pressure_kPa_to_hPa(field_value('orificePressure'))
        Qinstrument = field_value('sampleFlowRate')
        liquid_level = field_value('liquidLevel')
        N = optional_field_value('concentration')
        C = optional_field_value('counts1Second')
        PCT = optional_field_value('pulseHeight')
        Toptics = optional_field_value('opticsTemp')
        P = pressure_kPa_to_hPa(field_value('ambientPressure'))
        PDnozzle = pressure_kPa_to_hPa(field_value('nozzlePressure'))
        Alaser = optional_field_value('laserCurrent')

        try:
            Tcondenser = field_value('condenserTemp')
        except ValueError:
            Tcondenser = field_value('conditionerTemp')
            self.variable_condenser.data.name = "conditioner_temperature"
            self.variable_condenser.data.attributes["long_name"] = "conditioner temperature"
        try:
            Tsaturator = field_value('saturatorTemp')
        except ValueError:
            Tsaturator = field_value('initiatorTemp')
            self.variable_saturator.data.name = "initiator_temperature"
            self.variable_saturator.data.attributes["long_name"] = "initiator temperature"
        try:
            Twatertrap = field_value('waterTrapTemp')
            if Twatertrap == 0.0 and self._reported_model != "3757":
                Twatertrap = None
        except ValueError:
            try:
                Twatertrap = field_value('moderatorTemp')
                self.variable_water_trap.data.name = "moderator_temperature"
                self.variable_water_trap.data.attributes["long_name"] = "moderator temperature"
            except ValueError:
                Twatertrap = None

        device_status = values.get("deviceStatus") or []
        device_status = set(device_status)

        Qinstrument = self.data_Qinstrument(Qinstrument)
        Q = self.data_Q(Qinstrument)
        if N is not None:
            N *= Qinstrument / Q
            self.data_N(N)
        if C is not None:
            self.data_C(C)

        self.data_Qinlet(Qinlet)
        self.data_liquid_level(liquid_level)
        self.data_P(P)
        self.data_PDinlet(PDinlet)
        self.data_PDnozzle(PDnozzle)
        self.data_Tcondenser(Tcondenser)
        self.data_Tsaturator(Tsaturator)
        self.data_Tcabinet(Tcabinet)
        if PDorifice is not None:
            self.data_PDorifice(PDorifice)
        if PCT is not None:
            if 0.0 < PCT <= 1.0:
                PCT *= 100.0
            self.data_PCT(PCT)
        if Toptics is not None:
            self.data_Toptics(Toptics)
        if Alaser is not None:
            self.data_Alaser(Alaser)
        if Twatertrap is not None:
            self.data_Twatertrap(Twatertrap)

        for status in list(device_status):
            add = self.device_status_remap.get(status)
            if add:
                device_status.add(add)
        for status, notify in self.device_status_notify.items():
            notify(bool(status in device_status))

        if self.report_not_3757:
            self.report_not_3757()
        if self.report_3757_3789:
            self.report_3757_3789()
        self.instrument_report()
