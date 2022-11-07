import typing
import asyncio
import logging
import time
import struct
import crc
import re
from math import nan, isfinite
from enum import IntEnum
from forge.tasks import wait_cancelable
from forge.acquisition import LayeredConfiguration
from forge.data.structure.variable import variable_flags
from ..streaming import StreamingInstrument, StreamingContext, CommunicationsError
from ..flexio import AnalogInput, AnalogOutput, DigitalOutput, CutSize
from ..state import State
from ..serial import set_dtr, set_rts

_LOGGER = logging.getLogger(__name__)
_INSTRUMENT_TYPE = __name__.split('.')[-2]
_FIRMWARE_VERSION_MATCH = re.compile(br'Rev\.?\s*(\d+\.\d+)', re.IGNORECASE)


class Instrument(StreamingInstrument):
    INSTRUMENT_TYPE = _INSTRUMENT_TYPE
    MANUFACTURER = "Azonix"
    MODEL = "uMAC"
    DISPLAY_LETTER = "U"
    TAGS = frozenset({"aerosol", _INSTRUMENT_TYPE})
    SERIAL_PORT = {'baudrate': 9600}

    ANALOG_INPUT_START = 0
    ANALOG_INPUT_COUNT = 24
    ANALOG_INPUT_TEMPERATURE = 60
    ANALOG_INPUT_VOLTAGE = 61

    ANALOG_OUTPUT_COUNT = 10
    DIGITAL_OUTPUT_COUNT = 16

    class _AnalogInput(AnalogInput):
        def __init__(self, name: str, config: LayeredConfiguration, inp: StreamingInstrument.Input, channel: int):
            super().__init__(name, config, inp)
            self.channel: int = channel
            self.attributes['channel'] = str(channel)

        @classmethod
        def construct(cls, instrument: "Instrument", name: str,
                      config: LayeredConfiguration) -> typing.Optional["Instrument._AnalogInput"]:
            channel = config.get('CHANNEL')
            if isinstance(channel, str):
                channel = channel.lower()
                if channel == 't' or channel == 'temperature':
                    channel = Instrument.ANALOG_INPUT_TEMPERATURE
                elif channel == 'v' or channel == 'voltage':
                    channel = Instrument.ANALOG_INPUT_VOLTAGE
            try:
                channel = int(channel)
                if channel < 0 or channel > 61:
                    raise ValueError
            except (ValueError, TypeError):
                _LOGGER.warning(f"Invalid analog input channel for {name}", exc_info=True)
                return None
            return cls(name, config, instrument.input(name), channel)

    class _AnalogOutput(AnalogOutput):
        def __init__(self, name: str, config: LayeredConfiguration, channel: int):
            super().__init__(name, config)
            self.channel = channel
            self.attributes['channel'] = str(channel)
            self.command_channel = channel
            self.last_output_value: typing.Optional[float] = None

        @classmethod
        def construct(cls, instrument: "Instrument", name: str,
                      config: typing.Union[int, LayeredConfiguration]) -> typing.Optional["Instrument._AnalogOutput"]:
            if not isinstance(config, LayeredConfiguration):
                try:
                    channel = int(config)
                    if channel < 0 or channel >= Instrument.ANALOG_OUTPUT_COUNT:
                        raise ValueError
                except (TypeError, ValueError):
                    _LOGGER.warning(f"Invalid analog output channel for {name}", exc_info=True)
                    return None
                return cls(name, LayeredConfiguration(), channel)

            try:
                channel = int(config.get('CHANNEL'))
                if channel < 0 or channel >= Instrument.ANALOG_OUTPUT_COUNT:
                    raise ValueError
            except (TypeError, ValueError):
                _LOGGER.warning(f"Invalid analog output channel for {name}", exc_info=True)
                return None
            return cls(name, config, channel)

    class _DigitalOutput(DigitalOutput):
        def __init__(self, name: str, config: LayeredConfiguration, channel: int):
            super().__init__(name, config)
            self.channel = channel
            self.port: int = channel // 8
            self.bit: int = channel % 8
            if channel < 63:
                try:
                    self.command_bit = (1 << channel)
                except OverflowError:
                    pass

        @classmethod
        def construct(cls, instrument: "Instrument", name: str,
                      config: typing.Union[int, LayeredConfiguration]) -> typing.Optional["Instrument._DigitalOutput"]:
            if not isinstance(config, LayeredConfiguration):
                try:
                    channel = int(config)
                    if channel < 0 or channel >= Instrument.DIGITAL_OUTPUT_COUNT:
                        raise ValueError
                except (TypeError, ValueError):
                    _LOGGER.warning(f"Invalid digital output channel for {name}", exc_info=True)
                    return None
                return cls(name, LayeredConfiguration(), channel)

            try:
                channel = int(config.get('CHANNEL'))
                if channel < 0 or channel > Instrument.DIGITAL_OUTPUT_COUNT:
                    raise ValueError
            except (TypeError, ValueError):
                _LOGGER.warning(f"Invalid digital output channel for {name}", exc_info=True)
                return None
            return cls(name, config, channel)

    class _Command(IntEnum):
        AIN = 0x09
        AOT = 0x22
        DOT = 0x32
        CNFGLD = 0xA1
        RESET = 0xB1
        REV = 0xB4

    def __init__(self, context: StreamingContext):
        super().__init__(context)

        self._report_interval: float = float(context.config.get('REPORT_INTERVAL', default=1.0))
        self._address: int = int(context.config.get('ADDRESS', default=0))
        self._sleep_time: float = 0.0
        self._sequence_number: int = 0
        self._crc = crc.CrcCalculator(crc.Configuration(
            width=16,
            polynomial=0x8005,
            init_value=0,
            reverse_input=True,
            reverse_output=True,
        ), True)

        self.data_T = self.input("T")
        self.data_V = self.input("V")

        self.data_value = self.input_array("value")
        self.data_raw = self.input_array("raw")
        self.data_analog_outputs = self.persistent("output", save_value=False)
        self.data_digital_outputs = self.persistent("digital", save_value=False)

        self._analog_inputs: typing.List[Instrument._AnalogInput] = self._AnalogInput.create_inputs(self)

        for no_cut in (self.data_T, self.data_V, self.data_value, self.data_raw):
            if no_cut.field.use_cut_size is None:
                no_cut.field.use_cut_size = False

        variable_names: typing.List[str] = [""] * self.ANALOG_INPUT_COUNT
        self.instrument_info['variable'] = variable_names
        variables: typing.List[Instrument.Variable] = list()
        for inp in self._analog_inputs:
            if inp.channel and inp.channel < self.ANALOG_INPUT_COUNT:
                variable_names[inp.channel] = inp.name
            if inp.variable:
                variables.append(inp.variable)

        self.analog_input_report = self.report(
            *variables,

            self.variable_array(self.data_raw, name='analog_input', code='ZINPUTS', attributes={
                'long_name': "raw analog input voltages",
                'units': "V",
                'C_format': "%5.3f"
            }),

            self.variable(self.data_V, "board_voltage", code="V", attributes={
                'long_name': "control board supply voltage",
                'units': "V",
                'C_format': "%5.3f"
            }),
            self.variable_temperature(self.data_T, "board_temperature", code="T", attributes={
                'long_name': "control board temperature",
            }),

            auxiliary_variables=[
                self.variable_array(self.data_value),
            ],
        )

        state: typing.List[State] = list()

        self._analog_outputs: typing.List[Instrument._AnalogOutput] = self._AnalogOutput.create_outputs(self)
        analog_output_names: typing.List[str] = list()
        self.instrument_info['output'] = analog_output_names        
        for out in self._analog_outputs:
            while out.channel >= len(analog_output_names):
                analog_output_names.append("")
            analog_output_names[out.channel] = out.name
            if out.state is not None:
                state.append(out.state)

        self._digital_outputs: typing.List[Instrument._DigitalOutput] = self._DigitalOutput.create_outputs(self)
        digital_output_names: typing.List[str] = list()
        self.instrument_info['digital'] = digital_output_names
        self._apply_digital_state: typing.List[int] = list()
        self._digital_state: typing.List[typing.Optional[int]] = [None] * ((self.DIGITAL_OUTPUT_COUNT + 7) // 8)
        self._digital_mask: typing.List[int] = [0] * ((self.DIGITAL_OUTPUT_COUNT + 7) // 8)
        for out in self._digital_outputs:
            while out.channel >= len(digital_output_names):
                digital_output_names.append("")
            digital_output_names[out.channel] = out.name
            self._digital_mask[out.port] |= (1 << out.bit)

        self.cut_size = CutSize(self.context.cutsize_config)

        if bool(context.config.get('LOG_DIGITAL_STATE', default=False)):
            digital_state = self.state_unsigned_integer(
                self.data_digital_outputs, 'digital_output', code='F2', attributes={
                    'long_name': "digital output state",
                    'standard_name': None,
                })

            state_flags: typing.Dict[int, str] = dict()
            for out in self._digital_outputs:
                if out.channel > 63:
                    continue
                try:
                    bit = (1 << out.channel)
                except OverflowError:
                    continue
                state_flags[bit] = out.name

            def configure(var):
                variable_flags(var, state_flags)

            digital_state.data.configure_variable = configure

            state = [digital_state] + state
        if state:
            self.state_changed = self.change_event(*state)
        else:
            self.state_changed = None

        self.context.bus.connect_command('set_digital_output', self._override_digital_state)

    def _override_digital_state(self, data: int) -> None:
        try:
            bits = int(data)
        except (ValueError, TypeError, OverflowError):
            return
        self._apply_digital_state = list()
        for port in range((self.DIGITAL_OUTPUT_COUNT + 7) // 8):
            self._apply_digital_state.append((bits >> (port * 8)) & 0xFF)

    async def _send_packet(self, command: "Instrument._Command", payload: bytes = None) -> int:
        command = int(command)
        bcs = ((payload and len(payload) or 0) + 4) & 0x3FF
        bcs |= (self._sequence_number & 0x3F) << 10
        frame = struct.pack('<BBHBB',
                            self._address,  # Broadcast = 255
                            1,  # UTP (always one)
                            bcs,
                            0,  # PKT (always zero)
                            command,
                            )
        if payload:
            frame = frame + payload
        packet_crc = self._crc.calculate_checksum(frame)
        self.writer.write(frame + struct.pack('<H', packet_crc))

        seq = self._sequence_number
        self._sequence_number = (self._sequence_number + 1) % 64
        return seq

    async def _receive_packet(self, sequence_number: typing.Optional[int] = None) -> bytes:
        header = await self.reader.readexactly(6)
        adr, utp, bcs, pkt, sta = struct.unpack('<BBHBB', header)
        if adr != self._address:
            raise CommunicationsError(f"invalid response address {adr}")
        if utp != 1:
            raise CommunicationsError(f"invalid response unit type {utp}")
        data_length = bcs & 0x3FF
        seq = (bcs >> 10) & 0x3F
        if data_length < 4:
            raise CommunicationsError(f"packet length {data_length} too short to be valid")
        payload = await self.reader.readexactly(data_length - 4)

        received_crc = await self.reader.readexactly(2)
        received_crc = struct.unpack('<H', received_crc)[0]
        calculated_crc = self._crc.calculate_checksum(header + payload)
        if received_crc != calculated_crc:
            raise CommunicationsError(f"CRC mismatch, calculated {calculated_crc:04X} but got {received_crc:04X}")

        if sequence_number is not None and seq != sequence_number:
            raise CommunicationsError(f"out of order packet {seq} but expecting {sequence_number}")

        # broadcast_received = (pkt & (1 << 5)) != 0
        # cam_sequence_active = (sta & (1 << 3)) != 0
        # analog_alarm_present = (sta & (1 << 4)) != 0
        # eeprom_write_failed = (sta & (1 << 5)) != 0
        # eeprom_write_in_progress = (sta & (1 << 6)) != 0
        error_detected = (sta & (1 << 7)) != 0

        packet_type = pkt & 0xF
        if packet_type == 3 or error_detected:
            if len(payload) == 1:
                error_code = struct.unpack('<B', payload)[0]
            elif len(payload) == 2:
                error_code = struct.unpack('<H', payload)[0]
            else:
                raise CommunicationsError(f"invalid error packet")
            raise CommunicationsError(f"internal instrument error {error_code} (see manual Appendix E)")
        if packet_type != 1:
            raise CommunicationsError(f"invalid packet type {pkt:02X}")

        return payload

    async def _try_hardware_reset(self) -> None:
        # These lines may be tied to the hardware reset signal, so try a cycle there
        set_dtr(self.writer, False)
        set_rts(self.writer, False)
        await asyncio.sleep(1.0)

        set_dtr(self.writer, True)
        set_rts(self.writer, True)
        await asyncio.sleep(1.0)

        self._sequence_number = 0

    async def start_communications(self) -> None:
        if not self.writer:
            raise CommunicationsError

        await self.drain_reader(0.5)

        await self._send_packet(self._Command.RESET)
        try:
            await wait_cancelable(self._receive_packet(), 2.0)
        except (asyncio.TimeoutError, TimeoutError, CommunicationsError):
            _LOGGER.debug("Software reset failed, trying DTR and RTS cycle", exc_info=True)
            await asyncio.sleep(1.0)
            await self. _try_hardware_reset()
            await self._send_packet(self._Command.RESET)
            await wait_cancelable(self._receive_packet(), 2.0)

        await asyncio.sleep(1.0)

        seq = await self._send_packet(self._Command.CNFGLD, struct.pack('<B', 1))
        await self.writer.drain()
        await wait_cancelable(self._receive_packet(seq), 2.0)

        seq = await self._send_packet(self._Command.REV)
        await self.writer.drain()
        response = await wait_cancelable(self._receive_packet(seq), 2.0)
        if response:
            hit = _FIRMWARE_VERSION_MATCH.search(response)
            if hit:
                self.set_firmware_version(hit.group(1))
            else:
                self.set_firmware_version(response)

        await wait_cancelable(self._read_analog_input(), 2.0)

        await self._update_all_digital_out(force=True)
        await self._update_all_analog_out(force=True)
        self._sleep_time = 0.0

    async def _read_analog_input(self) -> typing.List[float]:
        seq = await self._send_packet(
            self._Command.AIN, struct.pack('<BB', self.ANALOG_INPUT_START,
                                           self.ANALOG_INPUT_START + self.ANALOG_INPUT_COUNT-1))
        await self.writer.drain()
        response = await self._receive_packet(seq)
        try:
            return list(struct.unpack('<' + str(self.ANALOG_INPUT_COUNT) + 'f', response))
        except struct.error as e:
            raise CommunicationsError(f"invalid analog read response {response}") from e

    async def _read_internal_sensors(self) -> typing.Tuple[float, float]:
        seq = await self._send_packet(self._Command.AIN, struct.pack('<BB',
                                                                     self.ANALOG_INPUT_TEMPERATURE,
                                                                     self.ANALOG_INPUT_VOLTAGE))
        await self.writer.drain()
        response = await self._receive_packet(seq)
        try:
            t, v = struct.unpack('<ff', response)
        except struct.error as e:
            raise CommunicationsError(f"invalid sensor read response {response}") from e
        return t, v

    async def _write_digital_state(self, port: int, mask: int, bits: int) -> None:
        seq = await self._send_packet(self._Command.DOT, struct.pack('<BBBB', port, port, mask, bits))
        await self.writer.drain()
        await self._receive_packet(seq)

    async def _write_analog_output(self, channel: int, value: float) -> None:
        seq = await self._send_packet(self._Command.AOT, struct.pack('<BBf', channel, channel, value))
        await self.writer.drain()
        await self._receive_packet(seq)

    async def _update_all_digital_out(self, force: bool = False) -> None:
        if self._apply_digital_state:
            update_state = list(self._apply_digital_state)
            update_mask = [0xFF] * max(len(update_state), len(self._digital_mask))
        else:
            update_state: typing.List[typing.Optional[int]] = list(self._digital_state)
            update_mask = self._digital_mask
        for out in self._digital_outputs:
            is_set = out.value
            if is_set is None:
                continue

            if update_state[out.port] is None:
                update_state[out.port] = 0

            if is_set:
                update_state[out.port] |= (1 << out.bit)
            else:
                update_state[out.port] &= ~(1 << out.bit)

        for i in range(len(update_state)):
            if update_state[i] is None:
                continue
            if not force and update_state[i] == self._digital_state[i]:
                continue

            await wait_cancelable(self._write_digital_state(i, update_mask[i], update_state[i]), 2.0)
            self._digital_state[i] = update_state[i]

        self._apply_digital_state = None

    async def _update_all_analog_out(self, force: bool = False) -> None:
        for out in self._analog_outputs:
            value = out.value
            if value is None:
                continue
            if not isfinite(value):
                continue
            if not force and value == out.last_output_value:
                continue

            await wait_cancelable(self._write_analog_output(out.channel, value), 2.0)
            out.last_output_value = value

    async def communicate(self) -> None:
        if self._sleep_time > 0.0:
            await asyncio.sleep(self._sleep_time)
            self._sleep_time = 0.0
        begin_read = time.monotonic()

        temperature, voltage = await wait_cancelable(self._read_internal_sensors(), 2.0)
        ain = await wait_cancelable(self._read_analog_input(), 2.0)

        active_cut_size: CutSize.Active = self.cut_size.current()
        is_bypassed = self.context.bus.bypassed
        for out in self._digital_outputs:
            out.update_cut_size(active_cut_size.size)
            out.update_bypass(is_bypassed)
        await self._update_all_digital_out()

        await self._update_all_analog_out()

        digital_bits: int = 0
        for i in range(min(8, len(self._digital_state))):
            port = self._digital_state[i]
            if not port:
                continue
            digital_bits |= port << (i * 8)

        analog_output_values: typing.List[float] = list()
        for out in self._analog_outputs:
            value = out.value
            if value is None:
                continue
            if not isfinite(value):
                continue
            while out.channel >= len(analog_output_values):
                analog_output_values.append(nan)
            analog_output_values[out.channel] = value

        self.data_digital_outputs(digital_bits)
        self.data_analog_outputs(analog_output_values)

        self.data_T(temperature)
        self.data_V(voltage)
        self.data_raw(ain)

        calibrated_values: typing.List[float] = list()
        for inp in self._analog_inputs:
            if inp.channel == self.ANALOG_INPUT_TEMPERATURE:
                inp(temperature)
                continue
            elif inp.channel == self.ANALOG_INPUT_VOLTAGE:
                inp(temperature)
                continue
            if inp.channel >= len(ain):
                continue

            inp(ain[inp.channel])

            while inp.channel >= len(calibrated_values):
                calibrated_values.append(nan)
            calibrated_values[inp.channel] = inp.value
        self.data_value(calibrated_values)

        self.analog_input_report()

        end_read = time.monotonic()
        self._sleep_time = self._report_interval - (end_read - begin_read)

    async def _send_shutdown_state(self) -> None:
        if not self.writer:
            return

        updated_digital_state: typing.List[int] = list()
        updated_digital_mask: typing.List[int] = list()
        for out in self._digital_outputs:
            if out.shutdown_state is None:
                continue
            while out.port >= len(updated_digital_state):
                updated_digital_state.append(0)
                updated_digital_mask.append(0)
            updated_digital_mask[out.port] |= (1 << out.bit)
            if out.shutdown_state:
                updated_digital_state[out.port] |= (1 << out.bit)
        for i in range(len(updated_digital_state)):
            if updated_digital_mask[i] == 0:
                continue
            await wait_cancelable(self._write_digital_state(i, updated_digital_mask[i], updated_digital_state[i]), 2.0)

    async def run(self) -> typing.NoReturn:
        try:
            await super().run()
        finally:
            try:
                await self._send_shutdown_state()
            except:
                _LOGGER.debug("Error sending shutdown state", exc_info=True)
