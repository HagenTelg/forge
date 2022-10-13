import typing
import asyncio
from forge.tasks import wait_cancelable
from forge.units import flow_ccs_to_lpm, flow_lpm_to_ccs
from ..streaming import StreamingInstrument, StreamingContext, CommunicationsError
from ..parse import parse_number, parse_datetime_field_fixed, parse_flags_bits

_INSTRUMENT_TYPE = __name__.split('.')[-2]


class Instrument(StreamingInstrument):
    INSTRUMENT_TYPE = _INSTRUMENT_TYPE
    MANUFACTURER = "NOAA/CSL"
    MODEL = "POPS"
    DISPLAY_LETTER = "P"
    TAGS = frozenset({"aerosol", "size", "opc", _INSTRUMENT_TYPE})
    SERIAL_PORT = {'baudrate': 115200}

    def __init__(self, context: StreamingContext):
        super().__init__(context)

        bin_diameters = context.config.get('DIAMETER', default=[])
        if not isinstance(bin_diameters, list):
            bin_diameters = []

        self.data_N = self.input("N")
        self.data_C = self.input("C")
        self.data_Q = self.input("Q")
        self.data_P = self.input("P")
        self.data_Tpressure = self.input("Tpressure")
        self.data_Tinternal = self.input("Tinternal")
        self.data_Tlaser = self.input("Tlaser")
        self.data_Vsupply = self.input("Vsupply")
        self.data_Alaser = self.input("Alaser")
        self.data_peak_width = self.input("peak_width")
        self.data_laser_monitor = self.input("laser_monitor")
        self.data_laser_feedback = self.input("laser_feedback")
        self.data_baseline = self.input("baseline")
        self.data_baseline_stddev = self.input("baseline_stddev")
        self.data_baseline_threshold = self.input("baseline_threshold")
        self.data_baseline_stddevmax = self.input("baseline_stddevmax")
        self.data_pump_on_time = self.input("pump_on_time")
        self.data_pump_feedback = self.input("pump_feedback")

        self.data_dN = self.input_array("dN")
        self.data_Dp = self.persistent("Dp", save_value=False)
        self.data_Dp(bin_diameters)

        if not self.data_N.field.comment and self.data_Q.field.comment:
            self.data_N.field.comment = self.data_Q.field.comment
        if not self.data_dN.field.comment and self.data_Q.field.comment:
            self.data_dN.field.comment = self.data_Q.field.comment

        dimension_Dp = self.dimension_size_distribution_diameter(self.data_Dp, code="Ns", attributes={
            'comment': context.config.comment('DIAMETER'),
            'cell_methods': "time: mean",  # Makes the most sense to average this, even if it's constant at acquisition
        })

        self.bit_flags: typing.Dict[int, Instrument.Notification] = dict()
        self.instrument_report = self.report(
            self.variable_number_concentration(self.data_N, code="N"),
            self.variable_size_distribution_dN(self.data_dN, dimension_Dp, code="Nb"),
            self.variable_sample_flow(self.data_Q, code="Q",
                                      attributes={'C_format': "%5.3f"}),
            self.variable_pressure(self.data_P, "pressure", code="P",
                                   attributes={'long_name': "board pressure"}),
            self.variable_temperature(self.data_Tpressure, "temperature_of_pressure", code="T1",
                                      attributes={'long_name': "temperature of pressure sensor"}),
            self.variable_temperature(self.data_Tlaser, "laser_temperature", code="T2",
                                      attributes={'long_name': "laser temperature"}),
            self.variable_temperature(self.data_Tinternal, "internal_temperature", code="T3",
                                      attributes={'long_name': "internal temperature"}),
            self.variable(self.data_baseline, code="I", attributes={
                'long_name': "baseline value",
                'C_format': "%4.0f"
            }),
            self.variable(self.data_baseline_stddev, code="Ig", attributes={
                'long_name': "baseline standard deviation",
                'C_format': "%4.0f"
            }),
            self.variable(self.data_laser_monitor, code="ZLASERMON", attributes={
                'long_name': "laser monitor",
                'C_format': "%4.0f"
            }),
            self.variable(self.data_pump_feedback, code="ZPUMPFB", attributes={
                'long_name': "pump feedback",
                'C_format': "%3.0f"
            }),

            flags=[
                self.flag_bit(self.bit_flags, 0x1, "too_many_particles", is_warning=True),
                self.flag_bit(self.bit_flags, 0x2, "timing_uncertainty", is_warning=True),
            ],

        )

    async def start_communications(self) -> None:
        # Flush the first record
        await self.drain_reader(0.5)
        await wait_cancelable(self.read_line(), 5)

        # Process a valid record
        await self.communicate()

    async def communicate(self) -> None:
        line: bytes = await wait_cancelable(self.read_line(), 5)
        if len(line) < 4:
            raise CommunicationsError

        fields = line.split(b',')
        try:
            (
                POPS, serial_number,
                _,  # Peak file name
                date_time,
                _,  # Time SSM
                _,  # Aircraft status
                flags, C,
                _,  # Particle Number sum of histogram
                N, baseline, baseline_threshold, baseline_stddev, baseline_stddevmax, P, Tpressure, pump_on_time,
                _,  # Width SD (always zero?)
                peak_width, Q, pump_feedback, Tlaser, laser_feedback, laser_monitor, Tinternal, Vsupply, Alaser,
                _,  # Flow_Set
                _,  # BL Start
                _,  # Threshold multiplier
                number_of_bins,
                _,  # Bin LogMin
                _,  # Bin LogMax
                _,  # Skip Save
                _,  # Min Pk Pts
                _,  # Max Pk Pts
                _,  # Raw Pts
                *counts,
            ) = fields
        except ValueError:
            raise CommunicationsError(f"invalid number of fields in {line}")
        if POPS.strip() != b"POPS":
            raise CommunicationsError(f"invalid instrument identifier in {line}")
        try:
            number_of_bins = int(number_of_bins.strip())
        except (ValueError, OverflowError):
            raise CommunicationsError(f"invalid number of bins {number_of_bins}")
        if number_of_bins != len(counts):
            raise CommunicationsError(f"invalid number of counts in {line}")

        parse_datetime_field_fixed(date_time, datetime_seperator=b'T')

        self.data_baseline(parse_number(baseline))
        self.data_baseline_threshold(parse_number(baseline_threshold))
        self.data_baseline_stddev(parse_number(baseline_stddev))
        self.data_baseline_stddevmax(parse_number(baseline_stddevmax))
        self.data_P(parse_number(P))
        self.data_Tpressure(parse_number(Tpressure))
        self.data_pump_on_time(parse_number(pump_on_time))
        self.data_peak_width(parse_number(peak_width))
        self.data_pump_feedback(parse_number(pump_feedback))
        self.data_Tlaser(parse_number(Tlaser))
        self.data_laser_feedback(parse_number(laser_feedback))
        self.data_laser_monitor(parse_number(laser_monitor))
        self.data_Tinternal(parse_number(Tinternal))
        self.data_Vsupply(parse_number(Vsupply))
        self.data_Alaser(parse_number(Alaser))

        Qinstrument = flow_ccs_to_lpm(parse_number(Q))
        Q = self.data_Q(Qinstrument)

        N = parse_number(N)
        N *= Qinstrument / Q
        self.data_N(N)

        dT = 1.0
        self.data_C(parse_number(C) / dT)
        Q_ccs = flow_lpm_to_ccs(Q)
        dN: typing.List[float] = list()
        for c in counts:
            c = parse_number(c) / dT
            c /= Q_ccs
            dN.append(c)
        self.data_dN(dN)

        if serial_number.startswith(b"POPS-"):
            serial_number = serial_number[5:]
        elif serial_number.startswith(b"POPS_"):
            serial_number = serial_number[5:]
        if serial_number:
            self.set_serial_number(serial_number)

        parse_flags_bits(flags, self.bit_flags, base=10)

        self.instrument_report()
