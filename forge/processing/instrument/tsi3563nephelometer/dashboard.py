import typing
import numpy as np
from netCDF4 import Dataset, Group
from forge.dashboard.report.action import DashboardAction
from ..default.dashboard import Analyzer as BaseAnalyzer, DataRecord as BaseDataRecord, RecordAnalyzer, ConditionAccumulator, SpancheckRecord


class DataRecord(BaseDataRecord):
    def process_lamp_voltage(self):
        lamp_voltage = self.group.variables.get("lamp_voltage")
        if lamp_voltage is None:
            return
        lamp_voltage = lamp_voltage[...]
        accumulator = ConditionAccumulator.from_instrument_code(self.analyzer, "lamp_voltage_high",
                                                                self.analyzer.target.Severity.ERROR)
        accumulator.emit_true(lamp_voltage > 20.0, self.times)

    def process_rh(self):
        sensor_rh = self.group.variables.get("sample_humidity")
        if sensor_rh is None:
            return
        sensor_rh = sensor_rh[...]
        accumulator = ConditionAccumulator.from_instrument_code(self.analyzer, "rh_suspect",
                                                                self.analyzer.target.Severity.ERROR)
        accumulator.emit_true(np.any([
            sensor_rh < -5.0,
            sensor_rh > 99.0
        ], axis=0), self.times)

    def process_chopper_shutter(self):
        lamp_voltage = self.group.variables.get("lamp_voltage")
        if lamp_voltage is None:
            return
        system_flags = self.group.variables.get("system_flags")
        if system_flags is None:
            return
        flag_meanings = system_flags.flag_meanings.split(' ')
        flag_masks = system_flags.flag_masks

        bit_shutter_fault: int = 0
        bit_chopper_fault: int = 0
        for i in range(len(flag_meanings)):
            flag_name = flag_meanings[i]
            if len(flag_meanings) == 1:
                flag_bits = int(flag_masks)
            else:
                flag_bits = flag_masks[i]
            if flag_name == "shutter_fault":
                bit_shutter_fault = flag_bits
            elif flag_name == "chopper_fault":
                bit_chopper_fault = flag_bits

        lamp_voltage = lamp_voltage[...]
        system_flags = system_flags[...]
                
        accumulate_shutter_fault = ConditionAccumulator.from_instrument_code(self.analyzer, "shutter_fault",
                                                                             self.analyzer.target.Severity.ERROR)
        accumulate_chopper_fault = ConditionAccumulator.from_instrument_code(self.analyzer, "chopper_fault",
                                                                             self.analyzer.target.Severity.ERROR)

        lamp_valid = lamp_voltage <= 20.0
        accumulate_shutter_fault.emit_true(
            np.all([
                np.bitwise_and(system_flags, bit_shutter_fault) != 0,
                lamp_valid
            ], axis=0),
            self.times
        )
        accumulate_chopper_fault.emit_true(
            np.all([
                np.bitwise_and(system_flags, bit_chopper_fault) != 0,
                lamp_valid
            ], axis=0),
            self.times
        )

    def analyze(self) -> None:
        if not self.analyzer.instrument:
            return
        self.process_lamp_voltage()
        self.process_rh()
        self.process_chopper_shutter()
        super().analyze()


class Analyzer(BaseAnalyzer):
    def record_analyzer(self, group: Group) -> typing.Optional["RecordAnalyzer"]:
        if group.name == "data":
            return DataRecord(self, group)
        elif group.name == "spancheck":
            return SpancheckRecord(self, group)
        return super().record_analyzer(group)


def analyze_acquisition(station: str, root: Dataset, target: DashboardAction) -> None:
    analyzer = Analyzer(station, root, target)
    analyzer.analyze()
