import typing
import netCDF4
from math import isfinite
import forge.cpd3.variant as cpd3_variant
from forge.cpd3.identity import Identity, Name
from ..default.converter import Converter as BaseConverter, RecordConverter, StateRecord


class Parameters(RecordConverter):
    def __init__(self, converter: "Converter", group: netCDF4.Group):
        super().__init__(converter, group)
        self.converter: "Converter" = converter

        variable_name = "ZPARAMETERS_" + self.converter.source
        self.base_name = Name(self.converter.station, 'raw', variable_name)

    def metadata(self) -> cpd3_variant.Metadata:
        meta = cpd3_variant.MetadataHash()
        self.converter.insert_metadata(meta)
        meta["Description"] = "Instrument parameter values"
        meta["Smoothing"] = {"Mode": "None"}
        meta.children["SL"] = cpd3_variant.MetadataString({"Description": "Calibration label"})
        meta.children["B"] = cpd3_variant.MetadataInteger({"Description": "Blower power (0-255)", })
        meta.children["SMB"] = cpd3_variant.MetadataBoolean({"Description": "Calibration label"})
        meta.children["SMZ"] = cpd3_variant.MetadataBoolean({
            "Description": "Zero mode: 0=manual only, 1-24=autozero with average of last N zeros"
        })
        meta.children["SP"] = cpd3_variant.MetadataInteger({
            "Description": "Lamp power",
            "Units": "W",
        })
        meta.children["STA"] = cpd3_variant.MetadataInteger({
            "Description": "Averaging time",
            "Units": "s",
        })
        meta.children["STB"] = cpd3_variant.MetadataInteger({
            "Description": "Blanking time",
            "Units": "s",
        })
        meta.children["STP"] = cpd3_variant.MetadataInteger({
            "Description": "Autozero interval",
            "Units": "s",
        })
        meta.children["STZ"] = cpd3_variant.MetadataInteger({
            "Description": "Zero length",
            "Units": "s",
        })

        def SK(wl: float) -> cpd3_variant.Metadata:
            meta = cpd3_variant.MetadataHash()
            meta["Wavelength"] = wl
            meta["Description"] = "Wavelength channel calibration"

            meta.children["K1"] = cpd3_variant.MetadataInteger({
                "Description": "Photomultiplier tube dead time",
                "Units": "ps",
            })
            meta.children["K2"] = cpd3_variant.MetadataReal({
                "Description": "Total scattering calibration",
                "Units": "m⁻¹",
                "Format": "0.000E0",
            })
            meta.children["K3"] = cpd3_variant.MetadataReal({
                "Description": "Air Rayleigh scattering",
                "Units": "m⁻¹",
                "Format": "0.000E0",
                "ReportT": 0.0,
                "ReportP": 1013.25,
            })
            meta.children["K4"] = cpd3_variant.MetadataReal({
                "Description": "Backscattering Rayleigh contribution fraction",
                "Format": "0.000",
            })

            return meta

        for code, wavelength in (("B", 450.0), ("G", 550.0), ("R", 700.0)):
            meta.children["SK" + code] = SK(wavelength)
            meta.children["SV" + code] = cpd3_variant.MetadataInteger({
                "Description": "Photomultiplier tube voltage",
                "Units": "V",
                "Wavelength": wavelength,
            })

        return meta

    def value(self) -> typing.Dict[str, typing.Any]:
        result: typing.Dict[str, typing.Any] = dict()

        if self.converter.calibration_label:
            result["SL"] = self.converter.calibration_label

        def set_integer(name: str):
            var = self.group.variables.get(name)
            if var is None:
                return
            val = var[0]
            if val.mask:
                return
            result[name] = int(val)

        def set_boolean(name: str):
            var = self.group.variables.get(name)
            if var is None:
                return
            val = var[0]
            if val.mask:
                return
            result[name] = bool(int(val))

        set_integer("SMZ")
        set_integer("SP")
        set_integer("STA")
        set_integer("STB")
        set_integer("STP")
        set_integer("STZ")
        set_integer("B")
        set_boolean("H")
        set_boolean("SMB")

        SV = self.group.variables.get("SV")
        if SV is not None:
            for idx, code in ((0, "B"), (1, "G"), (2, "R")):
                val = SV[idx]
                if val.mask:
                    continue
                if not isfinite(val * 1):
                    continue
                result["SV" + code] = int(val)

        def set_calibration(source: netCDF4.Variable, idx: int, code: str, field: str, convert: typing.Callable):
            val = source[idx]
            if val.mask:
                return
            if not isfinite(val * 1):
                return
            val = convert(val)

            target = result.get("SK" + code)
            if target is None:
                target = {}
                result["SK" + code] = target

            target[field] = val

        K1 = self.group.variables.get("K1")
        if K1 is not None:
            for idx, code in ((0, "B"), (1, "G"), (2, "R")):
                set_calibration(K1, idx, code, "K1", int)

        K2 = self.group.variables.get("K2")
        if K2 is not None:
            for idx, code in ((0, "B"), (1, "G"), (2, "R")):
                set_calibration(K2, idx, code, "K2", float)

        K3 = self.group.variables.get("K3")
        if K3 is not None:
            for idx, code in ((0, "B"), (1, "G"), (2, "R")):
                set_calibration(K3, idx, code, "K3", float)

        K4 = self.group.variables.get("K4")
        if K3 is not None:
            for idx, code in ((0, "B"), (1, "G"), (2, "R")):
                set_calibration(K4, idx, code, "K4", float)

        return result

    def convert(self, result: typing.List[typing.Tuple[Identity, typing.Any]]) -> None:
        start_time: float = self.converter.file_start_time
        if start_time is None or not isfinite(start_time):
            return

        meta_start_time = start_time
        end_time: float = self.converter.file_end_time
        if self.converter.system_start_time and self.converter.system_start_time < meta_start_time:
            meta_start_time = self.converter.system_start_time

        result.append((Identity(name=self.base_name.to_metadata(), start=meta_start_time, end=end_time),
                       self.metadata()))
        result.append((Identity(name=self.base_name, start=start_time, end=end_time),
                       self.value()))


class Converter(BaseConverter):
    def __init__(self, station: str, root: netCDF4.Dataset):
        super().__init__(station, root)

        inst_group = self.root.groups.get("instrument")
        self.calibration_label: typing.Optional[str] = None
        if inst_group is not None:
            calibration = inst_group.variables.get("calibration")
            if calibration is not None:
                self.calibration_label = str(calibration[0])
                self.source_metadata["CalibrationLabel"] = self.calibration_label

    def record_converter(self, group: netCDF4.Group) -> typing.Optional[RecordConverter]:
        if group.name == "zero":
            return StateRecord(self, group)
        elif group.name == "spancheck":
            return StateRecord(self, group)
        elif group.name == "parameters":
            return Parameters(self, group)
        return super().record_converter(group)


def convert(station: str, root: netCDF4.Dataset) -> typing.List[typing.Tuple[Identity, typing.Any]]:
    return Converter(station, root).convert()
