import typing
import numpy as np
import forge.data.structure.variable as netcdf_var
import forge.data.structure.timeseries as netcdf_timeseries
from math import nan
from .converter import InstrumentConverter


class Converter(InstrumentConverter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._average_interval: typing.Optional[float] = None

    @property
    def tags(self) -> typing.Optional[typing.Set[str]]:
        return {"aerosol", "purpleair"}

    @property
    def instrument_type(self) -> typing.Optional[str]:
        return "purpleair"

    @property
    def average_interval(self) -> typing.Optional[float]:
        return self._average_interval

    def run(self) -> bool:
        data_ZXa = self.load_variable(f"ZXa_{self.instrument_id}")
        if data_ZXa.time.shape[0] == 0:
            return False
        self._average_interval = self.calculate_average_interval(data_ZXa.time)
        if not super().run():
            return False

        data_ZXb = self.load_variable(f"ZXb_{self.instrument_id}")
        data_Ipa = self.load_variable(f"Ipa_{self.instrument_id}")
        data_Ipb = self.load_variable(f"Ipb_{self.instrument_id}")
        data_T = self.load_variable(f"T_{self.instrument_id}")
        data_U = self.load_variable(f"U_{self.instrument_id}")
        data_P = self.load_variable(f"P_{self.instrument_id}")

        g, times = self.data_group([data_ZXa])
        self.declare_system_flags(g, times)

        var_ZXa = g.createVariable("detector_a_mass_concentration", "f8", ("time",), fill_value=nan)
        netcdf_timeseries.variable_coordinates(g, var_ZXa)
        var_ZXa.variable_id = "ZXa"
        var_ZXa.coverage_content_type = "physicalMeasurement"
        var_ZXa.cell_methods = "time: mean"
        var_ZXa.long_name = "detector A reported PM2.5 mass concentration"
        var_ZXa.units = "ug m-3"
        var_ZXa.C_format = "%6.1f"
        self.apply_data(times, var_ZXa, data_ZXa)

        var_ZXb = g.createVariable("detector_b_mass_concentration", "f8", ("time",), fill_value=nan)
        netcdf_timeseries.variable_coordinates(g, var_ZXb)
        var_ZXb.variable_id = "ZXb"
        var_ZXb.coverage_content_type = "physicalMeasurement"
        var_ZXb.cell_methods = "time: mean"
        var_ZXb.long_name = "detector B reported PM2.5 mass concentration"
        var_ZXb.units = "ug m-3"
        var_ZXb.C_format = "%6.1f"
        self.apply_data(times, var_ZXb, data_ZXb)

        var_Ipa = g.createVariable("detector_a_intensity", "f8", ("time",), fill_value=nan)
        netcdf_timeseries.variable_coordinates(g, var_Ipa)
        var_Ipa.variable_id = "Ipa"
        var_Ipa.coverage_content_type = "physicalMeasurement"
        var_Ipa.cell_methods = "time: mean"
        var_Ipa.long_name = "detector A raw intensity"
        var_Ipa.C_format = "%7.2f"
        self.apply_data(times, var_Ipa, data_Ipa)

        var_Ipb = g.createVariable("detector_b_intensity", "f8", ("time",), fill_value=nan)
        netcdf_timeseries.variable_coordinates(g, var_Ipb)
        var_Ipb.variable_id = "Ipb"
        var_Ipb.coverage_content_type = "physicalMeasurement"
        var_Ipb.cell_methods = "time: mean"
        var_Ipb.long_name = "detector B raw intensity"
        var_Ipb.C_format = "%7.2f"
        self.apply_data(times, var_Ipb, data_Ipb)

        var_T = g.createVariable("ambient_temperature", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_air_temperature(var_T)
        netcdf_timeseries.variable_coordinates(g, var_T)
        var_T.variable_id = "T"
        var_T.coverage_content_type = "physicalMeasurement"
        var_T.cell_methods = "time: mean"
        self.apply_data(times, var_T, data_T)

        var_U = g.createVariable("ambient_humidity", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_air_rh(var_U)
        netcdf_timeseries.variable_coordinates(g, var_U)
        var_U.variable_id = "U"
        var_U.coverage_content_Uype = "physicalMeasurement"
        var_U.cell_methods = "time: mean"
        self.apply_data(times, var_U, data_U)

        var_P = g.createVariable("ambient_pressure", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_air_pressure(var_P)
        netcdf_timeseries.variable_coordinates(g, var_P)
        var_P.variable_id = "P"
        var_P.coverage_content_Pype = "physicalMeasurement"
        var_P.cell_methods = "time: mean"
        self.apply_data(times, var_P, data_P)

        self.apply_coverage(g, times, f"ZXa_{self.instrument_id}")

        def meta_extra(m) -> typing.Dict[str, typing.Tuple[str, str]]:
            result: typing.Dict[str, typing.Tuple[str, str]] = dict()
            source = m.get("Source")
            if source is not None:
                mac_address = source.get("MACAddress")
                if mac_address is not None:
                    result["mac_address"] = (str(mac_address), "instrument Wi-Fi MAC address")
                hardware = source.get("SensorHardware")
                if hardware is not None:
                    result["hardware"] = (str(hardware), "instrument hardware description")
            return result

        self.apply_instrument_metadata(f"ZXa_{self.instrument_id}", manufacturer="Purple Air", model="PA-II",
                                       extra=meta_extra)

        return True