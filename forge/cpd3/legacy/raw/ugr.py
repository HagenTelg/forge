#!/usr/bin/env python3

import typing
import os
import numpy as np
import forge.data.structure.variable as netcdf_var
import forge.data.structure.timeseries as netcdf_timeseries
from math import nan
from forge.const import STATIONS as VALID_STATIONS
from forge.cpd3.legacy.raw.write import InstrumentTimeConversion as C
from forge.cpd3.legacy.instrument.converter import InstrumentConverter
from forge.cpd3.legacy.instrument.campbellcr1000gmd import Converter as CR1000
from forge.cpd3.legacy.instrument.tsi3563nephelometer import Converter as TSI3563

STATION = os.path.basename(__file__).split('.', 1)[0].lower()
assert STATION in VALID_STATIONS


class CCNFixedName(InstrumentConverter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._average_interval: typing.Optional[float] = None

    @property
    def tags(self) -> typing.Optional[typing.Set[str]]:
        return {"aerosol", "dmtccn", "secondary"}

    @property
    def instrument_type(self) -> typing.Optional[str]:
        return "dmtccn"

    @property
    def average_interval(self) -> typing.Optional[float]:
        return self._average_interval

    def run(self) -> bool:
        variable_instrument_id = ' "N21"'

        data_N = self.load_variable(f"N_{variable_instrument_id}")
        if data_N.time.shape[0] == 0:
            return False
        self._average_interval = self.calculate_average_interval(data_N.time)
        if not super().run():
            return False

        data_ZBin = self.load_variable(f"ZBin_{variable_instrument_id}", dtype=np.int64)
        data_Nb = self.load_array_variable(f"Nb_{variable_instrument_id}")
        data_Np = self.load_array_variable(f"Np_{variable_instrument_id}")
        data_Q1 = self.load_variable(f"Q1_{variable_instrument_id}")
        data_Q2 = self.load_variable(f"Q2_{variable_instrument_id}")
        data_P = self.load_variable(f"P_{variable_instrument_id}")
        data_U = self.load_variable(f"U_{variable_instrument_id}")
        data_Tu = self.load_variable(f"Tu_{variable_instrument_id}")
        data_T1 = self.load_variable(f"T1_{variable_instrument_id}")
        data_T2 = self.load_variable(f"T2_{variable_instrument_id}")
        data_T3 = self.load_variable(f"T3_{variable_instrument_id}")
        data_T4 = self.load_variable(f"T4_{variable_instrument_id}")
        data_T5 = self.load_variable(f"T5_{variable_instrument_id}")
        data_T6 = self.load_variable(f"T6_{variable_instrument_id}")
        data_DT = self.load_variable(f"DT_{variable_instrument_id}")
        data_DTg = self.load_variable(f"DTg_{variable_instrument_id}")
        data_V1 = self.load_variable(f"V1_{variable_instrument_id}")
        data_V2 = self.load_variable(f"V2_{variable_instrument_id}")
        data_A = self.load_variable(f"A_{variable_instrument_id}")
        data_Uc = self.load_variable(f"Uc_{variable_instrument_id}")

        g, times = self.data_group([data_N])
        self.declare_system_flags(g, times)

        var_N = g.createVariable("number_concentration", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_number_concentration(var_N)
        netcdf_timeseries.variable_coordinates(g, var_N)
        var_N.variable_id = "N"
        var_N.coverage_content_type = "physicalMeasurement"
        var_N.cell_methods = "time: mean"
        self.apply_data(times, var_N, data_N)

        n_diameters = max(
            data_Nb.value.shape[1] if data_Nb.value.shape[0] > 0 else 0,
            data_Np.value.shape[1] if data_Np.value.shape[0] > 0 else 0,
        )
        if n_diameters > 0:
            g.createDimension("diameter", n_diameters)

        if n_diameters > 0 and data_Nb.time.shape[0] > 0 and len(data_Nb.value.shape) == 2 and data_Nb.value.shape[1] > 0:
            var_Nb = g.createVariable("number_distribution", "f8", ("time", "diameter"), fill_value=nan)
            netcdf_var.variable_size_distribution_dN(var_Nb)
            netcdf_timeseries.variable_coordinates(g, var_Nb)
            var_Nb.variable_id = "Nb"
            var_Nb.coverage_content_type = "physicalMeasurement"
            var_Nb.long_name = "binned number concentration (dN) with ADC overflow in the final bin"
            n_add = n_diameters - data_Nb.value.shape[1]
            if n_add > 0:
                value_Nb = np.pad(data_Nb.value, ((0, 0), (0, n_add)), mode='constant', constant_values=nan)
            else:
                value_Nb = data_Nb.value
            self.apply_data(times, var_Nb, data_Nb.time, value_Nb)
        else:
            var_Nb = None

        if n_diameters > 0 and data_Np.time.shape[0] > 0 and len(data_Np.value.shape) == 2 and data_Np.value.shape[
            1] > 0:
            var_Np = g.createVariable("number_distribution_stable", "f8", ("time", "diameter"), fill_value=nan)
            netcdf_var.variable_size_distribution_dN(var_Np)
            netcdf_timeseries.variable_coordinates(g, var_Np)
            var_Np.variable_id = "Np"
            var_Np.coverage_content_type = "physicalMeasurement"
            var_Np.long_name = "binned number concentration (dN) with unstable data removed"
            n_add = n_diameters - data_Np.value.shape[1]
            if n_add > 0:
                value_Np = np.pad(data_Np.value, ((0, 0), (0, n_add)), mode='constant', constant_values=nan)
            else:
                value_Np = data_Np.value
            self.apply_data(times, var_Np, data_Np.time, value_Np)
        else:
            var_Np = None

        var_ZBin = g.createVariable("minimum_bin_number", "i8", ("time",), fill_value=False)
        netcdf_timeseries.variable_coordinates(g, var_ZBin)
        var_ZBin.variable_id = "ZBin"
        var_ZBin.coverage_content_type = "physicalMeasurement"
        var_ZBin.cell_methods = "time: mean"
        var_ZBin.long_name = "reported minimum bin setting number"
        var_ZBin.C_format = "%02lld"
        self.apply_data(times, var_ZBin, data_ZBin)

        var_Q1 = g.createVariable("sample_flow", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_sample_flow(var_Q1)
        netcdf_timeseries.variable_coordinates(g, var_Q1)
        var_Q1.variable_id = "Q1"
        var_Q1.coverage_content_type = "physicalMeasurement"
        var_Q1.cell_methods = "time: mean"
        self.apply_data(times, var_Q1, data_Q1)

        var_Q2 = g.createVariable("sheath_flow", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_flow(var_Q2)
        netcdf_timeseries.variable_coordinates(g, var_Q2)
        var_Q2.variable_id = "Q2"
        var_Q2.coverage_content_type = "physicalMeasurement"
        var_Q2.cell_methods = "time: mean"
        var_Q2.long_name = "sheath flow"
        self.apply_data(times, var_Q2, data_Q2)

        var_P = g.createVariable("sample_pressure", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_air_pressure(var_P)
        netcdf_timeseries.variable_coordinates(g, var_P)
        var_P.variable_id = "P"
        var_P.coverage_content_type = "physicalMeasurement"
        var_P.cell_methods = "time: mean"
        var_P.long_name = "sample pressure"
        self.apply_data(times, var_P, data_P)

        var_U = g.createVariable("supersaturation_setting", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_rh(var_U)
        netcdf_timeseries.variable_coordinates(g, var_U)
        var_U.variable_id = "U"
        var_U.coverage_content_type = "physicalMeasurement"
        var_U.cell_methods = "time: mean"
        var_U.long_name = "reported supersaturation from onboard instrument calibration"
        var_U.C_format = "%5.3f"
        self.apply_data(times, var_U, data_U)

        var_Tu = g.createVariable("inlet_temperature", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_air_temperature(var_Tu)
        netcdf_timeseries.variable_coordinates(g, var_Tu)
        var_Tu.variable_id = "Tu"
        var_Tu.coverage_content_type = "physicalMeasurement"
        var_Tu.cell_methods = "time: mean"
        var_Tu.long_name = "inlet temperature"
        self.apply_data(times, var_Tu, data_Tu)

        var_T1 = g.createVariable("tec1_temperature", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_temperature(var_T1)
        netcdf_timeseries.variable_coordinates(g, var_T1)
        var_T1.variable_id = "T1"
        var_T1.coverage_content_type = "physicalMeasurement"
        var_T1.cell_methods = "time: mean"
        var_T1.long_name = "temperature of TEC 1"
        self.apply_data(times, var_T1, data_T1)

        var_T2 = g.createVariable("tec2_temperature", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_temperature(var_T2)
        netcdf_timeseries.variable_coordinates(g, var_T2)
        var_T2.variable_id = "T2"
        var_T2.coverage_content_type = "physicalMeasurement"
        var_T2.cell_methods = "time: mean"
        var_T2.long_name = "temperature of TEC 2"
        self.apply_data(times, var_T2, data_T2)

        var_T3 = g.createVariable("tec3_temperature", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_temperature(var_T3)
        netcdf_timeseries.variable_coordinates(g, var_T3)
        var_T3.variable_id = "T3"
        var_T3.coverage_content_type = "physicalMeasurement"
        var_T3.cell_methods = "time: mean"
        var_T3.long_name = "temperature of TEC 3"
        self.apply_data(times, var_T3, data_T3)

        var_T4 = g.createVariable("sample_temperature", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_temperature(var_T4)
        netcdf_timeseries.variable_coordinates(g, var_T4)
        var_T4.variable_id = "T4"
        var_T4.coverage_content_type = "physicalMeasurement"
        var_T4.cell_methods = "time: mean"
        var_T4.long_name = "sample temperature"
        self.apply_data(times, var_T4, data_T4)

        var_T5 = g.createVariable("opc_temperature", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_temperature(var_T5)
        netcdf_timeseries.variable_coordinates(g, var_T5)
        var_T5.variable_id = "T5"
        var_T5.coverage_content_type = "physicalMeasurement"
        var_T5.cell_methods = "time: mean"
        var_T5.long_name = "OPC temperature"
        self.apply_data(times, var_T5, data_T5)

        var_T6 = g.createVariable("nafion_temperature", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_temperature(var_T6)
        netcdf_timeseries.variable_coordinates(g, var_T6)
        var_T6.variable_id = "T6"
        var_T6.coverage_content_type = "physicalMeasurement"
        var_T6.cell_methods = "time: mean"
        var_T6.long_name = "nafion temperature"
        self.apply_data(times, var_T6, data_T6)

        var_DT = g.createVariable("gradiant_setpoint", "f8", ("time",), fill_value=nan)
        netcdf_timeseries.variable_coordinates(g, var_DT)
        var_DT.variable_id = "DT"
        var_DT.coverage_content_type = "physicalMeasurement"
        var_DT.cell_methods = "time: mean"
        var_DT.long_name = "temperature gradiant setpoint"
        var_DT.units = "degC"
        var_DT.C_format = "%5.2f"
        self.apply_data(times, var_DT, data_DT)

        if data_DTg.time.shape[0] > 0:
            var_DTg = g.createVariable("gradiant_stddev", "f8", ("time",), fill_value=nan)
            netcdf_timeseries.variable_coordinates(g, var_DTg)
            var_DTg.variable_id = "DTg"
            var_DTg.coverage_content_type = "physicalMeasurement"
            var_DTg.cell_methods = "time: mean"
            var_DTg.long_name = "temperature gradiant standard deviation"
            var_DTg.units = "degC"
            var_DTg.C_format = "%5.2f"
            self.apply_data(times, var_DTg, data_DTg)
        else:
            var_DTg = None

        var_V1 = g.createVariable("first_stage_monitor", "f8", ("time",), fill_value=nan)
        netcdf_timeseries.variable_coordinates(g, var_V1)
        var_V1.variable_id = "V1"
        var_V1.coverage_content_type = "physicalMeasurement"
        var_V1.cell_methods = "time: mean"
        var_V1.long_name = "first stage monitor voltage"
        var_V1.units = "V"
        var_V1.C_format = "%5.2f"
        self.apply_data(times, var_V1, data_V1)

        var_V2 = g.createVariable("proportional_valve", "f8", ("time",), fill_value=nan)
        netcdf_timeseries.variable_coordinates(g, var_V2)
        var_V2.variable_id = "V2"
        var_V2.coverage_content_type = "physicalMeasurement"
        var_V2.cell_methods = "time: mean"
        var_V2.long_name = "proportional valve voltage"
        var_V2.units = "V"
        var_V2.C_format = "%5.2f"
        self.apply_data(times, var_V2, data_V2)

        var_A = g.createVariable("laser_current", "f8", ("time",), fill_value=nan)
        netcdf_timeseries.variable_coordinates(g, var_A)
        var_A.variable_id = "A"
        var_A.coverage_content_type = "physicalMeasurement"
        var_A.cell_methods = "time: mean"
        var_A.long_name = "laser current"
        var_A.units = "mA"
        var_A.C_format = "%7.2f"
        self.apply_data(times, var_A, data_A)

        if data_Uc.time.shape[0] > 0:
            var_Uc = g.createVariable("supersaturation_model", "f8", ("time",), fill_value=nan)
            netcdf_var.variable_rh(var_Uc)
            netcdf_timeseries.variable_coordinates(g, var_Uc)
            var_Uc.variable_id = "Uc"
            var_Uc.coverage_content_type = "physicalMeasurement"
            var_Uc.cell_methods = "time: mean"
            var_Uc.long_name = "supersaturation calculated from a numeric model of the instrument"
            var_Uc.C_format = "%5.3f"
            self.apply_data(times, var_Uc, data_Uc)
        else:
            var_Uc = None

        self.apply_cut_size(g, times, [
            (var_N, data_N),
            (var_ZBin, data_ZBin),
            (var_Nb, data_Nb),
            (var_Np, data_Np),
            (var_Q1, data_Q1),
            (var_Q2, data_Q2),
            (var_P, data_P),
            (var_Tu, data_Tu),
            (var_T1, data_T1),
            (var_T2, data_T2),
            (var_T3, data_T3),
            (var_T4, data_T4),
            (var_T5, data_T5),
            (var_T6, data_T6),
            (var_DT, data_DT),
            (var_DTg, data_DTg),
            (var_V1, data_V1),
            (var_V2, data_V2),
            (var_A, data_A),
            (var_Uc, data_Uc),
        ])
        self.apply_coverage(g, times, f"N_{variable_instrument_id}")

        self.apply_instrument_metadata(f"N_{variable_instrument_id}", manufacturer="DMT", model="CCN")

        return True


C.run(STATION, {
    "A11": [ C('psap3w', start='2008-06-18', end='2014-02-27'), ],
    "A21": [ C('mageeae31', start='2012-09-12', end='2014-06-24'), ],
    "A31": [
        C('thermomaap', end='2008-06-18'),
        C('thermomaap+secondary', start='2008-06-18', end='2014-02-27'),
        C('thermomaap', start='2014-02-27'),
    ],
    "A32": [ C('thermomaap+secondary', start='2018-02-21', end='2024-04-26'), ],
    "A41": [ C('mageeae33', start='2013-12-04'), ],
    "A42": [ C('mageeae33+secondary', start='2017-12-20', end='2021-11-03'), ],
    "A51": [
        C('mageeae33+secondary', start='2017-12-18', end='2017-12-19'),
        C('mageeae31+secondary', start='2018-02-21', end='2018-02-24'),
    ],
    "A52": [ C('mageeae31+secondary', start='2018-02-21', end='2018-02-24'), ],
    "N21": [ C('dmtccn', start='2018-10-10', end='2022-07-08'), ],
    "N22": [ C(CCNFixedName, start='2021-01-19', end='2021-04-08'), ],
    "S11": [
        C('tsi3563nephelometer', start='2005-11-01', end='2015-10-28T17:00:00Z'),
        C(TSI3563.with_instrument_override(serial_number="3563153801"), start='2015-10-28T17:00:00Z', end='2017-08-07'),
        C('tsi3563nephelometer', start='2017-08-07'),
    ],
    "S12": [ C('tsi3563nephelometer+secondary', start='2013-01-10', end='2024-07-31'), ],
    "S13": [ C('tsi3563nephelometer+secondary', start='2018-02-09', end='2018-02-16'), ],
    "X1": [ C(CR1000.with_variables({}, {
        "T_V11": {
            "units": "degC",
            "C_format": "%5.1f",
            "long_name": "reference neph internal temperature",
        },
        "T_V12": {
            "units": "degC",
            "C_format": "%5.1f",
            "long_name": "humidifier inlet temperature",
        },
        "T_V13": {
            "units": "degC",
            "C_format": "%5.1f",
            "long_name": "humidifier outlet temperature",
        },
        "T_V14": {
            "units": "degC",
            "C_format": "%5.1f",
            "long_name": "humidified neph internal temperature",
        },
        "U_V11": {
            "units": "%",
            "C_format": "%5.1f",
            "long_name": "reference neph internal RH",
        },
        "U_V12": {
            "units": "%",
            "C_format": "%5.1f",
            "long_name": "humidifier inlet RH",
        },
        "U_V13": {
            "units": "%",
            "C_format": "%5.1f",
            "long_name": "humidifier outlet RH",
        },
        "U_V14": {
            "units": "%",
            "C_format": "%5.1f",
            "long_name": "humidified neph internal RH",
        },
    }), start='2011-01-24', end='2017-02-01'), ],
})
