#!/usr/bin/env python3

import typing
import os
import forge.data.structure.variable as netcdf_var
import forge.data.structure.timeseries as netcdf_timeseries
import numpy as np
from math import nan
from forge.const import STATIONS as VALID_STATIONS
from forge.cpd3.legacy.raw.write import InstrumentTimeConversion as C
from forge.cpd3.legacy.instrument.azonixumac1050 import Converter as UMAC
from forge.cpd3.legacy.instrument.lovepid import Converter as LovePID
from forge.cpd3.legacy.instrument.generic_met import Converter as BaseMet
from forge.cpd3.legacy.instrument.generic_size_distribution import Converter as BaseSizeDistribution
from forge.cpd3.legacy.instrument.thermo49 import Converter as Thermo49

STATION = os.path.basename(__file__).split('.', 1)[0].lower()
assert STATION in VALID_STATIONS


class Met(BaseMet):
    @property
    def tags(self) -> typing.Optional[typing.Set[str]]:
        return {"met", "radiation"}


class DMPS(BaseSizeDistribution):
    def add_other_data(self, times, g) -> None:
        data_P1 = self.load_variable(f"P1_{self.instrument_id}")
        data_P2 = self.load_variable(f"P2_{self.instrument_id}")
        data_Q1 = self.load_variable(f"Q1_{self.instrument_id}")
        data_Q2 = self.load_variable(f"Q2_{self.instrument_id}")
        data_T1 = self.load_variable(f"T1_{self.instrument_id}")
        data_T2 = self.load_variable(f"T2_{self.instrument_id}")
        data_V = self.load_array_variable(f"V_{self.instrument_id}")
        data_ZNb = self.load_array_variable(f"ZNb_{self.instrument_id}")
        data_N_raw = self.load_variable(f"N_N12")

        var_P1 = g.createVariable("sample_pressure", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_air_pressure(var_P1)
        netcdf_timeseries.variable_coordinates(g, var_P1)
        var_P1.variable_id = "P1"
        var_P1.coverage_content_P1ype = "physicalMeasurement"
        var_P1.cell_methods = "time: mean"
        var_P1.long_name = "aerosol pressure"
        self.apply_data(times, var_P1, data_P1)

        var_P2 = g.createVariable("sheath_pressure", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_pressure(var_P2)
        netcdf_timeseries.variable_coordinates(g, var_P2)
        var_P2.variable_id = "P2"
        var_P2.coverage_content_P2ype = "physicalMeasurement"
        var_P2.cell_methods = "time: mean"
        var_P2.long_name = "sheath pressure"
        self.apply_data(times, var_P2, data_P2)

        var_Q1 = g.createVariable("sample_flow", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_sample_flow(var_Q1)
        netcdf_timeseries.variable_coordinates(g, var_Q1)
        var_Q1.variable_id = "Q1"
        var_Q1.coverage_content_Q1ype = "physicalMeasurement"
        var_Q1.cell_methods = "time: mean"
        var_Q1.long_name = "aerosol flow"
        self.apply_data(times, var_Q1, data_Q1)

        var_Q2 = g.createVariable("sheath_flow", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_flow(var_Q2)
        netcdf_timeseries.variable_coordinates(g, var_Q2)
        var_Q2.variable_id = "Q2"
        var_Q2.coverage_content_Q2ype = "physicalMeasurement"
        var_Q2.cell_methods = "time: mean"
        var_Q2.long_name = "sheath flow"
        self.apply_data(times, var_Q2, data_Q2)

        var_T1 = g.createVariable("sample_temperature", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_air_temperature(var_T1)
        netcdf_timeseries.variable_coordinates(g, var_T1)
        var_T1.variable_id = "T1"
        var_T1.coverage_content_T1ype = "physicalMeasurement"
        var_T1.cell_methods = "time: mean"
        var_T1.long_name = "aerosol temperature"
        self.apply_data(times, var_T1, data_T1)

        var_T2 = g.createVariable("sheath_temperature", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_temperature(var_T2)
        netcdf_timeseries.variable_coordinates(g, var_T2)
        var_T2.variable_id = "T2"
        var_T2.coverage_content_T2ype = "physicalMeasurement"
        var_T2.cell_methods = "time: mean"
        var_T2.long_name = "sheath temperature"
        self.apply_data(times, var_T2, data_T2)

        if data_V.time.shape[0] > 0:
            var_V = g.createVariable("dma_voltage", "f8", ("time", "diameter"), fill_value=nan)
            var_V.variable_id = "V"
            var_V.coverage_content_Vype = "physicalMeasurement"
            var_V.cell_methods = "time: mean"
            var_V.long_name = "DMA voltage"
            var_V.units = "V"
            var_V.C_format = "%5.0f"
            n_diameters = g.dimensions["diameter"].size
            n_add = n_diameters - data_V.value.shape[1]
            if n_add > 0:
                value_V = np.pad(data_V.value, ((0, 0), (0, n_add)), mode='constant', constant_values=nan)
            else:
                value_V = data_V.value
            self.apply_data(times, var_V, data_V.time, value_V)

        if data_ZNb.time.shape[0] > 0:
            var_ZNb = g.createVariable("noninverted_number_distribution", "f8", ("time", "diameter"), fill_value=nan)
            var_ZNb.variable_id = "ZNb"
            var_ZNb.coverage_content_ZNbype = "physicalMeasurement"
            var_ZNb.cell_methods = "time: mean"
            var_ZNb.long_name = "non-inverted binned number concentration (dN)"
            var_ZNb.units = "cm-3"
            var_ZNb.C_format = "%7.1f"
            n_diameters = g.dimensions["diameter"].size
            n_add = n_diameters - data_ZNb.value.shape[1]
            if n_add > 0:
                value_ZNb = np.pad(data_ZNb.value, ((0, 0), (0, n_add)), mode='constant', constant_values=nan)
            else:
                value_ZNb = data_ZNb.value
            self.apply_data(times, var_ZNb, data_ZNb.time, value_ZNb)

        var_N_raw = g.createVariable("raw_number_concentration", "f8", ("time",), fill_value=nan)
        netcdf_timeseries.variable_coordinates(g, var_N_raw)
        var_N_raw.variable_id = "N_N12"
        var_N_raw.coverage_content_N_rawype = "physicalMeasurement"
        var_N_raw.cell_methods = "time: mean"
        var_N_raw.long_name = "DMPS raw CPC concentration"
        var_N_raw.units = "cm-3"
        var_N_raw.C_format = "%7.1f"
        self.apply_data(times, var_N_raw, data_N_raw)


C.run(STATION, {
    "N71": [ C('tsi3760cpc', start='2019-08-07'), ],
    "N72": [ C('admagic250cpc+secondary', start='2020-09-02'), ],
    "N74": [ C('tsi3760cpc+secondary', start='2020-09-23', end='2022-06-13'), ],
    "N21": [ C('csdpops', start='2021-05-04'), ],
    "S11": [ C('tsi3563nephelometer', start='2019-08-07'), ],
    "S12": [ C('ecotechnephelometer+secondary', start='2023-01-26'), ],
    "M11": [ C('teledynet640', start='2021-08-10'), ],
    "A11": [ C('clap', start='2019-08-07'), ],
    "A81": [ C('mageeae33', start='2019-10-18'), ],
    "G81": [ C(Thermo49.with_instrument_override(serial_number="727625030")), ],
    "S81": [ C('purpleair', start='2019-11-06', end='2021-01-07'), ],
    "XM1": [ C(Met, start='2019-08-07'), ],
    "N11": [ C(DMPS, start='2019-08-14', end='2022-05-17'), ],

    "X1": [ C(UMAC.with_variables({
        "Pd_P11": {
            "units": "hPa",
            "C_format": "%5.1f",
            "long_name": "impactor pressure drop",
        },
        "T_V11": {
            "units": "degC",
            "C_format": "%5.1f",
            "long_name": "impactor box inlet temperature",
        },
    }, {
        "Pd_P01": {
            "units": "degC",
            "C_format": "%5.1f",
            "long_name": "stack pitot tube",
        },
        "Pd_P12": {
            "units": "degC",
            "C_format": "%5.1f",
            "long_name": "system vac",
        },
        "Q_Q71": {
            "units": "lpm",
            "C_format": "%6.2f",
            "long_name": "CPC flow",
        },
        "Q_Q72": {
            "units": "lpm",
            "C_format": "%6.2f",
            "long_name": "CPC drier flow",
        },
        "T_V01": {
            "units": "degC",
            "C_format": "%5.1f",
            "long_name": "room temperature",
        },
        "T_V51": {
            "units": "degC",
            "C_format": "%5.1f",
            "long_name": "splitter temperature",
        },
    }), start='2019-08-07') ],
    "X2": [ C(LovePID.with_variables({
        "Q_Q11": {
            "units": "lpm",
            "C_format": "%6.2f",
            "long_name": "analyzer flow",
        },
        "U_V11": {
            "units": "%",
            "C_format": "%5.1f",
            "long_name": "impactor box inlet RH",
        },
    }, {
        "U_V01": {
            "units": "%",
            "C_format": "%5.1f",
        },
        "U_V51": {
            "units": "%",
            "C_format": "%5.1f",
            "long_name": "splitter RH",
        },
    }), start='2019-08-07') ],
})
