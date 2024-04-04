import typing
from math import nan
from forge.vis.data.stream import DataStream
from forge.vis.data.archive import Selection, RealtimeSelection, InstrumentSelection, Record, DataRecord, RealtimeRecord, ContaminationRecord


STANDARD_CUT_SIZE_SPLIT: typing.Iterable[typing.Tuple[str, typing.Union[float, typing.Tuple[float, float]]]] = (
    ("whole", nan),
    ("pm10", (10, nan)),
    ("pm25", (2.5, 10)),
    ("pm1", (nan, 2.5)),
)
STANDARD_THREE_WAVELENGTHS: typing.Iterable[typing.Tuple[str, typing.Tuple[float, float]]] = (
    ("B", (400, 500)),
    ("G", (500, 600)),
    ("R", (600, 750)),
)


aerosol_data: typing.Dict[str, Record] = dict()

for archive in ("raw", "editing", "clean", "avgh"):
    aerosol_data[f"aerosol-{archive}-contamination"] = ContaminationRecord([
        InstrumentSelection(require_tags={"aerosol"}),
    ])
    aerosol_data[f"aerosol-{archive}-cnc"] = DataRecord({
        "cnc": [Selection(variable_name="number_concentration",
                          require_tags={"cpc"}, exclude_tags={"secondary"})],
    })
    for record, cut_size in STANDARD_CUT_SIZE_SPLIT:
        aerosol_data[f"aerosol-{archive}-scattering-{record}"] = DataRecord(dict([
            (f"Bs{code}", [Selection(variable_name="scattering_coefficient", wavelength=wavelength, cut_size=cut_size,
                                     require_tags={"scattering"}, exclude_tags={"secondary"})])
            for code, wavelength in STANDARD_THREE_WAVELENGTHS
        ] + [
            (f"Bbs{code}", [Selection(variable_name="backscattering_coefficient", wavelength=wavelength, cut_size=cut_size,
                                     require_tags={"scattering"}, exclude_tags={"secondary"})])
            for code, wavelength in STANDARD_THREE_WAVELENGTHS
        ]))
    for record, cut_size in STANDARD_CUT_SIZE_SPLIT:
        aerosol_data[f"aerosol-{archive}-absorption-{record}"] = DataRecord(dict([
            (f"Ba{code}", [Selection(variable_name="light_absorption", wavelength=wavelength, cut_size=cut_size,
                                     require_tags={"absorption"}, exclude_tags={"secondary", "aethalometer", "thermomaap"})])
            for code, wavelength in STANDARD_THREE_WAVELENGTHS
        ]))
    for record, cut_size in STANDARD_CUT_SIZE_SPLIT:
        aerosol_data[f"aerosol-{archive}-intensive-{record}"] = DataRecord(dict([
            (f"Bs{code}", [Selection(variable_name="scattering_coefficient", wavelength=wavelength, cut_size=cut_size,
                                     require_tags={"scattering"}, exclude_tags={"secondary"})])
            for code, wavelength in STANDARD_THREE_WAVELENGTHS
        ] + [
            (f"Bbs{code}", [Selection(variable_name="backscattering_coefficient", wavelength=wavelength, cut_size=cut_size,
                                     require_tags={"scattering"}, exclude_tags={"secondary"})])
            for code, wavelength in STANDARD_THREE_WAVELENGTHS
        ] + [
            (f"Ba{code}", [Selection(variable_name="light_absorption", wavelength=wavelength, cut_size=cut_size,
                                     require_tags={"absorption"}, exclude_tags={"secondary", "aethalometer", "thermomaap"})])
            for code, wavelength in STANDARD_THREE_WAVELENGTHS
        ]))
    aerosol_data[f"aerosol-{archive}-aethalometer"] = DataRecord(dict(
        [(f"Ba{wl+1}", [Selection(variable_id="Ba", wavelength_number=wl,
                                  require_tags={"aethalometer"}, exclude_tags={"secondary"})])
         for wl in range(7)] +
        [(f"X{wl+1}", [Selection(variable_name="equivalent_black_carbon", wavelength_number=wl,
                                 require_tags={"aethalometer"}, exclude_tags={"secondary"})])
         for wl in range(7)] +
        [(f"Ir{wl+1}", [Selection(variable_id="Ir", wavelength_number=wl,
                                  require_tags={"aethalometer"}, exclude_tags={"secondary"})])
         for wl in range(7)] +
        [(f"CF{wl+1}", [Selection(variable_name="correction_factor", wavelength_number=wl,
                                  require_tags={"aethalometer", "mageeae33"}, exclude_tags={"secondary"})])
         for wl in range(7)]
    ))
    aerosol_data[f"aerosol-{archive}-wind"] = DataRecord({
        "WS": [Selection(variable_name="wind_speed", exclude_tags={"secondary"})],
        "WD": [Selection(variable_name="wind_direction", exclude_tags={"secondary"})],
    })

aerosol_data["aerosol-raw-flow"] = DataRecord({
    "sample": [Selection(variable_id="Q_Q11")],
    "pitot": [Selection(variable_id="Pd_P01")],
})
aerosol_data["aerosol-raw-temperature"] = DataRecord({
    "Tinlet": [Selection(variable_id="T_V51")], "Uinlet": [Selection(variable_id="U_V51")],
    "Taux": [Selection(variable_id="T_V01")], "Uaux": [Selection(variable_id="U_V01")],
    "Tambient": [Selection(variable_id="T1", instrument_id="XM1")],
    "Uambient": [Selection(variable_id="U1", instrument_id="XM1")],
    "TDambient": [Selection(variable_id="TD1", instrument_id="XM1")],

    "Tsample": [Selection(variable_id="T_V11")], "Usample": [Selection(variable_id="U_V11")],

    "Tnephinlet": [Selection(variable_name="inlet_temperature", require_tags={"scattering"}, exclude_tags={"secondary"})],
    "Unephinlet": [Selection(variable_name="inlet_humidity", require_tags={"scattering"}, exclude_tags={"secondary"})],
    "Tneph": [Selection(variable_name="sample_temperature", require_tags={"scattering"}, exclude_tags={"secondary"})],
    "Uneph": [Selection(variable_name="sample_humidity", require_tags={"scattering"}, exclude_tags={"secondary"})],
})
aerosol_data["aerosol-raw-pressure"] = DataRecord({
    "ambient": [Selection(variable_id="P", instrument_id="XM1")],
    "pitot": [Selection(variable_id="Pd_P01")],
    "vacuum": [Selection(variable_id="Pd_P12")],
})
for record, cut_size in STANDARD_CUT_SIZE_SPLIT:
    aerosol_data[f"aerosol-raw-samplepressure-{record}"] = DataRecord({
        "neph": [Selection(variable_name="sample_pressure", cut_size=cut_size,
                           require_tags={"scattering"}, exclude_tags={"secondary"})],
        "impactor": [Selection(variable_id="Pd_P11", cut_size=cut_size)],
    })
aerosol_data["aerosol-raw-nephzero"] = DataRecord(dict([
    (f"Bsw{code}", [Selection(variable_name="wall_scattering_coefficient", wavelength=wavelength,
                              variable_type=Selection.VariableType.State,
                              require_tags={"scattering"}, exclude_tags={"secondary"})])
    for code, wavelength in STANDARD_THREE_WAVELENGTHS
] + [
    (f"Bbsw{code}", [Selection(variable_name="wall_backscattering_coefficient", wavelength=wavelength,
                              variable_type=Selection.VariableType.State,
                              require_tags={"scattering"}, exclude_tags={"secondary"})])
    for code, wavelength in STANDARD_THREE_WAVELENGTHS
]))
aerosol_data["aerosol-raw-nephstatus"] = DataRecord({
    "CfG": [Selection(variable_name="reference_counts", wavelength=(500, 600),
                      require_tags={"scattering"}, exclude_tags={"secondary"})],
    "Vl": [Selection(variable_name="lamp_voltage",
                     require_tags={"scattering"}, exclude_tags={"secondary"})],
    "Al": [Selection(variable_name="lamp_current",
                     require_tags={"scattering"}, exclude_tags={"secondary"})],
})
aerosol_data["aerosol-raw-clapstatus"] = DataRecord({
    "IrG": [Selection(variable_name="transmittance", wavelength=(500, 600),
                      instrument_code="clap", exclude_tags={"secondary"}),
            Selection(variable_name="transmittance", wavelength=(500, 600),
                      instrument_code="bmitap", exclude_tags={"secondary"})],
    "IfG": [Selection(variable_name="reference_intensity", wavelength=(500, 600),
                      instrument_code="clap", exclude_tags={"secondary"}),
            Selection(variable_name="reference_intensity", wavelength=(500, 600),
                      instrument_code="bmitap", exclude_tags={"secondary"})],
    "IpG": [Selection(variable_name="sample_intensity", wavelength=(500, 600),
                      instrument_code="clap", exclude_tags={"secondary"}),
            Selection(variable_name="sample_intensity", wavelength=(500, 600),
                      instrument_code="bmitap", exclude_tags={"secondary"})],
    "Q": [Selection(variable_name="sample_flow",
                    instrument_code="clap", exclude_tags={"secondary"}),
          Selection(variable_name="sample_flow",
                    instrument_code="bmitap", exclude_tags={"secondary"})],
    "Tsample": [Selection(variable_name="sample_temperature",
                          instrument_code="clap", exclude_tags={"secondary"}),
                Selection(variable_name="sample_temperature",
                          instrument_code="bmitap", exclude_tags={"secondary"})],
    "Tcase": [Selection(variable_name="case_temperature",
                        instrument_code="clap", exclude_tags={"secondary"}),
              Selection(variable_name="case_temperature",
                        instrument_code="bmitap", exclude_tags={"secondary"})],
    "spot": [Selection(variable_name="spot_number",
                       instrument_code="clap", exclude_tags={"secondary"},
                       variable_type=Selection.VariableType.State),
             Selection(variable_name="spot_number",
                       instrument_code="bmitap", exclude_tags={"secondary"},
                       variable_type=Selection.VariableType.State)],
}, hold_fields={"spot"})
aerosol_data["aerosol-raw-aethalometerstatus"] = DataRecord({
    "Tcontroller": [Selection(variable_name="controller_temperature",
                              instrument_code="mageeae33", exclude_tags={"secondary"})],
    "Tsupply": [Selection(variable_name="supply_temperature",
                          instrument_code="mageeae33", exclude_tags={"secondary"})],
    "Tled": [Selection(variable_name="led_temperature",
                       instrument_code="mageeae33", exclude_tags={"secondary"})],
    "Q1": [Selection(variable_name="spot_one_flow",
                     instrument_code="mageeae33", exclude_tags={"secondary"})],
    "Q2": [Selection(variable_name="spot_two_flow",
                     instrument_code="mageeae33", exclude_tags={"secondary"})],
    "Q": [Selection(variable_name="sample_flow",
                    require_tags={"aethalometer"}, exclude_tags={"secondary"})],
})
aerosol_data["aerosol-raw-cpcstatus"] = DataRecord({
    "Qsample": [Selection(variable_name="sample_flow",
                          require_tags={"cpc"}, exclude_tags={"secondary"}),
                Selection(variable_id="Q_Q71"), Selection(variable_id="Q_Q61")],
    "Qdrier": [Selection(variable_id="Q_Q72"), Selection(variable_id="Q_Q62")],
})
aerosol_data["aerosol-raw-umacstatus"] = DataRecord({
    "T": [Selection(variable_name="board_temperature", instrument_code="campbellcr1000gmd", exclude_tags={"secondary"}),
          Selection(variable_name="board_temperature", instrument_code="azonixumac1050", exclude_tags={"secondary"})],
    "V": [Selection(variable_name="supply_voltage", instrument_code="campbellcr1000gmd", exclude_tags={"secondary"}),
          Selection(variable_name="board_voltage", instrument_code="azonixumac1050", exclude_tags={"secondary"})],
})


aerosol_data["aerosol-realtime-cnc"] = RealtimeRecord({
    "cnc": [RealtimeSelection("N", variable_name="number_concentration",
                              require_tags={"cpc"}, exclude_tags={"secondary"})]
})
for record, cut_size in STANDARD_CUT_SIZE_SPLIT:
    aerosol_data[f"aerosol-realtime-scattering-{record}"] = RealtimeRecord(dict([
        (f"Bs{code}", [RealtimeSelection(f"Bs{code}", variable_name="scattering_coefficient", wavelength=wavelength, cut_size=cut_size,
                                         require_tags={"scattering"}, exclude_tags={"secondary"})])
        for code, wavelength in STANDARD_THREE_WAVELENGTHS
    ] + [
        (f"Bbs{code}", [RealtimeSelection(f"Bbs{code}", variable_name="backscattering_coefficient", wavelength=wavelength, cut_size=cut_size,
                                          require_tags={"scattering"}, exclude_tags={"secondary"})])
        for code, wavelength in STANDARD_THREE_WAVELENGTHS
    ]))
for record, cut_size in STANDARD_CUT_SIZE_SPLIT:
    aerosol_data[f"aerosol-realtime-absorption-{record}"] = RealtimeRecord(dict([
        (f"Ba{code}", [RealtimeSelection(f"Ba{code}", variable_name="light_absorption", wavelength=wavelength, cut_size=cut_size,
                                         require_tags={"absorption"}, exclude_tags={"secondary", "aethalometer", "thermomaap"})])
        for code, wavelength in STANDARD_THREE_WAVELENGTHS
    ]))
for record, cut_size in STANDARD_CUT_SIZE_SPLIT:
    aerosol_data[f"aerosol-realtime-intensive-{record}"] = RealtimeRecord(dict([
        (f"Bs{code}", [RealtimeSelection(f"Bs{code}", variable_name="scattering_coefficient", wavelength=wavelength, cut_size=cut_size,
                                         require_tags={"scattering"}, exclude_tags={"secondary"})])
        for code, wavelength in STANDARD_THREE_WAVELENGTHS
    ] + [
        (f"Bbs{code}", [RealtimeSelection(f"Bbs{code}", variable_name="backscattering_coefficient", wavelength=wavelength, cut_size=cut_size,
                                          require_tags={"scattering"}, exclude_tags={"secondary"})])
        for code, wavelength in STANDARD_THREE_WAVELENGTHS
    ] + [
        (f"Ba{code}", [RealtimeSelection(f"Ba{code}", variable_name="light_absorption", wavelength=wavelength, cut_size=cut_size,
                                         require_tags={"absorption"}, exclude_tags={"secondary", "aethalometer", "thermomaap"})])
        for code, wavelength in STANDARD_THREE_WAVELENGTHS
    ]))
aerosol_data[f"aerosol-realtime-aethalometer"] = RealtimeRecord(dict(
    [(f"Ba{wl+1}", [RealtimeSelection(f"Ba{wl+1}", variable_id="Ba", wavelength_number=wl,
                                      require_tags={"aethalometer"}, exclude_tags={"secondary"})])
     for wl in range(7)] +
    [(f"X{wl+1}", [RealtimeSelection(f"X{wl+1}", variable_name="equivalent_black_carbon", wavelength_number=wl,
                                     require_tags={"aethalometer"}, exclude_tags={"secondary"})])
     for wl in range(7)] +
    [(f"Ir{wl+1}", [RealtimeSelection(f"Ir{wl+1}", variable_id="Ir", wavelength_number=wl,
                                      require_tags={"aethalometer"}, exclude_tags={"secondary"})])
     for wl in range(7)] +
    [(f"CF{wl+1}", [RealtimeSelection(f"k{wl+1}",variable_name="correction_factor", wavelength_number=wl,
                                      require_tags={"aethalometer", "mageeae33"}, exclude_tags={"secondary"})])
     for wl in range(7)]
))
aerosol_data["aerosol-realtime-wind"] = RealtimeRecord({
    "WS": [RealtimeSelection("WS", variable_name="wind_speed", exclude_tags={"secondary"})],
    "WD": [RealtimeSelection("WD", variable_name="wind_direction", exclude_tags={"secondary"})],
})
aerosol_data["aerosol-realtime-flow"] = RealtimeRecord({
    "sample": [RealtimeSelection("Q_Q11", variable_id="Q_Q11")],
    "pitot": [RealtimeSelection("Pd_P01", variable_id="Pd_P01")],
})
aerosol_data["aerosol-realtime-temperature"] = RealtimeRecord({
    "Tinlet": [RealtimeSelection("T_V51", variable_id="T_V51")], "Uinlet": [RealtimeSelection("U_V51", variable_id="U_V51")],
    "Taux": [RealtimeSelection("T_V01", variable_id="T_V01")], "Uaux": [RealtimeSelection("U_V01", variable_id="U_V01")],
    "Tambient": [RealtimeSelection("Tambient", variable_id="T1", instrument_id="XM1")],
    "Uambient": [RealtimeSelection("Uambient", variable_id="U1", instrument_id="XM1")],
    "TDambient": [RealtimeSelection("TDambient", variable_id="TD1", instrument_id="XM1")],

    "Tsample": [RealtimeSelection("T_V11", variable_id="T_V11")], "Usample": [RealtimeSelection("U_V11", variable_id="U_V11")],

    "Tnephinlet": [RealtimeSelection("Tinlet", variable_name="inlet_temperature", require_tags={"scattering"}, exclude_tags={"secondary"})],
    "Unephinlet": [RealtimeSelection("Uinlet", variable_name="inlet_humidity", require_tags={"scattering"}, exclude_tags={"secondary"})],
    "Tneph": [RealtimeSelection("Tsample", variable_name="sample_temperature", require_tags={"scattering"}, exclude_tags={"secondary"})],
    "Uneph": [RealtimeSelection("Usample", variable_name="sample_humidity", require_tags={"scattering"}, exclude_tags={"secondary"})],
})
aerosol_data["aerosol-realtime-pressure"] = RealtimeRecord({
    "ambient": [RealtimeSelection("P", variable_id="P", instrument_id="XM1")],
    "pitot": [RealtimeSelection("Pd_P01", variable_id="Pd_P01")],
    "vacuum": [RealtimeSelection("Pd_P12", variable_id="Pd_P12")],
})
for record, cut_size in STANDARD_CUT_SIZE_SPLIT:
    aerosol_data[f"aerosol-realtime-samplepressure-{record}"] = RealtimeRecord({
        "neph": [RealtimeSelection("Psample", variable_name="sample_pressure", cut_size=cut_size,
                                   require_tags={"scattering"}, exclude_tags={"secondary"})],
        "impactor": [RealtimeSelection("Pd_P11", variable_id="Pd_P11", cut_size=cut_size)],
    })
aerosol_data["aerosol-realtime-nephzero"] = RealtimeRecord(dict([
    (f"Bsw{code}", [RealtimeSelection(f"Bsw{code}", variable_name="wall_scattering_coefficient", wavelength=wavelength,
                                      variable_type=Selection.VariableType.State,
                                      require_tags={"scattering"}, exclude_tags={"secondary"})])
    for code, wavelength in STANDARD_THREE_WAVELENGTHS
] + [
    (f"Bbsw{code}", [RealtimeSelection(f"Bbsw{code}", variable_name="wall_backscattering_coefficient", wavelength=wavelength,
                                       variable_type=Selection.VariableType.State,
                                       require_tags={"scattering"}, exclude_tags={"secondary"})])
    for code, wavelength in STANDARD_THREE_WAVELENGTHS
]))
aerosol_data["aerosol-realtime-nephstatus"] = RealtimeRecord({
    "CfG": [RealtimeSelection("CfG", variable_name="reference_counts", wavelength=(500, 600),
                              require_tags={"scattering"}, exclude_tags={"secondary"})],
    "Vl": [RealtimeSelection("Vl", variable_name="lamp_voltage",
                             require_tags={"scattering"}, exclude_tags={"secondary"})],
    "Al": [RealtimeSelection("Al", variable_name="lamp_current",
                             require_tags={"scattering"}, exclude_tags={"secondary"})],
})
aerosol_data["aerosol-realtime-clapstatus"] = RealtimeRecord({
    "IrG": [RealtimeSelection("IrG", variable_name="transmittance", wavelength=(500, 600),
                              instrument_code="clap", exclude_tags={"secondary"}),
            RealtimeSelection("IrG", variable_name="transmittance", wavelength=(500, 600),
                              instrument_code="bmitap", exclude_tags={"secondary"})],
    "IfG": [RealtimeSelection("IfG", variable_name="reference_intensity", wavelength=(500, 600),
                              instrument_code="clap", exclude_tags={"secondary"}),
            RealtimeSelection("IfG", variable_name="reference_intensity", wavelength=(500, 600),
                              instrument_code="bmitap", exclude_tags={"secondary"})],
    "IpG": [RealtimeSelection("IpG", variable_name="sample_intensity", wavelength=(500, 600),
                              instrument_code="clap", exclude_tags={"secondary"}),
            RealtimeSelection("IpG", variable_name="sample_intensity", wavelength=(500, 600),
                              instrument_code="bmitap", exclude_tags={"secondary"})],
    "Q": [RealtimeSelection("Q", variable_name="sample_flow",
                            instrument_code="clap", exclude_tags={"secondary"}),
          RealtimeSelection("Q", variable_name="sample_flow",
                            instrument_code="bmitap", exclude_tags={"secondary"})],
    "Tsample": [RealtimeSelection("Tsample", variable_name="sample_temperature",
                                  instrument_code="clap", exclude_tags={"secondary"}),
                RealtimeSelection("Tsample", variable_name="sample_temperature",
                                  instrument_code="bmitap", exclude_tags={"secondary"})],
    "Tcase": [RealtimeSelection("Tcase", variable_name="case_temperature",
                                instrument_code="clap", exclude_tags={"secondary"}),
              RealtimeSelection("Tcase", variable_name="case_temperature",
                                instrument_code="bmitap", exclude_tags={"secondary"})],
    "spot": [RealtimeSelection("Fn", variable_name="spot_number",
                               instrument_code="clap", exclude_tags={"secondary"},
                               variable_type=Selection.VariableType.State),
             RealtimeSelection("Fn", variable_name="spot_number",
                               instrument_code="bmitap", exclude_tags={"secondary"},
                               variable_type=Selection.VariableType.State)],
}, hold_fields={"spot"})
aerosol_data["aerosol-realtime-aethalometerstatus"] = RealtimeRecord({
    "Tcontroller": [RealtimeSelection("Tcontroller", variable_name="controller_temperature",
                                      instrument_code="mageeae33", exclude_tags={"secondary"})],
    "Tsupply": [RealtimeSelection("Tsupply", variable_name="supply_temperature",
                                  instrument_code="mageeae33", exclude_tags={"secondary"})],
    "Tled": [RealtimeSelection("Tled", variable_name="led_temperature",
                               instrument_code="mageeae33", exclude_tags={"secondary"})],
    "Q1": [RealtimeSelection("Q1", variable_name="spot_one_flow",
                             instrument_code="mageeae33", exclude_tags={"secondary"})],
    "Q2": [RealtimeSelection("Q2", variable_name="spot_two_flow",
                             instrument_code="mageeae33", exclude_tags={"secondary"})],
    "Q": [RealtimeSelection("Q", variable_name="sample_flow",
                            require_tags={"aethalometer"}, exclude_tags={"secondary"})],
})
aerosol_data["aerosol-realtime-cpcstatus"] = RealtimeRecord({
    "Qsample": [RealtimeSelection("Q", variable_name="sample_flow",
                                  require_tags={"cpc"}, exclude_tags={"secondary"}),
                RealtimeSelection("Q_Q71", variable_id="Q_Q71"), RealtimeSelection("Q_Q61", variable_id="Q_Q61")],
    "Qdrier": [RealtimeSelection("Q_Q72", variable_id="Q_Q72"), RealtimeSelection("Q_Q62", variable_id="Q_Q62")],
})
aerosol_data["aerosol-realtime-umacstatus"] = RealtimeRecord({
    "T": [RealtimeSelection("T", variable_name="board_temperature", instrument_code="campbellcr1000gmd", exclude_tags={"secondary"}),
          RealtimeSelection("T", variable_name="board_temperature", instrument_code="azonixumac1050", exclude_tags={"secondary"})],
    "V": [RealtimeSelection("V", variable_name="supply_voltage", instrument_code="campbellcr1000gmd", exclude_tags={"secondary"}),
          RealtimeSelection("V", variable_name="board_voltage", instrument_code="azonixumac1050", exclude_tags={"secondary"})],
})


ozone_data: typing.Dict[str, Record] = dict()
for archive in ("raw", "editing", "clean", "avgh"):
    ozone_data[f"ozone-{archive}-contamination"] = ContaminationRecord([
        InstrumentSelection(require_tags={"ozone"}),
    ])
    ozone_data[f"ozone-{archive}-ozone"] = DataRecord({
        "ozone": [Selection(standard_name="mole_fraction_of_ozone_in_air",
                            require_tags={"ozone"}, exclude_tags={"secondary"})],
    })
    ozone_data[f"ozone-{archive}-wind"] = DataRecord({
        "WS": [Selection(variable_name="wind_speed", exclude_tags={"secondary"})],
        "WD": [Selection(variable_name="wind_direction", exclude_tags={"secondary"})],
    })
ozone_data["ozone-raw-status"] = DataRecord({
    "Tsample": [Selection(variable_name="sample_temperature",
                          require_tags={"ozone"}, exclude_tags={"secondary"})],
    "Tlamp": [Selection(variable_name="lamp_temperature",
                        require_tags={"ozone"}, exclude_tags={"secondary"})],
    "Psample": [Selection(variable_name="sample_pressure",
                          require_tags={"ozone"}, exclude_tags={"secondary"})],
})
ozone_data["ozone-raw-cells"] = DataRecord({
    "Q": [Selection(variable_name="cell_a_flow",
                    require_tags={"ozone"}, exclude_tags={"secondary"})],
    "Ca": [Selection(variable_name="cell_a_count_rate",
                     require_tags={"ozone"}, exclude_tags={"secondary"})],
    "Cb": [Selection(variable_name="cell_b_count_rate",
                     require_tags={"ozone"}, exclude_tags={"secondary"})],
})

ozone_data["ozone-realtime-ozone"] = RealtimeRecord({
    "ozone": [RealtimeSelection("X", standard_name="mole_fraction_of_ozone_in_air",
                                require_tags={"ozone"}, exclude_tags={"secondary"})],
})
ozone_data["ozone-realtime-wind"] = RealtimeRecord({
    "WS": [RealtimeSelection("WS", variable_name="wind_speed", exclude_tags={"secondary"})],
    "WD": [RealtimeSelection("WD", variable_name="wind_direction", exclude_tags={"secondary"})],
})
ozone_data["ozone-realtime-status"] = RealtimeRecord({
    "Tsample": [RealtimeSelection("Tsample", variable_name="sample_temperature",
                          require_tags={"ozone"}, exclude_tags={"secondary"})],
    "Tlamp": [RealtimeSelection("Tlamp", variable_name="lamp_temperature",
                        require_tags={"ozone"}, exclude_tags={"secondary"})],
    "Psample": [RealtimeSelection("Psample", variable_name="sample_pressure",
                          require_tags={"ozone"}, exclude_tags={"secondary"})],
})
ozone_data["ozone-realtime-cells"] = RealtimeRecord({
    "Q": [RealtimeSelection("Q", variable_name="cell_a_flow",
                            instrument_code="thermo49iq", require_tags={"ozone"}, exclude_tags={"secondary"}),
          RealtimeSelection("Qa", variable_name="cell_a_flow",
                            instrument_code="thermo49", require_tags={"ozone"}, exclude_tags={"secondary"})],
    "Ca": [RealtimeSelection("Ca", variable_name="cell_a_count_rate",
                             require_tags={"ozone"}, exclude_tags={"secondary"})],
    "Cb": [RealtimeSelection("Cb", variable_name="cell_b_count_rate",
                             require_tags={"ozone"}, exclude_tags={"secondary"})],
})


met_data: typing.Dict[str, Record] = dict()
for archive in ("raw", "editing", "clean", "avgh"):
    met_data[f"met-{archive}-wind"] = DataRecord({
        "WSambient": [Selection(variable_id="WS1", instrument_id="XM1")],
        "WDambient": [Selection(variable_id="WD1", instrument_id="XM1")],
        "WS2": [Selection(variable_id="WS2", instrument_id="XM1")],
        "WD2": [Selection(variable_id="WD2", instrument_id="XM1")],
        "WS3": [Selection(variable_id="WS3", instrument_id="XM1")],
        "WD3": [Selection(variable_id="WD3", instrument_id="XM1")],
    })
    met_data[f"met-{archive}-temperature"] = DataRecord({
        "Uambient": [Selection(variable_id="U1", instrument_id="XM1")],
        "Tambient": [Selection(variable_id="T1", instrument_id="XM1")],
        "TDambient": [Selection(variable_id="TD1", instrument_id="XM1")],
        "U2": [Selection(variable_id="U2", instrument_id="XM1")],
        "T2": [Selection(variable_id="T2", instrument_id="XM1")],
        "TD2": [Selection(variable_id="TD2", instrument_id="XM1")],
        "U3": [Selection(variable_id="U3", instrument_id="XM1")],
        "T3": [Selection(variable_id="T3", instrument_id="XM1")],
        "TD3": [Selection(variable_id="TD3", instrument_id="XM1")],
    })
    met_data[f"met-{archive}-pressure"] = DataRecord({
        "ambient": [Selection(variable_id="P", instrument_id="XM1")],
    })
    met_data[f"met-{archive}-precipitation"] = DataRecord({
        "precipitation": [Selection(variable_id="WI", instrument_id="XM1")],
    })
    met_data[f"met-{archive}-tower"] = DataRecord({
        "Tmiddle": [Selection(variable_id="T2", instrument_id="XM1")],
        "Ttop": [Selection(variable_id="T3", instrument_id="XM1")],
    })


radiation_data: typing.Dict[str, Record] = dict()
for archive in ("raw", "editing", "clean", "avgh"):
    radiation_data[f"radiation-{archive}-contamination"] = ContaminationRecord([
        InstrumentSelection(require_tags={"radiation"}),
    ])
    radiation_data[f"radiation-{archive}-ambient"] = DataRecord({
        "WS": [Selection(variable_name="wind_speed", exclude_tags={"secondary"})],
        "WD": [Selection(variable_name="wind_direction", exclude_tags={"secondary"})],
        "Pambient": [Selection(variable_id="P", instrument_id="XM1")],
        "Tambient": [Selection(variable_id="T1", instrument_id="XM1")],
        "Uambient": [Selection(variable_id="U1", instrument_id="XM1")],
        "TDambient": [Selection(variable_id="TD1", instrument_id="XM1")],
    })
    radiation_data[f"radiation-{archive}-status"] = DataRecord({
        "Cg1": [Selection(variable_id="Cg1", require_tags={"radiation"}, exclude_tags={"secondary"})],
        "Cg2": [Selection(variable_id="Cg2", require_tags={"radiation"}, exclude_tags={"secondary"})],
        "Cf": [Selection(variable_id="Cf", require_tags={"radiation"}, exclude_tags={"secondary"})],
        "Ci": [Selection(variable_id="Ci", require_tags={"radiation"}, exclude_tags={"secondary"})],
        "Cui": [Selection(variable_id="Cui", require_tags={"radiation"}, exclude_tags={"secondary"})],
        "Cug": [Selection(variable_id="Cug", require_tags={"radiation"}, exclude_tags={"secondary"})],
        "Tlogger": [Selection(variable_id="Tx", require_tags={"radiation"}, exclude_tags={"secondary"})],
        "Vbattery": [Selection(variable_id="Vx", require_tags={"radiation"}, exclude_tags={"secondary"})],
    })
    radiation_data[f"radiation-{archive}-shortwave"] = DataRecord({
        "Rdg": [Selection(variable_id="Rdg", require_tags={"radiation"}, exclude_tags={"secondary"})],
        "Rug": [Selection(variable_id="Rug", require_tags={"radiation"}, exclude_tags={"secondary"})],
        "Rdn": [Selection(variable_id="Rdn", require_tags={"radiation"}, exclude_tags={"secondary"})],
        "Rdf": [Selection(variable_id="Rdf", require_tags={"radiation"}, exclude_tags={"secondary"})],
        "Rdg2": [Selection(variable_id="Rdg2", require_tags={"radiation"}, exclude_tags={"secondary"})],
        "Rug2": [Selection(variable_id="Rug2", require_tags={"radiation"}, exclude_tags={"secondary"})],
        "Rdn2": [Selection(variable_id="Rdn2", require_tags={"radiation"}, exclude_tags={"secondary"})],
        "Rdf2": [Selection(variable_id="Rdf2", require_tags={"radiation"}, exclude_tags={"secondary"})],
        "Rdg3": [Selection(variable_id="Rdg3", require_tags={"radiation"}, exclude_tags={"secondary"})],
        "Rug3": [Selection(variable_id="Rug3", require_tags={"radiation"}, exclude_tags={"secondary"})],
        "Rdn3": [Selection(variable_id="Rdn3", require_tags={"radiation"}, exclude_tags={"secondary"})],
        "Rdf3": [Selection(variable_id="Rdf3", require_tags={"radiation"}, exclude_tags={"secondary"})],
        "Rst": [Selection(variable_id="Rst", require_tags={"radiation"}, exclude_tags={"secondary"})],
        "Rsd": [Selection(variable_id="Rsd", require_tags={"radiation"}, exclude_tags={"secondary"})],
        "Rv": [Selection(variable_id="Rv", require_tags={"radiation"}, exclude_tags={"secondary"})],
        "Rp": [Selection(variable_id="Rp", require_tags={"radiation"}, exclude_tags={"secondary"})],
    })
    radiation_data[f"radiation-{archive}-longwave"] = DataRecord({
        "Rdi": [Selection(variable_id="Rdi", require_tags={"radiation"}, exclude_tags={"secondary"})],
        "Rui": [Selection(variable_id="Rui", require_tags={"radiation"}, exclude_tags={"secondary"})],
    })
    radiation_data[f"radiation-{archive}-pyranometertemperature"] = DataRecord({
        "Tdic": [Selection(variable_id="Tdic", require_tags={"radiation"}, exclude_tags={"secondary"})],
        "Tdid": [Selection(variable_id="Tdid", require_tags={"radiation"}, exclude_tags={"secondary"})],
        "Tuic": [Selection(variable_id="Tuic", require_tags={"radiation"}, exclude_tags={"secondary"})],
        "Tuid": [Selection(variable_id="Tuid", require_tags={"radiation"}, exclude_tags={"secondary"})],
    })
    radiation_data[f"radiation-{archive}-albedo"] = DataRecord({
        "down": [Selection(variable_id="Rdg", require_tags={"radiation"}, exclude_tags={"secondary"})],
        "up": [Selection(variable_id="Rug", require_tags={"radiation"}, exclude_tags={"secondary"})],
        "zsa": [Selection(variable_id="ZSA", require_tags={"radiation"}, exclude_tags={"secondary"})],
    })
    radiation_data[f"radiation-{archive}-totalratio"] = DataRecord({
        "direct": [Selection(variable_id="Rdn", require_tags={"radiation"}, exclude_tags={"secondary"})],
        "diffuse": [Selection(variable_id="Rdf", require_tags={"radiation"}, exclude_tags={"secondary"})],
        "global": [Selection(variable_id="Rdg", require_tags={"radiation"}, exclude_tags={"secondary"})],
        "zsa": [Selection(variable_id="ZSA", require_tags={"radiation"}, exclude_tags={"secondary"})],
    })
    radiation_data[f"radiation-{archive}-spn1ratio"] = DataRecord({
        "total": [Selection(variable_id="Rst", require_tags={"radiation"}, exclude_tags={"secondary"})],
        "global": [Selection(variable_id="Rdg", require_tags={"radiation"}, exclude_tags={"secondary"})],
        "zsa": [Selection(variable_id="ZSA", require_tags={"radiation"}, exclude_tags={"secondary"})],
    })
    radiation_data[f"radiation-{archive}-diffuseratio"] = DataRecord({
        "diffuse": [Selection(variable_id="Rdf", require_tags={"radiation"}, exclude_tags={"secondary"})],
        "global": [Selection(variable_id="Rdg", require_tags={"radiation"}, exclude_tags={"secondary"})],
        "zsa": [Selection(variable_id="ZSA", require_tags={"radiation"}, exclude_tags={"secondary"})],
    })
    radiation_data[f"radiation-{archive}-pirdownratio"] = DataRecord({
        "pir": [Selection(variable_id="Rdi", require_tags={"radiation"}, exclude_tags={"secondary"})],
        "temperature": [Selection(variable_id="T1", instrument_id="XM1")],
    })
    radiation_data[f"radiation-{archive}-wind"] = DataRecord({
        "WSambient": [Selection(variable_id="WS1", instrument_id="XM1")],
        "WDambient": [Selection(variable_id="WD1", instrument_id="XM1")],
        "WS2": [Selection(variable_id="WS2", instrument_id="XM1")],
        "WD2": [Selection(variable_id="WD2", instrument_id="XM1")],
        "WS3": [Selection(variable_id="WS3", instrument_id="XM1")],
        "WD3": [Selection(variable_id="WD3", instrument_id="XM1")],
    })
    radiation_data[f"radiation-{archive}-temperature"] = DataRecord({
        "Uambient": [Selection(variable_id="U1", instrument_id="XM1")],
        "Tambient": [Selection(variable_id="T1", instrument_id="XM1")],
        "TDambient": [Selection(variable_id="TD1", instrument_id="XM1")],
        "U2": [Selection(variable_id="U2", instrument_id="XM1")],
        "T2": [Selection(variable_id="T2", instrument_id="XM1")],
        "TD2": [Selection(variable_id="TD2", instrument_id="XM1")],
        "U3": [Selection(variable_id="U3", instrument_id="XM1")],
        "T3": [Selection(variable_id="T3", instrument_id="XM1")],
        "TD3": [Selection(variable_id="TD3", instrument_id="XM1")],
    })
    radiation_data[f"radiation-{archive}-pressure"] = DataRecord({
        "Pambient": [Selection(variable_id="P", instrument_id="XM1")],
    })
    radiation_data[f"radiation-{archive}-bsrnqc"] = DataRecord({
        "Tambient": [Selection(variable_id="T1", instrument_id="XM1")],
        "zsa": [Selection(variable_id="ZSA", require_tags={"radiation"}, exclude_tags={"secondary"})],
        "S0": [Selection(variable_id="ZS0", require_tags={"radiation"}, exclude_tags={"secondary"})],
        "AU": [Selection(variable_id="ZAU", require_tags={"radiation"}, exclude_tags={"secondary"})],
        "Rdg": [Selection(variable_id="Rdg", require_tags={"radiation"}, exclude_tags={"secondary"})],
        "Rug": [Selection(variable_id="Rug", require_tags={"radiation"}, exclude_tags={"secondary"})],
        "Rdn": [Selection(variable_id="Rdn", require_tags={"radiation"}, exclude_tags={"secondary"})],
        "Rdf": [Selection(variable_id="Rdf", require_tags={"radiation"}, exclude_tags={"secondary"})],
        "Rdi": [Selection(variable_id="Rdi", require_tags={"radiation"}, exclude_tags={"secondary"})],
        "Rui": [Selection(variable_id="Rui", require_tags={"radiation"}, exclude_tags={"secondary"})],
    })


data_records = dict()
data_records.update(aerosol_data)
data_records.update(ozone_data)
data_records.update(met_data)
data_records.update(radiation_data)


def data_get(station: str, data_name: str, start_epoch_ms: int, end_epoch_ms: int,
             send: typing.Callable[[typing.Dict], typing.Awaitable[None]],
             data: typing.Dict[str, Record]) -> typing.Optional[DataStream]:
    rec = data.get(data_name)
    if not rec:
        return None
    return rec.stream(station, data_name, start_epoch_ms, end_epoch_ms, send)


def get(station: str, data_name: str, start_epoch_ms: int, end_epoch_ms: int,
        send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
    from forge.vis.station.cpd3 import use_cpd3, data_get as cpd3_get
    if use_cpd3(station):
        return cpd3_get(station, data_name, start_epoch_ms, end_epoch_ms, send)
    return data_get(station, data_name, start_epoch_ms, end_epoch_ms, send, data_records)


def modes(station: str, data_name: str) -> typing.List[str]:
    # Just assume the same naming hierarchy
    return ['-'.join(data_name.split('-')[0:2])]
