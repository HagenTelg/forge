import typing
from ..default.flags import CPD3Flag


lookup: typing.Dict[str, CPD3Flag] = {
    'caps_board_communication_error': CPD3Flag("CAPSBoardCommunicatorsWarning", "CAPS board communications warning active"),
    'cell_pressure_out_of_range': CPD3Flag("CellPressureWarning", "Cell pressure warning active"),
    'reference_out_of_range': CPD3Flag("ReferenceWarning", "Manual auto reference warning active"),
    'ozone_pressure_out_of_range': CPD3Flag("OzonePressureWarning", "Ozone pressure warning active"),
    'system_reset_warning': CPD3Flag("SystemResetWarning","System reset warning active"),
    'ozone_tower_communications_error': CPD3Flag("OzoneTowerCommunicationsWarning", "Ozone tower communication warning active"),
    'sample_flow_out_of_range': CPD3Flag("SampleFlowWarning", "Sample flow warning active"),
    'sample_pressure_out_of_range': CPD3Flag("SamplePressureWarning", "Sample pressure warning active"),
    'sample_temperature_out_of_range': CPD3Flag("SampleTemperatureWarning", "Sample temperature warning active"),
    'auto_calibration_sequence_failed': CPD3Flag("AutoCalibrationSequenceFailed", "Auto calibration sequence failed"),
}
