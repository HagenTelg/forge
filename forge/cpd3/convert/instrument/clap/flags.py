import typing
from ..default.flags import CPD3Flag


lookup: typing.Dict[str, CPD3Flag] = {
    'flow_error': CPD3Flag("FlowError", "Flow error", 0x00020000),
    'led_error': CPD3Flag("LampError", "Lamp error", 0x01000000),
    'temperature_out_of_range': CPD3Flag("TemperatureOutOfRange", "Temperature out of range", 0x02000000),
    'case_temperature_control_error': CPD3Flag("CaseTemperatureUnstable", "Case temperature too far from setpoint", 0x04000000),

    'filter_change': CPD3Flag("FilterChanging", "Changing filter", 0x00010000),
    'white_filter_change': CPD3Flag("WhiteFilterChanging", "Changing to a known white filter"),
    'wait_spot_stability': CPD3Flag("Normalizing", "Establishing normalization factors for the active spot"),
    'bypass_wait_spot_stability': CPD3Flag("Normalizing", "Establishing normalization factors for the active spot"),
    'filter_was_not_white': CPD3Flag("NonWhiteFilter", "Filter did not appear to be white"),
}
