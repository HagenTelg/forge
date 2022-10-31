import typing
from ..default.flags import CPD3Flag


lookup: typing.Dict[str, CPD3Flag] = {
    'box_temperature_out_of_range': CPD3Flag("BoxTemperatureWarning", "Box temperature warning active"),
    'sample_flow_out_of_range': CPD3Flag("FlowAlarm", "Flow alarm active"),
    'internal_serial_timeout': CPD3Flag("InternalSerialTimeout", "Internal serial timeout detected"),
    'system_reset_warning': CPD3Flag("SystemResetWarning", "System reset detected"),
    'sample_temperature_out_of_range': CPD3Flag("SampleTemperatureWarning", "Sample temperature warning active"),
    'bypass_flow_out_of_range': CPD3Flag("BypassFlowWarning", "Bypass flow warning active"),
    'system_fault_warning': CPD3Flag("SystemFaultWarning", "System fault warning active"),
}
