import typing
from ..default.flags import CPD3Flag


lookup: typing.Dict[str, CPD3Flag] = {
    'laser_over_current': CPD3Flag("LaserOverCurrent", "Laser current too high", 0x00010000),
    'first_stage_monitor_over_voltage': CPD3Flag("FirstStageMonitorOverVoltage", "First stage montor voltage greater than 4.7V", 0x00020000),
    'flow_out_of_range': CPD3Flag("FlowOutOfRange", "Flow deviation greater than 20% or flow ratio outside of 5-15", 0x00040000),
    'temperature_out_of_range': CPD3Flag("TemperatureOutOfRange", "Any temperature setpoint more than 10C from the target", 0x00080000),
    'sample_temperature_out_of_range': CPD3Flag("SampleTemperatureOutOfRange", "Sample temperature outside of 0-50C", 0x00100000),
    'opc_error': CPD3Flag("OPCError", "First stage monitor greater than 4.7V and CCN counts too low", 0x00200000),
    'ccn_counts_low': CPD3Flag("CCNCountsLow", "CCN counts too low", 0x00400000),
    'column_temperature_unstable': CPD3Flag("ColumnTemperaturesUnstableAlarm", "Column temperature unstable alarm active", 0x00800000),
    'no_opc_communications': CPD3Flag("NoOPCCommunications", "OPC not communicating with CCN control computer", 0x01000000),
    'duplicate_file': CPD3Flag("DuplicatedFile", "A duplicate file was detected on the CCN computer", 0x02000000),
    'instrument_temperature_instability': CPD3Flag("ReportedTemperatureInstability", "The temperature difference setpoint is unstable as calculated by the instrument", 0x10000000),
    'safe_mode_active': CPD3Flag("SafeModeActive", "The CCN control program has entered safe mode due to a serious alarm", 0x80000000),
}
