import typing
from ..default.flags import CPD3Flag


lookup: typing.Dict[str, CPD3Flag] = {
    'self_test_failure': CPD3Flag("SelfTestFault", "Self test failure [code 128]", 0x800000),
    'memory_card_fault': CPD3Flag("MemoryCardFault", "Memory card fault [code 64]", 0x400000),
    'nozzle_fault': CPD3Flag("NozzleFault", "Nozzle fault (whirls >5%) [code 32]", 0x200000),
    'battery_drained': CPD3Flag("BatteryDrained", "Battery drained (0%) [code 16]", 0x100000),
    'battery_low': CPD3Flag("BatteryLow", "Battery low (<10%) [code 8]", 0x080000),
    'pump_current_high': CPD3Flag("PumpCurrentHigh", "Pump current too high (>100%) [code 4]", 0x040000),
    'flow_error': CPD3Flag("FlowError", "Flow error (pump out of range) [code 3]"),
    'pump_low': CPD3Flag("PumpLow", "Pump low, check filter (Imot < 20%) [code 2]", 0x020000),
    'pump_high': CPD3Flag("PumpHigh", "Pump high, check filter (Imot > 60%) [code 1]", 0x020000),
}
