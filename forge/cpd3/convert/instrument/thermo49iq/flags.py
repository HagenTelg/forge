import typing
from ..default.flags import CPD3Flag


lookup: typing.Dict[str, CPD3Flag] = {
    'alarm_intensity_a_high': CPD3Flag("IntensityAHighAlarm", "Cell A frequency max alarm active"),
    'alarm_intensity_b_high': CPD3Flag("IntensityBHighAlarm", "Cell B frequency max alarm active"),
    'lamp_temperature_short': CPD3Flag("LampTemperatureShortAlarm", "Lamp temperature sensor short alarm"),
    'lamp_temperature_open': CPD3Flag("LampTemperatureOpenAlarm", "Lamp temperature sensor open alarm"),
    'sample_temperature_short': CPD3Flag("SampleTemperatureShortAlarm", "Bench temperature sensor short alarm"),
    'sample_temperature_open': CPD3Flag("SampleTemperatureOpenAlarm", "Bench temperature sensor open alarm"),
    'lamp_connection_alarm': CPD3Flag("LampConnectionAlarm", "Lamp connection alarm"),
    'lamp_short': CPD3Flag("LampShortAlarm", "Lamp short alarm"),
    'communications_alarm': CPD3Flag("CommunicationsAlarm", "Communications alarm"),
    'power_supply_alarm': CPD3Flag("PowerSupplyAlarm", "Power supply alarm"),
    'lamp_current_alarm': CPD3Flag("LampCurrentAlarm", "Lamp current alarm"),
    'lamp_temperature_alarm': CPD3Flag("LampTemperatureAlarm", "Lamp temperature alarm"),
    'sample_temperature_alarm': CPD3Flag("SampleTemperatureAlarm", "Bench temperature alarm"),
}
