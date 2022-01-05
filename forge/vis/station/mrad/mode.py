import typing
from ..default.mode import Mode, Editing


modes: typing.Dict[str, Mode] = {
    'radiation-editing': Editing('radiation-editing', "Editing", [
        Editing.Entry('radiation-editing-solar', "Solar"),
        Editing.Entry('radiation-editing-ir', "Infrared"),
        Editing.Entry('radiation-editing-pyranometertemperature', "Pyranometer Temperature"),
        Editing.Entry('radiation-editing-albedo', "Albedo"),
        Editing.Entry('radiation-editing-totalratio', "Total Ratio"),
        Editing.Entry('radiation-editing-ambient', "Ambient Conditions"),
        Editing.Entry('radiation-editing-solarposition', "Solar Position"),
    ])
}


def get(station: str, mode_name: str) -> typing.Optional[Mode]:
    return modes.get(mode_name)
