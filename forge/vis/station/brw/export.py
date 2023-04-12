import typing
from forge.vis.export import ExportList
from ..default.export import visible as default_visible


async def visible(station: str, mode_name: str) -> typing.Optional[ExportList]:
    # Disable per Betsy/Ian email 2023-04-12 so it hides from the station techs
    if mode_name.startswith('met-'):
        return None
    return await default_visible(station, mode_name)
