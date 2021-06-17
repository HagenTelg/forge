import typing
from copy import deepcopy
from forge.vis.mode import Mode, ModeGroup, VisibleModes
from forge.vis.mode.viewlist import ViewList, Editing
from forge.vis.station.lookup import station_data


def _construct_modes(modes: typing.List[Mode]) -> typing.Dict[str, Mode]:
    result: typing.Dict[str, Mode] = dict()
    for mode in modes:
        result[mode.mode_name] = mode
    return result


aerosol_modes: typing.Dict[str, Mode] = _construct_modes([
    ViewList('aerosol-raw', "Raw", [
        ViewList.Entry('aerosol-raw-counts', "Counts"),
        ViewList.Entry('aerosol-raw-optical', "Optical"),
        ViewList.Entry('aerosol-raw-green', "Green Adjusted"),
        ViewList.Entry('aerosol-raw-aethalometer', "Aethalometer"),
        ViewList.Entry('aerosol-raw-intensive', "Intensive"),
        ViewList.Entry('aerosol-raw-wind', "Wind"),
        ViewList.Entry('aerosol-raw-flow', "Flow"),
        ViewList.Entry('aerosol-raw-temperature', "Temperature and RH"),
        ViewList.Entry('aerosol-raw-pressure', "Pressure"),
        ViewList.Entry('aerosol-raw-nephelometerzero', "Nephelometer Zero"),
        ViewList.Entry('aerosol-raw-nephelometerstatus', "Nephelometer Status"),
        ViewList.Entry('aerosol-raw-clapstatus', "CLAP Status"),
        ViewList.Entry('aerosol-raw-aethalometerstatus', "Aethalometer Status"),
        ViewList.Entry('aerosol-raw-cpcstatus', "CPC Status"),
        ViewList.Entry('aerosol-raw-umacstatus', "μMAC Status"),
    ]),
    Editing('aerosol-editing', "Editing", [
        Editing.Entry('aerosol-editing-counts', "Counts"),
        Editing.Entry('aerosol-editing-scattering', "Scattering"),
        Editing.Entry('aerosol-editing-backscattering', "Back Scattering"),
        Editing.Entry('aerosol-editing-absorption', "Absorption"),
        Editing.Entry('aerosol-editing-aethalometer', "Aethalometer"),
        Editing.Entry('aerosol-editing-aethalometerstatus', "Aethalometer Status"),
        Editing.Entry('aerosol-editing-wind', "Wind"),
        Editing.Entry('aerosol-editing-intensive', "Intensive"),
        Editing.Entry('aerosol-editing-extensive', "Extensive"),
    ]),
    ViewList('aerosol-clean', "Clean", [
        ViewList.Entry('aerosol-clean-counts', "Counts"),
        ViewList.Entry('aerosol-clean-optical', "Optical"),
        ViewList.Entry('aerosol-clean-green', "Green Adjusted"),
        ViewList.Entry('aerosol-clean-aethalometer', "Aethalometer"),
        ViewList.Entry('aerosol-clean-intensive', "Intensive"),
        ViewList.Entry('aerosol-clean-extensive', "Extensive"),
        ViewList.Entry('aerosol-clean-wind', "Wind"),
    ]),
    ViewList('aerosol-avgh', "Hourly Average", [
        ViewList.Entry('aerosol-avgh-counts', "Counts"),
        ViewList.Entry('aerosol-avgh-optical', "Optical"),
        ViewList.Entry('aerosol-avgh-green', "Green Adjusted"),
        ViewList.Entry('aerosol-avgh-aethalometer', "Aethalometer"),
        ViewList.Entry('aerosol-avgh-intensive', "Intensive"),
        ViewList.Entry('aerosol-avgh-extensive', "Extensive"),
        ViewList.Entry('aerosol-avgh-wind', "Wind"),
    ]),
])
ozone_modes: typing.Dict[str, Mode] = _construct_modes([
    ViewList('ozone-raw', "Raw", [
        ViewList.Entry('ozone-raw-concentration', "Ozone"),
        ViewList.Entry('ozone-raw-status', "Status"),
        ViewList.Entry('ozone-raw-cells', "Cells"),
        ViewList.Entry('ozone-raw-wind', "Wind"),
    ]),
    ViewList('ozone-editing', "Editing", [
        ViewList.Entry('ozone-editing-concentration', "Ozone"),
        ViewList.Entry('ozone-editing-wind', "Wind"),
    ]),
    ViewList('ozone-clean', "Clean", [
        ViewList.Entry('ozone-clean-concentration', "Ozone"),
        ViewList.Entry('ozone-clean-wind', "Wind"),
    ]),
    ViewList('ozone-avgh', "Hourly Average", [
        ViewList.Entry('ozone-avgh-concentration', "Ozone"),
        ViewList.Entry('ozone-avgh-wind', "Wind"),
    ]),
])
met_modes: typing.Dict[str, Mode] = _construct_modes([
    ViewList('met-raw', "Raw", [
        ViewList.Entry('met-raw-wind', "Wind"),
        ViewList.Entry('met-raw-temperature', "Temperature and RH"),
        ViewList.Entry('met-raw-pressure', "Pressure"),
    ]),
    ViewList('met-editing', "Editing", [
        ViewList.Entry('met-editing-windspeed', "Wind Speed"),
        ViewList.Entry('met-editing-winddirection', "Wind Direction"),
        ViewList.Entry('met-editing-temperature', "Temperature"),
        ViewList.Entry('met-editing-dewpoint', "Dewpoint"),
        ViewList.Entry('met-editing-rh', "Relative Humidity"),
        ViewList.Entry('met-editing-pressure', "Pressure"),
    ]),
    ViewList('met-clean', "Clean", [
        ViewList.Entry('met-clean-wind', "Wind"),
        ViewList.Entry('met-clean-temperature', "Temperature and RH"),
        ViewList.Entry('met-clean-pressure', "Pressure"),
    ]),
    ViewList('met-avgh', "Hourly Average", [
        ViewList.Entry('met-avgh-wind', "Wind"),
        ViewList.Entry('met-avgh-temperature', "Temperature and RH"),
        ViewList.Entry('met-avgh-pressure', "Pressure"),
    ]),
])


def detach(*modes: typing.Dict[str, Mode]) -> typing.Dict[str, Mode]:
    result: typing.Dict[str, Mode] = dict()
    for add in modes:
        result.update(deepcopy(add))
    return result


def get(station: str, mode_name: str) -> typing.Optional[Mode]:
    return aerosol_modes.get(mode_name)


def visible(station: str, mode_name: typing.Optional[str] = None) -> VisibleModes:
    lookup: typing.Callable[[str, str], typing.Optional[Mode]] = station_data(station, 'mode', 'get')
    visible_modes = VisibleModes()

    def _assemble_mode(display_name: str, mode_names: typing.List[str]):
        result = ModeGroup(display_name)
        for mode in mode_names:
            add = lookup(station, mode)
            if not add:
                continue
            result.modes.append(add)
        if len(result.modes) == 0:
            return
        visible_modes.groups.append(result)

    _assemble_mode("Aerosol", [
        'aerosol-raw',
        'aerosol-editing',
        'aerosol-clean',
        'aerosol-avgh',
    ]),
    _assemble_mode("Ozone", [
        'ozone-raw',
        'ozone-editing',
        'ozone-clean',
        'ozone-avgh',
    ]),
    _assemble_mode("Metrological", [
        'met-raw',
        'met-editing',
        'met-clean',
        'met-avgh',
    ]),

    return visible_modes
