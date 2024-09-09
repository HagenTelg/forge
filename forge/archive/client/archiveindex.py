import typing
from json import loads as from_json, dumps as to_json
from netCDF4 import Dataset, Variable
from forge.data.history import parse_history


class ArchiveIndex:
    INDEX_VERSION = 1
    _EMPTY_SET = frozenset({})

    def __init__(self, json_data: typing.Optional[bytes] = None):
        if not json_data:
            self.tags: typing.Dict[str, typing.Set[str]] = dict()
            self.instrument_codes: typing.Dict[str, typing.Set[str]] = dict()
            self.standard_names: typing.Dict[str, typing.Set[str]] = dict()
            self.variable_ids: typing.Dict[str, typing.Dict[str, int]] = dict()
            self.variable_names: typing.Dict[str, typing.Set[str]] = dict()
            return

        contents = from_json(json_data)
        try:
            version = contents['version']
        except KeyError:
            raise RuntimeError("No index version available")
        if version != self.INDEX_VERSION:
            raise RuntimeError(f"Index version mismatch ({version} vs {self.INDEX_VERSION})")

        self.tags: typing.Dict[str, typing.Set[str]] = {
            k: set(v) for k, v in contents['instrument_tags'].items()
        }
        self.instrument_codes: typing.Dict[str, typing.Set[str]] = {
            k: set(v) for k, v in contents['instrument_codes'].items()
        }
        self.standard_names: typing.Dict[str, typing.Set[str]] = {
            k: set(v) for k, v in contents['standard_names'].items()
        }
        self.variable_names: typing.Dict[str, typing.Set[str]] = {
            k: set(v) for k, v in contents['variable_names'].items()
        }
        self.variable_ids: typing.Dict[str, typing.Dict[str, int]] = {
            var: {
                instrument: int(count) for instrument, count in wl.items()
            } for var, wl in contents['variable_ids'].items()
        }

    def tags_for_instrument_id(self, instrument_id: str) -> typing.Set[str]:
        return self.tags.get(instrument_id, self._EMPTY_SET)

    def instrument_codes_for_instrument_id(self, instrument_id: str) -> typing.Set[str]:
        return self.instrument_codes.get(instrument_id, self._EMPTY_SET)

    @property
    def known_instrument_ids(self) -> typing.Iterable[str]:
        return self.tags.keys()

    def integrate_file(self, file: Dataset) -> None:
        instrument_id = file.instrument_id

        add = getattr(file, 'instrument', None)
        if add:
            instrument_codes = self.instrument_codes.get(instrument_id)
            if not instrument_codes:
                instrument_codes = set()
                self.instrument_codes[instrument_id] = instrument_codes
            instrument_codes.add(add)
            history = getattr(file, 'instrument_history', None)
            if history is not None:
                instrument_codes.update(parse_history(history).values())

        instrument_tags = self.tags.get(instrument_id)
        if not instrument_tags:
            instrument_tags = set()
            self.tags[instrument_id] = instrument_tags
        instrument_tags.update(str(getattr(file, 'forge_tags', "")).split())

        def recurse_group(group: Dataset) -> None:
            def record_variable_id(var: Variable):
                var_id = getattr(var, 'variable_id', None)
                if var_id is None:
                    return
                var_id = str(var_id)
                if not var_id:
                    return

                variable_info = self.variable_ids.get(var_id)
                if not variable_info:
                    variable_info = dict()
                    self.variable_ids[var_id] = variable_info

                if 'wavelength' in var.dimensions:
                    check_group = group
                    while True:
                        try:
                            count = check_group.dimensions['wavelength'].size
                            break
                        except KeyError:
                            check_group = check_group.parent
                            assert check_group is not None
                    variable_info[instrument_id] = max(count, variable_info.get(instrument_id, 0))
                elif 'wavelength' in getattr(var, 'ancillary_variables', "").split():
                    variable_info[instrument_id] = variable_info.get(instrument_id, 1)
                elif var_id not in variable_info:
                    variable_info[instrument_id] = 0

            def record_variable_name(var: Variable):
                name = var.name
                if name in ('time', 'averaged_count', 'averaged_time'):
                    return
                if var.group().parent is None:
                    if name in ('station_name', 'lat', 'lon', 'alt', 'station_inlet_height'):
                        return
                if var.group().name == 'instrument':
                    if name in ('model', 'serial_number', 'firmware_version', 'calibration'):
                        return

                instruments = self.variable_names.get(name)
                if not instruments:
                    instruments = set()
                    self.variable_names[name] = instruments

                instruments.add(instrument_id)

            def record_standard_name_name(var: Variable):
                name = getattr(var, 'standard_name', None)
                if not name:
                    return
                if name == 'time':
                    return
                if var.group().parent is None:
                    if name in ('platform_id', 'latitude', 'longitude', 'altitude', 'height'):
                        return

                instruments = self.standard_names.get(name)
                if not instruments:
                    instruments = set()
                    self.standard_names[name] = instruments

                instruments.add(instrument_id)

            for var in group.variables.values():
                record_variable_id(var)
                record_variable_name(var)
                record_standard_name_name(var)

            for g in group.groups.values():
                if g.name == 'statistics':
                    continue
                recurse_group(g)

        recurse_group(file)

    def integrate_existing(self, contents: bytes) -> None:
        if not contents:
            return
        contents = from_json(contents)
        try:
            version = contents['version']
        except KeyError:
            raise RuntimeError("No index version available")
        if version != self.INDEX_VERSION:
            raise RuntimeError(f"Index version mismatch ({version} vs {self.INDEX_VERSION})")

        def merge_set_lookup(existing: typing.Dict[str, typing.List[str]],
                             destination: typing.Dict[str, typing.Set[str]]):
            for key, values in existing.items():
                target = destination.get(key)
                if not target:
                    target = set()
                    destination[key] = target
                target.update(values)

        merge_set_lookup(contents['instrument_codes'], self.instrument_codes)
        merge_set_lookup(contents['instrument_tags'], self.tags)
        merge_set_lookup(contents['variable_names'], self.variable_names)
        merge_set_lookup(contents['standard_names'], self.standard_names)

        for var_id, instrument_count in contents['variable_ids'].items():
            target = self.variable_ids.get(var_id)
            if not target:
                self.variable_ids[var_id] = instrument_count
                continue
            for instrument, count in instrument_count.items():
                target[instrument] = max(count, target.get(instrument, 0))

    def commit(self) -> bytes:
        result = {
            'version': self.INDEX_VERSION,
            'variable_ids': self.variable_ids,
        }

        def apply_set_lookup(result_key: str, source: typing.Dict[str, typing.Set[str]]):
            output_value = dict()
            for key, values in source.items():
                output_value[key] = sorted(values)
            result[result_key] = output_value

        apply_set_lookup('instrument_codes', self.instrument_codes)
        apply_set_lookup('instrument_tags', self.tags)
        apply_set_lookup('variable_names', self.variable_names)
        apply_set_lookup('standard_names', self.standard_names)

        return to_json(result, sort_keys=True).encode('ascii')