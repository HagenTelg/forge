#!/usr/bin/env python3

import typing
import os
import logging
import argparse
import time
from copy import deepcopy
from forge.formattime import format_iso8601_time
from forge.timeparse import parse_time_argument
from forge.cpd3.convert.station.lookup import station_data
from forge.cpd3.legacy.readarchive import read_archive, Selection
from write import write_all, EditDirective as BaseEditDirective, EditIndex, Identity, EditConversionFailed, to_json

_LOGGER = logging.getLogger(__name__)


class EditDirective(BaseEditDirective):
    def __init__(self, identity: Identity, info: typing.Dict[str, typing.Any],
                 modified: typing.Optional[float] = None,
                 allocated_uids: typing.Set[int] = None):
        super().__init__(identity, info, modified, allocated_uids)

        self.is_aethalometer_edit = identity.variable == "aethalometer"
        if not self.is_aethalometer_edit and self.end_epoch and self.end_epoch < 1298592000:
            op = self._action.get('Type')
            if isinstance(op, str):
                op = op.lower()
            if op == 'invalidate' or not op:
                sel = self._action.get("Selection")
                if not isinstance(sel, list):
                    sel = [sel]
                for check in sel:
                    if isinstance(check, str):
                        check = {'Variable': check}
                    var = check.get('Variable')
                    if var and str(var).endswith('_A81'):
                        self.is_aethalometer_edit = True
                        break

        if self.end_epoch and self.end_epoch < 1184112000:
            op = self._action.get('Type')
            if isinstance(op, str):
                op = op.lower()

            if op == 'invalidate' or not op:
                sel = self._action.get("Selection")
                if not isinstance(sel, list):
                    sel = [sel]
                all_A12 = False
                for check in sel:
                    if isinstance(check, str):
                        check = {'Variable': check}
                    var = check.get('Variable')
                    if var and str(var).endswith('_A12'):
                        all_A12 = True
                    else:
                        break
                else:
                    if all_A12:
                        self.skip_conversion = True

        if self.end_epoch and self.end_epoch < 1122595200 and self._action.get('Type', '').lower() == 'setcut':
            self.skip_conversion = True

        if self.end_epoch and self.end_epoch < 1325376000 and self._action.get('Type', '').lower() == 'uncontaminate':
            self.skip_conversion = True

    def convert_action(self, action: typing.Dict[str, typing.Any], index: EditIndex) -> typing.Tuple[str, str]:
        op = action.get('Type')
        if isinstance(op, str):
            op = op.lower()

        if op == 'invalidate' or not op:
            sel = action.get("Selection")
            if self.is_aethalometer_edit and self.end_epoch and self.end_epoch < 1298592000 and self.start_epoch and self.start_epoch > 1262304000:
                # Possibly not converted k__cum file, so import edits anyway for future fixup
                try:
                    return super().convert_action(action, index)
                except EditConversionFailed:
                    return "Invalidate", to_json({
                        "selection": [
                            {
                                "instrument_id": "A81",
                                "variable_id": var,
                                "wavelength": wl,
                            }
                            for wl in (370.0, 470.0, 520.0, 590.0, 660.0, 880.0, 950.0)
                            for var in ("Ba", "X")
                        ],
                    }, sort_keys=True)

        return super().convert_action(action, index)

    @property
    def is_optional(self) -> bool:
        return self.disabled or self.is_aethalometer_edit


def main():
    station = "alt"

    parser = argparse.ArgumentParser(description=f"CPD3 legacy conversion for {station.upper()} edit directives")
    parser.add_argument('--debug',
                        dest='debug', action='store_true',
                        help="enable debug output")
    parser.add_argument('--start',
                        dest='start',
                        help="override start time")
    parser.add_argument('--end',
                        dest='end',
                        help="override end time")
    args = parser.parse_args()
    if args.debug:
        from forge.log import set_debug_logger
        set_debug_logger()
    DATA_START_TIME = parse_time_argument(args.start).timestamp() if args.start else station_data(station, 'legacy',
                                                                                                  'DATA_START_TIME')
    DATA_END_TIME = parse_time_argument(args.end).timestamp() if args.end else station_data(station, 'legacy',
                                                                                            'DATA_END_TIME')
    assert DATA_START_TIME < DATA_END_TIME
    begin_time = time.monotonic()
    _LOGGER.info(
        f"Starting edit directive conversion for {station.upper()} in {format_iso8601_time(DATA_START_TIME)} to {format_iso8601_time(DATA_END_TIME)}")

    year_data: typing.Dict[int, typing.List[EditDirective]] = dict()
    cpd3_edits = read_archive([Selection(
        start=DATA_START_TIME,
        end=DATA_END_TIME,
        stations=[station],
        archives=["edits"],
        include_meta_archive=False,
        include_default_station=False,
    )])
    _LOGGER.debug(f"Loaded {len(cpd3_edits)} CPD3 edit directives")
    allocated_uids: typing.Set[int] = set()
    for identity, info, modified in cpd3_edits:
        if identity.start and identity.end and identity.start >= identity.end:
            _LOGGER.debug(f"Skipping zero length edit at {format_iso8601_time(identity.start)}")
            continue

        forge_edits = None
        if info and (((info.get("Parameters") or {}).get("Trigger") or {}).get("Type") or "").lower() == "outsiderange":
            try:
                info1 = deepcopy(info)
                info1["Parameters"]["Trigger"] = {
                    'Type': "Less",
                    'Left': info["Parameters"]["Trigger"]["Value"],
                    'Right': float(info["Parameters"]["Trigger"]["Start"]),
                }
                info2 = deepcopy(info)
                info2["Parameters"]["Trigger"] = {
                    'Type': "Greater",
                    'Left': info["Parameters"]["Trigger"]["Value"],
                    'Right': float(info["Parameters"]["Trigger"]["End"]),
                }

                forge_edits = [
                    EditDirective(identity, info1, modified, allocated_uids=allocated_uids),
                    EditDirective(identity, info2, modified, allocated_uids=allocated_uids),
                ]
            except (ValueError, TypeError, KeyError):
                pass
        if not forge_edits:
            forge_edits = [EditDirective(identity, info, modified, allocated_uids=allocated_uids)]

        for converted in forge_edits:
            if converted.skip_conversion:
                continue
            if converted.is_clap_correction:
                _LOGGER.info(f"Ignoring CLAP correction edit {converted}")
                continue

            for year in range(*converted.affected_years):
                dest = year_data.get(year)
                if not dest:
                    dest = list()
                    year_data[year] = dest
                dest.append(converted)

    total, modified = write_all(station, year_data, DATA_START_TIME, DATA_END_TIME)

    end_time = time.monotonic()
    _LOGGER.info(f"Conversion of {len(cpd3_edits)} ({modified}/{total}) edit directives completed in {(end_time - begin_time):.2f} seconds")


if __name__ == '__main__':
    main()

