#!/usr/bin/env python3

import typing
import os
import logging
from forge.const import STATIONS as VALID_STATIONS
from write import EditDirective as BaseEditDirective, EditIndex, EditConversionFailed, Identity
from default import main

_LOGGER = logging.getLogger(__name__)


class EditDirective(BaseEditDirective):
    def __init__(self, identity: Identity, info: typing.Dict[str, typing.Any],
                 modified: typing.Optional[float] = None,
                 allocated_uids: typing.Set[int] = None):
        super().__init__(identity, info, modified, allocated_uids)

        if self.end_epoch and int(self.end_epoch) <= 1379894400:
            if self._action == {
                "Type": "Invalidate",
                "Selection": [{"Variable": "BaB_A11"}]
            }:
                self.skip_conversion = True
            elif self._action == {
                "Type": "Invalidate",
                "Selection": [{"Variable": "BaR_A11"}]
            }:
                self.skip_conversion = True


station = os.path.basename(__file__).split('.', 1)[0].lower()
assert station in VALID_STATIONS
main(station, EditDirective)
