import typing
import asyncio
import base64
import time
import struct
import logging
from signal import SIGTERM
from math import floor, ceil
from copy import deepcopy
from starlette.responses import StreamingResponse
from forge.const import __version__
from forge.vis import CONFIGURATION
from forge.vis.access import BaseAccessUser
from forge.vis.data.stream import DataStream, RecordStream
from forge.vis.export import Export, ExportList
from forge.cpd3.identity import Name, Identity
from forge.cpd3.variant import serialize as variant_serialize
from forge.cpd3.variant import deserialize as variant_deserialize
from forge.cpd3.datareader import StandardDataInput, RecordInput


_LOGGER = logging.getLogger(__name__)
_interface = CONFIGURATION.get('CPD3.INTERFACE', 'cpd3_forge_interface')


def _to_cpd3_selection(selection: typing.List[typing.Dict[str, str]]) -> typing.List[typing.Any]:
    if not isinstance(selection, list):
        return []
    result = []
    for entry in selection:
        if not isinstance(entry, dict):
            continue
        if entry.get('type') != 'variable':
            continue
        variable = entry.get('variable')
        if not variable or not isinstance(variable, str):
            continue
        add = {
            'Variable': variable
        }
        if 'station' in entry:
            add['Station'] = entry['station']
        if 'archive' in entry:
            add['Archive'] = entry['archive']
        if 'flavors' in entry:
            add['Flavors'] = entry['flavors']
        if 'has_flavors' in entry:
            add['HasFlavors'] = entry['has_flavors']
        if 'lacks_flavors' in entry:
            add['LacksFlavors'] = entry['lacks_flavors']
        result.append(add)
    return result


def _from_cpd3_selection(selection: typing.Any) -> typing.List[typing.Dict[str, typing.Any]]:
    def append_add(item, key, add):
        if key not in result:
            item[key] = []
        item[key].append(add)

    def single_or_list(value):
        if isinstance(value, list) and len(value) == 1:
            return value[0]
        return value

    if selection is None:
        return []
    elif isinstance(selection, str):
        parts = selection.split(':')
        if len(parts) == 0:
            return []
        elif len(parts) == 1:
            if len(parts[0]) == 0:
                return []
            return [{'variable': parts[0]}]
        elif len(parts) == 2:
            return [{'archive': parts[0], 'variable': parts[1]}]
        elif len(parts) == 3:
            return [{'station': parts[0], 'archive': parts[1], 'variable': parts[2]}]
        else:
            result = {'station': parts[0], 'archive': parts[1], 'variable': parts[2]}
            for i in range(3, len(parts)):
                flavor = parts[i]
                if flavor.startswith('!') or flavor.startswith('-'):
                    result.pop('flavors')
                    append_add(result, 'lacks_flavors', flavor[1:])
                elif flavor.startswith('='):
                    result.pop('lacks_flavors')
                    result.pop('has_flavors')
                    if len(flavor) == 1:
                        result['flavors'] = ['']
                    else:
                        append_add(result, 'flavors', flavor[1:])
                elif flavor.startswith('+'):
                    result.pop('flavors')
                    append_add(result, 'has_flavors', flavor[1:])
                else:
                    result.pop('flavors')
                    append_add(result, 'has_flavors', flavor)
    elif isinstance(selection, dict):
        return _from_cpd3_selection([selection])

    result: typing.List[typing.Dict[str, typing.Any]] = list()
    for entry in selection:
        converted: typing.Dict[str, typing.Any] = {
            'type': 'variable',
        }
        if 'Station' in entry:
            converted['station'] = single_or_list(entry['Station'])
        if 'Archive' in entry:
            converted['archive'] = single_or_list(entry['Archive'])
        if 'Variable' in entry:
            converted['variable'] = single_or_list(entry['Variable'])
        if 'Flavors' in entry:
            converted['flavors'] = entry['Flavors']
        else:
            if 'HasFlavors' in entry:
                converted['has_flavors'] = entry['HasFlavors']
            if 'LacksFlavors' in entry:
                converted['lacks_flavors'] = entry['LacksFlavors']
        result.append(converted)
    return result


def _to_cpd3_calibration(calibration: typing.List[float]) -> typing.List[float]:
    if not isinstance(calibration, list):
        return []
    result = []
    for coefficient in calibration:
        try:
            coefficient = float(coefficient)
        except (ValueError, TypeError):
            return []
        result.append(coefficient)
    while len(result) > 0 and result[-1] == 0.0:
        result.pop()
    return result


def _from_cpd3_calibration(calibration: typing.Any) -> typing.List[float]:
    if calibration is None:
        return []

    if isinstance(calibration, float) or isinstance(calibration, int):
        return [float(calibration)]
    elif isinstance(calibration, dict):
        return _from_cpd3_calibration(calibration.get('Coefficients'))
    elif not isinstance(calibration, list):
        return []

    result = []
    for coefficient in calibration:
        try:
            coefficient = float(coefficient)
        except (ValueError, TypeError):
            return []
        result.append(coefficient)
    return result


def _selection_to_single_cutsize(selection: typing.List[typing.Dict[str, typing.Any]]) -> typing.Optional[str]:
    if len(selection) != 1:
        return None
    if selection[0] == {'HasFlavors': ['pm10']}:
        return 'pm10'
    if selection[0] == {'HasFlavors': ['pm25']}:
        return 'pm25'
    if selection[0] == {'HasFlavors': ['pm1']}:
        return 'pm1'
    if selection[0] == {
        'LacksFlavors': ['pm1', 'pm10', 'pm25'],
        'Variable': '((Ba[cfs]*)|(Bb?s)|Be|Ir|L|(N[nbs]?)|(X[cfs]*))[BGRQ0-9]*_.*',
    }:
        return ''
    return None


def _to_cpd3_action(directive: typing.Dict[str, typing.Any]) -> typing.Dict[str, typing.Any]:
    op = directive.get('action', 'invalidate')
    if op == 'contaminate':
        return {
            'Type': 'Contaminate',
        }
    elif op == 'calibration':
        return {
            'Type': 'Polynomial',
            'Selection': _to_cpd3_selection(directive.get('selection')),
            'Calibration': _to_cpd3_calibration(directive.get('calibration')),
        }
    elif op == 'recalibrate':
        return {
            'Type': 'Recalibrate',
            'Selection': _to_cpd3_selection(directive.get('selection')),
            'Calibration': _to_cpd3_calibration(directive.get('calibration')),
            'Original': _to_cpd3_calibration(directive.get('reverse_calibration')),
        }
    elif op == 'flow_correction':
        instrument = str(directive.get('instrument', '')).strip()
        if len(instrument) <= 0:
            raise ValueError
        return {
            'Type': 'FlowCorrection',
            'Instrument': str(directive.get('instrument', '')),
            'Calibration': _to_cpd3_calibration(directive.get('calibration')),
            'Original': _to_cpd3_calibration(directive.get('reverse_calibration')),
        }
    elif op == 'cut_size':
        original_size = str(directive.get('cutsize', '')).strip()
        if original_size == '':
            original_selection = [{
                'LacksFlavors': ['pm1', 'pm10', 'pm25'],
                'Variable': '((Ba[cfs]*)|(Bb?s)|Be|Ir|L|(N[nbs]?)|(X[cfs]*))[BGRQ0-9]*_.*',
            }]
        else:
            original_selection = [{
                'HasFlavors': [original_size],
            }]

        modified_size = str(directive.get('modified_cutsize', 'invalidate'))
        if modified_size == 'invalidate':
            return {
                'Type': 'Invalidate',
                'Selection': original_selection,
            }

        # Not a perfect test, but good enough for most use cases
        is_stuck_impactor = ((original_size == 'pm1' and modified_size == 'pm10') or
                             (original_size == 'pm10' and modified_size == 'pm1'))
        return {
            'Type': 'SetCut',
            'ApplyToMetadata': not is_stuck_impactor,
            'Cut': modified_size.lower(),
            'Selection': original_selection,
        }
    else:
        return {
            'Type': 'Invalidate',
            'Selection': _to_cpd3_selection(directive.get('selection')),
        }


def _new_directive(user: BaseAccessUser, station: str, profile: str,
                   directive: typing.Dict[str, typing.Any]) -> typing.Tuple[Identity, typing.Dict[str, typing.Any]]:
    start = directive.get('start_epoch_ms')
    end = directive.get('end_epoch_ms')
    start = floor(start) if start else None
    end = ceil(end) if end else None

    identity = Identity(station=station, archive='edits', variable=profile,
                        start=(start / 1000.0 if start else None),
                        end=(end / 1000.0 if end else None))

    result = {
        'Author': directive.get('author'),
        'Comment': directive.get('comment'),
        'History': [{
            'Type': 'Created',
            'At': time.time(),
            'Environment': 'forge-vis',
            'Version': __version__,
            'User': user.display_name,
        }],
        'Parameters': {
            'Action': _to_cpd3_action(directive),
        },
    }

    return identity, result


def _modify_directive(user: BaseAccessUser, station: str, profile: str,
                      existing: typing.Dict[str, typing.Any], identity: Identity,
                      modification: typing.Dict[str, typing.Any]) -> None:
    history = existing.get('History')
    if not history:
        history = []
        existing['History'] = history

    def add_history(info):
        info['At'] = time.time()
        info['Environment'] = 'forge-vis'
        info['Version'] = __version__
        info['User'] = user.display_name
        history.append(info)

    if identity.name.variable != profile:
        add_history({
            'Type': 'ProfileChanged',
            'OriginalProfile': identity.name.variable,
        })
        identity.name.variable = profile

    start = modification.get('start_epoch_ms')
    end = modification.get('end_epoch_ms')
    start = (floor(start) / 1000.0) if start else None
    end = (ceil(end) / 1000.0) if end else None
    if identity.start != start or identity.end != end:
        add_history({
            'Type': 'BoundsChanged',
            'OriginalBounds': {'Start': identity.start, 'End': identity.end},
            'RevisedBounds': {'Start': start, 'End': end},
        })
        identity.start = start
        identity.end = end

    parameters: typing.Dict[str, typing.Any] = {
        'Action': _to_cpd3_action(modification),
    }
    if existing.get('Parameters') != parameters:
        add_history({
            'Type': 'ParametersChanged',
            'OriginalParameters': existing['Parameters'],
        })
        existing['Parameters'] = parameters

    if existing.get('Author') != modification.get('author'):
        add_history({
            'Type': 'AuthorChanged',
            'OriginalAuthor': existing.get('Author'),
        })
        existing['Author'] = modification.get('author')

    if existing.get('Comment') != modification.get('comment'):
        add_history({
            'Type': 'CommentChanged',
            'OriginalComment': existing.get('Comment'),
        })
        existing['Comment'] = modification.get('author')

    was_enabled = not existing.get('Disabled')
    is_enabled = not modification.get('deleted')
    if was_enabled and not is_enabled:
        add_history({
            'Type': 'Disabled',
        })
        existing['Disabled'] = True
    elif not was_enabled and is_enabled:
        add_history({
            'Type': 'Enabled',
        })
        existing.pop('Disabled')


def _convert_history(history: typing.List[typing.Dict[str, typing.Any]]) -> typing.List[typing.Dict[str, typing.Any]]:
    def format_time(ts: float) -> str:
        if not ts:
            return "∞"
        ts = time.gmtime(ts)
        return f"{ts.tm_year:04}-{ts.tm_mon:02}-{ts.tm_mday:02}T{ts.tm_hour:02}:{ts.tm_min:02}:{ts.tm_sec:02}Z"

    result: typing.List[typing.Dict[str, typing.Any]] = list()
    for entry in history:
        operation = entry.get('Type')
        if operation == 'ProfileChanged':
            operation = f"Profile changed from {entry['OriginalProfile']}"
        elif operation == 'BoundsChanged':
            operation = f"Bounds change from {format_time(entry['OriginalBounds']['Start'])} - {format_time(entry['OriginalBounds']['End'])}"
        elif operation == 'ParametersChanged':
            operation = "Parameters changed"
        elif operation == 'ExtendChanged':
            operation = "Extension changed"
        elif operation == 'AuthorChanged':
            operation = f"Author changed from {entry['OriginalAuthor']}"
        elif operation == 'CommentChanged':
            operation = "Comment changed"
        elif operation == 'PriorityChanged':
            operation = "Priority changed"
        elif operation == 'SystemInternalChanged':
            operation = "System internal flag changed"
        elif operation == 'Disabled':
            operation = "Deleted"
        elif operation == 'Enabled':
            operation = "Restored"
        elif operation == 'Created':
            operation = "Created"

        result.append({
            'time_epoch_ms': round(entry.get('At', 0) * 1000),
            'user': entry.get('User', ''),
            'operation': operation,
        })

    return result


def _convert_directive(profile: str, identity: Identity,
                       original: typing.Dict[str, typing.Any]) -> typing.Dict[str, typing.Any]:
    result = {
        '_id': base64.b64encode(identity.serialize()).decode('ascii'),
        'start_epoch_ms': floor(identity.start * 1000) if identity.start else None,
        'end_epoch_ms': ceil(identity.end * 1000) if identity.end else None,
        'author': original.get('Author', ""),
        'comment': original.get('Comment', ""),
        'history': _convert_history(original.get('History', [])),
        'other_type': identity.variable != profile,
        'type': identity.variable.title(),
        'condition': {'type': 'none'},
    }

    try:
        result['modified_epoch_ms'] = result['history'][-1]['time_epoch_ms']
    except IndexError:
        pass

    if original.get('Disabled'):
        result['deleted'] = True

    parameters: typing.Dict[str, typing.Any] = original.get('Parameters', {})

    action: typing.Dict[str, typing.Any] = parameters.get('Action', {})
    op = action.get('Type')
    if isinstance(op, str):
        op = op.lower()
    if op == 'contaminate' or op == 'contam':
        result['action'] = 'contaminate'
    elif op == 'polynomial' or op == 'poly' or op == 'cal' or op == 'calibration':
        result['action'] = 'calibration'
        result['selection'] = _from_cpd3_selection(action.get('Selection'))
        result['calibration'] = _from_cpd3_calibration(action.get('Calibration'))
    elif op == 'recalibrate':
        result['action'] = 'recalibrate'
        result['selection'] = _from_cpd3_selection(action.get('Selection'))
        result['calibration'] = _from_cpd3_calibration(action.get('Calibration'))
        result['reverse_calibration'] = _from_cpd3_calibration(action.get('Original'))
    elif op == 'flowcorrection' or op == 'flowcalibration':
        result['action'] = 'flow_correction'
        result['instrument'] = str(action.get('Instrument'), '')
        result['calibration'] = _from_cpd3_calibration(action.get('Calibration'))
        result['reverse_calibration'] = _from_cpd3_calibration(action.get('Original'))
    elif op == 'setcut' or op == 'cut':
        result['action'] = 'cut_size'
        result['cutsize'] = _selection_to_single_cutsize(_from_cpd3_selection(parameters.get('Selection')))
        result['modified_cutsize'] = str(action.get('Cut'), '')
    else:
        selection = _from_cpd3_selection(action.get('Selection'))
        single_cutsize = _selection_to_single_cutsize(selection)
        if single_cutsize is not None:
            result['action'] = 'cut_size'
            result['cutsize'] = single_cutsize
            result['modified_cutsize'] = 'invalidate'
        else:
            result['action'] = 'invalidate'
            result['selection'] = selection

    return result


def _display_directive(raw: typing.Dict[str, typing.Any]) -> bool:
    if raw.get('Parameters', {}).get('Trigger'):
        return False
    if raw.get('SystemInternal'):
        return False

    def is_valid_action(parameters: typing.Dict[str, typing.Any]):
        action = parameters.get('Type')
        if action is None:
            return True
        if not isinstance(action, str):
            return False
        action = action.lower()

        def _matches(*args):
            for arg in args:
                if action == arg.lower():
                    return True
            return False

        if _matches("Remove"):
            return False
        elif _matches("Poly", "Polynomial", "Cal", "Calibration"):
            return True
        elif _matches("PolyInvert", "PolynomialInvert", "InvertCal", "InvertCalibration"):
            return False
        elif _matches("Recalibrate"):
            return True
        elif _matches("Wrap", "Modular", "Modulus"):
            return False
        elif _matches("Overlay", "Set"):
            return False
        elif _matches("Meta", "Metadata", "OverlayMeta", "OverlayMetadata"):
            return False
        elif _matches("Serial", "SetSerial"):
            return False
        elif _matches("Flag", "Addflag", "AddFlags"):
            return False
        elif _matches("RemoveFlag", "RemoveFlags"):
            return False
        elif _matches("Contaminate", "Contam"):
            return True
        elif _matches("Uncontaminate", "ClearContam"):
            return False
        elif _matches("FlowCorrection", "FlowCalibration"):
            if len(raw.get('Instrument', '')) > 0:
                return True
            return False
        elif _matches("Spot", "SpotSize"):
            return False
        elif _matches("MultiSpot", "MultiSpotSize", "CLAPSpot"):
            return False
        elif _matches("Unit", "SetUnit", "Duplicate"):
            return False
        elif _matches("UnitReplace", "Translate", "DuplicateTranslate"):
            return False
        elif _matches("Flavors"):
            return False
        elif _matches("SetCut", "Cut"): 
            if _selection_to_single_cutsize(_from_cpd3_selection(parameters.get('Selection'))) is None:
                return False
            return True
        elif _matches("MultiPoly", "MultiPolynomial", "MultiCal", "MultiCalibration"):
            return False
        elif _matches("Arithmetic", "Math", "Function"):
            return False
        elif _matches("ScriptValue", "ScriptValues"):
            return False
        elif _matches("ScriptSegment", "ScriptSegments", "Script"):
            return False
        elif _matches("ScriptDemultiplexer", "Demultiplexer"):
            return False
        elif _matches("ScriptGeneralValue", "ScriptGeneralValues"):
            return False
        elif _matches("ScriptGeneralSegment", "ScriptGeneralSegments"):
            return False
        elif _matches("NOOP", "None"):
            return False

        # Invalidate
        return True

    if not is_valid_action(raw.get('Parameters', {}).get('Action', {})):
        return False
    return True


async def _write_directive(user: BaseAccessUser, station: str, profile: str,
                           directive: typing.Dict[str, typing.Any]) -> typing.Optional[typing.Dict]:
    if '_id' not in directive:
        try:
            identity, raw = _new_directive(user, station, profile, directive)
        except:
            _LOGGER.debug(f"Error creating directive for {user.display_id} on {station}:{profile}", exc_info=True)
            return None

        target = await asyncio.create_subprocess_exec(_interface, 'directive_create',
                                                      stdout=asyncio.subprocess.PIPE,
                                                      stdin=asyncio.subprocess.PIPE)

        target.stdin.write(identity.serialize())
        target.stdin.write(variant_serialize(raw))
        target.stdin.close()

        result = await target.stdout.read()
        await target.wait()
        if target.returncode != 0:
            _LOGGER.warning(f"Error writing directive to database, return code {target.returncode}")
            return None
        if not result:
            return None
        identity = Identity.deserialize(result)

        return _convert_directive(profile, identity, raw)

    try:
        identity = Identity.deserialize(base64.b64decode(directive['_id']))
    except:
        _LOGGER.debug(f"Error reading directive ID for {user.display_id} on {station}:{profile}", exc_info=True)
        return None
    if identity.station != station or identity.archive != 'edits' or len(identity.flavors) != 0:
        _LOGGER.debug(f"Invalid directive ID {identity} for {user.display_id} on {station}:{profile}", exc_info=True)
        return None

    while True:
        target = await asyncio.create_subprocess_exec(_interface, 'directive_rmw',
                                                      stdout=asyncio.subprocess.PIPE,
                                                      stdin=asyncio.subprocess.PIPE)

        target.stdin.write(identity.serialize())
        try:
            n = struct.unpack('<I', await target.stdout.readexactly(4))[0]
            existing = await target.stdout.readexactly(n)
            if len(existing) != n:
                _LOGGER.debug(f"Error reading directive {identity} for {user.display_id} on {station}:{profile}")
                target.terminate()
                await target.wait()
                return None
        except:
            _LOGGER.debug(f"Error reading directive {identity} for {user.display_id} on {station}:{profile}",
                          exc_info=True)
            try:
                target.terminate()
                target.stdin.close()
                await target.wait()
            except OSError:
                pass
            return None

        existing = variant_deserialize(existing)
        if not isinstance(existing, dict):
            _LOGGER.debug(f"Error deserializing directive {identity}")
            try:
                target.terminate()
                await target.wait()
            except OSError:
                pass
            return None

        modified_identity = Identity(station=station, archive='edits', variable=identity.variable,
                                     start=identity.start, end=identity.end)
        try:
            _modify_directive(user, station, profile, existing, modified_identity, directive)
        except:
            _LOGGER.debug(f"Error modifying directive for {user.display_id} on {station}:{profile}", exc_info=True)
            try:
                target.terminate()
                target.stdin.close()
                await target.wait()
            except OSError:
                pass
            return None

        target.stdin.write(modified_identity.serialize())
        target.stdin.write(variant_serialize(existing))
        target.stdin.close()

        result = await target.stdout.read()
        await target.wait()
        if target.returncode == 100:
            continue
        if target.returncode != 0:
            _LOGGER.warning(f"Error modifying directive ({identity}) in database, return code {target.returncode}")
            return None

        identity = Identity.deserialize(result)
        break

    return _convert_directive(profile, identity, existing)


async def _queue_pass(station: str, profile: str, start_epoch: int, end_epoch: int, comment: str) -> None:
    reader, writer = await asyncio.open_unix_connection(
        CONFIGURATION.get('CPD3.PASS.SOCKET', '/run/forge-cpd3-pass.socket'))
    header = struct.pack('<BQQ', 0, start_epoch, end_epoch)
    for a in (station, profile, comment):
        raw = a.encode('utf-8')
        header += struct.pack('<I', len(raw))
        header += raw
    writer.write(header)
    await writer.drain()
    await reader.read(1)
    writer.close()


async def _get_latest_passed(station: str, profile: str) -> typing.Optional[int]:
    class Input(StandardDataInput):
        def __init__(self):
            super().__init__()
            self.latest_passed: typing.Optional[int] = None

        def value_ready(self, identity: Identity, value: typing.Any) -> None:
            if not identity.end:
                return
            if not self.latest_passed or self.latest_passed < identity.end:
                self.latest_passed = int(ceil(identity.end))

    async def read_passed(start_epoch: int, end_epoch: int) -> typing.Optional[int]:
        _LOGGER.debug(f"Starting latest passed read for {station} {profile} {start_epoch},{end_epoch}")
        reader = await asyncio.create_subprocess_exec(_interface, 'archive_read',
                                                      str(start_epoch), str(end_epoch),
                                                      f'{station}:passed:{profile}:=',
                                                      stdout=asyncio.subprocess.PIPE,
                                                      stdin=asyncio.subprocess.DEVNULL)
        await reader.stdout.readexactly(3)

        status = Input()
        while True:
            data = await reader.stdout.read(65536)
            if not data:
                break
            status.incoming_raw(data)
        await reader.wait()
        return status.latest_passed

    current_end = int(time.time())
    current_start = current_end - 366 * 24 * 60 * 60
    last_possible_start = current_start - 10 * 366 * 24 * 60 * 60
    while current_start > last_possible_start:
        latest = await read_passed(current_start, current_end)
        if latest:
            return latest * 1000
        current_end = current_start
        current_start = current_end - 366 * 24 * 60 * 60
    return None


class DataReader(RecordStream):
    _PASS_STALL_ARCHIVES = frozenset({"clean", "avgh"})

    class Input(RecordInput):
        def __init__(self, reader: "DataReader",
                     record_buffer: typing.List[typing.Tuple[int, typing.Dict[Name, typing.Any]]]):
            super().__init__()
            self.reader = reader
            self.record_buffer = record_buffer

        def record_ready(self, start: typing.Optional[float], end: typing.Optional[float],
                         record: typing.Dict[Name, typing.Any]) -> None:
            if not start:
                return
            start = round(start * 1000)
            if start < self.reader.clip_start_ms:
                start = self.reader.clip_start_ms
            self.record_buffer.append((start, record))

        def record_break(self, start: float, end: float) -> None:
            start = round(start * 1000)
            if start < self.reader.clip_start_ms:
                start = self.reader.clip_start_ms
            self.record_buffer.append((start, {}))

    def __init__(self, start_epoch_ms: int, end_epoch_ms: int,
                 data: typing.Dict[Name, str],
                 send: typing.Callable[[typing.Dict], typing.Awaitable[None]]):
        super().__init__(send, list(data.values()))
        self.clip_start_ms = start_epoch_ms
        self.start_epoch = int(floor(start_epoch_ms / 1000.0))
        self.end_epoch = int(ceil(end_epoch_ms / 1000.0))
        self.data = data

    async def _convert(self, epoch_ms: int, record: typing.Dict[Name, typing.Any]):
        fields: typing.Dict[str, typing.Optional[float]] = dict()

        def convert_value(value):
            if value is None:
                return None
            if isinstance(value, float):
                return value
            if isinstance(value, int):
                return value
            if isinstance(value, list):
                for i in range(len(value)):
                    check = value[i]
                    if i is None:
                        continue
                    try:
                        value[i] = float(check)
                    except (ValueError, TypeError):
                        value[i] = None
                return value
            try:
                return float(value)
            except (ValueError, TypeError):
                return None

        for name, value in record.items():
            target = self.data.get(name)
            if target is None:
                continue
            fields[target] = convert_value(value)
        await self.send_record(epoch_ms, fields)

    async def create_reader(self) -> asyncio.subprocess.Process:
        selections = list()
        for sel in self.data:
            if len(sel.flavors) == 0:
                arg = ':='
            else:
                arg = ''
                for f in sel.flavors:
                    arg += f':={f}'
            arg = f'{sel.station}:{sel.archive}:{sel.variable}' + arg
            selections.append(arg)
        _LOGGER.debug(f"Starting data read for {self.start_epoch},{self.end_epoch} with {len(selections)} selections")
        return await asyncio.create_subprocess_exec(_interface, 'archive_read',
                                                    str(self.start_epoch), str(self.end_epoch),
                                                    *selections,
                                                    stdout=asyncio.subprocess.PIPE,
                                                    stdin=asyncio.subprocess.DEVNULL)

    class _CleanReadyStall(RecordStream.Stall):
        def __init__(self, readers: typing.List[asyncio.StreamReader], writers: typing.List[asyncio.StreamWriter]):
            super().__init__("Passed data update in progress")
            self.readers = readers
            self.writers = writers

        async def block(self):
            for r in self.readers:
                try:
                    await r.read(1)
                except (OSError, EOFError):
                    pass
            for w in self.writers:
                try:
                    w.close()
                except OSError:
                    pass

    async def stall(self) -> typing.Optional["DataReader._CleanReadyStall"]:
        blocking_stations = set()
        for check in self.data.keys():
            if check.archive in self._PASS_STALL_ARCHIVES:
                blocking_stations.add(check.station)

        if len(blocking_stations) == 0:
            return None

        readers: typing.List[asyncio.StreamReader] = list()
        writers: typing.List[asyncio.StreamWriter] = list()

        for station in blocking_stations:
            try:
                reader, writer = await asyncio.open_unix_connection(
                    CONFIGURATION.get('CPD3.PASS.SOCKET', '/run/forge-cpd3-pass.socket'))
                enc = station.encode('utf-8')
                writer.write(struct.pack('<BI', 1, len(enc)))
                writer.write(enc)
                await writer.drain()
                response = await reader.read(1)
                if not response:
                    writer.close()
                    continue
            except (OSError, EOFError):
                continue
            readers.append(reader)
            writers.append(writer)

        if len(readers) == 0:
            return None
        return self._CleanReadyStall(readers, writers)

    async def run(self) -> None:
        reader = await self.create_reader()

        try:
            await reader.stdout.readexactly(3)

            buffer: typing.List[typing.Tuple[int, typing.Dict[Name, typing.Any]]] = list()
            converter = self.Input(self, buffer)

            while True:
                data = await reader.stdout.read(65536)
                if not data:
                    break
                converter.incoming_raw(data)
                for r in buffer:
                    await self._convert(r[0], r[1])
                buffer.clear()

            converter.flush()
            for r in buffer:
                await self._convert(r[0], r[1])
            await self.flush()
            await reader.wait()
        except asyncio.CancelledError:
            try:
                reader.terminate()
            except:
                pass
            raise


class EditedReader(DataReader):
    class Input(DataReader.Input):
        def value_ready(self, identity: Identity, value: typing.Any) -> None:
            if identity.name not in self.reader.data:
                return
            super().value_ready(identity, value)

    def __init__(self, start_epoch_ms: int, end_epoch_ms: int, station: str, profile: str,
                 data: typing.Dict[Name, str],
                 send: typing.Callable[[typing.Dict], typing.Awaitable[None]]):
        # Limit data request amount, so we don't bog down the system
        if end_epoch_ms - start_epoch_ms > 32 * 24 * 60 * 60 * 1000:
            end_epoch_ms = start_epoch_ms + 32 * 24 * 60 * 60 * 1000

        super().__init__(start_epoch_ms=start_epoch_ms, end_epoch_ms=end_epoch_ms, data=data, send=send)
        self.station = station
        self.profile = profile

    async def create_reader(self) -> asyncio.subprocess.Process:
        _LOGGER.debug(f"Starting edited read for {self.station}-{self.profile} {self.start_epoch},{self.end_epoch}")

        return await asyncio.create_subprocess_exec(_interface, 'edited_read',
                                                    str(self.start_epoch), str(self.end_epoch),
                                                    self.station, self.profile,
                                                    stdout=asyncio.subprocess.PIPE,
                                                    stdin=asyncio.subprocess.DEVNULL)

    async def stall(self) -> typing.Optional["DataStream.Stall"]:
        return None


class EditReader(DataStream):
    class _Input(StandardDataInput):
        def __init__(self, profile: str, result: typing.List[typing.Dict]):
            super().__init__()
            self.profile = profile
            self.result = result

        def value_ready(self, identity: Identity, value: typing.Any) -> None:
            if not isinstance(value, dict):
                return
            if not _display_directive(value):
                return
            self.result.append(_convert_directive(self.profile, identity, value))

    def __init__(self, station: str, profile: str, start_epoch_ms: int, end_epoch_ms: int,
                 send: typing.Callable[[typing.Dict], typing.Awaitable[None]]):
        super().__init__(send)
        self.station = station
        self.profile = profile
        self.start_epoch = int(floor(start_epoch_ms / 1000.0))
        self.end_epoch = int(ceil(end_epoch_ms / 1000.0))

    async def run(self) -> None:
        reader = await asyncio.create_subprocess_exec(_interface, 'directive_read',
                                                      str(self.start_epoch), str(self.end_epoch), self.station,
                                                      stdout=asyncio.subprocess.PIPE,
                                                      stdin=asyncio.subprocess.DEVNULL)
        try:
            await reader.stdout.readexactly(3)

            buffer: typing.List[typing.Dict] = list()
            converter = self._Input(self.profile, buffer)

            while True:
                data = await reader.stdout.read(65536)
                if not data:
                    break
                converter.incoming_raw(data)
                for r in buffer:
                    await self.send(r)
                buffer.clear()

            for r in buffer:
                await self.send(r)
            await reader.wait()
        except asyncio.CancelledError:
            try:
                reader.terminate()
            except OSError:
                pass
            raise


class EditAvailable(DataStream):
    def __init__(self, station: str, profile: str, start_epoch_ms: int, end_epoch_ms: int,
                 send: typing.Callable[[typing.Dict], typing.Awaitable[None]]):
        super().__init__(send)
        self.station = station
        self.profile = profile
        self.start_epoch = int(floor(start_epoch_ms / 1000.0))
        self.end_epoch = int(ceil(end_epoch_ms / 1000.0))

    async def run(self) -> None:
        reader = await asyncio.create_subprocess_exec(_interface, 'edited_available',
                                                      str(self.start_epoch), str(self.end_epoch),
                                                      self.station, self.profile,
                                                      stdout=asyncio.subprocess.PIPE,
                                                      stdin=asyncio.subprocess.DEVNULL)
        try:
            while True:
                line = await reader.stdout.readline()
                if not line:
                    break
                line = line.decode('utf-8').strip()
                if not line:
                    continue
                await self.send({
                    'type': 'variable',
                    'variable': line,
                })

            await reader.wait()
        except asyncio.CancelledError:
            try:
                reader.terminate()
            except:
                pass
            raise


class ContaminationReader(DataStream):
    class _State:
        def __init__(self):
            self.active_contamination: typing.Set[str] = set()
            self.active_start: typing.Optional[int] = None
            self.buffer: typing.List[typing.Dict[str, typing.Any]] = list()

        def complete(self, end_ms: int) -> None:
            if not self.active_start:
                return
            self.buffer.append({
                'start_epoch_ms': self.active_start,
                'end_epoch_ms': end_ms,
                #'flags': list(self.active_contamination),
            })
            self.active_start = None
            self.active_contamination.clear()

        async def flush(self, send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> None:
            for segment in self.buffer:
                await send(segment)
            self.buffer.clear()

        def convert(self, start_ms: int, record: typing.Dict[Name, typing.Any]) -> None:
            contamination_flags: typing.Set[str] = set()
            for value in record.values():
                if not isinstance(value, set):
                    continue
                for flag in value:
                    if flag.startswith('contam') or flag.startswith('Contam'):
                        contamination_flags.add(flag)
            if contamination_flags == self.active_contamination:
                return
            self.complete(start_ms)
            if len(contamination_flags) == 0:
                return
            self.active_contamination = contamination_flags
            self.active_start = start_ms

        def record_break(self, start_ms: int) -> None:
            self.complete(start_ms)

    class Input(RecordInput):
        def __init__(self, reader: "ContaminationReader", state: "ContaminationReader._State"):
            super().__init__()
            self.reader = reader
            self.state = state

        def record_ready(self, start: typing.Optional[float], end: typing.Optional[float],
                         record: typing.Dict[Name, typing.Any]) -> None:
            if not start:
                return
            start = round(start * 1000)
            if start < self.reader.clip_start_ms:
                start = self.reader.clip_start_ms
            self.state.convert(start, record)

        def record_break(self, start: float, end: float) -> None:
            start = round(start * 1000)
            if start < self.reader.clip_start_ms:
                start = self.reader.clip_start_ms
            self.state.record_break(start)

    def __init__(self, start_epoch_ms: int, end_epoch_ms: int,
                 data: typing.Set[Name],
                 send: typing.Callable[[typing.Dict], typing.Awaitable[None]]):
        super().__init__(send)
        self.clip_start_ms = start_epoch_ms
        self.clip_end_ms = end_epoch_ms
        self.start_epoch = int(floor(start_epoch_ms / 1000.0))
        self.end_epoch = int(ceil(end_epoch_ms / 1000.0))
        self.data = data

    async def create_reader(self) -> asyncio.subprocess.Process:
        selections = list()
        for sel in self.data:
            selections.append(f'{sel.station}:{sel.archive}:{sel.variable}:-cover:-stats')
        _LOGGER.debug(f"Starting contamination read for {self.start_epoch},{self.end_epoch} with {len(selections)} selections")
        return await asyncio.create_subprocess_exec(_interface, 'archive_read',
                                                    str(self.start_epoch), str(self.end_epoch),
                                                    *selections,
                                                    stdout=asyncio.subprocess.PIPE,
                                                    stdin=asyncio.subprocess.DEVNULL)

    async def run(self) -> None:
        reader = await self.create_reader()

        try:
            await reader.stdout.readexactly(3)

            state = self._State()
            converter = self.Input(self, state)

            while True:
                data = await reader.stdout.read(65536)
                if not data:
                    break
                converter.incoming_raw(data)
                await state.flush(self.send)

            converter.flush()
            state.complete(self.clip_end_ms)
            await state.flush(self.send)
            await reader.wait()
        except asyncio.CancelledError:
            try:
                reader.terminate()
            except:
                pass
            raise


class EditedContaminationReader(ContaminationReader):
    class Input(ContaminationReader.Input):
        def value_ready(self, identity: Identity, value: typing.Any) -> None:
            if 'cover' in identity.flavors or 'stats' in identity.flavors:
                return
            check = Name(station=identity.station, archive=identity.archive, variable=identity.variable)
            if check not in self.reader.data:
                return
            super().value_ready(identity, value)

    def __init__(self, start_epoch_ms: int, end_epoch_ms: int, station: str, profile: str,
                 data: typing.Set[Name],
                 send: typing.Callable[[typing.Dict], typing.Awaitable[None]]):
        # Limit data request amount, so we don't bog down the system
        if end_epoch_ms - start_epoch_ms > 32 * 24 * 60 * 60 * 1000:
            end_epoch_ms = start_epoch_ms + 32 * 24 * 60 * 60 * 1000

        super().__init__(start_epoch_ms=start_epoch_ms, end_epoch_ms=end_epoch_ms, data=data, send=send)
        self.station = station
        self.profile = profile

    async def create_reader(self) -> asyncio.subprocess.Process:
        _LOGGER.debug(f"Starting edited contamination read for {self.station}-{self.profile} {self.start_epoch},{self.end_epoch}")

        return await asyncio.create_subprocess_exec(_interface, 'edited_read',
                                                    str(self.start_epoch), str(self.end_epoch),
                                                    self.station, self.profile,
                                                    stdout=asyncio.subprocess.PIPE,
                                                    stdin=asyncio.subprocess.DEVNULL)

    async def stall(self) -> typing.Optional["DataStream.Stall"]:
        return None


class EventLogReader(DataStream):
    class _Input(StandardDataInput):
        def __init__(self, result: typing.List[typing.Dict]):
            super().__init__()
            self.result = result

        @staticmethod
        def _convert_event(identity: Identity, value: typing.Any) -> typing.Dict[str, typing.Any]:
            result: typing.Dict[str, typing.Any] = {
                'epoch_ms': floor(identity.start * 1000) if identity.start else None,
                'message': value.get('Text', ""),
            }

            if identity.variable == 'acquisition':
                if value.get('Source') == 'EXTERNAL':
                    result['type'] = "User"
                    result['author'] = value.get('Author', "")
                else:
                    result['type'] = "Instrument"
                    result['author'] = value.get('Source', "")
                    result['acquisition'] = True

            return result

        def value_ready(self, identity: Identity, value: typing.Any) -> None:
            if not isinstance(value, dict):
                return
            if not _display_directive(value):
                return
            self.result.append(self._convert_event(identity, value))

    def __init__(self, station: str, start_epoch_ms: int, end_epoch_ms: int,
                 send: typing.Callable[[typing.Dict], typing.Awaitable[None]]):
        super().__init__(send)
        self.station = station
        self.start_epoch = int(floor(start_epoch_ms / 1000.0))
        self.end_epoch = int(ceil(end_epoch_ms / 1000.0))

    async def run(self) -> None:
        reader = await asyncio.create_subprocess_exec(_interface, 'archive_read',
                                                      str(self.start_epoch), str(self.end_epoch),
                                                      f'{self.station}:events:acquisition:=',
                                                      stdout=asyncio.subprocess.PIPE,
                                                      stdin=asyncio.subprocess.DEVNULL)
        try:
            await reader.stdout.readexactly(3)

            buffer: typing.List[typing.Dict] = list()
            converter = self._Input(buffer)

            while True:
                data = await reader.stdout.read(65536)
                if not data:
                    break
                converter.incoming_raw(data)
                for r in buffer:
                    await self.send(r)
                buffer.clear()

            for r in buffer:
                await self.send(r)
            await reader.wait()
        except asyncio.CancelledError:
            try:
                reader.terminate()
            except:
                pass
            raise


def editing_get(station: str, mode_name: str, start_epoch_ms: int, end_epoch_ms: int,
                send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
    profile = mode_name.split('-', 1)[0]
    return EditReader(station, profile, start_epoch_ms, end_epoch_ms, send)


def editing_available(station: str, mode_name: str, start_epoch_ms: int, end_epoch_ms: int,
                      send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
    profile = mode_name.split('-', 1)[0]
    return EditAvailable(station, profile, start_epoch_ms, end_epoch_ms, send)


def editing_writable(user: BaseAccessUser, station: str, mode_name: str,
                     directive: typing.Dict[str, typing.Any]) -> bool:
    profile = mode_name.split('-', 1)[0]
    if '_id' in directive:
        try:
            identity = Identity.deserialize(base64.b64decode(directive['_id']))
        except:
            return False
        if identity.station != station:
            # Require doing it from the station targeted
            return False
        if identity.variable == profile:
            # Already checked
            return True
        return user.allow_mode(station, identity.variable + '-editing', write=True)
    return True


def editing_save(user: BaseAccessUser, station: str, mode_name: str,
                 directive: typing.Dict[str, typing.Any]) -> typing.Awaitable[typing.Optional[dict]]:
    profile = mode_name.split('-', 1)[0]
    return _write_directive(user, station, profile, directive)


def editing_pass(station: str, mode_name: str, start_epoch_ms: int, end_epoch_ms: int,
                 comment: typing.Optional[str] = None) -> typing.Awaitable[None]:
    profile = mode_name.split('-', 1)[0]
    start_epoch = int(floor(start_epoch_ms / 1000.0))
    end_epoch = int(ceil(end_epoch_ms / 1000.0))
    if not comment:
        comment = ''
    return _queue_pass(station, profile, start_epoch, end_epoch, comment)


def eventlog_get(station: str, mode_name: str, start_epoch_ms: int, end_epoch_ms: int,
                 send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
    return EventLogReader(station, start_epoch_ms, end_epoch_ms, send)


def data_get(station: str, data_name: str, start_epoch_ms: int, end_epoch_ms: int,
             send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
    return data_profile_get(station, data_name, start_epoch_ms, end_epoch_ms, send, profile_data)


def latest_passed(station: str, mode_name: str) -> typing.Awaitable[typing.Optional[int]]:
    profile = mode_name.split('-', 1)[0]
    return _get_latest_passed(station, profile)


def data_profile_get(station: str, data_name: str, start_epoch_ms: int, end_epoch_ms: int,
                     send: typing.Callable[[typing.Dict], typing.Awaitable[None]],
                     lookup: typing.Dict[str, typing.Dict[str, typing.Dict[str, typing.Callable[[str, int, int, typing.Callable], DataStream]]]]) -> typing.Optional[DataStream]:
    components = data_name.split('-', 2)
    if len(components) != 3:
        return None
    profile = components[0]
    archive = components[1]
    record = components[2]

    result = lookup.get(profile)
    if not result:
        _LOGGER.debug(f"No information for profile in {data_name}")
        return None
    result = result.get(archive)
    if not result:
        _LOGGER.debug(f"No information for archive in {data_name}")
        return None
    result = result.get(record)
    if not result:
        _LOGGER.debug(f"No information for record in {data_name}")
        return None
    return result(station, start_epoch_ms, end_epoch_ms, send)


class DataExport(Export):
    def __init__(self, start_epoch_ms: int, end_epoch_ms: int, export_mode: str, data: typing.Set[Name],
                 limit_flavors: bool = False, filename: str = 'export.csv', media_type: str = 'text/csv'):
        self.start_epoch = int(floor(start_epoch_ms / 1000.0))
        self.end_epoch = int(ceil(end_epoch_ms / 1000.0))
        self.export_mode = export_mode
        self.data = data
        self.limit_flavors = limit_flavors
        self.filename = filename
        self.media_type = media_type

    async def create_reader(self) -> asyncio.subprocess.Process:
        selections = list()
        for sel in self.data:
            arg = ''
            if len(sel.flavors) == 0:
                if self.limit_flavors:
                    arg = ':='
            else:
                for f in sel.flavors:
                    arg += f':={f}'
            arg = f'{sel.station}:{sel.archive}:{sel.variable}' + arg
            selections.append(arg)
        _LOGGER.debug(f"Starting data export for {self.start_epoch},{self.end_epoch} with {len(selections)} selections")
        return await asyncio.create_subprocess_exec(_interface, 'export', self.export_mode,
                                                    str(self.start_epoch), str(self.end_epoch),
                                                    *selections,
                                                    stdout=asyncio.subprocess.PIPE,
                                                    stdin=asyncio.subprocess.DEVNULL)

    async def __call__(self) -> StreamingResponse:
        reader = await self.create_reader()

        async def run():
            while True:
                chunk = await reader.stdout.read(4096)
                if not chunk:
                    break
                yield chunk
            try:
                reader.terminate()
            except:
                pass
            try:
                await reader.wait()
            except OSError:
                pass

        return StreamingResponse(run(), media_type=self.media_type, headers={
            'Content-Disposition': f'attachment; filename="{self.filename}"',
        })


class DataExportList(ExportList):
    class Entry(ExportList.Entry):
        def __init__(self, key: str, display: str, data: typing.Callable[[str, int, int], Export],
                     time_limit_days: typing.Optional[int] = 366):
            super().__init__(key, display)
            self.data = data
            if time_limit_days:
                self.time_limit_ms: typing.Optional[int] = time_limit_days * 86400 * 1000
            else:
                self.time_limit_ms: typing.Optional[int] = None

        def __deepcopy__(self, memo):
            y = type(self)(self.key, self.display, self.data)
            y.time_limit_ms = self.time_limit_ms
            memo[id(self)] = y
            return y

    def __init__(self, exports: typing.Optional[typing.List["DataExportList.Entry"]] = None):
        super().__init__(exports)

    def create_export(self, station: str, export_key: str,
                      start_epoch_ms: int, end_epoch_ms: int) -> typing.Optional[Export]:
        for export in self.exports:
            if export.key == export_key:
                if export.time_limit_ms and (end_epoch_ms - start_epoch_ms) > export.time_limit_ms:
                    return None
                return export.data(station, start_epoch_ms, end_epoch_ms)
        return None


def export_profile_lookup(station: str, mode_name: str, lookup) -> typing.Optional[DataExportList]:
    components = mode_name.split('-', 2)
    if len(components) != 2:
        return None
    profile = components[0]
    archive = components[1]

    return lookup.get(profile, {}).get(archive)


def export_get(station: str, mode_name: str, export_key: str,
               start_epoch_ms: int, end_epoch_ms: int) -> typing.Optional[Export]:
    return export_profile_get(station, mode_name, export_key, start_epoch_ms, end_epoch_ms, profile_export)


def export_available(station: str, mode_name: str) -> typing.Optional[ExportList]:
    return export_profile_lookup(station, mode_name, profile_export)


def export_profile_get(station: str, mode_name: str, export_key: str, start_epoch_ms: int, end_epoch_ms: int,
                       lookup: typing.Dict[str, typing.Dict[str, DataExportList]]) -> typing.Optional[DataExportList]:
    components = mode_name.split('-', 2)
    if len(components) != 2:
        return None
    profile = components[0]
    archive = components[1]

    result = lookup.get(profile)
    if not result:
        _LOGGER.debug(f"No information for profile in {mode_name}")
        return None
    result = result.get(archive)
    if not result:
        _LOGGER.debug(f"No information for archive in {mode_name}")
        return None
    return result.create_export(station, export_key, start_epoch_ms, end_epoch_ms)


def detach(*profiles: typing.Union[
        typing.Dict[str, typing.Dict[str, typing.Dict[str, typing.Callable[[str, int, int, typing.Callable], DataStream]]]],
        typing.Dict[str, typing.Dict[str, DataExportList]]]) -> typing.Union[
        typing.Dict[str, typing.Dict[str, typing.Dict[str, typing.Callable[[str, int, int, typing.Callable], DataStream]]]],
        typing.Dict[str, typing.Dict[str, DataExportList]]]:
    result: typing.Union[
        typing.Dict[str, typing.Dict[str, typing.Dict[str, typing.Callable[[str, int, int, typing.Callable], DataStream]]]],
        typing.Dict[str, typing.Dict[str, DataExportList]]] = dict()
    for profile in profiles:
        result.update(deepcopy(profile))
    return result


aerosol_export: typing.Dict[str, DataExportList] = {
    'raw': DataExportList([
        DataExportList.Entry('extensive', "Extensive", lambda station, start_epoch_ms, end_epoch_ms: DataExport(
            start_epoch_ms, end_epoch_ms, 'basic', {
                Name(station, 'raw', 'T_S11'),
                Name(station, 'raw', 'P_S11'),
                Name(station, 'raw', 'U_S11'),
                Name(station, 'raw', 'BsB_S11'),
                Name(station, 'raw', 'BsG_S11'),
                Name(station, 'raw', 'BsR_S11'),
                Name(station, 'raw', 'BbsB_S11'),
                Name(station, 'raw', 'BbsG_S11'),
                Name(station, 'raw', 'BbsR_S11'),
                Name(station, 'raw', 'BaB_A11'),
                Name(station, 'raw', 'BaG_A11'),
                Name(station, 'raw', 'BaR_A11'),
                Name(station, 'raw', 'N_N71'),
                Name(station, 'raw', 'N_N61'),
            },
        )),
        DataExportList.Entry('scattering', "Scattering", lambda station, start_epoch_ms, end_epoch_ms: DataExport(
            start_epoch_ms, end_epoch_ms, 'basic', {
                Name(station, 'raw', 'T_S11'),
                Name(station, 'raw', 'P_S11'),
                Name(station, 'raw', 'U_S11'),
                Name(station, 'raw', 'BsB_S11'),
                Name(station, 'raw', 'BsG_S11'),
                Name(station, 'raw', 'BsR_S11'),
                Name(station, 'raw', 'BbsB_S11'),
                Name(station, 'raw', 'BbsG_S11'),
                Name(station, 'raw', 'BbsR_S11'),
            },
        )),
        DataExportList.Entry('absorption', "Absorption", lambda station, start_epoch_ms, end_epoch_ms: DataExport(
            start_epoch_ms, end_epoch_ms, 'basic', {
                Name(station, 'raw', 'Q_A11'),
                Name(station, 'raw', 'L_A11'),
                Name(station, 'raw', 'Fn_A11'),
                Name(station, 'raw', 'BaB_A11'),
                Name(station, 'raw', 'BaG_A11'),
                Name(station, 'raw', 'BaR_A11'),
                Name(station, 'raw', 'IrB_A11'),
                Name(station, 'raw', 'IrG_A11'),
                Name(station, 'raw', 'IrR_A11'),
            },
        )),
        DataExportList.Entry('counts', "Counts", lambda station, start_epoch_ms, end_epoch_ms: DataExport(
            start_epoch_ms, end_epoch_ms, 'unsplit', {
                Name(station, 'raw', 'N_N71'),
                Name(station, 'raw', 'N_N61'),
            },
        )),
        DataExportList.Entry('aethalometer', "Aethalometer", lambda station, start_epoch_ms, end_epoch_ms: DataExport(
            start_epoch_ms, end_epoch_ms, 'unsplit', set(
                [Name(station, 'raw', f'Ba{i + 1}_A81') for i in range(7)] +
                [Name(station, 'raw', f'X{i + 1}_A81') for i in range(7)] +
                [Name(station, 'raw', f'ZFACTOR{i + 1}_A81') for i in range(7)] +
                [Name(station, 'raw', f'Ir{i + 1}_A81') for i in range(7)]
            ),
        )),
    ]),
    'clean': DataExportList([
        DataExportList.Entry('intensive', "Intensive", lambda station, start_epoch_ms, end_epoch_ms: DataExport(
            start_epoch_ms, end_epoch_ms, 'basic', {
                Name(station, 'clean', 'N_XI'),
                Name(station, 'clean', 'BsB_XI'),
                Name(station, 'clean', 'BsG_XI'),
                Name(station, 'clean', 'BsR_XI'),
                Name(station, 'clean', 'BaB_XI'),
                Name(station, 'clean', 'BaG_XI'),
                Name(station, 'clean', 'BaR_XI'),
                Name(station, 'clean', 'ZSSAG_XI'),
                Name(station, 'clean', 'ZBfrG_XI'),
                Name(station, 'clean', 'ZAngBsG_XI'),
                Name(station, 'clean', 'ZRFEG_XI'),
                Name(station, 'clean', 'ZGG_XI'),
            },
        )),
        DataExportList.Entry('scattering', "Scattering", lambda station, start_epoch_ms, end_epoch_ms: DataExport(
            start_epoch_ms, end_epoch_ms, 'basic', {
                Name(station, 'clean', 'T_S11'),
                Name(station, 'clean', 'P_S11'),
                Name(station, 'clean', 'U_S11'),
                Name(station, 'clean', 'BsB_S11'),
                Name(station, 'clean', 'BsG_S11'),
                Name(station, 'clean', 'BsR_S11'),
                Name(station, 'clean', 'BbsB_S11'),
                Name(station, 'clean', 'BbsG_S11'),
                Name(station, 'clean', 'BbsR_S11'),
            },
        )),
        DataExportList.Entry('absorption', "Absorption", lambda station, start_epoch_ms, end_epoch_ms: DataExport(
            start_epoch_ms, end_epoch_ms, 'basic', {
                Name(station, 'clean', 'Q_A11'),
                Name(station, 'clean', 'L_A11'),
                Name(station, 'clean', 'Fn_A11'),
                Name(station, 'clean', 'BaB_A11'),
                Name(station, 'clean', 'BaG_A11'),
                Name(station, 'clean', 'BaR_A11'),
                Name(station, 'clean', 'IrB_A11'),
                Name(station, 'clean', 'IrG_A11'),
                Name(station, 'clean', 'IrR_A11'),
            },
        )),
        DataExportList.Entry('counts', "Counts", lambda station, start_epoch_ms, end_epoch_ms: DataExport(
            start_epoch_ms, end_epoch_ms, 'unsplit', {
                Name(station, 'clean', 'N_N71'),
                Name(station, 'clean', 'N_N61'),
            },
        )),
        DataExportList.Entry('aethalometer', "Aethalometer", lambda station, start_epoch_ms, end_epoch_ms: DataExport(
            start_epoch_ms, end_epoch_ms, 'unsplit', set(
                [Name(station, 'clean', f'Ba{i + 1}_A81') for i in range(7)] +
                [Name(station, 'clean', f'X{i + 1}_A81') for i in range(7)] +
                [Name(station, 'clean', f'ZFACTOR{i + 1}_A81') for i in range(7)] +
                [Name(station, 'clean', f'Ir{i + 1}_A81') for i in range(7)]
            ),
        )),
    ]),
    'avgh': DataExportList([
        DataExportList.Entry('intensive', "Intensive", lambda station, start_epoch_ms, end_epoch_ms: DataExport(
            start_epoch_ms, end_epoch_ms, 'average', {
                Name(station, 'avgh', 'N_XI'),
                Name(station, 'avgh', 'BsB_XI'),
                Name(station, 'avgh', 'BsG_XI'),
                Name(station, 'avgh', 'BsR_XI'),
                Name(station, 'avgh', 'BaB_XI'),
                Name(station, 'avgh', 'BaG_XI'),
                Name(station, 'avgh', 'BaR_XI'),
                Name(station, 'avgh', 'ZSSAG_XI'),
                Name(station, 'avgh', 'ZBfrG_XI'),
                Name(station, 'avgh', 'ZAngBsG_XI'),
                Name(station, 'avgh', 'ZRFEG_XI'),
                Name(station, 'avgh', 'ZGG_XI'),
            },
        ), time_limit_days=None),
        DataExportList.Entry('scattering', "Scattering", lambda station, start_epoch_ms, end_epoch_ms: DataExport(
            start_epoch_ms, end_epoch_ms, 'average', {
                Name(station, 'avgh', 'T_S11'),
                Name(station, 'avgh', 'P_S11'),
                Name(station, 'avgh', 'U_S11'),
                Name(station, 'avgh', 'BsB_S11'),
                Name(station, 'avgh', 'BsG_S11'),
                Name(station, 'avgh', 'BsR_S11'),
                Name(station, 'avgh', 'BbsB_S11'),
                Name(station, 'avgh', 'BbsG_S11'),
                Name(station, 'avgh', 'BbsR_S11'),
            },
        ), time_limit_days=None),
        DataExportList.Entry('absorption', "Absorption", lambda station, start_epoch_ms, end_epoch_ms: DataExport(
            start_epoch_ms, end_epoch_ms, 'average', {
                Name(station, 'avgh', 'Q_A11'),
                Name(station, 'avgh', 'L_A11'),
                Name(station, 'avgh', 'Fn_A11'),
                Name(station, 'avgh', 'BaB_A11'),
                Name(station, 'avgh', 'BaG_A11'),
                Name(station, 'avgh', 'BaR_A11'),
                Name(station, 'avgh', 'IrB_A11'),
                Name(station, 'avgh', 'IrG_A11'),
                Name(station, 'avgh', 'IrR_A11'),
            },
        ), time_limit_days=None),
        DataExportList.Entry('counts', "Counts", lambda station, start_epoch_ms, end_epoch_ms: DataExport(
            start_epoch_ms, end_epoch_ms, 'average', {
                Name(station, 'avgh', 'N_N71'),
                Name(station, 'avgh', 'N_N61'),
            },
        ), time_limit_days=None),
        DataExportList.Entry('aethalometer', "Aethalometer", lambda station, start_epoch_ms, end_epoch_ms: DataExport(
            start_epoch_ms, end_epoch_ms, 'average', set(
                [Name(station, 'avgh', f'Ba{i + 1}_A81') for i in range(7)] +
                [Name(station, 'avgh', f'X{i + 1}_A81') for i in range(7)] +
                [Name(station, 'avgh', f'ZFACTOR{i + 1}_A81') for i in range(7)] +
                [Name(station, 'avgh', f'Ir{i + 1}_A81') for i in range(7)]
            ),
        ), time_limit_days=None),
    ]),
}

ozone_export: typing.Dict[str, DataExportList] = {
    'raw': DataExportList([
        DataExportList.Entry('basic', "Basic", lambda station, start_epoch_ms, end_epoch_ms: DataExport(
            start_epoch_ms, end_epoch_ms, 'unsplit', {
                Name(station, 'raw', 'X_G81'),
                Name(station, 'raw', 'T1_G81'),
                Name(station, 'raw', 'T2_G81'),
                Name(station, 'raw', 'P_G81'),
                Name(station, 'raw', 'P1_G81'),                
                Name(station, 'raw', 'Q1_G81'),
                Name(station, 'raw', 'Q2_G81'),
                Name(station, 'raw', 'C1_G81'),
                Name(station, 'raw', 'C2_G81'),
                Name(station, 'raw', 'WS1_XM1'),
                Name(station, 'raw', 'WD1_XM1'),
            },
        )),
    ]),
    'clean': DataExportList([
        DataExportList.Entry('basic', "Basic", lambda station, start_epoch_ms, end_epoch_ms: DataExport(
            start_epoch_ms, end_epoch_ms, 'unsplit', {
                Name(station, 'clean', 'X_G81'),
                Name(station, 'clean', 'T1_G81'),
                Name(station, 'clean', 'T2_G81'),
                Name(station, 'clean', 'P_G81'),
                Name(station, 'clean', 'P1_G81'),                
                Name(station, 'clean', 'Q1_G81'),
                Name(station, 'clean', 'Q2_G81'),
                Name(station, 'clean', 'C1_G81'),
                Name(station, 'clean', 'C2_G81'),
                Name(station, 'clean', 'WS1_XM1'),
                Name(station, 'clean', 'WD1_XM1'),
            },
        )),
    ]),
    'avgh': DataExportList([
        DataExportList.Entry('basic', "Basic", lambda station, start_epoch_ms, end_epoch_ms: DataExport(
            start_epoch_ms, end_epoch_ms, 'unsplit', {
                Name(station, 'avgh', 'X_G81'),
                Name(station, 'avgh', 'T1_G81'),
                Name(station, 'avgh', 'T2_G81'),
                Name(station, 'avgh', 'P_G81'),
                Name(station, 'avgh', 'P1_G81'),                
                Name(station, 'avgh', 'Q1_G81'),
                Name(station, 'avgh', 'Q2_G81'),
                Name(station, 'avgh', 'C1_G81'),
                Name(station, 'avgh', 'C2_G81'),
                Name(station, 'avgh', 'WS1_XM1'),
                Name(station, 'avgh', 'WD1_XM1'),
            },
        )),
    ]),
}

met_export: typing.Dict[str, DataExportList] = {
    'raw': DataExportList([
        DataExportList.Entry('ambient', "Ambient", lambda station, start_epoch_ms, end_epoch_ms: DataExport(
            start_epoch_ms, end_epoch_ms, 'unsplt', {
                Name(station, 'raw', 'WS1_XM1'), Name(station, 'raw', 'WD1_XM1'),
                Name(station, 'raw', 'WS2_XM1'), Name(station, 'raw', 'WD2_XM1'),
                Name(station, 'raw', 'WS3_XM1'), Name(station, 'raw', 'WD3_XM1'),
                Name(station, 'raw', 'T1_XM1'), Name(station, 'raw', 'U1_XM1'), Name(station, 'raw', 'TD1_XM1'),
                Name(station, 'raw', 'T2_XM1'), Name(station, 'raw', 'U2_XM1'), Name(station, 'raw', 'TD2_XM1'),
                Name(station, 'raw', 'T3_XM1'), Name(station, 'raw', 'U3_XM1'), Name(station, 'raw', 'TD3_XM1'),
                Name(station, 'raw', 'P_XM1'),
                Name(station, 'raw', 'WI_XM1'),
            },
        )),
    ]),
    'clean': DataExportList([
        DataExportList.Entry('ambient', "Ambient", lambda station, start_epoch_ms, end_epoch_ms: DataExport(
            start_epoch_ms, end_epoch_ms, 'unsplt', {
                Name(station, 'clean', 'WS1_XM1'), Name(station, 'clean', 'WD1_XM1'),
                Name(station, 'clean', 'WS2_XM1'), Name(station, 'clean', 'WD2_XM1'),
                Name(station, 'clean', 'WS3_XM1'), Name(station, 'clean', 'WD3_XM1'),
                Name(station, 'clean', 'T1_XM1'), Name(station, 'clean', 'U1_XM1'), Name(station, 'clean', 'TD1_XM1'),
                Name(station, 'clean', 'T2_XM1'), Name(station, 'clean', 'U2_XM1'), Name(station, 'clean', 'TD2_XM1'),
                Name(station, 'clean', 'T3_XM1'), Name(station, 'clean', 'U3_XM1'), Name(station, 'clean', 'TD3_XM1'),
                Name(station, 'clean', 'P_XM1'),
                Name(station, 'clean', 'WI_XM1'),
            },
        )),
    ]),
    'avgh': DataExportList([
        DataExportList.Entry('ambient', "Ambient", lambda station, start_epoch_ms, end_epoch_ms: DataExport(
            start_epoch_ms, end_epoch_ms, 'unsplt', {
                Name(station, 'avgh', 'WS1_XM1'), Name(station, 'avgh', 'WD1_XM1'),
                Name(station, 'avgh', 'WS2_XM1'), Name(station, 'avgh', 'WD2_XM1'),
                Name(station, 'avgh', 'WS3_XM1'), Name(station, 'avgh', 'WD3_XM1'),
                Name(station, 'avgh', 'T1_XM1'), Name(station, 'avgh', 'U1_XM1'), Name(station, 'avgh', 'TD1_XM1'),
                Name(station, 'avgh', 'T2_XM1'), Name(station, 'avgh', 'U2_XM1'), Name(station, 'avgh', 'TD2_XM1'),
                Name(station, 'avgh', 'T3_XM1'), Name(station, 'avgh', 'U3_XM1'), Name(station, 'avgh', 'TD3_XM1'),
                Name(station, 'avgh', 'P_XM1'),
                Name(station, 'avgh', 'WI_XM1'),
            },
        )),
    ]),
}

profile_export: typing.Dict[str, typing.Dict[str, DataExportList]] = {
    'aerosol': aerosol_export,
    'ozone': ozone_export,
    'met': met_export,    
}


aerosol_data: typing.Dict[str, typing.Dict[str, typing.Callable[[str, int, int, typing.Callable], DataStream]]] = {
    'raw': {
        'contamination': lambda station, start_epoch_ms, end_epoch_ms, send: ContaminationReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'raw', 'F1_N71'),
                Name(station, 'raw', 'F1_N61'),
                Name(station, 'raw', 'F1_S11'),
                Name(station, 'raw', 'F1_A11'),
            }, send
        ),

        'cnc': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'raw', 'N_N71'): 'cnc',
                Name(station, 'raw', 'N_N61'): 'cnc',
            }, send
        ),
        
        'scattering-whole': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'raw', 'BsB_S11'): 'BsB',
                Name(station, 'raw', 'BsG_S11'): 'BsG',
                Name(station, 'raw', 'BsR_S11'): 'BsR',
                Name(station, 'raw', 'BbsB_S11'): 'BbsB',
                Name(station, 'raw', 'BbsG_S11'): 'BbsG',
                Name(station, 'raw', 'BbsR_S11'): 'BbsR',
            }, send
        ),
        'scattering-pm10': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'raw', 'BsB_S11', {'pm10'}): 'BsB',
                Name(station, 'raw', 'BsG_S11', {'pm10'}): 'BsG',
                Name(station, 'raw', 'BsR_S11', {'pm10'}): 'BsR',
                Name(station, 'raw', 'BbsB_S11', {'pm10'}): 'BbsB',
                Name(station, 'raw', 'BbsG_S11', {'pm10'}): 'BbsG',
                Name(station, 'raw', 'BbsR_S11', {'pm10'}): 'BbsR',
            }, send
        ),
        'scattering-pm25': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'raw', 'BsB_S11', {'pm25'}): 'BsB',
                Name(station, 'raw', 'BsG_S11', {'pm25'}): 'BsG',
                Name(station, 'raw', 'BsR_S11', {'pm25'}): 'BsR',
                Name(station, 'raw', 'BbsB_S11', {'pm25'}): 'BbsB',
                Name(station, 'raw', 'BbsG_S11', {'pm25'}): 'BbsG',
                Name(station, 'raw', 'BbsR_S11', {'pm25'}): 'BbsR',
            }, send
        ),
        'scattering-pm1': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'raw', 'BsB_S11', {'pm1'}): 'BsB',
                Name(station, 'raw', 'BsG_S11', {'pm1'}): 'BsG',
                Name(station, 'raw', 'BsR_S11', {'pm1'}): 'BsR',
                Name(station, 'raw', 'BbsB_S11', {'pm1'}): 'BbsB',
                Name(station, 'raw', 'BbsG_S11', {'pm1'}): 'BbsG',
                Name(station, 'raw', 'BbsR_S11', {'pm1'}): 'BbsR',
            }, send
        ),
        
        'absorption-whole': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'raw', 'BaB_A11'): 'BaB',
                Name(station, 'raw', 'BaG_A11'): 'BaG',
                Name(station, 'raw', 'BaR_A11'): 'BaR',
            }, send
        ),
        'absorption-pm10': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'raw', 'BaB_A11', {'pm10'}): 'BaB',
                Name(station, 'raw', 'BaG_A11', {'pm10'}): 'BaG',
                Name(station, 'raw', 'BaR_A11', {'pm10'}): 'BaR',
            }, send
        ),
        'absorption-pm25': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'raw', 'BaB_A11', {'pm25'}): 'BaB',
                Name(station, 'raw', 'BaG_A11', {'pm25'}): 'BaG',
                Name(station, 'raw', 'BaR_A11', {'pm25'}): 'BaR',
            }, send
        ),
        'absorption-pm1': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'raw', 'BaB_A11', {'pm1'}): 'BaB',
                Name(station, 'raw', 'BaG_A11', {'pm1'}): 'BaG',
                Name(station, 'raw', 'BaR_A11', {'pm1'}): 'BaR',
            }, send
        ),

        'aethalometer': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, dict(
                [(Name(station, 'raw', f'Ba{i+1}_A81'), f'Ba{i+1}') for i in range(7)] +
                [(Name(station, 'raw', f'X{i+1}_A81'), f'X{i+1}') for i in range(7)] +
                [(Name(station, 'raw', f'ZFACTOR{i+1}_A81'), f'CF{i+1}') for i in range(7)] +
                [(Name(station, 'raw', f'Ir{i+1}_A81'), f'Ir{i+1}') for i in range(7)]
            ), send
        ),

        'intensive-whole': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'raw', 'BsB_S11'): 'BsB',
                Name(station, 'raw', 'BsG_S11'): 'BsG',
                Name(station, 'raw', 'BsR_S11'): 'BsR',
                Name(station, 'raw', 'BbsB_S11'): 'BbsB',
                Name(station, 'raw', 'BbsG_S11'): 'BbsG',
                Name(station, 'raw', 'BbsR_S11'): 'BbsR',
                Name(station, 'raw', 'BaB_A11'): 'BaB',
                Name(station, 'raw', 'BaG_A11'): 'BaG',
                Name(station, 'raw', 'BaR_A11'): 'BaR',
            }, send
        ),
        'intensive-pm10': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'raw', 'BsB_S11', {'pm10'}): 'BsB',
                Name(station, 'raw', 'BsG_S11', {'pm10'}): 'BsG',
                Name(station, 'raw', 'BsR_S11', {'pm10'}): 'BsR',
                Name(station, 'raw', 'BbsB_S11', {'pm10'}): 'BbsB',
                Name(station, 'raw', 'BbsG_S11', {'pm10'}): 'BbsG',
                Name(station, 'raw', 'BbsR_S11', {'pm10'}): 'BbsR',
                Name(station, 'raw', 'BaB_A11', {'pm10'}): 'BaB',
                Name(station, 'raw', 'BaG_A11', {'pm10'}): 'BaG',
                Name(station, 'raw', 'BaR_A11', {'pm10'}): 'BaR',
            }, send
        ),
        'intensive-pm25': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'raw', 'BsB_S11', {'pm25'}): 'BsB',
                Name(station, 'raw', 'BsG_S11', {'pm25'}): 'BsG',
                Name(station, 'raw', 'BsR_S11', {'pm25'}): 'BsR',
                Name(station, 'raw', 'BbsB_S11', {'pm25'}): 'BbsB',
                Name(station, 'raw', 'BbsG_S11', {'pm25'}): 'BbsG',
                Name(station, 'raw', 'BbsR_S11', {'pm25'}): 'BbsR',
                Name(station, 'raw', 'BaB_A11', {'pm25'}): 'BaB',
                Name(station, 'raw', 'BaG_A11', {'pm25'}): 'BaG',
                Name(station, 'raw', 'BaR_A11', {'pm25'}): 'BaR',
            }, send
        ),
        'intensive-pm1': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'raw', 'BsB_S11', {'pm1'}): 'BsB',
                Name(station, 'raw', 'BsG_S11', {'pm1'}): 'BsG',
                Name(station, 'raw', 'BsR_S11', {'pm1'}): 'BsR',
                Name(station, 'raw', 'BbsB_S11', {'pm1'}): 'BbsB',
                Name(station, 'raw', 'BbsG_S11', {'pm1'}): 'BbsG',
                Name(station, 'raw', 'BbsR_S11', {'pm1'}): 'BbsR',
                Name(station, 'raw', 'BaB_A11', {'pm1'}): 'BaB',
                Name(station, 'raw', 'BaG_A11', {'pm1'}): 'BaG',
                Name(station, 'raw', 'BaR_A11', {'pm1'}): 'BaR',
            }, send
        ),

        'wind': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'raw', 'WS1_XM1'): 'WS',
                Name(station, 'raw', 'WD1_XM1'): 'WD',
            }, send
        ),
        'flow': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'raw', 'Q_Q11'): 'sample',
                Name(station, 'raw', 'Q_Q11', {'pm10'}): 'sample',
                Name(station, 'raw', 'Q_Q11', {'pm1'}): 'sample',
                Name(station, 'raw', 'Q_Q11', {'pm25'}): 'sample',
                Name(station, 'raw', 'Pd_P01'): 'pitot',
            }, send
        ),
        'temperature': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'raw', 'T_V51'): 'Tinlet', Name(station, 'raw', 'U_V51'): 'Uinlet',
                Name(station, 'raw', 'T_V01'): 'Taux', Name(station, 'raw', 'U_V01'): 'Uaux',
                Name(station, 'raw', 'T1_XM1'): 'Tambient',
                Name(station, 'raw', 'U1_XM1'): 'Uambient',
                Name(station, 'raw', 'TD1_XM1'): 'TDambient',

                Name(station, 'raw', 'T_V11'): 'Tsample', Name(station, 'raw', 'U_V11'): 'Usample',
                Name(station, 'raw', 'T_V11', {'pm10'}): 'Tsample', Name(station, 'raw', 'U_V11', {'pm10'}): 'Usample',
                Name(station, 'raw', 'T_V11', {'pm1'}): 'Tsample', Name(station, 'raw', 'U_V11', {'pm1'}): 'Usample',
                Name(station, 'raw', 'T_V11', {'pm25'}): 'Tsample', Name(station, 'raw', 'U_V11', {'pm25'}): 'Usample',

                Name(station, 'raw', 'Tu_S11'): 'Tnephinlet', Name(station, 'raw', 'Uu_S11'): 'Unephinlet',
                Name(station, 'raw', 'Tu_S11', {'pm10'}): 'Tnephinlet', Name(station, 'raw', 'Uu_S11', {'pm10'}): 'Unephinlet',
                Name(station, 'raw', 'Tu_S11', {'pm1'}): 'Tnephinlet', Name(station, 'raw', 'Uu_S11', {'pm1'}): 'Unephinlet',
                Name(station, 'raw', 'Tu_S11', {'pm25'}): 'Tnephinlet', Name(station, 'raw', 'Uu_S11', {'pm25'}): 'Unephinlet',

                Name(station, 'raw', 'T_S11'): 'Tneph', Name(station, 'raw', 'U_S11'): 'Uneph',
                Name(station, 'raw', 'T_S11', {'pm10'}): 'Tneph', Name(station, 'raw', 'U_S11', {'pm10'}): 'Uneph',
                Name(station, 'raw', 'T_S11', {'pm1'}): 'Tneph', Name(station, 'raw', 'U_S11', {'pm1'}): 'Uneph',
                Name(station, 'raw', 'T_S11', {'pm25'}): 'Tneph', Name(station, 'raw', 'U_S11', {'pm25'}): 'Uneph',
            }, send
        ),
        'pressure': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'raw', 'P_XM1'): 'ambient',
                Name(station, 'raw', 'Pd_P01'): 'pitot',
                Name(station, 'raw', 'Pd_P12'): 'vacuum',
                Name(station, 'raw', 'Pd_P12', {'pm10'}): 'vacuum',
                Name(station, 'raw', 'Pd_P12', {'pm1'}): 'vacuum',
                Name(station, 'raw', 'Pd_P12', {'pm25'}): 'vacuum',
            }, send
        ),
        'samplepressure-whole': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'raw', 'P_S11'): 'neph',
                Name(station, 'raw', 'Pd_P11'): 'impactor',
            }, send
        ),
        'samplepressure-pm10': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'raw', 'P_S11', {'pm10'}): 'neph',
                Name(station, 'raw', 'Pd_P11', {'pm10'}): 'impactor',
            }, send
        ),
        'samplepressure-pm25': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'raw', 'P_S11', {'pm25'}): 'neph',
                Name(station, 'raw', 'Pd_P11', {'pm25'}): 'impactor',
            }, send
        ),
        'samplepressure-pm1': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'raw', 'P_S11', {'pm1'}): 'neph',
                Name(station, 'raw', 'Pd_P11', {'pm1'}): 'impactor',
            }, send
        ),

        'nephzero': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'raw', 'BswB_S11'): 'BswB',
                Name(station, 'raw', 'BswG_S11'): 'BswG',
                Name(station, 'raw', 'BswR_S11'): 'BswR',
                Name(station, 'raw', 'BbswB_S11'): 'BbswB',
                Name(station, 'raw', 'BbswG_S11'): 'BbswG',
                Name(station, 'raw', 'BbswR_S11'): 'BbswR',
            }, send
        ),
        'nephstatus': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'raw', 'CfG_S11'): 'CfG',
                Name(station, 'raw', 'CfG_S11', {'pm10'}): 'CfG',
                Name(station, 'raw', 'CfG_S11', {'pm1'}): 'CfG',
                Name(station, 'raw', 'CfG_S11', {'pm25'}): 'CfG',
                Name(station, 'raw', 'Vl_S11'): 'Vl',
                Name(station, 'raw', 'Vl_S11', {'pm10'}): 'Vl',
                Name(station, 'raw', 'Vl_S11', {'pm1'}): 'Vl',
                Name(station, 'raw', 'Vl_S11', {'pm25'}): 'Vl',
                Name(station, 'raw', 'Al_S11'): 'Al',
                Name(station, 'raw', 'Al_S11', {'pm10'}): 'Al',
                Name(station, 'raw', 'Al_S11', {'pm1'}): 'Al',
                Name(station, 'raw', 'Al_S11', {'pm25'}): 'Al',
            }, send
        ),
        
        'clapstatus': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'raw', 'IrG_A11'): 'IrG',
                Name(station, 'raw', 'IrG_A11', {'pm10'}): 'IrG',
                Name(station, 'raw', 'IrG_A11', {'pm1'}): 'IrG',
                Name(station, 'raw', 'IrG_A11', {'pm25'}): 'IrG',
                Name(station, 'raw', 'IfG_A11'): 'IfG',
                Name(station, 'raw', 'IfG_A11', {'pm10'}): 'IfG',
                Name(station, 'raw', 'IfG_A11', {'pm1'}): 'IfG',
                Name(station, 'raw', 'IfG_A11', {'pm25'}): 'IfG',
                Name(station, 'raw', 'IpG_A11'): 'IpG',
                Name(station, 'raw', 'IpG_A11', {'pm10'}): 'IpG',
                Name(station, 'raw', 'IpG_A11', {'pm1'}): 'IpG',
                Name(station, 'raw', 'IpG_A11', {'pm25'}): 'IpG',
                Name(station, 'raw', 'Q_A11'): 'Q',
                Name(station, 'raw', 'Q_A11', {'pm10'}): 'Q',
                Name(station, 'raw', 'Q_A11', {'pm1'}): 'Q',
                Name(station, 'raw', 'Q_A11', {'pm25'}): 'Q',
                Name(station, 'raw', 'T1_A11'): 'Tsample',
                Name(station, 'raw', 'T1_A11', {'pm10'}): 'Tsample',
                Name(station, 'raw', 'T1_A11', {'pm1'}): 'Tsample',
                Name(station, 'raw', 'T1_A11', {'pm25'}): 'Tsample',
                Name(station, 'raw', 'T2_A11'): 'Tcase',
                Name(station, 'raw', 'T2_A11', {'pm10'}): 'Tcase',
                Name(station, 'raw', 'T2_A11', {'pm1'}): 'Tcase',
                Name(station, 'raw', 'T2_A11', {'pm25'}): 'Tcase',
                Name(station, 'raw', 'Fn_A11'): 'spot',
            }, send
        ),

        'aethalometerstatus': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'raw', 'T1_A81'): 'Tcontroller',
                Name(station, 'raw', 'T2_A81'): 'Tsupply',
                Name(station, 'raw', 'T3_A81'): 'Tled',
            }, send
        ),

        'cpcstatus': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'raw', 'Q_Q71'): 'Qsample',
                Name(station, 'raw', 'Q_Q61'): 'Qsample',
                Name(station, 'raw', 'Q_Q72'): 'Qdrier',
                Name(station, 'raw', 'Q_Q62'): 'Qdrier',
            }, send
        ),

        'umacstatus': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'raw', 'T_X1'): 'T',
                Name(station, 'raw', 'V_X1'): 'V',
            }, send
        ),
    },
    
    'clean': {
        'contamination': lambda station, start_epoch_ms, end_epoch_ms, send: ContaminationReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'clean', 'F1_N71'),
                Name(station, 'clean', 'F1_N61'),
                Name(station, 'clean', 'F1_S11'),
                Name(station, 'clean', 'F1_A11'),
            }, send
        ),

        'cnc': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'clean', 'N_N71'): 'cnc',
                Name(station, 'clean', 'N_N61'): 'cnc',
            }, send
        ),

        'scattering-whole': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'clean', 'BsB_S11'): 'BsB',
                Name(station, 'clean', 'BsG_S11'): 'BsG',
                Name(station, 'clean', 'BsR_S11'): 'BsR',
                Name(station, 'clean', 'BbsB_S11'): 'BbsB',
                Name(station, 'clean', 'BbsG_S11'): 'BbsG',
                Name(station, 'clean', 'BbsR_S11'): 'BbsR',
            }, send
        ),
        'scattering-pm10': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'clean', 'BsB_S11', {'pm10'}): 'BsB',
                Name(station, 'clean', 'BsG_S11', {'pm10'}): 'BsG',
                Name(station, 'clean', 'BsR_S11', {'pm10'}): 'BsR',
                Name(station, 'clean', 'BbsB_S11', {'pm10'}): 'BbsB',
                Name(station, 'clean', 'BbsG_S11', {'pm10'}): 'BbsG',
                Name(station, 'clean', 'BbsR_S11', {'pm10'}): 'BbsR',
            }, send
        ),
        'scattering-pm25': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'clean', 'BsB_S11', {'pm25'}): 'BsB',
                Name(station, 'clean', 'BsG_S11', {'pm25'}): 'BsG',
                Name(station, 'clean', 'BsR_S11', {'pm25'}): 'BsR',
                Name(station, 'clean', 'BbsB_S11', {'pm25'}): 'BbsB',
                Name(station, 'clean', 'BbsG_S11', {'pm25'}): 'BbsG',
                Name(station, 'clean', 'BbsR_S11', {'pm25'}): 'BbsR',
            }, send
        ),
        'scattering-pm1': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'clean', 'BsB_S11', {'pm1'}): 'BsB',
                Name(station, 'clean', 'BsG_S11', {'pm1'}): 'BsG',
                Name(station, 'clean', 'BsR_S11', {'pm1'}): 'BsR',
                Name(station, 'clean', 'BbsB_S11', {'pm1'}): 'BbsB',
                Name(station, 'clean', 'BbsG_S11', {'pm1'}): 'BbsG',
                Name(station, 'clean', 'BbsR_S11', {'pm1'}): 'BbsR',
            }, send
        ),

        'absorption-whole': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'clean', 'BaB_A11'): 'BaB',
                Name(station, 'clean', 'BaG_A11'): 'BaG',
                Name(station, 'clean', 'BaR_A11'): 'BaR',
            }, send
        ),
        'absorption-pm10': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'clean', 'BaB_A11', {'pm10'}): 'BaB',
                Name(station, 'clean', 'BaG_A11', {'pm10'}): 'BaG',
                Name(station, 'clean', 'BaR_A11', {'pm10'}): 'BaR',
            }, send
        ),
        'absorption-pm25': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'clean', 'BaB_A11', {'pm1'}): 'BaB',
                Name(station, 'clean', 'BaG_A11', {'pm1'}): 'BaG',
                Name(station, 'clean', 'BaR_A11', {'pm1'}): 'BaR',
            }, send
        ),
        'absorption-pm1': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'clean', 'BaB_A11', {'pm1'}): 'BaB',
                Name(station, 'clean', 'BaG_A11', {'pm1'}): 'BaG',
                Name(station, 'clean', 'BaR_A11', {'pm1'}): 'BaR',
            }, send
        ),

        'aethalometer': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, dict(
                [(Name(station, 'clean', f'Ba{i + 1}_A81'), f'Ba{i + 1}') for i in range(7)] +
                [(Name(station, 'clean', f'X{i + 1}_A81'), f'X{i + 1}') for i in range(7)] +
                [(Name(station, 'clean', f'ZFACTOR{i + 1}_A81'), f'CF{i + 1}') for i in range(7)] +
                [(Name(station, 'clean', f'Ir{i + 1}_A81'), f'Ir{i + 1}') for i in range(7)]
            ), send
        ),

        'intensive-whole': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'clean', 'BsB_S11'): 'BsB',
                Name(station, 'clean', 'BsG_S11'): 'BsG',
                Name(station, 'clean', 'BsR_S11'): 'BsR',
                Name(station, 'clean', 'BbsB_S11'): 'BbsB',
                Name(station, 'clean', 'BbsG_S11'): 'BbsG',
                Name(station, 'clean', 'BbsR_S11'): 'BbsR',
                Name(station, 'clean', 'BaB_A11'): 'BaB',
                Name(station, 'clean', 'BaG_A11'): 'BaG',
                Name(station, 'clean', 'BaR_A11'): 'BaR',
            }, send
        ),
        'intensive-pm10': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'clean', 'BsB_S11', {'pm10'}): 'BsB',
                Name(station, 'clean', 'BsG_S11', {'pm10'}): 'BsG',
                Name(station, 'clean', 'BsR_S11', {'pm10'}): 'BsR',
                Name(station, 'clean', 'BbsB_S11', {'pm10'}): 'BbsB',
                Name(station, 'clean', 'BbsG_S11', {'pm10'}): 'BbsG',
                Name(station, 'clean', 'BbsR_S11', {'pm10'}): 'BbsR',
                Name(station, 'clean', 'BaB_A11', {'pm10'}): 'BaB',
                Name(station, 'clean', 'BaG_A11', {'pm10'}): 'BaG',
                Name(station, 'clean', 'BaR_A11', {'pm10'}): 'BaR',
            }, send
        ),
        'intensive-pm25': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'clean', 'BsB_S11', {'pm25'}): 'BsB',
                Name(station, 'clean', 'BsG_S11', {'pm25'}): 'BsG',
                Name(station, 'clean', 'BsR_S11', {'pm25'}): 'BsR',
                Name(station, 'clean', 'BbsB_S11', {'pm25'}): 'BbsB',
                Name(station, 'clean', 'BbsG_S11', {'pm25'}): 'BbsG',
                Name(station, 'clean', 'BbsR_S11', {'pm25'}): 'BbsR',
                Name(station, 'clean', 'BaB_A11', {'pm25'}): 'BaB',
                Name(station, 'clean', 'BaG_A11', {'pm25'}): 'BaG',
                Name(station, 'clean', 'BaR_A11', {'pm25'}): 'BaR',
            }, send
        ),
        'intensive-pm1': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'clean', 'BsB_S11', {'pm1'}): 'BsB',
                Name(station, 'clean', 'BsG_S11', {'pm1'}): 'BsG',
                Name(station, 'clean', 'BsR_S11', {'pm1'}): 'BsR',
                Name(station, 'clean', 'BbsB_S11', {'pm1'}): 'BbsB',
                Name(station, 'clean', 'BbsG_S11', {'pm1'}): 'BbsG',
                Name(station, 'clean', 'BbsR_S11', {'pm1'}): 'BbsR',
                Name(station, 'clean', 'BaB_A11', {'pm1'}): 'BaB',
                Name(station, 'clean', 'BaG_A11', {'pm1'}): 'BaG',
                Name(station, 'clean', 'BaR_A11', {'pm1'}): 'BaR',
            }, send
        ),

        'wind': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'clean', 'WS1_XM1'): 'WS',
                Name(station, 'clean', 'WD1_XM1'): 'WD',
            }, send
        ),
    },
    
    'avgh': {
        'contamination': lambda station, start_epoch_ms, end_epoch_ms, send: ContaminationReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'avgh', 'F1_N71'),
                Name(station, 'avgh', 'F1_N61'),
                Name(station, 'avgh', 'F1_S11'),
                Name(station, 'avgh', 'F1_A11'),
            }, send
        ),

        'cnc': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'avgh', 'N_N71'): 'cnc',
                Name(station, 'avgh', 'N_N61'): 'cnc',
            }, send
        ),

        'scattering-whole': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'avgh', 'BsB_S11'): 'BsB',
                Name(station, 'avgh', 'BsG_S11'): 'BsG',
                Name(station, 'avgh', 'BsR_S11'): 'BsR',
                Name(station, 'avgh', 'BbsB_S11'): 'BbsB',
                Name(station, 'avgh', 'BbsG_S11'): 'BbsG',
                Name(station, 'avgh', 'BbsR_S11'): 'BbsR',
            }, send
        ),
        'scattering-pm10': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'avgh', 'BsB_S11', {'pm10'}): 'BsB',
                Name(station, 'avgh', 'BsG_S11', {'pm10'}): 'BsG',
                Name(station, 'avgh', 'BsR_S11', {'pm10'}): 'BsR',
                Name(station, 'avgh', 'BbsB_S11', {'pm10'}): 'BbsB',
                Name(station, 'avgh', 'BbsG_S11', {'pm10'}): 'BbsG',
                Name(station, 'avgh', 'BbsR_S11', {'pm10'}): 'BbsR',
            }, send
        ),
        'scattering-pm25': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'avgh', 'BsB_S11', {'pm25'}): 'BsB',
                Name(station, 'avgh', 'BsG_S11', {'pm25'}): 'BsG',
                Name(station, 'avgh', 'BsR_S11', {'pm25'}): 'BsR',
                Name(station, 'avgh', 'BbsB_S11', {'pm25'}): 'BbsB',
                Name(station, 'avgh', 'BbsG_S11', {'pm25'}): 'BbsG',
                Name(station, 'avgh', 'BbsR_S11', {'pm25'}): 'BbsR',
            }, send
        ),
        'scattering-pm1': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'avgh', 'BsB_S11', {'pm1'}): 'BsB',
                Name(station, 'avgh', 'BsG_S11', {'pm1'}): 'BsG',
                Name(station, 'avgh', 'BsR_S11', {'pm1'}): 'BsR',
                Name(station, 'avgh', 'BbsB_S11', {'pm1'}): 'BbsB',
                Name(station, 'avgh', 'BbsG_S11', {'pm1'}): 'BbsG',
                Name(station, 'avgh', 'BbsR_S11', {'pm1'}): 'BbsR',
            }, send
        ),

        'absorption-whole': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'avgh', 'BaB_A11'): 'BaB',
                Name(station, 'avgh', 'BaG_A11'): 'BaG',
                Name(station, 'avgh', 'BaR_A11'): 'BaR',
            }, send
        ),
        'absorption-pm10': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'avgh', 'BaB_A11', {'pm10'}): 'BaB',
                Name(station, 'avgh', 'BaG_A11', {'pm10'}): 'BaG',
                Name(station, 'avgh', 'BaR_A11', {'pm10'}): 'BaR',
            }, send
        ),
        'absorption-pm25': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'avgh', 'BaB_A11', {'pm25'}): 'BaB',
                Name(station, 'avgh', 'BaG_A11', {'pm25'}): 'BaG',
                Name(station, 'avgh', 'BaR_A11', {'pm25'}): 'BaR',
            }, send
        ),
        'absorption-pm1': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'avgh', 'BaB_A11', {'pm1'}): 'BaB',
                Name(station, 'avgh', 'BaG_A11', {'pm1'}): 'BaG',
                Name(station, 'avgh', 'BaR_A11', {'pm1'}): 'BaR',
            }, send
        ),

        'aethalometer': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, dict(
                [(Name(station, 'avgh', f'Ba{i + 1}_A81'), f'Ba{i + 1}') for i in range(7)] +
                [(Name(station, 'avgh', f'X{i + 1}_A81'), f'X{i + 1}') for i in range(7)] +
                [(Name(station, 'avgh', f'ZFACTOR{i + 1}_A81'), f'CF{i + 1}') for i in range(7)] +
                [(Name(station, 'avgh', f'Ir{i + 1}_A81'), f'Ir{i + 1}') for i in range(7)]
            ), send
        ),

        'intensive-whole': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'avgh', 'BsB_S11'): 'BsB',
                Name(station, 'avgh', 'BsG_S11'): 'BsG',
                Name(station, 'avgh', 'BsR_S11'): 'BsR',
                Name(station, 'avgh', 'BbsB_S11'): 'BbsB',
                Name(station, 'avgh', 'BbsG_S11'): 'BbsG',
                Name(station, 'avgh', 'BbsR_S11'): 'BbsR',
                Name(station, 'avgh', 'BaB_A11'): 'BaB',
                Name(station, 'avgh', 'BaG_A11'): 'BaG',
                Name(station, 'avgh', 'BaR_A11'): 'BaR',
            }, send
        ),
        'intensive-pm10': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'avgh', 'BsB_S11', {'pm10'}): 'BsB',
                Name(station, 'avgh', 'BsG_S11', {'pm10'}): 'BsG',
                Name(station, 'avgh', 'BsR_S11', {'pm10'}): 'BsR',
                Name(station, 'avgh', 'BbsB_S11', {'pm10'}): 'BbsB',
                Name(station, 'avgh', 'BbsG_S11', {'pm10'}): 'BbsG',
                Name(station, 'avgh', 'BbsR_S11', {'pm10'}): 'BbsR',
                Name(station, 'avgh', 'BaB_A11', {'pm10'}): 'BaB',
                Name(station, 'avgh', 'BaG_A11', {'pm10'}): 'BaG',
                Name(station, 'avgh', 'BaR_A11', {'pm10'}): 'BaR',
            }, send
        ),
        'intensive-pm25': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'avgh', 'BsB_S11', {'pm25'}): 'BsB',
                Name(station, 'avgh', 'BsG_S11', {'pm25'}): 'BsG',
                Name(station, 'avgh', 'BsR_S11', {'pm25'}): 'BsR',
                Name(station, 'avgh', 'BbsB_S11', {'pm25'}): 'BbsB',
                Name(station, 'avgh', 'BbsG_S11', {'pm25'}): 'BbsG',
                Name(station, 'avgh', 'BbsR_S11', {'pm25'}): 'BbsR',
                Name(station, 'avgh', 'BaB_A11', {'pm25'}): 'BaB',
                Name(station, 'avgh', 'BaG_A11', {'pm25'}): 'BaG',
                Name(station, 'avgh', 'BaR_A11', {'pm25'}): 'BaR',
            }, send
        ),
        'intensive-pm1': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'avgh', 'BsB_S11', {'pm1'}): 'BsB',
                Name(station, 'avgh', 'BsG_S11', {'pm1'}): 'BsG',
                Name(station, 'avgh', 'BsR_S11', {'pm1'}): 'BsR',
                Name(station, 'avgh', 'BbsB_S11', {'pm1'}): 'BbsB',
                Name(station, 'avgh', 'BbsG_S11', {'pm1'}): 'BbsG',
                Name(station, 'avgh', 'BbsR_S11', {'pm1'}): 'BbsR',
                Name(station, 'avgh', 'BaB_A11', {'pm1'}): 'BaB',
                Name(station, 'avgh', 'BaG_A11', {'pm1'}): 'BaG',
                Name(station, 'avgh', 'BaR_A11', {'pm1'}): 'BaR',
            }, send
        ),

        'wind': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'avgh', 'WS1_XM1'): 'WS',
                Name(station, 'avgh', 'WD1_XM1'): 'WD',
            }, send
        ),
    },

    'editing': {
        'contamination': lambda station, start_epoch_ms, end_epoch_ms, send: EditedContaminationReader(
            start_epoch_ms, end_epoch_ms, station, 'aerosol', {
                Name(station, 'clean', 'F1_N71'),
                Name(station, 'clean', 'F1_N61'),
                Name(station, 'clean', 'F1_S11'),
                Name(station, 'clean', 'F1_A11'),
            }, send
        ),

        'cnc': lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
            start_epoch_ms, end_epoch_ms, station, 'aerosol', {
                Name(station, 'clean', 'N_N71'): 'cnc',
                Name(station, 'clean', 'N_N61'): 'cnc',
            }, send
        ),

        'scattering-whole': lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
            start_epoch_ms, end_epoch_ms, station, 'aerosol', {
                Name(station, 'clean', 'BsB_S11'): 'BsB',
                Name(station, 'clean', 'BsG_S11'): 'BsG',
                Name(station, 'clean', 'BsR_S11'): 'BsR',
                Name(station, 'clean', 'BbsB_S11'): 'BbsB',
                Name(station, 'clean', 'BbsG_S11'): 'BbsG',
                Name(station, 'clean', 'BbsR_S11'): 'BbsR',
            }, send
        ),
        'scattering-pm10': lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
            start_epoch_ms, end_epoch_ms, station, 'aerosol', {
                Name(station, 'clean', 'BsB_S11', {'pm10'}): 'BsB',
                Name(station, 'clean', 'BsG_S11', {'pm10'}): 'BsG',
                Name(station, 'clean', 'BsR_S11', {'pm10'}): 'BsR',
                Name(station, 'clean', 'BbsB_S11', {'pm10'}): 'BbsB',
                Name(station, 'clean', 'BbsG_S11', {'pm10'}): 'BbsG',
                Name(station, 'clean', 'BbsR_S11', {'pm10'}): 'BbsR',
            }, send
        ),
        'scattering-pm25': lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
            start_epoch_ms, end_epoch_ms, station, 'aerosol', {
                Name(station, 'clean', 'BsB_S11', {'pm25'}): 'BsB',
                Name(station, 'clean', 'BsG_S11', {'pm25'}): 'BsG',
                Name(station, 'clean', 'BsR_S11', {'pm25'}): 'BsR',
                Name(station, 'clean', 'BbsB_S11', {'pm25'}): 'BbsB',
                Name(station, 'clean', 'BbsG_S11', {'pm25'}): 'BbsG',
                Name(station, 'clean', 'BbsR_S11', {'pm25'}): 'BbsR',
            }, send
        ),
        'scattering-pm1': lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
            start_epoch_ms, end_epoch_ms, station, 'aerosol', {
                Name(station, 'clean', 'BsB_S11', {'pm1'}): 'BsB',
                Name(station, 'clean', 'BsG_S11', {'pm1'}): 'BsG',
                Name(station, 'clean', 'BsR_S11', {'pm1'}): 'BsR',
                Name(station, 'clean', 'BbsB_S11', {'pm1'}): 'BbsB',
                Name(station, 'clean', 'BbsG_S11', {'pm1'}): 'BbsG',
                Name(station, 'clean', 'BbsR_S11', {'pm1'}): 'BbsR',
            }, send
        ),

        'absorption-whole': lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
            start_epoch_ms, end_epoch_ms, station, 'aerosol', {
                Name(station, 'clean', 'BaB_A11'): 'BaB',
                Name(station, 'clean', 'BaG_A11'): 'BaG',
                Name(station, 'clean', 'BaR_A11'): 'BaR',
            }, send
        ),
        'absorption-pm10': lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
            start_epoch_ms, end_epoch_ms, station, 'aerosol', {
                Name(station, 'clean', 'BaB_A11', {'pm10'}): 'BaB',
                Name(station, 'clean', 'BaG_A11', {'pm10'}): 'BaG',
                Name(station, 'clean', 'BaR_A11', {'pm10'}): 'BaR',
            }, send
        ),
        'absorption-pm25': lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
            start_epoch_ms, end_epoch_ms, station, 'aerosol', {
                Name(station, 'clean', 'BaB_A11', {'pm25'}): 'BaB',
                Name(station, 'clean', 'BaG_A11', {'pm25'}): 'BaG',
                Name(station, 'clean', 'BaR_A11', {'pm25'}): 'BaR',
            }, send
        ),
        'absorption-pm1': lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
            start_epoch_ms, end_epoch_ms, station, 'aerosol', {
                Name(station, 'clean', 'BaB_A11', {'pm1'}): 'BaB',
                Name(station, 'clean', 'BaG_A11', {'pm1'}): 'BaG',
                Name(station, 'clean', 'BaR_A11', {'pm1'}): 'BaR',
            }, send
        ),

        'aethalometer': lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
            start_epoch_ms, end_epoch_ms, station, 'aerosol', dict(
                [(Name(station, 'clean', f'Ba{i + 1}_A81'), f'Ba{i + 1}') for i in range(7)] +
                [(Name(station, 'clean', f'X{i + 1}_A81'), f'X{i + 1}') for i in range(7)] +
                [(Name(station, 'clean', f'ZFACTOR{i + 1}_A81'), f'CF{i + 1}') for i in range(7)] +
                [(Name(station, 'clean', f'Ir{i + 1}_A81'), f'Ir{i + 1}') for i in range(7)]
            ), send
        ),

        'intensive-whole': lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
            start_epoch_ms, end_epoch_ms, station, 'aerosol', {
                Name(station, 'clean', 'BsB_S11'): 'BsB',
                Name(station, 'clean', 'BsG_S11'): 'BsG',
                Name(station, 'clean', 'BsR_S11'): 'BsR',
                Name(station, 'clean', 'BbsB_S11'): 'BbsB',
                Name(station, 'clean', 'BbsG_S11'): 'BbsG',
                Name(station, 'clean', 'BbsR_S11'): 'BbsR',
                Name(station, 'clean', 'BaB_A11'): 'BaB',
                Name(station, 'clean', 'BaG_A11'): 'BaG',
                Name(station, 'clean', 'BaR_A11'): 'BaR',
            }, send
        ),
        'intensive-pm10': lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
            start_epoch_ms, end_epoch_ms, station, 'aerosol', {
                Name(station, 'clean', 'BsB_S11', {'pm10'}): 'BsB',
                Name(station, 'clean', 'BsG_S11', {'pm10'}): 'BsG',
                Name(station, 'clean', 'BsR_S11', {'pm10'}): 'BsR',
                Name(station, 'clean', 'BbsB_S11', {'pm10'}): 'BbsB',
                Name(station, 'clean', 'BbsG_S11', {'pm10'}): 'BbsG',
                Name(station, 'clean', 'BbsR_S11', {'pm10'}): 'BbsR',
                Name(station, 'clean', 'BaB_A11', {'pm10'}): 'BaB',
                Name(station, 'clean', 'BaG_A11', {'pm10'}): 'BaG',
                Name(station, 'clean', 'BaR_A11', {'pm10'}): 'BaR',
            }, send
        ),
        'intensive-pm25': lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
            start_epoch_ms, end_epoch_ms, station, 'aerosol', {
                Name(station, 'clean', 'BsB_S11', {'pm25'}): 'BsB',
                Name(station, 'clean', 'BsG_S11', {'pm25'}): 'BsG',
                Name(station, 'clean', 'BsR_S11', {'pm25'}): 'BsR',
                Name(station, 'clean', 'BbsB_S11', {'pm25'}): 'BbsB',
                Name(station, 'clean', 'BbsG_S11', {'pm25'}): 'BbsG',
                Name(station, 'clean', 'BbsR_S11', {'pm25'}): 'BbsR',
                Name(station, 'clean', 'BaB_A11', {'pm25'}): 'BaB',
                Name(station, 'clean', 'BaG_A11', {'pm25'}): 'BaG',
                Name(station, 'clean', 'BaR_A11', {'pm25'}): 'BaR',
            }, send
        ),
        'intensive-pm1': lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
            start_epoch_ms, end_epoch_ms, station, 'aerosol', {
                Name(station, 'clean', 'BsB_S11', {'pm1'}): 'BsB',
                Name(station, 'clean', 'BsG_S11', {'pm1'}): 'BsG',
                Name(station, 'clean', 'BsR_S11', {'pm1'}): 'BsR',
                Name(station, 'clean', 'BbsB_S11', {'pm1'}): 'BbsB',
                Name(station, 'clean', 'BbsG_S11', {'pm1'}): 'BbsG',
                Name(station, 'clean', 'BbsR_S11', {'pm1'}): 'BbsR',
                Name(station, 'clean', 'BaB_A11', {'pm1'}): 'BaB',
                Name(station, 'clean', 'BaG_A11', {'pm1'}): 'BaG',
                Name(station, 'clean', 'BaR_A11', {'pm1'}): 'BaR',
            }, send
        ),

        'wind': lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
            start_epoch_ms, end_epoch_ms, station, 'met', {
                Name(station, 'clean', 'WS1_XM1'): 'WS',
                Name(station, 'clean', 'WD1_XM1'): 'WD',
            }, send
        ),
    },
}

ozone_data: typing.Dict[str, typing.Dict[str, typing.Callable[[str, int, int, typing.Callable], DataStream]]] = {
    'raw': {
        'contamination': lambda station, start_epoch_ms, end_epoch_ms, send: ContaminationReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'raw', 'F1_G81'),
            }, send
        ),

        'ozone': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'raw', 'X_G81'): 'ozone',
            }, send
        ),

        'status': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'raw', 'T1_G81'): 'Tsample',
                Name(station, 'raw', 'T2_G81'): 'Tlamp',
                Name(station, 'raw', 'P_G81'): 'Psample',
                Name(station, 'raw', 'P1_G81'): 'Psample',
            }, send
        ),

        'cells': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'raw', 'Q1_G81'): 'Qa',
                Name(station, 'raw', 'Q2_G81'): 'Qb',
                Name(station, 'raw', 'C1_G81'): 'Ca',
                Name(station, 'raw', 'C2_G81'): 'Cb',
            }, send
        ),

        'wind': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'raw', 'WS1_XM1'): 'WS',
                Name(station, 'raw', 'WD1_XM1'): 'WD',
            }, send
        ),
    },
    
    'clean': {
        'contamination': lambda station, start_epoch_ms, end_epoch_ms, send: ContaminationReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'clean', 'F1_G81'),
            }, send
        ),

        'ozone': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'clean', 'X_G81'): 'ozone',
            }, send
        ),

        'wind': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'clean', 'WS1_XM1'): 'WS',
                Name(station, 'clean', 'WD1_XM1'): 'WD',
            }, send
        ),
    },
    
    'avgh': {
        'contamination': lambda station, start_epoch_ms, end_epoch_ms, send: ContaminationReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'avgh', 'F1_G81'),
            }, send
        ),

        'ozone': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'avgh', 'X_G81'): 'ozone',
            }, send
        ),

        'wind': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'avgh', 'WS1_XM1'): 'WS',
                Name(station, 'avgh', 'WD1_XM1'): 'WD',
            }, send
        ),
    },

    'editing': {
        'contamination': lambda station, start_epoch_ms, end_epoch_ms, send: EditedContaminationReader(
            start_epoch_ms, end_epoch_ms, station, 'ozone', {
                Name(station, 'clean', 'F1_G81'),
            }, send
        ),

        'ozone': lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
            start_epoch_ms, end_epoch_ms, station, 'ozone', {
                Name(station, 'clean', 'X_G81'): 'ozone',
            }, send
        ),

        'wind': lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
            start_epoch_ms, end_epoch_ms, station, 'met', {
                Name(station, 'clean', 'WS1_XM1'): 'WS',
                Name(station, 'clean', 'WD1_XM1'): 'WD',
            }, send
        ),
    },
}

met_data: typing.Dict[str, typing.Dict[str, typing.Callable[[str, int, int, typing.Callable], DataStream]]] = {
    'raw': {
        'wind': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'raw', 'WS1_XM1'): 'WSambient', Name(station, 'raw', 'WD1_XM1'): 'WDambient',
                Name(station, 'raw', 'WS2_XM1'): 'WS2', Name(station, 'raw', 'WD2_XM1'): 'WD2',
                Name(station, 'raw', 'WS3_XM1'): 'WS3', Name(station, 'raw', 'WD3_XM1'): 'WD3',
            }, send
        ),

        'temperature': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'raw', 'U1_XM1'): 'Uambient',
                Name(station, 'raw', 'T1_XM1'): 'Tambient',
                Name(station, 'raw', 'TD1_XM1'): 'TDambient',

                Name(station, 'raw', 'U2_XM1'): 'U2',
                Name(station, 'raw', 'T2_XM1'): 'T2',
                Name(station, 'raw', 'TD2_XM1'): 'TD2',

                Name(station, 'raw', 'U3_XM1'): 'U3',
                Name(station, 'raw', 'T3_XM1'): 'T3',
                Name(station, 'raw', 'TD3_XM1'): 'TD3',
            }, send
        ),

        'pressure': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'raw', 'P_XM1'): 'ambient',
            }, send
        ),

        'precipitation': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'raw', 'WI_XM1'): 'precipitation',
            }, send
        ),

        'tower': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'raw', 'T2_XM1'): 'Tmiddle',
                Name(station, 'raw', 'T3_XM1'): 'Ttop',
            }, send
        ),
    },
    
    'clean': {
        'wind': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'clean', 'WS1_XM1'): 'WSambient', Name(station, 'clean', 'WD1_XM1'): 'WDambient',
                Name(station, 'clean', 'WS2_XM1'): 'WS2', Name(station, 'clean', 'WD2_XM1'): 'WD2',
                Name(station, 'clean', 'WS3_XM1'): 'WS3', Name(station, 'clean', 'WD3_XM1'): 'WD3',
            }, send
        ),

        'temperature': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'clean', 'U1_XM1'): 'Uambient',
                Name(station, 'clean', 'T1_XM1'): 'Tambient',
                Name(station, 'clean', 'TD1_XM1'): 'TDambient',

                Name(station, 'clean', 'U2_XM1'): 'U2',
                Name(station, 'clean', 'T2_XM1'): 'T2',
                Name(station, 'clean', 'TD2_XM1'): 'TD2',

                Name(station, 'clean', 'U3_XM1'): 'U3',
                Name(station, 'clean', 'T3_XM1'): 'T3',
                Name(station, 'clean', 'TD3_XM1'): 'TD3',
            }, send
        ),

        'pressure': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'clean', 'P_XM1'): 'ambient',
            }, send
        ),

        'precipitation': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'clean', 'WI_XM1'): 'precipitation',
            }, send
        ),
    },
    
    'avgh': {
        'wind': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'avgh', 'WS1_XM1'): 'WSambient', Name(station, 'avgh', 'WD1_XM1'): 'WDambient',
                Name(station, 'avgh', 'WS2_XM1'): 'WS2', Name(station, 'avgh', 'WD2_XM1'): 'WD2',
                Name(station, 'avgh', 'WS3_XM1'): 'WS3', Name(station, 'avgh', 'WD3_XM1'): 'WD3',
            }, send
        ),

        'temperature': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'avgh', 'U1_XM1'): 'Uambient',
                Name(station, 'avgh', 'T1_XM1'): 'Tambient',
                Name(station, 'avgh', 'TD1_XM1'): 'TDambient',

                Name(station, 'avgh', 'U2_XM1'): 'U2',
                Name(station, 'avgh', 'T2_XM1'): 'T2',
                Name(station, 'avgh', 'TD2_XM1'): 'TD2',

                Name(station, 'avgh', 'U3_XM1'): 'U3',
                Name(station, 'avgh', 'T3_XM1'): 'T3',
                Name(station, 'avgh', 'TD3_XM1'): 'TD3',
            }, send
        ),

        'pressure': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'avgh', 'P_XM1'): 'ambient',
            }, send
        ),

        'precipitation': lambda station, start_epoch_ms, end_epoch_ms, send: DataReader(
            start_epoch_ms, end_epoch_ms, {
                Name(station, 'avgh', 'WI_XM1'): 'precipitation',
            }, send
        ),
    },
    
    'editing': {
        'wind': lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
            start_epoch_ms, end_epoch_ms, station, 'met', {
                Name(station, 'clean', 'WS1_XM1'): 'WSambient', Name(station, 'clean', 'WD1_XM1'): 'WDambient',
                Name(station, 'clean', 'WS2_XM1'): 'WS2', Name(station, 'clean', 'WD2_XM1'): 'WD2',
                Name(station, 'clean', 'WS3_XM1'): 'WS3', Name(station, 'clean', 'WD3_XM1'): 'WD3',
            }, send
        ),

        'temperature': lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
            start_epoch_ms, end_epoch_ms, station, 'met', {
                Name(station, 'clean', 'U1_XM1'): 'Uambient',
                Name(station, 'clean', 'T1_XM1'): 'Tambient',
                Name(station, 'clean', 'TD1_XM1'): 'TDambient',

                Name(station, 'clean', 'U2_XM1'): 'U2',
                Name(station, 'clean', 'T2_XM1'): 'T2',
                Name(station, 'clean', 'TD2_XM1'): 'TD2',

                Name(station, 'clean', 'U3_XM1'): 'U3',
                Name(station, 'clean', 'T3_XM1'): 'T3',
                Name(station, 'clean', 'TD3_XM1'): 'TD3',
            }, send
        ),

        'pressure': lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
            start_epoch_ms, end_epoch_ms, station, 'met', {
                Name(station, 'clean', 'P_XM1'): 'ambient',
            }, send
        ),

        'precipitation': lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
            start_epoch_ms, end_epoch_ms, station, 'met', {
                Name(station, 'clean', 'WI_XM1'): 'precipitation',
            }, send
        ),

        'tower': lambda station, start_epoch_ms, end_epoch_ms, send: EditedReader(
            start_epoch_ms, end_epoch_ms, station, 'met', {
                Name(station, 'clean', 'T2_XM1'): 'Tmiddle',
                Name(station, 'clean', 'T3_XM1'): 'Ttop',
            }, send
        ),
    },
}

profile_data: typing.Dict[str, typing.Dict[str, typing.Dict[str, typing.Callable[[str, int, int, typing.Callable], DataStream]]]] = {
    'aerosol': aerosol_data,
    'ozone': ozone_data,
    'met': met_data,
}

