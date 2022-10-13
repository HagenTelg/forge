import typing
import asyncio
import logging
from pathlib import Path
from json import load as load_json, dump as save_json, JSONDecodeError
from .base import BasePersistentInterface
from forge.const import __version__ as forge_version

_LOGGER = logging.getLogger(__name__)


class PersistentInterface(BasePersistentInterface):
    def __init__(self, storage_file: Path):
        self.storage_file = storage_file

        self._instrument_data: typing.Dict[str, typing.Dict[str, typing.Any]] = dict()
        self._loaded: bool = False

    def _load_contents(self) -> None:
        if self._loaded:
            return
        self._loaded = True

        try:
            with self.storage_file.open('rt') as f:
                contents = load_json(f)
        except FileNotFoundError:
            _LOGGER.debug(f"State file {self.storage_file} not found")
            return
        except JSONDecodeError:
            _LOGGER.warning(f"State file {self.storage_file} corrupted", exc_info=True)
            return

        file_version = contents.get('version')
        if file_version != forge_version:
            _LOGGER.info(f"Version mismatch in state file {self.storage_file} ({file_version} vs {forge_version})")
            return

        instrument_data = contents.get('state')
        if not instrument_data or not isinstance(instrument_data, dict):
            _LOGGER.warning(f"State file {self.storage_file} does not contain state data")
            return

        _LOGGER.debug(f"Loaded state file {self.storage_file}")
        self._instrument_data = instrument_data

    def load(self, name: str) -> typing.Tuple[typing.Any, typing.Optional[float]]:
        self._load_contents()
        data = self._instrument_data.get(name)
        if not data or not isinstance(data, dict):
            return None, None
        value = data.get('value')
        if value is None:
            return None, None
        effective_time = data.get('time_ms')
        if effective_time is not None:
            effective_time /= 1000.0
        return value, effective_time

    async def save(self, name: str, value: typing.Any, effective_time: typing.Optional[float]) -> None:
        self._load_contents()
        state_contents = {
            'value': value
        }
        if effective_time is not None:
            state_contents['time_ms'] = round(effective_time * 1000.0)
        self._instrument_data[name] = state_contents

        with self.storage_file.open('wt') as f:
            save_json({
                'version': forge_version,
                'state': self._instrument_data,
            }, f)

