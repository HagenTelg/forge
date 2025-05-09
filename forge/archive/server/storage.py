import typing
import logging
import shutil
import fcntl
import struct
import random
import stat
import os
from bisect import bisect_left, bisect_right
from pathlib import Path
from forge.archive import CONFIGURATION

_LOGGER = logging.getLogger(__name__)


class _RedirectedWrittenFile:
    def __init__(self, storage: Path):
        self.storage = storage

    def open(self) -> typing.BinaryIO:
        return self.storage.open('rb')

    def release(self) -> None:
        try:
            self.storage.unlink()
        except IOError:
            pass


class _RedirectedCreatedFile:
    def open(self) -> typing.BinaryIO:
        raise FileNotFoundError

    def release(self) -> None:
        pass


class _Redirection:
    def __init__(self, generation: int, root: Path):
        self.generation = generation
        self.root = root
        self.contents: typing.Dict[str, typing.Union[_RedirectedWrittenFile, _RedirectedCreatedFile]] = dict()

    def read(self, name: str) -> typing.Optional[typing.BinaryIO]:
        c = self.contents.get(name)
        if c:
            _LOGGER.debug("Using file %s from redirection generation %d", name, self.generation)
            return c.open()
        return None

    def release(self) -> None:
        _LOGGER.debug("Releasing redirections for generation %d", self.generation)
        try:
            for c in self.contents.values():
                c.release()
            self.root.rmdir()
        except:
            _LOGGER.error("Backend redirection release failed on generation %d", self.generation, exc_info=True)
            exit(1)


def _remove_empty_directories(root: Path, destination: Path) -> None:
    for p in destination.parents:
        if p == root:
            break
        try:
            p.rmdir()
        except:
            break


class _ActionWriteFile:
    def __init__(self, contents: Path):
        self.contents = contents

    def read(self) -> typing.BinaryIO:
        return self.contents.open('rb')

    def discard(self) -> None:
        self.contents.unlink(missing_ok=True)

    @property
    def removable(self) -> bool:
        return True

    def journal(self, destination: typing.BinaryIO) -> None:
        raw = str(self.contents.name).encode('utf-8')
        destination.write(struct.pack('<H', len(raw)))
        destination.write(raw)

    def apply(self, root: Path, destination: str,
              redirection: typing.Optional[Path] = None) -> typing.Optional[typing.Union[_RedirectedWrittenFile, _RedirectedCreatedFile]]:
        destination = root / destination
        try:
            try:
                created = not destination.exists()
                if redirection and not created:
                    redirection = redirection / self.contents.name
                    destination.rename(redirection)
                else:
                    destination.unlink(missing_ok=True)
                destination.parent.mkdir(parents=True, exist_ok=True)
                self.contents.rename(destination)
            except:
                _LOGGER.error("Write file action apply failed for %s (%s)", destination, redirection,
                              exc_info=True)
                os._exit(1)
            if redirection:
                if created:
                    return _RedirectedCreatedFile()
                else:
                    return _RedirectedWrittenFile(redirection)
        except:
            _remove_empty_directories(root, destination)
            raise


class _ActionRemoveFile:
    def read(self) -> None:
        return None

    def discard(self) -> None:
        pass

    @property
    def removable(self) -> bool:
        return False

    def journal(self, destination: typing.BinaryIO) -> None:
        destination.write(struct.pack('<H', 0))

    def apply(self, root: Path, destination: str,
              redirection: typing.Optional[Path] = None) -> typing.Optional[_RedirectedWrittenFile]:
        destination = root / destination
        try:
            if redirection:
                target_name = destination.name
                check_rename = redirection / target_name
                while check_rename.exists():
                    check_rename = redirection / (target_name + "_" + str(random.getrandbits(32)))
                redirection = check_rename
                destination.rename(redirection)
            else:
                destination.unlink(missing_ok=True)
            if redirection:
                return _RedirectedWrittenFile(redirection)
        except:
            _LOGGER.error("Remove file action apply failed for %s (%s)", destination, redirection,
                          exc_info=True)
            os._exit(1)
        finally:
            _remove_empty_directories(root, destination)


class Storage:
    _STORAGE_VERSION = 1
    _REDIRECTION_PREFIX = ".redirection_"
    _TRANSACTION_PREFIX = ".transaction_"

    def __init__(self, root_directory: Path = None):
        if not root_directory:
            root_directory = Path(CONFIGURATION.get('ARCHIVE.STORAGE_DIRECTORY', '/var/lib/forge/archive'))
        self._root = root_directory
        if not self._root.is_dir():
            raise RuntimeError(f"Archive storage {self._root} is not a directory")

        self._version_file: typing.Optional[typing.TextIO] = None

        self._generation: int = 1

        self._redirection_generation: typing.List[int] = list()
        self._redirections: typing.List[_Redirection] = list()

        self._refcount_generation: typing.List[int] = list()
        self._refcount: typing.List[typing.Set] = list()

        self._pending_changes: typing.Set[str] = set()

    @staticmethod
    def normalize_filename(name: str, allow_toplevel: bool = False) -> str:
        if not name:
            raise ValueError
        name = Path(name)
        if name.anchor:
            raise ValueError(f"File {name} must be relative")
        has_parents = False
        for check in name.parents:
            if not check.name:
                break
            if check.name.startswith('.'):
                raise ValueError(f"File {name} cannot contain components starting with .")
            has_parents = True
        else:
            raise ValueError
        if not has_parents and not allow_toplevel:
            raise ValueError
        return str(name)

    def _replay_transaction(self, transaction_root: Path) -> None:
        def read_string(journal: typing.BinaryIO) -> typing.Optional[str]:
            raw_length = journal.read(2)
            if not raw_length:
                raise EOFError
            raw_length = struct.unpack('<H', raw_length)[0]
            if not raw_length:
                return None
            raw = journal.read(raw_length)
            return raw.decode('utf-8')

        with (transaction_root / ".journal").open('rb') as journal:
            while True:
                try:
                    destination = read_string(journal)
                except EOFError:
                    break
                destination = self._root / destination
                source = read_string(journal)
                if not source:
                    try:
                        destination.unlink()
                        _LOGGER.debug("Replayed remove of %s", destination)
                    except FileNotFoundError:
                        _LOGGER.debug("Ignored stale remove of %s", destination)
                    _remove_empty_directories(self._root, destination)
                else:
                    source = transaction_root / source
                    if not source.exists():
                        _LOGGER.debug("Ignored stale rename of %s to %s", source, destination)
                    else:
                        _LOGGER.debug("Replaying move of %s to %s", source, destination)
                        destination.parent.mkdir(parents=True, exist_ok=True)
                        destination.unlink(missing_ok=True)
                        source.rename(destination)

    def initialize(self) -> None:
        lock_archive = bool(CONFIGURATION.get('ARCHIVE.LOCK_STORAGE', True))
        try:
            self._version_file = (self._root / ".version").open('rt')
            if lock_archive:
                try:
                    fcntl.flock(self._version_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except OSError:
                    _LOGGER.error("Archive lock failed, is another copy running?", exc_info=True)
                    raise
            version = int(self._version_file.read())
            self._version_file.seek(0)
            if version != self._STORAGE_VERSION:
                _LOGGER.debug("Archive storage version does not match")
                raise ValueError("Archive storage version does not match")
        except FileNotFoundError:
            _LOGGER.debug("Initializing archive storage")
            self._version_file = (self._root / '.version').open('wt')
            if lock_archive:
                try:
                    fcntl.flock(self._version_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except OSError:
                    _LOGGER.error("Archive lock failed, is another copy running?", exc_info=True)
                    raise
            self._version_file.write(str(self._STORAGE_VERSION))
            self._version_file.seek(0)
        if not lock_archive:
            self._version_file.close()
            self._version_file = None

        # No guarantees about commit order, so don't worry about replay order
        for check in self._root.iterdir():
            if check.name.startswith(self._REDIRECTION_PREFIX):
                _LOGGER.info("Removing stale redirection %s", check.name)
                shutil.rmtree(check)
            elif check.name.startswith(self._TRANSACTION_PREFIX):
                journal = check / ".journal"
                if not journal.exists():
                    _LOGGER.info("Removing stale transaction %s", check.name)
                else:
                    _LOGGER.info("Replaying transaction %s", check.name)
                    self._replay_transaction(check)
                shutil.rmtree(check)

    def shutdown(self) -> None:
        self._release_redirections(None)

        if self._version_file:
            self._version_file.close()
            self._version_file = None

    def __enter__(self) -> "Storage":
        self.initialize()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.shutdown()

    def reference_generation(self, generation: int, key: "typing.Hashable") -> None:
        assert generation <= self._generation

        target = bisect_left(self._refcount_generation, generation)
        if target >= len(self._refcount_generation):
            self._refcount_generation.append(generation)
            self._refcount.append({key})
            return

        self._refcount[target].add(key)

    def release_generation(self, generation: int, key: "typing.Hashable") -> None:
        target = bisect_left(self._refcount_generation, generation)
        assert target < len(self._refcount_generation)

        refs = self._refcount[target]
        refs.remove(key)
        if target != 0:
            return

        for i in range(len(self._refcount)):
            if len(self._refcount[i]) == 0:
                continue

            if i != 0:
                gen = self._refcount_generation[i]
                del self._refcount_generation[:i]
                del self._refcount[:i]
                self._release_redirections(gen)

            break
        else:
            self._refcount_generation.clear()
            self._refcount.clear()
            self._release_redirections(None)

    class ReadHandle:
        def __init__(self, storage: "Storage"):
            self.storage = storage
            self.generation = storage._generation
            self._was_released = False

        def release(self) -> None:
            self.storage.release_generation(self.generation, self)
            self._was_released = True

        def __enter__(self) -> "Storage.ReadHandle":
            return self

        def __exit__(self, exc_type, exc_val, exc_tb) -> None:
            self.release()

        def read_file(self, name: str) -> typing.Optional[typing.BinaryIO]:
            return self.storage.read_file(name, self.generation)

        def __del__(self):
            if not self._was_released:
                _LOGGER.error("Leaked handle for generation %d", self.generation)
                self._was_released = True

    def begin_read(self) -> ReadHandle:
        h = self.ReadHandle(self)
        self.reference_generation(h.generation, h)
        return h

    class WriteHandle(ReadHandle):
        def __init__(self, storage: "Storage"):
            super().__init__(storage)
            self._transaction_root = self.storage._root / (self.storage._TRANSACTION_PREFIX + str(self.generation))
            try:
                self._transaction_root.mkdir()
            except:
                _LOGGER.error("Unable to create transaction (%d) write directory", self.generation, exc_info=True)
                exit(1)
            self._actions: typing.Dict[str, typing.Union[_ActionWriteFile, _ActionRemoveFile]] = dict()

        def commit(self, progress: typing.Optional[typing.Callable[[int, int], None]] = None) -> None:
            super().release()
            self.storage._commit(self.generation + 1, self._transaction_root, self._actions, progress)

        def abort(self) -> None:
            super().release()
            for k in self._actions.keys():
                self.storage._pending_changes.discard(k)
            _LOGGER.debug("Removing aborted transaction (%d) storage", self.generation)
            shutil.rmtree(self._transaction_root)

        def __enter__(self) -> "Storage.WriteHandle":
            return self

        def __exit__(self, exc_type, exc_val, exc_tb) -> None:
            if exc_type is not None:
                self.abort()
            else:
                self.commit()

        def read_file(self, name: str) -> typing.Optional[typing.BinaryIO]:
            changed = self._actions.get(name)
            if changed:
                return changed.read()
            return super().read_file(name)

        def write_file(self, name: str) -> typing.BinaryIO:
            name = self.storage.normalize_filename(name)

            changed = self._actions.pop(name, None)
            if changed:
                _LOGGER.debug("Replacing transaction (%d) write file %s", self.generation, name)
                changed.discard()
            elif name in self.storage._pending_changes:
                raise FileExistsError(f"Duplicate transaction write to {name}: probable locking failure")
            if (self.storage._root / name).is_dir():
                raise IsADirectoryError(f"Directory exists as {name}")

            target_name = Path(name).name
            destination = self._transaction_root / target_name
            while destination.exists():
                destination = self._transaction_root / (target_name + "_" + str(random.getrandbits(32)))

            self.storage._pending_changes.add(name)
            self._actions[name] = _ActionWriteFile(destination)
            try:
                return destination.open('wb')
            except OSError:
                _LOGGER.error("Unable to write file (%s) in transaction (%d)", name, self.generation, exc_info=True)
                exit(1)

        def remove_file(self, name: str) -> bool:
            name = self.storage.normalize_filename(name)

            changed = self._actions.pop(name, None)
            removed = True
            if changed:
                _LOGGER.debug("Removing transaction (%d) write file %s", self.generation, name)
                removed = changed.removable
                changed.discard()
            elif name in self.storage._pending_changes:
                raise FileExistsError(f"Duplicate transaction write to {name}: probable locking failure")
            elif not self.storage.check_file_exists(name, self.generation):
                return False
            if (self.storage._root / name).is_dir():
                raise IsADirectoryError(f"Directory exists as {name}")

            self.storage._pending_changes.add(name)
            self._actions[name] = _ActionRemoveFile()
            return removed

    def begin_write(self) -> "Storage.WriteHandle":
        h = self.WriteHandle(self)
        self.reference_generation(h.generation, h)
        self._generation += 1
        return h

    def _release_redirections(self, before_generation: typing.Optional[int]) -> None:
        if before_generation is None:
            _LOGGER.debug("Releasing all redirections")
            for r in self._redirections:
                r.release()
            self._redirection_generation.clear()
            self._redirections.clear()
            return

        retain_index = bisect_left(self._redirection_generation, before_generation)
        _LOGGER.debug("Releasing redirections before generation %d at position %d", before_generation, retain_index)
        for r in self._redirections[:retain_index]:
            r.release()
        del self._redirection_generation[:retain_index]
        del self._redirections[:retain_index]

    def read_file(self, name: str, generation: int) -> typing.Optional[typing.BinaryIO]:
        try:
            name = self.normalize_filename(name)
        except ValueError:
            return None

        consider_index = bisect_right(self._redirection_generation, generation)
        for redirection in self._redirections[consider_index:]:
            try:
                r = redirection.read(name)
            except (FileNotFoundError, IsADirectoryError):
                return None
            if r:
                return r

        source = self._root / name
        try:
            return source.open('rb')
        except (FileNotFoundError, IsADirectoryError):
            return None

    def check_file_exists(self, name: str, generation: int) -> bool:
        try:
            name = self.normalize_filename(name)
        except ValueError:
            return False

        consider_index = bisect_right(self._redirection_generation, generation)
        for redirection in self._redirections[consider_index:]:
            if not name not in redirection.contents:
                continue
            return True
        return (self._root / name).exists()

    @staticmethod
    def _write_journal(journal_file: Path,
                       actions: typing.Dict[str, typing.Union[_ActionWriteFile, _ActionRemoveFile]]) -> None:
        try:
            with journal_file.open('wb') as journal:
                for name, act in actions.items():
                    raw = name.encode('utf-8')
                    journal.write(struct.pack('<H', len(raw)))
                    journal.write(raw)
                    act.journal(journal)
                journal.flush()
                try:
                    os.fdatasync(journal.fileno())
                except AttributeError:
                    os.fsync(journal.fileno())
        except:
            _LOGGER.error("Journal write failed", exc_info=True)
            exit(1)

    def _commit(self, generation: int, transaction_root: Path,
                actions: typing.Dict[str, typing.Union[_ActionWriteFile, _ActionRemoveFile]],
                progress: typing.Optional[typing.Callable[[int, int], None]] = None) -> None:
        _LOGGER.debug("Committing %d changes at generation %d", len(actions), generation)

        # First, make the journal
        journal_file = transaction_root / ".journal"
        self._write_journal(journal_file, actions)

        # Insert a redirection, if needed
        if len(self._refcount_generation) > 0 and self._refcount_generation[0] <= generation:
            redirection_root = self._root / (self._REDIRECTION_PREFIX + str(generation))
            try:
                redirection_root.mkdir()
            except:
                _LOGGER.error("Unable to create redirection directory on generation (%d)", generation, exc_info=True)
                exit(1)
            redirection = _Redirection(generation, redirection_root)
            index = bisect_right(self._redirection_generation, generation)
            self._redirection_generation.insert(index, generation)
            self._redirections.insert(index, redirection)
        else:
            redirection = None

        # Now apply changes
        completed_actions = 0
        for name, act in actions.items():
            if redirection:
                redirection.contents[name] = act.apply(self._root, name, redirection.root)
            else:
                act.apply(self._root, name)

            # Release pending flag
            self._pending_changes.discard(name)

            completed_actions += 1
            if progress is not None:
                progress(completed_actions, len(actions))

        # Changes applied, remove the journal
        journal_file.unlink()

        # Remove storage directory
        shutil.rmtree(transaction_root)

        _LOGGER.debug("Transaction commit completed at generation %d", generation)

    def list_files(self, path: str, modified_after: float = 0) -> typing.List[str]:
        _LOGGER.debug("Listing files at %s modified after time %.0f", path, modified_after)
        path = self._root / self.normalize_filename(path, allow_toplevel=True)

        result: typing.List[str] = list()

        def recurse(path: Path):
            descend: typing.Set[Path] = set()
            try:
                for file in os.scandir(path):
                    if file.name.startswith('.'):
                        continue
                    try:
                        if file.is_dir(follow_symlinks=False):
                            descend.add(Path(file.path))
                            continue
                        st = file.stat(follow_symlinks=False)
                    except FileNotFoundError:
                        continue
                    if not stat.S_ISREG(st.st_mode):
                        continue
                    if st.st_mtime <= modified_after:
                        continue
                    result.append(str(Path(file.path).relative_to(self._root)))
            except (FileNotFoundError, IsADirectoryError, NotADirectoryError):
                pass
            for d in descend:
                recurse(d)

        recurse(path)
        return result
