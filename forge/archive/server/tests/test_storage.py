import pytest
import typing
import time
from forge.archive import CONFIGURATION
from forge.archive.server.storage import Storage


CONFIGURATION.set('ARCHIVE.LOCK_STORAGE', False)


@pytest.fixture
def storage(tmp_path):
    dest = tmp_path / "storage"
    dest.mkdir(exist_ok=True)
    storage = Storage(dest)
    storage.initialize()
    return storage


def test_initialize(tmp_path, storage):
    storage.initialize()
    assert (tmp_path / "storage" / ".version").exists()
    storage.shutdown()


def test_read(tmp_path, storage):
    (tmp_path / "storage" / "test").mkdir()
    with (tmp_path / "storage" / "test" / "file").open("wb") as f:
        f.write(b"TestData")
    storage.initialize()
    with storage.read_file("test/file", 1) as f:
        assert f.read() == b"TestData"
    storage.shutdown()


def test_generation_reference(storage):
    key1 = "test_ref1"
    key2 = "test_ref2"
    with storage:
        storage.reference_generation(1, key1)
        storage.reference_generation(1, key2)
        storage.release_generation(1, key1)
        storage.release_generation(1, key2)


def test_read_write(storage):
    with storage:
        with storage.begin_write() as w:
            with w.write_file("test/file") as f:
                f.write(b"TestData")
            with w.write_file("test/rem") as f:
                f.write(b"TestRemove")
            with w.read_file("test/file") as f:
                assert f.read() == b"TestData"

        with storage.begin_read() as r:
            with r.read_file("test/file") as f:
                assert f.read() == b"TestData"
            with r.read_file("test/rem") as f:
                assert f.read() == b"TestRemove"

        with storage.begin_write() as w:
            with w.write_file("test/file") as f:
                f.write(b"TestData2")
            with w.write_file("test/file2") as f:
                f.write(b"TestData3")
            assert w.remove_file("test/rem")
            with w.read_file("test/file") as f:
                assert f.read() == b"TestData2"
            assert w.read_file("test/rem") is None
            assert not w.remove_file("test/rem")
            assert not w.remove_file("test/never_existed")

        with storage.begin_read() as r:
            with r.read_file("test/file") as f:
                assert f.read() == b"TestData2"
            with r.read_file("test/file2") as f:
                assert f.read() == b"TestData3"
            assert r.read_file("test/rem") is None

        w = storage.begin_write()
        with w.write_file("test/file") as f:
            f.write(b"Aborted")
        w.abort()
        with storage.begin_read() as r:
            with r.read_file("test/file") as f:
                assert f.read() == b"TestData2"


def test_cleanup_directory(tmp_path, storage):
    with storage:
        with storage.begin_write() as w:
            with w.write_file("test/sub/file") as f:
                f.write(b"TestData")

        assert (tmp_path / "storage" / "test" / "sub").is_dir()

        with storage.begin_write() as w:
            assert w.remove_file("test/sub/file")

        assert not (tmp_path / "storage" / "test" / "sub").exists()
        assert not (tmp_path / "storage" / "test").exists()
        assert (tmp_path / "storage").exists()


def test_read_old_generation(tmp_path, storage):
    with storage:
        with storage.begin_write() as w:
            with w.write_file("test/file") as f:
                f.write(b"FirstGen1")
            with w.write_file("test/rem") as f:
                f.write(b"FirstGen2")

        r = storage.begin_read()

        with storage.begin_write() as w:
            with w.write_file("test/file") as f:
                f.write(b"SecondGen")
            assert w.remove_file("test/rem")

        with r.read_file("test/file") as f:
            assert f.read() == b"FirstGen1"
        with r.read_file("test/rem") as f:
            assert f.read() == b"FirstGen2"
        r.release()

        with storage.begin_read() as r:
            with r.read_file("test/file") as f:
                assert f.read() == b"SecondGen"
            assert r.read_file("test/rem") is None

        for check in (tmp_path / "storage").iterdir():
            assert not check.name.startswith(".redirection_")
            assert not check.name.startswith(".transaction_")


def test_transaction_replay(tmp_path, storage):
    storage.initialize()
    with storage.begin_write() as w:
        with w.write_file("test/file") as f:
            f.write(b"FirstGen1")
        with w.write_file("test/rem") as f:
            f.write(b"FirstGen2")

    w = storage.begin_write()
    with w.write_file("test/file") as f:
        f.write(b"SecondGen")
    assert w.remove_file("test/rem")

    journal_file = w._transaction_root / ".journal"
    storage._write_journal(journal_file, w._actions)
    assert journal_file.exists()

    with (tmp_path / "storage" / "test" / "file").open("rb") as f:
        assert f.read() == b"FirstGen1"
    with (tmp_path / "storage" / "test" / "rem").open("rb") as f:
        assert f.read() == b"FirstGen2"

    w._was_released = True
    storage = Storage(tmp_path / "storage")
    with storage:
        for check in (tmp_path / "storage").iterdir():
            assert not check.name.startswith(".redirection_")
            assert not check.name.startswith(".transaction_")

        with storage.begin_read() as r:
            with r.read_file("test/file") as f:
                assert f.read() == b"SecondGen"
            assert r.read_file("test/rem") is None


def test_list_files(storage):
    with storage:
        begin_write = time.time()
        with storage.begin_write() as w:
            with w.write_file("test/file") as f:
                f.write(b"A")
            with w.write_file("test2/file") as f:
                f.write(b"A")
            with w.write_file("test2/test3/file") as f:
                f.write(b"A")
            with w.write_file("test4/test5/file1") as f:
                f.write(b"A")
            with w.write_file("test4/test5/file2") as f:
                f.write(b"A")

        assert sorted(storage.list_files("test")) == [
            "test/file",
        ]
        assert sorted(storage.list_files("test2")) == [
            "test2/file",
            "test2/test3/file",
        ]
        assert sorted(storage.list_files("test4/test5")) == [
            "test4/test5/file1",
            "test4/test5/file2",
        ]
        assert sorted(storage.list_files("test", modified_after=begin_write-1)) == [
            "test/file",
        ]
        assert storage.list_files("test/file") == []
