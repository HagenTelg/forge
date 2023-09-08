import pytest
import typing
from forge.archive.server.lock import ArchiveLocker, LockDenied


@pytest.fixture
def locker():
    return ArchiveLocker()


def test_basic(locker):
    u1 = "1"
    lock = locker.acquire_read(1, u1, "lock_key", 0, 100)
    lock.release()
    lock = locker.acquire_write(1, u1, "lock_key", 0, 100)
    lock.release()


def test_overlapping_read(locker):
    lock1 = locker.acquire_read(1, "r1", "lock_key", 25, 100)
    lock2 = locker.acquire_read(1, "r2", "lock_key", 50, 150)
    lock3 = locker.acquire_read(1, "r3", "lock_key", 0, 75)
    lock4 = locker.acquire_read(1, "r3", "lock_key", 0, 200)
    lock1.release()
    lock4.release()
    lock3.release()
    lock2.release()


def test_read_write(locker):
    r1 = locker.acquire_read(1, "r1", "lock_key", 0, 100)
    with locker.acquire_write(1, "w1", "lock_key", 0, 100):
        r2 = locker.acquire_read(1, "r2", "lock_key", 0, 100)
        try:
            locker.acquire_read(2, "r3", "lock_key", 0, 100)
            assert False
        except LockDenied:
            pass
    r3 = locker.acquire_read(2, "r3", "lock_key", 0, 100)
    r3.release()
    r2.release()
    r1.release()


def test_write_overlap(locker):
    with locker.acquire_write(1, "w1", "lock_key", 0, 100):
        try:
            locker.acquire_write(2, "w2", "lock_key", 0, 100)
            assert False
        except LockDenied:
            pass


def test_late_write_deny(locker):
    with locker.acquire_read(1, "w1", "lock_key", 0, 100):
        with locker.acquire_read(2, "r1", "lock_key", 0, 100):
            try:
                locker.acquire_write(1, "w2", "lock_key", 0, 100)
                assert False
            except LockDenied:
                pass


def test_time_overlap(locker):
    with locker.acquire_write(1, "w1", "key1", 100, 200):
        try:
            locker.acquire_write(2, "w2", "key1", 100, 200)
            assert False
        except LockDenied:
            pass
        try:
            locker.acquire_write(2, "w2", "key1", 50, 250)
            assert False
        except LockDenied:
            pass
        try:
            locker.acquire_write(2, "w2", "key1", 50, 150)
            assert False
        except LockDenied:
            pass
        try:
            locker.acquire_write(2, "w2", "key1", 150, 250)
            assert False
        except LockDenied:
            pass
        with locker.acquire_write(2, "w2", "key1", 25, 50):
            pass
        with locker.acquire_write(2, "w2", "key1", 250, 275):
            pass
    with locker.acquire_write(3, "w2", "key1", 100, 200):
        pass


def test_key_overlap(locker):
    with locker.acquire_write(1, "w1", "key1", 100, 200):
        with locker.acquire_write(2, "w2", "key2", 100, 200):
            pass
        with locker.acquire_read(3, "w2", "key2", 100, 200):
            pass