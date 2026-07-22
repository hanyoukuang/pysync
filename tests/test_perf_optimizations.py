import time
import threading
import pytest
from pysync import RwLock, AtomicInteger, Actor

def test_rwlock_direct_acquire_release():
    """
    TDD Test 1: Verify direct acquire_read/release_read and acquire_write/release_write
    methods on RwLock (bypassing guard object allocations).
    """
    lock = RwLock()
    state = [0]

    # Direct read lock
    lock.acquire_read()
    assert state[0] == 0
    lock.release_read()

    # Direct write lock
    lock.acquire_write()
    state[0] = 42
    lock.release_write()

    assert state[0] == 42


def test_rwlock_direct_try_acquire():
    """
    TDD Test 2: Verify try_acquire_read and try_acquire_write on RwLock.
    """
    lock = RwLock()
    assert lock.try_acquire_read() is True
    assert lock.try_acquire_write() is False  # Cannot acquire write while read lock is held
    lock.release_read()

    assert lock.try_acquire_write() is True
    assert lock.try_acquire_read() is False  # Cannot acquire read while write lock is held
    lock.release_write()


def test_atomic_add_sub_and_get():
    """
    TDD Test 3: Verify add_and_get and sub_and_get on AtomicInteger.
    """
    atomic = AtomicInteger(10)
    assert atomic.add_and_get(5) == 15
    assert atomic.sub_and_get(3) == 12
    assert atomic.get() == 12


def test_rwlock_direct_concurrent_performance():
    """
    TDD Test 4: Concurrent multi-threaded test using direct acquire_read/release_read.
    """
    lock = RwLock()
    shared_counter = [0]
    errors = []

    def reader():
        try:
            for _ in range(1000):
                lock.acquire_read()
                _ = shared_counter[0]
                lock.release_read()
        except Exception as e:
            errors.append(e)

    def writer():
        try:
            for _ in range(100):
                lock.acquire_write()
                shared_counter[0] += 1
                lock.release_write()
        except Exception as e:
            errors.append(e)

    threads = [threading.Thread(target=reader) for _ in range(4)] + \
              [threading.Thread(target=writer) for _ in range(2)]

    for t in threads: t.start()
    for t in threads: t.join(timeout=3.0)

    assert not errors
    assert shared_counter[0] == 200
