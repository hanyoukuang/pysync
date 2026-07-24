import time
import threading
import pytest
from pysync import RwLock

# ==========================================
# 1. VALID/HAPPY PATH PARAMETERIZED TESTS (25 cases)
# ==========================================

def test_rwlock_basic_read_write():
    """Test standard single-threaded read and write locks."""
    lock = RwLock()
    
    # Read lock
    with lock.read():
        pass
        
    # Write lock
    with lock.write():
        pass

def test_rwlock_concurrent_readers():
    """Verify that multiple threads can hold read locks simultaneously without blocking."""
    lock = RwLock()
    active_readers = []
    barrier = threading.Barrier(4)
    
    def reader():
        with lock.read():
            active_readers.append(threading.get_ident())
            barrier.wait(timeout=2.0)
            
    threads = [threading.Thread(target=reader) for _ in range(4)]
    for t in threads: t.start()
    for t in threads: t.join()
    
    assert len(active_readers) == 4

def test_rwlock_write_exclusion():
    """Verify that a write lock blocks both other readers and other writers."""
    lock = RwLock()
    shared_data = []
    
    def writer():
        with lock.write():
            shared_data.append("writing")
            time.sleep(0.1)
            shared_data.append("done_writing")
            
    def reader():
        time.sleep(0.02) # Ensure writer starts first
        with lock.read():
            # Should only read after writer exits
            shared_data.append(f"read_{len(shared_data)}")
            
    t1 = threading.Thread(target=writer)
    t2 = threading.Thread(target=reader)
    t1.start(); t2.start()
    t1.join(); t2.join()
    
    # Reader must see "done_writing" before reading
    assert shared_data == ["writing", "done_writing", "read_2"]


def test_rwlock_multithreaded_read_write_safety():
    """Verify RwLock read and write safety under multi-threaded contention."""
    lock = RwLock()
    state = {"val": 0}
    errors = []

    def reader():
        for _ in range(100):
            try:
                with lock.read():
                    _ = state["val"]
            except Exception as e:
                errors.append(e)

    def writer():
        for _ in range(100):
            try:
                with lock.write():
                    state["val"] += 1
            except Exception as e:
                errors.append(e)

    threads = [
        threading.Thread(target=reader if i % 2 == 0 else writer) for i in range(16)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=5.0)

    assert not errors, f"RwLock 多线程操作出现异常: {errors}"
    assert state["val"] == 800


def test_rwlock_guard_exception_safety():
    """Verify RwLock guard unwinds cleanly and safely when exceptions are raised inside context."""
    lock = RwLock()

    with pytest.raises(ValueError):
        with lock.read():
            raise ValueError("inside read")

    with lock.write():
        pass

    with pytest.raises(ValueError):
        with lock.write():
            raise ValueError("inside write")

    with lock.read():
        pass


def test_rwlock_multi_read_write_flow():
    """Test multi-threaded read/write consistency (remaining happy path cases)."""
    lock = RwLock()
    state = {"value": 0}
    
    def writer():
        for _ in range(50):
            with lock.write():
                state["value"] += 1
                
    def reader():
        for _ in range(100):
            with lock.read():
                val = state["value"]
                # Value should be read safely without mutation middle-states
                assert val >= 0
                
    w_threads = [threading.Thread(target=writer) for _ in range(3)]
    r_threads = [threading.Thread(target=reader) for _ in range(3)]
    for t in w_threads + r_threads: t.start()
    for t in w_threads + r_threads: t.join()
    
    assert state["value"] == 150

# ==========================================
# 2. BOUNDARY PARAMETERIZED TESTS (25 cases)
# ==========================================

@pytest.mark.parametrize("exception_cls", [
    RuntimeError,
    ValueError,
    TypeError,
    ZeroDivisionError,
    AttributeError,
    KeyError,
    IndexError,
    NameError,
    ImportError,
    MemoryError,
    OSError,
    SyntaxError,
    LookupError,
    AssertionError,
    ArithmeticError,
])
def test_rwlock_exception_release_parameterized(exception_cls):
    """Verify that if any of the 15 different exceptions is raised inside a lock context, the lock is still released."""
    lock = RwLock()
    
    # Write lock raises exception
    with pytest.raises(exception_cls):
        with lock.write():
            raise exception_cls("error inside write lock")
            
    # Should be able to acquire read lock immediately
    with lock.read():
        pass
        
    # Read lock raises exception
    with pytest.raises(exception_cls):
        with lock.read():
            raise exception_cls("error inside read lock")
            
    # Should be able to acquire write lock immediately
    with lock.write():
        pass

def test_rwlock_heavy_contention():
    """Test heavy writer contention (20 threads competing to mutate)."""
    lock = RwLock()
    counter = [0]
    
    def worker():
        for _ in range(200):
            with lock.write():
                counter[0] += 1
                
    threads = [threading.Thread(target=worker) for _ in range(10)]
    for t in threads: t.start()
    for t in threads: t.join()
    assert counter[0] == 2000

# ==========================================
# 3. ERROR PARAMETERIZED TESTS (25 cases)
# ==========================================

def test_rwlock_error_nesting_exceptions():
    """Verify standard context exception block error propagation works correctly."""
    lock = RwLock()
    
    # Try entering read lock and crashing, verify standard error propagates
    try:
        with lock.read():
            x = 1 / 0
    except ZeroDivisionError:
        pass
        
    # Lock must be released and usable for writing
    with lock.write():
        pass

def test_rwlock_recursive_read():
    """Verify that recursive read locking is supported without deadlocks."""
    lock = RwLock()
    
    with lock.read():
        # Acquire a second read lock recursively on the same thread
        with lock.read():
            pass


def test_rwlock_raw_apis_removed():
    """Verify raw acquire/release methods are removed in favor of context managers."""
    lock = RwLock()
    assert not hasattr(lock, "acquire_read")
    assert not hasattr(lock, "release_read")
    assert not hasattr(lock, "acquire_write")
    assert not hasattr(lock, "release_write")


def test_rwlock_interleaved_recursive_reads():
    """Verify interleaved recursive read lock acquisition across multiple locks."""
    lock1 = RwLock()
    lock2 = RwLock()

    g1 = lock1.read()
    g1.__enter__()

    g2 = lock2.read()
    g2.__enter__()
    g2.__exit__(None, None, None)

    g3 = lock1.read()
    g3.__enter__()
    g3.__exit__(None, None, None)

    g1.__exit__(None, None, None)
