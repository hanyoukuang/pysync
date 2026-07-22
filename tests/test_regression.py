import pytest
import threading
import time
from pysync import (
    Channel,
    ConcurrentDict,
    ConcurrentMap,
    ThreadPool,
    Actor,
    ThreadGroup,
    RwLock,
    AtomicInteger,
    AtomicBoolean,
    select,
)

# ==============================================================================
# 1. CHANNEL TIMEOUT & CONCURRENT CLOSE TESTS
# ==============================================================================

@pytest.mark.parametrize("timeout_val", [0.1, 0.5, 1.0])
def test_channel_send_recv_timeout_args(timeout_val):
    """Verify Channel.send and recv accept positional and keyword timeout arguments."""
    ch = Channel(capacity=2)
    ch.send("val1", timeout_val)
    ch.send("val2", timeout=timeout_val)
    
    val1 = ch.recv(timeout_val)
    assert val1 == "val1"
    
    val2 = ch.recv(timeout=timeout_val)
    assert val2 == "val2"

def test_channel_send_recv_timeout_expiration():
    """Verify timeout expiration raises TimeoutError when timeout argument is passed to send/recv."""
    ch = Channel(capacity=1)
    ch.send("full")
    
    with pytest.raises(TimeoutError):
        ch.send("overflow", timeout=0.05)
        
    ch2 = Channel()
    with pytest.raises(TimeoutError):
        ch2.recv(timeout=0.05)

def test_channel_concurrent_close():
    """Verify calling Channel.close() concurrently with send/recv does not cause PyBorrowMutError."""
    ch = Channel()
    errors = []
    
    def receiver():
        try:
            for _ in range(100):
                try:
                    ch.recv()
                except ValueError:
                    break
        except Exception as e:
            errors.append(e)

    def closer():
        time.sleep(0.01)
        ch.close()

    t_rec = threading.Thread(target=receiver)
    t_cls = threading.Thread(target=closer)
    t_rec.start()
    t_cls.start()
    t_rec.join()
    t_cls.join()

    assert not errors, f"Unexpected errors during concurrent close: {errors}"

# ==============================================================================
# 2. CONCURRENTMAP ATOMIC LOOKUP & NONE VALUE HANDLING
# ==============================================================================

def test_concurrent_map_get_val_atomic():
    """Verify ConcurrentMap.get_val returns (found: bool, val: Any) atomically."""
    m = ConcurrentMap()
    m.set("key1", 100)
    m.set("key2", None)

    found1, val1 = m.get_val("key1")
    assert found1 is True
    assert val1 == 100

    found2, val2 = m.get_val("key2")
    assert found2 is True
    assert val2 is None

    found3, val3 = m.get_val("non_existent")
    assert found3 is False
    assert val3 is None

def test_concurrent_dict_none_value_handling():
    """Verify ConcurrentDict handles None values correctly without KeyError or missing keys."""
    d = ConcurrentDict()
    d["none_key"] = None

    assert "none_key" in d
    assert d["none_key"] is None
    assert d.get("none_key") is None
    assert d.get("none_key", "default") is None

    assert d.get("absent_key") is None
    assert d.get("absent_key", "default") == "default"

# ==============================================================================
# 3. THREADGROUP RE-ENTRANCY & WRITER EXCLUSION
# ==============================================================================

def test_thread_group_reentrant_nesting():
    """Verify ThreadGroup supports re-entrant nesting blocks."""
    executed = []
    with ThreadGroup() as tg1:
        tg1.spawn(lambda: executed.append("level1"))
        with ThreadGroup() as tg2:
            tg2.spawn(lambda: executed.append("level2"))
    assert "level1" in executed
    assert "level2" in executed

# ==============================================================================
# 4. CONCURRENTDICT GET COMPATIBILITY TESTS
# ==============================================================================

@pytest.mark.parametrize("key,val", [
    ("str_key", 100),
    (42, "int_key_val"),
    ((1, 2), [3, 4]),
    (True, "bool_key"),
    (3.14, "float_key"),
    ("k1", None),
    ("k2", False),
    ("k3", 0),
    ("k4", ""),
    ("k5", {}),
    ("k6", []),
    ("k7", (1,)),
    ("k8", set()),
    ("k9", 999999),
    ("k10", "final_val")
])
def test_concurrent_dict_get_valid(key, val):
    d = ConcurrentDict()
    d[key] = val
    assert d.get(key) == val
    assert d.get(key, "default_fallback") == val

@pytest.mark.parametrize("missing_key,default_val", [
    ("missing_1", None),
    ("missing_2", "custom_def"),
    ("missing_3", 0),
    ("missing_4", False),
    ("missing_5", []),
    ("missing_6", {}),
    (999, "int_def"),
    ((9, 9), "tuple_def"),
    (False, "bool_def"),
    (2.71, "float_def"),
    ("missing_11", -1),
    ("missing_12", "fallback_str"),
    ("missing_13", (1, 2)),
    ("missing_14", object()),
    ("missing_15", Exception)
])
def test_concurrent_dict_get_boundary(missing_key, default_val):
    d = ConcurrentDict()
    assert d.get(missing_key, default_val) == default_val

# ==============================================================================
# 5. RWLOCK GUARD RE-ENTRANCY TESTS
# ==============================================================================

@pytest.mark.parametrize("mode", ["read", "write"] * 4)
def test_rwlock_guard_valid_usage(mode):
    lock = RwLock()
    if mode == "read":
        with lock.read():
            pass
    else:
        with lock.write():
            pass

@pytest.mark.parametrize("lock_type", ["read", "write"] * 4)
def test_rwlock_guard_double_enter_error(lock_type):
    lock = RwLock()
    if lock_type == "read":
        guard = lock.read()
        with guard:
            with pytest.raises(RuntimeError, match="Lock guard already entered"):
                guard.__enter__()
    else:
        guard = lock.write()
        with guard:
            with pytest.raises(RuntimeError, match="Lock guard already entered"):
                guard.__enter__()

# ==============================================================================
# 6. ATOMICINTEGER & BOOLEAN REPR TESTS
# ==============================================================================

@pytest.mark.parametrize("val,expected_repr", [
    (0, "AtomicInteger(0)"),
    (42, "AtomicInteger(42)"),
    (-100, "AtomicInteger(-100)"),
])
def test_atomic_integer_repr_valid(val, expected_repr):
    a = AtomicInteger(val)
    assert repr(a) == expected_repr
    assert str(a) == str(val)
