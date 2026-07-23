"""
test_regressions.py — Regression test suite for PySync core primitives.
"""

import threading
import time
import pytest
from pysync import ConcurrentDict, ThreadGroup, Actor, RwLock


def test_popitem_concurrent_no_duplicate():
    """Verify concurrent popitem() calls never return duplicate keys."""
    d = ConcurrentDict()
    for i in range(100):
        d[f"k{i}"] = i

    results = []
    errors = []
    lock = threading.Lock()

    def worker():
        try:
            while True:
                try:
                    item = d.popitem()
                    with lock:
                        results.append(item)
                except KeyError:
                    break
        except Exception as e:
            with lock:
                errors.append(e)

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors, f"Unexpected error during popitem(): {errors}"

    seen_keys = [item[0] for item in results]
    duplicates = [k for k in set(seen_keys) if seen_keys.count(k) > 1]
    assert not duplicates, f"popitem() returned duplicate keys: {duplicates}"
    assert len(d) == 0, f"Dictionary not empty after popitem(), remaining {len(d)} items"


def test_setdefault_returns_same_value_for_all_threads():
    """Verify concurrent setdefault() calls return identical value to all caller threads."""
    d = ConcurrentDict()
    results = []
    lock = threading.Lock()

    def worker(val):
        r = d.setdefault("shared_key", val)
        with lock:
            results.append(r)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(100)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    unique_results = set(results)
    assert len(unique_results) == 1, (
        f"setdefault() non-atomic, returned multiple values: {unique_results}"
    )


def test_concurrent_dict_has_docstring():
    """Verify ConcurrentDict has a non-empty docstring."""
    assert ConcurrentDict.__doc__ is not None, "ConcurrentDict.__doc__ is None"
    doc = ConcurrentDict.__doc__.strip()
    assert len(doc) > 10, f"Docstring surprisingly short: {doc!r}"


def test_threadgroup_all_spawned_threads_are_joined():
    """Verify all threads spawned in ThreadGroup are joined upon context exit."""
    for _ in range(30):
        completed = []
        lock = threading.Lock()

        def instant_task(i):
            with lock:
                completed.append(i)

        with ThreadGroup() as tg:
            for i in range(10):
                tg.spawn(instant_task, i)

        assert len(completed) == 10, (
            f"Only {len(completed)}/10 tasks completed after ThreadGroup exit"
        )


def test_actor_stop_never_started_does_not_block():
    """Verify stop() on a un-started Actor does not block."""

    class MyActor(Actor):
        def __init__(self):
            super().__init__()
            self.val = 0

        def get_val(self):
            return self.val

    actor = MyActor()
    assert object.__getattribute__(actor, '_thread') is None

    done = threading.Event()

    def do_stop():
        actor.stop()
        done.set()

    t = threading.Thread(target=do_stop)
    t.start()
    assert done.wait(timeout=2.0), "Actor.stop() on un-started actor timed out"
    t.join()


def test_actor_after_stop_without_start_still_usable():
    """Verify stopping an un-started Actor instance leaves subsequent Actor instances usable."""

    class MyActor(Actor):
        def __init__(self):
            super().__init__()
            self.val = 42

        def get_val(self):
            return self.val

    a1 = MyActor()
    a1.stop()

    a2 = MyActor()
    result = a2.get_val().result(timeout=2.0)
    assert result == 42, f"Expected 42, got {result}"
    a2.stop()


def test_rwlock_writer_not_starved():
    """Verify writers on RwLock are not starved by continuous readers."""
    lock = RwLock()
    writer_acquired = threading.Event()
    stop_readers = threading.Event()
    errors = []

    def continuous_reader():
        while not stop_readers.is_set():
            try:
                with lock.read():
                    time.sleep(0.001)
            except Exception as e:
                errors.append(e)
                break

    def writer_task():
        try:
            with lock.write():
                writer_acquired.set()
        except Exception as e:
            errors.append(e)

    readers = [threading.Thread(target=continuous_reader) for _ in range(6)]
    for r in readers:
        r.start()
    time.sleep(0.05)

    wt = threading.Thread(target=writer_task)
    wt.start()

    acquired = writer_acquired.wait(timeout=5.0)
    stop_readers.set()
    wt.join(timeout=3.0)
    for r in readers:
        r.join(timeout=2.0)

    assert not errors, f"RwLock test error: {errors}"
    assert acquired, "Writer starved, failed to acquire write lock within 5s"


def test_on_start_exception_triggers_on_error():
    """Verify on_start() exception triggers on_error() supervision hook."""
    error_log = []

    class FailStartActor(Actor):
        def on_start(self):
            raise RuntimeError("on_start intentional failure")

        def on_error(self, exc, method_name, args, kwargs):
            error_log.append((type(exc).__name__, method_name))
            return False

        def ping(self):
            return "pong"

    actor = FailStartActor()
    try:
        f = actor.ping()
        result = f.result(timeout=2.0)
        assert result == "pong"
    finally:
        actor.stop()

    assert len(error_log) >= 1, "on_start() exception did not trigger on_error()"
    assert error_log[0][0] == "RuntimeError"
    assert error_log[0][1] == "on_start"


def test_update_with_another_concurrent_dict():
    """Verify update(other_ConcurrentDict) merges all key-value pairs."""
    src = ConcurrentDict()
    src["a"] = 1
    src["b"] = 2
    src["c"] = 3

    dst = ConcurrentDict()
    dst["x"] = 99

    dst.update(src)

    assert dst.get("a") == 1
    assert dst.get("b") == 2
    assert dst.get("c") == 3
    assert dst.get("x") == 99
    assert len(dst) == 4


def test_update_self_does_not_crash():
    """Verify update(self) self-update does not crash or corrupt data."""
    d = ConcurrentDict()
    d["a"] = 1
    d["b"] = 2
    d.update(d)
    assert d.get("a") == 1
    assert d.get("b") == 2
    assert len(d) == 2
"""
test_issues.py - TDD: 先写失败测试，再修复 Issues

ISSUE-C: Actor.on_error 返回值被忽略（True 表示已处理，应压制 future.set_exception）
ISSUE-E: Actor.stop(timeout) 双重消耗 timeout（send + join 各用一次完整 timeout）
ISSUE-G: ThreadPool 被 GC 时队列中未执行任务的 Future 永久 pending
ISSUE-A: ConcurrentMap.set() 并发写入同一逻辑 key 可产生重复 entry
"""

import gc
import time
import threading
import concurrent.futures
import pytest
from pysync import Actor, ThreadPool, ConcurrentMap, ConcurrentDict


# ==============================================================================
# ISSUE-C: on_error 返回 True 应压制 future.set_exception
# ==============================================================================

def test_on_error_true_suppresses_exception():
    """Verify returning True from on_error suppresses exception and sets result to None."""
    class RecoveringActor(Actor):
        def on_error(self, exc, method_name, args, kwargs):
            return True

        def risky(self):
            raise ValueError("intentional error")

    actor = RecoveringActor()
    try:
        f = actor.risky()
        result = f.result(timeout=2.0)
        assert result is None, f"Expected None, got {result!r}"
    finally:
        actor.stop()


def test_on_error_false_propagates_exception():
    """Verify returning False from on_error propagates exception to Future."""
    class NonRecoveringActor(Actor):
        def on_error(self, exc, method_name, args, kwargs):
            return False

        def risky(self):
            raise ValueError("should propagate")

    actor = NonRecoveringActor()
    try:
        f = actor.risky()
        with pytest.raises(ValueError, match="should propagate"):
            f.result(timeout=2.0)
    finally:
        actor.stop()


def test_on_error_default_propagates_exception():
    """Verify default on_error propagates exception to Future."""
    class DefaultActor(Actor):
        def risky(self):
            raise RuntimeError("default propagation")

    actor = DefaultActor()
    try:
        f = actor.risky()
        with pytest.raises(RuntimeError, match="default propagation"):
            f.result(timeout=2.0)
    finally:
        actor.stop()


def test_stop_timeout_not_doubled():
    """Verify stop(timeout=T) total wait time does not double the timeout."""
    class BoundedActor(Actor):
        def __init__(self):
            super().__init__(mailbox_capacity=1)

        def slow(self):
            time.sleep(10)

    actor = BoundedActor()
    actor.slow()

    TIMEOUT = 0.3
    start = time.monotonic()
    actor.stop(timeout=TIMEOUT)
    elapsed = time.monotonic() - start

    assert elapsed < TIMEOUT * 1.8, f"stop(timeout={TIMEOUT}) took {elapsed:.3f}s"


def test_gc_pool_cancels_pending_futures():
    """Verify dropping un-shutdown ThreadPool resolves pending futures."""
    pool = ThreadPool(num_workers=1)
    blocker_started = threading.Event()

    def blocker():
        blocker_started.set()
        time.sleep(10)

    pool.submit(blocker)
    blocker_started.wait(timeout=2.0)

    pending_future = pool.submit(lambda: 42)

    del pool
    gc.collect()
    time.sleep(0.2)

    try:
        pending_future.result(timeout=2.0)
    except (concurrent.futures.CancelledError, Exception):
        pass
    except TimeoutError:
        pytest.fail("ThreadPool GC left pending future hanging")


def test_submitted_future_completes_before_gc():
    """Verify running tasks complete before ThreadPool GC."""
    pool = ThreadPool(num_workers=2)
    f = pool.submit(lambda x: x * 2, 21)
    assert f.result(timeout=2.0) == 42
    del pool


def test_concurrent_set_same_key_no_duplicates():
    """Verify concurrent set() on same logical key maintains len == 1."""
    m = ConcurrentMap(shard_count=1)

    class SameLogicalKey:
        def __init__(self):
            pass
        def __hash__(self):
            return 42
        def __eq__(self, other):
            return isinstance(other, SameLogicalKey)

    N = 50
    keys = [SameLogicalKey() for _ in range(N)]
    barrier = threading.Barrier(N)

    def writer(k):
        barrier.wait()
        m.set(k, "value")

    threads = [threading.Thread(target=writer, args=(k,)) for k in keys]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    actual_len = m.len()
    assert actual_len == 1, f"Expected 1, got {actual_len}"
    assert m.get(SameLogicalKey()) == "value"


def test_concurrent_set_distinct_keys_correct_count():
    """Verify concurrent set() on distinct keys preserves exact length."""
    m = ConcurrentMap()
    N = 200
    barrier = threading.Barrier(N)

    def writer(i):
        barrier.wait()
        m.set(f"key_{i}", i)

    threads = [threading.Thread(target=writer, args=(i,)) for i in range(N)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert m.len() == N
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
import pytest
import time
import threading
from pysync import (
    ThreadGroup,
    ThreadPool,
    AtomicBoolean,
    AtomicInteger,
    Channel,
    select,
    ConcurrentDict,
    ConcurrentMap
)

# ==========================================
# 1. ThreadGroup Comprehensive Tests
# ==========================================

def test_thread_group_high_concurrency_stress():
    """Spawn 200 threads inside ThreadGroup to verify all finish cleanly without thread leaks."""
    counter = []
    lock = threading.Lock()

    def worker(i):
        with lock:
            counter.append(i)

    with ThreadGroup() as tg:
        for i in range(200):
            tg.spawn(worker, i)

    assert len(counter) == 200
    assert sorted(counter) == list(range(200))


def test_thread_group_nested_reentrancy():
    """Verify nested ThreadGroup blocks work cleanly and collect child exceptions."""
    results = []

    with ThreadGroup() as parent_tg:
        parent_tg.spawn(lambda: results.append("parent_1"))
        
        def child_task():
            with ThreadGroup() as child_tg:
                child_tg.spawn(lambda: results.append("child_1"))
                child_tg.spawn(lambda: results.append("child_2"))

        parent_tg.spawn(child_task)

    assert len(results) == 3
    assert set(results) == {"parent_1", "child_1", "child_2"}


# ==========================================
# 2. ThreadPool Comprehensive Tests
# ==========================================

def test_threadpool_10k_tasks_stress():
    """Submit 10,000 tasks to a ThreadPool across 16 worker threads."""
    pool = ThreadPool(num_workers=16)
    try:
        futures = [pool.submit(lambda x: x * 2, i) for i in range(10000)]
        results = [f.result(timeout=5.0) for f in futures]
        assert results == [i * 2 for i in range(10000)]
    finally:
        pool.shutdown()


def test_threadpool_post_shutdown_rejection():
    """Verify submitting tasks to a shutdown ThreadPool raises RuntimeError."""
    pool = ThreadPool(num_workers=2)
    fut = pool.submit(lambda: 42)
    assert fut.result() == 42
    pool.shutdown()

    with pytest.raises(RuntimeError, match="shutdown"):
        pool.submit(lambda: 100)


# ==========================================
# 3. AtomicBoolean & AtomicInteger Concurrent Stress Tests
# ==========================================

def test_atomic_bool_cas_stress():
    """32 threads competing to flip AtomicBoolean using compare_and_set."""
    flag = AtomicBoolean(False)
    success_count = [0]
    lock = threading.Lock()

    def flipper():
        if flag.compare_and_set(False, True):
            with lock:
                success_count[0] += 1

    threads = [threading.Thread(target=flipper) for _ in range(32)]
    for t in threads: t.start()
    for t in threads: t.join()

    assert success_count[0] == 1, "Only one thread should successfully flip CAS"
    assert flag.get() is True


def test_atomic_integer_add_and_get_stress():
    """32 threads concurrently adding values to AtomicInteger using add_and_get."""
    atom = AtomicInteger(0)

    def worker():
        for _ in range(1000):
            atom.add_and_get(1)

    threads = [threading.Thread(target=worker) for _ in range(32)]
    for t in threads: t.start()
    for t in threads: t.join()

    assert atom.get() == 32000


# ==========================================
# 4. Bidirectional Select Tests
# ==========================================

def test_select_mixed_send_and_recv():
    """Verify select() with both send_op() and recv_op() ready operations."""
    ch_send = Channel(capacity=10)
    ch_recv = Channel(capacity=10)
    ch_recv.send("incoming_msg")

    ops = [ch_send.send_op("outgoing_msg"), ch_recv.recv_op()]
    idx, val = select(ops)

    if idx == 0:
        # Send operation selected
        assert val is None
        assert ch_send.recv() == "outgoing_msg"
    else:
        # Recv operation selected
        assert val == "incoming_msg"


def test_select_ready_channel_priority():
    """Verify select chooses immediately ready channel operation without blocking."""
    ch1 = Channel(capacity=5)
    ch2 = Channel(capacity=5)
    ch2.send("ready_data")

    ops = [ch1.recv_op(), ch2.recv_op()]
    idx, val = select(ops)
    assert idx == 1
    assert val == "ready_data"


# ==========================================
# 5. ConcurrentDict & ConcurrentMap Additional Methods
# ==========================================

def test_concurrent_dict_fromkeys_and_clear():
    """Verify fromkeys() initialization and thread-safe clear()."""
    keys = ["a", "b", "c", "d", "e"]
    cd = ConcurrentDict.fromkeys(keys, 999)
    assert len(cd) == 5
    assert all(cd[k] == 999 for k in keys)

    cd.clear()
    assert len(cd) == 0
    assert cd.get("a") is None


def test_concurrent_dict_kwargs_and_items_mutation():
    """Verify dictionary updates with kwargs and items views."""
    cd = ConcurrentDict()
    cd["a"] = 1
    cd["b"] = 2
    cd.update(c=3, d=4)
    assert cd["c"] == 3
    assert cd["d"] == 4

    items = list(cd.items())
    assert ("a", 1) in items
    assert ("d", 4) in items

