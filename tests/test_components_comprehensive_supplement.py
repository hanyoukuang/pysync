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

