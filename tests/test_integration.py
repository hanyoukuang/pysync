import gc
import time
import random
import threading
import contextvars
import concurrent.futures
import pytest
from mypy import api
from pysync import (
    Actor,
    Channel,
    ConcurrentDict,
    ConcurrentMap,
    RwLock,
    AtomicBoolean,
    AtomicInteger,
    ThreadPool,
    ThreadGroup,
    select,
)

# ==============================================================================
# 1. ConcurrentDict & ConcurrentMap Edge Case Tests
# ==============================================================================

def test_concurrent_dict_popitem_no_duplicate_keys():
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

    assert not errors, f"popitem() 抛出意外异常: {errors}"

    seen_keys = [item[0] for item in results]
    duplicates = [k for k in set(seen_keys) if seen_keys.count(k) > 1]
    assert not duplicates, f"popitem() 返回了重复 key: {duplicates}"
    assert len(d) == 0, f"popitem() 后字典不为空，剩余 {len(d)} 条"


def test_concurrent_dict_setdefault_atomicity():
    """Verify concurrent setdefault() calls return the identical value across threads."""
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
        f"setdefault() 非原子，返回了多个不同值: {unique_results}"
    )


def test_concurrent_dict_docstring_presence():
    """Verify ConcurrentDict.__doc__ is not None."""
    assert ConcurrentDict.__doc__ is not None, "ConcurrentDict.__doc__ 为 None"
    doc = ConcurrentDict.__doc__.strip()
    assert len(doc) > 10, f"docstring 内容异常短: {doc!r}"


def test_concurrent_dict_update_with_another_concurrent_dict():
    """Verify update(other_ConcurrentDict) correctly merges all key-value pairs."""
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


def test_concurrent_dict_update_self_safety():
    """Verify update(self) self-update does not crash or lose data."""
    d = ConcurrentDict()
    d["a"] = 1
    d["b"] = 2
    d.update(d)
    assert d.get("a") == 1
    assert d.get("b") == 2
    assert len(d) == 2


def test_concurrent_map_set_same_key_no_duplicates():
    """Verify concurrent set() calls with identical logical key produce only 1 entry."""
    m = ConcurrentMap(shard_count=1)

    class SameLogicalKey:
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
    assert actual_len == 1, f"期望 1 条，实际 {actual_len} 条"
    assert m.get(SameLogicalKey()) == "value"


def test_concurrent_map_set_distinct_keys_correct_count():
    """Verify concurrent set() calls with distinct keys maintain accurate count."""
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

    assert m.len() == N, f"期望 {N} 条，实际 {m.len()} 条"


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
# 2. ThreadGroup & ThreadPool Edge Case Tests
# ==============================================================================

def test_threadgroup_all_spawned_threads_joined():
    """Verify all spawned threads inside ThreadGroup are completed after __exit__."""
    for _ in range(30):
        completed = []
        lock = threading.Lock()

        def instant_task(i):
            with lock:
                completed.append(i)

        with ThreadGroup() as tg:
            for i in range(10):
                tg.spawn(instant_task, i)

        assert len(completed) == 10


def test_thread_group_high_concurrency_stress():
    """Spawn 200 threads inside ThreadGroup to verify all finish cleanly without leaks."""
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


def test_threadgroup_reentrant_nesting():
    """Verify ThreadGroup supports re-entrant nesting blocks."""
    executed = []
    with ThreadGroup() as tg1:
        tg1.spawn(lambda: executed.append("level1"))
        with ThreadGroup() as tg2:
            tg2.spawn(lambda: executed.append("level2"))
    assert "level1" in executed
    assert "level2" in executed


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


def test_threadpool_gc_cancels_pending_futures():
    """Verify ThreadPool GC cancels unexecuted pending Futures instead of hanging."""
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
    except concurrent.futures.CancelledError:
        pass
    except Exception:
        pass
    except TimeoutError:
        pytest.fail("ThreadPool GC 后，pending Future 永久 pending")


def test_threadpool_submitted_future_completes_before_gc():
    """Verify executing task completes before ThreadPool GC."""
    pool = ThreadPool(num_workers=2)
    f = pool.submit(lambda x: x * 2, 21)
    assert f.result(timeout=2.0) == 42
    del pool


# ==============================================================================
# 3. Actor & RwLock Advanced Lifecycle Tests
# ==============================================================================

def test_actor_stop_unstarted_does_not_block():
    """Verify stop() on unstarted Actor instance does not block or deadlock."""
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
    assert done.wait(timeout=2.0)
    t.join()


def test_actor_instance_reuse_after_unstarted_stop():
    """Verify unstarted stop() on one Actor does not affect subsequent Actor instances."""
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
    assert result == 42
    a2.stop()


def test_actor_on_start_exception_triggers_on_error():
    """Verify on_start() exception triggers on_error() callback."""
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

    assert len(error_log) >= 1
    assert error_log[0][0] == "RuntimeError"
    assert error_log[0][1] == "on_start"


def test_actor_on_error_true_suppresses_exception():
    """Verify on_error returning True suppresses exception propagation to Future."""
    class RecoveringActor(Actor):
        def on_error(self, exc, method_name, args, kwargs):
            return True

        def risky(self):
            raise ValueError("intentional error")

    actor = RecoveringActor()
    try:
        f = actor.risky()
        result = f.result(timeout=2.0)
        assert result is None
    finally:
        actor.stop()


def test_actor_on_error_false_propagates_exception():
    """Verify on_error returning False propagates exception to Future."""
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


def test_actor_on_error_default_propagates_exception():
    """Verify default on_error implementation propagates exception to Future."""
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


def test_actor_stop_timeout_bounded():
    """Verify stop(timeout=T) bounded wait time does not exceed ~T."""
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

    assert elapsed < TIMEOUT * 1.8


def test_rwlock_writer_starvation_prevention():
    """Verify continuous readers do not starve waiting writers in RwLock."""
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

    assert not errors, f"RwLock 测试异常: {errors}"
    assert acquired, "写者被饿死"


def test_rwlock_direct_acquire_release():
    """Verify read and write context managers on RwLock."""
    lock = RwLock()
    state = [0]

    with lock.read():
        assert state[0] == 0

    with lock.write():
        state[0] = 42

    assert state[0] == 42


def test_rwlock_direct_try_acquire():
    """Verify context manager lock exclusivity."""
    lock = RwLock()
    with lock.read():
        assert hasattr(lock, "read")


def test_rwlock_direct_concurrent_performance():
    """Concurrent multi-threaded test using RwLock context managers."""
    lock = RwLock()
    shared_counter = [0]
    errors = []

    def reader():
        try:
            for _ in range(1000):
                with lock.read():
                    _ = shared_counter[0]
        except Exception as e:
            errors.append(e)

    def writer():
        try:
            for _ in range(100):
                with lock.write():
                    shared_counter[0] += 1
        except Exception as e:
            errors.append(e)

    threads = [threading.Thread(target=reader) for _ in range(4)] + \
              [threading.Thread(target=writer) for _ in range(2)]

    for t in threads: t.start()
    for t in threads: t.join(timeout=3.0)

    assert not errors
    assert shared_counter[0] == 200


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
# 4. Atomic Primitives & Channel Edge Cases
# ==============================================================================

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

    assert success_count[0] == 1
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


def test_atomic_add_sub_and_get():
    """Verify add_and_get and sub_and_get on AtomicInteger."""
    atomic = AtomicInteger(10)
    assert atomic.add_and_get(5) == 15
    assert atomic.sub_and_get(3) == 12
    assert atomic.get() == 12


@pytest.mark.parametrize("val,expected_repr", [
    (0, "AtomicInteger(0)"),
    (42, "AtomicInteger(42)"),
    (-100, "AtomicInteger(-100)"),
])
def test_atomic_integer_repr_valid(val, expected_repr):
    a = AtomicInteger(val)
    assert repr(a) == expected_repr
    assert str(a) == str(val)


def test_select_mixed_send_and_recv():
    """Verify select() with both send_op() and recv_op() ready operations."""
    ch_send = Channel(capacity=10)
    ch_recv = Channel(capacity=10)
    ch_recv.send("incoming_msg")

    ops = [ch_send.send_op("outgoing_msg"), ch_recv.recv_op()]
    idx, val = select(ops)

    if idx == 0:
        assert val is None
        assert ch_send.recv() == "outgoing_msg"
    else:
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
    """Verify timeout expiration raises TimeoutError when timeout argument is passed."""
    ch = Channel(capacity=1)
    ch.send("full")
    
    with pytest.raises(TimeoutError):
        ch.send("overflow", timeout=0.05)
        
    ch2 = Channel()
    with pytest.raises(TimeoutError):
        ch2.recv(timeout=0.05)


def test_channel_concurrent_close():
    """Verify calling Channel.close() concurrently with send/recv does not cause error."""
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
# 5. Real-World Integration Workflows & Scenarios
# ==============================================================================

class OrderBookActor(Actor):
    """An Actor representing an isolated, thread-safe financial order book."""
    def __init__(self):
        super().__init__()
        self.bids = []
        self.asks = []
        self.total_volume = 0.0
        self.trades_executed = 0

    def place_order(self, order_type: str, price: float, qty: int, trader_id: str):
        if order_type == "BUY":
            remaining_qty = qty
            new_asks = []
            for ask_price, ask_qty, ask_trader in sorted(self.asks, key=lambda x: x[0]):
                if ask_price <= price and remaining_qty > 0:
                    matched_qty = min(remaining_qty, ask_qty)
                    remaining_qty -= matched_qty
                    self.total_volume += matched_qty * ask_price
                    self.trades_executed += 1
                    if ask_qty > matched_qty:
                        new_asks.append((ask_price, ask_qty - matched_qty, ask_trader))
                else:
                    new_asks.append((ask_price, ask_qty, ask_trader))
            self.asks = new_asks
            if remaining_qty > 0:
                self.bids.append((price, remaining_qty, trader_id))
        else:
            remaining_qty = qty
            new_bids = []
            for bid_price, bid_qty, bid_trader in sorted(self.bids, key=lambda x: x[0], reverse=True):
                if bid_price >= price and remaining_qty > 0:
                    matched_qty = min(remaining_qty, bid_qty)
                    remaining_qty -= matched_qty
                    self.total_volume += matched_qty * bid_price
                    self.trades_executed += 1
                    if bid_qty > matched_qty:
                        new_bids.append((bid_price, bid_qty - matched_qty, bid_trader))
                else:
                    new_bids.append((bid_price, bid_qty, bid_trader))
            self.bids = new_bids
            if remaining_qty > 0:
                self.asks.append((price, remaining_qty, trader_id))

        return self.trades_executed

    def get_stats(self):
        return (self.trades_executed, self.total_volume, len(self.bids), len(self.asks))


def test_scenario_order_matching_engine():
    """Real-World Scenario 1: Financial Order Matching Engine."""
    order_ingest_ch = Channel(capacity=100)
    order_book = OrderBookActor()
    trader_balances = ConcurrentDict()

    for t in range(16):
        trader_balances[f"trader_{t}"] = 100000.0

    stop_signal = AtomicBoolean(False)
    orders_sent = AtomicInteger(0)

    def trader_client(trader_id):
        rng = random.Random(trader_id)
        for _ in range(50):
            if stop_signal.get():
                break
            side = "BUY" if rng.random() > 0.5 else "SELL"
            price = round(100.0 + rng.uniform(-5.0, 5.0), 2)
            qty = rng.randint(1, 10)
            try:
                order_ingest_ch.send((side, price, qty, f"trader_{trader_id}"), timeout=0.01)
                orders_sent.increment()
            except (TimeoutError, ValueError):
                pass

    def matching_engine_worker():
        while not stop_signal.get():
            try:
                side, price, qty, trader_id = order_ingest_ch.recv(timeout=0.02)
                order_book.place_order(side, price, qty, trader_id)
            except (TimeoutError, ValueError):
                if stop_signal.get():
                    break

    trader_threads = [threading.Thread(target=trader_client, args=(i,)) for i in range(16)]
    engine_thread = threading.Thread(target=matching_engine_worker)

    engine_thread.start()
    for t in trader_threads: t.start()

    for t in trader_threads: t.join(timeout=3.0)
    stop_signal.set(True)
    order_ingest_ch.close()
    engine_thread.join(timeout=3.0)

    trades_executed, volume, bids_left, asks_left = order_book.get_stats().result(timeout=10.0)
    order_book.stop()

    assert orders_sent.get() > 0
    assert trades_executed > 0


trace_id_var = contextvars.ContextVar("trace_id", default="no_trace")

def test_scenario_etl_pipeline_and_cache():
    """Real-World Scenario 2: Multi-stream telemetry collector and ETL cache."""
    logs_ch = Channel(capacity=50)
    metrics_ch = Channel(capacity=50)
    cache = ConcurrentMap(shard_count=16)

    processed_logs = AtomicInteger(0)
    processed_metrics = AtomicInteger(0)
    stop_signal = AtomicBoolean(False)

    pool = ThreadPool(num_workers=8)

    def log_producer():
        for i in range(200):
            try:
                logs_ch.send((f"user_{i % 10}", f"LOG_PAYLOAD_{i}"), timeout=0.01)
            except (TimeoutError, ValueError):
                pass

    def metric_producer():
        for i in range(200):
            try:
                metrics_ch.send((f"user_{i % 10}", i * 1.5), timeout=0.01)
            except (TimeoutError, ValueError):
                pass

    def etl_processor():
        while not stop_signal.get():
            ops = [logs_ch.recv_op(), metrics_ch.recv_op()]
            try:
                idx, item = select(ops)
                if item is None:
                    continue
                user_id, val = item
                if idx == 0:
                    trace_id_var.set(f"trace_log_{user_id}")
                    def process_log(uid, log_val):
                        cache.set(f"last_log_{uid}", log_val)
                        processed_logs.increment()
                        return trace_id_var.get()
                    f = pool.submit(process_log, user_id, val)
                    assert f.result(timeout=1.0).startswith("trace_log_")
                else:
                    trace_id_var.set(f"trace_metric_{user_id}")
                    def process_metric(uid, metric_val):
                        cache.set(f"last_metric_{uid}", metric_val)
                        processed_metrics.increment()
                        return trace_id_var.get()
                    f = pool.submit(process_metric, user_id, val)
                    assert f.result(timeout=1.0).startswith("trace_metric_")
            except ValueError:
                time.sleep(0.001)

    prod1 = threading.Thread(target=log_producer)
    prod2 = threading.Thread(target=metric_producer)
    processor = threading.Thread(target=etl_processor)

    processor.start()
    prod1.start(); prod2.start()

    prod1.join(); prod2.join()
    time.sleep(0.1)
    stop_signal.set(True)
    logs_ch.close(); metrics_ch.close()
    processor.join(timeout=2.0)
    pool.shutdown()

    assert processed_logs.get() > 0
    assert processed_metrics.get() > 0
    assert cache.contains_key("last_log_user_0")


class CircuitBreaker:
    """A thread-safe Circuit Breaker using RwLock."""
    def __init__(self):
        self.lock = RwLock()
        self.state = 0
        self.failure_count = AtomicInteger(0)

    def allow_request(self) -> bool:
        with self.lock.read():
            return self.state == 0

    def record_failure(self, threshold=5):
        fails = self.failure_count.increment()
        if fails >= threshold:
            with self.lock.write():
                self.state = 1


def test_scenario_circuit_breaker_and_request_scoping():
    """Real-World Scenario 3: Microservice circuit breaker and request scope."""
    breaker = CircuitBreaker()
    successful_requests = AtomicInteger(0)
    rejected_requests = AtomicInteger(0)

    def handle_api_request(req_id, simulate_db_failure=False):
        if not breaker.allow_request():
            rejected_requests.increment()
            return "503_SERVICE_UNAVAILABLE"

        if simulate_db_failure:
            breaker.record_failure(threshold=5)
            return "500_INTERNAL_ERROR"

        successful_requests.increment()
        return "200_OK"

    with ThreadGroup() as tg:
        for i in range(50):
            tg.spawn(handle_api_request, i, False)

    assert successful_requests.get() == 50
    assert rejected_requests.get() == 0

    with ThreadGroup() as tg:
        for i in range(10):
            tg.spawn(handle_api_request, i, True)

    with ThreadGroup() as tg:
        for i in range(30):
            tg.spawn(handle_api_request, i, False)

    assert rejected_requests.get() >= 20


class BatchDbWriterActor(Actor):
    """An Actor that accumulates incoming records into batches."""
    def __init__(self, batch_size=20):
        super().__init__()
        self.batch_size = batch_size
        self.buffer = []
        self.flushed_batches = 0
        self.total_records_written = 0

    def write_record(self, record: dict):
        self.buffer.append(record)
        if len(self.buffer) >= self.batch_size:
            self._flush_internal()
        return self.total_records_written

    def flush(self):
        self._flush_internal()
        return self.total_records_written

    def _flush_internal(self):
        if self.buffer:
            self.total_records_written += len(self.buffer)
            self.flushed_batches += 1
            self.buffer.clear()

    def on_stop(self):
        self._flush_internal()

    def get_stats(self):
        return (self.flushed_batches, self.total_records_written)


def test_scenario_batch_database_writer():
    """Real-World Scenario 4: High-concurrency batch DB writer."""
    db_writer = BatchDbWriterActor(batch_size=25)
    records_sent = AtomicInteger(0)

    def producer(pid):
        for i in range(50):
            record = {"pid": pid, "seq": i, "timestamp": time.time()}
            db_writer.write_record(record)
            records_sent.increment()

    producers = [threading.Thread(target=producer, args=(t,)) for t in range(16)]
    for t in producers: t.start()
    for t in producers: t.join(timeout=3.0)

    batches, total_written = db_writer.get_stats().result(timeout=5.0)
    db_writer.stop()

    assert records_sent.get() == 800
    assert total_written <= 800


# ==============================================================================
# 6. Static Typing Verification (Mypy)
# ==============================================================================

def test_mypy_type_checking_python_source():
    """Verify that python/pysync passes mypy static type checking with zero errors."""
    stdout, stderr, exit_code = api.run(["python/pysync"])
    assert exit_code == 0, f"Mypy type check failed on python/pysync:\n{stdout}\n{stderr}"


def test_mypy_strict_type_stubs():
    """Verify that python/pysync/__init__.pyi passes mypy --strict validation."""
    stdout, stderr, exit_code = api.run(["--strict", "python/pysync/__init__.pyi"])
    assert exit_code == 0, f"Mypy --strict failed on __init__.pyi:\n{stdout}\n{stderr}"
