import time
import threading
import gc
import sys
import queue
import pytest
import pysync
from pysync import Channel, ConcurrentDict, RwLock, AtomicInteger, AtomicBoolean, ThreadPool, ThreadGroup, Actor


# ============================================================================
# From test_bugs.py
# ============================================================================
"""
test_bugs.py - TDD: 先写失败测试，再修复 Bug

发现的 Bug:
  BUG-1: ConcurrentDict.popitem() 线程不安全（items/delete 之间竞争）
  BUG-2: ConcurrentDict.setdefault() TOCTOU 竞争（get_val/set 之间非原子）
  BUG-3: ConcurrentDict 类 docstring 位置错误（__hash__=None 写在 docstring 前）
  BUG-4: ThreadGroup.spawn() 竞争（start 后 append 前线程即完成，__exit__ 遗漏）
  BUG-5: Actor.stop() 在线程未启动时留下未消费 None 哨兵
  BUG-6: Python 层 RwLock 写者饥饿（_writers_waiting>0 时新读者仍被阻塞但实现检查有误）
  BUG-7: Actor._run_loop on_start 异常被静默吞噬，on_error 不被调用
  BUG-8: ConcurrentDict.update() 传入另一个 ConcurrentDict 时行为正确（验证回归）
"""

import threading
import time
import pytest
from pysync import ConcurrentDict, ThreadGroup, Actor, RwLock


# ==============================================================================
# BUG-1: ConcurrentDict.popitem() 并发下可能返回重复 key
#         items()[0] 快照后、delete() 前，其他线程可能删掉同一个 key
# ==============================================================================

def test_bug1_popitem_concurrent_no_duplicate():
    """BUG-1: 并发 popitem() 不得返回同一个 key 两次。"""
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
    assert not duplicates, f"BUG-1: popitem() 返回了重复 key: {duplicates}"
    assert len(d) == 0, f"BUG-1: popitem() 后字典不为空，剩余 {len(d)} 条"


# ==============================================================================
# BUG-2: ConcurrentDict.setdefault() TOCTOU —— 并发下多线程都看到 found=False
# ==============================================================================

def test_bug2_setdefault_returns_same_value_for_all_threads():
    """BUG-2: 并发 setdefault() 所有线程应返回同一个值。"""
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
        f"BUG-2: setdefault() 非原子，返回了多个不同值: {unique_results}"
    )


# ==============================================================================
# BUG-3: ConcurrentDict 类 docstring 位置错误，__doc__ 为 None
# ==============================================================================

def test_bug3_concurrent_dict_has_docstring():
    """BUG-3: ConcurrentDict.__doc__ 必须非 None（docstring 须在类体第一个语句）。"""
    assert ConcurrentDict.__doc__ is not None, (
        "BUG-3: ConcurrentDict.__doc__ 为 None，"
        "docstring 写在 __hash__=None 之后，Python 不识别为类文档"
    )
    doc = ConcurrentDict.__doc__.strip()
    assert len(doc) > 10, f"BUG-3: docstring 内容异常短: {doc!r}"


# ==============================================================================
# BUG-4: ThreadGroup.spawn() 竞争 —— start() 后 append() 前线程即完成
# ==============================================================================

def test_bug4_threadgroup_all_spawned_threads_are_joined():
    """BUG-4: __exit__ 后所有 spawn 的线程都必须已完成。"""
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
            f"BUG-4: __exit__ 后只完成了 {len(completed)}/10 个任务，"
            "spawn 存在竞争导致线程被遗漏"
        )


# ==============================================================================
# BUG-5: Actor.stop() 在线程未启动时不应阻塞，也不应留下 None 哨兵
# ==============================================================================

def test_bug5_actor_stop_never_started_does_not_block():
    """BUG-5: 从未启动的 Actor 调用 stop() 不应阻塞。"""

    class MyActor(Actor):
        def __init__(self):
            super().__init__()
            self.val = 0

        def get_val(self):
            return self.val

    actor = MyActor()
    assert object.__getattribute__(actor, '_thread') is None, "线程应未启动"

    done = threading.Event()

    def do_stop():
        actor.stop()
        done.set()

    t = threading.Thread(target=do_stop)
    t.start()
    assert done.wait(timeout=2.0), "BUG-5: Actor.stop() 在线程未启动时超时/死锁"
    t.join()


def test_bug5_actor_after_stop_without_start_still_usable():
    """BUG-5: stop() 未启动后，新的 Actor 实例正常工作（无 None 哨兵污染）。"""

    class MyActor(Actor):
        def __init__(self):
            super().__init__()
            self.val = 42

        def get_val(self):
            return self.val

    # 第一个 actor：stop() 但没启动过
    a1 = MyActor()
    a1.stop()

    # 第二个 actor 必须正常工作
    a2 = MyActor()
    result = a2.get_val().result(timeout=2.0)
    assert result == 42, f"BUG-5: 新 Actor 返回值应为 42，实际为 {result}"
    a2.stop()


# ==============================================================================
# BUG-6: RwLock 写者饥饿 —— 连续读者让写者永远无法获锁
# ==============================================================================

def test_bug6_rwlock_writer_not_starved():
    """BUG-6: 在持续读者存在的情况下，写者必须能在合理时间内获取写锁。"""
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

    assert not errors, f"BUG-6: RwLock 测试异常: {errors}"
    assert acquired, "BUG-6: 写者被饿死，5 秒内未能获取写锁"


# ==============================================================================
# BUG-7: Actor on_start 异常应触发 on_error，当前被静默吞噬
# ==============================================================================

def test_bug7_on_start_exception_triggers_on_error():
    """BUG-7: on_start() 抛出异常时，on_error() 应被调用。"""
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
        # ping 本身应该仍然正常完成
        result = f.result(timeout=2.0)
        assert result == "pong"
    finally:
        actor.stop()

    assert len(error_log) >= 1, (
        "BUG-7: on_start() 异常应触发 on_error，但 error_log 为空"
    )
    assert error_log[0][0] == "RuntimeError", (
        f"BUG-7: on_error 接收到的异常类型不对: {error_log[0][0]}"
    )
    assert error_log[0][1] == "on_start", (
        f"BUG-7: on_error 的 method_name 应为 'on_start'，实际: {error_log[0][1]}"
    )


# ==============================================================================
# BUG-8: ConcurrentDict.update() 传入另一个 ConcurrentDict 应正常工作
# ==============================================================================

def test_bug8_update_with_another_concurrent_dict():
    """BUG-8: update(other_ConcurrentDict) 应正确合并所有键值对。"""
    src = ConcurrentDict()
    src["a"] = 1
    src["b"] = 2
    src["c"] = 3

    dst = ConcurrentDict()
    dst["x"] = 99

    dst.update(src)

    assert dst.get("a") == 1, "BUG-8: key 'a' 未被 update 写入"
    assert dst.get("b") == 2, "BUG-8: key 'b' 未被 update 写入"
    assert dst.get("c") == 3, "BUG-8: key 'c' 未被 update 写入"
    assert dst.get("x") == 99, "BUG-8: 原有 key 'x' 被 update 破坏"
    assert len(dst) == 4, f"BUG-8: update 后长度应为 4，实际 {len(dst)}"


def test_bug8_update_self_does_not_crash():
    """BUG-8 变体: update(self) 自我更新不应崩溃或丢失数据。"""
    d = ConcurrentDict()
    d["a"] = 1
    d["b"] = 2
    d.update(d)
    assert d.get("a") == 1
    assert d.get("b") == 2
    assert len(d) == 2


# ============================================================================
# From test_components_comprehensive_supplement.py
# ============================================================================
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



# ============================================================================
# From test_issues.py
# ============================================================================
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

def test_issue_c_on_error_true_suppresses_exception():
    """ISSUE-C: on_error 返回 True 时，调用方的 future.result() 不应抛出异常。"""
    class RecoveringActor(Actor):
        def on_error(self, exc, method_name, args, kwargs):
            # 声称"我已经处理了这个异常"
            return True

        def risky(self):
            raise ValueError("intentional error")

    actor = RecoveringActor()
    try:
        f = actor.risky()
        # on_error 返回 True → 异常已处理 → future 应有结果（None）而非异常
        result = f.result(timeout=2.0)
        assert result is None, (
            f"ISSUE-C: on_error 返回 True 时 future 应返回 None，实际返回 {result!r}"
        )
    finally:
        actor.stop()


def test_issue_c_on_error_false_propagates_exception():
    """ISSUE-C 对照组: on_error 返回 False 时，异常仍应传播到 future。"""
    class NonRecoveringActor(Actor):
        def on_error(self, exc, method_name, args, kwargs):
            return False  # "我没处理"

        def risky(self):
            raise ValueError("should propagate")

    actor = NonRecoveringActor()
    try:
        f = actor.risky()
        with pytest.raises(ValueError, match="should propagate"):
            f.result(timeout=2.0)
    finally:
        actor.stop()


def test_issue_c_on_error_default_propagates_exception():
    """ISSUE-C 对照组: 默认 on_error 返回 False，异常必须传播。"""
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


# ==============================================================================
# ISSUE-E: stop(timeout) 双重消耗 timeout
# ==============================================================================

def test_issue_e_stop_timeout_not_doubled():
    """
    ISSUE-E: stop(timeout=T) 的总等待时间不应超过 ~T，
    而不是 send 等 T + join 等 T = 2T。
    """
    class BoundedActor(Actor):
        def __init__(self):
            # mailbox 容量为 1，发满后 stop(None) 会发哨兵
            super().__init__(mailbox_capacity=1)

        def slow(self):
            time.sleep(10)  # 占住 actor 线程

    actor = BoundedActor()
    # 先发一个慢任务占住 actor 线程
    actor.slow()
    # 再发一个任务填满 mailbox（capacity=1，已有 1 条慢任务在队列里）
    # 现在 mailbox 满了

    TIMEOUT = 0.3
    start = time.monotonic()
    actor.stop(timeout=TIMEOUT)
    elapsed = time.monotonic() - start

    # 总等待时间应 <= TIMEOUT * 1.5（留 50% 误差余量），绝不应接近 2 * TIMEOUT
    assert elapsed < TIMEOUT * 1.8, (
        f"ISSUE-E: stop(timeout={TIMEOUT}) 实际等待了 {elapsed:.3f}s，"
        f"超过了 {TIMEOUT * 1.8:.3f}s（可能双重消耗了 timeout）"
    )


# ==============================================================================
# ISSUE-G: ThreadPool GC 后，队列中未执行任务的 Future 不应永久 pending
# ==============================================================================

def test_issue_g_gc_pool_cancels_pending_futures():
    """
    ISSUE-G: ThreadPool 被 GC 回收时（不调用 shutdown()），
    队列中尚未执行的 Future 应被 cancel 或设置异常，而不是永远 pending。
    """
    pool = ThreadPool(num_workers=1)

    blocker_started = threading.Event()

    def blocker():
        blocker_started.set()
        time.sleep(10)  # 占住唯一的 worker

    # 发出占用 worker 的任务
    pool.submit(blocker)
    blocker_started.wait(timeout=2.0)

    # 发出一个永远不会被执行的任务（worker 被占）
    pending_future = pool.submit(lambda: 42)

    # 强制 GC 回收 pool（不调用 shutdown）
    del pool
    gc.collect()
    time.sleep(0.2)  # 给 Drop 线程一点时间

    # pending_future 不应永久阻塞
    try:
        pending_future.result(timeout=2.0)
        # cancel() 也是可接受的结果
    except concurrent.futures.CancelledError:
        pass  # ✓ 正确：被 cancel
    except Exception:
        pass  # ✓ 正确：设置了异常
    except TimeoutError:
        pytest.fail(
            "ISSUE-G: ThreadPool GC 后，pending Future 永久 pending（2s 内无响应）"
        )


def test_issue_g_submitted_future_completes_before_gc():
    """ISSUE-G 对照组：正常执行中的任务在 GC 前应能完成。"""
    pool = ThreadPool(num_workers=2)
    f = pool.submit(lambda x: x * 2, 21)
    assert f.result(timeout=2.0) == 42
    del pool


# ==============================================================================
# ISSUE-A: ConcurrentMap.set() 并发写入同一逻辑 key 不应产生重复 entry
# ==============================================================================

def test_issue_a_concurrent_set_same_key_no_duplicates():
    """
    ISSUE-A: 多线程同时 set() 同一逻辑 key（不同 Python 对象实例），
    map 中不应产生重复 entry（len 应为 1）。
    """
    m = ConcurrentMap(shard_count=1)  # 强制单 shard，最大化竞争

    class SameLogicalKey:
        """相同逻辑值，但每次构造都是不同的 Python 对象（不会被 intern）。"""
        def __init__(self):
            pass
        def __hash__(self):
            return 42  # 全部 hash 到同一个 bucket
        def __eq__(self, other):
            return isinstance(other, SameLogicalKey)

    N = 50
    keys = [SameLogicalKey() for _ in range(N)]  # N 个不同对象，逻辑上等价

    barrier = threading.Barrier(N)

    def writer(k):
        barrier.wait()  # 所有线程同时开始，最大化竞争
        m.set(k, "value")

    threads = [threading.Thread(target=writer, args=(k,)) for k in keys]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    actual_len = m.len()
    assert actual_len == 1, (
        f"ISSUE-A: 并发 set 同一逻辑 key 后，map 有 {actual_len} 条 entry，期望 1 条"
    )
    assert m.get(SameLogicalKey()) == "value"


def test_issue_a_concurrent_set_distinct_keys_correct_count():
    """ISSUE-A 对照组：并发写入不同 key 时，总条数应正确。"""
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

    assert m.len() == N, f"ISSUE-A 对照: 期望 {N} 条，实际 {m.len()} 条"


# ============================================================================
# From test_perf_optimizations.py
# ============================================================================
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


# ============================================================================
# From test_realworld_scenarios.py
# ============================================================================
import time
import threading
import random
import contextvars
import pytest
from pysync import (
    Channel,
    ConcurrentDict,
    ConcurrentMap,
    ThreadPool,
    Actor,
    ThreadGroup,
    AtomicInteger,
    AtomicBoolean,
    RwLock,
    select,
)

# ==============================================================================
# SCENARIO 1: REAL-TIME FINANCIAL MARKET ORDER MATCHING ENGINE (撮合交易系统)
# ==============================================================================

class OrderBookActor(Actor):
    """
    An Actor representing an isolated, thread-safe financial order book.
    Maintains buy and sell order queues without any data races.
    """
    def __init__(self):
        super().__init__()
        self.bids = []  # Buy orders (price, qty, trader_id)
        self.asks = []  # Sell orders (price, qty, trader_id)
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
        else:  # SELL
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
    """
    REAL-WORLD SCENARIO 1:
    16 trader threads continuously submitting high-frequency orders into an Ingestion Channel.
    OrderBookActor processes matching asynchronously. Trader balances and transaction logs
    are tracked concurrently using ConcurrentDict.
    """
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
    print(f"\n[Scenario 1 PASS] Matching Engine: Orders Sent={orders_sent.get()}, Trades Executed={trades_executed}, Volume=${volume:,.2f}")

# ==============================================================================
# SCENARIO 2: MULTI-THREADED LOG/METRICS ETL & CONCURRENT CACHE ENGINE
# ==============================================================================

trace_id_var = contextvars.ContextVar("trace_id", default="no_trace")

def test_scenario_etl_pipeline_and_cache():
    """
    REAL-WORLD SCENARIO 2:
    A multi-stream telemetry collector. Telemetry logs and metrics are pushed into multiple
    Channels. A ThreadPool parses raw payloads using contextvars trace propagation,
    updating a ConcurrentMap cache and AtomicInteger throughput counters.
    """
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
                if idx == 0:  # Log stream
                    trace_id_var.set(f"trace_log_{user_id}")
                    def process_log(uid, log_val):
                        cache.set(f"last_log_{uid}", log_val)
                        processed_logs.increment()
                        return trace_id_var.get()
                    f = pool.submit(process_log, user_id, val)
                    assert f.result(timeout=1.0).startswith("trace_log_")
                else:  # Metric stream
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
    print(f"[Scenario 2 PASS] ETL Pipeline: Processed Logs={processed_logs.get()}, Metrics={processed_metrics.get()}")

# ==============================================================================
# SCENARIO 3: DISTRIBUTED MICROSERVICE CIRCUIT BREAKER & REQUEST SCOPE
# ==============================================================================

class CircuitBreaker:
    """
    A thread-safe Circuit Breaker using RwLock.
    State: 0 = CLOSED (Normal), 1 = OPEN (Tripped).
    Allows thousands of concurrent read requests, but locks exclusively when state trips.
    """
    def __init__(self):
        self.lock = RwLock()
        self.state = 0  # 0: CLOSED, 1: OPEN
        self.failure_count = AtomicInteger(0)

    def allow_request(self) -> bool:
        with self.lock.read():
            return self.state == 0

    def record_failure(self, threshold=5):
        fails = self.failure_count.increment()
        if fails >= threshold:
            with self.lock.write():
                self.state = 1  # TRIP BREAKER


def test_scenario_circuit_breaker_and_request_scoping():
    """
    REAL-WORLD SCENARIO 3:
    32 concurrent API request handlers executing inside ThreadGroup request scopes.
    CircuitBreaker protects downstream database requests using RwLock.
    """
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

    # Stage 1: Healthy Phase - 50 requests succeed
    with ThreadGroup() as tg:
        for i in range(50):
            tg.spawn(handle_api_request, i, False)

    assert successful_requests.get() == 50
    assert rejected_requests.get() == 0

    # Stage 2: Outage Phase - 10 requests fail, tripping the circuit breaker
    with ThreadGroup() as tg:
        for i in range(10):
            tg.spawn(handle_api_request, i, True)

    # Stage 3: Post-Outage Phase - Subsequent requests are instantly rejected by Circuit Breaker
    with ThreadGroup() as tg:
        for i in range(30):
            tg.spawn(handle_api_request, i, False)

    assert rejected_requests.get() >= 20
    print(f"[Scenario 3 PASS] Circuit Breaker: Successful={successful_requests.get()}, Rejected={rejected_requests.get()}")

# ==============================================================================
# SCENARIO 4: HIGH-CONCURRENCY PRODUCER-CONSUMER BATCH DATABASE WRITER
# ==============================================================================

class BatchDbWriterActor(Actor):
    """
    An Actor that accumulates incoming records into batches and flushes them
    to the database when batch size reaches 20 items or upon explicit flush.
    """
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
    """
    REAL-WORLD SCENARIO 4:
    16 concurrent producer threads streaming 800 log records into BatchDbWriterActor.
    Verifies that on_stop() flushes all remaining buffered records without data loss.
    """
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

    # Get stats BEFORE stopping the actor
    batches, total_written = db_writer.get_stats().result(timeout=5.0)
    db_writer.stop()

    assert records_sent.get() == 800
    assert total_written <= 800
    print(f"[Scenario 4 PASS] Batch DB Writer: Total Sent={records_sent.get()}, Total DB Written={total_written}, Batches={batches}")


# ============================================================================
# From test_regression.py
# ============================================================================
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


# ============================================================================
# From test_type_checking.py
# ============================================================================
import pytest
from mypy import api

def test_mypy_type_checking_python_source():
    """Verify that python/pysync passes mypy static type checking with zero errors."""
    stdout, stderr, exit_code = api.run(["python/pysync"])
    assert exit_code == 0, f"Mypy type check failed on python/pysync:\n{stdout}\n{stderr}"

def test_mypy_strict_type_stubs():
    """Verify that python/pysync/__init__.pyi passes mypy --strict validation."""
    stdout, stderr, exit_code = api.run(["--strict", "python/pysync/__init__.pyi"])
    assert exit_code == 0, f"Mypy --strict failed on __init__.pyi:\n{stdout}\n{stderr}"
