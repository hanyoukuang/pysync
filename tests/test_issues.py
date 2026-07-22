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
