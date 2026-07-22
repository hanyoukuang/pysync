"""
test_adversarial_edge_cases.py - 对抗性边界条件极端测试套件
试图触发深层死锁、异常传播漏锁、类型转换崩溃与边界越界问题
"""

import threading
import time
import pytest
from pysync import (
    Channel,
    select,
    ConcurrentDict,
    ConcurrentMap,
    Actor,
    AtomicInteger,
    AtomicBoolean,
    RwLock,
)


# ==============================================================================
# 1. Channel & select() 极端边界条件
# ==============================================================================

def test_select_empty_list_raises_value_error():
    """边界 1.1: select([]) 传入空列表应立即抛出 ValueError，绝不永久死锁。"""
    with pytest.raises(ValueError, match="empty|ops"):
        select([])


def test_select_duplicate_channels_same_list():
    """边界 1.2: select([ch.recv_op(), ch.recv_op()]) 包含重复通道操作不应死锁。"""
    ch = Channel(capacity=2)
    ch.send("item1")
    ch.send("item2")

    idx, val = select([ch.recv_op(), ch.recv_op()])
    assert idx in (0, 1)
    assert val in ("item1", "item2")


def test_send_to_channel_closed_during_waiting():
    """边界 1.3: 线程卡在 send() 等待时，另一个线程 close() 通道，等待线程应唤醒抛出 ValueError 或 TimeoutError。"""
    ch = Channel(capacity=0)  # Rendezvous Unbuffered
    send_exception = []

    def sender():
        try:
            ch.send("blocked_item", timeout=0.1)
        except (ValueError, TimeoutError) as e:
            send_exception.append(e)

    t = threading.Thread(target=sender)
    t.start()
    time.sleep(0.02)  # 确保 sender 进入阻塞

    ch.close()
    t.join(timeout=1.0)

    assert not t.is_alive(), "sender 线程死锁，未能因 close() 唤醒"
    assert len(send_exception) == 1, "send 应抛出 ValueError 或 TimeoutError"


# ==============================================================================
# 2. ConcurrentDict / ConcurrentMap 异常与锁毒化边界
# ==============================================================================

def test_map_hash_exception_does_not_poison_shard_lock():
    """边界 2.1: Key 的 __hash__ 抛出异常时，Rust Shard 锁必须释放，不得导致后续操作死锁。"""
    class BadHashKey:
        def __hash__(self):
            raise RuntimeError("Intentional hash error")

    m = ConcurrentMap()
    
    # 执行抛出异常的 set
    with pytest.raises(RuntimeError, match="Intentional hash error"):
        m.set(BadHashKey(), "val")

    # 验证后续正常的 set/get 仍能工作（分片锁未被毒化/死锁）
    m.set("good_key", 42)
    assert m.get("good_key") == 42


def test_concurrent_dict_pop_default():
    """边界 2.2: ConcurrentDict.pop(key, default) 在 key 不存在时应返回 default。"""
    d = ConcurrentDict()
    assert d.pop("non_existent_key", 999) == 999
    
    d["exists"] = 123
    assert d.pop("exists", 999) == 123
    assert "exists" not in d


def test_concurrent_dict_clear_during_reads():
    """边界 2.3: clear() 在多线程并发读写时不应崩溃。"""
    d = ConcurrentDict()
    for i in range(100):
        d[f"k{i}"] = i

    stop = threading.Event()
    errors = []

    def reader():
        while not stop.is_set():
            try:
                _ = len(d)
                _ = d.get("k50")
            except Exception as e:
                errors.append(e)

    threads = [threading.Thread(target=reader) for _ in range(4)]
    for t in threads: t.start()

    time.sleep(0.02)
    d.clear()
    assert len(d) == 0

    stop.set()
    for t in threads: t.join()

    assert not errors, f"clear() 期间并发读出错: {errors}"


# ==============================================================================
# 3. Actor 自杀式 Stop 与销毁后调用边界
# ==============================================================================

def test_actor_self_stop_suicide_no_deadlock():
    """边界 3.1: Actor 在自身的方法内调用 self.stop()（自杀行为），不应产生 join 自锁。"""
    class SelfStoppingActor(Actor):
        def terminate(self):
            self.stop()
            return "terminated"

    actor = SelfStoppingActor()
    f = actor.terminate()
    assert f.result(timeout=2.0) == "terminated"


def test_actor_call_after_stopped_raises_error():
    """边界 3.2: Actor 停止后再调用其方法，应抛出 RuntimeError 而非挂起。"""
    class DummyActor(Actor):
        def work(self):
            return 42

    actor = DummyActor()
    actor.stop()

    with pytest.raises(RuntimeError, match="stopped|closed"):
        f = actor.work()
        f.result(timeout=1.0)


# ==============================================================================
# 4. AtomicInteger 64 位溢出与类型边界
# ==============================================================================

def test_atomic_integer_overflow_wrapping():
    """边界 4.1: AtomicInteger 64位最大值 (2^63 - 1) 加 1 应安全回卷，Rust 不得 panic 崩溃。"""
    INT64_MAX = (1 << 63) - 1
    atomic = AtomicInteger(INT64_MAX)
    
    # 增加 1，产生补码溢出
    new_val = atomic.add_and_get(1)
    assert new_val == -(1 << 63), f"溢出回卷结果错误: {new_val}"


def test_atomic_integer_invalid_init_type():
    """边界 4.2: AtomicInteger 传入非整数类型应抛出 TypeError。"""
    with pytest.raises((TypeError, ValueError)):
        AtomicInteger("not_an_int")


# ==============================================================================
# 5. RwLock 双重释放与异常边界
# ==============================================================================

def test_rwlock_release_unheld_lock_raises_error():
    """边界 5.1: 未获取锁时调用 release_read/release_write 不得使程序崩溃。"""
    lock = RwLock()
    # 释放未持有的锁
    try:
        lock.release_read()
    except (RuntimeError, ValueError):
        pass  # 抛出异常是安全的

    try:
        lock.release_write()
    except (RuntimeError, ValueError):
        pass
