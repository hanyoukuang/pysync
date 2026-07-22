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
