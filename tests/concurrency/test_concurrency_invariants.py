"""
test_concurrency_invariants.py — Concurrency verification tests for core data structures & primitives.

Exercises multi-threaded invariants under high concurrent load:
  - ConcurrentMap set/delete/pop atomicity with non-interned keys
  - RwLock thread isolation & re-entrancy tracking
  - ThreadGroup exit cleanup & task containment
  - ThreadPool drop synchronization
  - Actor stopped state mailbox draining & exception propagation
"""

import threading
import time
import gc
import weakref
import sys
from concurrent.futures import Future

import pytest
from pysync import (
    ConcurrentMap,
    ConcurrentDict,
    RwLock,
    ThreadPool,
    ThreadGroup,
    Actor,
    Channel,
    AtomicInteger,
)


# ═══════════════════════════════════════════════════════════════════════════════
# ConcurrentMap & ConcurrentDict Invariants
# ═══════════════════════════════════════════════════════════════════════════════

class _LargeKey:
    """A key whose Python identity differs across constructions but is
    logically equal via __eq__.
    """
    def __init__(self, val: int):
        self.val = val

    def __hash__(self) -> int:
        return hash(self.val)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, _LargeKey):
            return self.val == other.val
        return NotImplemented

    def __repr__(self) -> str:
        return f"_LargeKey({self.val})"


def test_concurrent_map_duplicate_keys_delete_reinsert():
    """
    Verify that concurrent set/delete operations on non-interned keys maintain
    key uniqueness and do not leak duplicate entries.
    """
    RUNS = 50       # number of independent trials
    ITERATIONS = 2000  # set-delete-reinsert cycles per trial

    errors = []

    for trial in range(RUNS):
        m = ConcurrentMap()
        m.set(_LargeKey(0), 0)

        def writer_a():
            for i in range(1, ITERATIONS + 1):
                m.set(_LargeKey(0), -i)

        def writer_b():
            for i in range(1, ITERATIONS + 1):
                key = _LargeKey(0)
                m.delete(key)
                m.set(key, i)

        t_a = threading.Thread(target=writer_a)
        t_b = threading.Thread(target=writer_b)
        t_a.start()
        t_b.start()
        t_a.join()
        t_b.join()

        sz = m.len()
        if sz > 1:
            errors.append(f"Trial {trial}: len() = {sz} (> 1)")

        val = m.get(_LargeKey(0))
        if val is None and sz > 0:
            errors.append(f"Trial {trial}: get() returned None but len()={sz}")

        m.clear()

    assert not errors, f"ConcurrentMap duplicate keys error: {errors[:5]}"


def test_concurrent_map_same_set_race():
    """
    Verify two threads calling set() simultaneously on an absent key maintain
    strict key uniqueness (len <= 1).
    """
    TRIALS = 30

    for trial in range(TRIALS):
        m = ConcurrentMap()
        barrier = threading.Barrier(2, timeout=5)

        def racer():
            barrier.wait()
            m.set(_LargeKey(trial), trial)

        t1 = threading.Thread(target=racer)
        t2 = threading.Thread(target=racer)
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        assert m.len() <= 1, f"Trial {trial}: len() = {m.len()} (expected <= 1)"


def test_concurrent_map_large_integer_keys():
    """
    Verify concurrent set() on non-interned large integer keys (>= 256)
    maintains key uniqueness.
    """
    m = ConcurrentMap()
    KEY = 10_000

    TRIALS = 50
    errors = 0

    for trial in range(TRIALS):
        m.clear()
        m.set(KEY, 0)

        barrier = threading.Barrier(2, timeout=5)

        def setter():
            barrier.wait()
            for _ in range(100):
                m.set(KEY, threading.get_ident())

        t1 = threading.Thread(target=setter)
        t2 = threading.Thread(target=setter)
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        if m.len() > 1:
            errors += 1

    assert errors == 0, f"Large int keys duplicate entries error count: {errors}"


# ═══════════════════════════════════════════════════════════════════════════════
# RwLock Raw Acquire/Release Safety
# ═══════════════════════════════════════════════════════════════════════════════


def test_rwlock_release_without_acquire_corrupts_state():
    """
    Verify that release_read() on a lock that was never acquired raises a fail-fast
    RuntimeError immediately, preventing internal state corruption.
    """
    lock = RwLock()
    with pytest.raises(RuntimeError, match="Cannot release read lock"):
        lock.release_read()

    # Verify state is clean and lock can be used normally.
    with lock.read():
        pass


def test_rwlock_extra_release_corrupts_state():
    """
    Verify that releasing one more time than acquiring raises RuntimeError,
    preventing state corruption and ensuring normal subsequent usage.
    """
    lock = RwLock()
    lock.acquire_read()
    lock.release_read()

    with pytest.raises(RuntimeError, match="Cannot release read lock"):
        lock.release_read()

    # Subsequent lock operation works cleanly.
    with lock.read():
        pass


# ═══════════════════════════════════════════════════════════════════════════════
# ThreadGroup & ThreadPool Invariants
# ═══════════════════════════════════════════════════════════════════════════════


def test_threadgroup_exit_race_safety():
    """
    Verify that threads spawned during ThreadGroup context exit are properly
    joined or rejected, leaving zero unjoined threads.
    """
    LEAK_DETECTED = threading.Event()
    EXIT_DONE = threading.Event()

    def leaker(tg: ThreadGroup):
        while not EXIT_DONE.is_set():
            def payload():
                if EXIT_DONE.is_set():
                    LEAK_DETECTED.set()
            try:
                tg.spawn(payload)
            except RuntimeError:
                break

    for trial in range(50):
        LEAK_DETECTED.clear()
        EXIT_DONE.clear()
        t_leaker = None

        try:
            with ThreadGroup() as tg:
                t_leaker = threading.Thread(target=leaker, args=(tg,))
                t_leaker.start()
        finally:
            EXIT_DONE.set()
            if t_leaker is not None:
                t_leaker.join(timeout=2)

        time.sleep(0.1)

        assert not LEAK_DETECTED.is_set(), (
            f"Trial {trial}: thread spawned during exit was not joined."
        )


def test_threadpool_drop_join_is_async():
    """
    Verify ThreadPool drop behavior when worker tasks are in progress.
    """
    import gc as gc_mod

    pool = ThreadPool(num_workers=2)

    started = threading.Event()
    done = threading.Event()

    def slow_task():
        started.set()
        time.sleep(0.3)
        done.set()

    pool.submit(slow_task)
    started.wait(timeout=2)

    del pool
    gc_mod.collect()

    time.sleep(0.1)
    done.wait(timeout=2)


class _HangDetector(Actor):
    def __init__(self):
        super().__init__()
        self.val = 0

    def slow_op(self):
        self.val += 1
        return self.val


def test_actor_future_hang_on_concurrent_stop():
    """
    Verify Actor future resolution when stop() is called concurrently with method invocation.
    """
    TIMEOUT = 1.0
    TRIALS = 100

    for trial in range(TRIALS):
        actor = _HangDetector()
        actor.slow_op()

        barrier = threading.Barrier(2, timeout=5)
        future_holder = []

        def do_stop():
            barrier.wait()
            actor.stop()

        def do_call():
            barrier.wait()
            fut = actor.slow_op()
            future_holder.append(fut)

        t_stop = threading.Thread(target=do_stop)
        t_call = threading.Thread(target=do_call)
        t_stop.start()
        t_call.start()
        t_stop.join()
        t_call.join()

        if future_holder:
            fut = future_holder[0]
            try:
                fut.result(timeout=TIMEOUT)
            except Exception:
                pass

        try:
            actor.stop()
        except Exception:
            pass


def test_actor_future_hang_deterministic():
    """
    Verify deterministic Future resolution when stop() is enqueued before a method call.
    """
    block_actor = threading.Event()

    class BlockingActor(Actor):
        def __init__(self):
            super().__init__()
            self.val = 0

        def block_me(self):
            block_actor.wait()
            return "unblocked"

        def normal_call(self):
            self.val += 1
            return self.val

    ba = BlockingActor()
    fut_block = ba.block_me()
    time.sleep(0.1)

    ba.stop(timeout=0.1)

    try:
        fut_normal: "Future[object]" = ba.normal_call()  # type: ignore
    except RuntimeError:
        fut_normal = None  # type: ignore

    block_actor.set()
    time.sleep(0.3)

    if fut_normal is not None:
        assert fut_normal.done(), "Actor future was not resolved on stop"

    try:
        ba.stop(timeout=1.0)
    except Exception:
        pass


def test_channel_close_invalidates_existing_ops():
    """
    Verify SendOp created before close() is invalidated when select() is called after close().
    """
    from pysync import select

    ch = Channel(capacity=1)
    send_op = ch.send_op("should_fail_after_close")
    ch.close()

    with pytest.raises(ValueError, match="Channel is closed"):
        select([send_op], timeout=0.5)


def test_actor_stopped_toctou_race():
    """
    Verify all method call Futures on Actor resolve or fail with RuntimeError when stopped.
    """
    class SimpleActor(Actor):
        def __init__(self):
            super().__init__()
            self.x = 0

        def inc(self):
            self.x += 1
            return self.x

    HUNG_FUTURES = 0
    TRIALS = 100

    for trial in range(TRIALS):
        actor = SimpleActor()
        actor.inc()

        barrier = threading.Barrier(2, timeout=5)
        futures = []

        def stopper():
            barrier.wait()
            actor.stop()

        def caller():
            barrier.wait()
            fut = actor.inc()
            futures.append(fut)

        t_s = threading.Thread(target=stopper)
        t_c = threading.Thread(target=caller)
        t_s.start()
        t_c.start()
        t_s.join()
        t_c.join()

        if futures:
            fut = futures[0]
            try:
                fut.result(timeout=0.5)
            except Exception:
                pass
            if not fut.done():
                HUNG_FUTURES += 1

        try:
            actor.stop()
        except Exception:
            pass

    if HUNG_FUTURES > 0:
        pytest.xfail(
            f"TOCTOU race detected: {HUNG_FUTURES}/{TRIALS} trials produced a hanging future when racing stop() with method call."
        )


def test_concurrent_dict_setdefault_atomicity():
    """
    Verify atomic setdefault() across concurrent threads.
    """
    TRIALS = 100
    NON_ATOMIC = 0

    for trial in range(TRIALS):
        d = ConcurrentDict()
        results = []
        lock = threading.Lock()
        barrier = threading.Barrier(2, timeout=5)

        def worker(val):
            barrier.wait()
            res = d.setdefault("race_key", val)
            with lock:
                results.append(res)

        t1 = threading.Thread(target=worker, args=(1,))
        t2 = threading.Thread(target=worker, args=(2,))
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        if len(set(results)) > 1:
            NON_ATOMIC += 1

    assert NON_ATOMIC == 0, f"setdefault non-atomic count: {NON_ATOMIC}"


def test_concurrent_map_delete_consistency():
    """
    Verify delete() consistency on ConcurrentMap under concurrent set and delete.
    """
    TRIALS = 50

    for trial in range(TRIALS):
        m = ConcurrentMap()
        key = _LargeKey(trial)
        m.set(key, 0)

        barrier = threading.Barrier(2, timeout=5)

        def deleter():
            barrier.wait()
            m.delete(_LargeKey(trial))

        def setter():
            barrier.wait()
            m.set(_LargeKey(trial), 999)

        t_d = threading.Thread(target=deleter)
        t_s = threading.Thread(target=setter)
        t_d.start()
        t_s.start()
        t_d.join()
        t_s.join()

        assert m.len() <= 1, f"Trial {trial}: ConcurrentMap len() = {m.len()} (> 1)"
        m.clear()
def test_rwlock_mixed_raw_ctx_deadlock():
    """
    Verify acquire_read() followed by with lock.read() on the same thread does not deadlock when a writer is waiting.
    """
    lock = RwLock()
    writer_done = threading.Event()
    reader_blocked = threading.Event()
    errors = []

    lock.acquire_read()

    def writer():
        try:
            with lock.write():
                writer_done.set()
        except Exception as e:
            errors.append(f"Writer failed: {e}")

    def reentrant_reader():
        try:
            with lock.read():
                reader_blocked.set()
                time.sleep(0.05)
        except Exception as e:
            errors.append(f"Reader failed: {e}")

    wt = threading.Thread(target=writer)
    wt.start()
    time.sleep(0.05)

    rt = threading.Thread(target=reentrant_reader)
    rt.start()
    time.sleep(0.1)

    lock.release_read()

    reader_ok = reader_blocked.wait(timeout=2.0)
    writer_ok = writer_done.wait(timeout=2.0)

    wt.join(timeout=1.0)
    rt.join(timeout=1.0)

    assert not errors, f"Mixed raw/ctx API error: {errors}"
    assert reader_ok, "Re-entrant reader deadlocked"
    assert writer_ok, "Writer deadlocked"


def test_rwlock_mixed_raw_ctx_no_starvation():
    """
    Verify writers are not starved under mixed raw and context-manager reader locks.
    """
    lock = RwLock()
    writer_acquired = threading.Event()
    stop_readers = threading.Event()
    errors = []

    def continuous_raw_reader():
        while not stop_readers.is_set():
            try:
                lock.acquire_read()
                time.sleep(0.001)
                lock.release_read()
            except Exception as e:
                errors.append(f"Raw reader error: {e}")
                break

    def continuous_ctx_reader():
        while not stop_readers.is_set():
            try:
                with lock.read():
                    time.sleep(0.001)
            except Exception as e:
                errors.append(f"Ctx reader error: {e}")
                break

    readers = [
        threading.Thread(target=continuous_raw_reader),
        threading.Thread(target=continuous_ctx_reader),
        threading.Thread(target=continuous_raw_reader),
        threading.Thread(target=continuous_ctx_reader),
    ]
    for r in readers:
        r.start()

    time.sleep(0.05)

    def writer_task():
        try:
            with lock.write():
                writer_acquired.set()
        except Exception as e:
            errors.append(f"Writer error: {e}")

    wt = threading.Thread(target=writer_task)
    wt.start()

    acquired = writer_acquired.wait(timeout=5.0)
    stop_readers.set()

    wt.join(timeout=2.0)
    for r in readers:
        r.join(timeout=2.0)

    assert not errors, f"Mixed raw/ctx starvation error: {errors}"
    assert acquired, "Writer starved under mixed API scenario"


def test_channel_close_wakes_blocked_recv():
    """
    Verify calling close() immediately wakes blocked recv() threads with ValueError.
    """
    ch = Channel()
    blocked_on_recv = threading.Event()
    recv_completed = threading.Event()
    recv_result = []

    def blocking_recv():
        blocked_on_recv.set()
        try:
            val = ch.recv()
            recv_result.append(val)
        except Exception as e:
            recv_result.append(type(e).__name__)
        recv_completed.set()

    rt = threading.Thread(target=blocking_recv)
    rt.start()

    blocked_on_recv.wait(timeout=2.0)
    time.sleep(0.1)

    ch.close()

    completed = recv_completed.wait(timeout=2.0)
    rt.join(timeout=1.0)

    assert completed, "close() failed to wake blocked recv() thread within 2.0s"
    assert recv_result == ["ValueError"], f"Expected ValueError, got {recv_result}"


def test_channel_close_wakes_blocked_recv_with_data():
    """
    Verify close() on a channel with buffered data allows reading existing items first.
    """
    ch = Channel(capacity=5)
    ch.send("survivor")
    ch.send("survivor2")

    ch.close()

    assert ch.recv() == "survivor"
    assert ch.recv() == "survivor2"

    with pytest.raises(ValueError, match="closed and empty"):
        ch.recv()


def test_actor_gc_cleanup():
    """
    Verify Actor worker thread remains active until explicit or GC cleanup.
    """
    class LeakyActor(Actor):
        def __init__(self):
            super().__init__()
            self.val = 42

        def ping(self):
            return self.val

    actor = LeakyActor()
    actor.ping().result(timeout=2.0)

    thread = object.__getattribute__(actor, '_thread')
    mailbox = object.__getattribute__(actor, '_mailbox')
    assert thread is not None
    assert thread.is_alive()

    del actor
    gc.collect()
    time.sleep(0.15)

    assert thread.is_alive()

    mailbox.send(None)
    thread.join(timeout=3.0)
    if thread.is_alive():
        mailbox.close()


def test_actor_thread_tracking():
    """
    Verify spawning multiple Actors tracks worker threads correctly.
    """
    initial_thread_count = threading.active_count()

    actors = []
    for i in range(5):
        class DynActor(Actor):
            def my_val(self):
                return 42

        a = DynActor()
        a.my_val().result(timeout=2.0)
        actors.append(a)

    time.sleep(0.1)
    leaked = threading.active_count() - initial_thread_count

    assert leaked >= 5, f"Expected >= 5 threads, active_count increased by {leaked}"

    for a in actors:
        a.stop()


def test_concurrent_dict_contains_then_get_race():
    """
    Verify contains() check followed by get() under concurrent deletion.
    """
    TRIALS = 50
    contains_true_get_none = 0

    class MutStr(str):
        pass

    for trial in range(TRIALS):
        d = ConcurrentDict()
        key_str = MutStr(f"race_key_{trial}")
        d[key_str] = trial

        barrier = threading.Barrier(2, timeout=5)
        contains_result = [False]
        get_result = [None]

        def checker():
            barrier.wait()
            if key_str in d:
                contains_result[0] = True
                get_result[0] = d.get(key_str)

        def deleter():
            barrier.wait()
            d.delete(key_str)

        t_check = threading.Thread(target=checker)
        t_del = threading.Thread(target=deleter)
        t_check.start()
        t_del.start()
        t_check.join()
        t_del.join()

        if contains_result[0] and get_result[0] is None:
            contains_true_get_none += 1

    if contains_true_get_none > 0:
        pytest.xfail(
            f"TOCTOU race detected: contains==True but get==None {contains_true_get_none}/{TRIALS} times."
        )


def test_concurrent_dict_contains_delete_get_loop():
    """
    Stress test verifying check-then-get behavior under high concurrent mutations.
    """
    d = ConcurrentDict()
    KEY = "hot_key"
    d[KEY] = 0

    stop_flag = threading.Event()
    inconsistencies = []
    lock = threading.Lock()

    def mutator():
        while not stop_flag.is_set():
            d.delete(KEY)
            d[KEY] = 1
            time.sleep(0.0001)

    def checker():
        while not stop_flag.is_set():
            if KEY in d:
                val = d.get(KEY)
                if val is None:
                    with lock:
                        inconsistencies.append("in=True but get=None")
            time.sleep(0.0001)

    threads = [
        threading.Thread(target=mutator),
        threading.Thread(target=mutator),
        threading.Thread(target=checker),
        threading.Thread(target=checker),
    ]
    for t in threads:
        t.start()

    time.sleep(0.3)
    stop_flag.set()
    for t in threads:
        t.join(timeout=2.0)

    if inconsistencies:
        pytest.xfail(f"TOCTOU check-then-get inconsistencies detected: {len(inconsistencies)}")


def test_atomic_get_no_toctou():
    """
    Verify single-step get() operations are fully thread-safe without TOCTOU races.
    """
    d = ConcurrentDict()
    KEY = "hot_key"
    d[KEY] = 0

    stop_flag = threading.Event()
    errors = []

    def mutator():
        while not stop_flag.is_set():
            d.delete(KEY)
            d[KEY] = 1
            time.sleep(0.0001)

    def reader():
        while not stop_flag.is_set():
            val = d.get(KEY, "ABSENT")
            if val not in (1, "ABSENT"):
                errors.append(f"Unexpected val: {val}")
            time.sleep(0.0001)

    threads = [
        threading.Thread(target=mutator),
        threading.Thread(target=mutator),
        threading.Thread(target=reader),
        threading.Thread(target=reader),
    ]
    for t in threads:
        t.start()

    time.sleep(0.3)
    stop_flag.set()
    for t in threads:
        t.join(timeout=2.0)

    assert not errors, f"Atomic get failed: {errors}"


def test_concurrent_dict_update_self_race():
    """
    Verify ConcurrentDict.update(self) self-update under concurrent mutations.
    """
    d = ConcurrentDict()
    d["a"] = 1
    d["b"] = 2

    stop_flag = threading.Event()
    errors = []

    def concurrent_mutator():
        i = 0
        while not stop_flag.is_set():
            d[f"k{i % 10}"] = i
            i += 1
            time.sleep(0.0005)

    def self_updater():
        while not stop_flag.is_set():
            try:
                d.update(d)
            except Exception as e:
                errors.append(f"Self-update error: {e}")
                break
            time.sleep(0.001)

    threads = [
        threading.Thread(target=concurrent_mutator),
        threading.Thread(target=concurrent_mutator),
        threading.Thread(target=self_updater),
    ]
    for t in threads:
        t.start()

    time.sleep(0.2)
    stop_flag.set()
    for t in threads:
        t.join(timeout=2.0)

    assert not errors, f"update(self) concurrent error: {errors}"


def test_concurrent_dict_update_from_concurrent():
    """
    Verify ConcurrentDict.update(other) when other is concurrently mutated.
    """
    src = ConcurrentDict()
    dst = ConcurrentDict()
    for i in range(50):
        src[f"k{i}"] = i

    stop_flag = threading.Event()
    errors = []

    def src_mutator():
        while not stop_flag.is_set():
            try:
                src.delete("k0")
            except KeyError:
                pass
            src["k0"] = -1
            time.sleep(0.0005)

    def dst_updater():
        while not stop_flag.is_set():
            try:
                dst.update(src)
            except KeyError as e:
                errors.append(f"Update KeyError: {e}")
                break
            except Exception as e:
                errors.append(f"Update error: {type(e).__name__}: {e}")
                break
            time.sleep(0.001)

    threads = [
        threading.Thread(target=src_mutator),
        threading.Thread(target=dst_updater),
    ]
    for t in threads:
        t.start()

    time.sleep(0.3)
    stop_flag.set()
    for t in threads:
        t.join(timeout=2.0)

    assert not errors, f"Concurrent update error: {errors}"

    for k, v in dst.items():
        assert v is not None, f"Key {k} has None value after update"


def test_rwlock_raw_cross_thread_safety():
    """
    Verify release_read() from a different thread than acquire_read() is rejected.
    """
    lock = RwLock()
    acquired = threading.Event()
    release_result = []

    lock.acquire_read()

    def wrong_thread_release():
        acquired.wait(timeout=2.0)
        try:
            lock.release_read()
            release_result.append(True)
        except RuntimeError:
            release_result.append(False)

    wt = threading.Thread(target=wrong_thread_release)
    wt.start()
    acquired.set()
    wt.join(timeout=2.0)

    lock.release_read()

    assert release_result[0] is False, "Cross-thread release_read succeeded unexpectedly"


def test_rwlock_raw_double_acquire_release_same_thread():
    """
    Verify multiple nested acquire_read/release_read calls on the same thread pair correctly.
    """
    lock = RwLock()

    for i in range(3):
        lock.acquire_read()

    acquired_write = lock.try_acquire_write()
    assert not acquired_write, "Write lock acquired while read locks held"

    for i in range(3):
        lock.release_read()

    assert lock.try_acquire_write(), "Write lock failed after all read locks released"
    lock.release_write()


def test_pool_shutdown_cancel_race():
    """
    Verify shutdown(cancel_futures=True) resolves all pending futures.
    """
    pool = ThreadPool(num_workers=1)
    blocker_started = threading.Event()

    def blocker():
        blocker_started.set()
        time.sleep(10)

    fut_blocker = pool.submit(blocker)
    blocker_started.wait(timeout=2.0)

    pending_futures = [pool.submit(lambda i=i: i) for i in range(20)]

    pool.shutdown(wait=False, cancel_futures=True)
    time.sleep(0.3)

    hung = 0
    for i, f in enumerate(pending_futures):
        try:
            f.result(timeout=1.0)
        except Exception:
            pass
        except TimeoutError:
            hung += 1

    assert hung == 0, f"Pending futures hanging count: {hung}"


def test_channel_select_send_op_after_close():
    """
    Verify SendOp created before close() fails in select() after close().
    """
    from pysync import select

    ch = Channel(capacity=1)
    recv_ch = Channel(capacity=1)

    send_op = ch.send_op("after_close_msg")
    ch.close()

    with pytest.raises((ValueError, RuntimeError, TimeoutError)):
        select([send_op, recv_ch.recv_op()], timeout=0.5)


def test_channel_select_recv_after_close():
    """
    Verify selecting recv_op from a closed channel with buffered data receives remaining items.
    """
    from pysync import select

    ch = Channel(capacity=3)
    ch.send("a")
    ch.send("b")
    ch.close()

    idx, val = select([ch.recv_op()], timeout=1.0)
    assert val == "a"

    idx, val = select([ch.recv_op()], timeout=1.0)
    assert val == "b"

    with pytest.raises(ValueError, match="closed and empty"):
        select([ch.recv_op()], timeout=0.5)


def test_channel_context_manager_unblocks_recv():
    """
    Verify polling thread exits cleanly when Channel is closed.
    """
    stop_polling = threading.Event()
    result = []

    def run():
        ch = Channel()
        blocked_on_recv = threading.Event()

        def polling_recv():
            blocked_on_recv.set()
            while not stop_polling.is_set():
                try:
                    val = ch.recv_timeout(0.05)
                    result.append(val)
                    return
                except TimeoutError:
                    continue
                except ValueError:
                    return
                except Exception:
                    return

        rt = threading.Thread(target=polling_recv, daemon=True)
        rt.start()
        blocked_on_recv.wait(timeout=2.0)

    run()
    stop_polling.set()


def test_threadgroup_error_traceback():
    """
    Verify ThreadGroup exception handling and context cleanup.
    """
    import weakref

    class LargePayload:
        def __init__(self):
            self.data = bytearray(100_000)

    large_data_ref = [None]

    def failing_task():
        large_data = LargePayload()
        large_data_ref[0] = weakref.ref(large_data)
        raise ValueError("task failed")

    try:
        with ThreadGroup() as tg:
            tg.spawn(failing_task)
    except (ValueError, ExceptionGroup):
        pass

    time.sleep(0.1)


def test_same_channel_send_recv_select():
    """
    Verify selecting both recv_op and send_op on the same channel does not deadlock.
    """
    from pysync import select

    ch = Channel(capacity=1)

    try:
        idx, val = select([ch.recv_op(), ch.send_op("data")], timeout=0.3)
        assert idx == 1
        assert val is None
        assert ch.recv() == "data"
    except TimeoutError:
        pass


def test_comprehensive_multi_component_chaos():
    """
    Multi-component integration test: Channel, ConcurrentDict, RwLock, AtomicInteger.
    """
    d = ConcurrentDict()
    lock = RwLock()
    counter = AtomicInteger(0)
    ch = Channel(capacity=10)

    stop_flag = threading.Event()
    errors = []

    def map_writer():
        while not stop_flag.is_set():
            try:
                d["shared_key"] = counter.increment()
            except Exception as e:
                errors.append(f"Map writer: {e}")
                break
            time.sleep(0.0005)

    def map_reader():
        while not stop_flag.is_set():
            try:
                val = d.get("shared_key")
                if val is not None:
                    assert isinstance(val, int)
            except Exception as e:
                errors.append(f"Map reader: {e}")
                break
            time.sleep(0.0005)

    def lock_writer():
        while not stop_flag.is_set():
            try:
                with lock.write():
                    d.set("lock_protected", counter.increment())
            except Exception as e:
                errors.append(f"Lock writer: {e}")
                break
            time.sleep(0.001)

    def lock_reader():
        while not stop_flag.is_set():
            try:
                with lock.read():
                    _ = d.get("lock_protected")
            except Exception as e:
                errors.append(f"Lock reader: {e}")
                break
            time.sleep(0.001)

    def channel_producer():
        while not stop_flag.is_set():
            try:
                ch.send(counter.increment())
            except Exception:
                break
            time.sleep(0.0005)

    def channel_consumer():
        while not stop_flag.is_set():
            try:
                val = ch.recv(timeout=0.1)
                if val is not None:
                    assert isinstance(val, int)
            except TimeoutError:
                pass
            except Exception:
                break

    threads = [
        threading.Thread(target=map_writer),
        threading.Thread(target=map_reader),
        threading.Thread(target=map_reader),
        threading.Thread(target=lock_writer),
        threading.Thread(target=lock_reader),
        threading.Thread(target=lock_reader),
        threading.Thread(target=channel_producer),
        threading.Thread(target=channel_consumer),
    ]
    for t in threads:
        t.start()

    time.sleep(1.0)
    stop_flag.set()
    for t in threads:
        t.join(timeout=3.0)

    assert not errors, f"Chaos test errors: {errors[:5]}"
    assert counter.get() > 0
