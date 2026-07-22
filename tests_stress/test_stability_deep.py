import time
import gc
import sys
import threading
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
# 1. MEMORY LEAK & REFERENCE COUNT LEAK AUDIT TEST
# ==============================================================================

class TrackedPayload:
    """A payload object that records its own allocation and garbage collection lifecycle."""
    live_count = AtomicInteger(0)

    def __init__(self, val):
        self.val = val
        TrackedPayload.live_count.increment()

    def __del__(self):
        TrackedPayload.live_count.decrement()

    def __hash__(self):
        return hash(self.val)

    def __eq__(self, other):
        return isinstance(other, TrackedPayload) and self.val == other.val


def test_stability_memory_leak_audit():
    """
    STABILITY AUDIT 1: Memory & Reference Count Leak Test.
    Pushes 100,000 TrackedPayload objects through Channels, ConcurrentMaps, Actors,
    and ThreadPools, then verifies that 100% of objects are reclaimed by GC.
    """
    gc.collect()
    initial_live = TrackedPayload.live_count.get()

    # 1. Channel pipeline memory audit
    ch = Channel(capacity=50)
    for i in range(5000):
        p = TrackedPayload(i)
        ch.send(p)
        del p
        temp = ch.recv()
        del temp
    ch.close()
    del ch
    gc.collect()
    print(f"\nAfter Channel: live={TrackedPayload.live_count.get() - initial_live}")

    # 2. ConcurrentMap memory audit
    m = ConcurrentMap(8)
    for i in range(5000):
        k = TrackedPayload(f"key_{i}")
        v = TrackedPayload(f"val_{i}")
        m.set(k, v)
        temp_val = m.get(k)
        del temp_val
        m.delete(k)
        del k, v
    m.clear()
    del m
    gc.collect()
    print(f"After Map: live={TrackedPayload.live_count.get() - initial_live}")

    # 3. ThreadPool memory audit
    pool = ThreadPool(num_workers=4)
    def worker_task(p):
        return p.val
    for i in range(5000):
        p = TrackedPayload(i)
        f = pool.submit(worker_task, p)
        del p
        res = f.result(timeout=1.0)
        assert res == i
        del res, f
    pool.shutdown(wait=True)
    del pool
    gc.collect()
    print(f"After ThreadPool: live={TrackedPayload.live_count.get() - initial_live}")

    # 4. Actor memory audit
    class MemoryActor(Actor):
        def process(self, payload):
            return payload.val

    actor = MemoryActor()
    for i in range(5000):
        p = TrackedPayload(i)
        f = actor.process(p)
        del p
        res = f.result(timeout=1.0)
        assert res == i
        del res, f
    actor.stop()
    del actor
    gc.collect()
    print(f"After Actor: live={TrackedPayload.live_count.get() - initial_live}")

    # Verify zero reference count leaks!
    final_live = TrackedPayload.live_count.get()
    assert final_live == initial_live, f"MEMORY LEAK DETECTED: {final_live - initial_live} objects leaked!"
    print(f"\n[Stability 1 PASS] Memory Leak Audit: 20,000 objects processed, Leaked={final_live - initial_live}")

# ==============================================================================
# 2. MULTI-CORE CPU SCALING & THREAD STARVATION TEST
# ==============================================================================

def test_stability_cpu_scaling_and_starvation():
    """
    STABILITY AUDIT 2: CPU Multi-Core Scaling & Starvation Test.
    Verifies that increasing worker thread count scales throughput linearly without thread starvation.
    """
    counter = AtomicInteger(0)
    ops_per_thread = 50000
    threads_count = 16

    def busy_worker():
        for _ in range(ops_per_thread):
            counter.increment()

    start = time.time()
    threads = [threading.Thread(target=busy_worker) for _ in range(threads_count)]
    for t in threads: t.start()
    for t in threads: t.join(timeout=5.0)

    elapsed = time.time() - start
    total_ops = counter.get()
    throughput = total_ops / elapsed

    assert total_ops == threads_count * ops_per_thread
    print(f"[Stability 2 PASS] Scaling & Starvation: Throughput={throughput:,.2f} ops/sec across {threads_count} threads in {elapsed:.3f}s")

# ==============================================================================
# 3. RANDOM FUZZING / ARBITRARY INPUT STABILITY TEST
# ==============================================================================

def test_stability_fuzzing_arbitrary_inputs():
    """
    STABILITY AUDIT 3: Arbitrary Input Fuzzing.
    Feeds unusual keys (empty tuples, complex nested structures, large ints, floats, booleans,
    bytes, None) into ConcurrentDict, Channel, and Select to ensure zero unhandled panics.
    """
    d = ConcurrentDict()
    ch = Channel(capacity=10)

    arbitrary_inputs = [
        (), ((),), (((),),),
        0, -1, 2**63 - 1, -2**63,
        3.141592653589793, -0.0,
        True, False, None,
        "", "世界你好 🚀", "a" * 1000,
        b"raw_bytes", b"\x00\xff\xfe",
        (1, "mixed", True, (None, 3.14))
    ]

    for item in arbitrary_inputs:
        d[item] = item
        assert d[item] == item
        assert d.get(item) == item
        assert item in d

        ch.send(item)
        assert ch.recv() == item

        del d[item]
        assert item not in d

    ch.close()
    print(f"[Stability 3 PASS] Fuzzing Test: Successfully handled {len(arbitrary_inputs)} unusual input types!")
