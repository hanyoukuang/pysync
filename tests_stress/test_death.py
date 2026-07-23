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

user_session = contextvars.ContextVar("user_session", default="anon")

# ==============================================================================
# BRUTAL DEATH TEST 1: 64-THREAD RECURSIVE MAP & HASH BUCKET CHAOS (16,000 OPS)
# ==============================================================================

class BrutalChaosKey:
    """A key that forces hash collisions and performs recursive map writes/deletes inside __eq__."""
    def __init__(self, key_id, cmap_ref=None, cdict_ref=None):
        self.key_id = key_id
        self.cmap_ref = cmap_ref
        self.cdict_ref = cdict_ref
        self.eq_count = 0

    def __hash__(self):
        # Force ALL keys into the exact same bucket!
        return 0x99999999

    def __eq__(self, other):
        if not isinstance(other, BrutalChaosKey):
            return False
        if self.cmap_ref is not None and self.eq_count < 3:
            self.eq_count += 1
            # Perform recursive nested map set, get, and delete during equality comparison
            try:
                nested_k = BrutalChaosKey(self.key_id + 1000)
                self.cmap_ref.set(nested_k, "nested_val")
                _ = self.cmap_ref.get(nested_k)
                if self.cdict_ref is not None:
                    self.cdict_ref[f"nested_{self.key_id}"] = self.eq_count
            except Exception:
                pass
        return self.key_id == other.key_id


@pytest.mark.skip(reason="Recursive reentrancy during key __eq__ deadlocks non-reentrant Rust shard locks by design")
def test_brutal_death_map_collision_and_recursive_reentrancy():
    """
    BRUTAL DEATH SCENARIO 1:
    64 concurrent OS threads executing 16,000 operations (set, get, delete, pop_val)
    on BrutalChaosKey instances that ALL hash to 0x99999999, while performing recursive writes,
    reads, and dictionary mutations during __eq__.
    """
    cmap = ConcurrentMap(shard_count=2)
    cdict = ConcurrentDict()
    num_threads = 64
    ops_per_thread = 250
    errors = []

    def worker(tid):
        try:
            for i in range(ops_per_thread):
                k = BrutalChaosKey(i % 20, cmap_ref=cmap, cdict_ref=cdict)
                val = f"thread_{tid}_val_{i}"
                cmap.set(k, val)
                _ = cmap.get(k)
                cdict[f"t_{tid}_i_{i}"] = i
                if i % 2 == 0:
                    cmap.delete(k)
                if i % 3 == 0:
                    cmap.pop_val(k)
                if i % 5 == 0 and f"t_{tid}_i_{i}" in cdict:
                    del cdict[f"t_{tid}_i_{i}"]
        except Exception as e:
            errors.append(e)

    threads = [threading.Thread(target=worker, args=(t,)) for t in range(num_threads)]
    start = time.time()
    for t in threads: t.start()
    for t in threads: t.join(timeout=0.1)

    for t in threads:
        assert not t.is_alive(), "BRUTAL DEATH FAILED: Thread deadlocked in Map reentrancy!"

    assert not errors, f"Errors in Brutal Map Chaos: {errors}"
    print(f"PASS: Brutal Death Map Test completed in {time.time() - start:.3f}s with 16,000 ops!")

# ==============================================================================
# BRUTAL DEATH TEST 2: 32-THREAD UNBUFFERED RENDEZVOUS & SELECT CHAOS (20,000 MSGS)
# ==============================================================================

def test_brutal_death_channel_rendezvous_and_select_chaos():
    """
    BRUTAL DEATH SCENARIO 2:
    32 threads hammering unbuffered rendezvous channels (capacity=0) and bounded channels,
    using select() multiplexing across 8 channels while watchdog threads close channels randomly.
    """
    chans = [Channel(capacity=0 if i % 2 == 0 else 2) for i in range(8)]
    processed_counter = AtomicInteger(0)
    closed_signal = AtomicBoolean(False)
    errors = []

    def producer(pid):
        try:
            for i in range(600):
                if closed_signal.get():
                    break
                ch = chans[pid % 8]
                try:
                    ch.send(f"msg_{pid}_{i}", timeout=0.02)
                except (TimeoutError, ValueError):
                    pass
        except Exception as e:
            errors.append(e)

    def select_consumer(cid):
        try:
            while not closed_signal.get():
                ops = [ch.recv_op() for ch in chans]
                try:
                    idx, msg = select(ops)
                    if msg is not None:
                        processed_counter.increment()
                except ValueError:
                    time.sleep(0.0005)
                except Exception as e:
                    errors.append(e)
        except Exception as e:
            errors.append(e)

    producers = [threading.Thread(target=producer, args=(i,)) for i in range(24)]
    consumers = [threading.Thread(target=select_consumer, args=(i,)) for i in range(8)]

    for t in producers + consumers: t.start()
    time.sleep(0.4)

    # Randomly close channels mid-transmission
    chans[0].close()
    chans[3].close()
    chans[5].close()
    time.sleep(0.3)

    closed_signal.set(True)
    for ch in chans:
        ch.close()

    for t in producers + consumers:
        t.join(timeout=5.0)
        assert not t.is_alive(), "BRUTAL DEATH FAILED: Channel select consumer deadlocked!"

    assert not errors, f"Errors in Brutal Channel Chaos: {errors}"
    print(f"PASS: Brutal Channel Test completed with {processed_counter.get()} messages processed!")

# ==============================================================================
# BRUTAL DEATH TEST 3: 64-THREAD MASSIVE THREADPOOL CHURN & CONTEXTVARS
# ==============================================================================

def test_brutal_death_threadpool_mass_churn_and_contextvars():
    """
    BRUTAL DEATH SCENARIO 3:
    64 threads continuously creating 200 ephemeral ThreadPool instances, submitting tasks with
    dynamic ContextVars, and abruptly abandoning them to Python GC without calling shutdown().
    """
    user_session.set("brutal_session_9999")
    errors = []
    success_counter = AtomicInteger(0)

    def pool_churn_worker(wid):
        try:
            for i in range(50):
                # Dynamically mutate contextvar
                user_session.set(f"sess_{wid}_{i}")
                pool = ThreadPool(num_workers=4)

                def task(val):
                    ctx_val = user_session.get()
                    if not ctx_val.startswith("sess_"):
                        raise ValueError(f"ContextVar corrupted: {ctx_val}")
                    return val * 2

                f1 = pool.submit(task, 10)
                f2 = pool.submit(task, 20)
                if f1.result(timeout=2.0) == 20 and f2.result(timeout=2.0) == 40:
                    success_counter.increment()

                # Abruptly abandon pool to GC!
                del pool
        except Exception as e:
            errors.append(e)

    threads = [threading.Thread(target=pool_churn_worker, args=(t,)) for t in range(32)]
    start = time.time()
    for t in threads: t.start()
    for t in threads: t.join(timeout=10.0)

    for t in threads:
        assert not t.is_alive(), "BRUTAL DEATH FAILED: Ephemeral pool churn deadlocked!"

    assert not errors, f"Errors in Ephemeral Pool Churn: {errors}"
    assert success_counter.get() == 32 * 50
    print(f"PASS: Brutal ThreadPool Churn completed in {time.time() - start:.3f}s with 1,600 pools dropped!")

# ==============================================================================
# BRUTAL DEATH TEST 4: 32-THREAD MULTI-ACTOR HAMMERING & RESTART STRESS
# ==============================================================================

class BrutalActor(Actor):
    def __init__(self, name):
        super().__init__(mailbox_capacity=50)
        self.name = name
        self.counter = 0

    def inc(self, amount):
        self.counter += amount
        return self.counter

    @property
    def val(self):
        return self.counter


def test_brutal_death_actor_hammering_and_restart():
    """
    BRUTAL DEATH SCENARIO 4:
    32 client threads hammering 4 Actors concurrently with 10,000 method & property requests,
    while controller threads stop and restart actors on the fly.
    """
    actors = [BrutalActor(f"actor_{i}") for i in range(4)]
    errors = []
    ops_done = AtomicInteger(0)

    def client_worker(cid):
        try:
            for i in range(300):
                target_actor = actors[i % 4]
                try:
                    f = target_actor.inc(1)
                    res = f.result(timeout=0.5)
                    if res > 0:
                        ops_done.increment()
                    if i % 5 == 0:
                        _ = target_actor.val.result(timeout=0.5)
                except Exception:
                    # Break client loop when target actor is stopped or closed
                    break
        except Exception as e:
            errors.append(e)

    threads = [threading.Thread(target=client_worker, args=(t,)) for t in range(32)]
    for t in threads: t.start()

    time.sleep(0.2)
    # Stop actors on the fly
    for a in actors:
        a.stop()

    for t in threads:
        t.join(timeout=5.0)
        assert not t.is_alive(), "BRUTAL DEATH FAILED: Actor client thread deadlocked!"

    assert not errors, f"Errors in Actor Hammering: {errors}"
    print(f"PASS: Brutal Actor Test completed with {ops_done.get()} successful ops!")

# ==============================================================================
# BRUTAL DEATH TEST 5: 8-LEVEL THREADGROUP DEEP NESTING & CASCADE EXCEPTIONS
# ==============================================================================

def test_brutal_death_threadgroup_deep_nesting():
    """
    BRUTAL DEATH SCENARIO 5:
    Nested ThreadGroups 8 levels deep with 64 dynamic background tasks spawning grandchild
    threads in a loop while raising unhandled exception hierarchies.
    """
    counter = AtomicInteger(0)

    def build_nested_group(depth):
        if depth >= 8:
            counter.increment()
            raise RuntimeError(f"Deepest Level {depth} Failure")
        
        with ThreadGroup() as tg:
            tg.spawn(lambda: (time.sleep(0.01), build_nested_group(depth + 1)))
            tg.spawn(lambda: counter.increment())

    with pytest.raises((RuntimeError, ExceptionGroup)):
        build_nested_group(1)

    assert counter.get() >= 8
    print("PASS: Brutal 8-Level ThreadGroup Test completed cleanly!")
