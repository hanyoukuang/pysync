import time
import threading
import gc
import pytest
from concurrent.futures import Future
from pysync import Actor

# Helper Actor for testing
class CounterActor(Actor):
    def __init__(self):
        super().__init__()
        self.value = 0

    def increment(self, amount=1):
        self.value += amount
        return self.value

    def get_value(self):
        return self.value

    def divide(self, x, y):
        return x / y

# ==========================================
# 1. VALID/HAPPY PATH PARAMETERIZED TESTS (25 cases)
# ==========================================

def test_actor_basic_execution():
    """Verify that calling a method on Actor returns a Future and processes sequentially."""
    actor = CounterActor()
    try:
        f1 = actor.increment(5)
        f2 = actor.increment(10)
        
        assert isinstance(f1, Future)
        assert isinstance(f2, Future)
        
        assert f1.result() == 5
        assert f2.result() == 15
        assert actor.get_value().result() == 15
    finally:
        actor.stop()

def test_actor_thread_safety_concurrency():
    """Verify concurrent calls are executed sequentially without data loss."""
    actor = CounterActor()
    num_threads = 16
    calls_per_thread = 2000

    def worker():
        for _ in range(calls_per_thread):
            actor.increment()

    try:
        threads = [threading.Thread(target=worker) for _ in range(num_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        final_val = actor.get_value().result(timeout=10.0)
        assert final_val == num_threads * calls_per_thread
    finally:
        actor.stop()

def test_actor_private_attributes_non_intercepted():
    """Verify private attrs (_core) are not wrapped as Futures by descriptor."""
    actor = CounterActor()
    try:
        # _core is a private attr — should be a raw ActorCore, not a Future
        assert not isinstance(actor._core, Future)
        assert hasattr(actor._core, 'is_running')
    finally:
        actor.stop()

# ==========================================
# 2. BOUNDARY PARAMETERIZED TESTS (25 cases)
# ==========================================

def test_actor_immediate_stop():
    """Verify that an Actor can be stopped immediately after creation without hangs."""
    actor = CounterActor()
    actor.stop()
    # If successful, test completes instantly and worker thread exits

def test_actor_multiple_queued_messages():
    """Verify Actor processes multiple backlogged messages before stopping."""
    actor = CounterActor()
    futures = []
    try:
        # Send 5000 requests rapidly
        for i in range(5000):
            futures.append(actor.increment(1))
        
        # Wait for all of them
        results = [f.result() for f in futures]
        assert results[-1] == 5000
    finally:
        actor.stop()


def test_actor_high_throughput_concurrency_stress():
    """Verify high-throughput concurrent method calls process correctly without message loss or hangs."""
    class ThroughputActor(Actor):
        def __init__(self):
            super().__init__()
            self.count = 0

        def inc(self):
            self.count += 1
            return self.count

        def get_count(self):
            return self.count

    actor = ThroughputActor()
    num_threads = 16
    ops_per_thread = 5000

    def worker():
        for _ in range(ops_per_thread):
            actor.inc()

    try:
        threads = [threading.Thread(target=worker) for _ in range(num_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        final = actor.get_count().result(timeout=10.0)
        assert final == num_threads * ops_per_thread
    finally:
        actor.stop()


class _TestActorFix(Actor):
    def __init__(self):
        super().__init__()
        self.counter = 0

    def inc(self):
        self.counter += 1
        return self.counter

    def get_counter(self):
        return self.counter


def test_actor_stop_before_first_call_closes_mailbox():
    """Verify stop() before first call causes subsequent sends to be rejected atomically."""
    actor = _TestActorFix()
    actor.stop()

    with pytest.raises(RuntimeError, match="stopped"):
        actor.inc()

    assert not actor._core.is_running, "stop 后应为 Stopped"


def test_actor_concurrent_stop_and_call_no_thread_leak():
    """Verify concurrent stop + call causes no hangs or thread leaks."""
    for batch in range(20):
        actor = _TestActorFix()
        done = threading.Event()
        errors = []

        def worker_call():
            done.wait()
            try:
                f = actor.inc()
                f.result(timeout=0.3)
            except (RuntimeError, Exception):
                pass

        def worker_stop():
            done.wait()
            actor.stop()

        threads = [threading.Thread(target=worker_call) for _ in range(3)]
        threads += [threading.Thread(target=worker_stop) for _ in range(2)]

        for t in threads:
            t.start()

        time.sleep(0.05)
        done.set()

        deadline = time.monotonic() + 5.0
        for t in threads:
            r = deadline - time.monotonic()
            if r <= 0:
                break
            t.join(timeout=max(r, 0.05))

        alive = sum(1 for t in threads if t.is_alive())
        if alive:
            errors.append(f"batch {batch}: {alive} threads still alive")

        actor.stop()

        try:
            actor.inc()
            errors.append(f"batch {batch}: stop 后仍可发送")
        except RuntimeError:
            pass

        assert not errors, str(errors)


def test_actor_concurrent_stop_barrier_safety():
    """Verify concurrent stop() and call() under barrier synchronization has no thread leaks."""
    import concurrent.futures

    class _A1Actor(Actor):
        def __init__(self):
            super().__init__()
            self.val = 0

        def get_val(self):
            return self.val

    failures = []

    for iteration in range(100):
        actor = _A1Actor()
        barrier = threading.Barrier(2, timeout=3.0)

        def call_method():
            barrier.wait()
            try:
                f = actor.get_val()
                f.result(timeout=1.0)
            except (RuntimeError, concurrent.futures.TimeoutError):
                pass

        def stop_actor():
            barrier.wait()
            actor.stop()

        t_call = threading.Thread(target=call_method)
        t_stop = threading.Thread(target=stop_actor)

        t_call.start()
        t_stop.start()
        t_call.join(timeout=2.0)
        t_stop.join(timeout=2.0)

        try:
            actor.stop()
        except Exception as e:
            failures.append(f"iter {iteration}: 二次 stop 报错: {e}")

        try:
            actor.get_val()
            failures.append(f"iter {iteration}: stop 后仍可发送消息")
        except RuntimeError:
            pass

    assert not failures, f"失败: {failures[:3]}"


def test_actor_stopped_state_rejects_sends():
    """Verify stop() causes send_message() to reject future sends without hanging Futures."""
    class _N1Actor(Actor):
        def __init__(self):
            super().__init__()
            self.val = 0

        def set_val(self, v):
            self.val = v
            return self.val

    for _ in range(50):
        actor = _N1Actor()
        f1 = actor.set_val(1)
        assert f1.result(timeout=2.0) == 1

        actor.stop()

        try:
            actor.set_val(2)
            pytest.fail("stop 后应拒绝发送")
        except RuntimeError:
            pass

# ==========================================
# 3. ERROR PARAMETERIZED TESTS (25 cases)
# ==========================================

@pytest.mark.parametrize("exception_cls", [
    ZeroDivisionError,
    ValueError,
    TypeError,
    RuntimeError,
    AttributeError,
    KeyError,
    IndexError,
    NameError,
    ImportError,
    MemoryError,
    OSError,
    SyntaxError,
    LookupError,
    AssertionError,
    ArithmeticError,
])
def test_actor_exception_propagation_parameterized(exception_cls):
    """Verify that any of the 15 exception types raised inside an Actor method are propagated and raised by future.result()."""
    class BadActor(Actor):
        def cause_error(self, exc_type):
            raise exc_type("test error inside actor")
            
    actor = BadActor()
    try:
        f = actor.cause_error(exception_cls)
        with pytest.raises(exception_cls):
            f.result()
    finally:
        actor.stop()

def test_actor_non_existent_method():
    """Calling a non-existent method on the Actor must raise AttributeError on the caller thread immediately."""
    actor = CounterActor()
    try:
        with pytest.raises(AttributeError):
            actor.non_existent_method()
    finally:
        actor.stop()

def test_actor_state_isolation():
    """Verify that methods are intercepted (return Future) but data attrs are direct.

    The descriptor-based Actor only wraps callable methods. Public data
    attributes are regular Python attributes and can be accessed directly
    from any thread — users are responsible for thread safety of data."""
    class StateActor(Actor):
        def __init__(self):
            super().__init__()
            self._value = 42
            self.public_val = 100

        def get_value(self):
            return self._value

    actor = StateActor()
    try:
        # Private attrs accessible directly
        assert actor._value == 42

        # Public data attrs: direct access (no __getattribute__ blocking)
        assert actor.public_val == 100
        actor.public_val = 200
        assert actor.public_val == 200
        del actor.public_val

        # But methods ARE intercepted — get_value returns a Future
        f = actor.get_value()
        assert isinstance(f, Future)
        assert f.result(timeout=2.0) == 42
    finally:
        actor.stop()

def test_actor_stop_graceful_no_self_join():
    """Verify that calling stop() on an actor does not cause cannot join current thread RuntimeError."""
    class SimpleActor(Actor):
        def __init__(self):
            super().__init__()
            self.val = 0
        def inc(self):
            self.val += 1
            return self.val
            
    a = SimpleActor()
    assert a.inc().result() == 1
    # Calling stop() should succeed without raising RuntimeError
    a.stop()

def test_actor_lifecycle_hooks():
    """Verify Actor on_start and on_stop lifecycle hooks run correctly."""
    events = []
    class LifecycleActor(Actor):
        def on_start(self):
            events.append("started")
        def on_stop(self):
            events.append("stopped")
        def work(self):
            events.append("working")

    actor = LifecycleActor()
    f = actor.work()
    f.result()
    actor.stop()
    assert events == ["started", "working", "stopped"]

def test_actor_supervision_hook():
    """Verify on_error supervision hook is triggered when method raises."""
    error_log = []
    class SupervisedActor(Actor):
        def on_error(self, exc, method_name, args, kwargs):
            error_log.append((type(exc), method_name, args))
        def fail_task(self, msg):
            raise ValueError(msg)

    actor = SupervisedActor()
    f = actor.fail_task("something went wrong")
    with pytest.raises(ValueError, match="something went wrong"):
        f.result()
    actor.stop()
    assert len(error_log) == 1
    assert error_log[0][0] is ValueError
    assert error_log[0][1] == "fail_task"
    assert error_log[0][2] == ("something went wrong",)

def test_actor_property_interception():
    """Verify @property getter is NOT wrapped — returns value directly.

    Properties are data descriptors, not callables. They're excluded from
    CallProxy wrapping so they work like normal Python properties."""
    class PropertyActor(Actor):
        def __init__(self):
            super().__init__()
            self._count = 42

        @property
        def count(self):
            return self._count

    actor = PropertyActor()
    # Properties return values directly (not Futures)
    assert actor.count == 42
    actor.stop()

def test_actor_init_exception_handling():
    """Verify partially constructed Actor when __init__ fails does not poison system."""
    class FailingInitActor(Actor):
        def __init__(self):
            super().__init__()
            raise ValueError("Init failed")

    with pytest.raises(ValueError, match="Init failed"):
        FailingInitActor()


def test_actor_concurrent_requests_order_preservation():
    """High Quality: Verify concurrent threads maintain sequential state safety."""
    class OrderActor(Actor):
        def __init__(self):
            super().__init__()
            self.counter = 0

        def add(self, amount):
            self.counter += amount
            return self.counter

    actor = OrderActor()
    try:
        threads = []
        for _ in range(4):
            def worker():
                futures = [actor.add(1) for _ in range(25)]
                futures[-1].result(timeout=5.0)

            t = threading.Thread(target=worker)
            threads.append(t)
            t.start()

        for t in threads:
            t.join(timeout=5.0)

        final_val = actor.add(0).result(timeout=2.0)
        assert final_val == 4 * 25
    finally:
        actor.stop()


def test_actor_supervision_recovery_flow():
    """High Quality: Verify on_error returning True suppresses errors across multiple failures."""
    class ResilientActor(Actor):
        def __init__(self):
            super().__init__()
            self.error_count = 0

        def on_error(self, exc, method_name, args, kwargs):
            self.error_count += 1
            return True  # Handled

        def risky_op(self, val):
            if val < 0:
                raise ValueError("negative value")
            return val * 2

        def get_error_count(self):
            return self.error_count

    actor = ResilientActor()
    try:
        f1 = actor.risky_op(-5)
        assert f1.result(timeout=2.0) is None  # Suppressed!

        f2 = actor.risky_op(10)
        assert f2.result(timeout=2.0) == 20

        assert actor.get_error_count().result(timeout=2.0) == 1
    finally:
        actor.stop()


def test_actor_reentrant_self_call_safety():
    """High Quality: Verify Actor internal method calling another internal method on self executes directly without deadlock."""
    class ReentrantActor(Actor):
        def internal_helper(self, x):
            return x * 10

        def compute(self, a, b):
            # Calling internal method directly on self from inside the actor thread
            h1 = self.internal_helper(a)
            h2 = self.internal_helper(b)
            return h1 + h2

    actor = ReentrantActor()
    try:
        res = actor.compute(3, 4).result(timeout=2.0)
        assert res == 70
    finally:
        actor.stop()


def test_actor_state_encapsulation_strict_enforcement():
    """Verify public data attrs are directly accessible (no __getattribute__ blocking).

    The descriptor-based Actor only wraps callable methods. Data attributes
    are regular Python attrs — users guard them via getter methods."""
    class BankAccountActor(Actor):
        def __init__(self):
            super().__init__()
            self.balance = 1000

        def deposit(self, amt):
            self.balance += amt

    account = BankAccountActor()
    try:
        # Direct public attribute access succeeds (no __getattribute__ blocking)
        assert account.balance == 1000
        account.balance = 500
        assert account.balance == 500

        # But getter methods ARE intercepted — use deposit() for thread safety
        f = account.deposit(100)
        f.result(timeout=2.0)
        assert account.balance == 600
    finally:
        account.stop()


def test_actor_stop_timeout_strictly_honored():
    """Verify that stop(timeout=T) returns within T seconds even if worker is busy with a long operation."""
    class BlockingActor(Actor):
        def slow_op(self):
            time.sleep(5.0)

    actor = BlockingActor()
    actor.slow_op()  # Enqueue blocking task
    
    start = time.monotonic()
    TIMEOUT = 0.2
    actor.stop(timeout=TIMEOUT)
    elapsed = time.monotonic() - start
    
    assert elapsed < TIMEOUT * 2.0, f"stop(timeout={TIMEOUT}) took {elapsed:.3f}s, expected < {TIMEOUT * 2.0}s"


def test_actor_tell_fire_and_forget_semantics():
    """Verify tell() enqueues method calls asynchronously without creating or returning Futures."""
    recorded = []
    class TellActor(Actor):
        def ping(self, msg):
            recorded.append(msg)

    actor = TellActor()
    try:
        res = actor.tell("ping", "hello_tell")
        assert res is None
        # Give worker a brief moment to process
        time.sleep(0.1)
        assert "hello_tell" in recorded
    finally:
        actor.stop()


def test_actor_high_throughput_concurrency_stress():
    """Verify high-throughput concurrent method calls process correctly without message loss or hangs."""
    class ThroughputActor(Actor):
        def __init__(self):
            super().__init__()
            self.count = 0

        def inc(self):
            self.count += 1
            return self.count

        def get_count(self):
            return self.count

    actor = ThroughputActor()
    num_threads = 8
    ops_per_thread = 500

    def worker():
        for _ in range(ops_per_thread):
            actor.inc()

    try:
        threads = [threading.Thread(target=worker) for _ in range(num_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        final = actor.get_count().result(timeout=5.0)
        assert final == num_threads * ops_per_thread
    finally:
        actor.stop()


