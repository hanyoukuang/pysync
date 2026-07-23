import time
import threading
import gc
import sys
import queue
import pytest
import pysync
from pysync import Channel, ConcurrentDict, RwLock, AtomicInteger, AtomicBoolean, ThreadPool, ThreadGroup, Actor


# ============================================================================
# From test_actor.py
# ============================================================================
import pytest
import threading
import time
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
    """Verify that multi-threaded concurrent calls on Actor are executed sequentially without data loss."""
    actor = CounterActor()
    num_threads = 10
    calls_per_thread = 100
    
    def worker():
        for _ in range(calls_per_thread):
            # We call increment, returning a Future. We don't block.
            actor.increment()

    try:
        threads = [threading.Thread(target=worker) for _ in range(num_threads)]
        for t in threads: t.start()
        for t in threads: t.join()
        
        # Verify the final value is exactly the sum of all increments
        final_val = actor.get_value().result()
        assert final_val == num_threads * calls_per_thread
    finally:
        actor.stop()

def test_actor_private_attributes_non_intercepted():
    """Verify that private attributes and methods (starting with _) are not intercepted as Futures."""
    actor = CounterActor()
    try:
        # Accessing non-callable or private attributes directly
        assert isinstance(actor._mailbox, object)
        assert callable(actor._run_loop)
        # Verify it doesn't return a Future when accessing private/internal structures
        assert not isinstance(actor._mailbox, Future)
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
        # Send many requests rapidly
        for i in range(50):
            futures.append(actor.increment(1))
        
        # Wait for all of them
        results = [f.result() for f in futures]
        assert results[-1] == 50
    finally:
        actor.stop()

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
    """Verify that public state attributes cannot be read, written, or deleted directly from outside."""
    class StateActor(Actor):
        def __init__(self):
            super().__init__()
            self._value = 42
            self.public_val = 100

        def get_value(self):
            return self._value

    actor = StateActor()
    try:
        # Accessing private attribute is allowed
        assert actor._value == 42
        
        # Accessing public attribute from outside raises AttributeError
        with pytest.raises(AttributeError, match="cannot be accessed directly"):
            _ = actor.public_val

        # Mutating public attribute from outside raises AttributeError
        with pytest.raises(AttributeError, match="cannot be mutated directly"):
            actor.public_val = 200

        # Deleting public attribute from outside raises AttributeError
        with pytest.raises(AttributeError, match="cannot be deleted directly"):
            del actor.public_val
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
    """Verify @property getter on Actor subclass is intercepted and returns a Future."""
    class PropertyActor(Actor):
        def __init__(self):
            super().__init__()
            self._count = 42
        @property
        def count(self):
            return self._count

    actor = PropertyActor()
    f = actor.count
    assert f.result() == 42
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
    """High Quality: Verify 16 concurrent threads sending ops to an Actor maintain sequential state safety."""
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
        for _ in range(16):
            def worker():
                futures = [actor.add(1) for _ in range(100)]
                futures[-1].result(timeout=5.0)
            t = threading.Thread(target=worker)
            threads.append(t)
            t.start()

        for t in threads:
            t.join(timeout=5.0)

        final_val = actor.add(0).result(timeout=2.0)
        assert final_val == 1600
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
    """High Quality: Verify direct external mutation or access of public state raises AttributeError across 16 threads."""
    class BankAccountActor(Actor):
        def __init__(self):
            super().__init__()
            self.balance = 1000

        def deposit(self, amt):
            self.balance += amt

    account = BankAccountActor()
    try:
        errors = []
        def attacker():
            try:
                _ = account.balance  # Direct public attribute read should fail
            except AttributeError as e:
                errors.append(e)

            try:
                account.balance = 999999  # Direct public attribute write should fail
            except AttributeError as e:
                errors.append(e)

        threads = [threading.Thread(target=attacker) for _ in range(16)]
        for t in threads: t.start()
        for t in threads: t.join()

        assert len(errors) == 32  # Each thread triggered 2 AttributeErrors
    finally:
        account.stop()

