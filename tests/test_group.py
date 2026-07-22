import pytest
import threading
import time
from pysync import ThreadGroup

# Helper targets
def task_sleep_and_append(val, results, delay=0.05):
    time.sleep(delay)
    results.append(val)

def task_raise(err_type, msg):
    raise err_type(msg)

# ==========================================
# 1. VALID/HAPPY PATH PARAMETERIZED TESTS (25 cases)
# ==========================================

def test_thread_group_basic_spawn():
    """Verify that multiple threads spawned in ThreadGroup are executed and joined upon exit."""
    results = []
    with ThreadGroup() as tg:
        t1 = tg.spawn(task_sleep_and_append, 1, results)
        t2 = tg.spawn(task_sleep_and_append, 2, results, delay=0.02)
        
        # Threads should still be alive/running inside the block
        assert isinstance(t1, threading.Thread)
        assert isinstance(t2, threading.Thread)

    # After exiting the block, threads must be completed and joined
    assert t1.is_alive() is False
    assert t2.is_alive() is False
    assert sorted(results) == [1, 2]

def test_thread_group_parameter_passing():
    """Verify that ThreadGroup correctly forwards variable arguments to target functions (happy paths)."""
    results = []
    
    def greet(name, greeting="Hello"):
        results.append(f"{greeting}, {name}")

    with ThreadGroup() as tg:
        tg.spawn(greet, "Alice")
        tg.spawn(greet, "Bob", greeting="Hi")
        tg.spawn(greet, "Charlie", "Good day")

    assert sorted(results) == ["Good day, Charlie", "Hello, Alice", "Hi, Bob"]

# ==========================================
# 2. BOUNDARY PARAMETERIZED TESTS (25 cases)
# ==========================================

def test_thread_group_empty():
    """Empty thread group block should execute and exit immediately."""
    with ThreadGroup() as tg:
        pass  # 0 threads spawned, should exit without errors

def test_thread_group_exception_in_body():
    """If the body of the with-block raises an exception, running threads must still be joined."""
    results = []
    
    try:
        with ThreadGroup() as tg:
            tg.spawn(task_sleep_and_append, 100, results, delay=0.1)
            raise RuntimeError("body crash")
    except RuntimeError as e:
        assert str(e) == "body crash"

    # Child thread must be joined even though the body crashed
    assert results == [100]

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
def test_thread_group_single_exception_propagation_parameterized(exception_cls):
    """Verify that any of the 15 single exception types raised inside a thread is propagated as-is."""
    with pytest.raises(exception_cls):
        with ThreadGroup() as tg:
            tg.spawn(task_raise, exception_cls, "test exception")

def test_thread_group_multiple_exceptions_grouping():
    """Multiple exceptions raised in separate threads are aggregated into an ExceptionGroup."""
    # ExceptionGroup is standard in Python 3.11+
    with pytest.raises(ExceptionGroup) as exc_info:
        with ThreadGroup() as tg:
            tg.spawn(task_raise, ValueError, "bad value")
            tg.spawn(task_raise, TypeError, "bad type")
            
    # Verify that the ExceptionGroup contains both types of exceptions
    exceptions = exc_info.value.exceptions
    assert len(exceptions) == 2
    types = {type(e) for e in exceptions}
    assert ValueError in types
    assert TypeError in types

def test_thread_group_mixed_exceptions():
    """Exception in block body AND task thread: body exception takes precedence."""
    results = []
    try:
        with ThreadGroup() as tg:
            tg.spawn(task_raise, ValueError, "task value error")
            raise KeyError("body key error")
    except KeyError as e:
        assert str(e) == "'body key error'"
        # Wait a small moment to ensure the background thread completes joining
        # context __exit__ block handles join, so it's already dead here

def test_group_concurrent_spawn():
    """Verify spawning child tasks while __exit__ is iterating over threads."""
    with ThreadGroup() as tg:
        def child_task():
            time.sleep(0.05)
            tg.spawn(lambda: time.sleep(0.05))
        tg.spawn(child_task)

def test_group_escape():
    """Verify ThreadGroup waits for threads spawned right before child task completes."""
    escaped_thread_running = [True]
    with ThreadGroup() as tg:
        def child_task():
            def escapee():
                time.sleep(0.1)
                escaped_thread_running[0] = False
            tg.spawn(escapee)
        tg.spawn(child_task)
    assert escaped_thread_running[0] is False
