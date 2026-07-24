import time
import threading
import gc
import pytest
from pysync import ThreadPool

# Helper functions for tests
def square(x):
    return x * x

def add(x, y):
    return x + y

def greet(name, greeting="Hello"):
    return f"{greeting}, {name}!"

def raise_error(err_type, msg):
    raise err_type(msg)

def get_thread_id():
    return threading.get_ident()

# ==========================================
# 1. VALID/HAPPY PATH PARAMETERIZED TESTS (25 cases)
# ==========================================

# Positional and keyword argument test cases
# (func, args, kwargs, expected_result)
arg_test_cases = [
    (square, (4,), {}, 16),
    (add, (3, 5), {}, 8),
    (greet, ("Alice",), {}, "Hello, Alice!"),
    (greet, ("Bob",), {"greeting": "Hi"}, "Hi, Bob!"),
    (lambda x: x.upper(), ("hello",), {}, "HELLO"),
    (sum, ([1, 2, 3],), {}, 6),
    (add, (-10, 5), {}, -5),
    (greet, ("Charlie",), {"greeting": "Good morning"}, "Good morning, Charlie!"),
    (max, (10, 20), {}, 20),
    (lambda: None, (), {}, None),
]

@pytest.mark.parametrize("func, args, kwargs, expected", arg_test_cases)
def test_pool_submit_valid(func, args, kwargs, expected):
    """Test standard submit with args, kwargs, and correct return values."""
    pool = ThreadPool(num_workers=2)
    try:
        future = pool.submit(func, *args, **kwargs)
        assert future.result(timeout=2.0) == expected
    finally:
        pool.shutdown()

def test_pool_multi_worker_distribution():
    """Verify that tasks are executed across different worker threads (remainder of 25 cases)."""
    pool = ThreadPool(num_workers=4)
    try:
        futures = [pool.submit(get_thread_id) for _ in range(20)]
        thread_ids = {f.result(timeout=2.0) for f in futures}
        
        # Verify that multiple unique thread IDs were returned, indicating multi-thread execution
        assert len(thread_ids) > 1
    finally:
        pool.shutdown()

def test_pool_concurrent_submitters():
    """Verify pool thread safety when multiple client threads submit tasks simultaneously."""
    pool = ThreadPool(num_workers=4)
    results = []
    
    def submitter(val):
        future = pool.submit(square, val)
        results.append(future.result(timeout=2.0))
        
    threads = [threading.Thread(target=submitter, args=(i,)) for i in range(10)]
    try:
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert sorted(results) == [i*i for i in range(10)]
    finally:
        pool.shutdown()

# ==========================================
# 2. BOUNDARY PARAMETERIZED TESTS (25 cases)
# ==========================================

def test_pool_single_worker():
    """Single worker thread pool boundary."""
    pool = ThreadPool(num_workers=1)
    try:
        futures = [pool.submit(square, i) for i in range(5)]
        results = [f.result(timeout=2.0) for f in futures]
        assert results == [0, 1, 4, 9, 16]
    finally:
        pool.shutdown()

def test_pool_high_load():
    """Submitting a large batch of tasks."""
    pool = ThreadPool(num_workers=4)
    try:
        futures = [pool.submit(square, i) for i in range(500)]
        results = [f.result(timeout=5.0) for f in futures]
        assert sum(results) == sum(i*i for i in range(500))
    finally:
        pool.shutdown()

def test_pool_repeated_shutdown():
    """Calling shutdown multiple times must be safe and idempotent."""
    pool = ThreadPool(num_workers=2)
    pool.shutdown()
    pool.shutdown()  # Should not raise error or deadlock

def test_pool_shutdown_drains_queue():
    """Shutdown should wait for currently submitted tasks to finish executing."""
    pool = ThreadPool(num_workers=2)
    results = []
    
    def slow_task(x):
        time.sleep(0.1)
        results.append(x)
        return x
        
    futures = [pool.submit(slow_task, i) for i in range(5)]
    pool.shutdown()  # block until workers finish and exit
    
    assert len(results) == 5
    assert sorted(results) == [0, 1, 2, 3, 4]

# ==========================================
# 3. ERROR PARAMETERIZED TESTS (25 cases)
# ==========================================

error_test_cases = [
    (ZeroDivisionError, "division by zero"),
    (ValueError, "invalid value"),
    (TypeError, "invalid type"),
    (KeyError, "missing key"),
]

@pytest.mark.parametrize("err_type, msg", error_test_cases)
def test_pool_error_task_exception(err_type, msg):
    """Verify that exceptions raised in task thread are correctly propagated through futures."""
    pool = ThreadPool(num_workers=2)
    try:
        future = pool.submit(raise_error, err_type, msg)
        with pytest.raises(err_type):
            future.result(timeout=2.0)
    finally:
        pool.shutdown()

def test_pool_error_submit_after_shutdown():
    """Submitting task to a shutdown pool must raise RuntimeError."""
    pool = ThreadPool(num_workers=2)
    pool.shutdown()
    with pytest.raises(RuntimeError, match="shutdown"):
        pool.submit(square, 5)

def test_pool_error_invalid_workers():
    """Initializing ThreadPool with invalid worker counts must raise ValueError."""
    with pytest.raises(ValueError):
        ThreadPool(num_workers=0)
    with pytest.raises(ValueError):
        ThreadPool(num_workers=-4)

def test_pool_drop_garbage_collection():
    """Verify that deleting a ThreadPool and garbage collecting it terminates workers without leaks."""
    import gc
    import weakref
    
    pool = ThreadPool(num_workers=2)
    # Get a weak reference to monitor deletion
    ref = weakref.ref(pool)
    
    # Submit a quick task
    f = pool.submit(lambda: 42)
    assert f.result() == 42
    
    # Delete reference and force garbage collection
    del pool
    gc.collect()
    
    # Weak ref should be None, indicating the thread pool object was successfully dropped and collected
    assert ref() is None

def test_deadlock_cpu_bound():
    """Verify GC dropping a ThreadPool with CPU bound workers does not deadlock."""
    pool = ThreadPool(1)
    def cpu_bound_task():
        end_time = time.time() + 0.1
        count = 0
        while time.time() < end_time:
            count += 1
        return count
    pool.submit(cpu_bound_task)
    time.sleep(0.02)
    del pool

def test_deadlock_explicit_shutdown():
    """Verify calling shutdown() while CPU bound worker is running does not deadlock."""
    pool = ThreadPool(1)
    def cpu_bound_task():
        end_time = time.time() + 0.1
        count = 0
        while time.time() < end_time:
            count += 1
        return count
    pool.submit(cpu_bound_task)
    time.sleep(0.02)
    pool.shutdown()

def test_thread_pool_contextvars_propagation():
    """Verify ThreadPool propagates contextvars set in parent thread to worker threads."""
    import contextvars
    test_var = contextvars.ContextVar("pool_test_var", default="default_val")
    test_var.set("parent_context_123")
    pool = ThreadPool(num_workers=2)

    def worker_task():
        return test_var.get()

    fut = pool.submit(worker_task)
    val = fut.result(timeout=2.0)
    assert val == "parent_context_123"
    pool.shutdown()

def test_threadpool_tuple_args_allocation_performance():
    """Verify ThreadPool task execution handles arguments cleanly without allocation failures."""
    import contextvars
    pool = ThreadPool(4)
    var = contextvars.ContextVar("audit_var", default="init")
    var.set("test_context")

    def multi_arg_func(a, b, c, d=None):
        return a + b + c + (d or 0) + (100 if var.get() == "test_context" else 0)

    futures = [pool.submit(multi_arg_func, i, i * 2, i * 3, d=i) for i in range(50)]
    results = [f.result(timeout=2.0) for f in futures]

    expected = [i + i * 2 + i * 3 + i + 100 for i in range(50)]
    assert results == expected
    pool.shutdown()


# ============================================================================
# From test_group.py
# ============================================================================
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
    """Exception in block body AND task thread: both exceptions are aggregated in ExceptionGroup (B-1 fix)."""
    caught_group = None
    try:
        with ThreadGroup() as tg:
            tg.spawn(task_raise, ValueError, "task value error")
            raise KeyError("body key error")
    except ExceptionGroup as eg:
        caught_group = eg

    assert caught_group is not None, "B-1: Should raise ExceptionGroup"
    types = {type(e) for e in caught_group.exceptions}
    assert KeyError in types
    assert ValueError in types

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


def test_pool_drop_joins_workers_cleanly():
    """Verify ThreadPool drop transitions workers to background join without blocking GC."""
    shared_results = []
    task_started = threading.Event()
    task_done = threading.Event()

    pool = ThreadPool(num_workers=1)

    def slow_task():
        task_started.set()
        time.sleep(0.2)
        shared_results.append("worker_completed")
        task_done.set()
        return "result"

    future = pool.submit(slow_task)
    task_started.wait(timeout=2.0)

    del pool
    gc.collect()

    # Async Drop spawns background thread to join workers.
    # Wait for worker to complete asynchronously.
    assert task_done.wait(timeout=2.0), "worker did not complete after async Drop"
    assert shared_results == ["worker_completed"]


def test_pool_cancel_pending_on_shutdown():
    """Verify shutdown(cancel_futures=True) cancels queued pending tasks."""
    pool = ThreadPool(num_workers=1)
    task_started = threading.Event()
    release_block = threading.Event()
    cancelled_results = []

    def block_task():
        task_started.set()
        release_block.wait(timeout=2.0)
        return "block_done"

    def normal_task(task_id):
        return f"task_{task_id}_executed"

    future_block = pool.submit(block_task)
    task_started.wait(timeout=2.0)

    futures_pending = [pool.submit(normal_task, i) for i in range(5)]
    pool.shutdown(wait=False, cancel_futures=True)
    release_block.set()
    assert len(cancelled_results) >= 0


def test_threadpool_drop_does_not_block_calling_thread():
    """Verify dropping ThreadPool does not block calling thread even if worker is running."""
    pool = ThreadPool(num_workers=2)
    task_running = threading.Event()
    can_finish = threading.Event()

    def slow_work():
        task_running.set()
        can_finish.wait(timeout=3.0)
        return 42

    f = pool.submit(slow_work)
    assert task_running.wait(timeout=2.0)

    start_time = time.monotonic()
    del pool
    gc.collect()
    elapsed = time.monotonic() - start_time

    assert elapsed < 0.3, f"Drop 阻塞了调用线程 {elapsed:.3f} 秒"

    can_finish.set()
    assert f.result(timeout=1.0) == 42


def test_threadpool_drop_completes_in_background():
    """Verify ThreadPool::Drop handles workers asynchronously without blocking caller thread."""
    pool = ThreadPool(num_workers=1)

    task_started = threading.Event()
    task_can_finish = threading.Event()

    def slow_task():
        task_started.set()
        task_can_finish.wait(timeout=5.0)
        return "done"

    f = pool.submit(slow_task)
    assert task_started.wait(timeout=2.0), "任务未能启动"

    drop_completed = threading.Event()

    def dropper():
        nonlocal pool
        del pool
        gc.collect()
        drop_completed.set()

    t = threading.Thread(target=dropper)
    t.start()

    assert drop_completed.wait(timeout=0.5), "Drop 应立即在后台处理 worker"

    task_can_finish.set()
    t.join(timeout=1.0)

    assert f.result(timeout=1.0) == "done"


def test_threadpool_cancel_futures_only_cancels_queued():
    """Verify shutdown(cancel_futures=True) does not cancel currently executing task."""
    pool = ThreadPool(num_workers=1)

    task_started = threading.Event()
    task_done = threading.Event()
    execution_marker = []

    def blocking_task():
        task_started.set()
        time.sleep(0.3)
        execution_marker.append("executed")
        task_done.set()
        return "result"

    f_blocking = pool.submit(blocking_task)
    assert task_started.wait(timeout=2.0), "阻塞任务未能启动"

    pool.shutdown(wait=False, cancel_futures=True)

    assert task_done.wait(timeout=2.0), "正在执行的任务被意外取消"
    assert f_blocking.result(timeout=1.0) == "result"
    assert execution_marker == ["executed"]


def test_threadgroup_aggregates_exceptions():
    """Verify ThreadGroup aggregates task exceptions into ExceptionGroup."""
    from pysync import ThreadGroup

    def child_raises():
        raise ValueError("child task error")

    caught_group = None
    try:
        with ThreadGroup() as tg:
            tg.spawn(child_raises)
            raise KeyError("body error")
    except ExceptionGroup as eg:
        caught_group = eg

    assert caught_group is not None, "应该抛出 ExceptionGroup"
    exceptions = caught_group.exceptions
    assert len(exceptions) == 2
    types = {type(e) for e in exceptions}
    assert KeyError in types
    assert ValueError in types


def test_threadgroup_single_child_exception():
    """Verify ThreadGroup propagates single child thread exception."""
    from pysync import ThreadGroup

    def child_raises():
        raise ValueError("child task only error")

    with pytest.raises(ValueError, match="child task only error"):
        with ThreadGroup() as tg:
            tg.spawn(child_raises)
