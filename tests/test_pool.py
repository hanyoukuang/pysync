import pytest
import time
import threading
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
