import pytest
import threading
from pysync import AtomicInteger, AtomicBoolean

# ==========================================
# 1. Execution & Concurrency Tests
# ==========================================

def test_atomic_integer_basic():
    """Test standard single-threaded AtomicInteger operations."""
    a = AtomicInteger(10)
    assert a.get() == 10
    
    a.set(20)
    assert a.get() == 20
    
    assert a.fetch_add(5) == 20
    assert a.get() == 25
    
    assert a.fetch_sub(3) == 25
    assert a.get() == 22
    
    assert a.increment() == 23
    assert a.decrement() == 22
    
    assert a.get_and_set(100) == 22
    assert a.get() == 100

def test_atomic_boolean_basic():
    """Test standard single-threaded AtomicBoolean operations."""
    b = AtomicBoolean(False)
    assert b.get() is False
    
    b.set(True)
    assert b.get() is True
    
    assert b.get_and_set(False) is True
    assert b.get() is False

# Parameterized cases for CAS (compare_and_set) - 15 cases
cas_integer_cases = [
    (10, 10, 20, True, 20),
    (10, 99, 20, False, 10),
    (0, 0, -1, True, -1),
    (-5, -5, 5, True, 5),
    (100, 100, 100, True, 100),
    (2**63 - 1, 2**63 - 1, 0, True, 0),
    (-2**63, -2**63, 0, True, 0),
    (123456789, 123456789, 987654321, True, 987654321),
    (5, 6, 7, False, 5),
    (-1, -1, -2, True, -2),
    (-2, -1, -3, False, -2),
    (999, 999, -999, True, -999),
    (0, 1, 2, False, 0),
    (42, 42, 1337, True, 1337),
    (1337, 1337, 42, True, 42),
]

@pytest.mark.parametrize("initial, expected, new_val, success, final_val", cas_integer_cases)
def test_atomic_integer_cas(initial, expected, new_val, success, final_val):
    a = AtomicInteger(initial)
    assert a.compare_and_set(expected, new_val) == success
    assert a.get() == final_val

cas_boolean_cases = [
    (False, False, True, True, True),
    (False, True, True, False, False),
    (True, True, False, True, False),
    (True, False, False, False, True),
    (False, False, False, True, False),
    (True, True, True, True, True),
]

@pytest.mark.parametrize("initial, expected, new_val, success, final_val", cas_boolean_cases)
def test_atomic_boolean_cas(initial, expected, new_val, success, final_val):
    b = AtomicBoolean(initial)
    assert b.compare_and_set(expected, new_val) == success
    assert b.get() == final_val

def test_atomic_integer_concurrent_updates():
    """Ensure no lost updates when multiple threads mutate AtomicInteger concurrently."""
    counter = AtomicInteger(0)
    num_threads = 20
    increments_per_thread = 1000
    
    def worker():
        for _ in range(increments_per_thread):
            counter.increment()
            
    threads = [threading.Thread(target=worker) for _ in range(num_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
        
    assert counter.get() == num_threads * increments_per_thread

# ==========================================
# 2. Boundary Tests
# ==========================================

boundary_cases = [
    (0, 0),
    (0, -999999),
    (9223372036854775807, 1), # Max 64-bit int + 1 (overflow)
    (-9223372036854775808, -1), # Min 64-bit int - 1 (overflow)
    (9223372036854775807, 2), # Overflows by 2
    (9223372036854775807, -1), # Max minus 1 (no overflow)
    (-9223372036854775808, 1), # Min plus 1 (no overflow)
    (9223372036854775806, 2), # Hits max exactly
    (-9223372036854775807, -1), # Hits min exactly
    (2**62, 2**62), # Large positive values
    (-2**62, -2**62), # Large negative values
    (9223372036854775807, 9223372036854775807), # Add max to itself
    (-9223372036854775808, -9223372036854775808), # Add min to itself
    (0, 9223372036854775807), # Add max directly
    (0, -9223372036854775808), # Add min directly
]

def wrap_to_i64(val):
    return ((val + 2**63) % 2**64) - 2**63

@pytest.mark.parametrize("initial, delta", boundary_cases)
def test_atomic_integer_boundary_values(initial, delta):
    a = AtomicInteger(initial)
    a.fetch_add(delta)
    assert a.get() == wrap_to_i64(initial + delta)

# ==========================================
# 3. Error Handling Tests
# ==========================================

invalid_init_types = [
    "string", 1.5, [1, 2], {"val": 10}, None, (1, 2)
]

@pytest.mark.parametrize("bad_val", invalid_init_types)
def test_atomic_integer_invalid_init(bad_val):
    with pytest.raises(TypeError):
        AtomicInteger(bad_val)

@pytest.mark.parametrize("bad_val", invalid_init_types)
def test_atomic_boolean_invalid_init(bad_val):
    with pytest.raises(TypeError):
        AtomicBoolean(bad_val)

def test_atomic_errors_type_checking():
    a = AtomicInteger(0)
    # 5 additional error checks
    with pytest.raises(TypeError):
        a.set("string")
    with pytest.raises(TypeError):
        a.fetch_add(1.5)
    with pytest.raises(TypeError):
        a.compare_and_set("expected", 5)
    with pytest.raises(TypeError):
        a.compare_and_set(0, "new")
    with pytest.raises(TypeError):
        a.get_and_set([1, 2])

    b = AtomicBoolean(False)
    # 5 additional error checks
    with pytest.raises(TypeError):
        b.set(100)
    with pytest.raises(TypeError):
        b.compare_and_set(False, "not_a_bool")
    with pytest.raises(TypeError):
        b.compare_and_set("not_a_bool", True)
    with pytest.raises(TypeError):
        b.get_and_set("string")
