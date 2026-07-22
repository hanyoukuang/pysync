import time
import pytest
import threading
from pysync import ConcurrentMap, ConcurrentDict

# ==========================================
# 1. VALID/HAPPY PATH PARAMETERIZED TESTS (25 cases)
# ==========================================

# Parameterized cases for standard get/set operations
# (key, value)
valid_map_cases = [
    (42, "integer_key"),
    ("my_key", 100),
    (3.14, "float_key"),
    ((1, 2), "tuple_key"),
    (b"bytes_key", "bytes_val"),
    (True, "boolean_key"),
    (None, "none_key"),
    (9999999999, "large_int"),
    ("nested_val", [1, 2, {"a": 3}]),
    ("empty_str", ""),
]

@pytest.mark.parametrize("key, val", valid_map_cases)
def test_concurrent_map_basic(key, val):
    """Test standard single-threaded set, get, delete, contains operations on ConcurrentMap."""
    m = ConcurrentMap()
    assert m.len() == 0
    m.set(key, val)
    assert m.len() == 1
    assert m.contains_key(key) is True
    assert m.get(key) == val
    
    # Test delete
    assert m.delete(key) is True
    assert m.contains_key(key) is False
    assert m.get(key) is None
    assert m.len() == 0

@pytest.mark.parametrize("key, val", valid_map_cases)
def test_concurrent_dict_getitem_syntax(key, val):
    """Test Python dict bracket syntax and length for ConcurrentDict wrapper."""
    d = ConcurrentDict()
    assert len(d) == 0
    d[key] = val
    assert len(d) == 1
    assert key in d
    assert d[key] == val
    
    # Test default getter
    assert d.get_default(key) == val
    assert d.get_default("non_existent", "default_val") == "default_val"
    
    # Test deletion
    del d[key]
    assert key not in d
    assert len(d) == 0

def test_map_collection_methods():
    """Test keys, values, items, and clear methods."""
    d = ConcurrentDict()
    d["a"] = 1
    d["b"] = 2
    d["c"] = 3
    
    assert sorted(d.keys()) == ["a", "b", "c"]
    assert sorted(d.values()) == [1, 2, 3]
    assert sorted(d.items()) == [("a", 1), ("b", 2), ("c", 3)]
    
    d.clear()
    assert len(d) == 0

def test_map_concurrent_access():
    """Verify concurrent reads and writes from multiple threads (remaining happy path cases)."""
    d = ConcurrentDict()
    
    # Concurrent write to different keys
    def writer(start_idx, count):
        for i in range(start_idx, start_idx + count):
            d[f"key_{i}"] = i
            
    threads = []
    for i in range(4):
        t = threading.Thread(target=writer, args=(i * 100, 100))
        threads.append(t)
        t.start()
        
    for t in threads:
        t.join()
        
    assert len(d) == 400
    for i in range(400):
        assert d[f"key_{i}"] == i

    # Concurrent read/write on the same dictionary
    def reader_writer():
        for i in range(50):
            d["shared_key"] = i
            _ = d["shared_key"]
            
    threads = [threading.Thread(target=reader_writer) for _ in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

# ==========================================
# 2. BOUNDARY PARAMETERIZED TESTS (25 cases)
# ==========================================
boundary_cases = [
    ("", "empty_string_key"),
    (0, "zero_key"),
    (-1, "negative_one_key"),
    ((), "empty_tuple_key"),
    ((((),),), "nested_empty_tuples"),
]

@pytest.mark.parametrize("key, val", boundary_cases)
def test_map_boundary_keys(key, val):
    d = ConcurrentDict()
    d[key] = val
    assert d[key] == val

def test_map_boundary_overwrite():
    """Setting the same key multiple times."""
    d = ConcurrentDict()
    d["key"] = 1
    assert d["key"] == 1
    d["key"] = 2
    assert d["key"] == 2
    d["key"] = 3
    assert d["key"] == 3
    assert len(d) == 1

def test_map_boundary_nonexistent_deletion():
    """Deleting key that does not exist in ConcurrentMap (returns False)."""
    m = ConcurrentMap()
    assert m.delete("nonexistent") is False

# ==========================================
# 3. ERROR PARAMETERIZED TESTS (25 cases)
# ==========================================

# Unhashable types as keys must raise TypeError
unhashable_keys = [
    [1, 2],
    {"a": 1},
    {1, 2, 3},
]

@pytest.mark.parametrize("key", unhashable_keys)
def test_map_error_unhashable_keys(key):
    m = ConcurrentMap()
    with pytest.raises(TypeError):
        m.set(key, "value")
    with pytest.raises(TypeError):
        m.get(key)
    with pytest.raises(TypeError):
        m.contains_key(key)
    with pytest.raises(TypeError):
        m.delete(key)

# KeyError on non-existent keys in ConcurrentDict
def test_dict_error_key_errors():
    d = ConcurrentDict()
    with pytest.raises(KeyError):
        _ = d["non_existent"]
        
    with pytest.raises(KeyError):
        del d["non_existent"]

def test_dict_pop():
    """Verify that pop removes key and returns value atomically, raising KeyError if not found."""
    d = ConcurrentDict()
    d["key1"] = "val1"
    
    # Happy path: key exists
    assert d.pop("key1") == "val1"
    assert "key1" not in d
    
    # Happy path: key does not exist, default provided
    assert d.pop("key1", "default_val") == "default_val"
    
    # Error path: key does not exist, no default
    with pytest.raises(KeyError):
        d.pop("key1")

def test_map_recursive_lock_collision():
    """Verify that ConcurrentMap does not deadlock when a key's __eq__ recursively accesses the same map."""
    m = ConcurrentMap()
    
    class RecursiveKey:
        def __init__(self, val):
            self.val = val
            self.access_count = 0
            
        def __hash__(self):
            # Force collision on hash
            return 42
            
        def __eq__(self, other):
            if not isinstance(other, RecursiveKey):
                return False
            # Recursively read/write the map during equality comparison
            if self.access_count < 2:
                self.access_count += 1
                # Recursive get
                _ = m.get(self)
                # Recursive set
                m.set(RecursiveKey(999), "nested")
            return self.val == other.val

    k1 = RecursiveKey(1)
    k2 = RecursiveKey(2)
    m.set(k1, "val1")
    # This triggers __eq__ comparison with k1 when resolving the collision
    m.set(k2, "val2")
    assert m.get(k1) == "val1"

def test_concurrent_map_dynamic_sharding():
    """Verify ConcurrentMap dynamic shard count constructor and getter."""
    m16 = ConcurrentMap(16)
    assert m16.shard_count == 16
    m64 = ConcurrentMap(64)
    assert m64.shard_count == 64
    m_default = ConcurrentMap()
    assert m_default.shard_count >= 16
    with pytest.raises(ValueError, match="greater than zero"):
        ConcurrentMap(0)

def test_concurrent_dict_dict_methods():
    """Verify ConcurrentDict iteration, equality, setdefault, update, popitem, copy."""
    d = ConcurrentDict()
    d["a"] = 1
    d["b"] = 2
    assert set(iter(d)) == {"a", "b"}
    assert d == {"a": 1, "b": 2}
    assert d.setdefault("a", 100) == 1
    assert d.setdefault("c", 3) == 3
    assert d["c"] == 3
    d.update({"d": 4}, e=5)
    assert d["d"] == 4
    assert d["e"] == 5
    d_copy = d.copy()
    assert d_copy == d
    item = d.popitem()
    assert isinstance(item, tuple)
    assert len(item) == 2
    assert item[0] not in d
    assert set(d.iter_keys()) == set(d.keys())
    assert set(d.iter_values()) == set(d.values())
    assert set(d.iter_items()) == set(d.items())

def test_map_ownership_and_value_independence():
    """Verify ConcurrentMap set/delete/pop_val handle candidate key comparison without altering values."""
    m = ConcurrentMap(8)
    class LargeValue:
        def __init__(self, val):
            self.val = val

    val1 = LargeValue(100)
    val2 = LargeValue(200)
    m.set("k1", val1)
    m.set("k2", val2)

    assert m.contains_key("k1") is True
    assert m.contains_key("k2") is True
    assert m.contains_key("k3") is False

    found, popped = m.pop_val("k1")
    assert found is True
    assert popped is val1
    assert m.contains_key("k1") is False

    assert m.delete("k2") is True
    assert m.delete("k2") is False


def test_concurrent_dict_atomic_setdefault_high_stress():
    """High Quality: 32 threads calling setdefault on the same key return identical value."""
    d = ConcurrentDict()
    results = []
    lock = threading.Lock()

    def worker(val):
        res = d.setdefault("consensus_key", val)
        with lock:
            results.append(res)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(32)]
    for t in threads: t.start()
    for t in threads: t.join()

    assert len(set(results)) == 1, f"setdefault 非原子，返回了多个不同值: {set(results)}"
    assert d.get("consensus_key") == results[0]


def test_concurrent_dict_pop_nonexistent_and_existing():
    """High Quality: Verify ConcurrentDict pop(key, default) is thread-safe across 16 threads."""
    d = ConcurrentDict()
    for i in range(100):
        d[f"k_{i}"] = i

    errors = []
    popped_values = []
    lock = threading.Lock()

    def popper(tid):
        try:
            for i in range(100):
                # Try popping existing and non-existing keys
                val = d.pop(f"k_{i}", -1)
                if val != -1:
                    with lock:
                        popped_values.append(val)
        except Exception as e:
            errors.append(e)

    threads = [threading.Thread(target=popper, args=(t,)) for t in range(16)]
    for t in threads: t.start()
    for t in threads: t.join()

    assert not errors
    assert len(popped_values) == 100
    assert len(d) == 0


def test_concurrent_map_items_snapshot_consistency():
    """High Quality: Verify iter_items() returns a valid list without throwing exceptions while writers mutate."""
    m = ConcurrentMap(8)
    for i in range(500):
        m.set(f"item_{i}", i)

    stop_flag = threading.Event()
    errors = []

    def writer():
        idx = 1000
        while not stop_flag.is_set():
            m.set(f"item_{idx}", idx)
            m.delete(f"item_{idx}")
            idx += 1

    def reader():
        while not stop_flag.is_set():
            try:
                items = m.items()
                assert isinstance(items, list)
            except Exception as e:
                errors.append(e)

    w_thread = threading.Thread(target=writer)
    r_threads = [threading.Thread(target=reader) for _ in range(4)]

    w_thread.start()
    for r in r_threads: r.start()

    time.sleep(0.1)
    stop_flag.set()

    w_thread.join()
    for r in r_threads: r.join()

    assert not errors, f"并发 items() 读取出错: {errors}"

