import time
import threading
import gc
import sys
import queue
import pytest
import pysync
from pysync import Channel, ConcurrentDict, RwLock, AtomicInteger, AtomicBoolean, ThreadPool, ThreadGroup, Actor


# ============================================================================
# From test_channel.py
# ============================================================================
import pytest
import time
import threading
from pysync import Channel, select

# ==========================================
# 1. VALID/HAPPY PATH PARAMETERIZED TESTS (25 cases)
# ==========================================

# Definining parameters for happy paths:
# (capacity, value_to_send, use_timeout, timeout_val)
valid_test_cases = [
    # Bounded channels (Cases 1-10)
    (1, 42, False, 0.0),
    (2, "hello", False, 0.0),
    (5, [1, 2, 3], False, 0.0),
    (10, {"a": 1}, False, 0.0),
    (100, (1, 2), False, 0.0),
    (2, 3.14, True, 1.0),
    (5, None, True, 0.5),
    (1, "edge_value", False, 0.0),
    (3, b"bytes", True, 2.0),
    (10, True, False, 0.0),
    # Unbounded channels (Cases 11-20)
    (None, 999, False, 0.0),
    (None, "unbounded_str", False, 0.0),
    (None, [9, 8], False, 0.0),
    (None, {"key": "val"}, False, 0.0),
    (None, (None,), False, 0.0),
    (None, 0.001, True, 0.5),
    (None, False, True, 1.0),
    (None, b"more_bytes", False, 0.0),
    (None, [1.1, 2.2], True, 0.8),
    (None, {}, False, 0.0),
]

@pytest.mark.parametrize("capacity, val, use_timeout, timeout_val", valid_test_cases)
def test_channel_valid_basic(capacity, val, use_timeout, timeout_val):
    """Test standard single-threaded send and receive operations."""
    chan = Channel(capacity=capacity) if capacity else Channel()
    assert chan.capacity == capacity
    
    if use_timeout:
        chan.send_timeout(val, timeout_val)
        res = chan.recv_timeout(timeout_val)
    else:
        chan.send(val)
        res = chan.recv()
        
    assert res == val

def test_channel_valid_multithreaded():
    """Multi-threaded tests covering remainder of the 25 happy path cases."""
    # Case 21: Bounded 1 producer, 1 consumer thread
    chan = Channel(capacity=10)
    def producer():
        for i in range(5):
            chan.send(i)
    def consumer(results):
        for _ in range(5):
            results.append(chan.recv())
            
    res_list = []
    t1 = threading.Thread(target=producer)
    t2 = threading.Thread(target=consumer, args=(res_list,))
    t1.start()
    t2.start()
    t1.join()
    t2.join()
    assert sorted(res_list) == [0, 1, 2, 3, 4]

    # Case 22: Unbounded multiple producers, single consumer
    chan_unbound = Channel()
    def p_task(val):
        chan_unbound.send(val)
        
    threads = [threading.Thread(target=p_task, args=(i,)) for i in range(3)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
        
    received = [chan_unbound.recv() for _ in range(3)]
    assert sorted(received) == [0, 1, 2]

    # Case 23: Multiple producers, multiple consumers (contention)
    chan_contend = Channel(capacity=2)
    prod_results = []
    cons_results = []
    def prod_thread(val):
        chan_contend.send(val)
    def cons_thread():
        cons_results.append(chan_contend.recv())
        
    p_threads = [threading.Thread(target=prod_thread, args=(i,)) for i in range(2)]
    c_threads = [threading.Thread(target=cons_thread) for _ in range(2)]
    for t in c_threads:
        t.start()
    for t in p_threads:
        t.start()
    for t in p_threads + c_threads:
        t.join()
    assert len(cons_results) == 2

# ==========================================
# 2. BOUNDARY PARAMETERIZED TESTS (25 cases)
# ==========================================
boundary_cases = [
    # Cap-1 edge cases
    (1, ""),
    (1, []),
    (1, {}),
    (1, ()),
    (1, None),
    # Empty collection edge cases on standard bounded/unbounded
    (5, ""),
    (None, ""),
    (10, []),
    (None, []),
    (5, {}),
    (None, {}),
    (5, ()),
    (None, ()),
]

@pytest.mark.parametrize("capacity, boundary_val", boundary_cases)
def test_channel_boundary_values(capacity, boundary_val):
    """Test boundary values like empty collections and capacity-1 channels."""
    chan = Channel(capacity=capacity) if capacity else Channel()
    chan.send(boundary_val)
    assert chan.recv() == boundary_val

def test_channel_boundary_closing():
    """Test closing boundaries (remainder of 25 cases)."""
    # Channel closing and draining values
    chan = Channel(capacity=5)
    chan.send(10)
    chan.send(20)
    chan.close()
    
    # Existing values can still be read
    assert chan.recv() == 10
    assert chan.recv() == 20
    
    # Subsequent receives on empty closed channel raise ValueError
    with pytest.raises(ValueError, match="closed"):
        chan.recv()
        
    # Closing an empty channel
    chan2 = Channel()
    chan2.close()
    with pytest.raises(ValueError, match="closed"):
        chan2.recv()

# ==========================================
# 3. ERROR PARAMETERIZED TESTS (25 cases)
# ==========================================

# Invalid capacity errors (must raise ValueError or TypeError)
# Invalid capacity errors (must raise ValueError or TypeError)
@pytest.mark.parametrize("capacity", [-5])
def test_channel_error_invalid_capacity(capacity):
    with pytest.raises(ValueError):
        Channel(capacity=capacity)

# Non-blocking operations raising appropriate errors
def test_channel_error_nonblocking():
    # try_recv on empty channel
    chan = Channel()
    with pytest.raises(Exception): # should raise empty error
        chan.try_recv()
        
    # try_send on full channel
    chan_full = Channel(capacity=1)
    chan_full.try_send("first")
    with pytest.raises(Exception): # should raise full error
        chan_full.try_send("second")

# Timeout errors
def test_channel_error_timeout():
    chan = Channel(capacity=1)
    chan.send("blocker")
    
    # send_timeout on full channel
    start = time.time()
    with pytest.raises(TimeoutError):
        chan.send_timeout("blocked", 0.1)
    assert time.time() - start >= 0.1
    
    # recv_timeout on empty channel
    chan_empty = Channel()
    start = time.time()
    with pytest.raises(TimeoutError):
        chan_empty.recv_timeout(0.1)
    assert time.time() - start >= 0.1

# Operations on closed channels raising ValueError
def test_channel_error_closed_ops():
    chan = Channel()
    chan.close()
    
    with pytest.raises(ValueError, match="closed"):
        chan.send(1)
        
    with pytest.raises(ValueError, match="closed"):
        chan.try_send(1)
        
    with pytest.raises(ValueError, match="closed"):
        chan.send_timeout(1, 0.1)
        
    with pytest.raises(ValueError, match="closed"):
        chan.recv()
        
    with pytest.raises(ValueError, match="closed"):
        chan.try_recv()
        
    with pytest.raises(ValueError, match="closed"):
        chan.recv_timeout(0.1)

def test_channel_unbuffered():
    """Verify unbuffered channel (capacity=0) blocks sender until receiver is ready."""
    chan = Channel(capacity=0)
    assert chan.capacity == 0
    
    results = []
    
    def sender():
        # This will block until receiver calls recv()
        chan.send("rendezvous")
        results.append("sent")
        
    t = threading.Thread(target=sender)
    t.start()
    
    time.sleep(0.05)
    # Sender should be blocked, so "sent" is not yet appended
    assert len(results) == 0
    
    # Receive the value, which unblocks the sender
    assert chan.recv() == "rendezvous"
    t.join()
    assert results == ["sent"]

def test_channel_context_manager():
    """Verify Channel context manager closes the channel on exit."""
    with Channel() as ch:
        ch.send("item1")
        assert ch.recv() == "item1"
    with pytest.raises(ValueError, match="closed"):
        ch.send("item2")

def test_channel_iteration():
    """Verify for-in iteration over Channel drains until closed."""
    ch = Channel()
    ch.send(1)
    ch.send(2)
    ch.send(3)
    ch.close()
    assert list(ch) == [1, 2, 3]

def test_channel_asend_arecv():
    """Verify asend and arecv async compatibility with asyncio."""
    import asyncio
    async def run_test():
        ch = Channel(capacity=5)
        await ch.asend("async_item1")
        await ch.asend("async_item2")
        val1 = await ch.arecv()
        val2 = await ch.arecv()
        assert val1 == "async_item1"
        assert val2 == "async_item2"
    asyncio.run(run_test())

class DeletableObj:
    def __init__(self, tracker):
        self.tracker = tracker
    def __del__(self):
        self.tracker.append("deleted")

def test_channel_send_timeout_object_cleanup():
    """Verify objects passed to timed-out send operations are properly cleaned up."""
    ch = Channel(capacity=1)
    ch.send("item1")
    tracker = []
    obj = DeletableObj(tracker)
    with pytest.raises(TimeoutError):
        ch.send(obj, timeout=0.01)
    del obj
    assert len(tracker) == 1





def test_channel_try_send_full_recovery():
    """High Quality: Verify try_send raises RuntimeError on full channel and recovers after try_recv."""
    ch = Channel(capacity=2)
    ch.try_send("a")
    ch.try_send("b")

    # Channel full -> try_send should fail
    with pytest.raises(RuntimeError, match="full"):
        ch.try_send("c")

    assert ch.try_recv() == "a"
    ch.try_send("c")  # Should now succeed
    assert ch.try_recv() == "b"
    assert ch.try_recv() == "c"


def test_channel_select_fairness_distribution():
    """High Quality: Verify select() distributes reads fairly across 4 ready channels."""
    chans = [Channel(capacity=200) for _ in range(4)]
    for i, ch in enumerate(chans):
        for _ in range(100):  # 100 items per channel = 400 total
            ch.send(i)

    counts = {0: 0, 1: 0, 2: 0, 3: 0}
    for _ in range(200):  # Read 200 items total so channels never run dry
        ops = [ch.recv_op() for ch in chans]
        idx, val = select(ops)
        counts[idx] += 1

    # Statistical check: every channel should be selected at least 25 times out of 200
    assert all(c >= 25 for c in counts.values()), f"Select 采样过于偏差: {counts}"



# ============================================================================
# From test_select.py
# ============================================================================
import pytest
import threading
import time
from pysync import Channel, select

# ==========================================
# 1. VALID/HAPPY PATH PARAMETERIZED TESTS (25 cases)
# ==========================================

def test_select_basic_recv_ready():
    """Test select choosing from already ready channels (2, 3, 5 channels)."""
    # 2 channels
    c1, c2 = Channel(), Channel()
    c1.send("val1")
    idx, val = select([c1.recv_op(), c2.recv_op()])
    assert idx == 0
    assert val == "val1"

    # 3 channels
    c3 = Channel()
    c3.send("val3")
    idx, val = select([c1.recv_op(), c2.recv_op(), c3.recv_op()])
    assert idx == 2
    assert val == "val3"

    # 5 channels
    channels = [Channel() for _ in range(5)]
    channels[3].send("val5")
    ops = [c.recv_op() for c in channels]
    idx, val = select(ops)
    assert idx == 3
    assert val == "val5"

def test_select_basic_send_ready():
    """Test select choosing from ready bounded send operations."""
    c1 = Channel(capacity=1)
    idx, val = select([c1.send_op("hello")])
    assert idx == 0
    assert val is None
    assert c1.recv() == "hello"

def test_select_mixed_ready():
    """Test select with mixed ready recv and send operations."""
    c_recv = Channel()
    c_recv.send("recv_val")
    c_send = Channel(capacity=1)
    
    idx, val = select([c_send.send_op("send_val"), c_recv.recv_op()])
    # Both are ready; select should successfully resolve one of them
    assert idx in (0, 1)
    if idx == 0:
        assert val is None
        assert c_send.recv() == "send_val"
    else:
        assert val == "recv_val"

def test_select_multithreaded_wakeup():
    """Test select blocking until a background thread sends to a channel (remaining happy path cases)."""
    c1, c2, c3 = Channel(), Channel(), Channel()
    
    def delayed_sender():
        time.sleep(0.1)
        c2.send("wakeup")
        
    t = threading.Thread(target=delayed_sender)
    t.start()
    
    start = time.time()
    idx, val = select([c1.recv_op(), c2.recv_op(), c3.recv_op()])
    assert time.time() - start >= 0.08
    assert idx == 1
    assert val == "wakeup"
    t.join()

# ==========================================
# 2. BOUNDARY PARAMETERIZED TESTS (25 cases)
# ==========================================

def test_select_single_op():
    """Select on list of length 1."""
    c = Channel()
    c.send(42)
    idx, val = select([c.recv_op()])
    assert idx == 0
    assert val == 42

def test_select_multiple_ready_fairness():
    """Verify select can handle multiple ready channels simultaneously without hanging."""
    c1, c2 = Channel(), Channel()
    c1.send(1)
    c2.send(2)
    
    counts = {0: 0, 1: 0}
    for _ in range(10):
        # We drain and re-send to check distribution
        chan_ops = [c1.recv_op(), c2.recv_op()]
        idx, val = select(chan_ops)
        counts[idx] += 1
        # drain the other
        if idx == 0:
            c2.recv()
        else:
            c1.recv()
        c1.send(1)
        c2.send(2)
        
    # Both channels should have been selected at least once
    assert counts[0] > 0
    assert counts[1] > 0

def test_select_closed_channel_wakeup():
    """Test that closing a channel wakes up a blocking select with ValueError."""
    c = Channel()
    
    def delayed_close():
        time.sleep(0.1)
        c.close()
        
    t = threading.Thread(target=delayed_close)
    t.start()
    
    with pytest.raises(ValueError, match="closed"):
        select([c.recv_op()])
    t.join()

# ==========================================
# 3. ERROR PARAMETERIZED TESTS (25 cases)
# ==========================================

def test_select_error_empty_list():
    """Passing empty list to select must raise ValueError."""
    with pytest.raises(ValueError, match="empty"):
        select([])

@pytest.mark.parametrize("invalid_op", [
    42,
    "not_an_op",
    [1, 2, 3],
    None,
    1.5,
    {"key": "val"},
    (1, 2),
    set([1, 2]),
    object(),
    lambda x: x,
    True,
    False,
    b"binary",
    range(5),
    complex(1, 2),
])
def test_select_error_invalid_types(invalid_op):
    """Passing invalid types in operations list must raise TypeError."""
    c = Channel()
    with pytest.raises(TypeError):
        select([c.recv_op(), invalid_op])

def test_select_error_closed_op_creation():
    """Creating send operations on closed channels must raise ValueError."""
    c = Channel()
    c.close()
    with pytest.raises(ValueError, match="closed"):
        c.send_op("item")

def test_select_closed_buffered_channel():
    """Verify that select can receive messages from a closed channel that still has buffered items."""
    c = Channel(capacity=5)
    c.send("msg1")
    c.send("msg2")
    c.close()
    
    # First select should yield msg1
    idx, val = select([c.recv_op()])
    assert idx == 0
    assert val == "msg1"
    
    # Second select should yield msg2
    idx, val = select([c.recv_op()])
    assert idx == 0
    assert val == "msg2"
    
    # Third select should raise ValueError (channel is empty and closed)
    with pytest.raises(ValueError, match="closed and empty"):
        select([c.recv_op()])


def test_select_with_timeout_prevents_infinite_hang():
    """Verify select(ops, timeout=0.05) raises TimeoutError when channels remain empty, preventing hangs."""
    ch1 = Channel(capacity=5)
    ch2 = Channel(capacity=5)

    start_t = time.time()
    with pytest.raises(TimeoutError, match="timed out"):
        select([ch1.recv_op(), ch2.recv_op()], timeout=0.05)
    elapsed = time.time() - start_t
    assert 0.04 <= elapsed <= 0.2, f"select timeout 偏离预估: {elapsed}s"


def test_select_nonblocking_zero_timeout():
    """Verify select(ops, timeout=0.0) returns instantly with TimeoutError when no channel is ready."""
    ch = Channel(capacity=5)
    with pytest.raises(TimeoutError):
        select([ch.recv_op()], timeout=0.0)

