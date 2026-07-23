import pytest
import threading
import time
from pysync import Channel, select

# ==========================================
# 1. Execution & Concurrency Tests
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
    """Test select blocking until a background thread sends to a channel."""
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
# 2. Boundary Tests
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
# 3. Error Handling Tests
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

