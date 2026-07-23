from typing import Any, Callable, List, Optional, Tuple, TypeVar
from concurrent.futures import Future

K = TypeVar("K")
V = TypeVar("V")

class Channel:
    """
    A fast, thread-safe message passing channel based on Rust's crossbeam-channel.
    
    Supports bounded and unbounded capacities, blocking send/recv with timeouts,
    and Go-style multiplexed selection.
    
    Examples:
        >>> from pysync import Channel
        >>> ch = Channel(capacity=10)
        >>> ch.send("hello")
        >>> ch.recv()
        'hello'
        >>> ch.close()
    """
    def __init__(self, capacity: Optional[int] = None) -> None:
        """
        Initialize the Channel.
        
        Args:
            capacity: Bounded capacity. If None, the channel is unbounded.
        """
        ...
        
    def send(self, item: Any, timeout: Optional[float] = None) -> None:
        """
        Send an item to the channel. Blocks if bounded channel is full.
        
        Args:
            item: The Python object to send.
            timeout: Optional maximum wait time in seconds.
            
        Raises:
            ValueError: If the channel is closed.
            TimeoutError: If the send operation times out.
        """
        ...
        
    def recv(self, timeout: Optional[float] = None) -> Any:
        """
        Receive an item from the channel. Blocks if the channel is empty.
        
        Args:
            timeout: Optional maximum wait time in seconds.
            
        Returns:
            The received Python object.
            
        Raises:
            ValueError: If the channel is closed and empty.
            TimeoutError: If the receive operation times out.
        """
        ...

    def try_send(self, item: Any) -> None: ...
    def try_recv(self) -> Any: ...
    def send_timeout(self, item: Any, timeout: float) -> None: ...
    def recv_timeout(self, timeout: float) -> Any: ...
        
    def close(self) -> None:
        """
        Close the channel, preventing any further sends. Existing buffered items
        can still be received.
        """
        ...
        
    def __iter__(self) -> 'Channel': ...
    def __next__(self) -> Any: ...
    def __enter__(self) -> 'Channel': ...
    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None: ...

    def recv_op(self) -> Any:
        """
        Create a RecvOp operation instance for use in `pysync.select()`.
        
        Returns:
            A RecvOp object.
            
        Raises:
            ValueError: If the channel is closed.
        """
        ...
        
    def send_op(self, item: Any) -> Any:
        """
        Create a SendOp operation instance for use in `pysync.select()`.
        
        Args:
            item: The Python object to send when selected.
            
        Returns:
            A SendOp object.
            
        Raises:
            ValueError: If the channel is closed.
        """
        ...
        
    async def asend(self, item: Any, timeout: Optional[float] = None) -> None: ...
    async def arecv(self, timeout: Optional[float] = None) -> Any: ...

    @property
    def capacity(self) -> Optional[int]:
        """The capacity of the channel (None if unbounded)."""
        ...

class ConcurrentMap:
    """
    A highly concurrent, thread-safe hash map with dynamically configurable shard count.
    
    Operates without GIL contention in free-threaded Python 3.14.
    """
    def __init__(self, shard_count: Optional[int] = None) -> None: ...
    @property
    def shard_count(self) -> int: ...
    
    def get(self, key: Any, default: Optional[Any] = None) -> Optional[Any]:
        """
        Get the value associated with the key.
        
        Args:
            key: The key to look up.
            
        Returns:
            The value if found, otherwise None.
        """
        ...

    def get_val(self, key: Any) -> Tuple[bool, Any]:
        """
        Retrieve the value associated with the key as an atomic tuple `(found: bool, val: Any)`.
        """
        ...

    def get_or_insert(self, key: Any, default: Optional[Any] = None) -> Any:
        """
        Atomically retrieve the value associated with key, or insert and return default if key is not present.
        """
        ...
        
    def set(self, key: Any, value: Any) -> None:
        """
        Associate the key with the value.
        
        Args:
            key: The key to set.
            value: The value to set.
            
        Raises:
            TypeError: If the key is not hashable.
        """
        ...
        
    def delete(self, key: Any) -> bool:
        """
        Delete the key and its value from the map.
        
        Args:
            key: The key to delete.
            
        Returns:
            True if the key existed and was deleted, False otherwise.
        """
        ...
        
    def contains_key(self, key: Any) -> bool:
        """
        Check if the key exists in the map.
        
        Args:
            key: The key to check.
            
        Returns:
            True if found, False otherwise.
        """
        ...
        
    def len(self) -> int:
        """The number of elements in the map."""
        ...

    def pop_val(self, key: Any) -> Tuple[bool, Any]:
        """
        Atomically remove the key and return a tuple (found, value).
        """
        ...

    def keys(self) -> List[Any]: ...
    def values(self) -> List[Any]: ...
    def items(self) -> List[Tuple[Any, Any]]: ...
    def clear(self) -> None: ...

class ConcurrentDict(ConcurrentMap):
    """
    A highly concurrent, thread-safe dictionary wrapping dashmap.
    
    Fully conforms to Python's mapping protocols. Optimized for multi-threaded
    reads and writes under free-threaded Python 3.14.
    
    Examples:
        >>> from pysync import ConcurrentDict
        >>> d = ConcurrentDict()
        >>> d["key"] = 100
        >>> "key" in d
        True
        >>> len(d)
        1
        >>> d.get_default("non-existent", 42)
        42
    """
    __hash__: Any = None
    def __getitem__(self, key: Any) -> Any: ...
    def __setitem__(self, key: Any, value: Any) -> None: ...
    def __delitem__(self, key: Any) -> None: ...
    def __contains__(self, key: Any) -> bool: ...
    def __len__(self) -> int: ...
    def __repr__(self) -> str: ...
    def get_default(self, key: Any, default: Optional[Any] = None) -> Any:
        """
        Get the value associated with the key, or return the default value if key is not found.
        
        Args:
            key: The key to look up.
            default: The default value to return if key is not found.
            
        Returns:
            The value if found, otherwise the default.
        """
        ...

    def pop(self, key: Any, default: Any = ...) -> Any:
        """
        Remove the specified key and return the corresponding value.
        If the key is not found, default is returned if given, otherwise KeyError is raised.
        """
        ...

    def clear(self) -> None:
        """Remove all items from the ConcurrentDict."""
        ...

    @classmethod
    def fromkeys(cls, iterable: Any, value: Any = None) -> 'ConcurrentDict':
        """Create a new ConcurrentDict with keys from iterable and values set to value."""
        ...


class ThreadPool:
    """
    An OS-level physical thread pool scheduler for heavy parallel computing.
    
    Avoids GIL contention and allows true parallel multi-threaded execution.
    
    Warning:
        To prevent thread pool starvation deadlocks, do not submit nested tasks
        to the same ThreadPool and wait on their futures synchronously (e.g.,
        via future.result()). Doing so under heavy concurrency will consume
        all worker threads, preventing nested tasks from ever running.
    
    Examples:
        >>> from pysync import ThreadPool
        >>> pool = ThreadPool(num_workers=4)
        >>> future = pool.submit(lambda x: x * 2, 21)
        >>> future.result()
        42
        >>> pool.shutdown()
    """
    def __init__(self, num_workers: Optional[int] = None) -> None:
        """
        Initialize the ThreadPool.
        
        Args:
            num_workers: Number of worker threads.
        """
        ...
        
    def submit(self, func: Callable[..., Any], *args: Any, **kwargs: Any) -> Future[Any]:
        """
        Submit a task for parallel execution in the thread pool.
        
        Args:
            func: The target function.
            *args: Positional arguments for the target.
            **kwargs: Keyword arguments for the target.
            
        Returns:
            A standard concurrent.futures.Future object.
        """
        ...
        
    def shutdown(self, wait: bool = True, cancel_futures: bool = False) -> None:
        """Shutdown the pool, joining all worker threads."""
        ...

class AtomicInteger:
    """
    A lock-free, thread-safe 64-bit integer using native CPU atomic operations.
    
    Guarantees atomic modification and comparison without thread blocking or locks.
    
    Examples:
        >>> from pysync import AtomicInteger
        >>> a = AtomicInteger(10)
        >>> a.increment()
        11
        >>> a.compare_and_set(11, 20)
        True
        >>> a.get()
        20
    """
    def __init__(self, initial: int = 0) -> None:
        """
        Initialize the AtomicInteger.
        
        Args:
            initial: Initial 64-bit signed integer value.
        """
        ...
        
    def get(self, ordering: Optional[str] = None) -> int:
        """Get the current value."""
        ...
        
    def set(self, val: int, ordering: Optional[str] = None) -> None:
        """Set the value."""
        ...
        
    def fetch_add(self, delta: int, ordering: Optional[str] = None) -> int:
        """
        Atomically add delta and return the OLD value.
        
        Args:
            delta: Value to add.
            ordering: Optional memory ordering ("seq_cst", "relaxed", "acquire", "release", "acq_rel").
            
        Returns:
            The old value before addition.
        """
        ...

    def add_and_get(self, delta: int, ordering: Optional[str] = None) -> int: ...
    def sub_and_get(self, delta: int, ordering: Optional[str] = None) -> int: ...
        
    def fetch_sub(self, delta: int, ordering: Optional[str] = None) -> int:
        """
        Atomically subtract delta and return the OLD value.
        
        Args:
            delta: Value to subtract.
            ordering: Optional memory ordering.
            
        Returns:
            The old value before subtraction.
        """
        ...
        
    def increment(self, ordering: Optional[str] = None) -> int:
        """
        Atomically increment the integer by 1 and return the NEW value.
        
        Returns:
            The new incremented value.
        """
        ...
        
    def decrement(self, ordering: Optional[str] = None) -> int:
        """
        Atomically decrement the integer by 1 and return the NEW value.
        
        Returns:
            The new decremented value.
        """
        ...

    def increment_relaxed(self) -> int: ...
    def decrement_relaxed(self) -> int: ...
    def fetch_add_relaxed(self, delta: int) -> int: ...
    def add_and_get_relaxed(self, delta: int) -> int: ...
    def fetch_sub_relaxed(self, delta: int) -> int: ...
    def sub_and_get_relaxed(self, delta: int) -> int: ...
        
    def get_and_set(self, val: int, ordering: Optional[str] = None) -> int:
        """
        Atomically set the new value and return the OLD value.
        
        Args:
            val: The new value.
            ordering: Optional memory ordering.
            
        Returns:
            The old value.
        """
        ...
        
    def compare_and_set(self, expected: int, new_val: int, ordering: Optional[str] = None) -> bool:
        """
        Atomically compare the current value to expected, and if equal, set it to new_val.
        
        Args:
            expected: The expected value.
            new_val: The new value to set.
            ordering: Optional memory ordering.
            
        Returns:
            True if the exchange succeeded, False otherwise.
        """
        ...

class AtomicBoolean:
    """
    A lock-free, thread-safe boolean using native CPU atomic operations.
    
    Examples:
        >>> from pysync import AtomicBoolean
        >>> b = AtomicBoolean(False)
        >>> b.compare_and_set(False, True)
        True
        >>> b.get()
        True
    """
    def __init__(self, initial: bool = False) -> None:
        """
        Initialize the AtomicBoolean.
        
        Args:
            initial: Initial boolean value.
        """
        ...
        
    def get(self) -> bool:
        """Get the current value."""
        ...
        
    def set(self, val: bool) -> None:
        """Set the value."""
        ...
        
    def get_and_set(self, val: bool) -> bool:
        """
        Atomically set the new value and return the OLD value.
        
        Args:
            val: The new boolean value.
            
        Returns:
            The old boolean value.
        """
        ...
        
    def compare_and_set(self, expected: bool, new_val: bool) -> bool:
        """
        Atomically compare the current value to expected, and if equal, set it to new_val.
        
        Args:
            expected: The expected boolean value.
            new_val: The new boolean value to set.
            
        Returns:
            True if the exchange succeeded, False otherwise.
        """
        ...

class RwLockReadGuard:
    """An RAII guard for holding shared read access of an RwLock."""
    def __enter__(self) -> RwLockReadGuard: ...
    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None: ...

class RwLockWriteGuard:
    """An RAII guard for holding exclusive write access of an RwLock."""
    def __enter__(self) -> RwLockWriteGuard: ...
    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None: ...

class RwLock:
    """
    A native reader-writer lock based on parking_lot::RwLock.
    
    Allows multiple concurrent readers or a single exclusive writer. 
    Acquisition releases the GIL while blocking to prevent deadlocks.
    
    Examples:
        >>> from pysync import RwLock
        >>> lock = RwLock()
        >>> with lock.read():
        ...     # Perform concurrent read operations
        ...     pass
        >>> with lock.write():
        ...     # Perform exclusive write operations
        ...     pass
    """
    def __init__(self) -> None: ...
    
    def read(self) -> RwLockReadGuard:
        """
        Acquire shared read access. Multiple threads can read concurrently.
        
        Returns:
            A context-manager RwLockReadGuard.
        """
        ...
        
    def write(self) -> RwLockWriteGuard:
        """
        Acquire exclusive write access. Blocks other readers and writers.
        
        Returns:
            A context-manager RwLockWriteGuard.
        """
        ...

    def acquire_read(self) -> None: ...
    def release_read(self) -> None: ...
    def try_acquire_read(self) -> bool: ...
    def acquire_write(self) -> None: ...
    def release_write(self) -> None: ...
    def try_acquire_write(self) -> bool: ...

def select(
    ops: List[Any],
    timeout: Optional[float] = None
) -> Tuple[int, Optional[Any]]:
    """
    Block until one of the channel operations (recv_op or send_op) is ready.
    
    Go-style multiplexing for multiple channel operations.
    
    Args:
        ops: A list of RecvOp or SendOp objects.
        
    Returns:
        A tuple containing:
            - The index of the selected operation in the input list.
            - The received item (for RecvOp) or None (for SendOp).
            
    Examples:
        >>> from pysync import Channel, select
        >>> c1, c2 = Channel(), Channel()
        >>> c1.send("first")
        >>> idx, val = select([c1.recv_op(), c2.recv_op()])
        >>> idx, val
        (0, 'first')
    """
    ...

class ThreadGroup:
    """
    A structured concurrency thread manager.
    
    Ensures spawned thread lifetimes are securely bounded. Upon context exit,
    it joins all spawned threads. Any exceptions raised in tasks are aggregated
    and propagated as an ExceptionGroup.
    
    Examples:
        >>> from pysync import ThreadGroup
        >>> results = []
        >>> with ThreadGroup() as tg:
        ...     tg.spawn(lambda: results.append(1))
        ...     tg.spawn(lambda: results.append(2))
        >>> sorted(results)
        [1, 2]
    """
    def __init__(self) -> None: ...
    
    def spawn(self, target: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        """
        Spawn a thread inside the group.
        
        Args:
            target: Target function to run in parallel.
            *args: Positional arguments for the target.
            **kwargs: Keyword arguments for the target.
            
        Returns:
            The spawned Thread instance.
        """
        ...
        
    def __enter__(self) -> ThreadGroup: ...
    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> Optional[bool]: ...

class Actor:
    """
    A lightweight Actor base class for lock-free isolated state concurrency.
    
    Intercepts public method calls and queues them asynchronously in a mailbox,
    executing them sequentially in a dedicated background thread.
    
    Examples:
        >>> from pysync import Actor
        >>> class Counter(Actor):
        ...     def __init__(self):
        ...         super().__init__()
        ...         self.val = 0
        ...     def inc(self):
        ...         self.val += 1
        ...         return self.val
        >>> c = Counter()
        >>> f = c.inc()
        >>> f.result()
        1
        >>> c.stop()
    """
    def __init__(self, mailbox_capacity: Optional[int] = None) -> None: ...
    def tell(self, method_name: str, *args: Any, **kwargs: Any) -> None:
        """
        Send a non-blocking fire-and-forget message to the actor.
        
        Does not create or return a Future, maximizing throughput.
        """
        ...
    def stop(self) -> None:
        """
        Gracefully stop the actor, waiting for mailbox backlog to finish.
        """
        ...
