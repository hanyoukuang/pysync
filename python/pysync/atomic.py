import threading

def _wrap_i64(val: int) -> int:
    val = val & 0xFFFFFFFFFFFFFFFF
    if val >= 0x8000000000000000:
        val -= 0x10000000000000000
    return val

class AtomicInteger:
    """
    A thread-safe 64-bit signed integer optimized for Python 3.14 free-threaded runtime.
    
    Eliminates PyO3 FFI boundary overhead by using CPython's native C-level lock mechanism
    and 64-bit wrapping.
    
    Examples:
        >>> from pysync import AtomicInteger
        >>> a = AtomicInteger(10)
        >>> a.fetch_add(5)
        10
        >>> a.get()
        15
        >>> a.compare_and_set(15, 100)
        True
        >>> a.get()
        100
    """
    __slots__ = ('_value', '_lock')

    def __init__(self, value: int = 0):
        if not isinstance(value, int) or isinstance(value, bool):
            raise TypeError("AtomicInteger initial value must be an integer")
        self._value = _wrap_i64(value)
        self._lock = threading.Lock()

    def get(self) -> int:
        """Load the current 64-bit integer value atomically."""
        with self._lock:
            return self._value

    def set(self, value: int) -> None:
        """Store a new 64-bit integer value atomically."""
        if not isinstance(value, int) or isinstance(value, bool):
            raise TypeError("Value must be an integer")
        with self._lock:
            self._value = _wrap_i64(value)

    def fetch_add(self, delta: int) -> int:
        """Atomically add delta to the value and return the OLD value."""
        if not isinstance(delta, int) or isinstance(delta, bool):
            raise TypeError("Delta must be an integer")
        with self._lock:
            old = self._value
            self._value = _wrap_i64(self._value + delta)
            return old

    def fetch_sub(self, delta: int) -> int:
        """Atomically subtract delta from the value and return the OLD value."""
        if not isinstance(delta, int) or isinstance(delta, bool):
            raise TypeError("Delta must be an integer")
        with self._lock:
            old = self._value
            self._value = _wrap_i64(self._value - delta)
            return old

    def increment(self) -> int:
        """Atomically increment the value by 1 and return the NEW value."""
        with self._lock:
            self._value = _wrap_i64(self._value + 1)
            return self._value

    def decrement(self) -> int:
        """Atomically decrement the value by 1 and return the NEW value."""
        with self._lock:
            self._value = _wrap_i64(self._value - 1)
            return self._value

    def get_and_set(self, new_value: int) -> int:
        """Atomically swap the value with new_value and return the OLD value."""
        if not isinstance(new_value, int) or isinstance(new_value, bool):
            raise TypeError("New value must be an integer")
        with self._lock:
            old = self._value
            self._value = _wrap_i64(new_value)
            return old

    def compare_and_set(self, expected: int, new_value: int) -> bool:
        """
        Atomically compare the current value to expected, and if equal, swap with new_value.
        
        Returns:
            True if the swap succeeded, False otherwise.
        """
        if not isinstance(expected, int) or isinstance(expected, bool) or not isinstance(new_value, int) or isinstance(new_value, bool):
            raise TypeError("Expected and new values must be integers")
        expected_wrapped = _wrap_i64(expected)
        with self._lock:
            if self._value == expected_wrapped:
                self._value = _wrap_i64(new_value)
                return True
            return False

    def __repr__(self) -> str:
        return f"AtomicInteger({self.get()})"

    def __str__(self) -> str:
        return str(self.get())


class AtomicBoolean:
    """
    A thread-safe boolean variable optimized for Python 3.14 free-threaded runtime.
    
    Examples:
        >>> from pysync import AtomicBoolean
        >>> b = AtomicBoolean(False)
        >>> b.get()
        False
        >>> b.get_and_set(True)
        False
        >>> b.get()
        True
    """
    __slots__ = ('_value', '_lock')

    def __init__(self, value: bool = False):
        if not isinstance(value, bool):
            raise TypeError("AtomicBoolean initial value must be a boolean")
        self._value = value
        self._lock = threading.Lock()

    def get(self) -> bool:
        """Load the current boolean value atomically."""
        with self._lock:
            return self._value

    def set(self, value: bool) -> None:
        """Store a new boolean value atomically."""
        if not isinstance(value, bool):
            raise TypeError("Value must be a boolean")
        with self._lock:
            self._value = value

    def get_and_set(self, new_value: bool) -> bool:
        """Atomically swap the boolean with new_value and return the OLD value."""
        if not isinstance(new_value, bool):
            raise TypeError("New value must be a boolean")
        with self._lock:
            old = self._value
            self._value = new_value
            return old

    def compare_and_set(self, expected: bool, new_value: bool) -> bool:
        """
        Atomically compare the current value to expected, and if equal, swap with new_value.
        
        Returns:
            True if the swap succeeded, False otherwise.
        """
        if not isinstance(expected, bool) or not isinstance(new_value, bool):
            raise TypeError("Expected and new values must be booleans")
        with self._lock:
            if self._value == expected:
                self._value = new_value
                return True
            return False

    def __repr__(self) -> str:
        return f"AtomicBoolean({self.get()})"

    def __str__(self) -> str:
        return str(self.get())
