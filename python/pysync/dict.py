from pysync._pysync import ConcurrentMap

_sentinel = object()

class ConcurrentDict(ConcurrentMap):
    """
    A highly concurrent, thread-safe dictionary wrapping dashmap.
    
    Fully conforms to Python's mapping protocols. Optimized for multi-threaded
    reads and writes under free-threaded Python 3.14 (GIL-free).
    
    Examples:
        >>> from pysync import ConcurrentDict
        >>> d = ConcurrentDict()
        >>> d["key"] = 100
        >>> "key" in d
        True
        >>> len(d)
        1
        >>> d.get("non-existent", 42)
        42
    """
    __hash__ = None  # type: ignore[assignment]

    def __getitem__(self, key):
        found, val = self.get_val(key)
        if not found:
            raise KeyError(key)
        return val

    def __setitem__(self, key, value):
        self.set(key, value)

    def __delitem__(self, key):
        if not self.delete(key):
            raise KeyError(key)

    def __contains__(self, key):
        return self.contains_key(key)

    def __len__(self):
        return self.len()

    def __repr__(self):
        items_str = ", ".join(f"{k!r}: {v!r}" for k, v in self.items())
        return f"{{{items_str}}}"

    def __iter__(self):
        return iter(self.keys())

    def __eq__(self, other):
        if not isinstance(other, (dict, ConcurrentDict)):
            return False
        if len(self) != len(other):
            return False
        for k, v in self.items():
            if k not in other or other[k] != v:
                return False
        return True

    def get(self, key, default=None):
        """
        Get the value associated with the key, or return the default value if key is not found.
        
        Args:
            key: The key to look up.
            default: The default value to return if key is not found.
            
        Returns:
            The value if found, otherwise the default.
        """
        found, val = self.get_val(key)
        if not found:
            return default
        return val

    get_default = get

    def pop(self, key, default=_sentinel):
        """
        Remove the specified key and return the corresponding value.
        If the key is not found, default is returned if given, otherwise KeyError is raised.
        
        Args:
            key: The key to remove.
            default: The default value to return if key is not found.
            
        Returns:
            The removed value if key is found, otherwise the default value.
            
        Raises:
            KeyError: If key is not found and no default is provided.
        """
        found, val = self.pop_val(key)
        if not found:
            if default is _sentinel:
                raise KeyError(key)
            return default
        return val

    def setdefault(self, key, default=None):
        """
        Return the value of key if present, otherwise set key to default and return default.
        """
        found, val = self.get_val(key)
        if found:
            return val
        self.set(key, default)
        return default

    def update(self, other=None, **kwargs):
        """
        Update the dictionary with key-value pairs from other, overwriting existing keys.
        """
        if other is not None:
            if hasattr(other, 'keys'):
                for k in other.keys():
                    self.set(k, other[k])
            elif hasattr(other, 'items'):
                for k, v in other.items():
                    self.set(k, v)
            else:
                for k, v in other:
                    self.set(k, v)
        for k, v in kwargs.items():
            self.set(k, v)

    def popitem(self):
        """
        Remove and return a (key, value) pair from the dictionary.
        Raises KeyError if the dictionary is empty.

        Thread-safe: uses atomic pop_val() to avoid TOCTOU races where two
        threads could popitem() the same key simultaneously.
        """
        # BUG-1 fix: iterate snapshot keys and use atomic pop_val().
        # If another thread already popped a candidate key, skip to the next one.
        # Retry the full snapshot if every candidate was taken concurrently.
        while True:
            items = self.items()
            if not items:
                raise KeyError("popitem(): dictionary is empty")
            for k, _ in items:
                found, v = self.pop_val(k)
                if found:
                    return (k, v)
            # All snapshot candidates were stolen by concurrent popitem() calls;
            # re-snapshot and retry (or raise if now truly empty).

    def copy(self):
        """Return a shallow copy of the ConcurrentDict."""
        new_dict = ConcurrentDict()
        for k, v in self.items():
            new_dict.set(k, v)
        return new_dict

    def iter_keys(self):
        """Yield keys lazily."""
        for k in self.keys():
            yield k

    def iter_values(self):
        """Yield values lazily."""
        for v in self.values():
            yield v

    def iter_items(self):
        """Yield key-value pairs lazily."""
        for item in self.items():
            yield item

    def clear(self):
        """Remove all items from the ConcurrentDict."""
        for k in self.keys():
            self.delete(k)

    @classmethod
    def fromkeys(cls, iterable, value=None):
        """Create a new ConcurrentDict with keys from iterable and values set to value."""
        d = cls()
        for k in iterable:
            d[k] = value
        return d



