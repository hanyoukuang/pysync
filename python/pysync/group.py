import threading

class ThreadGroup:
    """
    A structured concurrency thread manager.
    
    Ensures spawned thread lifetimes are securely bounded within the context manager block.
    Upon context exit, it blocks and joins all spawned child threads, including any threads
    dynamically spawned by child tasks. Any exceptions raised in tasks are aggregated and
    propagated as an ExceptionGroup.
    
    Examples:
        >>> from pysync import ThreadGroup
        >>> results = []
        >>> with ThreadGroup() as tg:
        ...     tg.spawn(lambda: results.append(1))
        ...     tg.spawn(lambda: results.append(2))
        >>> sorted(results)
        [1, 2]
    """
    def __init__(self):
        self._threads = []
        self._errors = []
        self._closed = False
        self._lock = threading.Lock()

    def spawn(self, target, *args, **kwargs):
        """
        Spawn a new physical OS thread inside the thread group.
        
        Args:
            target: The callable function to execute in the thread.
            *args: Positional arguments to forward to the target.
            **kwargs: Keyword arguments to forward to the target.
            
        Returns:
            The spawned threading.Thread object.
        """
        with self._lock:
            if self._closed:
                raise RuntimeError("Cannot spawn task on closed ThreadGroup")

        def wrapper():
            try:
                target(*args, **kwargs)
            except BaseException as e:
                with self._lock:
                    self._errors.append(e)

        t = threading.Thread(target=wrapper)
        with self._lock:
            if self._closed:
                raise RuntimeError("Cannot spawn task on closed ThreadGroup")
            t.start()
            self._threads.append(t)
        return t

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        joined = set()
        while True:
            with self._lock:
                unjoined = [t for t in self._threads if t not in joined]
                if not unjoined:
                    self._closed = True
                    break
            for t in unjoined:
                if t.is_alive() or t.ident is not None:
                    t.join()
                joined.add(t)

        # If the main context block body raised an exception, prioritize it over thread exceptions.
        if exc_val is not None:
            return False

        # Propagate the aggregated exceptions
        if self._errors:
            if len(self._errors) == 1:
                raise self._errors[0]
            raise ExceptionGroup("Multiple exceptions occurred inside ThreadGroup tasks", self._errors)

