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
        def wrapper():
            try:
                target(*args, **kwargs)
            except BaseException as e:
                with self._lock:
                    self._errors.append(e)

        t = threading.Thread(target=wrapper)
        t.start()
        with self._lock:
            self._threads.append(t)
        return t

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Exit the ThreadGroup context. Joins all spawned threads, then aggregates
        any exceptions from child threads together with the context body exception
        into a single ExceptionGroup (Python 3.11+). Single exceptions are raised
        directly; multiple exceptions are always aggregated to prevent silent loss
        of diagnostic information.
        """
        joined = set()
        while True:
            with self._lock:
                unjoined = [t for t in self._threads if t not in joined]
                if not unjoined:
                    break
            for t in unjoined:
                if t.is_alive() or t.ident is not None:
                    t.join()
                joined.add(t)

        # Collect all exceptions: context body + child thread exceptions.
        # PEP 654 / Trio / asyncio.TaskGroup convention:
        #   single exception → raise directly
        #   multiple exceptions → ExceptionGroup
        
        # Aggregate main block exception with all child thread exceptions
        all_errors = []
        if exc_val is not None:
            all_errors.append(exc_val)
        with self._lock:
            all_errors.extend(self._errors)

        if not all_errors:
            return False

        # If only the main block raised an exception, let normal exception propagation happen
        if len(all_errors) == 1 and exc_val is not None:
            return False

        # If only a single child thread raised an exception and main block succeeded, raise it directly
        if len(all_errors) == 1:
            raise all_errors[0]

        # Aggregate multiple exceptions into an ExceptionGroup
        raise ExceptionGroup("Multiple exceptions occurred inside ThreadGroup tasks", all_errors)
