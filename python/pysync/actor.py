import threading
from typing import Any
from concurrent.futures import Future
from pysync._pysync import Channel

def _wrap_init(original_init):
    """
    Wrap an __init__ method to track recursive initialization depth.
    Sets _initialized = True only when the most-derived class's __init__ exits without exception.
    This allows subclasses to initialize public attributes without triggering
    AttributeError during construction.
    """
    def wrapped(self, *args, **kwargs):
        try:
            depth = object.__getattribute__(self, '_init_depth')
        except AttributeError:
            depth = 0
        object.__setattr__(self, '_init_depth', depth + 1)
        
        success = False
        try:
            original_init(self, *args, **kwargs)
            success = True
        finally:
            try:
                current_depth = object.__getattribute__(self, '_init_depth') - 1
            except AttributeError:
                current_depth = 0
            object.__setattr__(self, '_init_depth', current_depth)
            if success and current_depth <= 0:
                object.__setattr__(self, '_initialized', True)
    return wrapped

class Actor:
    """
    An Actor model implementation guaranteeing thread-safe state isolation.
    
    All public methods and properties invoked on an Actor from external threads are intercepted
    and executed sequentially on a dedicated background OS thread via an internal Channel mailbox.
    Public state attributes cannot be directly read or mutated from outside the Actor thread,
    preventing data races in multi-threaded GIL-free environments.
    
    Supports lifecycle hooks `on_start()` and `on_stop()`, and supervision hook `on_error()`.
    
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
    def __new__(cls, *args, **kwargs):
        obj = super().__new__(cls)
        object.__setattr__(obj, '_initialized', False)
        object.__setattr__(obj, '_init_depth', 0)
        return obj

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        if '__init__' in cls.__dict__:
            cls.__init__ = _wrap_init(cls.__init__)  # type: ignore[method-assign]

    def __init__(self, mailbox_capacity=None):
        self._mailbox = Channel(capacity=mailbox_capacity)
        self._thread = None
        self._stopped = False
        self._lock = threading.Lock()

    def on_start(self):
        """Lifecycle hook called on the worker thread when the Actor starts."""
        pass

    def on_stop(self):
        """Lifecycle hook called on the worker thread when the Actor stops."""
        pass

    def on_error(self, exc: BaseException, method_name: str, args: tuple, kwargs: dict) -> bool:
        """
        Supervision hook called when a method invocation raises an exception.
        Override to implement custom logging, retry, or recovery strategies.
        """
        return False

    def _ensure_thread_started(self):
        """Helper to lazily start the background worker thread using double-checked locking."""
        if object.__getattribute__(self, '_thread') is not None:
            return

        with object.__getattribute__(self, '_lock'):
            if object.__getattribute__(self, '_thread') is not None:
                return

            thread = threading.Thread(target=self._run_loop, name=f"Actor-{type(self).__name__}")
            object.__setattr__(self, '_thread', thread)
            thread.start()

    def _run_loop(self):
        try:
            self.on_start()
        except BaseException as e:
            # Route on_start() failures through the on_error supervision hook
            # hook so users can observe/handle Actor startup failures, instead of
            # swallowing the exception silently.
            try:
                self.on_error(e, "on_start", (), {})
            except BaseException:
                pass

        try:
            while True:
                try:
                    msg = self._mailbox.recv()
                    if msg is None:  # Shutdown sentinel
                        break
                    method_name, args, kwargs, future = msg
                    try:
                        # Bypass the interceptor to get the actual method or property
                        method = object.__getattribute__(self, method_name)
                        if callable(method):
                            result = method(*args, **kwargs)
                        else:
                            result = method
                        if future is not None:
                            future.set_result(result)
                    except BaseException as e:
                        handled = False
                        try:
                            handled = bool(self.on_error(e, method_name, args, kwargs))
                        except BaseException:
                            pass
                        if future is not None:
                            if handled:
                                future.set_result(None)
                            else:
                                future.set_exception(e)
                    finally:
                        method = None
                        args = None
                        kwargs = None
                        future = None
                        result = None
                        msg = None
                except ValueError:  # Mailbox closed
                    break
        finally:
            # Close mailbox first so any concurrent/subsequent send() fails fast with ValueError,
            # then drain all remaining messages in the buffer to set exception on their futures.
            try:
                mailbox = object.__getattribute__(self, '_mailbox')
                try:
                    mailbox.close()
                except Exception:
                    pass
                while True:
                    try:
                        msg = mailbox.recv()
                        if msg is None:
                            continue
                        _, _, _, future = msg
                        if future is not None and not future.done():
                            future.set_exception(RuntimeError("Actor is stopped"))
                    except ValueError:
                        # Channel is closed and empty
                        break
                    except Exception:
                        break
            except Exception:
                pass

            try:
                self.on_stop()
            except BaseException:
                pass

    def __getattribute__(self, name):
        # Private methods/attributes and stop() are returned directly without interception
        if name.startswith('_') or name == 'stop':
            return object.__getattribute__(self, name)

        current_thread = threading.current_thread()
        try:
            actor_thread = object.__getattribute__(self, '_thread')
        except AttributeError:
            actor_thread = None

        # Internal thread or self-invocation: return attribute directly to avoid deadlock or stopped check
        if actor_thread is not None and current_thread == actor_thread:
            return object.__getattribute__(self, name)

        # Check if attribute is a property descriptor on the class
        cls_attr = getattr(type(self), name, None)
        is_prop = isinstance(cls_attr, property)

        if is_prop:
            self._ensure_thread_started()
            def async_prop_get():
                future: Future[Any] = Future()
                try:
                    if object.__getattribute__(self, '_stopped'):
                        future.set_exception(RuntimeError("Actor is stopped"))
                        return future

                    object.__getattribute__(self, '_mailbox').send((name, (), {}, future))
                except (ValueError, RuntimeError) as err:
                    if not future.done():
                        future.set_exception(RuntimeError(f"Actor is stopped: {err}"))
                return future
            return async_prop_get()

        try:
            attr = object.__getattribute__(self, name)
        except AttributeError:
            raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")

        if callable(attr):
            self._ensure_thread_started()
            def async_call(*args, **kwargs):
                future: Future[Any] = Future()
                try:
                    if object.__getattribute__(self, '_stopped'):
                        future.set_exception(RuntimeError("Actor is stopped"))
                        return future

                    object.__getattribute__(self, '_mailbox').send((name, args, kwargs, future))
                except (ValueError, RuntimeError) as err:
                    if not future.done():
                        future.set_exception(RuntimeError(f"Actor is stopped: {err}"))
                return future
            return async_call

        # Protect Actor state: forbid direct read access to public attributes to prevent data races
        try:
            initialized = object.__getattribute__(self, '_initialized')
        except AttributeError:
            initialized = False

        if initialized:
            raise AttributeError(
                f"Public state attribute '{name}' cannot be accessed directly on Actor. "
                "Use getter methods to read state safely."
            )
        return attr

    def __setattr__(self, name, value):
        current_thread = threading.current_thread()
        try:
            actor_thread = object.__getattribute__(self, '_thread')
        except AttributeError:
            actor_thread = None

        try:
            initialized = object.__getattribute__(self, '_initialized')
        except AttributeError:
            initialized = False

        # Only allow mutations during initialization, on the actor's thread, or for private attributes
        if not initialized or name.startswith('_') or (actor_thread is not None and current_thread == actor_thread):
            object.__setattr__(self, name, value)
            return

        raise AttributeError(
            f"Public state attribute '{name}' cannot be mutated directly from outside the Actor thread. "
            "Use setter methods."
        )

    def __delattr__(self, name):
        current_thread = threading.current_thread()
        try:
            actor_thread = object.__getattribute__(self, '_thread')
        except AttributeError:
            actor_thread = None

        try:
            initialized = object.__getattribute__(self, '_initialized')
        except AttributeError:
            initialized = False

        if not initialized or name.startswith('_') or (actor_thread is not None and current_thread == actor_thread):
            object.__delattr__(self, name)
            return

        raise AttributeError(
            f"Public state attribute '{name}' cannot be deleted directly from outside the Actor thread."
        )

    def stop(self, timeout=None):
        """
        Gracefully stop the actor, waiting for mailbox backlog to finish.
        """
        object.__setattr__(self, '_stopped', True)
        thread = object.__getattribute__(self, '_thread')
        if thread is not None:
            try:
                object.__getattribute__(self, '_mailbox').send(None, timeout=timeout)
            except (ValueError, TimeoutError):
                pass
            current_thread = threading.current_thread()
            if current_thread != thread:
                thread.join(timeout=timeout)
            try:
                object.__getattribute__(self, '_mailbox').close()
            except Exception:
                pass

    def __del__(self):
        try:
            self.stop(timeout=0.05)
        except Exception:
            pass
        try:
            object.__getattribute__(self, '_mailbox').close()
        except Exception:
            pass

# Wrap Actor's own __init__
Actor.__init__ = _wrap_init(Actor.__init__)  # type: ignore[method-assign]
