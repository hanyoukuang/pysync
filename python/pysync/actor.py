"""
Actor model implementation with a Rust concurrency-safe backend.

All TOCTOU-prone operations (state check + message enqueue, thread
lifecycle management, stop sequencing) are handled by ActorCore (Rust).
The Python layer is a thin facade: __init_subclass__ generates CallProxy
descriptors for public methods, and every call flows through the single
atomic ActorCore.send_message() path.
"""

import threading
from typing import Any, ClassVar
from concurrent.futures import Future

from pysync._pysync import ActorCore


class CallProxy:
    """
    Descriptor that replaces a public method on an Actor subclass.

    ``Counter.inc`` is replaced by ``CallProxy("inc")`` at class-creation
    time.  When accessed on an instance, it returns a _BoundCallProxy that
    captures the instance reference.  Calling the bound proxy sends a
    message through ActorCore.send_message() — an atomic check+send in Rust.
    """

    __slots__ = ("_method_name",)

    def __init__(self, method_name: str):
        self._method_name = method_name

    def __get__(self, obj, objtype=None):
        if obj is None:
            return self
        return _BoundCallProxy(obj, self._method_name)

    def __set_name__(self, owner, name):
        self._method_name = name


class _BoundCallProxy:
    """A method-call proxy bound to a specific Actor instance."""

    __slots__ = ("_actor", "_method_name")

    def __init__(self, actor: "Actor", method_name: str):
        self._actor = actor
        self._method_name = method_name

    def __call__(self, *args, **kwargs) -> Any:
        # Fast zero-overhead Self-call bypass: if called from inside the Actor worker thread,
        # call the raw method directly to avoid queue deadlock.
        worker_id = getattr(self._actor, "_worker_thread_id", None)
        if worker_id is not None and threading.get_ident() == worker_id:
            method = type(self._actor)._methods[self._method_name]
            return method(self._actor, *args, **kwargs)

        return self._actor._send(self._method_name, args, kwargs)

    def __repr__(self):
        return (
            f"<_BoundCallProxy method={self._method_name!r}"
            f" actor={type(self._actor).__name__}>"
        )


class Actor:
    """
    An Actor model with thread-safe state isolation.

    All public methods are intercepted by CallProxy descriptors (generated
    automatically at class-creation time).  Calling a method sends an
    asynchronous message to a dedicated worker thread and returns a
    ``concurrent.futures.Future``.

    Lifecycle hooks (called on the worker thread):
        on_start()   — called once when the worker thread starts.
        on_stop()    — called once before the worker thread exits.
        on_error()   — supervision hook for unhandled exceptions.

    Examples::

        class Counter(Actor):
            def __init__(self):
                self.val = 0

            def inc(self):
                self.val += 1
                return self.val

        c = Counter()
        f = c.inc()       # -> Future
        assert f.result() == 1
        c.stop()
    """

    _methods: ClassVar[dict[str, Any]] = {}

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)

        # Scan class __dict__ and replace every public callable with CallProxy.
        cls._methods = {}
        for name in list(cls.__dict__):
            if name.startswith("_") or name in ("on_start", "on_stop", "on_error"):
                continue
            attr = cls.__dict__[name]
            if callable(attr) and not isinstance(
                attr, (classmethod, staticmethod, CallProxy, property)
            ):
                cls._methods[name] = attr
                setattr(cls, name, CallProxy(name))

        if "__init__" in cls.__dict__:
            cls.__init__ = _wrap_init(cls.__init__)  # type: ignore[method-assign]

    def __init__(self, **kwargs):
        _ = kwargs
        self._worker_thread_id: int | None = None
        self._core: ActorCore = ActorCore(self)
        self._thread: threading.Thread | None = None

    def _send(self, method_name: str, args: tuple, kwargs: dict) -> Any:
        """Send a method-call message. Returns a Future."""
        return self._core.send_message(method_name, args, kwargs)

    def _dispatch(self, method_name: str, args, kwargs):
        """
        Called by the Rust worker thread to invoke a method.
        """
        method = type(self)._methods[method_name]
        if kwargs is None:
            kwargs = {}
        if callable(method):
            return method(self, *args, **kwargs)
        return method

    def tell(self, method_name: str, *args, **kwargs) -> None:
        """Fire-and-forget: send a message without returning a Future."""
        self._core.tell_message(method_name, args, kwargs)

    def stop(self, timeout=None):
        """
        Gracefully stop the Actor.
        """
        self._core.stop(timeout)

    # -- lifecycle hooks (called by the Rust worker thread) --

    def on_start(self):
        """Called on the worker thread when the Actor starts."""

    def on_stop(self):
        """Called on the worker thread when the Actor stops."""

    def on_error(
        self, exc: BaseException, method_name: str, args: tuple, kwargs: dict
    ) -> bool:
        """
        Supervision hook.  Return True to swallow the exception (Future
        resolves with None), or False to propagate it to the caller.
        """
        return False

    def __repr__(self):
        status = "running" if self._core.is_running else "stopped"
        return f"<{type(self).__name__} status={status} id={id(self):#x}>"


def _wrap_init(original_init):
    """
    Wrap __init__ to call self._core.start() after the most-derived
    class's initializer exits successfully.
    """

    def wrapped(self, *args, **kwargs):
        try:
            depth = object.__getattribute__(self, "_init_depth")
        except AttributeError:
            depth = 0
        object.__setattr__(self, "_init_depth", depth + 1)

        success = False
        try:
            original_init(self, *args, **kwargs)
            success = True
        finally:
            try:
                current_depth = object.__getattribute__(self, "_init_depth") - 1
            except AttributeError:
                current_depth = 0
            object.__setattr__(self, "_init_depth", current_depth)
            if success and current_depth <= 0:
                object.__setattr__(self, "_initialized", True)
                self._core.start()

    return wrapped


# Wrap Actor's own __init__ (handles the case where users don't subclass).
Actor.__init__ = _wrap_init(Actor.__init__)  # type: ignore[method-assign]

