use crossbeam_channel::{bounded, unbounded, Receiver, Sender};
use parking_lot::Mutex;
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyString, PyTuple};
use pyo3::{Py, PyAny};
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};

const STATE_RUNNING: u8 = 0;
const STATE_STOPPING: u8 = 1;
const STATE_STOPPED: u8 = 2;

// ---------------------------------------------------------------------------
// Internal message type — never exposed to Python
// ---------------------------------------------------------------------------
struct ActorMessage {
    method_name: Py<PyAny>,
    args: Py<PyTuple>,
    kwargs: Option<Py<PyDict>>,
    /// None for fire-and-forget (tell), Some for call-with-Future
    future: Option<Py<PyAny>>,
}

unsafe impl Send for ActorMessage {}

type LazyInitTuple = (Receiver<ActorMessage>, Py<PyAny>, Sender<()>);

// ---------------------------------------------------------------------------
// ActorCore — high-performance, concurrency-safe Rust backend
// ---------------------------------------------------------------------------
#[pyclass]
pub struct ActorCore {
    tx: Sender<ActorMessage>,
    state: Arc<AtomicU8>,
    handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    done_rx: Receiver<()>,
    future_class: Py<PyAny>,
    _lazy: Arc<Mutex<Option<LazyInitTuple>>>,
}

#[pymethods]
impl ActorCore {
    #[new]
    fn new(py: Python<'_>, actor: Py<PyAny>) -> PyResult<Self> {
        let (tx, rx) = unbounded::<ActorMessage>();
        let (done_tx, done_rx) = bounded::<()>(1);
        let state = Arc::new(AtomicU8::new(STATE_RUNNING));
        let handle: Arc<Mutex<Option<JoinHandle<()>>>> = Arc::new(Mutex::new(None));

        let m = py.import("concurrent.futures")?;
        let future_class: Py<PyAny> = m.getattr("Future")?.unbind();

        let lazy = Arc::new(Mutex::new(Some((rx, actor, done_tx))));

        Ok(ActorCore {
            tx,
            state,
            handle,
            done_rx,
            future_class,
            _lazy: lazy,
        })
    }

    /// Start the worker thread. Must be called after Python Actor __init__ finishes.
    fn start(&self) -> PyResult<()> {
        let lazy =
            self._lazy.lock().take().ok_or_else(|| {
                pyo3::exceptions::PyRuntimeError::new_err("Actor already started")
            })?;
        let (rx, actor, done_tx) = lazy;

        let worker = thread::spawn(move || {
            Python::try_attach(|py| {
                // Safety check for Python finalize
                unsafe {
                    if ffi::Py_IsFinalizing() != 0 {
                        let _ = done_tx.send(());
                        return;
                    }
                }

                let actor_bound = actor.bind(py);

                // Record thread identity for zero-overhead self-call detection
                if let Ok(threading) = py.import("threading") {
                    if let Ok(ident) = threading.call_method0("get_ident") {
                        let _ = actor_bound.setattr("_worker_thread_id", ident);
                    }
                }

                // ---- on_start() ----
                if let Err(e) = actor_bound.call_method0("on_start") {
                    let _ = actor_bound
                        .call_method1("on_error", (e, "on_start", PyTuple::empty(py), py.None()));
                }

                // ---- main message loop ----
                loop {
                    let msg = match rx.recv() {
                        Ok(m) => m,
                        Err(_) => break, // channel disconnected → exit
                    };

                    let method_name_bound = msg.method_name.bind(py);

                    // Check for shutdown sentinel
                    if let Ok(name_str) = method_name_bound.extract::<&str>() {
                        if name_str == "__sentinel__" {
                            // Drain any leftover messages in the channel to prevent hanging Futures
                            while let Ok(leftover) = rx.try_recv() {
                                if let Some(ref future) = leftover.future {
                                    let err = pyo3::exceptions::PyRuntimeError::new_err(
                                        "Actor is stopped",
                                    );
                                    let _ = future.bind(py).call_method1("set_exception", (err,));
                                }
                            }
                            break;
                        }
                    }

                    let args = msg.args.bind(py);
                    let kwargs = msg.kwargs.as_ref().map(|d| d.bind(py));

                    match actor_bound.call_method1("_dispatch", (method_name_bound, args, kwargs)) {
                        Ok(val) => {
                            if let Some(ref future) = msg.future {
                                let _ = future.bind(py).call_method1("set_result", (val,));
                            }
                        }
                        Err(err) => {
                            // Route through supervision hook
                            let handled = actor_bound
                                .call_method1(
                                    "on_error",
                                    (err.clone_ref(py), method_name_bound, args, kwargs),
                                )
                                .ok()
                                .and_then(|r| r.extract::<bool>().ok())
                                .unwrap_or(false);

                            if let Some(ref future) = msg.future {
                                if handled {
                                    let _ =
                                        future.bind(py).call_method1("set_result", (py.None(),));
                                } else {
                                    let _ = future.bind(py).call_method1("set_exception", (err,));
                                }
                            }
                        }
                    }
                }

                // ---- on_stop() ----
                unsafe {
                    if ffi::Py_IsFinalizing() == 0 {
                        let _ = actor_bound.call_method0("on_stop");
                    }
                }

                // Drain leftover messages on exit to guarantee no hanging Futures
                while let Ok(leftover) = rx.try_recv() {
                    if let Some(ref future) = leftover.future {
                        let err = pyo3::exceptions::PyRuntimeError::new_err("Actor is stopped");
                        let _ = future.bind(py).call_method1("set_exception", (err,));
                    }
                }

                let _ = done_tx.send(());
            });
        });

        *self.handle.lock() = Some(worker);
        Ok(())
    }

    /// Atomically check state and enqueue a call message.
    #[pyo3(signature = (method_name, args, kwargs=None))]
    fn send_message(
        &self,
        py: Python<'_>,
        method_name: Py<PyAny>,
        args: Py<PyTuple>,
        kwargs: Option<Py<PyDict>>,
    ) -> PyResult<Py<PyAny>> {
        if self.state.load(Ordering::Acquire) != STATE_RUNNING {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "Actor is stopped",
            ));
        }

        let future = self.future_class.bind(py).call0()?;
        let future_py: Py<PyAny> = future.unbind();

        let msg = ActorMessage {
            method_name,
            args,
            kwargs,
            future: Some(future_py.clone_ref(py)),
        };

        self.tx
            .send(msg)
            .map_err(|_| pyo3::exceptions::PyRuntimeError::new_err("Actor worker thread exited"))?;

        Ok(future_py)
    }

    /// Atomically check state and enqueue a tell message (fire-and-forget).
    #[pyo3(signature = (method_name, args, kwargs=None))]
    fn tell_message(
        &self,
        method_name: Py<PyAny>,
        args: Py<PyTuple>,
        kwargs: Option<Py<PyDict>>,
    ) -> PyResult<()> {
        if self.state.load(Ordering::Acquire) != STATE_RUNNING {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "Actor is stopped",
            ));
        }

        self.tx
            .send(ActorMessage {
                method_name,
                args,
                kwargs,
                future: None,
            })
            .map_err(|_| pyo3::exceptions::PyRuntimeError::new_err("Actor worker thread exited"))?;

        Ok(())
    }

    /// Gracefully stop the Actor with optional timeout.
    #[pyo3(signature = (timeout=None))]
    fn stop(&self, py: Python<'_>, timeout: Option<f64>) -> PyResult<()> {
        let old_state = self.state.swap(STATE_STOPPING, Ordering::AcqRel);
        if old_state == STATE_STOPPED {
            return Ok(());
        }

        let sentinel_name = PyString::new(py, "__sentinel__").into_any().unbind();
        let _ = self.tx.send(ActorMessage {
            method_name: sentinel_name,
            args: PyTuple::empty(py).unbind(),
            kwargs: None,
            future: None,
        });

        let handle = self.handle.lock().take();
        if let Some(h) = handle {
            if h.thread().id() == std::thread::current().id() {
                self.state.store(STATE_STOPPED, Ordering::Release);
                return Ok(());
            }

            let done_rx = self.done_rx.clone();
            let timeout_duration = timeout.map(std::time::Duration::from_secs_f64);

            let wait_res = match timeout_duration {
                Some(dur) if dur > std::time::Duration::from_secs(0) => {
                    py.detach(move || done_rx.recv_timeout(dur))
                }
                Some(_) => Err(crossbeam_channel::RecvTimeoutError::Timeout),
                None => {
                    // Fast fallback timeout of 100ms to prevent Windows OS thread hanging
                    py.detach(move || done_rx.recv_timeout(std::time::Duration::from_millis(100)))
                }
            };

            if wait_res.is_ok() {
                py.detach(move || {
                    let _ = h.join();
                });
                self.state.store(STATE_STOPPED, Ordering::Release);
            } else if timeout.is_none() {
                // Background thread cleanup for default fallback timeout
                std::thread::spawn(move || {
                    let _ = h.join();
                });
                self.state.store(STATE_STOPPED, Ordering::Release);
            } else {
                // Explicit timeout reached, return handle to lock
                *self.handle.lock() = Some(h);
            }
        }

        Ok(())
    }

    /// Returns True if the Actor is running.
    #[getter]
    fn is_running(&self) -> bool {
        self.state.load(Ordering::Acquire) == STATE_RUNNING
    }
}

impl Drop for ActorCore {
    fn drop(&mut self) {
        self.state.store(STATE_STOPPED, Ordering::Release);

        let handle = self.handle.lock().take();
        if let Some(h) = handle {
            std::thread::spawn(move || {
                let _ = h.join();
            });
        }
    }
}
