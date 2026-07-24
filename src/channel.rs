use crossbeam_channel::{bounded, unbounded, Receiver, Sender, TryRecvError, TrySendError};
use parking_lot::{Mutex, RwLock};
use pyo3::exceptions::{PyRuntimeError, PyTimeoutError, PyValueError};
use pyo3::prelude::*;
use pyo3::{Py, PyAny};
use std::time::Duration;

/// A thread-safe, lock-free message passing channel based on Rust's crossbeam-channel.
/// Supports bounded, unbounded, and unbuffered (capacity=0) rendezvous modes.
#[pyclass]
pub struct Channel {
    // RwLock allows concurrent shared reads for high-frequency send() / try_send() operations across threads.
    sender: RwLock<Option<Sender<Py<PyAny>>>>,
    receiver: RwLock<Option<Receiver<Py<PyAny>>>>,
    // Mutex is used for close_tx since close() is a low-frequency single-write operation.
    close_tx: Mutex<Option<Sender<()>>>,
    close_rx: Receiver<()>,
    capacity: Option<usize>,
}

impl Channel {
    /// Internal helper to execute blocking send operations detachment.
    /// Guarantees Py<PyAny> destruction always happens on the attached Python thread state
    /// if send fails, is cancelled by close(), or times out.
    fn send_internal(
        py: Python<'_>,
        sender: &Sender<Py<PyAny>>,
        close_rx: &Receiver<()>,
        item: Py<PyAny>,
        timeout: Option<Duration>,
    ) -> PyResult<()> {
        let res = py.detach(|| match timeout {
            Some(duration) => {
                let timeout_rx = crossbeam_channel::after(duration);
                crossbeam_channel::select! {
                    send(sender, item) -> res => match res {
                        Ok(_) => Ok(()),
                        Err(e) => Err((Some(e.into_inner()), PyValueError::new_err("Channel is closed"))),
                    },
                    recv(close_rx) -> _ => Err((Some(item), PyValueError::new_err("Channel is closed"))),
                    recv(timeout_rx) -> _ => Err((Some(item), PyTimeoutError::new_err("send() timed out waiting for channel capacity"))),
                }
            }
            None => {
                crossbeam_channel::select! {
                    send(sender, item) -> res => match res {
                        Ok(_) => Ok(()),
                        Err(e) => Err((Some(e.into_inner()), PyValueError::new_err("Channel is closed"))),
                    },
                    recv(close_rx) -> _ => Err((Some(item), PyValueError::new_err("Channel is closed"))),
                }
            }
        });

        match res {
            Ok(()) => Ok(()),
            Err((returned_item, err)) => {
                drop(returned_item);
                Err(err)
            }
        }
    }
}

#[pymethods]
impl Channel {
    #[new]
    #[pyo3(signature = (capacity=None))]
    fn new(capacity: Option<isize>) -> PyResult<Self> {
        if let Some(cap) = capacity {
            if cap < 0 {
                return Err(PyValueError::new_err("Capacity must be non-negative"));
            }
        }

        let (tx, rx) = match capacity {
            None => unbounded(),
            Some(cap) => bounded(cap as usize),
        };

        let (close_tx, close_rx) = bounded(0);

        Ok(Channel {
            sender: RwLock::new(Some(tx)),
            receiver: RwLock::new(Some(rx)),
            close_tx: Mutex::new(Some(close_tx)),
            close_rx,
            capacity: capacity.map(|c| c as usize),
        })
    }

    /// Block and send an item into the channel. Releases the GIL while waiting.
    #[pyo3(signature = (item, timeout=None))]
    fn send(&self, py: Python<'_>, item: Py<PyAny>, timeout: Option<f64>) -> PyResult<()> {
        let sender = {
            let guard = self.sender.read();
            guard
                .clone()
                .ok_or_else(|| PyValueError::new_err("Channel is closed"))?
        };

        let mut item = item;
        if timeout.is_none() {
            match sender.try_send(item) {
                Ok(()) => return Ok(()),
                Err(TrySendError::Full(returned_item)) => {
                    item = returned_item;
                }
                Err(TrySendError::Disconnected(_returned_item)) => {
                    return Err(PyValueError::new_err("Channel is closed"));
                }
            }
        }

        let duration = match timeout {
            Some(t) => {
                if t < 0.0 {
                    return Err(PyValueError::new_err("Timeout must be non-negative"));
                }
                Some(Duration::from_secs_f64(t))
            }
            None => None,
        };

        Self::send_internal(py, &sender, &self.close_rx, item, duration)
    }

    /// Block and receive an item from the channel. Releases the GIL while waiting.
    ///
    /// # Concurrency & Closing Behavior
    /// When multiple worker threads are blocked on `recv()` when `close()` is called,
    /// all threads are unblocked simultaneously. Any buffered items in the channel
    /// are drained by the unblocked threads until empty. Threads calling `recv()` after
    /// the buffer is drained will receive `PyValueError("Channel is closed and empty")`.
    #[pyo3(signature = (timeout=None))]
    fn recv(&self, py: Python<'_>, timeout: Option<f64>) -> PyResult<Py<PyAny>> {
        let receiver = {
            let guard = self.receiver.read();
            guard
                .clone()
                .ok_or_else(|| PyValueError::new_err("Channel is closed"))?
        };

        if timeout.is_none() {
            if let Ok(item) = receiver.try_recv() {
                return Ok(item);
            }
        }

        let close_rx = self.close_rx.clone();

        if let Some(t) = timeout {
            if t < 0.0 {
                return Err(PyValueError::new_err("Timeout must be non-negative"));
            }
            let duration = Duration::from_secs_f64(t);
            py.detach(|| {
                let timeout_rx = crossbeam_channel::after(duration);
                crossbeam_channel::select! {
                    recv(receiver) -> res => match res {
                        Ok(item) => Ok(item),
                        Err(_) => Err(PyValueError::new_err("Channel is closed and empty")),
                    },
                    recv(close_rx) -> _ => {
                        if let Ok(item) = receiver.try_recv() {
                            return Ok(item);
                        }
                        Err(PyValueError::new_err("Channel is closed and empty"))
                    },
                    recv(timeout_rx) -> _ => Err(PyTimeoutError::new_err("recv() timed out waiting for available item")),
                }
            })
        } else {
            py.detach(|| {
                crossbeam_channel::select! {
                    recv(receiver) -> res => match res {
                        Ok(item) => Ok(item),
                        Err(_) => Err(PyValueError::new_err("Channel is closed and empty")),
                    },
                    recv(close_rx) -> _ => {
                        if let Ok(item) = receiver.try_recv() {
                            return Ok(item);
                        }
                        Err(PyValueError::new_err("Channel is closed and empty"))
                    },
                }
            })
        }
    }

    /// Non-blocking send. Raises RuntimeError if full, ValueError if closed.
    fn try_send(&self, item: Py<PyAny>) -> PyResult<()> {
        let guard = self.sender.read();
        let tx = guard
            .as_ref()
            .ok_or_else(|| PyValueError::new_err("Channel is closed"))?;

        match tx.try_send(item) {
            Ok(_) => Ok(()),
            Err(TrySendError::Full(_)) => Err(PyRuntimeError::new_err("Channel is full")),
            Err(TrySendError::Disconnected(_)) => Err(PyValueError::new_err("Channel is closed")),
        }
    }

    /// Non-blocking receive. Raises RuntimeError if empty, ValueError if closed.
    fn try_recv(&self) -> PyResult<Py<PyAny>> {
        let guard = self.receiver.read();
        let rx = guard
            .as_ref()
            .ok_or_else(|| PyValueError::new_err("Channel is closed"))?;

        match rx.try_recv() {
            Ok(item) => Ok(item),
            Err(TryRecvError::Empty) => Err(PyRuntimeError::new_err("Channel is empty")),
            Err(TryRecvError::Disconnected) => {
                Err(PyValueError::new_err("Channel is closed and empty"))
            }
        }
    }

    /// Block and send an item with a timeout. Releases the GIL.
    fn send_timeout(&self, py: Python<'_>, item: Py<PyAny>, timeout: f64) -> PyResult<()> {
        self.send(py, item, Some(timeout))
    }

    /// Block and receive an item with a timeout. Releases the GIL.
    fn recv_timeout(&self, py: Python<'_>, timeout: f64) -> PyResult<Py<PyAny>> {
        self.recv(py, Some(timeout))
    }

    /// Close the channel for sending. Drops sender and unblocks pending receivers.
    /// Note: Receiver remains active to allow draining any remaining buffered items.
    fn close(&self) {
        *self.sender.write() = None;
        *self.close_tx.lock() = None;
    }

    /// Create a Receive Operation wrapper for `select()`.
    /// Remains available after `close()` to allow draining existing items in the channel buffer.
    fn recv_op(&self) -> PyResult<RecvOp> {
        let guard = self.receiver.read();
        let rx = guard
            .clone()
            .ok_or_else(|| PyValueError::new_err("Channel is closed"))?;
        Ok(RecvOp {
            receiver: rx,
            close_rx: self.close_rx.clone(),
        })
    }

    /// Create a Send Operation wrapper for `select()`.
    fn send_op(&self, item: Py<PyAny>) -> PyResult<SendOp> {
        let guard = self.sender.read();
        let tx = guard
            .clone()
            .ok_or_else(|| PyValueError::new_err("Channel is closed"))?;
        Ok(SendOp {
            sender: tx,
            item,
            close_rx: self.close_rx.clone(),
        })
    }

    #[getter]
    fn capacity(&self) -> PyResult<Option<usize>> {
        Ok(self.capacity)
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        match self.recv(py, None) {
            Ok(val) => Ok(val),
            Err(err) => {
                // When timeout is None, the only error from recv() is PyValueError (channel closed and empty).
                // Directly convert PyValueError to PyStopIteration without re-locking.
                if err.is_instance_of::<pyo3::exceptions::PyValueError>(py) {
                    Err(pyo3::exceptions::PyStopIteration::new_err(()))
                } else {
                    Err(err)
                }
            }
        }
    }

    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __exit__(
        &self,
        _exc_type: &Bound<'_, PyAny>,
        _exc_val: &Bound<'_, PyAny>,
        _exc_tb: &Bound<'_, PyAny>,
    ) {
        self.close();
    }

    /// Async version of send() for compatibility with asyncio event loops.
    #[pyo3(signature = (item, timeout=None))]
    fn asend<'py>(
        slf: PyRef<'py, Self>,
        item: Py<PyAny>,
        timeout: Option<f64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let py = slf.py();
        let asyncio = py.import("asyncio")?;
        let loop_obj = asyncio.call_method0("get_running_loop")?;
        let send_method = slf.into_pyobject(py)?.getattr("send")?;
        loop_obj.call_method1("run_in_executor", (py.None(), send_method, item, timeout))
    }

    /// Async version of recv() for compatibility with asyncio event loops.
    #[pyo3(signature = (timeout=None))]
    fn arecv<'py>(slf: PyRef<'py, Self>, timeout: Option<f64>) -> PyResult<Bound<'py, PyAny>> {
        let py = slf.py();
        let asyncio = py.import("asyncio")?;
        let loop_obj = asyncio.call_method0("get_running_loop")?;
        let recv_method = slf.into_pyobject(py)?.getattr("recv")?;
        loop_obj.call_method1("run_in_executor", (py.None(), recv_method, timeout))
    }
}

/// An operation descriptor for receiving from a channel inside `select()`.
#[pyclass]
pub struct RecvOp {
    pub(crate) receiver: Receiver<Py<PyAny>>,
    pub(crate) close_rx: Receiver<()>,
}

/// An operation descriptor for sending to a channel inside `select()`.
#[pyclass]
pub struct SendOp {
    pub(crate) sender: Sender<Py<PyAny>>,
    pub(crate) item: Py<PyAny>,
    pub(crate) close_rx: Receiver<()>,
}
