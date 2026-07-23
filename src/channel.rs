use pyo3::exceptions::{PyValueError, PyRuntimeError, PyTimeoutError};
use pyo3::prelude::*;
use pyo3::{Py, PyAny};
use crossbeam_channel::{
    Sender, Receiver, bounded, unbounded,
    RecvTimeoutError, SendTimeoutError, TrySendError, TryRecvError
};
use std::time::Duration;
use parking_lot::Mutex;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// A thread-safe, lock-free message passing channel based on Rust's crossbeam-channel.
/// Supports bounded, unbounded, and unbuffered (capacity=0) rendezvous modes.
#[pyclass]
pub struct Channel {
    sender: Mutex<Option<Sender<Py<PyAny>>>>,
    receiver: Mutex<Option<Receiver<Py<PyAny>>>>,
    capacity: Option<usize>,
    pub(crate) closed: Arc<AtomicBool>,
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
        let cap_usize = capacity.map(|cap| cap as usize);
        
        let (s, r) = match cap_usize {
            Some(cap) => bounded(cap),
            _ => unbounded(),
        };
        
        Ok(Channel {
            sender: Mutex::new(Some(s)),
            receiver: Mutex::new(Some(r)),
            capacity: cap_usize,
            closed: Arc::new(AtomicBool::new(false)),
        })
    }

    /// Block and send an item to the channel with optional timeout. Releases the GIL.
    #[pyo3(signature = (item, timeout=None))]
    fn send(&self, py: Python<'_>, item: Py<PyAny>, timeout: Option<f64>) -> PyResult<()> {
        let sender = {
            let guard = self.sender.lock();
            guard.clone().ok_or_else(|| PyValueError::new_err("Channel is closed"))?
        };
        let closed = self.closed.clone();
        
        if let Some(t) = timeout {
            if t < 0.0 {
                return Err(PyValueError::new_err("Timeout must be non-negative"));
            }
            let duration = Duration::from_secs_f64(t);
            let start = std::time::Instant::now();
            let mut val = item;
            loop {
                if closed.load(Ordering::SeqCst) {
                    return Err(PyValueError::new_err("Channel is closed"));
                }
                let remaining = duration.saturating_sub(start.elapsed());
                if remaining.is_zero() {
                    return Err(PyTimeoutError::new_err("Send operation timed out"));
                }
                let poll_time = Duration::from_millis(50).min(remaining);
                let res = py.detach(|| sender.send_timeout(val, poll_time));
                match res {
                    Ok(_) => return Ok(()),
                    Err(SendTimeoutError::Timeout(item_back)) => {
                        val = item_back;
                        continue;
                    }
                    Err(SendTimeoutError::Disconnected(_)) => return Err(PyValueError::new_err("Channel is closed")),
                }
            }
        } else {
            let mut val = item;
            loop {
                if closed.load(Ordering::SeqCst) {
                    return Err(PyValueError::new_err("Channel is closed"));
                }
                let poll_time = Duration::from_millis(50);
                let res = py.detach(|| sender.send_timeout(val, poll_time));
                match res {
                    Ok(_) => return Ok(()),
                    Err(SendTimeoutError::Timeout(item_back)) => {
                        val = item_back;
                        continue;
                    }
                    Err(SendTimeoutError::Disconnected(_)) => return Err(PyValueError::new_err("Channel is closed")),
                }
            }
        }
    }

    /// Block and receive an item from the channel with optional timeout. Releases the GIL.
    #[pyo3(signature = (timeout=None))]
    fn recv(&self, py: Python<'_>, timeout: Option<f64>) -> PyResult<Py<PyAny>> {
        let receiver = {
            let guard = self.receiver.lock();
            guard.clone().ok_or_else(|| PyValueError::new_err("Channel is closed and empty"))?
        };
        let closed = self.closed.clone();
        
        if let Some(t) = timeout {
            if t < 0.0 {
                return Err(PyValueError::new_err("Timeout must be non-negative"));
            }
            let duration = Duration::from_secs_f64(t);
            let start = std::time::Instant::now();
            loop {
                if closed.load(Ordering::SeqCst) {
                    if let Ok(item) = receiver.try_recv() {
                        return Ok(item);
                    }
                    return Err(PyValueError::new_err("Channel is closed and empty"));
                }
                let remaining = duration.saturating_sub(start.elapsed());
                if remaining.is_zero() {
                    return Err(PyTimeoutError::new_err("Receive operation timed out"));
                }
                let poll_time = Duration::from_millis(50).min(remaining);
                let res = py.detach(|| receiver.recv_timeout(poll_time));
                match res {
                    Ok(item) => return Ok(item),
                    Err(RecvTimeoutError::Timeout) => continue,
                    Err(RecvTimeoutError::Disconnected) => {
                        if let Ok(item) = receiver.try_recv() {
                            return Ok(item);
                        }
                        return Err(PyValueError::new_err("Channel is closed and empty"));
                    }
                }
            }
        } else {
            loop {
                if closed.load(Ordering::SeqCst) {
                    if let Ok(item) = receiver.try_recv() {
                        return Ok(item);
                    }
                    return Err(PyValueError::new_err("Channel is closed and empty"));
                }
                let poll_time = Duration::from_millis(50);
                let res = py.detach(|| receiver.recv_timeout(poll_time));
                match res {
                    Ok(item) => return Ok(item),
                    Err(RecvTimeoutError::Timeout) => continue,
                    Err(RecvTimeoutError::Disconnected) => {
                        if let Ok(item) = receiver.try_recv() {
                            return Ok(item);
                        }
                        return Err(PyValueError::new_err("Channel is closed and empty"));
                    }
                }
            }
        }
    }

    /// Attempt to send an item immediately without blocking.
    fn try_send(&self, item: Py<PyAny>) -> PyResult<()> {
        let sender = {
            let guard = self.sender.lock();
            guard.clone().ok_or_else(|| PyValueError::new_err("Channel is closed"))?
        };
        
        match sender.try_send(item) {
            Ok(_) => Ok(()),
            Err(TrySendError::Full(_)) => Err(PyRuntimeError::new_err("Channel is full")),
            Err(TrySendError::Disconnected(_)) => Err(PyValueError::new_err("Channel is closed")),
        }
    }

    /// Attempt to receive an item immediately without blocking.
    fn try_recv(&self) -> PyResult<Py<PyAny>> {
        let receiver = {
            let guard = self.receiver.lock();
            guard.clone().ok_or_else(|| PyValueError::new_err("Channel is closed"))?
        };
        
        match receiver.try_recv() {
            Ok(item) => Ok(item),
            Err(TryRecvError::Empty) => Err(PyRuntimeError::new_err("Channel is empty")),
            Err(TryRecvError::Disconnected) => Err(PyValueError::new_err("Channel is closed and empty")),
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

    /// Close the channel by dropping the sender, causing receivers to wake up with Disconnected once buffered items are drained.
    fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
        *self.sender.lock() = None;
    }

    /// Create a Receive Operation wrapper for `select()`.
    fn recv_op(&self) -> PyResult<RecvOp> {
        let guard = self.receiver.lock();
        let rx = guard.clone().ok_or_else(|| PyValueError::new_err("Channel is closed"))?;
        Ok(RecvOp { receiver: rx, closed: self.closed.clone() })
    }

    /// Create a Send Operation wrapper for `select()`.
    fn send_op(&self, item: Py<PyAny>) -> PyResult<SendOp> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(PyValueError::new_err("Channel is closed"));
        }
        let guard = self.sender.lock();
        let tx = guard.clone().ok_or_else(|| PyValueError::new_err("Channel is closed"))?;
        Ok(SendOp { sender: tx, item, closed: self.closed.clone() })
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
            Err(_) => Err(pyo3::exceptions::PyStopIteration::new_err(())),
        }
    }

    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __exit__(&self, _exc_type: &Bound<'_, PyAny>, _exc_val: &Bound<'_, PyAny>, _exc_tb: &Bound<'_, PyAny>) {
        self.close();
    }

    /// Async version of send() for compatibility with asyncio event loops.
    #[pyo3(signature = (item, timeout=None))]
    fn asend<'py>(slf: PyRef<'py, Self>, item: Py<PyAny>, timeout: Option<f64>) -> PyResult<Bound<'py, PyAny>> {
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
    pub(crate) closed: Arc<AtomicBool>,
}

/// An operation descriptor for sending to a channel inside `select()`.
#[pyclass]
pub struct SendOp {
    pub(crate) sender: Sender<Py<PyAny>>,
    pub(crate) item: Py<PyAny>,
    pub(crate) closed: Arc<AtomicBool>,
}
