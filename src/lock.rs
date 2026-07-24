use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use std::cell::RefCell;
use std::sync::Arc;

thread_local! {
    static READ_DEPTH: RefCell<usize> = const { RefCell::new(0) };
}

/// FFI wrapper to satisfy PyO3 `py.detach`'s `Ungil` (`Send`) bound for `parking_lot::ArcRwLockReadGuard`.
/// In `lock_api`, `ArcRwLockReadGuard` contains `GuardNoSend(*mut ())` (`!Send`).
/// Since `()` is `Send + Sync` and the guard is unwrapped immediately on the same thread
/// right after `py.detach` returns, implementing `Send` specifically for this concrete guard is safe.
struct SendableArcReadGuard(parking_lot::ArcRwLockReadGuard<parking_lot::RawRwLock, ()>);
unsafe impl Send for SendableArcReadGuard {}

/// FFI wrapper to satisfy PyO3 `py.detach`'s `Ungil` (`Send`) bound for `parking_lot::ArcRwLockWriteGuard`.
struct SendableArcWriteGuard(parking_lot::ArcRwLockWriteGuard<parking_lot::RawRwLock, ()>);
unsafe impl Send for SendableArcWriteGuard {}

/// A native Reader-Writer lock based on parking_lot::RwLock.
/// Allows multiple concurrent readers or a single exclusive writer.
/// Context-manager guards (`with lock.read():` / `with lock.write():`) ensure exception-safe RAII scoping.
#[pyclass]
pub struct RwLock {
    lock: Arc<parking_lot::RwLock<()>>,
}

/// A context-manager guard for holding shared read access of an RwLock.
/// Since it keeps references to local FFI contexts, it is marked as `unsendable`.
#[pyclass(unsendable)]
pub struct RwLockReadGuard {
    lock: Arc<parking_lot::RwLock<()>>,
    guard: Option<parking_lot::ArcRwLockReadGuard<parking_lot::RawRwLock, ()>>,
}

/// A context-manager guard for holding exclusive write access of an RwLock.
/// Since it keeps references to local FFI contexts, it is marked as `unsendable`.
#[pyclass(unsendable)]
pub struct RwLockWriteGuard {
    lock: Arc<parking_lot::RwLock<()>>,
    guard: Option<parking_lot::ArcRwLockWriteGuard<parking_lot::RawRwLock, ()>>,
}

#[pymethods]
impl RwLock {
    #[new]
    fn new() -> Self {
        RwLock {
            lock: Arc::new(parking_lot::RwLock::new(())),
        }
    }

    /// Prepare a read lock guard. Acquisition occurs upon entering the context.
    fn read(&self) -> RwLockReadGuard {
        RwLockReadGuard {
            lock: self.lock.clone(),
            guard: None,
        }
    }

    /// Prepare a write lock guard. Acquisition occurs upon entering the context.
    fn write(&self) -> RwLockWriteGuard {
        RwLockWriteGuard {
            lock: self.lock.clone(),
            guard: None,
        }
    }
}

#[pymethods]
impl RwLockReadGuard {
    /// Enter the read lock context.
    fn __enter__(s: Bound<'_, Self>) -> PyResult<Bound<'_, Self>> {
        {
            let s_ref = s.borrow();
            if s_ref.guard.is_some() {
                return Err(PyRuntimeError::new_err("Lock guard already entered"));
            }
        }

        let is_reentrant = READ_DEPTH.with(|depth| *depth.borrow() > 0);

        // Fast-path: try to acquire without GIL release
        let try_opt = {
            let s_ref = s.borrow();
            if is_reentrant {
                s_ref.lock.try_read_recursive_arc()
            } else {
                s_ref.lock.try_read_arc()
            }
        };

        if let Some(guard) = try_opt {
            READ_DEPTH.with(|depth| *depth.borrow_mut() += 1);
            let mut s_mut = s.borrow_mut();
            s_mut.guard = Some(guard);
            return Ok(s.clone());
        }

        // Slow-path: release GIL and block with segmented retry loop
        let lock = {
            let s_ref = s.borrow();
            s_ref.lock.clone()
        };

        let wrapper = s.py().detach(|| loop {
            let try_opt = if is_reentrant {
                if let Some(g) = lock.try_read_recursive_arc() {
                    Some(g)
                } else {
                    std::thread::sleep(std::time::Duration::from_millis(10));
                    None
                }
            } else {
                lock.try_read_arc_for(std::time::Duration::from_millis(500))
            };
            if let Some(guard) = try_opt {
                return SendableArcReadGuard(guard);
            }
        });

        READ_DEPTH.with(|depth| *depth.borrow_mut() += 1);

        let mut s_mut = s.borrow_mut();
        s_mut.guard = Some(wrapper.0);

        Ok(s.clone())
    }

    /// Exit the read lock context, releasing the lock.
    fn __exit__(
        &mut self,
        _exc_type: &Bound<'_, PyAny>,
        _exc_value: &Bound<'_, PyAny>,
        _traceback: &Bound<'_, PyAny>,
    ) {
        if self.guard.is_some() {
            READ_DEPTH.with(|depth| {
                let mut d = depth.borrow_mut();
                if *d > 0 {
                    *d -= 1;
                }
            });
            self.guard = None;
        }
    }
}

#[pymethods]
impl RwLockWriteGuard {
    /// Enter the write lock context.
    fn __enter__(s: Bound<'_, Self>) -> PyResult<Bound<'_, Self>> {
        {
            let s_ref = s.borrow();
            if s_ref.guard.is_some() {
                return Err(PyRuntimeError::new_err("Lock guard already entered"));
            }
        }

        // Fast-path: try to acquire exclusive write lock immediately without releasing GIL.
        let try_opt = {
            let s_ref = s.borrow();
            s_ref.lock.try_write_arc()
        };

        if let Some(guard) = try_opt {
            let mut s_mut = s.borrow_mut();
            s_mut.guard = Some(guard);
        } else {
            // Slow-path: release GIL and block with 500ms segmented retry loop.
            let lock = {
                let s_ref = s.borrow();
                s_ref.lock.clone()
            };
            let wrapper = s.py().detach(|| loop {
                if let Some(guard) = lock.try_write_arc_for(std::time::Duration::from_millis(500)) {
                    return SendableArcWriteGuard(guard);
                }
            });
            let mut s_mut = s.borrow_mut();
            s_mut.guard = Some(wrapper.0);
        }

        Ok(s.clone())
    }

    /// Exit the write lock context, releasing the lock.
    fn __exit__(
        &mut self,
        _exc_type: &Bound<'_, PyAny>,
        _exc_value: &Bound<'_, PyAny>,
        _traceback: &Bound<'_, PyAny>,
    ) {
        self.guard = None;
    }
}
