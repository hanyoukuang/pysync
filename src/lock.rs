use pyo3::prelude::*;
use pyo3::exceptions::PyRuntimeError;
use std::sync::Arc;
use std::cell::RefCell;
use parking_lot::lock_api::RawRwLock as _;

thread_local! {
    static READ_DEPTH: RefCell<usize> = RefCell::new(0);
}

/// A simple FFI escape hatch to bypass Rust's Send/Sync constraints for PyO3.
///
/// # SAFETY
/// `parking_lot::ArcRwLockReadGuard` and `ArcRwLockWriteGuard` contain raw pointers
/// which are marked `!Send`. `UnsafeSend` wraps them so they can be passed through
/// PyO3's `py.detach(|| ...)` GIL-releasing closures.
///
/// **Safety invariant**: `UnsafeSend` is strictly restricted to internal scope within
/// context manager `__enter__` calls where the guard is constructed inside `py.detach`
/// on the caller's OS thread and immediately moved back to `RwLockReadGuard` / `RwLockWriteGuard`.
/// It must never be transferred across arbitrary worker threads.
struct UnsafeSend<T>(T);
unsafe impl<T> Send for UnsafeSend<T> {}
unsafe impl<T> Sync for UnsafeSend<T> {}

/// A native Reader-Writer lock based on parking_lot::RwLock.
/// Allows multiple concurrent readers or a single exclusive writer.
#[pyclass]
#[repr(align(64))]
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

    /// Direct read lock acquisition (zero Python object allocation).
    fn acquire_read(&self, py: Python<'_>) {
        let lock = self.lock.clone();
        py.detach(|| {
            unsafe {
                lock.raw().lock_shared();
            }
        });
    }

    /// Direct read lock release.
    fn release_read(&self) -> PyResult<()> {
        unsafe {
            self.lock.raw().unlock_shared();
        }
        Ok(())
    }

    /// Try direct read lock acquisition without blocking.
    fn try_acquire_read(&self) -> bool {
        unsafe { self.lock.raw().try_lock_shared() }
    }

    /// Direct write lock acquisition (zero Python object allocation).
    fn acquire_write(&self, py: Python<'_>) {
        let lock = self.lock.clone();
        py.detach(|| {
            unsafe {
                lock.raw().lock_exclusive();
            }
        });
    }

    /// Direct write lock release.
    fn release_write(&self) -> PyResult<()> {
        unsafe {
            self.lock.raw().unlock_exclusive();
        }
        Ok(())
    }

    /// Try direct write lock acquisition without blocking.
    fn try_acquire_write(&self) -> bool {
        unsafe { self.lock.raw().try_lock_exclusive() }
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

        // Slow-path: release GIL and block
        let lock = {
            let s_ref = s.borrow();
            s_ref.lock.clone()
        };

        let guard_wrapper = s.py().detach(|| {
            if is_reentrant {
                UnsafeSend(lock.read_arc_recursive())
            } else {
                UnsafeSend(lock.read_arc())
            }
        });

        READ_DEPTH.with(|depth| *depth.borrow_mut() += 1);

        let mut s_mut = s.borrow_mut();
        s_mut.guard = Some(guard_wrapper.0);

        Ok(s.clone())
    }

    /// Exit the read lock context, releasing the lock.
    fn __exit__(&mut self, _exc_type: &Bound<'_, PyAny>, _exc_value: &Bound<'_, PyAny>, _traceback: &Bound<'_, PyAny>) {
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
            // Slow-path: release GIL and block waiting for exclusive write lock.
            let lock = {
                let s_ref = s.borrow();
                s_ref.lock.clone()
            };
            let guard_wrapper = s.py().detach(|| {
                UnsafeSend(lock.write_arc())
            });
            let mut s_mut = s.borrow_mut();
            s_mut.guard = Some(guard_wrapper.0);
        }

        Ok(s.clone())
    }

    /// Exit the write lock context, releasing the lock.
    fn __exit__(&mut self, _exc_type: &Bound<'_, PyAny>, _exc_value: &Bound<'_, PyAny>, _traceback: &Bound<'_, PyAny>) {
        self.guard = None;
    }
}

