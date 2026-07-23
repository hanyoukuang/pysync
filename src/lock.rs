use pyo3::prelude::*;
use pyo3::exceptions::PyRuntimeError;
use std::sync::Arc;
use std::thread::ThreadId;
use std::collections::HashMap;
use parking_lot::Mutex;

/// A simple FFI escape hatch to bypass Rust's Send/Sync constraints for PyO3.
/// Because parking_lot guards contain raw pointers which are !Send, we wrap them
/// in UnsafeSend to safely pass them through PyO3's GIL-detached closures.
struct UnsafeSend<T>(T);
unsafe impl<T> Send for UnsafeSend<T> {}
unsafe impl<T> Sync for UnsafeSend<T> {}

/// Per-lock read-holder registry.
/// Maps a thread ID to the number of read locks that thread currently holds.
/// Used to distinguish first-time reads (writer-fair) from recursive re-entries
/// (must use recursive API to avoid self-deadlock).
type ReaderRegistry = Arc<Mutex<HashMap<ThreadId, usize>>>;

/// A native Reader-Writer lock based on parking_lot::RwLock.
/// Allows multiple concurrent readers or a single exclusive writer.
///
/// ## Writer-starvation prevention
/// To prevent new readers from starving waiting writers while avoiding re-entrancy deadlocks:
/// - First acquisition on a thread  → `read_arc()` / `try_read_arc()` (writer-preferred queue).
/// - Re-entrant acquisition on the same thread → `read_arc_recursive()` (bypasses queue for self-deadlock prevention).
#[pyclass]
pub struct RwLock {
    lock: Arc<parking_lot::RwLock<()>>,
    reader_registry: ReaderRegistry,
    raw_read_counts: Arc<Mutex<HashMap<ThreadId, usize>>>,
    raw_write_owners: Arc<Mutex<HashMap<ThreadId, usize>>>,
}

/// A context-manager guard for holding shared read access of an RwLock.
/// Since it keeps references to local FFI contexts, it is marked as `unsendable`.
#[pyclass(unsendable)]
pub struct RwLockReadGuard {
    lock: Arc<parking_lot::RwLock<()>>,
    reader_registry: ReaderRegistry,
    guard: Option<parking_lot::ArcRwLockReadGuard<parking_lot::RawRwLock, ()>>,
}

/// A context-manager guard for holding exclusive write access of an RwLock.
/// Since it keeps references to local FFI contexts, it is marked as `unsendable`.
#[pyclass(unsendable)]
pub struct RwLockWriteGuard {
    lock: Arc<parking_lot::RwLock<()>>,
    guard: Option<parking_lot::ArcRwLockWriteGuard<parking_lot::RawRwLock, ()>>,
}

use parking_lot::lock_api::RawRwLock as _;

#[pymethods]
impl RwLock {
    #[new]
    fn new() -> Self {
        RwLock {
            lock: Arc::new(parking_lot::RwLock::new(())),
            reader_registry: Arc::new(Mutex::new(HashMap::new())),
            raw_read_counts: Arc::new(Mutex::new(HashMap::new())),
            raw_write_owners: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Prepare a read lock guard. Acquisition occurs upon entering the context.
    fn read(&self) -> RwLockReadGuard {
        RwLockReadGuard {
            lock: self.lock.clone(),
            reader_registry: Arc::clone(&self.reader_registry),
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
        let tid = std::thread::current().id();
        py.detach(|| {
            unsafe {
                lock.raw().lock_shared();
            }
        });
        *self.raw_read_counts.lock().entry(tid).or_insert(0) += 1;
        *self.reader_registry.lock().entry(tid).or_insert(0) += 1;
    }

    /// Direct read lock release.
    fn release_read(&self) -> PyResult<()> {
        let tid = std::thread::current().id();
        let mut counts = self.raw_read_counts.lock();
        match counts.get_mut(&tid) {
            Some(count) if *count > 0 => {
                *count -= 1;
                if let Some(reg_count) = self.reader_registry.lock().get_mut(&tid) {
                    if *reg_count > 0 {
                        *reg_count -= 1;
                    }
                }
                unsafe {
                    self.lock.raw().unlock_shared();
                }
                Ok(())
            }
            _ => Err(PyRuntimeError::new_err("Cannot release read lock: lock was not acquired by current thread")),
        }
    }

    /// Try direct read lock acquisition without blocking.
    fn try_acquire_read(&self) -> bool {
        if unsafe { self.lock.raw().try_lock_shared() } {
            let tid = std::thread::current().id();
            *self.raw_read_counts.lock().entry(tid).or_insert(0) += 1;
            *self.reader_registry.lock().entry(tid).or_insert(0) += 1;
            true
        } else {
            false
        }
    }

    /// Direct write lock acquisition (zero Python object allocation).
    fn acquire_write(&self, py: Python<'_>) {
        let lock = self.lock.clone();
        let tid = std::thread::current().id();
        py.detach(|| {
            unsafe {
                lock.raw().lock_exclusive();
            }
        });
        *self.raw_write_owners.lock().entry(tid).or_insert(0) += 1;
    }

    /// Direct write lock release.
    fn release_write(&self) -> PyResult<()> {
        let tid = std::thread::current().id();
        let mut owners = self.raw_write_owners.lock();
        match owners.get_mut(&tid) {
            Some(count) if *count > 0 => {
                *count -= 1;
                unsafe {
                    self.lock.raw().unlock_exclusive();
                }
                Ok(())
            }
            _ => Err(PyRuntimeError::new_err("Cannot release write lock: lock was not acquired by current thread")),
        }
    }

    /// Try direct write lock acquisition without blocking.
    fn try_acquire_write(&self) -> bool {
        if unsafe { self.lock.raw().try_lock_exclusive() } {
            let tid = std::thread::current().id();
            *self.raw_write_owners.lock().entry(tid).or_insert(0) += 1;
            true
        } else {
            false
        }
    }
}

#[pymethods]
impl RwLockReadGuard {
    /// Enter the read lock context.
    ///
    /// Acquisition strategy:
    /// - If this thread already holds a read lock on this RwLock instance,
    ///   use the *recursive* API so we don't deadlock against a waiting writer.
    /// - Otherwise use the *non-recursive* API so the writer-preference queue
    ///   is respected and writers are not starved.
    fn __enter__(s: Bound<'_, Self>) -> PyResult<Bound<'_, Self>> {
        {
            let s_ref = s.borrow();
            if s_ref.guard.is_some() {
                return Err(PyRuntimeError::new_err("Lock guard already entered"));
            }
        }

        let tid = std::thread::current().id();

        // Check if the current thread already holds a read lock (re-entrant case).
        let is_reentrant = {
            let s_ref = s.borrow();
            let registry = s_ref.reader_registry.lock();
            registry.get(&tid).copied().unwrap_or(0) > 0
        };

        if is_reentrant {
            // Re-entrant path: this thread already holds a read lock.
            // Must use recursive API to avoid self-deadlock against a pending writer.
            //
            // Fast-path (no GIL release):
            let try_opt = {
                let s_ref = s.borrow();
                s_ref.lock.try_read_recursive_arc()
            };

            if let Some(guard) = try_opt {
                let mut s_mut = s.borrow_mut();
                s_mut.guard = Some(guard);
            } else {
                // Slow-path: release the CPython GIL and block.
                let lock = {
                    let s_ref = s.borrow();
                    s_ref.lock.clone()
                };
                let guard_wrapper = s.py().detach(|| {
                    UnsafeSend(lock.read_arc_recursive())
                });
                let mut s_mut = s.borrow_mut();
                s_mut.guard = Some(guard_wrapper.0);
            }
        } else {
            // First-time (non-reentrant) path: use writer-fair APIs.
            // New readers will block when a writer is queued, preventing starvation.
            //
            // Fast-path (no GIL release):
            let try_opt = {
                let s_ref = s.borrow();
                s_ref.lock.try_read_arc()
            };

            if let Some(guard) = try_opt {
                let mut s_mut = s.borrow_mut();
                s_mut.guard = Some(guard);
            } else {
                // Slow-path: release the CPython GIL and block waiting for the lock.
                let lock = {
                    let s_ref = s.borrow();
                    s_ref.lock.clone()
                };
                let guard_wrapper = s.py().detach(|| {
                    UnsafeSend(lock.read_arc())
                });
                let mut s_mut = s.borrow_mut();
                s_mut.guard = Some(guard_wrapper.0);
            }
        }

        // Record that this thread now holds one more read lock.
        {
            let s_ref = s.borrow();
            let mut registry = s_ref.reader_registry.lock();
            *registry.entry(tid).or_insert(0) += 1;
        }

        Ok(s.clone())
    }

    /// Exit the read lock context, releasing the lock.
    fn __exit__(&mut self, _exc_type: &Bound<'_, PyAny>, _exc_value: &Bound<'_, PyAny>, _traceback: &Bound<'_, PyAny>) {
        self.guard = None;

        // Decrement (and clean up) this thread's read-lock counter.
        let tid = std::thread::current().id();
        let mut registry = self.reader_registry.lock();
        if let Some(count) = registry.get_mut(&tid) {
            if *count > 1 {
                *count -= 1;
            } else {
                registry.remove(&tid);
            }
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

        // Fast-path: try to acquire the exclusive write lock immediately without releasing GIL.
        let try_opt = {
            let s_ref = s.borrow();
            s_ref.lock.try_write_arc()
        };

        if let Some(guard) = try_opt {
            let mut s_mut = s.borrow_mut();
            s_mut.guard = Some(guard);
        } else {
            // Slow-path: release the CPython GIL and block waiting for the exclusive write lock.
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
