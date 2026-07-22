use pyo3::prelude::*;
use std::sync::atomic::{AtomicI64, AtomicBool, Ordering};

/// A thread-safe, lock-free 64-bit signed integer using CPU atomic operations.
/// All methods use Ordering::SeqCst to guarantee sequential consistency across all threads,
/// preventing instruction/memory reordering bugs on weak-memory architectures like ARM (Apple Silicon).
#[pyclass]
pub struct AtomicInteger {
    value: AtomicI64,
}

#[pymethods]
impl AtomicInteger {
    #[new]
    #[pyo3(signature = (value=0))]
    fn new(value: i64) -> Self {
        AtomicInteger {
            value: AtomicI64::new(value),
        }
    }

    /// Load the current value atomically.
    fn get(&self) -> i64 {
        self.value.load(Ordering::SeqCst)
    }

    /// Store a new value atomically.
    fn set(&self, value: i64) {
        self.value.store(value, Ordering::SeqCst);
    }

    /// Atomically add delta to the value and return the OLD value.
    fn fetch_add(&self, delta: i64) -> i64 {
        self.value.fetch_add(delta, Ordering::SeqCst)
    }

    /// Atomically add delta to the value and return the NEW value.
    fn add_and_get(&self, delta: i64) -> i64 {
        self.value.fetch_add(delta, Ordering::SeqCst).wrapping_add(delta)
    }

    /// Atomically subtract delta from the value and return the OLD value.
    fn fetch_sub(&self, delta: i64) -> i64 {
        self.value.fetch_sub(delta, Ordering::SeqCst)
    }

    /// Atomically subtract delta from the value and return the NEW value.
    fn sub_and_get(&self, delta: i64) -> i64 {
        self.value.fetch_sub(delta, Ordering::SeqCst).wrapping_sub(delta)
    }

    /// Atomically increment the value by 1 and return the NEW value.
    fn increment(&self) -> i64 {
        self.value.fetch_add(1, Ordering::SeqCst).wrapping_add(1)
    }

    /// Atomically decrement the value by 1 and return the NEW value.
    fn decrement(&self) -> i64 {
        self.value.fetch_sub(1, Ordering::SeqCst).wrapping_sub(1)
    }

    /// Atomically swap the value with a new one and return the OLD value.
    fn get_and_set(&self, new_value: i64) -> i64 {
        self.value.swap(new_value, Ordering::SeqCst)
    }

    /// Atomically compare the current value to expected, and if equal, swap with new_value.
    /// Returns True if the swap succeeded, otherwise False.
    fn compare_and_set(&self, expected: i64, new_value: i64) -> bool {
        self.value.compare_exchange(
            expected,
            new_value,
            Ordering::SeqCst,
            Ordering::SeqCst
        ).is_ok()
    }

    fn __repr__(&self) -> String {
        format!("AtomicInteger({})", self.get())
    }

    fn __str__(&self) -> String {
        format!("{}", self.get())
    }
}

/// A thread-safe, lock-free boolean variable using CPU atomic operations.
#[pyclass]
pub struct AtomicBoolean {
    value: AtomicBool,
}

#[pymethods]
impl AtomicBoolean {
    #[new]
    #[pyo3(signature = (value=false))]
    fn new(value: bool) -> Self {
        AtomicBoolean {
            value: AtomicBool::new(value),
        }
    }

    /// Load the current boolean value atomically.
    fn get(&self) -> bool {
        self.value.load(Ordering::SeqCst)
    }

    /// Store a new boolean value atomically.
    fn set(&self, value: bool) {
        self.value.store(value, Ordering::SeqCst);
    }

    /// Atomically swap the value with a new one and return the OLD value.
    fn get_and_set(&self, new_value: bool) -> bool {
        self.value.swap(new_value, Ordering::SeqCst)
    }

    /// Atomically compare the current value to expected, and if equal, swap with new_value.
    fn compare_and_set(&self, expected: bool, new_value: bool) -> bool {
        self.value.compare_exchange(
            expected,
            new_value,
            Ordering::SeqCst,
            Ordering::SeqCst
        ).is_ok()
    }

    fn __repr__(&self) -> String {
        format!("AtomicBoolean({})", if self.get() { "True" } else { "False" })
    }

    fn __str__(&self) -> String {
        format!("{}", if self.get() { "True" } else { "False" })
    }
}
