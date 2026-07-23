use pyo3::prelude::*;
use std::sync::atomic::{AtomicI64, AtomicBool, Ordering};

fn parse_ordering(ordering: Option<&str>) -> PyResult<Ordering> {
    match ordering {
        None | Some("seq_cst") | Some("SeqCst") => Ok(Ordering::SeqCst),
        Some("relaxed") | Some("Relaxed") => Ok(Ordering::Relaxed),
        Some("acquire") | Some("Acquire") => Ok(Ordering::Acquire),
        Some("release") | Some("Release") => Ok(Ordering::Release),
        Some("acq_rel") | Some("AcqRel") => Ok(Ordering::AcqRel),
        Some(other) => Err(pyo3::exceptions::PyValueError::new_err(format!("Unsupported memory ordering: {}", other))),
    }
}

fn derive_failure_ordering(ord: Ordering) -> Ordering {
    match ord {
        Ordering::Release => Ordering::Relaxed,
        Ordering::AcqRel => Ordering::Acquire,
        other => other,
    }
}

/// A thread-safe, lock-free 64-bit signed integer using CPU atomic operations.
/// All methods use Ordering::SeqCst by default to guarantee sequential consistency across all threads.
/// Accepts optional ordering="relaxed" or shortcut methods like increment_relaxed() for maximum performance on ARM64.
#[pyclass]
#[repr(align(64))]
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
    #[pyo3(signature = (ordering=None))]
    fn get(&self, ordering: Option<&str>) -> PyResult<i64> {
        let ord = parse_ordering(ordering)?;
        Ok(self.value.load(ord))
    }

    /// Store a new value atomically.
    #[pyo3(signature = (value, ordering=None))]
    fn set(&self, value: i64, ordering: Option<&str>) -> PyResult<()> {
        let ord = parse_ordering(ordering)?;
        self.value.store(value, ord);
        Ok(())
    }

    /// Atomically add delta to the value and return the OLD value.
    #[pyo3(signature = (delta, ordering=None))]
    fn fetch_add(&self, delta: i64, ordering: Option<&str>) -> PyResult<i64> {
        let ord = parse_ordering(ordering)?;
        Ok(self.value.fetch_add(delta, ord))
    }

    /// Fast-path relaxed fetch_add.
    fn fetch_add_relaxed(&self, delta: i64) -> i64 {
        self.value.fetch_add(delta, Ordering::Relaxed)
    }

    /// Atomically add delta to the value and return the NEW value.
    #[pyo3(signature = (delta, ordering=None))]
    fn add_and_get(&self, delta: i64, ordering: Option<&str>) -> PyResult<i64> {
        let ord = parse_ordering(ordering)?;
        Ok(self.value.fetch_add(delta, ord).wrapping_add(delta))
    }

    /// Fast-path relaxed add_and_get.
    fn add_and_get_relaxed(&self, delta: i64) -> i64 {
        self.value.fetch_add(delta, Ordering::Relaxed).wrapping_add(delta)
    }

    /// Atomically subtract delta from the value and return the OLD value.
    #[pyo3(signature = (delta, ordering=None))]
    fn fetch_sub(&self, delta: i64, ordering: Option<&str>) -> PyResult<i64> {
        let ord = parse_ordering(ordering)?;
        Ok(self.value.fetch_sub(delta, ord))
    }

    /// Fast-path relaxed fetch_sub.
    fn fetch_sub_relaxed(&self, delta: i64) -> i64 {
        self.value.fetch_sub(delta, Ordering::Relaxed)
    }

    /// Atomically subtract delta from the value and return the NEW value.
    #[pyo3(signature = (delta, ordering=None))]
    fn sub_and_get(&self, delta: i64, ordering: Option<&str>) -> PyResult<i64> {
        let ord = parse_ordering(ordering)?;
        Ok(self.value.fetch_sub(delta, ord).wrapping_sub(delta))
    }

    /// Fast-path relaxed sub_and_get.
    fn sub_and_get_relaxed(&self, delta: i64) -> i64 {
        self.value.fetch_sub(delta, Ordering::Relaxed).wrapping_sub(delta)
    }

    /// Atomically increment the value by 1 and return the NEW value.
    #[pyo3(signature = (ordering=None))]
    fn increment(&self, ordering: Option<&str>) -> PyResult<i64> {
        let ord = parse_ordering(ordering)?;
        Ok(self.value.fetch_add(1, ord).wrapping_add(1))
    }

    /// Fast-path relaxed increment.
    fn increment_relaxed(&self) -> i64 {
        self.value.fetch_add(1, Ordering::Relaxed).wrapping_add(1)
    }

    /// Atomically decrement the value by 1 and return the NEW value.
    #[pyo3(signature = (ordering=None))]
    fn decrement(&self, ordering: Option<&str>) -> PyResult<i64> {
        let ord = parse_ordering(ordering)?;
        Ok(self.value.fetch_sub(1, ord).wrapping_sub(1))
    }

    /// Fast-path relaxed decrement.
    fn decrement_relaxed(&self) -> i64 {
        self.value.fetch_sub(1, Ordering::Relaxed).wrapping_sub(1)
    }

    /// Atomically swap the value with a new one and return the OLD value.
    #[pyo3(signature = (new_value, ordering=None))]
    fn get_and_set(&self, new_value: i64, ordering: Option<&str>) -> PyResult<i64> {
        let ord = parse_ordering(ordering)?;
        Ok(self.value.swap(new_value, ord))
    }

    /// Atomically compare the current value to expected, and if equal, swap with new_value.
    /// Returns True if the swap succeeded, otherwise False.
    #[pyo3(signature = (expected, new_value, ordering=None))]
    fn compare_and_set(&self, expected: i64, new_value: i64, ordering: Option<&str>) -> PyResult<bool> {
        let ord = parse_ordering(ordering)?;
        let fail_ord = derive_failure_ordering(ord);
        Ok(self.value.compare_exchange(
            expected,
            new_value,
            ord,
            fail_ord
        ).is_ok())
    }

    fn __repr__(&self) -> String {
        format!("AtomicInteger({})", self.value.load(Ordering::SeqCst))
    }

    fn __str__(&self) -> String {
        format!("{}", self.value.load(Ordering::SeqCst))
    }
}

/// A thread-safe, lock-free boolean variable using CPU atomic operations.
#[pyclass]
#[repr(align(64))]
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
    #[pyo3(signature = (ordering=None))]
    fn get(&self, ordering: Option<&str>) -> PyResult<bool> {
        let ord = parse_ordering(ordering)?;
        Ok(self.value.load(ord))
    }

    /// Store a new boolean value atomically.
    #[pyo3(signature = (value, ordering=None))]
    fn set(&self, value: bool, ordering: Option<&str>) -> PyResult<()> {
        let ord = parse_ordering(ordering)?;
        self.value.store(value, ord);
        Ok(())
    }

    /// Atomically swap the value with a new one and return the OLD value.
    #[pyo3(signature = (new_value, ordering=None))]
    fn get_and_set(&self, new_value: bool, ordering: Option<&str>) -> PyResult<bool> {
        let ord = parse_ordering(ordering)?;
        Ok(self.value.swap(new_value, ord))
    }

    /// Atomically compare the current value to expected, and if equal, swap with new_value.
    #[pyo3(signature = (expected, new_value, ordering=None))]
    fn compare_and_set(&self, expected: bool, new_value: bool, ordering: Option<&str>) -> PyResult<bool> {
        let ord = parse_ordering(ordering)?;
        let fail_ord = derive_failure_ordering(ord);
        Ok(self.value.compare_exchange(
            expected,
            new_value,
            ord,
            fail_ord
        ).is_ok())
    }

    fn __repr__(&self) -> String {
        format!("AtomicBoolean({})", if self.value.load(Ordering::SeqCst) { "True" } else { "False" })
    }

    fn __str__(&self) -> String {
        format!("{}", if self.value.load(Ordering::SeqCst) { "True" } else { "False" })
    }
}
