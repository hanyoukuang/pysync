use pyo3::prelude::*;

mod channel;
mod map;
mod pool;
mod atomic;
mod select;
mod lock;

/// The entry point for the compiled PyO3 native extension module `_pysync`.
/// Registers all core Rust concurrency primitives so they are visible from Python.
#[pymodule]
fn _pysync(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // 1. CSP Message-Passing Channels
    m.add_class::<channel::Channel>()?;
    m.add_class::<channel::RecvOp>()?;
    m.add_class::<channel::SendOp>()?;
    
    // 2. High-Performance Concurrent Maps
    m.add_class::<map::ConcurrentMap>()?;
    
    // 3. Native Thread Pool Scheduling
    m.add_class::<pool::ThreadPool>()?;
    
    // 4. Lock-Free CPU Atomics
    m.add_class::<atomic::AtomicInteger>()?;
    m.add_class::<atomic::AtomicBoolean>()?;
    
    // 5. Reader-Writer Locks & Guards
    m.add_class::<lock::RwLock>()?;
    m.add_class::<lock::RwLockReadGuard>()?;
    m.add_class::<lock::RwLockWriteGuard>()?;
    
    // 6. Go-Style select() Function
    m.add_function(wrap_pyfunction!(select::select, m)?)?;
    
    Ok(())
}
