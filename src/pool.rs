use pyo3::prelude::*;
use pyo3::exceptions::{PyValueError, PyRuntimeError};
use pyo3::{Py, PyAny};
use pyo3::types::{PyTuple, PyDict};
use crossbeam_channel::{Sender, unbounded};
use std::thread::{self, JoinHandle};
use std::sync::Arc;
use parking_lot::Mutex;

/// A task wrapper containing callable methods, arguments, target Python future handles, and contextvars context.
struct Task {
    callable: Py<PyAny>,
    args: Py<PyTuple>,
    kwargs: Option<Py<PyDict>>,
    future: Py<PyAny>,
    context: Option<Py<PyAny>>,
}

/// An OS-level physical thread pool scheduler for heavy parallel computing.
/// Avoids GIL contention and allows true parallel multi-threaded execution.
/// Supports CPython weakref and implements automatic RAII resource reclamation on Drop.
#[pyclass(weakref)]
pub struct ThreadPool {
    sender: Option<Sender<Task>>,
    workers: Vec<JoinHandle<()>>,
    active: Arc<Mutex<bool>>,
    cancel_pending: Arc<Mutex<bool>>,
    future_class: Py<PyAny>,
}

#[pymethods]
impl ThreadPool {
    #[new]
    #[pyo3(signature = (num_workers=None))]
    fn new(num_workers: Option<isize>) -> PyResult<Self> {
        if let Some(val) = num_workers {
            if val <= 0 {
                return Err(PyValueError::new_err("num_workers must be greater than zero"));
            }
        }

        // Default to the machine's available physical CPU core count
        let workers_count = num_workers.unwrap_or_else(|| {
            thread::available_parallelism().map(|n| n.get() as isize).unwrap_or(4)
        }) as usize;

        let (sender, receiver) = unbounded::<Task>();
        let active = Arc::new(Mutex::new(true));
        let cancel_pending = Arc::new(Mutex::new(false));
        let mut workers = Vec::with_capacity(workers_count);

        let future_class = Python::attach(|py| {
            let futures_mod = py.import("concurrent.futures")?;
            let cls = futures_mod.getattr("Future")?;
            Ok::<_, PyErr>(cls.unbind())
        })?;

        for _ in 0..workers_count {
            let rx = receiver.clone();
            let cancel_flag = Arc::clone(&cancel_pending);

            let handle = thread::spawn(move || {
                // Block waiting for new tasks until channel disconnects
                while let Ok(task) = rx.recv() {
                    Python::attach(|py| {
                        let future_bound = task.future.bind(py);
                        if *cancel_flag.lock() {
                            let _ = future_bound.call_method0("cancel");
                        } else {
                            // Invoke task within the captured Python contextvars Context
                            let result = if let Some(ref ctx) = task.context {
                                let bound_args = task.args.bind(py);
                                let mut run_args = Vec::with_capacity(1 + bound_args.len());
                                run_args.push(task.callable.bind(py).clone().into_any());
                                run_args.extend(bound_args.iter());
                                if let Ok(tuple) = PyTuple::new(py, &run_args) {
                                    let kwargs_bound = task.kwargs.as_ref().map(|d| d.bind(py));
                                    ctx.bind(py).call_method("run", tuple, kwargs_bound)
                                } else {
                                    let callable_bound = task.callable.bind(py);
                                    let args_bound = task.args.bind(py);
                                    let kwargs_bound = task.kwargs.as_ref().map(|d| d.bind(py));
                                    callable_bound.call(args_bound, kwargs_bound)
                                }
                            } else {
                                let callable_bound = task.callable.bind(py);
                                let args_bound = task.args.bind(py);
                                let kwargs_bound = task.kwargs.as_ref().map(|d| d.bind(py));
                                callable_bound.call(args_bound, kwargs_bound)
                            };

                            // Propagate the result or error back to the Python Future
                            match result {
                                Ok(val) => {
                                    let _ = future_bound.call_method1("set_result", (val,));
                                }
                                Err(err) => {
                                    let _ = future_bound.call_method1("set_exception", (err,));
                                }
                            }
                        }
                    });
                }
            });
            workers.push(handle);
        }

        Ok(ThreadPool {
            sender: Some(sender),
            workers,
            active,
            cancel_pending,
            future_class,
        })
    }

    /// Submit a task for parallel execution in the thread pool.
    /// Instantiates and returns a standard `concurrent.futures.Future`.
    #[pyo3(signature = (func, *args, **kwargs))]
    fn submit(
        &self,
        py: Python<'_>,
        func: Py<PyAny>,
        args: Bound<'_, PyTuple>,
        kwargs: Option<Bound<'_, PyDict>>,
    ) -> PyResult<Py<PyAny>> {
        if !*self.active.lock() {
            return Err(PyRuntimeError::new_err("ThreadPool is shutdown"));
        }

        let sender = self.sender.as_ref().ok_or_else(|| {
            PyRuntimeError::new_err("ThreadPool is shutdown")
        })?;

        // Capture current thread's contextvars Context
        let context = match py.import("contextvars") {
            Ok(cv) => match cv.getattr("copy_context") {
                Ok(copy_fn) => copy_fn.call0().ok().map(|c| c.unbind()),
                Err(_) => None,
            },
            Err(_) => None,
        };

        // Instantiate a Future using the pre-cached reference
        let future = self.future_class.bind(py).call0()?;

        let task = Task {
            callable: func,
            args: args.unbind(),
            kwargs: kwargs.map(|k| k.unbind()),
            future: future.clone().unbind(),
            context,
        };

        sender.send(task).map_err(|_| {
            PyRuntimeError::new_err("Failed to submit task to worker threads")
        })?;

        Ok(future.unbind())
    }

    /// Shut down the thread pool, dropping the sender and joining all worker threads.
    #[pyo3(signature = (wait=true, cancel_futures=false))]
    fn shutdown(&mut self, py: Python<'_>, wait: bool, cancel_futures: bool) {
        {
            let mut active = self.active.lock();
            if !*active {
                return;
            }
            *active = false;
        }

        if cancel_futures {
            *self.cancel_pending.lock() = true;
        }

        // Drop the sender, waking up worker threads with RecvError once queue drains
        self.sender = None;

        // Block and join all worker threads detached from GIL if wait is true
        let workers = std::mem::take(&mut self.workers);
        if wait && !workers.is_empty() {
            py.detach(|| {
                for handle in workers {
                    let _ = handle.join();
                }
            });
        } else if !workers.is_empty() {
            // When wait is false, spawn a background helper thread to join workers cleanly
            std::thread::spawn(move || {
                for handle in workers {
                    let _ = handle.join();
                }
            });
        }
    }
}

/// Implement Drop trait to ensure automatic resource cleanup.
/// Prevents orphaned background threads if the Python GC reclaims ThreadPool.
impl Drop for ThreadPool {
    fn drop(&mut self) {
        {
            let mut active = self.active.lock();
            *active = false;
        }

        // Drops the sender, causing worker receivers to exit once work queue is empty.
        self.sender = None;

        // Join worker threads to ensure clean shutdown without detaching threads
        for handle in self.workers.drain(..) {
            let _ = handle.join();
        }
    }
}
