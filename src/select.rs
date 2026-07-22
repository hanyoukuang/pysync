use std::time::Duration;
use pyo3::prelude::*;
use pyo3::exceptions::{PyValueError, PyTypeError, PyRuntimeError, PyTimeoutError};
use crossbeam_channel::Select;
use crate::channel::{RecvOp, SendOp};
use parking_lot::Mutex;

/// An internal enum representing parsed operation types.
enum OpType {
    Recv(crossbeam_channel::Receiver<Py<PyAny>>),
    // Send items are wrapped in a Mutex so they can be safely moved/taken inside
    // the GIL-detached closure where we need a Send-conforming contract.
    Send(crossbeam_channel::Sender<Py<PyAny>>, Mutex<Option<Py<PyAny>>>),
}

enum SelectResult {
    RecvSuccess(usize, Py<PyAny>),
    RecvClosed,
    SendSuccess(usize),
    SendClosed(Py<PyAny>),
    ItemConsumed,
    Timeout,
}

/// Block until one of the channel operations (recv_op or send_op) is ready, or until optional timeout expires.
/// Employs Go-style multiplexing for multiple channel operations.
#[pyfunction]
#[pyo3(signature = (ops, timeout=None))]
pub fn select(
    py: Python<'_>,
    ops: Vec<Bound<'_, PyAny>>,
    timeout: Option<f64>,
) -> PyResult<(usize, Option<Py<PyAny>>)> {
    if ops.is_empty() {
        return Err(PyValueError::new_err("Operations list cannot be empty"));
    }

    let mut parsed_ops = Vec::with_capacity(ops.len());

    // Step 1: Parse and clone all channel handles to ensure they outlive the Select instance
    for op in &ops {
        if let Ok(recv_op_bound) = op.cast::<RecvOp>() {
            let recv_op = recv_op_bound.borrow();
            parsed_ops.push(OpType::Recv(recv_op.receiver.clone()));
        } else if let Ok(send_op_bound) = op.cast::<SendOp>() {
            let send_op = send_op_bound.borrow();
            parsed_ops.push(OpType::Send(
                send_op.sender.clone(),
                Mutex::new(Some(send_op.item.clone_ref(py))),
            ));
        } else {
            return Err(PyTypeError::new_err("Operations must be of type RecvOp or SendOp"));
        }
    }

    // Step 2: Register the channels from our cloned storage
    let mut sel = Select::new();
    for op in &parsed_ops {
        match op {
            OpType::Recv(rx) => {
                sel.recv(rx);
            }
            OpType::Send(tx, _) => {
                sel.send(tx);
            }
        }
    }

    let timeout_duration = timeout.map(|t| Duration::from_secs_f64(t.max(0.0)));

    // Step 3: Block and perform selection. Releases the GIL to allow other threads to run.
    let sel_res = py.detach(|| -> SelectResult {
        let oper_res = match timeout_duration {
            Some(duration) => sel.select_timeout(duration).map_err(|_| ()),
            None => Ok(sel.select()),
        };

        let oper = match oper_res {
            Ok(op) => op,
            Err(_) => return SelectResult::Timeout,
        };

        let idx = oper.index();
        match &parsed_ops[idx] {
            OpType::Recv(rx) => {
                match oper.recv(rx) {
                    Ok(res) => SelectResult::RecvSuccess(idx, res),
                    Err(_) => SelectResult::RecvClosed,
                }
            }
            OpType::Send(tx, item_mutex) => {
                let item = match item_mutex.lock().take() {
                    Some(it) => it,
                    None => return SelectResult::ItemConsumed,
                };
                match oper.send(tx, item) {
                    Ok(_) => SelectResult::SendSuccess(idx),
                    Err(err) => SelectResult::SendClosed(err.0),
                }
            }
        }
    });

    match sel_res {
        SelectResult::RecvSuccess(idx, item) => Ok((idx, Some(item))),
        SelectResult::SendSuccess(idx) => Ok((idx, None)),
        SelectResult::RecvClosed => Err(PyValueError::new_err("Selected channel is closed and empty")),
        SelectResult::SendClosed(_dropped_item) => Err(PyValueError::new_err("Selected channel is closed")),
        SelectResult::ItemConsumed => Err(PyRuntimeError::new_err("Send item already consumed")),
        SelectResult::Timeout => Err(PyTimeoutError::new_err("select() timed out waiting for ready channel")),
    }
}

