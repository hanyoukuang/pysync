use crate::channel::{RecvOp, SendOp};
use crossbeam_channel::{Receiver, Select, Sender};
use parking_lot::Mutex;
use pyo3::exceptions::{PyRuntimeError, PyTimeoutError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use std::collections::HashMap;
use std::time::Duration;

/// An internal enum representing parsed operation types.
enum OpType {
    Recv(Receiver<Py<PyAny>>, Receiver<()>),
    // Send items are wrapped in a Mutex so they can be safely moved/taken inside
    // the GIL-detached closure where we need a Send-conforming contract.
    Send(Sender<Py<PyAny>>, Mutex<Option<Py<PyAny>>>, Receiver<()>),
}

enum SelectResult {
    RecvSuccess(usize, Py<PyAny>),
    RecvClosed,
    SendSuccess(usize),
    SendClosed,
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
            parsed_ops.push(OpType::Recv(
                recv_op.receiver.clone(),
                recv_op.close_rx.clone(),
            ));
        } else if let Ok(send_op_bound) = op.cast::<SendOp>() {
            let send_op = send_op_bound.borrow();
            parsed_ops.push(OpType::Send(
                send_op.sender.clone(),
                Mutex::new(Some(send_op.item.clone_ref(py))),
                send_op.close_rx.clone(),
            ));
        } else {
            return Err(PyTypeError::new_err(
                "Operations must be of type RecvOp or SendOp",
            ));
        }
    }

    // Step 2: Register the channels and close signal receivers
    let mut sel = Select::new();
    let mut close_op_map = HashMap::new();
    let mut main_op_map = HashMap::new();

    for (op_idx, op) in parsed_ops.iter().enumerate() {
        match op {
            OpType::Recv(rx, close_rx) => {
                let m_idx = sel.recv(rx);
                main_op_map.insert(m_idx, op_idx);
                let c_idx = sel.recv(close_rx);
                close_op_map.insert(c_idx, (op_idx, false));
            }
            OpType::Send(tx, _, close_rx) => {
                let m_idx = sel.send(tx);
                main_op_map.insert(m_idx, op_idx);
                let c_idx = sel.recv(close_rx);
                close_op_map.insert(c_idx, (op_idx, true));
            }
        }
    }

    let timeout_duration = match timeout {
        Some(t) => {
            if t < 0.0 {
                return Err(PyValueError::new_err("Timeout must be non-negative"));
            }
            Some(Duration::from_secs_f64(t))
        }
        None => None,
    };

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

        let select_idx = oper.index();

        if let Some(&(op_idx, is_send)) = close_op_map.get(&select_idx) {
            let close_rx = match &parsed_ops[op_idx] {
                OpType::Recv(_, close_rx) => close_rx,
                OpType::Send(_, _, close_rx) => close_rx,
            };
            let _ = oper.recv(close_rx);

            if is_send {
                return SelectResult::SendClosed;
            } else {
                if let OpType::Recv(rx, _) = &parsed_ops[op_idx] {
                    if let Ok(item) = rx.try_recv() {
                        return SelectResult::RecvSuccess(op_idx, item);
                    }
                }
                return SelectResult::RecvClosed;
            }
        }

        let &op_idx = match main_op_map.get(&select_idx) {
            Some(idx) => idx,
            None => return SelectResult::Timeout,
        };

        match &parsed_ops[op_idx] {
            OpType::Recv(rx, _) => match oper.recv(rx) {
                Ok(res) => SelectResult::RecvSuccess(op_idx, res),
                Err(_) => SelectResult::RecvClosed,
            },
            OpType::Send(tx, item_mutex, _) => {
                let item = match item_mutex.lock().take() {
                    Some(it) => it,
                    None => return SelectResult::ItemConsumed,
                };
                match oper.send(tx, item) {
                    Ok(_) => SelectResult::SendSuccess(op_idx),
                    Err(_) => SelectResult::SendClosed,
                }
            }
        }
    });

    match sel_res {
        SelectResult::RecvSuccess(idx, item) => Ok((idx, Some(item))),
        SelectResult::SendSuccess(idx) => Ok((idx, None)),
        SelectResult::RecvClosed => Err(PyValueError::new_err(
            "Selected channel is closed and empty",
        )),
        SelectResult::SendClosed => Err(PyValueError::new_err("Selected channel is closed")),
        SelectResult::ItemConsumed => Err(PyRuntimeError::new_err("Send item already consumed")),
        SelectResult::Timeout => Err(PyTimeoutError::new_err(
            "select() timed out waiting for ready channel",
        )),
    }
}
