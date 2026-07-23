use pyo3::prelude::*;
use pyo3::{Py, PyAny};
use std::collections::HashMap;
use parking_lot::Mutex;

/// Helper function to check if a Python object is hashable.
/// If not, it raises a Python `TypeError` directly at the API boundary.
fn check_hashable(_py: Python<'_>, key: &Bound<'_, PyAny>) -> PyResult<()> {
    key.hash()?;
    Ok(())
}

/// A highly concurrent, thread-safe hash map with dynamically configurable shard count.
/// Each shard is protected by a Mutex. Python callbacks (like `__eq__` and `__hash__`)
/// are executed entirely OUTSIDE the shard locks, preventing any lock-ordering
/// or recursive deadlocks in multi-threaded GIL-free environments.
#[pyclass(subclass)]
pub struct ConcurrentMap {
    shards: Vec<Mutex<HashMap<u64, Vec<(Py<PyAny>, Py<PyAny>)>>>>,
}

#[pymethods]
impl ConcurrentMap {
    #[new]
    #[pyo3(signature = (shard_count=None))]
    fn new(shard_count: Option<usize>) -> PyResult<Self> {
        let count = match shard_count {
            Some(n) => {
                if n == 0 {
                    return Err(pyo3::exceptions::PyValueError::new_err("shard_count must be greater than zero"));
                }
                n
            }
            None => {
                std::thread::available_parallelism()
                    .map(|p| p.get().next_power_of_two())
                    .unwrap_or(16)
                    .max(16)
            }
        };

        let mut shards = Vec::with_capacity(count);
        for _ in 0..count {
            shards.push(Mutex::new(HashMap::new()));
        }

        Ok(ConcurrentMap { shards })
    }

    #[getter]
    fn shard_count(&self) -> usize {
        self.shards.len()
    }

    /// Retrieve the value associated with the key.
    fn get(&self, py: Python<'_>, key: Bound<'_, PyAny>) -> PyResult<Option<Py<PyAny>>> {
        check_hashable(py, &key)?;
        let h = key.hash()? as u64;
        let idx = (h as usize) % self.shards.len();

        // Retrieve and clone candidate keys under the lock
        let candidates = {
            let shard = self.shards[idx].lock();
            shard.get(&h).map(|list| {
                list.iter()
                    .map(|(k, v)| (k.clone_ref(py), v.clone_ref(py)))
                    .collect::<Vec<_>>()
            })
        };

        // Perform key comparisons outside the lock
        if let Some(list) = candidates {
            for (k, v) in list {
                if k.bind(py).eq(&key)? {
                    return Ok(Some(v));
                }
            }
        }
        Ok(None)
    }

    /// Retrieve the value associated with the key as a tuple (found, value).
    fn get_val(&self, py: Python<'_>, key: Bound<'_, PyAny>) -> PyResult<(bool, Py<PyAny>)> {
        check_hashable(py, &key)?;
        let h = key.hash()? as u64;
        let idx = (h as usize) % self.shards.len();

        let candidates = {
            let shard = self.shards[idx].lock();
            shard.get(&h).map(|list| {
                list.iter()
                    .map(|(k, v)| (k.clone_ref(py), v.clone_ref(py)))
                    .collect::<Vec<_>>()
            })
        };

        if let Some(list) = candidates {
            for (k, v) in list {
                if k.bind(py).eq(&key)? {
                    return Ok((true, v));
                }
            }
        }
        Ok((false, py.None()))
    }

    /// Set the value for the key.
    fn set(&self, py: Python<'_>, key: Bound<'_, PyAny>, value: Py<PyAny>) -> PyResult<()> {
        check_hashable(py, &key)?;
        let h = key.hash()? as u64;
        let idx = (h as usize) % self.shards.len();
        let pykey = key.clone().unbind();

        // 1. Retrieve and clone candidate keys ONLY (avoid cloning values)
        let candidate_keys = {
            let shard = self.shards[idx].lock();
            shard.get(&h).map(|list| {
                list.iter()
                    .map(|(k, _)| k.clone_ref(py))
                    .collect::<Vec<_>>()
            })
        };

        let mut matching_key = None;
        if let Some(keys) = &candidate_keys {
            // 2. Perform key comparisons outside the lock
            for k in keys {
                if k.bind(py).eq(&key)? {
                    matching_key = Some(k.clone_ref(py));
                    break;
                }
            }
        }

        // 3. Re-lock and update or insert
        let mut shard = self.shards[idx].lock();
        let list = shard.entry(h).or_insert_with(Vec::new);

        if let Some(m_key) = matching_key {
            // Find and update the existing key using pointer equality
            if let Some(pos) = list.iter().position(|(k, _)| k.is(&m_key)) {
                list[pos].1 = value;
                return Ok(());
            }
        }

        // Re-check existing list entries under lock in case key was deleted and re-inserted during the lock-release window
        for (pos, (k, _)) in list.iter().enumerate() {
            if k.is(&pykey) || k.bind(py).eq(&key)? {
                list[pos].1 = value;
                return Ok(());
            }
        }

        list.push((pykey, value));
        Ok(())
    }

    /// Delete the key from the map. Returns True if the key was present, otherwise False.
    fn delete(&self, py: Python<'_>, key: Bound<'_, PyAny>) -> PyResult<bool> {
        check_hashable(py, &key)?;
        let h = key.hash()? as u64;
        let idx = (h as usize) % self.shards.len();

        // 1. Retrieve and clone candidate keys ONLY (avoid cloning values)
        let candidate_keys = {
            let shard = self.shards[idx].lock();
            shard.get(&h).map(|list| {
                list.iter()
                    .map(|(k, _)| k.clone_ref(py))
                    .collect::<Vec<_>>()
            })
        };

        let mut matching_key = None;
        if let Some(keys) = &candidate_keys {
            // 2. Perform key comparisons outside the lock
            for k in keys {
                if k.bind(py).eq(&key)? {
                    matching_key = Some(k.clone_ref(py));
                    break;
                }
            }
        }

        let mut shard = self.shards[idx].lock();
        if let Some(list) = shard.get_mut(&h) {
            if let Some(m_key) = matching_key {
                if let Some(pos) = list.iter().position(|(k, _)| k.is(&m_key)) {
                    list.remove(pos);
                    if list.is_empty() {
                        shard.remove(&h);
                    }
                    return Ok(true);
                }
            }
            // Fallback check: verify equality under lock if key was re-inserted concurrently
            for pos in 0..list.len() {
                if list[pos].0.is(&key) || list[pos].0.bind(py).eq(&key)? {
                    list.remove(pos);
                    if list.is_empty() {
                        shard.remove(&h);
                    }
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    /// Atomically remove and return the key's value.
    /// Returns `(true, value)` if found, or `(false, None)` if absent.
    fn pop_val(&self, py: Python<'_>, key: Bound<'_, PyAny>) -> PyResult<(bool, Py<PyAny>)> {
        check_hashable(py, &key)?;
        let h = key.hash()? as u64;
        let idx = (h as usize) % self.shards.len();

        // 1. Retrieve and clone candidate keys ONLY (avoid cloning values)
        let candidate_keys = {
            let shard = self.shards[idx].lock();
            shard.get(&h).map(|list| {
                list.iter()
                    .map(|(k, _)| k.clone_ref(py))
                    .collect::<Vec<_>>()
            })
        };

        let mut matching_key = None;
        if let Some(keys) = &candidate_keys {
            // 2. Perform key comparisons outside the lock
            for k in keys {
                if k.bind(py).eq(&key)? {
                    matching_key = Some(k.clone_ref(py));
                    break;
                }
            }
        }

        let mut shard = self.shards[idx].lock();
        if let Some(list) = shard.get_mut(&h) {
            if let Some(m_key) = matching_key {
                if let Some(pos) = list.iter().position(|(k, _)| k.is(&m_key)) {
                    let (_, val) = list.remove(pos);
                    if list.is_empty() {
                        shard.remove(&h);
                    }
                    return Ok((true, val));
                }
            }
            // Fallback check: verify equality under lock if key was re-inserted concurrently
            for pos in 0..list.len() {
                if list[pos].0.is(&key) || list[pos].0.bind(py).eq(&key)? {
                    let (_, val) = list.remove(pos);
                    if list.is_empty() {
                        shard.remove(&h);
                    }
                    return Ok((true, val));
                }
            }
        }
        Ok((false, py.None()))
    }

    /// Atomic setdefault: if key is present, returns `(true, existing_value)`;
    /// if absent, inserts `default` and returns `(false, default)`.
    fn setdefault_val(&self, py: Python<'_>, key: Bound<'_, PyAny>, default: Py<PyAny>) -> PyResult<(bool, Py<PyAny>)> {
        check_hashable(py, &key)?;
        let h = key.hash()? as u64;
        let idx = (h as usize) % self.shards.len();
        let pykey = key.clone().unbind();

        let candidate_keys = {
            let shard = self.shards[idx].lock();
            shard.get(&h).map(|list| {
                list.iter()
                    .map(|(k, _)| k.clone_ref(py))
                    .collect::<Vec<_>>()
            })
        };

        let mut matching_key = None;
        if let Some(keys) = &candidate_keys {
            for k in keys {
                if k.bind(py).eq(&key)? {
                    matching_key = Some(k.clone_ref(py));
                    break;
                }
            }
        }

        let mut shard = self.shards[idx].lock();
        let list = shard.entry(h).or_insert_with(Vec::new);

        if let Some(m_key) = matching_key {
            if let Some(pos) = list.iter().position(|(k, _)| k.is(&m_key)) {
                return Ok((true, list[pos].1.clone_ref(py)));
            }
        }

        for (k, v) in list.iter() {
            if k.is(&pykey) || k.bind(py).eq(&key)? {
                return Ok((true, v.clone_ref(py)));
            }
        }

        list.push((pykey, default.clone_ref(py)));
        Ok((false, default))
    }

    /// Check if the key exists in the map.
    fn contains_key(&self, py: Python<'_>, key: Bound<'_, PyAny>) -> PyResult<bool> {
        check_hashable(py, &key)?;
        let h = key.hash()? as u64;
        let idx = (h as usize) % self.shards.len();

        // Retrieve and clone candidate keys ONLY (avoid cloning values)
        let candidate_keys = {
            let shard = self.shards[idx].lock();
            shard.get(&h).map(|list| {
                list.iter()
                    .map(|(k, _)| k.clone_ref(py))
                    .collect::<Vec<_>>()
            })
        };

        // Perform key comparisons outside the lock
        if let Some(keys) = candidate_keys {
            for k in keys {
                if k.bind(py).eq(&key)? {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    /// Return the number of elements in the map.
    fn len(&self) -> usize {
        let mut total = 0;
        for shard in &self.shards {
            let guard = shard.lock();
            for list in guard.values() {
                total += list.len();
            }
        }
        total
    }

    fn __len__(&self) -> usize {
        self.len()
    }

    /// Clear all elements from the map.
    fn clear(&self) {
        for shard in &self.shards {
            shard.lock().clear();
        }
    }

    /// Retrieve all keys in the map.
    fn keys(&self, py: Python<'_>) -> Vec<Py<PyAny>> {
        let mut all_keys = Vec::new();
        for shard in &self.shards {
            let guard = shard.lock();
            for list in guard.values() {
                for (k, _) in list {
                    all_keys.push(k.clone_ref(py));
                }
            }
        }
        all_keys
    }

    /// Retrieve all values in the map.
    fn values(&self, py: Python<'_>) -> Vec<Py<PyAny>> {
        let mut all_values = Vec::new();
        for shard in &self.shards {
            let guard = shard.lock();
            for list in guard.values() {
                for (_, v) in list {
                    all_values.push(v.clone_ref(py));
                }
            }
        }
        all_values
    }

    /// Retrieve all key-value tuples in the map.
    fn items(&self, py: Python<'_>) -> Vec<(Py<PyAny>, Py<PyAny>)> {
        let mut all_items = Vec::new();
        for shard in &self.shards {
            let guard = shard.lock();
            for list in guard.values() {
                for (k, v) in list {
                    all_items.push((k.clone_ref(py), v.clone_ref(py)));
                }
            }
        }
        all_items
    }
}
