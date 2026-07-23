use pyo3::prelude::*;
use pyo3::{Py, PyAny};
use std::collections::HashMap;
use parking_lot::Mutex;

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

    /// Atomically get the existing value for key, or insert default and return default if absent.
    fn get_or_insert(&self, py: Python<'_>, key: Bound<'_, PyAny>, default: Py<PyAny>) -> PyResult<Py<PyAny>> {
        let h = key.hash()? as u64;
        let idx = (h as usize) % self.shards.len();
        let pykey = key.clone().unbind();

        let mut checked_keys: Vec<Py<PyAny>> = Vec::new();

        loop {
            // 1. Collect candidate keys under shard lock that haven't been checked yet
            let new_candidates = {
                let shard = self.shards[idx].lock();
                if let Some(list) = shard.get(&h) {
                    list.iter()
                        .filter_map(|(k, v)| {
                            if checked_keys.iter().any(|ck| ck.is(k)) {
                                None
                            } else {
                                Some((k.clone_ref(py), v.clone_ref(py)))
                            }
                        })
                        .collect::<Vec<_>>()
                } else {
                    Vec::new()
                }
            };

            // 2. Perform Python equality checks OUTSIDE the lock
            for (k, v) in new_candidates {
                if k.bind(py).eq(&key)? {
                    return Ok(v);
                }
                checked_keys.push(k);
            }

            // 3. Re-lock shard and attempt insertion if no new candidates appeared
            let mut shard = self.shards[idx].lock();
            let list = shard.entry(h).or_insert_with(Vec::new);

            // Check if any candidate exists in list that hasn't been checked for equality
            let has_unbound_candidate = list.iter().any(|(k, _)| !checked_keys.iter().any(|ck| ck.is(k)));

            if has_unbound_candidate {
                // Another thread inserted an item under hash `h` while lock was released!
                // Retry loop to evaluate the new candidate outside lock.
                continue;
            }

            // Fallback pointer identity check
            if let Some(pos) = list.iter().position(|(k, _)| k.is(&pykey)) {
                return Ok(list[pos].1.clone_ref(py));
            }

            // Atomically insert key and default value
            list.push((pykey.clone_ref(py), default.clone_ref(py)));
            return Ok(default);
        }
    }

    /// Set the value for the key.
    fn set(&self, py: Python<'_>, key: Bound<'_, PyAny>, value: Py<PyAny>) -> PyResult<()> {
        let h = key.hash()? as u64;
        let idx = (h as usize) % self.shards.len();
        let pykey = key.clone().unbind();

        // Fast-path: empty bucket or pointer identity match under lock (zero heap allocation)
        {
            let mut shard = self.shards[idx].lock();
            match shard.entry(h) {
                std::collections::hash_map::Entry::Vacant(v) => {
                    v.insert(vec![(pykey, value)]);
                    return Ok(());
                }
                std::collections::hash_map::Entry::Occupied(mut o) => {
                    let list = o.get_mut();
                    if list.is_empty() {
                        list.push((pykey, value));
                        return Ok(());
                    }
                    if let Some(pos) = list.iter().position(|(k, _)| k.is(&pykey)) {
                        list[pos].1 = value;
                        return Ok(());
                    }
                }
            }
        }

        // Slow-path for hash collision with distinct PyObjects: retry loop
        let mut checked_keys: Vec<Py<PyAny>> = Vec::new();

        loop {
            // 1. Collect candidate keys under lock that haven't been checked yet
            let new_candidates = {
                let shard = self.shards[idx].lock();
                if let Some(list) = shard.get(&h) {
                    list.iter()
                        .filter_map(|(k, _)| {
                            if checked_keys.iter().any(|ck| ck.is(k)) {
                                None
                            } else {
                                Some(k.clone_ref(py))
                            }
                        })
                        .collect::<Vec<_>>()
                } else {
                    Vec::new()
                }
            };

            // 2. Perform key comparisons outside the lock
            for k in new_candidates {
                let is_match = k.bind(py).eq(&key)?;
                checked_keys.push(k.clone_ref(py));
                if is_match {
                    let mut shard = self.shards[idx].lock();
                    if let Some(list) = shard.get_mut(&h) {
                        if let Some(pos) = list.iter().position(|(candidate, _)| candidate.is(&k)) {
                            list[pos].1 = value;
                            return Ok(());
                        }
                    }
                    // k matched __eq__ outside lock, but pointer identity failed after re-locking
                    // because another thread replaced or removed the key object while lock was released.
                    // k is pushed to checked_keys above so it is explicitly marked as evaluated.
                    break;
                }
            }

            // 3. Re-lock and update or insert if no unchecked candidate remains
            let mut shard = self.shards[idx].lock();
            let list = shard.entry(h).or_insert_with(Vec::new);

            let has_unbound_candidate = list.iter().any(|(k, _)| !checked_keys.iter().any(|ck| ck.is(k)));
            if has_unbound_candidate {
                continue;
            }

            if let Some(pos) = list.iter().position(|(k, _)| k.is(&pykey)) {
                list[pos].1 = value;
                return Ok(());
            }

            list.push((pykey, value));
            return Ok(());
        }
    }

    /// Delete the key from the map. Returns True if the key was present, otherwise False.
    fn delete(&self, py: Python<'_>, key: Bound<'_, PyAny>) -> PyResult<bool> {
        let h = key.hash()? as u64;
        let idx = (h as usize) % self.shards.len();
        let pykey = key.clone().unbind();

        // Fast-path: empty bucket or pointer identity match under lock (zero heap allocation)
        {
            let mut shard = self.shards[idx].lock();
            if let Some(list) = shard.get_mut(&h) {
                if list.is_empty() {
                    return Ok(false);
                }
                if let Some(pos) = list.iter().position(|(k, _)| k.is(&pykey)) {
                    list.remove(pos);
                    if list.is_empty() {
                        shard.remove(&h);
                    }
                    return Ok(true);
                }
            } else {
                return Ok(false);
            }
        }

        // Slow-path for hash collision with distinct PyObjects: retry loop
        let mut checked_keys: Vec<Py<PyAny>> = Vec::new();

        loop {
            // 1. Collect candidate keys under lock that haven't been checked yet
            let new_candidates = {
                let shard = self.shards[idx].lock();
                if let Some(list) = shard.get(&h) {
                    list.iter()
                        .filter_map(|(k, _)| {
                            if checked_keys.iter().any(|ck| ck.is(k)) {
                                None
                            } else {
                                Some(k.clone_ref(py))
                            }
                        })
                        .collect::<Vec<_>>()
                } else {
                    Vec::new()
                }
            };

            // 2. Perform key comparisons outside the lock
            for k in new_candidates {
                let is_match = k.bind(py).eq(&key)?;
                checked_keys.push(k.clone_ref(py));
                if is_match {
                    let mut shard = self.shards[idx].lock();
                    if let Some(list) = shard.get_mut(&h) {
                        if let Some(pos) = list.iter().position(|(candidate, _)| candidate.is(&k)) {
                            list.remove(pos);
                            if list.is_empty() {
                                shard.remove(&h);
                            }
                            return Ok(true);
                        }
                    }
                    // k matched __eq__ outside lock, but pointer identity failed after re-locking
                    // because another thread replaced or removed the key object while lock was released.
                    // k is pushed to checked_keys above so it is explicitly marked as evaluated.
                    break;
                }
            }

            // 3. Re-lock shard and return false if no unchecked candidate remains
            let mut shard = self.shards[idx].lock();
            if let Some(list) = shard.get_mut(&h) {
                let has_unbound_candidate = list.iter().any(|(k, _)| !checked_keys.iter().any(|ck| ck.is(k)));
                if has_unbound_candidate {
                    continue;
                }

                if let Some(pos) = list.iter().position(|(k, _)| k.is(&pykey)) {
                    list.remove(pos);
                    if list.is_empty() {
                        shard.remove(&h);
                    }
                    return Ok(true);
                }
            }

            return Ok(false);
        }
    }

    /// Atomically remove and return the key's value.
    /// Returns `(true, value)` if found, or `(false, None)` if absent.
    fn pop_val(&self, py: Python<'_>, key: Bound<'_, PyAny>) -> PyResult<(bool, Py<PyAny>)> {
        let h = key.hash()? as u64;
        let idx = (h as usize) % self.shards.len();
        let pykey = key.clone().unbind();

        // Fast-path: empty bucket or pointer identity match under lock (zero heap allocation)
        {
            let mut shard = self.shards[idx].lock();
            if let Some(list) = shard.get_mut(&h) {
                if list.is_empty() {
                    return Ok((false, py.None()));
                }
                if let Some(pos) = list.iter().position(|(k, _)| k.is(&pykey)) {
                    let (_, val) = list.remove(pos);
                    if list.is_empty() {
                        shard.remove(&h);
                    }
                    return Ok((true, val));
                }
            } else {
                return Ok((false, py.None()));
            }
        }

        // Slow-path for hash collision with distinct PyObjects: retry loop
        let mut checked_keys: Vec<Py<PyAny>> = Vec::new();

        loop {
            // 1. Collect candidate keys under lock that haven't been checked yet
            let new_candidates = {
                let shard = self.shards[idx].lock();
                if let Some(list) = shard.get(&h) {
                    list.iter()
                        .filter_map(|(k, _)| {
                            if checked_keys.iter().any(|ck| ck.is(k)) {
                                None
                            } else {
                                Some(k.clone_ref(py))
                            }
                        })
                        .collect::<Vec<_>>()
                } else {
                    Vec::new()
                }
            };

            // 2. Perform key comparisons outside the lock
            for k in new_candidates {
                let is_match = k.bind(py).eq(&key)?;
                checked_keys.push(k.clone_ref(py));
                if is_match {
                    let mut shard = self.shards[idx].lock();
                    if let Some(list) = shard.get_mut(&h) {
                        if let Some(pos) = list.iter().position(|(candidate, _)| candidate.is(&k)) {
                            let (_, val) = list.remove(pos);
                            if list.is_empty() {
                                shard.remove(&h);
                            }
                            return Ok((true, val));
                        }
                    }
                    // k matched __eq__ outside lock, but pointer identity failed after re-locking
                    // because another thread replaced or removed the key object while lock was released.
                    // k is pushed to checked_keys above so it is explicitly marked as evaluated.
                    break;
                }
            }

            // 3. Re-lock shard and return (false, None) if no unchecked candidate remains
            let mut shard = self.shards[idx].lock();
            if let Some(list) = shard.get_mut(&h) {
                let has_unbound_candidate = list.iter().any(|(k, _)| !checked_keys.iter().any(|ck| ck.is(k)));
                if has_unbound_candidate {
                    continue;
                }

                if let Some(pos) = list.iter().position(|(k, _)| k.is(&pykey)) {
                    let (_, val) = list.remove(pos);
                    if list.is_empty() {
                        shard.remove(&h);
                    }
                    return Ok((true, val));
                }
            }

            return Ok((false, py.None()));
        }
    }

    /// Check if the key exists in the map.
    fn contains_key(&self, py: Python<'_>, key: Bound<'_, PyAny>) -> PyResult<bool> {
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

    /// Return the total number of elements across all shards.
    /// Note: Under concurrent modifications, this represents a weakly-consistent
    /// snapshot and may return an approximate value rather than a global atomic freeze.
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
    /// Note: Returns a weakly-consistent snapshot collected shard-by-shard.
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
    /// Note: Returns a weakly-consistent snapshot collected shard-by-shard.
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
    /// Note: Returns a weakly-consistent snapshot collected shard-by-shard.
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
