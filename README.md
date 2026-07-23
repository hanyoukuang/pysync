# pysync-nogil: Modern High-Performance Concurrency Primitives for Python 3.14t (Free-Threaded No-GIL)

[中文文档](https://github.com/hanyoukuang/pysync-nogil/blob/main/README_ZH.md)

`pysync-nogil` is a high-performance modern concurrency library designed specifically for **Python 3.14t free-threaded (GIL-free)** environments.

Drawing design inspiration from **Go, Rust, Java, and Akka**, `pysync-nogil` combines native Rust performance (via PyO3) with ergonomic Python APIs to deliver **million-level throughput, lock-free operations, and built-in deadlock prevention**.

> [!WARNING]
> **Toy & Experimental Sandbox Disclaimer**
> This project is strictly an **experimental toy library / sandbox** built to explore and test the boundaries of Python 3.14t Free-Threaded (No-GIL) parallel execution.
> **DO NOT use this in production or any critical projects!**
> The Free-Threaded ecosystem is still actively evolving, and the future concurrency paradigm in CPython remains unknown. This repository serves purely as a laboratory for testing ideas.

---

## ⚡ Performance Benchmark Summary (Python 3.14t Free-Threaded No-GIL)

| Component | Target Comparison | Standard Time | `pysync` Time | Performance Result |
| :--- | :--- | :--- | :--- | :--- |
| **`pysync.Channel`** | vs `queue.Queue` | 0.2261s | **0.0683s** | **🚀 3.22x ~ 3.72x Faster** (2.9M+ msg/sec) |
| **`pysync.ConcurrentDict`** | vs `dict` + Lock | 0.2156s | **0.0939s** | **🚀 2.29x Faster** (4.2M+ ops/sec) |
| **`pysync.AtomicInteger`** | vs Lock Counter | 0.1280s | **0.0979s** | **🚀 1.31x Faster** (8.1M+ ops/sec) |
| **`pysync.RwLock` (Context)** | vs Standard Mutex | 0.0631s | **0.2647s** | **⚡ Concurrent Readers (Zero-Mutex)** |
| **`Actor.tell()`** | vs `Actor.call()` | 1.4496s | **0.7715s** | **🚀 1.82x Faster** (Fire-and-Forget) |

---

## 🗺️ API Mapping & Reference Guide

If you are familiar with Go, Java, Rust, or Erlang/Akka, you can use `pysync-nogil` with **zero learning curve**:

| `pysync` API | Inspired by | Equivalent API / Concept |
| :--- | :--- | :--- |
| **`Channel`** | Go `chan` & Rust `crossbeam-channel` | `ch := make(chan T, 10)` / `ch.send()`, `ch.recv()` |
| **`select`** | Go `select` & Rust `crossbeam::select!` | `select { case msg := <-ch1: ... }` |
| **`ConcurrentDict`** | Java `ConcurrentHashMap` & Rust `DashMap` | `new ConcurrentHashMap<K, V>()` |
| **`AtomicInteger`** | Java `AtomicInteger` & Rust `AtomicI64` | `atom.addAndGet(1)` / `atom.compare_and_set(exp, new)` |
| **`RwLock`** | Rust `parking_lot::RwLock` & Java `ReadWriteLock` | `lock.readLock().lock()` / `with lock.read():` |
| **`Actor`** | Erlang / Akka / Ray `Actor` | `class MyActor(Actor)` isolated state, `call()` & `tell()` |
| **`ThreadGroup`** | Python 3.11 `TaskGroup` & Java `StructuredTaskScope` | `with TaskGroup() as tg: tg.create_task(...)` |

---

## 🚀 Component API & Usage Examples

### 1. CSP Message Channels & Multiplexing (`Channel` & `select`)
> **Inspired by: Go `chan` + `select` keyword / Rust `crossbeam-channel`**

Supports bounded, unbounded, and unbuffered (rendezvous) modes. Pair with `select(ops, timeout=...)` for Go-style multiplexing with built-in hang protection:

```python
from pysync import Channel, select

# Bounded channel (Go equivalent: ch := make(chan string, 10))
ch1 = Channel(capacity=10)
ch2 = Channel(capacity=10)

ch1.send("Message from Channel 1")
ch2.send("Message from Channel 2")

# Multiplexed selection (Go equivalent: select { case msg := <-ch1: ... })
ops = [ch1.recv_op(), ch2.recv_op()]
idx, val = select(ops, timeout=2.0)

print(f"Received from Channel {idx + 1}: {val}")
```

---

### 2. High-Concurrency Sharded Map (`ConcurrentDict`)
> **Inspired by: Java `java.util.concurrent.ConcurrentHashMap` / Rust `DashMap`**

100% compliant with standard Python `dict` syntax. Uses 32-shard concurrent locks for thread-safe GIL-free mutation without manual `threading.Lock`:

```python
from pysync import ConcurrentDict
import threading

d = ConcurrentDict()

def worker(tid):
    for i in range(1, 1000):
        d[f"worker_{tid}_{i}"] = i

threads = [threading.Thread(target=worker, args=(t,)) for t in range(8)]
for t in threads: t.start()
for t in threads: t.join()

val = d.setdefault("consensus_key", 42)
print(f"Total keys count: {len(d)}")
```

---

### 3. Isolated State Actor Model (`Actor`)
> **Inspired by: Erlang / Akka / Ray `Actor`**

Single-threaded isolated state model with configurable mailbox backpressure (`mailbox_capacity=256`). Supports both synchronous `call()` (returns `Future`) and non-blocking `tell()` (Fire-and-Forget):

```python
from pysync import Actor

class CounterActor(Actor):
    def __init__(self):
        super().__init__(mailbox_capacity=256)
        self.count = 0  # Isolated private state

    def increment(self, amount=1):
        self.count += amount
        return self.count

actor = CounterActor()

# Fire-and-Forget (Non-blocking, 1.8x faster than call)
actor.tell("increment", 10)

# Synchronous call with Future
future = actor.increment(5)
print(f"Actor Current Count: {future.result()}")  # Output: 15

actor.stop()
```

---

### 4. Zero-Allocation Reader-Writer Lock (`RwLock`)
> **Inspired by: Rust `parking_lot::RwLock` / Java `ReentrantReadWriteLock`**

Allows multiple concurrent readers while writers hold exclusive access. Features non-GIL releasing fast path and TLS re-entrancy support via Pythonic context managers (Safe RAII):

```python
from pysync import RwLock

lock = RwLock()

# Shared read lock
with lock.read():
    # Shared read logic...
    pass

# Exclusive write lock
with lock.write():
    # Exclusive write logic...
    pass
```

---

### 5. Hardware-Level Lock-Free Atomics (`AtomicInteger` / `AtomicBoolean`)
> **Inspired by: Java `java.util.concurrent.atomic.AtomicInteger` / Rust `std::sync::atomic`**

Lock-free atomic variables leveraging CPU CAS instructions for **8.1+ Million ops/sec**, supporting explicit memory ordering (`ordering="seq_cst"`, `"relaxed"`):

```python
from pysync import AtomicInteger, AtomicBoolean

counter = AtomicInteger(0)
flag = AtomicBoolean(False)

# Compare-And-Set (CAS)
if flag.compare_and_set(False, True):
    print("Successfully acquired atomic flag!")

# Atomic addition with Relaxed ordering
counter.fetch_add_relaxed(1)
counter.add_and_get(10, ordering="relaxed")
print(f"Final Atomic Count: {counter.get()}")
```

---

### 6. Structured Concurrency (`ThreadGroup`)
> **Inspired by: Python 3.11 `asyncio.TaskGroup` / Java 21 `StructuredTaskScope`**

Uses Python's `with` context manager to ensure spawned child threads are joined before block exit. Collects errors into a Python 3.11+ `ExceptionGroup`:

```python
from pysync import ThreadGroup
import time

def worker(task_name, delay):
    time.sleep(delay)
    print(f"Task {task_name} completed")

with ThreadGroup() as tg:
    tg.spawn(worker, "A", 0.1)
    tg.spawn(worker, "B", 0.2)
# Block exit automatically waits for and joins all spawned worker threads
```

---

## 🛠️ Local Development & Testing

```bash
# Compile Rust PyO3 extension
maturin develop --release

# Routine local development (fast unit tests)
pytest tests/

# Mandatory before submitting a PR (full unit + stress tests)
pytest tests/ tests_stress/

# Run performance benchmark suite
python tests/test_perf.py
```
