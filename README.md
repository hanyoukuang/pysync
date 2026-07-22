# pysync: Modern High-Performance Concurrency Primitives for Python 3.14+ (Free-Threaded)

[中文版](README_ZH.md)

`pysync` is a high-performance modern concurrency library designed specifically for **Python 3.14 free-threaded (GIL-free)** environments.

Drawing design inspiration from **Go, Rust, Java, and Akka**, `pysync` combines native Rust performance (via PyO3) with ergonomic Python APIs to deliver **million-level throughput, lock-free operations, and built-in deadlock prevention**.

---

## 🗺️ API Mapping & Reference Guide

If you are familiar with Go, Java, Rust, or Erlang/Akka, you can use `pysync` with **zero learning curve**:

| `pysync` API | Inspired by Famous Library / Language | Equivalent API / Concept |
| :--- | :--- | :--- |
| **`Channel`** | Go `chan` & Rust `crossbeam-channel` | `ch := make(chan T, 10)` / `ch.send()`, `ch.recv()` |
| **`select`** | Go `select` & Rust `crossbeam::select!` | `select { case msg := <-ch1: ... }` |
| **`ConcurrentDict`** | Java `ConcurrentHashMap` & Rust `DashMap` | `new ConcurrentHashMap<K, V>()` |
| **`AtomicInteger`** | Java `AtomicInteger` & Rust `AtomicI64` | `atom.addAndGet(1)` / `atom.compareAndSet(exp, new)` |
| **`RwLock`** | Rust `parking_lot::RwLock` & Java `ReadWriteLock` | `lock.readLock().lock()` / `with lock.read():` |
| **`Actor`** | Erlang / Akka / Ray `Actor` | `class MyActor(Actor)` isolated state & message passing |
| **`ThreadGroup`** | Python 3.11 `TaskGroup` & Java `StructuredTaskScope` | `with TaskGroup() as tg: tg.create_task(...)` |

---

## 🚀 Component API Comparison & Code Examples

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
# Optional timeout prevents infinite hangs if channels run dry!
ops = [ch1.recv_op(), ch2.recv_op()]
idx, val = select(ops, timeout=2.0)

print(f"Received from Channel {idx + 1}: {val}")
```

---

### 2. High-Concurrency Sharded Map (`ConcurrentDict`)
> **Inspired by: Java `java.util.concurrent.ConcurrentHashMap` / Rust `DashMap`**

100% compliant with standard Python `dict` syntax. Uses 64-shard concurrent locks for thread-safe GIL-free mutation without manual `threading.Lock`:

```python
from pysync import ConcurrentDict
import threading

# Java equivalent: ConcurrentHashMap<String, Integer> map = new ConcurrentHashMap<>();
d = ConcurrentDict()

# 8 threads mutating keys concurrently without manual locks
def worker(tid):
    for i in range(1, 1000):
        d[f"worker_{tid}_{i}"] = i

threads = [threading.Thread(target=worker, args=(t,)) for t in range(8)]
for t in threads: t.start()
for t in threads: t.join()

# Atomic operations (Java equivalent: map.putIfAbsent / computeIfAbsent)
val = d.setdefault("consensus_key", 42)
print(f"Total keys count: {len(d)}")
```

---

### 3. Isolated State Actor Model (`Actor`)
> **Inspired by: Erlang / Akka / Ray `Actor`**

Single-threaded isolated state model. External direct attribute access is blocked (raises `AttributeError`); all method invocations are queued onto an isolated mailbox thread:

```python
from pysync import Actor

# Ray equivalent: @ray.remote class CounterActor
class CounterActor(Actor):
    def __init__(self):
        super().__init__()
        self.count = 0  # Isolated private state

    def increment(self, amount=1):
        self.count += amount
        return self.count

    def get_count(self):
        return self.count

actor = CounterActor()

# Thread-safe async message passing & method calls
actor.increment(10)
actor.increment(5)
print(f"Actor Current Count: {actor.get_count()}")  # Output: 15

actor.stop()
```

---

### 4. Zero-Allocation Reader-Writer Lock (`RwLock`)
> **Inspired by: Rust `parking_lot::RwLock` / Java `ReentrantReadWriteLock`**

Allows multiple concurrent readers while writers hold exclusive access. Zero-allocation lock methods deliver ~300% higher read throughput:

```python
from pysync import RwLock

lock = RwLock()

# Method A: Zero-allocation direct lock methods (Maximum C/Rust native speed)
lock.acquire_read()
try:
    # Shared read logic...
    pass
finally:
    lock.release_read()

# Method B: Pythonic context manager API
with lock.read():
    pass

with lock.write():
    pass
```

---

### 5. Hardware-Level Lock-Free Atomics (`AtomicInteger` / `AtomicBoolean`)
> **Inspired by: Java `java.util.concurrent.atomic.AtomicInteger` / Rust `std::sync::atomic`**

Lock-free atomic variables leveraging CPU bus locks and CAS instructions for **3.3+ Million ops/sec**:

```python
from pysync import AtomicInteger, AtomicBoolean
import threading

# Java equivalent: AtomicInteger atomicInt = new AtomicInteger(0);
counter = AtomicInteger(0)
flag = AtomicBoolean(False)

# Compare-And-Set (CAS)
if flag.compare_and_set(False, True):
    print("Successfully acquired atomic flag!")

# Atomic addition (Java equivalent: atomicInt.addAndGet(1))
def worker():
    for _ in range(10000):
        counter.add_and_get(1)

threads = [threading.Thread(target=worker) for _ in range(10)]
for t in threads: t.start()
for t in threads: t.join()

print(f"Final Atomic Count: {counter.get()}")  # Output: 100000
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

# Python 3.11 equivalent: async with asyncio.TaskGroup() as tg:
with ThreadGroup() as tg:
    tg.spawn(worker, "A", 0.1)
    tg.spawn(worker, "B", 0.2)
# Block exit automatically waits for and joins all spawned worker threads
```

---

## 🛠️ Contributing & Local Setup (3-Step Quickstart)

### 1. Install Base Toolchain (Rust + uv)
```bash
# Install Rust compiler
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Install uv Python package manager
curl -LsSf https://astral.sh/uv/install.sh | sh
```

### 2. Set Up Python 3.14 (Free-Threaded / GIL-Free) Environment
```bash
git clone https://github.com/your-username/pysync.git
cd pysync

# Download Python 3.14t (free-threaded) and create a virtual environment
uv python install 3.14t
uv venv --python 3.14t
source .venv/bin/activate  # Windows users: .venv\Scripts\activate

# Install maturin build tool and pytest
uv pip install maturin pytest
```

### 3. Build & Test Locally
```bash
# Compile Rust PyO3 bindings locally (run after modifying src/*.rs)
maturin develop

# Run unit tests
pytest tests/
```

---

## 🧪 Testing & Benchmarks

```bash
# Run unit tests (364 tests, ~7 seconds)
pytest tests/

# Run 10-Million ops stress & chaos test suite
pytest tests_stress/
```

Empirical benchmarks measured on Apple Silicon (Python 3.14t No-GIL):

* **`Channel` Message Throughput**: **`1,049,000 msgs/sec`**
* **`AtomicInteger` Counter**: **`3,301,000 ops/sec`**
* **`RwLock` Reader-Writer Lock**: **`300% faster`** than standard mutexes (reduced from `1.369s` to `0.459s`)
