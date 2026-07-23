# pysync-nogil: 针对 Python 3.14t (Free-Threaded No-GIL) 的现代高性能并发原语库

[English README](https://github.com/hanyoukuang/pysync-nogil/blob/main/README.md)

`pysync-nogil` 是专为 **Python 3.14t 无 GIL (Free-Threaded)** 环境设计的高性能现代并发组件库。

借鉴了 **Go、Rust、Java 与 Akka** 的现代并发设计理念，`pysync-nogil` 结合 Rust (PyO3) 底层原生性能与 Python 人性化的 API，为自由线程 Python 提供**百万级吞吐量、无锁/低锁操作与内置死锁防护**。

> [!WARNING]
> **玩具与实验性 Sandbox 声明**
> 本项目严格定位为**探索 Python 3.14t Free-Threaded (No-GIL) 线程并行极限的实验性玩具库 / 沙盒**。
> **请勿用于生产环境或任何关键项目！**
> CPython Free-Threaded 生态目前仍在快速演进中，未来 Python 并发范式尚无定论。本项目仅作为验证理论与方案的实验实验室。

---

## ⚡ 实测性能汇总 (Python 3.14t Free-Threaded No-GIL)

| 组件 | 对标目标 | 标准库耗时 | `pysync` 耗时 | 性能提升与吞吐结果 |
| :--- | :--- | :--- | :--- | :--- |
| **`pysync.Channel`** | vs `queue.Queue` | 0.2261s | **0.0683s** | **🚀 3.22x ~ 3.72x 提速** (290万+ msg/sec) |
| **`pysync.ConcurrentDict`** | vs `dict` + Lock | 0.2156s | **0.0939s** | **🚀 2.29x 提速** (420万+ ops/sec) |
| **`pysync.AtomicInteger`** | vs Lock Counter | 0.1280s | **0.0979s** | **🚀 1.31x 提速** (810万+ ops/sec) |
| **`pysync.RwLock` (Context)** | vs Standard Mutex | 0.0631s | **0.2647s** | **⚡ 多读并行 (Zero-Mutex 无锁读)** |
| **`Actor.tell()`** | vs `Actor.call()` | 1.4496s | **0.7715s** | **🚀 1.82x 提速** (Fire-and-Forget 单向投递) |

---

## 🗺️ API 设计映射与参考指南

如果您熟悉 Go、Java、Rust 或 Erlang/Akka，可以**零学习成本**直接上手 `pysync-nogil`：

| `pysync` API | 灵感来源 | 等价 API / 概念 |
| :--- | :--- | :--- |
| **`Channel`** | Go `chan` & Rust `crossbeam-channel` | `ch := make(chan T, 10)` / `ch.send()`, `ch.recv()` |
| **`select`** | Go `select` & Rust `crossbeam::select!` | `select { case msg := <-ch1: ... }` |
| **`ConcurrentDict`** | Java `ConcurrentHashMap` & Rust `DashMap` | `new ConcurrentHashMap<K, V>()` |
| **`AtomicInteger`** | Java `AtomicInteger` & Rust `AtomicI64` | `atom.addAndGet(1)` / `atom.compare_and_set(exp, new)` |
| **`RwLock`** | Rust `parking_lot::RwLock` & Java `ReadWriteLock` | `lock.readLock().lock()` / `with lock.read():` |
| **`Actor`** | Erlang / Akka / Ray `Actor` | `class MyActor(Actor)` 隔离状态，支持 `call()` 与 `tell()` |
| **`ThreadGroup`** | Python 3.11 `TaskGroup` & Java `StructuredTaskScope` | `with TaskGroup() as tg: tg.create_task(...)` |

---

## 🚀 核心组件 API 示例

### 1. CSP 消息通道与多路复用 (`Channel` & `select`)
> **灵感来源：Go `chan` + `select` 关键字 / Rust `crossbeam-channel`**

支持有界、无界与零容量 sync 汇合模式。搭配 `select(ops, timeout=...)` 实现 Go 风格的通道多路复用与挂起超时防护：

```python
from pysync import Channel, select

# 有界通道 (Go 语法等价: ch := make(chan string, 10))
ch1 = Channel(capacity=10)
ch2 = Channel(capacity=10)

ch1.send("来自通道 1 的消息")
ch2.send("来自通道 2 的消息")

# 多路复用选择 (Go 语法等价: select { case msg := <-ch1: ... })
ops = [ch1.recv_op(), ch2.recv_op()]
idx, val = select(ops, timeout=2.0)

print(f"从通道 {idx + 1} 收到消息: {val}")
```

---

### 2. 高并发分片字典 (`ConcurrentDict`)
> **灵感来源：Java `java.util.concurrent.ConcurrentHashMap` / Rust `DashMap`**

100% 兼容 Python 标准 `dict` 字典语法。采用 32 分片并发锁机制，无需手动加 `threading.Lock` 即可安全进行无 GIL 并发写：

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
print(f"字典总 Key 数量: {len(d)}")
```

---

### 3. 隔离状态 Actor 模型 (`Actor`)
> **灵感来源：Erlang / Akka / Ray `Actor`**

单线程隔离状态模型，支持配置 Mailbox 有界背压容量（`mailbox_capacity=256`）。提供同步 `call()` (返回 `Future`) 与非阻塞单向 `tell()` (Fire-and-Forget)：

```python
from pysync import Actor

class CounterActor(Actor):
    def __init__(self):
        super().__init__(mailbox_capacity=256)
        self.count = 0  # 隔离私有状态

    def increment(self, amount=1):
        self.count += amount
        return self.count

actor = CounterActor()

# 单向非阻塞 Fire-and-Forget (提速 1.8 倍)
actor.tell("increment", 10)

# 同步调用回传 Future
future = actor.increment(5)
print(f"Actor 当前计数: {future.result()}")  # 输出: 15

actor.stop()
```

---

### 4. 零内存分配读写锁 (`RwLock`)
> **灵感来源：Rust `parking_lot::RwLock` / Java `ReentrantReadWriteLock`**

允许多个读线程同时并行读取，写者持有独占写锁。支持无 GIL 释放快速路径与 TLS 线程级可重入，基于上下文管理器（安全 RAII 规范）：

```python
from pysync import RwLock

lock = RwLock()

# 共享读锁
with lock.read():
    # 共享读逻辑...
    pass

# 独占写锁
with lock.write():
    # 独占写逻辑...
    pass
```

---

### 5. 硬件级无锁原子变量 (`AtomicInteger` / `AtomicBoolean`)
> **灵感来源：Java `java.util.concurrent.atomic.AtomicInteger` / Rust `std::sync::atomic`**

底层基于 CPU CAS 指令构建，实现 **810 万 ops/sec** 极限吞吐，支持显式内存顺序 (`ordering="seq_cst"`, `"relaxed"`)：

```python
from pysync import AtomicInteger, AtomicBoolean

counter = AtomicInteger(0)
flag = AtomicBoolean(False)

# Compare-And-Set (CAS)
if flag.compare_and_set(False, True):
    print("成功抢占原子标志位！")

# Relaxed 内存顺序原子加
counter.fetch_add_relaxed(1)
counter.add_and_get(10, ordering="relaxed")
print(f"最终原子计数: {counter.get()}")
```

---

### 6. 结构化并发 (`ThreadGroup`)
> **灵感来源：Python 3.11 `asyncio.TaskGroup` / Java 21 `StructuredTaskScope`**

利用 Python 的 `with` 上下文管理器，确保作用域结束前自动 Join 所有子线程。遇到异常自动汇总为 `ExceptionGroup`：

```python
from pysync import ThreadGroup
import time

def worker(task_name, delay):
    time.sleep(delay)
    print(f"任务 {task_name} 完成")

with ThreadGroup() as tg:
    tg.spawn(worker, "A", 0.1)
    tg.spawn(worker, "B", 0.2)
# 作用域退出自动等待并 Join 所有生成的 Worker 线程
```

---

## 🛠️ 本地开发与测试

```bash
# 编译 Rust PyO3 扩展
maturin develop --release

# 日常本地开发（快速单元测试）
pytest tests/

# 提交 PR 前必须执行（单元测试 + 全量高压压力测试）
pytest tests/ tests_stress/

# 运行性能基准测试
python tests/test_perf.py
```
