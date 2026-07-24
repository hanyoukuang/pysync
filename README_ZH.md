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

| 组件                          | 对标目标          | 标准库耗时 | `pysync` 耗时 | 性能提升与吞吐结果                          |
| :---------------------------- | :---------------- | :--------- | :------------ | :------------------------------------------ |
| **`pysync.Channel`**          | vs `queue.Queue`  | 0.2261s    | **0.0683s**   | **🚀 3.22x ~ 3.72x 提速** (290万+ msg/sec)   |
| **`pysync.ConcurrentDict`**   | vs `dict` + Lock  | 0.2156s    | **0.0939s**   | **🚀 2.29x 提速** (420万+ ops/sec)           |
| **`pysync.AtomicInteger`**    | vs Lock Counter   | 0.1280s    | **0.0979s**   | **🚀 1.31x 提速** (810万+ ops/sec)           |
| **`pysync.RwLock` (Context)** | vs Standard Mutex | 0.0631s    | **0.2647s**   | **⚡ 多读并行 (Zero-Mutex 无锁读)**          |
| **`Actor.tell()`**            | vs `Actor.call()` | 1.4496s    | **0.7715s**   | **🚀 1.82x 提速** (Fire-and-Forget 单向投递) |

---

## 📦 安装指南与 Python 3.14t (No-GIL) 环境配置

> **💡 为什么是 `3.14t`？**  
> CPython 官方使用 **`t`** 后缀表示 **Threaded (Free-Threaded / No-GIL)** 无全局解释器锁版本（启用 `--disable-gil` 编译），支持真正的多核 CPU 线程并行。

### 1. 快速安装库
```bash
# 通过 pip 或 uv 安装
pip install pysync-nogil
# 或
uv add pysync-nogil
```

### 2. 使用 `uv` 一键配置 Python 3.14t 开发环境
使用 [`uv`](https://github.com/astral-sh/uv) 可以免去手动下载源码编译 CPython 的繁琐流程：

```bash
# 安装 uv (如未安装)
curl -LsSf https://astral.sh/uv/install.sh | sh

# 1. 一键下载安装无 GIL 自由线程 Python 3.14t
uv python install 3.14t

# 2. 克隆项目并创建指向 3.14t 的虚拟环境
git clone https://github.com/hanyoukuang/pysync-nogil.git && cd pysync-nogil
uv venv --python 3.14t
source .venv/bin/activate

# 3. 编译 Rust (PyO3) 原生扩展并运行测试
uv run maturin develop --release
uv run pytest tests/
```

### 3. 验证 Free-Threaded (No-GIL) 运行状态
运行 Python 脚本时显式禁用 GIL：
```bash
python3.14t -Xgil=0 my_script.py
```
在 Python 代码中检测：
```python
import sys
print("GIL 是否启用:", getattr(sys, '_is_gil_enabled', lambda: True)())
# 预期输出: GIL 是否启用: False
```

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

## 🚀 核心组件 API 简单使用示例

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

## 生产环境适用性评估 (Production Readiness)

`pysync-nogil` 经过了 400+ 项严格的工程级测试（包含 100 Actor Swarm 拓扑、80,000 次 Actor 并发消息、1000万次无锁原子自增等海量高压测试，全量测试 100% Pass，Rust 侧 0 Clippy 警告）：

### 适合生产使用的优势亮点
- **高吞吐与无死锁保障**：Actor 采用 `AtomicU8` 状态机无锁并发，配合通道 Drain 拒绝机制，彻底杜绝了 Future 悬挂假死的问题；字典与 Map 采用 16~64 分片读写锁 + 锁外 `__eq__` 比较，避免单锁争用。
- **内存与资源安全**：`ThreadPool` 的 Drop 清理在后台线程异步处理，不会阻塞调用者主线程或 Python GC；所有多线程退出逻辑集成 `Py_IsFinalizing()` 防护，规避 CPython 关闭时的段错误（SIGSEGV）。

### 生产落地注意事项
1. **No-GIL 生态依赖**：运行需使用 CPython 3.14t（Free-threading）；若在 Actor/Worker 内调用第三方原生 C 扩展，请确保该扩展已适配 No-GIL 并保证线程安全。
2. **容量限制**：生产环境下建议为 Actor 显式指定 `mailbox_capacity`（如 1000~10000），防止无界消息队列堆积内存。

