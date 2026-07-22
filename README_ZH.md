# pysync: 专为 Python 3.14+ (No-GIL) 设计的高性能现代并发原语库

[English Version](README.md)

`pysync` 是一个专为 **Python 3.14 自由线程（无 GIL，free-threaded）** 环境打造的高性能并发原语库。

设计上吸取了 **Go、Rust、Java 及 Akka** 等现代语言与经典并发库的 API 精髓，底层基于原生 Rust (PyO3) 构建，在无 GIL 的 Python 3.14 运行时下提供**百万级吞吐、极速无锁、防死锁保护**的并发体验。

---

## 🗺️ API 设计参考与对标

如果你熟悉 Go、Java、Rust 或 Erlang/Akka，你可以**零学习成本**上手 `pysync`：

| `pysync` API | 参考与对标经典库 | 经典语言/库的对应用法 |
| :--- | :--- | :--- |
| **`Channel`** | Go `chan` & Rust `crossbeam-channel` | `ch := make(chan T, 10)` / `ch.send()`, `ch.recv()` |
| **`select`** | Go `select` & Rust `crossbeam::select!` | `select { case msg := <-ch1: ... }` |
| **`ConcurrentDict`** | Java `ConcurrentHashMap` & Rust `DashMap` | `new ConcurrentHashMap<K, V>()` |
| **`AtomicInteger`** | Java `AtomicInteger` & Rust `AtomicI64` | `atom.addAndGet(1)` / `atom.compareAndSet(exp, new)` |
| **`RwLock`** | Rust `parking_lot::RwLock` & Java `ReadWriteLock` | `lock.readLock().lock()` / `with lock.read():` |
| **`Actor`** | Erlang / Akka / Ray `Actor` | `class MyActor(Actor)` 隔离状态与消息传递 |
| **`ThreadGroup`** | Python 3.11 `TaskGroup` & Java `StructuredTaskScope` | `with TaskGroup() as tg: tg.create_task(...)` |

---

## 🚀 各组件 API 详细对照与使用示例

### 1. CSP 消息通道与多路复用 (`Channel` & `select`)
> **参考来源：Go 语言 `chan` + `select` 关键字 / Rust `crossbeam-channel`**

支持有缓冲、无缓冲（会合）与无界传输，结合 `select(ops, timeout=...)` 实现超时防死锁的多路复用：

```python
from pysync import Channel, select

# 创建带缓冲通道 (对标 Go: ch := make(chan string, 10))
ch1 = Channel(capacity=10)
ch2 = Channel(capacity=10)

ch1.send("来自通道 1 的消息")
ch2.send("来自通道 2 的消息")

# 多路复用选择 (对标 Go: select { case msg := <-ch1: ... })
# 支持可选的 timeout 参数，防止通道跑空时永久死锁挂起！
ops = [ch1.recv_op(), ch2.recv_op()]
idx, val = select(ops, timeout=2.0)

print(f"从通道 {idx + 1} 接收到数据: {val}")
```

---

### 2. 高并发分片无锁字典 (`ConcurrentDict`)
> **参考来源：Java `java.util.concurrent.ConcurrentHashMap` / Rust `DashMap`**

100% 兼容 Python 标准 `dict` 协议，底层采用 64 分片无锁锁段，在 No-GIL 环境下无需手动 `threading.Lock` 即可安全读写：

```python
from pysync import ConcurrentDict
import threading

# 对标 Java: ConcurrentHashMap<String, Integer> map = new ConcurrentHashMap<>();
d = ConcurrentDict()

# 8 个线程并发写入不同 Key，无需手动加锁
def worker(tid):
    for i in range(1, 1000):
        d[f"worker_{tid}_{i}"] = i

threads = [threading.Thread(target=worker, args=(t,)) for t in range(8)]
for t in threads: t.start()
for t in threads: t.join()

# 包含原子操作 (对标 Java: map.putIfAbsent / computeIfAbsent)
val = d.setdefault("consensus_key", 42)
print(f"当前字典长度: {len(d)}")
```

---

### 3. Actor 隔离状态模型 (`Actor`)
> **参考来源：Erlang / Akka / Ray `Actor`**

将状态与单线程绑定的 Actor 模型。外部无法直接读写 Actor 内部属性（抛出 `AttributeError`），所有方法调用自动转化为独立 Mailbox 线程中的消息排队执行：

```python
from pysync import Actor

# 对标 Ray: @ray.remote class CounterActor
class CounterActor(Actor):
    def __init__(self):
        super().__init__()
        self.count = 0  # 内部隔离私有状态

    def increment(self, amount=1):
        self.count += amount
        return self.count

    def get_count(self):
        return self.count

actor = CounterActor()

# 线程安全的消息投递与方法调用
actor.increment(10)
actor.increment(5)
print(f"Actor 当前计数: {actor.get_count()}")  # 输出: 15

actor.stop()
```

---

### 4. 零内存分配读写锁 (`RwLock`)
> **参考来源：Rust `parking_lot::RwLock` / Java `ReentrantReadWriteLock`**

允许多个并发读线程同时进入，写线程独占。提供**零分配直接加锁 API**，相比传统 `Lock` 提升近 300% 吞吐：

```python
from pysync import RwLock

lock = RwLock()

# 方式 A：零内存分配直加锁 API (极致性能，对标 C/Rust 原生锁)
lock.acquire_read()
try:
    # 执行共享只读逻辑...
    pass
finally:
    lock.release_read()

# 方式 B：上下文管理器 API (对标 Pythonic 风格)
with lock.read():
    pass

with lock.write():
    pass
```

---

### 5. 硬件级无锁原子变量 (`AtomicInteger` / `AtomicBoolean`)
> **参考来源：Java `java.util.concurrent.atomic.AtomicInteger` / Rust `std::sync::atomic`**

基于 CPU 锁总线/CAS 指令的硬件级无锁变量，提供 **330 万次/秒** 的强一致性加减与比较交换：

```python
from pysync import AtomicInteger, AtomicBoolean
import threading

# 对标 Java: AtomicInteger atomicInt = new AtomicInteger(0);
counter = AtomicInteger(0)
flag = AtomicBoolean(False)

# 原子比较并交换 (Compare-And-Set / CAS)
if flag.compare_and_set(False, True):
    print("成功抢占标记位！")

# 多线程原子自增 (对标 Java: atomicInt.addAndGet(1))
def worker():
    for _ in range(10000):
        counter.add_and_get(1)

threads = [threading.Thread(target=worker) for _ in range(10)]
for t in threads: t.start()
for t in threads: t.join()

print(f"最终原子计数结果: {counter.get()}")  # 输出: 100000
```

---

### 6. 结构化并发线程组 (`ThreadGroup`)
> **参考来源：Python 3.11 `asyncio.TaskGroup` / Java 21 `StructuredTaskScope`**

利用 Python `with` 上下文管理保证所有衍生子线程在离开作用域前必须汇合 (Join)。若子线程抛出异常，自动聚合为 Python 3.11+ 的 `ExceptionGroup`：

```python
from pysync import ThreadGroup
import time

def worker(task_name, delay):
    time.sleep(delay)
    print(f"任务 {task_name} 完成")

# 对标 Python 3.11: async with asyncio.TaskGroup() as tg:
with ThreadGroup() as tg:
    tg.spawn(worker, "A", 0.1)
    tg.spawn(worker, "B", 0.2)
# 离开 with 作用域时，自动阻塞并安全 join 所有衍生子线程
```

---

## 🛠️ 开发者参与与环境配置 (3 步上手)

### 1. 安装基础工具链 (Rust + uv)
```bash
# 1. 安装 Rust 编译器
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# 2. 安装 Python 高速管理工具 uv
curl -LsSf https://astral.sh/uv/install.sh | sh
```

### 2. 一键配置 Python 3.14 (无 GIL) 环境
```bash
git clone https://github.com/your-username/pysync.git
cd pysync

# 安装 Python 3.14 自由线程版 (No-GIL) 并创建虚拟环境
uv python install 3.14t
uv venv --python 3.14t
source .venv/bin/activate  # Windows 用户运行: .venv\Scripts\activate

# 安装 PyO3 构建工具 maturin 与测试框架 pytest
uv pip install maturin pytest
```

### 3. 本地编译与测试
```bash
# 编译 Rust C 扩展（修改 src/*.rs 后运行此命令）
maturin develop

# 运行单元测试
pytest tests/
```

---

## 🧪 测试与性能实测

```bash
# 从 PyPI 安装
pip install pysync-nogil

# 运行单元测试套件 (364 项，~7 秒完成)
pytest tests/

# 运行 1,000 万级极限压力与死亡混沌测试
pytest tests_stress/
```

在 Apple Silicon (Python 3.14t No-GIL) 上的吞吐量实测数据：

* **`Channel` 消息通道**：**`104.9 万条消息/秒`**
* **`AtomicInteger` 原子计数**：**`330.1 万次操作/秒`**
* **`RwLock` 读写锁**：相比传统互斥锁性能提升近 **`300%`** (耗时从 `1.369s` 降至 `0.459s`)
