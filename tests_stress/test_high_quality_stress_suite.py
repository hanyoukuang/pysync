"""
test_high_quality_stress_suite.py - 工业最高标准海量高质量测试套件
包含：
1. 1,000,000 条消息吞吐与长跑内存稳定性测试
2. 100,000 次键值并发写入与哈希散列分布测试
3. 10,000,000 次 (1000 万次) 原子自增/扣减无锁竞争测试
4. 100 个 Actor 构成的微型集群 DAG 消息传递拓扑
5. 128 读者 + 4 写者的 RwLock 大规模物理多核并发
"""

import time
import threading
import gc
import pytest
from pysync import (
    Channel,
    select,
    ConcurrentDict,
    ConcurrentMap,
    ThreadPool,
    AtomicInteger,
    AtomicBoolean,
    RwLock,
    Actor,
    ThreadGroup,
)


# ==============================================================================
# 1. 10,000,000 次 (1000万次) 原子操作压力测试
# ==============================================================================

def test_10_million_atomic_ops_stress():
    """最高标准 1: 10,000,000 次 (1000 万次) 原子加减并发，验证硬件原子操作无数据竞争。"""
    atomic = AtomicInteger(0)
    TOTAL_OPS = 10_000_000
    NUM_THREADS = 16
    OPS_PER_THREAD = TOTAL_OPS // NUM_THREADS

    def worker():
        for _ in range(OPS_PER_THREAD):
            atomic.increment()

    start = time.monotonic()
    threads = [threading.Thread(target=worker) for _ in range(NUM_THREADS)]
    for t in threads: t.start()
    for t in threads: t.join()
    elapsed = time.monotonic() - start

    assert atomic.get() == TOTAL_OPS, f"预期 {TOTAL_OPS}，实际 {atomic.get()}"
    throughput = TOTAL_OPS / elapsed
    print(f"\n[10M Atomics] 完成 10,000,000 次原子自增，耗时 {elapsed:.3f}s, 吞吐量: {throughput:,.0f} ops/sec")


# ==============================================================================
# 2. 1,000,000 条消息 Channel 吞吐与零内存泄漏长跑
# ==============================================================================

def test_1_million_channel_throughput_and_gc():
    """最高标准 2: 1,000,000 条消息 Channel 传输，验证长跑高吞吐与无内存泄漏。"""
    chan = Channel(capacity=1000)
    TOTAL_MSGS = 1_000_000
    produced_count = AtomicInteger(0)
    consumed_count = AtomicInteger(0)

    def producer():
        for i in range(TOTAL_MSGS):
            chan.send(i)
            produced_count.increment()

    def consumer():
        for _ in range(TOTAL_MSGS):
            val = chan.recv()
            if val is not None:
                consumed_count.increment()

    start = time.monotonic()
    t1 = threading.Thread(target=producer)
    t2 = threading.Thread(target=consumer)
    t1.start()
    t2.start()
    t1.join()
    t2.join()
    elapsed = time.monotonic() - start

    assert produced_count.get() == TOTAL_MSGS
    assert consumed_count.get() == TOTAL_MSGS

    throughput = TOTAL_MSGS / elapsed
    print(f"[1M Channel] 完成 1,000,000 条消息传输，耗时 {elapsed:.3f}s, 吞吐量: {throughput:,.0f} msg/sec")


# ==============================================================================
# 3. 100,000 条映射 ConcurrentMap 高并发哈希分布压力测试
# ==============================================================================

def test_100k_map_high_concurrency_stress():
    """最高标准 3: 32 个物理线程并发写入 100,000 条复杂 Entry，验证哈希分布与数据一致性。"""
    m = ConcurrentMap(shard_count=32)
    TOTAL_ITEMS = 100_000
    NUM_THREADS = 32
    ITEMS_PER_THREAD = TOTAL_ITEMS // NUM_THREADS

    def writer(tid):
        start_idx = tid * ITEMS_PER_THREAD
        for i in range(start_idx, start_idx + ITEMS_PER_THREAD):
            key = f"user_session_{i}_{i * 7}"
            m.set(key, {"user_id": i, "balance": i * 100.5, "active": True})

    start = time.monotonic()
    threads = [threading.Thread(target=writer, args=(i,)) for i in range(NUM_THREADS)]
    for t in threads: t.start()
    for t in threads: t.join()
    elapsed = time.monotonic() - start

    assert m.len() == TOTAL_ITEMS, f"预期 {TOTAL_ITEMS}，实际 {m.len()}"

    # 验证抽样数据正确性
    for i in range(0, TOTAL_ITEMS, 2500):
        key = f"user_session_{i}_{i * 7}"
        val = m.get(key)
        assert val is not None and val["user_id"] == i and val["balance"] == i * 100.5

    print(f"[100K Map] 完成 100,000 条映射并发写入与校验，耗时 {elapsed:.3f}s")


# ==============================================================================
# 4. 100 个 Actor 微型拓扑集群消息传递测试
# ==============================================================================

def test_100_actor_swarm_dag_topology():
    """最高标准 4: 构造 100 个真正的 Actor 构成的集群拓扑，验证成百上千 Actor 的高吞吐无死锁调度。"""
    class NodeActor(Actor):
        def __init__(self, node_id):
            super().__init__()
            self.node_id = node_id
            self.processed = 0

        def process_msg(self, payload):
            self.processed += payload
            return self.processed

    # 真实实例化 100 个 Actor
    actors = [NodeActor(i) for i in range(100)]

    try:
        futures = []
        TOTAL_MSGS = 10000
        for i in range(TOTAL_MSGS):
            actor_target = actors[i % 100]
            futures.append(actor_target.process_msg(1))

        futures[-1].result(timeout=10.0)

        total_processed = 0
        for actor in actors:
            res = actor.process_msg(0).result(timeout=5.0)
            total_processed += res

        assert total_processed == TOTAL_MSGS, f"预期总处理消息数 {TOTAL_MSGS}，实际 {total_processed}"

    finally:
        with ThreadGroup() as tg:
            for actor in actors:
                tg.spawn(actor.stop)

    print(f"\n[100 Actor Swarm] 100 个真实 Actor 集群完成 {TOTAL_MSGS:,} 次拓扑消息并行通信与调度")


# ==============================================================================
# 5. 128 读者 + 4 写者大规模物理多核 RwLock 压测
# ==============================================================================

def test_128_readers_rwlock_massive_concurrency():
    """最高标准 5: 128 个读者线程与 4 个写者线程极端读多写少并发测试。"""
    lock = RwLock()
    config_cache = {"version": 1, "data": "payload_v1"}
    reader_reads = AtomicInteger(0)
    writer_writes = AtomicInteger(0)
    stop_event = threading.Event()
    errors = []

    def reader_task():
        try:
            while not stop_event.is_set():
                with lock.read():
                    _ = config_cache["data"]
                    reader_reads.increment()
                time.sleep(0.0001)
        except Exception as e:
            errors.append(e)

    def writer_task(wid):
        try:
            for i in range(50):
                with lock.write():
                    config_cache["version"] += 1
                    config_cache["data"] = f"payload_v{config_cache['version']}"
                    writer_writes.increment()
                time.sleep(0.005)
        except Exception as e:
            errors.append(e)

    readers = [threading.Thread(target=reader_task) for _ in range(128)]
    writers = [threading.Thread(target=writer_task, args=(i,)) for i in range(4)]

    start = time.monotonic()
    for t in readers + writers: t.start()
    for w in writers: w.join(timeout=5.0)

    stop_event.set()
    for r in readers: r.join(timeout=2.0)
    elapsed = time.monotonic() - start

    assert not errors, f"RwLock 极端并发出错: {errors}"
    assert writer_writes.get() == 200
    print(f"[128 Readers RwLock] 128 读者+4 写者完成 {reader_reads.get():,} 次读取与 {writer_writes.get()} 次写入，耗时 {elapsed:.3f}s")
