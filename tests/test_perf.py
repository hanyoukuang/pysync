import time
import threading
import queue
from concurrent.futures import ThreadPoolExecutor
from pysync import Channel, ConcurrentDict, ThreadPool

# 1. Benchmark Channel vs queue.Queue
def benchmark_channels(msg_count=200000):
    print(f"\n--- 1. Benchmark: Channel vs queue.Queue (Messages: {msg_count}) ---")
    
    # Standard queue.Queue
    q = queue.Queue()
    start = time.time()
    def q_producer():
        for i in range(msg_count):
            q.put(i)
    def q_consumer():
        for _ in range(msg_count):
            q.get()
            
    t1 = threading.Thread(target=q_producer)
    t2 = threading.Thread(target=q_consumer)
    t1.start(); t2.start()
    t1.join(); t2.join()
    q_time = time.time() - start
    print(f"queue.Queue Time: {q_time:.4f} seconds ({msg_count / q_time:.2f} msg/sec)")

    # pysync.Channel
    chan = Channel()
    start = time.time()
    def chan_producer():
        for i in range(msg_count):
            chan.send(i)
    def chan_consumer():
        for _ in range(msg_count):
            chan.recv()
            
    t1 = threading.Thread(target=chan_producer)
    t2 = threading.Thread(target=chan_consumer)
    t1.start(); t2.start()
    t1.join(); t2.join()
    chan_time = time.time() - start
    print(f"pysync.Channel Time: {chan_time:.4f} seconds ({msg_count / chan_time:.2f} msg/sec)")
    print(f"Speedup: {q_time / chan_time:.2f}x")

# 2. Benchmark ConcurrentDict vs Standard dict under Write Contention
def benchmark_dicts(write_ops=100000, num_threads=4):
    print(f"\n--- 2. Benchmark: ConcurrentDict vs dict under Write Contention (Threads: {num_threads}, Ops: {write_ops}) ---")
    
    # Standard Dict
    d_std = {}
    d_lock = threading.Lock()
    start = time.time()
    def std_writer(tid):
        for i in range(write_ops):
            # CPython free-threaded dictionary requires locks for safe multi-threaded write mutations
            with d_lock:
                d_std[f"thread_{tid}_key_{i}"] = i
                
    threads = [threading.Thread(target=std_writer, args=(t,)) for t in range(num_threads)]
    for t in threads: t.start()
    for t in threads: t.join()
    std_time = time.time() - start
    print(f"Standard Dict with Lock Time: {std_time:.4f} seconds ({write_ops * num_threads / std_time:.2f} ops/sec)")

    # pysync.ConcurrentDict
    d_con = ConcurrentDict()
    start = time.time()
    def con_writer(tid):
        for i in range(write_ops):
            d_con[f"thread_{tid}_key_{i}"] = i
            
    threads = [threading.Thread(target=con_writer, args=(t,)) for t in range(num_threads)]
    for t in threads: t.start()
    for t in threads: t.join()
    con_time = time.time() - start
    print(f"pysync.ConcurrentDict Time: {con_time:.4f} seconds ({write_ops * num_threads / con_time:.2f} ops/sec)")
    print(f"Speedup: {std_time / con_time:.2f}x")

# 3. Benchmark ThreadPool vs ThreadPoolExecutor (CPU-bound load)
def cpu_heavy_work(n):
    # Standard fibonacci calculation to consume CPU cycles
    a, b = 0, 1
    for _ in range(n):
        a, b = b, a + b
    return a

def benchmark_threadpools(tasks_count=100, workers=4):
    print(f"\n--- 3. Benchmark: ThreadPool vs ThreadPoolExecutor (Workers: {workers}, Tasks: {tasks_count}) ---")
    n = 150000 # Fibonacci iteration count
    
    # standard ThreadPoolExecutor
    start = time.time()
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(cpu_heavy_work, n) for _ in range(tasks_count)]
        results = [f.result() for f in futures]
    std_time = time.time() - start
    print(f"ThreadPoolExecutor Time: {std_time:.4f} seconds")

    # pysync.ThreadPool
    start = time.time()
    pool = ThreadPool(num_workers=workers)
    try:
        futures = [pool.submit(cpu_heavy_work, n) for _ in range(tasks_count)]
        results = [f.result() for f in futures]
    finally:
        pool.shutdown()
    con_time = time.time() - start
    print(f"pysync.ThreadPool Time: {con_time:.4f} seconds")
    print(f"Speedup: {std_time / con_time:.2f}x")

# 4. Benchmark AtomicInteger vs Lock-Based Counter
def benchmark_atomics(ops=200000, num_threads=4):
    print(f"\n--- 4. Benchmark: AtomicInteger vs Lock-Based Counter (Threads: {num_threads}, Ops per thread: {ops}) ---")
    
    # Locked Counter
    class LockedCounter:
        def __init__(self):
            self.value = 0
            self.lock = threading.Lock()
        def increment(self):
            with self.lock:
                self.value += 1
                
    counter = LockedCounter()
    start = time.time()
    def locked_worker():
        for _ in range(ops):
            counter.increment()
            
    threads = [threading.Thread(target=locked_worker) for _ in range(num_threads)]
    for t in threads: t.start()
    for t in threads: t.join()
    locked_time = time.time() - start
    print(f"Lock-Based Counter Time: {locked_time:.4f} seconds ({ops * num_threads / locked_time:.2f} ops/sec)")

    # AtomicInteger
    from pysync import AtomicInteger
    atomic = AtomicInteger(0)
    start = time.time()
    def atomic_worker():
        for _ in range(ops):
            atomic.increment()
            
    threads = [threading.Thread(target=atomic_worker) for _ in range(num_threads)]
    for t in threads: t.start()
    for t in threads: t.join()
    atomic_time = time.time() - start
    print(f"AtomicInteger Time: {atomic_time:.4f} seconds ({ops * num_threads / atomic_time:.2f} ops/sec)")
    print(f"Speedup: {locked_time / atomic_time:.2f}x")

# 5. Benchmark RwLock vs standard Lock under Read-Heavy Contention
def benchmark_rwlock(readers_count=8, writers_count=2, ops=50000):
    print(f"\n--- 5. Benchmark: RwLock vs standard Lock (Readers: {readers_count}, Writers: {writers_count}) ---")
    
    # Standard Lock
    std_lock = threading.Lock()
    shared_state = [0]
    
    start = time.time()
    def std_reader():
        for _ in range(ops):
            with std_lock:
                val = shared_state[0]
                
    def std_writer():
        for _ in range(ops // 10):
            with std_lock:
                shared_state[0] += 1
                
    threads = []
    for _ in range(readers_count):
        threads.append(threading.Thread(target=std_reader))
    for _ in range(writers_count):
        threads.append(threading.Thread(target=std_writer))
        
    for t in threads: t.start()
    for t in threads: t.join()
    std_time = time.time() - start
    print(f"Standard Lock Time: {std_time:.4f} seconds")

    # RwLock
    from pysync import RwLock
    rwlock = RwLock()
    shared_state = [0]
    
    start = time.time()
    def rw_reader():
        for _ in range(ops):
            with rwlock.read():
                val = shared_state[0]
                
    def rw_writer():
        for _ in range(ops // 10):
            with rwlock.write():
                shared_state[0] += 1
                
    threads = []
    for _ in range(readers_count):
        threads.append(threading.Thread(target=rw_reader))
    for _ in range(writers_count):
        threads.append(threading.Thread(target=rw_writer))
        
    for t in threads: t.start()
    for t in threads: t.join()
    rw_time = time.time() - start
    print(f"pysync.RwLock (Context Guard) Time: {rw_time:.4f} seconds")

    # RwLock (Direct Zero-Allocation API)
    rwlock_direct = RwLock()
    shared_state = [0]
    start = time.time()
    def rw_direct_reader():
        for _ in range(ops):
            rwlock_direct.acquire_read()
            val = shared_state[0]
            rwlock_direct.release_read()

    def rw_direct_writer():
        for _ in range(ops // 10):
            rwlock_direct.acquire_write()
            shared_state[0] += 1
            rwlock_direct.release_write()

    threads = []
    for _ in range(readers_count):
        threads.append(threading.Thread(target=rw_direct_reader))
    for _ in range(writers_count):
        threads.append(threading.Thread(target=rw_direct_writer))

    for t in threads: t.start()
    for t in threads: t.join()
    direct_time = time.time() - start
    print(f"pysync.RwLock (Direct Zero-Alloc) Time: {direct_time:.4f} seconds ({std_time / direct_time:.2f}x vs std_lock)")

# 6. Benchmark Actor vs Lock-Based Counter
def benchmark_actor(ops=50000, num_threads=4):
    print(f"\n--- 6. Benchmark: Actor vs Lock-Based Counter (Threads: {num_threads}, Ops per thread: {ops}) ---")
    
    # Locked Counter
    class LockedCounter:
        def __init__(self):
            self.value = 0
            self.lock = threading.Lock()
        def increment(self):
            with self.lock:
                self.value += 1
                
    counter = LockedCounter()
    start = time.time()
    def locked_worker():
        for _ in range(ops):
            counter.increment()
            
    threads = [threading.Thread(target=locked_worker) for _ in range(num_threads)]
    for t in threads: t.start()
    for t in threads: t.join()
    locked_time = time.time() - start
    print(f"Lock-Based Counter Time: {locked_time:.4f} seconds ({ops * num_threads / locked_time:.2f} ops/sec)")

    # Actor
    from pysync import Actor
    class CounterActor(Actor):
        def __init__(self):
            super().__init__()
            self.value = 0
        def increment(self):
            self.value += 1
            
    actor = CounterActor()
    start = time.time()
    def actor_worker():
        futures = []
        for _ in range(ops):
            futures.append(actor.increment())
        # Block on the last future to wait for completion
        futures[-1].result()
            
    threads = [threading.Thread(target=actor_worker) for _ in range(num_threads)]
    for t in threads: t.start()
    for t in threads: t.join()
    actor_time = time.time() - start
    actor.stop()
    print(f"Actor Time: {actor_time:.4f} seconds ({ops * num_threads / actor_time:.2f} ops/sec)")
    print(f"Speedup: {locked_time / actor_time:.2f}x")

if __name__ == "__main__":
    benchmark_channels()
    benchmark_dicts()
    benchmark_threadpools()
    benchmark_atomics()
    benchmark_rwlock()
    benchmark_actor()
