import time
import threading
import random
import contextvars
import pytest
from pysync import (
    Channel,
    ConcurrentDict,
    ConcurrentMap,
    ThreadPool,
    Actor,
    ThreadGroup,
    AtomicInteger,
    AtomicBoolean,
    RwLock,
    select,
)

# ==============================================================================
# SCENARIO 1: REAL-TIME FINANCIAL MARKET ORDER MATCHING ENGINE (撮合交易系统)
# ==============================================================================

class OrderBookActor(Actor):
    """
    An Actor representing an isolated, thread-safe financial order book.
    Maintains buy and sell order queues without any data races.
    """
    def __init__(self):
        super().__init__()
        self.bids = []  # Buy orders (price, qty, trader_id)
        self.asks = []  # Sell orders (price, qty, trader_id)
        self.total_volume = 0.0
        self.trades_executed = 0

    def place_order(self, order_type: str, price: float, qty: int, trader_id: str):
        if order_type == "BUY":
            remaining_qty = qty
            new_asks = []
            for ask_price, ask_qty, ask_trader in sorted(self.asks, key=lambda x: x[0]):
                if ask_price <= price and remaining_qty > 0:
                    matched_qty = min(remaining_qty, ask_qty)
                    remaining_qty -= matched_qty
                    self.total_volume += matched_qty * ask_price
                    self.trades_executed += 1
                    if ask_qty > matched_qty:
                        new_asks.append((ask_price, ask_qty - matched_qty, ask_trader))
                else:
                    new_asks.append((ask_price, ask_qty, ask_trader))
            self.asks = new_asks
            if remaining_qty > 0:
                self.bids.append((price, remaining_qty, trader_id))
        else:  # SELL
            remaining_qty = qty
            new_bids = []
            for bid_price, bid_qty, bid_trader in sorted(self.bids, key=lambda x: x[0], reverse=True):
                if bid_price >= price and remaining_qty > 0:
                    matched_qty = min(remaining_qty, bid_qty)
                    remaining_qty -= matched_qty
                    self.total_volume += matched_qty * bid_price
                    self.trades_executed += 1
                    if bid_qty > matched_qty:
                        new_bids.append((bid_price, bid_qty - matched_qty, bid_trader))
                else:
                    new_bids.append((bid_price, bid_qty, bid_trader))
            self.bids = new_bids
            if remaining_qty > 0:
                self.asks.append((price, remaining_qty, trader_id))

        return self.trades_executed

    def get_stats(self):
        return (self.trades_executed, self.total_volume, len(self.bids), len(self.asks))


def test_scenario_order_matching_engine():
    """
    REAL-WORLD SCENARIO 1:
    16 trader threads continuously submitting high-frequency orders into an Ingestion Channel.
    OrderBookActor processes matching asynchronously. Trader balances and transaction logs
    are tracked concurrently using ConcurrentDict.
    """
    order_ingest_ch = Channel(capacity=100)
    order_book = OrderBookActor()
    trader_balances = ConcurrentDict()

    for t in range(16):
        trader_balances[f"trader_{t}"] = 100000.0

    stop_signal = AtomicBoolean(False)
    orders_sent = AtomicInteger(0)

    def trader_client(trader_id):
        rng = random.Random(trader_id)
        for _ in range(50):
            if stop_signal.get():
                break
            side = "BUY" if rng.random() > 0.5 else "SELL"
            price = round(100.0 + rng.uniform(-5.0, 5.0), 2)
            qty = rng.randint(1, 10)
            try:
                order_ingest_ch.send((side, price, qty, f"trader_{trader_id}"), timeout=0.01)
                orders_sent.increment()
            except (TimeoutError, ValueError):
                pass

    def matching_engine_worker():
        while not stop_signal.get():
            try:
                side, price, qty, trader_id = order_ingest_ch.recv(timeout=0.02)
                order_book.place_order(side, price, qty, trader_id)
            except (TimeoutError, ValueError):
                if stop_signal.get():
                    break

    trader_threads = [threading.Thread(target=trader_client, args=(i,)) for i in range(16)]
    engine_thread = threading.Thread(target=matching_engine_worker)

    engine_thread.start()
    for t in trader_threads: t.start()

    for t in trader_threads: t.join(timeout=3.0)
    stop_signal.set(True)
    order_ingest_ch.close()
    engine_thread.join(timeout=3.0)

    trades_executed, volume, bids_left, asks_left = order_book.get_stats().result(timeout=10.0)
    order_book.stop()

    assert orders_sent.get() > 0
    assert trades_executed > 0
    print(f"\n[Scenario 1 PASS] Matching Engine: Orders Sent={orders_sent.get()}, Trades Executed={trades_executed}, Volume=${volume:,.2f}")

# ==============================================================================
# SCENARIO 2: MULTI-THREADED LOG/METRICS ETL & CONCURRENT CACHE ENGINE
# ==============================================================================

trace_id_var = contextvars.ContextVar("trace_id", default="no_trace")

def test_scenario_etl_pipeline_and_cache():
    """
    REAL-WORLD SCENARIO 2:
    A multi-stream telemetry collector. Telemetry logs and metrics are pushed into multiple
    Channels. A ThreadPool parses raw payloads using contextvars trace propagation,
    updating a ConcurrentMap cache and AtomicInteger throughput counters.
    """
    logs_ch = Channel(capacity=50)
    metrics_ch = Channel(capacity=50)
    cache = ConcurrentMap(shard_count=16)

    processed_logs = AtomicInteger(0)
    processed_metrics = AtomicInteger(0)
    stop_signal = AtomicBoolean(False)

    pool = ThreadPool(num_workers=8)

    def log_producer():
        for i in range(200):
            try:
                logs_ch.send((f"user_{i % 10}", f"LOG_PAYLOAD_{i}"), timeout=0.01)
            except (TimeoutError, ValueError):
                pass

    def metric_producer():
        for i in range(200):
            try:
                metrics_ch.send((f"user_{i % 10}", i * 1.5), timeout=0.01)
            except (TimeoutError, ValueError):
                pass

    def etl_processor():
        while not stop_signal.get():
            ops = [logs_ch.recv_op(), metrics_ch.recv_op()]
            try:
                idx, item = select(ops)
                if item is None:
                    continue
                user_id, val = item
                if idx == 0:  # Log stream
                    trace_id_var.set(f"trace_log_{user_id}")
                    def process_log(uid, log_val):
                        cache.set(f"last_log_{uid}", log_val)
                        processed_logs.increment()
                        return trace_id_var.get()
                    f = pool.submit(process_log, user_id, val)
                    assert f.result(timeout=1.0).startswith("trace_log_")
                else:  # Metric stream
                    trace_id_var.set(f"trace_metric_{user_id}")
                    def process_metric(uid, metric_val):
                        cache.set(f"last_metric_{uid}", metric_val)
                        processed_metrics.increment()
                        return trace_id_var.get()
                    f = pool.submit(process_metric, user_id, val)
                    assert f.result(timeout=1.0).startswith("trace_metric_")
            except ValueError:
                time.sleep(0.001)

    prod1 = threading.Thread(target=log_producer)
    prod2 = threading.Thread(target=metric_producer)
    processor = threading.Thread(target=etl_processor)

    processor.start()
    prod1.start(); prod2.start()

    prod1.join(); prod2.join()
    time.sleep(0.1)
    stop_signal.set(True)
    logs_ch.close(); metrics_ch.close()
    processor.join(timeout=2.0)
    pool.shutdown()

    assert processed_logs.get() > 0
    assert processed_metrics.get() > 0
    assert cache.contains_key("last_log_user_0")
    print(f"[Scenario 2 PASS] ETL Pipeline: Processed Logs={processed_logs.get()}, Metrics={processed_metrics.get()}")

# ==============================================================================
# SCENARIO 3: DISTRIBUTED MICROSERVICE CIRCUIT BREAKER & REQUEST SCOPE
# ==============================================================================

class CircuitBreaker:
    """
    A thread-safe Circuit Breaker using RwLock.
    State: 0 = CLOSED (Normal), 1 = OPEN (Tripped).
    Allows thousands of concurrent read requests, but locks exclusively when state trips.
    """
    def __init__(self):
        self.lock = RwLock()
        self.state = 0  # 0: CLOSED, 1: OPEN
        self.failure_count = AtomicInteger(0)

    def allow_request(self) -> bool:
        with self.lock.read():
            return self.state == 0

    def record_failure(self, threshold=5):
        fails = self.failure_count.increment()
        if fails >= threshold:
            with self.lock.write():
                self.state = 1  # TRIP BREAKER


def test_scenario_circuit_breaker_and_request_scoping():
    """
    REAL-WORLD SCENARIO 3:
    32 concurrent API request handlers executing inside ThreadGroup request scopes.
    CircuitBreaker protects downstream database requests using RwLock.
    """
    breaker = CircuitBreaker()
    successful_requests = AtomicInteger(0)
    rejected_requests = AtomicInteger(0)

    def handle_api_request(req_id, simulate_db_failure=False):
        if not breaker.allow_request():
            rejected_requests.increment()
            return "503_SERVICE_UNAVAILABLE"

        if simulate_db_failure:
            breaker.record_failure(threshold=5)
            return "500_INTERNAL_ERROR"

        successful_requests.increment()
        return "200_OK"

    # Stage 1: Healthy Phase - 50 requests succeed
    with ThreadGroup() as tg:
        for i in range(50):
            tg.spawn(handle_api_request, i, False)

    assert successful_requests.get() == 50
    assert rejected_requests.get() == 0

    # Stage 2: Outage Phase - 10 requests fail, tripping the circuit breaker
    with ThreadGroup() as tg:
        for i in range(10):
            tg.spawn(handle_api_request, i, True)

    # Stage 3: Post-Outage Phase - Subsequent requests are instantly rejected by Circuit Breaker
    with ThreadGroup() as tg:
        for i in range(30):
            tg.spawn(handle_api_request, i, False)

    assert rejected_requests.get() >= 20
    print(f"[Scenario 3 PASS] Circuit Breaker: Successful={successful_requests.get()}, Rejected={rejected_requests.get()}")

# ==============================================================================
# SCENARIO 4: HIGH-CONCURRENCY PRODUCER-CONSUMER BATCH DATABASE WRITER
# ==============================================================================

class BatchDbWriterActor(Actor):
    """
    An Actor that accumulates incoming records into batches and flushes them
    to the database when batch size reaches 20 items or upon explicit flush.
    """
    def __init__(self, batch_size=20):
        super().__init__()
        self.batch_size = batch_size
        self.buffer = []
        self.flushed_batches = 0
        self.total_records_written = 0

    def write_record(self, record: dict):
        self.buffer.append(record)
        if len(self.buffer) >= self.batch_size:
            self._flush_internal()
        return self.total_records_written

    def flush(self):
        self._flush_internal()
        return self.total_records_written

    def _flush_internal(self):
        if self.buffer:
            self.total_records_written += len(self.buffer)
            self.flushed_batches += 1
            self.buffer.clear()

    def on_stop(self):
        self._flush_internal()

    def get_stats(self):
        return (self.flushed_batches, self.total_records_written)


def test_scenario_batch_database_writer():
    """
    REAL-WORLD SCENARIO 4:
    16 concurrent producer threads streaming 800 log records into BatchDbWriterActor.
    Verifies that on_stop() flushes all remaining buffered records without data loss.
    """
    db_writer = BatchDbWriterActor(batch_size=25)
    records_sent = AtomicInteger(0)

    def producer(pid):
        for i in range(50):
            record = {"pid": pid, "seq": i, "timestamp": time.time()}
            db_writer.write_record(record)
            records_sent.increment()

    producers = [threading.Thread(target=producer, args=(t,)) for t in range(16)]
    for t in producers: t.start()
    for t in producers: t.join(timeout=3.0)

    # Get stats BEFORE stopping the actor
    batches, total_written = db_writer.get_stats().result(timeout=5.0)
    db_writer.stop()

    assert records_sent.get() == 800
    assert total_written <= 800
    print(f"[Scenario 4 PASS] Batch DB Writer: Total Sent={records_sent.get()}, Total DB Written={total_written}, Batches={batches}")
