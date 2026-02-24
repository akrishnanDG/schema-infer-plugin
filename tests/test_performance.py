"""
Unit tests for performance utilities (PerformanceMonitor, time_operation,
BatchProcessor, MemoryManager, CacheManager, AsyncProcessor).
"""

import asyncio
import threading
import time
from unittest.mock import MagicMock, patch

import pytest

from schema_infer.utils.performance import (
    AsyncProcessor,
    BatchProcessor,
    CacheManager,
    MemoryManager,
    PerformanceMonitor,
    time_operation,
)


# ---------------------------------------------------------------------------
# TestPerformanceMonitor
# ---------------------------------------------------------------------------

class TestPerformanceMonitor:
    """Tests for PerformanceMonitor timing and statistics."""

    def test_start_and_end_timer(self):
        """Start a timer, sleep briefly, end it, and verify duration > 0."""
        monitor = PerformanceMonitor()
        monitor.start_timer("op")
        time.sleep(0.05)
        duration = monitor.end_timer("op")

        assert duration > 0
        stats = monitor.get_stats("op")
        assert stats["count"] == 1
        assert stats["total"] > 0

    def test_multiple_timings(self):
        """Time the same operation multiple times and verify aggregate stats."""
        monitor = PerformanceMonitor()

        for _ in range(3):
            monitor.start_timer("repeat")
            time.sleep(0.02)
            monitor.end_timer("repeat")

        stats = monitor.get_stats("repeat")
        assert stats["count"] == 3
        assert stats["total"] > 0
        assert stats["average"] == pytest.approx(stats["total"] / 3)
        assert stats["min"] <= stats["average"] <= stats["max"]

    def test_get_stats_unknown_operation(self):
        """Requesting stats for an unknown operation returns an empty dict."""
        monitor = PerformanceMonitor()
        stats = monitor.get_stats("nonexistent")
        assert stats == {}

    def test_get_all_stats(self):
        """Time several operations and verify get_all_stats returns all."""
        monitor = PerformanceMonitor()

        for name in ("alpha", "beta", "gamma"):
            monitor.start_timer(name)
            monitor.end_timer(name)

        all_stats = monitor.get_all_stats()
        assert set(all_stats.keys()) == {"alpha", "beta", "gamma"}
        for name in ("alpha", "beta", "gamma"):
            assert "count" in all_stats[name]

    def test_end_timer_without_start(self):
        """Ending a timer that was never started returns 0.0."""
        monitor = PerformanceMonitor()
        duration = monitor.end_timer("never_started")
        assert duration == 0.0


# ---------------------------------------------------------------------------
# TestTimeOperationDecorator
# ---------------------------------------------------------------------------

class TestTimeOperationDecorator:
    """Tests for the time_operation decorator."""

    def test_decorator_times_function(self):
        """Decorating a method on an object with performance_monitor records stats."""

        class Service:
            def __init__(self):
                self.performance_monitor = PerformanceMonitor()

            @time_operation("do_work")
            def do_work(self, x):
                time.sleep(0.02)
                return x * 2

        svc = Service()
        result = svc.do_work(5)

        assert result == 10
        stats = svc.performance_monitor.get_stats("do_work")
        assert stats["count"] == 1
        assert stats["total"] > 0

    def test_decorator_without_monitor(self):
        """Decorating a method on an object without performance_monitor does not error."""

        class Plain:
            @time_operation("noop")
            def compute(self, x):
                return x + 1

        obj = Plain()
        result = obj.compute(3)
        assert result == 4

    def test_decorator_preserves_return_value(self):
        """The decorated function returns the original value unaltered."""

        class Svc:
            performance_monitor = PerformanceMonitor()

            @time_operation("identity")
            def identity(self, val):
                return val

        svc = Svc()
        assert svc.identity({"key": [1, 2]}) == {"key": [1, 2]}


# ---------------------------------------------------------------------------
# TestBatchProcessor
# ---------------------------------------------------------------------------

class TestBatchProcessor:
    """Tests for BatchProcessor sequential and parallel processing."""

    def test_sequential_processing(self):
        """Process a list of items sequentially and verify all are processed."""
        processor = BatchProcessor(batch_size=3)
        data = list(range(7))
        processed_batches = []

        def handler(batch):
            processed_batches.append(list(batch))
            return [x * 2 for x in batch]

        results = processor.process_batches(data, handler, parallel=False)

        # 7 items / batch_size 3 => 3 batches (3, 3, 1)
        assert len(results) == 3
        assert processed_batches[0] == [0, 1, 2]
        assert processed_batches[1] == [3, 4, 5]
        assert processed_batches[2] == [6]
        assert results == [[0, 2, 4], [6, 8, 10], [12]]

    def test_parallel_processing(self):
        """Process items in parallel and verify all items are processed."""
        processor = BatchProcessor(batch_size=3, max_workers=2)
        data = list(range(9))

        def handler(batch):
            return sum(batch)

        results = processor.process_batches(data, handler, parallel=True)

        # 3 batches: [0,1,2]=3, [3,4,5]=12, [6,7,8]=21
        assert sorted(results) == [3, 12, 21]

    def test_batch_size_respected(self):
        """batch_size=3 with 10 items produces correct batch sizes."""
        processor = BatchProcessor(batch_size=3)
        data = list(range(10))
        batch_sizes_seen = []

        def handler(batch):
            batch_sizes_seen.append(len(batch))
            return batch

        processor.process_batches(data, handler, parallel=False)

        # Expect batches of sizes [3, 3, 3, 1]
        assert batch_sizes_seen == [3, 3, 3, 1]

    def test_empty_data(self):
        """Processing an empty list returns an empty result."""
        processor = BatchProcessor()
        results = processor.process_batches([], lambda b: b, parallel=False)
        assert results == []

    def test_parallel_single_batch_falls_back_to_sequential(self):
        """When data fits in one batch, parallel=True still processes sequentially."""
        processor = BatchProcessor(batch_size=100)
        data = [1, 2, 3]
        called = []

        def handler(batch):
            called.append(batch)
            return batch

        results = processor.process_batches(data, handler, parallel=True)

        assert len(results) == 1
        assert results[0] == [1, 2, 3]


# ---------------------------------------------------------------------------
# TestMemoryManager
# ---------------------------------------------------------------------------

class TestMemoryManager:
    """Tests for MemoryManager memory checks and optimization."""

    @patch("schema_infer.utils.performance.MemoryManager.check_memory_usage")
    def test_check_memory_usage_returns_expected_keys(self, mock_check):
        """check_memory_usage returns a dict with the expected keys."""
        mock_check.return_value = {
            "rss": 100 * 1024 * 1024,
            "vms": 200 * 1024 * 1024,
            "percent": 5.0,
            "available": 8000 * 1024 * 1024,
        }
        manager = MemoryManager()
        info = manager.check_memory_usage()

        assert "rss" in info
        assert "vms" in info
        assert "percent" in info
        assert "available" in info

    @patch("schema_infer.utils.performance.MemoryManager.check_memory_usage")
    def test_is_memory_limit_exceeded_true(self, mock_check):
        """With a very low limit and high RSS, exceeded returns True."""
        mock_check.return_value = {
            "rss": 600 * 1024 * 1024,  # 600 MB RSS
            "vms": 0,
            "percent": 0,
            "available": 0,
        }
        manager = MemoryManager(memory_limit_mb=100)  # 100 MB limit
        assert manager.is_memory_limit_exceeded() is True

    @patch("schema_infer.utils.performance.MemoryManager.check_memory_usage")
    def test_is_memory_limit_exceeded_false(self, mock_check):
        """With a very high limit, exceeded returns False."""
        mock_check.return_value = {
            "rss": 50 * 1024 * 1024,  # 50 MB RSS
            "vms": 0,
            "percent": 0,
            "available": 0,
        }
        manager = MemoryManager(memory_limit_mb=10000)  # 10 GB limit
        assert manager.is_memory_limit_exceeded() is False

    @patch("schema_infer.utils.performance.MemoryManager.check_memory_usage")
    def test_is_memory_limit_not_exceeded_when_psutil_unavailable(self, mock_check):
        """When psutil is missing, check_memory_usage returns {} and exceeded is False."""
        mock_check.return_value = {}
        manager = MemoryManager(memory_limit_mb=1)
        assert manager.is_memory_limit_exceeded() is False

    def test_optimize_memory_does_not_raise(self):
        """optimize_memory (gc.collect) completes without raising."""
        manager = MemoryManager()
        manager.optimize_memory()  # should not raise


# ---------------------------------------------------------------------------
# TestCacheManager
# ---------------------------------------------------------------------------

class TestCacheManager:
    """Tests for CacheManager set/get, expiry, eviction, and thread safety."""

    def test_set_and_get(self):
        """Set a value and retrieve it."""
        cache = CacheManager()
        cache.set("key1", "value1")
        assert cache.get("key1") == "value1"

    def test_get_missing_key(self):
        """Getting a nonexistent key returns None."""
        cache = CacheManager()
        assert cache.get("missing") is None

    def test_cache_expiry(self):
        """Items expire after TTL seconds."""
        cache = CacheManager(ttl=1)
        cache.set("ephemeral", 42)
        assert cache.get("ephemeral") == 42

        time.sleep(1.5)
        assert cache.get("ephemeral") is None

    def test_clear(self):
        """clear() removes all cached items."""
        cache = CacheManager()
        cache.set("a", 1)
        cache.set("b", 2)
        cache.set("c", 3)
        cache.clear()

        assert cache.get("a") is None
        assert cache.get("b") is None
        assert cache.get("c") is None

    def test_max_size(self):
        """When max_size is exceeded the oldest entry is evicted."""
        cache = CacheManager(max_size=2, ttl=3600)
        cache.set("first", 1)
        time.sleep(0.01)  # ensure distinct timestamps
        cache.set("second", 2)
        time.sleep(0.01)
        cache.set("third", 3)  # should evict "first"

        assert cache.get("first") is None
        assert cache.get("second") == 2
        assert cache.get("third") == 3

    def test_cleanup_expired(self):
        """cleanup_expired removes items past their TTL."""
        cache = CacheManager(ttl=1)
        cache.set("temp1", "a")
        cache.set("temp2", "b")

        time.sleep(1.5)
        cache.cleanup_expired()

        assert cache.get("temp1") is None
        assert cache.get("temp2") is None
        assert len(cache.cache) == 0

    def test_thread_safety(self):
        """Concurrent set/get from multiple threads does not raise."""
        cache = CacheManager(max_size=100, ttl=60)
        errors = []

        def writer(thread_id):
            try:
                for i in range(50):
                    cache.set(f"t{thread_id}_k{i}", i)
            except Exception as exc:
                errors.append(exc)

        def reader(thread_id):
            try:
                for i in range(50):
                    cache.get(f"t{thread_id}_k{i}")
            except Exception as exc:
                errors.append(exc)

        threads = []
        for tid in range(4):
            threads.append(threading.Thread(target=writer, args=(tid,)))
            threads.append(threading.Thread(target=reader, args=(tid,)))

        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)

        assert errors == [], f"Thread safety violations: {errors}"

    def test_set_overwrites_existing_key(self):
        """Setting the same key again updates the value."""
        cache = CacheManager()
        cache.set("k", "old")
        cache.set("k", "new")
        assert cache.get("k") == "new"


# ---------------------------------------------------------------------------
# TestAsyncProcessor
# ---------------------------------------------------------------------------

class TestAsyncProcessor:
    """Tests for AsyncProcessor async item and batch processing."""

    @pytest.mark.asyncio
    async def test_process_async(self):
        """Process a list of items with an async function."""
        processor = AsyncProcessor(max_concurrent=5)

        async def double(x):
            return x * 2

        results = await processor.process_async([1, 2, 3, 4], double)
        assert sorted(results) == [2, 4, 6, 8]

    @pytest.mark.asyncio
    async def test_process_async_with_sync_function(self):
        """process_async works with a plain synchronous callable too."""
        processor = AsyncProcessor(max_concurrent=5)

        results = await processor.process_async([10, 20], lambda x: x + 1)
        assert sorted(results) == [11, 21]

    @pytest.mark.asyncio
    async def test_process_batches_async(self):
        """Process batches with an async function and verify results."""
        processor = AsyncProcessor(max_concurrent=5)

        async def sum_batch(batch):
            return sum(batch)

        batches = [[1, 2, 3], [4, 5, 6], [7, 8, 9]]
        results = await processor.process_batches_async(batches, sum_batch)
        assert sorted(results) == [6, 15, 24]

    @pytest.mark.asyncio
    async def test_process_batches_async_with_sync_function(self):
        """process_batches_async also handles a synchronous callable."""
        processor = AsyncProcessor(max_concurrent=5)

        batches = [[1, 2], [3, 4]]
        results = await processor.process_batches_async(batches, sum)
        assert sorted(results) == [3, 7]

    @pytest.mark.asyncio
    async def test_concurrency_limiting(self):
        """max_concurrent limits the number of simultaneous operations."""
        max_concurrent = 2
        processor = AsyncProcessor(max_concurrent=max_concurrent)

        concurrent_count = 0
        max_observed = 0
        lock = asyncio.Lock()

        async def tracked_work(item):
            nonlocal concurrent_count, max_observed
            async with lock:
                concurrent_count += 1
                if concurrent_count > max_observed:
                    max_observed = concurrent_count
            await asyncio.sleep(0.05)
            async with lock:
                concurrent_count -= 1
            return item

        items = list(range(8))
        results = await processor.process_async(items, tracked_work)

        assert sorted(results) == list(range(8))
        assert max_observed <= max_concurrent

    @pytest.mark.asyncio
    async def test_process_async_empty_list(self):
        """Processing an empty list returns an empty result."""
        processor = AsyncProcessor()

        async def noop(x):
            return x

        results = await processor.process_async([], noop)
        assert results == []

    @pytest.mark.asyncio
    async def test_process_async_handles_errors_gracefully(self):
        """Items that raise exceptions are filtered out of results."""
        processor = AsyncProcessor(max_concurrent=5)

        async def maybe_fail(x):
            if x == 2:
                raise ValueError("boom")
            return x

        results = await processor.process_async([1, 2, 3], maybe_fail)
        # x=2 raises, so it should be excluded
        assert 2 not in results
        assert 1 in results
        assert 3 in results
