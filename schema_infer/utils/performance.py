"""
Performance optimization utilities for Schema Inference Plugin
"""

import asyncio
import concurrent.futures
import multiprocessing
import time
from functools import wraps
from typing import Any, Callable, Dict, List, Optional, Tuple

from ..utils.logger import get_logger


class PerformanceMonitor:
    """Monitor and track performance metrics."""
    
    def __init__(self):
        """Initialize performance monitor."""
        self.logger = get_logger(__name__)
        self.metrics: Dict[str, List[float]] = {}
        self.start_times: Dict[str, float] = {}
    
    def start_timer(self, operation: str) -> None:
        """Start timing an operation."""
        self.start_times[operation] = time.time()
    
    def end_timer(self, operation: str) -> float:
        """End timing an operation and return duration."""
        if operation not in self.start_times:
            return 0.0
        
        duration = time.time() - self.start_times[operation]
        
        if operation not in self.metrics:
            self.metrics[operation] = []
        
        self.metrics[operation].append(duration)
        del self.start_times[operation]
        
        return duration
    
    def get_stats(self, operation: str) -> Dict[str, float]:
        """Get statistics for an operation."""
        if operation not in self.metrics or not self.metrics[operation]:
            return {}
        
        durations = self.metrics[operation]
        
        return {
            "count": len(durations),
            "total": sum(durations),
            "average": sum(durations) / len(durations),
            "min": min(durations),
            "max": max(durations),
        }
    
    def get_all_stats(self) -> Dict[str, Dict[str, float]]:
        """Get statistics for all operations."""
        return {op: self.get_stats(op) for op in self.metrics.keys()}


def time_operation(operation_name: str):
    """Decorator to time operations."""
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            monitor = getattr(args[0], 'performance_monitor', None)
            if monitor:
                monitor.start_timer(operation_name)
            
            try:
                result = func(*args, **kwargs)
                return result
            finally:
                if monitor:
                    duration = monitor.end_timer(operation_name)
                    monitor.logger.debug(f"{operation_name} took {duration:.2f} seconds")
        
        return wrapper
    return decorator


class BatchProcessor:
    """Process data in batches for better performance."""
    
    def __init__(self, batch_size: int = 100, max_workers: int = 4):
        """
        Initialize batch processor.
        
        Args:
            batch_size: Size of each batch
            max_workers: Maximum number of worker threads
        """
        self.batch_size = batch_size
        self.max_workers = max_workers
        self.logger = get_logger(__name__)
    
    def process_batches(
        self, 
        data: List[Any], 
        processor_func: Callable[[List[Any]], Any],
        parallel: bool = False
    ) -> List[Any]:
        """
        Process data in batches.
        
        Args:
            data: Data to process
            processor_func: Function to process each batch
            parallel: Whether to process batches in parallel
            
        Returns:
            List of processed results
        """
        
        if not data:
            return []
        
        # Create batches
        batches = [
            data[i:i + self.batch_size] 
            for i in range(0, len(data), self.batch_size)
        ]
        
        self.logger.info(f"Processing {len(data)} items in {len(batches)} batches")
        
        if parallel and len(batches) > 1:
            return self._process_batches_parallel(batches, processor_func)
        else:
            return self._process_batches_sequential(batches, processor_func)
    
    def _process_batches_sequential(
        self, 
        batches: List[List[Any]], 
        processor_func: Callable[[List[Any]], Any]
    ) -> List[Any]:
        """Process batches sequentially."""
        
        results = []
        
        for i, batch in enumerate(batches):
            self.logger.debug(f"Processing batch {i + 1}/{len(batches)}")
            try:
                result = processor_func(batch)
                results.append(result)
            except Exception as e:
                self.logger.error(f"Error processing batch {i + 1}: {e}")
                results.append(None)
        
        return results
    
    def _process_batches_parallel(
        self, 
        batches: List[List[Any]], 
        processor_func: Callable[[List[Any]], Any]
    ) -> List[Any]:
        """Process batches in parallel."""
        
        results = []
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all batches
            future_to_batch = {
                executor.submit(processor_func, batch): i 
                for i, batch in enumerate(batches)
            }
            
            # Collect results as they complete
            for future in concurrent.futures.as_completed(future_to_batch):
                batch_index = future_to_batch[future]
                try:
                    result = future.result()
                    results.append((batch_index, result))
                except Exception as e:
                    self.logger.error(f"Error processing batch {batch_index + 1}: {e}")
                    results.append((batch_index, None))
        
        # Sort results by batch index
        results.sort(key=lambda x: x[0])
        return [result for _, result in results]


class MemoryManager:
    """Manage memory usage and optimization."""
    
    def __init__(self, memory_limit_mb: int = 512):
        """
        Initialize memory manager.
        
        Args:
            memory_limit_mb: Memory limit in MB
        """
        self.memory_limit_bytes = memory_limit_mb * 1024 * 1024
        self.logger = get_logger(__name__)
    
    def check_memory_usage(self) -> Dict[str, Any]:
        """Check current memory usage."""
        try:
            import psutil
            process = psutil.Process()
            memory_info = process.memory_info()
            
            return {
                "rss": memory_info.rss,  # Resident Set Size
                "vms": memory_info.vms,  # Virtual Memory Size
                "percent": process.memory_percent(),
                "available": psutil.virtual_memory().available,
            }
        except ImportError:
            self.logger.warning("psutil not available, cannot check memory usage")
            return {}
    
    def is_memory_limit_exceeded(self) -> bool:
        """Check if memory limit is exceeded."""
        memory_info = self.check_memory_usage()
        
        if not memory_info:
            return False
        
        return memory_info["rss"] > self.memory_limit_bytes
    
    def optimize_memory(self) -> None:
        """Perform memory optimization."""
        try:
            import gc
            gc.collect()
            self.logger.debug("Performed garbage collection")
        except Exception as e:
            self.logger.warning(f"Failed to optimize memory: {e}")


class CacheManager:
    """Manage caching for improved performance."""
    
    def __init__(self, max_size: int = 1000, ttl: int = 3600):
        """
        Initialize cache manager.
        
        Args:
            max_size: Maximum number of cached items
            ttl: Time-to-live in seconds
        """
        self.max_size = max_size
        self.ttl = ttl
        self.cache: Dict[str, Tuple[Any, float]] = {}
        self.logger = get_logger(__name__)
    
    def get(self, key: str) -> Optional[Any]:
        """Get value from cache."""
        if key not in self.cache:
            return None
        
        value, timestamp = self.cache[key]
        
        # Check TTL
        if time.time() - timestamp > self.ttl:
            del self.cache[key]
            return None
        
        return value
    
    def set(self, key: str, value: Any) -> None:
        """Set value in cache."""
        # Remove oldest items if cache is full
        if len(self.cache) >= self.max_size:
            oldest_key = min(self.cache.keys(), key=lambda k: self.cache[k][1])
            del self.cache[oldest_key]
        
        self.cache[key] = (value, time.time())
    
    def clear(self) -> None:
        """Clear all cached items."""
        self.cache.clear()
    
    def cleanup_expired(self) -> None:
        """Remove expired items from cache."""
        current_time = time.time()
        expired_keys = [
            key for key, (_, timestamp) in self.cache.items()
            if current_time - timestamp > self.ttl
        ]
        
        for key in expired_keys:
            del self.cache[key]
        
        if expired_keys:
            self.logger.debug(f"Cleaned up {len(expired_keys)} expired cache items")


class AsyncProcessor:
    """Handle asynchronous processing for better performance."""
    
    def __init__(self, max_concurrent: int = 10):
        """
        Initialize async processor.
        
        Args:
            max_concurrent: Maximum number of concurrent operations
        """
        self.max_concurrent = max_concurrent
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.logger = get_logger(__name__)
    
    async def process_async(
        self, 
        items: List[Any], 
        processor_func: Callable[[Any], Any]
    ) -> List[Any]:
        """
        Process items asynchronously.
        
        Args:
            items: Items to process
            processor_func: Async function to process each item
            
        Returns:
            List of processed results
        """
        
        async def process_item(item: Any) -> Any:
            async with self.semaphore:
                try:
                    if asyncio.iscoroutinefunction(processor_func):
                        return await processor_func(item)
                    else:
                        return processor_func(item)
                except Exception as e:
                    self.logger.error(f"Error processing item: {e}")
                    return None
        
        tasks = [process_item(item) for item in items]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Filter out exceptions
        return [r for r in results if not isinstance(r, Exception)]
    
    async def process_batches_async(
        self, 
        batches: List[List[Any]], 
        processor_func: Callable[[List[Any]], Any]
    ) -> List[Any]:
        """
        Process batches asynchronously.
        
        Args:
            batches: Batches to process
            processor_func: Async function to process each batch
            
        Returns:
            List of processed results
        """
        
        async def process_batch(batch: List[Any]) -> Any:
            async with self.semaphore:
                try:
                    if asyncio.iscoroutinefunction(processor_func):
                        return await processor_func(batch)
                    else:
                        return processor_func(batch)
                except Exception as e:
                    self.logger.error(f"Error processing batch: {e}")
                    return None
        
        tasks = [process_batch(batch) for batch in batches]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Filter out exceptions
        return [r for r in results if not isinstance(r, Exception)]
