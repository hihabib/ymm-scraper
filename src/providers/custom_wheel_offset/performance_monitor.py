#!/usr/bin/env python3
"""
Performance monitoring system for Custom Wheel Offset scraper.
Tracks metrics, timing, and bottlenecks to measure multithreading improvements.
"""

import time
import threading
import statistics
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from collections import defaultdict, deque
import json
from datetime import datetime

from .logging_config import init_module_logger

logger = init_module_logger(__name__)

@dataclass
class PerformanceMetric:
    """Individual performance metric data."""
    name: str
    start_time: float
    end_time: Optional[float] = None
    thread_id: Optional[int] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def duration(self) -> Optional[float]:
        """Get duration in seconds."""
        if self.end_time is not None:
            return self.end_time - self.start_time
        return None
    
    @property
    def is_completed(self) -> bool:
        """Check if metric is completed."""
        return self.end_time is not None

@dataclass
class ThreadMetrics:
    """Metrics for a specific thread."""
    thread_id: int
    start_time: float
    end_time: Optional[float] = None
    items_processed: int = 0
    errors: int = 0
    network_requests: int = 0
    database_operations: int = 0
    cache_hits: int = 0
    cache_misses: int = 0
    
    @property
    def duration(self) -> Optional[float]:
        """Get thread duration in seconds."""
        if self.end_time is not None:
            return self.end_time - self.start_time
        return None
    
    @property
    def items_per_second(self) -> Optional[float]:
        """Get items processed per second."""
        if self.duration and self.duration > 0:
            return self.items_processed / self.duration
        return None

class PerformanceMonitor:
    """Performance monitoring system for tracking multithreading improvements."""
    
    def __init__(self):
        self._metrics: Dict[str, PerformanceMetric] = {}
        self._thread_metrics: Dict[int, ThreadMetrics] = {}
        self._operation_times: Dict[str, deque] = defaultdict(lambda: deque(maxlen=1000))
        self._counters: Dict[str, int] = defaultdict(int)
        self._lock = threading.Lock()
        self._session_start = time.time()
        
    def start_metric(self, name: str, metadata: Dict[str, Any] = None) -> str:
        """Start tracking a performance metric."""
        metric_id = f"{name}_{int(time.time() * 1000000)}"
        thread_id = threading.current_thread().ident
        
        with self._lock:
            self._metrics[metric_id] = PerformanceMetric(
                name=name,
                start_time=time.time(),
                thread_id=thread_id,
                metadata=metadata or {}
            )
        
        logger.debug(f"[PerfMonitor] Started metric: {name} (ID: {metric_id})")
        return metric_id
    
    def end_metric(self, metric_id: str, metadata: Dict[str, Any] = None):
        """End tracking a performance metric."""
        end_time = time.time()
        
        with self._lock:
            if metric_id in self._metrics:
                metric = self._metrics[metric_id]
                metric.end_time = end_time
                if metadata:
                    metric.metadata.update(metadata)
                
                # Add to operation times for statistics
                self._operation_times[metric.name].append(metric.duration)
                
                logger.debug(f"[PerfMonitor] Ended metric: {metric.name} (Duration: {metric.duration:.3f}s)")
            else:
                logger.warning(f"[PerfMonitor] Metric ID not found: {metric_id}")
    
    def start_thread_tracking(self, thread_id: int = None):
        """Start tracking metrics for a thread."""
        if thread_id is None:
            thread_id = threading.current_thread().ident
        
        with self._lock:
            self._thread_metrics[thread_id] = ThreadMetrics(
                thread_id=thread_id,
                start_time=time.time()
            )
        
        logger.info(f"[PerfMonitor] Started tracking thread {thread_id}")
    
    def end_thread_tracking(self, thread_id: int = None):
        """End tracking metrics for a thread."""
        if thread_id is None:
            thread_id = threading.current_thread().ident
        
        with self._lock:
            if thread_id in self._thread_metrics:
                self._thread_metrics[thread_id].end_time = time.time()
                logger.info(f"[PerfMonitor] Ended tracking thread {thread_id}")
            else:
                logger.warning(f"[PerfMonitor] Thread metrics not found: {thread_id}")
    
    def increment_counter(self, name: str, value: int = 1, thread_id: int = None):
        """Increment a performance counter."""
        if thread_id is None:
            thread_id = threading.current_thread().ident
        
        with self._lock:
            self._counters[name] += value
            
            # Update thread-specific counters
            if thread_id in self._thread_metrics:
                thread_metrics = self._thread_metrics[thread_id]
                if name == "items_processed":
                    thread_metrics.items_processed += value
                elif name == "errors":
                    thread_metrics.errors += value
                elif name == "network_requests":
                    thread_metrics.network_requests += value
                elif name == "database_operations":
                    thread_metrics.database_operations += value
                elif name == "cache_hits":
                    thread_metrics.cache_hits += value
                elif name == "cache_misses":
                    thread_metrics.cache_misses += value
    
    def record_operation_time(self, operation_name: str, duration: float):
        """Record the time taken for an operation."""
        with self._lock:
            self._operation_times[operation_name].append(duration)
    
    def get_operation_stats(self, operation_name: str) -> Dict[str, float]:
        """Get statistics for an operation."""
        with self._lock:
            times = list(self._operation_times[operation_name])
        
        if not times:
            return {}
        
        return {
            'count': len(times),
            'mean': statistics.mean(times),
            'median': statistics.median(times),
            'min': min(times),
            'max': max(times),
            'stdev': statistics.stdev(times) if len(times) > 1 else 0.0,
            'total': sum(times)
        }
    
    def get_thread_summary(self) -> Dict[int, Dict[str, Any]]:
        """Get summary of all thread metrics."""
        with self._lock:
            summary = {}
            for thread_id, metrics in self._thread_metrics.items():
                summary[thread_id] = {
                    'duration': metrics.duration,
                    'items_processed': metrics.items_processed,
                    'items_per_second': metrics.items_per_second,
                    'errors': metrics.errors,
                    'network_requests': metrics.network_requests,
                    'database_operations': metrics.database_operations,
                    'cache_hits': metrics.cache_hits,
                    'cache_misses': metrics.cache_misses,
                    'cache_hit_rate': (
                        metrics.cache_hits / (metrics.cache_hits + metrics.cache_misses)
                        if (metrics.cache_hits + metrics.cache_misses) > 0 else 0.0
                    )
                }
            return summary
    
    def get_overall_stats(self) -> Dict[str, Any]:
        """Get overall performance statistics."""
        with self._lock:
            session_duration = time.time() - self._session_start
            
            # Calculate thread statistics
            active_threads = len([t for t in self._thread_metrics.values() if t.end_time is not None])
            total_items = sum(t.items_processed for t in self._thread_metrics.values())
            total_errors = sum(t.errors for t in self._thread_metrics.values())
            total_network_requests = sum(t.network_requests for t in self._thread_metrics.values())
            total_db_operations = sum(t.database_operations for t in self._thread_metrics.values())
            
            # Calculate throughput
            overall_throughput = total_items / session_duration if session_duration > 0 else 0.0
            
            # Calculate average thread performance
            completed_threads = [t for t in self._thread_metrics.values() if t.duration is not None]
            thread_throughputs = [t.items_per_second for t in completed_threads if t.items_per_second]
            avg_thread_throughput = (
                statistics.mean(thread_throughputs) if thread_throughputs else 0.0
            )
            
            return {
                'session_duration': session_duration,
                'active_threads': active_threads,
                'total_items_processed': total_items,
                'total_errors': total_errors,
                'total_network_requests': total_network_requests,
                'total_database_operations': total_db_operations,
                'overall_throughput': overall_throughput,
                'average_thread_throughput': avg_thread_throughput,
                'error_rate': total_errors / total_items if total_items > 0 else 0.0,
                'counters': dict(self._counters)
            }
    
    def print_performance_report(self):
        """Print a comprehensive performance report."""
        print("\n" + "="*80)
        print("PERFORMANCE MONITORING REPORT")
        print("="*80)
        
        # Overall statistics
        overall_stats = self.get_overall_stats()
        print(f"\nOVERALL STATISTICS:")
        print(f"  Session Duration: {overall_stats['session_duration']:.2f} seconds")
        print(f"  Active Threads: {overall_stats['active_threads']}")
        print(f"  Total Items Processed: {overall_stats['total_items_processed']}")
        print(f"  Overall Throughput: {overall_stats['overall_throughput']:.2f} items/sec")
        print(f"  Average Thread Throughput: {overall_stats['average_thread_throughput']:.2f} items/sec")
        print(f"  Error Rate: {overall_stats['error_rate']:.2%}")
        print(f"  Total Network Requests: {overall_stats['total_network_requests']}")
        print(f"  Total Database Operations: {overall_stats['total_database_operations']}")
        
        # Thread summary
        thread_summary = self.get_thread_summary()
        if thread_summary:
            print(f"\nTHREAD PERFORMANCE:")
            for thread_id, stats in thread_summary.items():
                print(f"  Thread {thread_id}:")
                print(f"    Duration: {stats['duration']:.2f}s" if stats['duration'] else "    Duration: Running")
                print(f"    Items Processed: {stats['items_processed']}")
                print(f"    Throughput: {stats['items_per_second']:.2f} items/sec" if stats['items_per_second'] else "    Throughput: N/A")
                print(f"    Errors: {stats['errors']}")
                print(f"    Cache Hit Rate: {stats['cache_hit_rate']:.2%}")
        
        # Operation statistics
        print(f"\nOPERATION STATISTICS:")
        for operation_name in self._operation_times.keys():
            stats = self.get_operation_stats(operation_name)
            if stats:
                print(f"  {operation_name}:")
                print(f"    Count: {stats['count']}")
                print(f"    Mean: {stats['mean']:.3f}s")
                print(f"    Median: {stats['median']:.3f}s")
                print(f"    Min/Max: {stats['min']:.3f}s / {stats['max']:.3f}s")
                print(f"    Std Dev: {stats['stdev']:.3f}s")
        
        print("="*80)
    
    def export_metrics(self, filename: str = None) -> str:
        """Export metrics to JSON file."""
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"performance_metrics_{timestamp}.json"
        
        export_data = {
            'timestamp': datetime.now().isoformat(),
            'overall_stats': self.get_overall_stats(),
            'thread_summary': self.get_thread_summary(),
            'operation_stats': {
                name: self.get_operation_stats(name)
                for name in self._operation_times.keys()
            }
        }
        
        try:
            with open(filename, 'w') as f:
                json.dump(export_data, f, indent=2)
            logger.info(f"[PerfMonitor] Metrics exported to {filename}")
            return filename
        except Exception as e:
            logger.error(f"[PerfMonitor] Failed to export metrics: {e}")
            return ""

# Global performance monitor instance
performance_monitor = PerformanceMonitor()

# Context manager for easy metric tracking
class MetricTracker:
    """Context manager for tracking performance metrics."""
    
    def __init__(self, name: str, metadata: Dict[str, Any] = None):
        self.name = name
        self.metadata = metadata or {}
        self.metric_id = None
    
    def __enter__(self):
        self.metric_id = performance_monitor.start_metric(self.name, self.metadata)
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            self.metadata['error'] = str(exc_val)
        performance_monitor.end_metric(self.metric_id, self.metadata)

# Decorator for automatic function timing
def track_performance(operation_name: str = None):
    """Decorator to automatically track function performance."""
    def decorator(func):
        def wrapper(*args, **kwargs):
            name = operation_name or f"{func.__module__}.{func.__name__}"
            with MetricTracker(name):
                return func(*args, **kwargs)
        return wrapper
    return decorator