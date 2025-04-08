#!/usr/bin/env python3
"""
Prometheus metrics exporter for the ETL pipeline.
"""

import logging
import threading
import time
from typing import Dict, List, Any, Optional
import http.server
import socketserver
from prometheus_client import Counter, Gauge, Histogram, Info, start_http_server, REGISTRY, PROCESS_COLLECTOR, PLATFORM_COLLECTOR

from memory_manager import get_memory_manager

logger = logging.getLogger("SierraChartETL.prometheus")

class PrometheusExporter:
    """
    Exports metrics in Prometheus format for monitoring the ETL pipeline.
    """
    
    def __init__(self, port: int = 9090):
        """
        Initialize the Prometheus exporter.
        
        Args:
            port: HTTP port for the metrics server
        """
        self.port = port
        self.server = None
        self.metrics = {}
        
        # Create metrics
        self._create_metrics()
    
    def _create_metrics(self) -> None:
        """Create Prometheus metrics."""
        # ETL pipeline metrics
        self.metrics["records_processed"] = Counter(
            "etl_records_processed_total",
            "Total number of records processed",
            ["contract_id", "data_type"]
        )
        
        self.metrics["records_filtered"] = Counter(
            "etl_records_filtered_total",
            "Total number of records filtered out",
            ["contract_id", "data_type", "reason"]
        )
        
        self.metrics["files_processed"] = Counter(
            "etl_files_processed_total",
            "Total number of files processed",
            ["contract_id", "data_type"]
        )
        
        self.metrics["processing_errors"] = Counter(
            "etl_processing_errors_total",
            "Total number of processing errors",
            ["component", "error_type"]
        )
        
        # Database metrics
        self.metrics["db_queries"] = Counter(
            "etl_db_queries_total",
            "Total number of database queries",
            ["operation_type"]
        )
        
        self.metrics["db_query_duration"] = Histogram(
            "etl_db_query_duration_seconds",
            "Duration of database queries",
            ["operation_type"],
            buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 10, 30]
        )
        
        self.metrics["db_connections"] = Gauge(
            "etl_db_connections",
            "Number of active database connections"
        )
        
        # Memory metrics
        self.metrics["memory_usage"] = Gauge(
            "etl_memory_usage_bytes",
            "Memory usage of the ETL process"
        )
        
        self.metrics["gc_collections"] = Counter(
            "etl_gc_collections_total",
            "Total number of garbage collections",
            ["generation"]
        )
        
        # Cache metrics
        self.metrics["cache_hits"] = Counter(
            "etl_cache_hits_total",
            "Total number of cache hits",
            ["cache_type"]
        )
        
        self.metrics["cache_misses"] = Counter(
            "etl_cache_misses_total",
            "Total number of cache misses",
            ["cache_type"]
        )
        
        # System metrics
        self.metrics["disk_usage"] = Gauge(
            "etl_disk_usage_bytes",
            "Disk usage",
            ["path", "type"]
        )
        
        # ETL info
        self.metrics["etl_info"] = Info(
            "etl_info",
            "Information about the ETL pipeline"
        )
    
    def start_server(self) -> None:
        """Start the Prometheus metrics server."""
        # Start the server in a new thread
        try:
            start_http_server(self.port)
            logger.info(f"Prometheus metrics server started on port {self.port}")
        except Exception as e:
            logger.error(f"Failed to start Prometheus server: {str(e)}")
    
    def update_etl_metrics(self, stats: Dict[str, Any]) -> None:
        """
        Update ETL pipeline metrics.
        
        Args:
            stats: ETL pipeline statistics
        """
        # Update record counts
        for contract_id in stats.get("contracts", {}).keys():
            for data_type in ["tas", "depth"]:
                count = stats.get("contracts", {}).get(contract_id, {}).get(f"{data_type}_records_processed", 0)
                if count > 0:
                    self.metrics["records_processed"].labels(contract_id=contract_id, data_type=data_type).inc(count)
                
                filtered = stats.get("contracts", {}).get(contract_id, {}).get(f"{data_type}_records_filtered", 0)
                if filtered > 0:
                    self.metrics["records_filtered"].labels(
                        contract_id=contract_id,
                        data_type=data_type,
                        reason="validation"
                    ).inc(filtered)
                
                files = stats.get("contracts", {}).get(contract_id, {}).get(f"{data_type}_files_processed", 0)
                if files > 0:
                    self.metrics["files_processed"].labels(contract_id=contract_id, data_type=data_type).inc(files)
        
        # Update error counts
        errors = stats.get("errors", 0)
        if errors > 0:
            self.metrics["processing_errors"].labels(component="etl", error_type="general").inc(errors)
        
        # Update ETL info
        self.metrics["etl_info"].info({
            "version": stats.get("version", "unknown"),
            "start_time": str(stats.get("start_time", 0)),
            "uptime_seconds": str(int(time.time() - stats.get("start_time", time.time()))),
            "mode": stats.get("mode", "unknown")
        })
    
    def update_database_metrics(self, db_stats: Any) -> None:
        """
        Update database metrics.
        
        Args:
            db_stats: Database statistics object or dict
        """
        # Check if db_stats is not None
        if not db_stats:
            logger.warning("Received empty database stats for Prometheus update.")
            return

        # Use direct attribute access if it's an object, fall back to get if it might be a dict
        reads = getattr(db_stats, 'reads', db_stats.get("reads", 0) if isinstance(db_stats, dict) else 0)
        writes = getattr(db_stats, 'writes', db_stats.get("writes", 0) if isinstance(db_stats, dict) else 0)
        read_time_ms = getattr(db_stats, 'read_time_ms', db_stats.get("read_time_ms", 0) if isinstance(db_stats, dict) else 0)
        write_time_ms = getattr(db_stats, 'write_time_ms', db_stats.get("write_time_ms", 0) if isinstance(db_stats, dict) else 0)
        connections = getattr(db_stats, 'active_connections', db_stats.get("active_connections", 0) if isinstance(db_stats, dict) else 0)

        # Update query counts
        if reads > 0:
            self.metrics["db_queries"].labels(operation_type="read").inc(reads)
        
        if writes > 0:
            self.metrics["db_queries"].labels(operation_type="write").inc(writes)
        
        # Update query durations
        read_time = read_time_ms / 1000  # Convert to seconds
        write_time = write_time_ms / 1000
        
        if reads > 0 and read_time > 0:
            self.metrics["db_query_duration"].labels(operation_type="read").observe(read_time / reads)
        
        if writes > 0 and write_time > 0:
            self.metrics["db_query_duration"].labels(operation_type="write").observe(write_time / writes)
        
        # Update connection count
        self.metrics["db_connections"].set(connections)
    
    def update_memory_metrics(self) -> None:
        """Update memory metrics."""
        # Get memory manager
        memory_manager = get_memory_manager()
        
        if memory_manager:
            # Get memory stats
            memory_stats = memory_manager.get_memory_stats()
            
            # Update memory usage
            self.metrics["memory_usage"].set(memory_stats["rss"])
            
            # Update GC collections
            collections = memory_stats.get("gc_stats", {}).get("counts", [0, 0, 0])
            for i, count in enumerate(collections):
                self.metrics["gc_collections"].labels(generation=i).inc(count)
    
    def update_cache_metrics(self, cache_stats: Dict[str, Any]) -> None:
        """
        Update cache metrics.
        
        Args:
            cache_stats: Cache statistics
        """
        # Update hits and misses
        hits = cache_stats.get("hits", 0)
        misses = cache_stats.get("misses", 0)
        
        if hits > 0:
            self.metrics["cache_hits"].labels(cache_type="redis").inc(hits)
        
        if misses > 0:
            self.metrics["cache_misses"].labels(cache_type="redis").inc(misses)
    
    def update_disk_metrics(self, disk_stats: Dict[str, Any]) -> None:
        """
        Update disk metrics.
        
        Args:
            disk_stats: Disk statistics
        """
        # Update disk usage
        for path, stats in disk_stats.items():
            self.metrics["disk_usage"].labels(path=path, type="total").set(stats.get("total", 0))
            self.metrics["disk_usage"].labels(path=path, type="used").set(stats.get("used", 0))
            self.metrics["disk_usage"].labels(path=path, type="free").set(stats.get("free", 0))
    
    def update_all_metrics(self, stats: Dict[str, Any]) -> None:
        """
        Update all metrics.
        
        Args:
            stats: All system statistics
        """
        # Update ETL metrics
        self.update_etl_metrics(stats.get("etl", {}))
        
        # Update database metrics
        self.update_database_metrics(stats.get("database", {}))
        
        # Update memory metrics
        self.update_memory_metrics()
        
        # Update cache metrics
        self.update_cache_metrics(stats.get("cache", {}))
        
        # Update disk metrics
        self.update_disk_metrics(stats.get("disk", {}))