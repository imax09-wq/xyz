#!/usr/bin/env python3
"""
Memory management utilities for the ETL pipeline to ensure stable 24/7 operation.
"""

import gc
import os
import time
import logging
import threading
import psutil
from typing import Dict, Any, Optional, Callable

logger = logging.getLogger("SierraChartETL.memory")

class MemoryManager:
    """
    Monitors and manages memory usage to prevent leaks and OOM conditions
    during long-running operations.
    """
    
    def __init__(self, 
                 warning_threshold_mb: int = 1000,
                 critical_threshold_mb: int = 1800,
                 check_interval_seconds: int = 300,
                 auto_collect: bool = True,
                 on_warning: Optional[Callable] = None,
                 on_critical: Optional[Callable] = None):
        """
        Initialize the memory manager.
        
        Args:
            warning_threshold_mb: Memory usage threshold for warnings (MB)
            critical_threshold_mb: Memory usage threshold for critical actions (MB)
            check_interval_seconds: How often to check memory usage
            auto_collect: Whether to automatically run garbage collection
            on_warning: Optional callback for warning threshold
            on_critical: Optional callback for critical threshold
        """
        self.warning_threshold = warning_threshold_mb * 1024 * 1024  # Convert to bytes
        self.critical_threshold = critical_threshold_mb * 1024 * 1024
        self.check_interval = check_interval_seconds
        self.auto_collect = auto_collect
        self.on_warning = on_warning
        self.on_critical = on_critical
        
        self.process = psutil.Process(os.getpid())
        self.running = False
        self.monitor_thread = None
        self.stats = {
            "collections_triggered": 0,
            "peak_memory_usage": 0,
            "last_check_time": 0,
            "last_memory_usage": 0,
            "start_time": time.time()
        }
    
    def start_monitoring(self) -> None:
        """Start the memory monitoring thread."""
        if self.running:
            logger.warning("Memory monitoring already running")
            return
        
        self.running = True
        self.monitor_thread = threading.Thread(
            target=self._monitoring_loop,
            daemon=True,
            name="MemoryMonitor"
        )
        self.monitor_thread.start()
        logger.info("Memory monitoring started")
    
    def stop_monitoring(self) -> None:
        """Stop the memory monitoring thread."""
        if not self.running:
            return
        
        self.running = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=2.0)
        logger.info("Memory monitoring stopped")
    
    def _monitoring_loop(self) -> None:
        """Main monitoring loop that checks memory usage periodically."""
        while self.running:
            try:
                self.check_memory()
                time.sleep(self.check_interval)
            except Exception as e:
                logger.error(f"Error in memory monitoring: {str(e)}")
                time.sleep(max(self.check_interval, 60))  # Longer sleep on error
    
    def check_memory(self) -> Dict[str, Any]:
        """
        Check current memory usage and take appropriate actions.
        
        Returns:
            Dictionary with memory statistics
        """
        # Get current memory usage
        try:
            self.process.memory_info()  # Refresh stats
            memory_info = self.process.memory_info()
            memory_usage = memory_info.rss  # Resident Set Size
            
            # Update stats
            self.stats["last_memory_usage"] = memory_usage
            self.stats["peak_memory_usage"] = max(self.stats["peak_memory_usage"], memory_usage)
            self.stats["last_check_time"] = time.time()
            
            # Log current usage
            memory_mb = memory_usage / (1024 * 1024)
            logger.debug(f"Current memory usage: {memory_mb:.2f} MB")
            
            # Check thresholds
            if memory_usage > self.critical_threshold:
                logger.warning(f"CRITICAL memory usage: {memory_mb:.2f} MB exceeded threshold {self.critical_threshold/(1024*1024):.2f} MB")
                
                # Force aggressive garbage collection
                if self.auto_collect:
                    collected = self._force_garbage_collection(aggressive=True)
                    logger.info(f"Forced aggressive garbage collection, collected {collected} objects")
                    self.stats["collections_triggered"] += 1
                
                # Call critical callback if provided
                if self.on_critical:
                    try:
                        self.on_critical(memory_usage)
                    except Exception as e:
                        logger.error(f"Error in critical memory callback: {str(e)}")
            
            elif memory_usage > self.warning_threshold:
                logger.info(f"HIGH memory usage: {memory_mb:.2f} MB exceeded warning threshold {self.warning_threshold/(1024*1024):.2f} MB")
                
                # Run standard garbage collection
                if self.auto_collect:
                    collected = self._force_garbage_collection(aggressive=False)
                    logger.info(f"Triggered garbage collection, collected {collected} objects")
                    self.stats["collections_triggered"] += 1
                
                # Call warning callback if provided
                if self.on_warning:
                    try:
                        self.on_warning(memory_usage)
                    except Exception as e:
                        logger.error(f"Error in warning memory callback: {str(e)}")
            
            # Return current stats
            return {
                "memory_usage": memory_usage,
                "memory_usage_mb": memory_mb,
                "peak_memory_mb": self.stats["peak_memory_usage"] / (1024 * 1024),
                "collections_triggered": self.stats["collections_triggered"]
            }
        
        except Exception as e:
            logger.error(f"Error checking memory usage: {str(e)}")
            return {"error": str(e)}
    
    def _force_garbage_collection(self, aggressive: bool = False) -> int:
        """
        Force Python's garbage collector to run.
        
        Args:
            aggressive: Whether to use aggressive collection
            
        Returns:
            Number of objects collected
        """
        # Get count before collection
        counts_before = gc.get_count()
        
        # Disable automatic collection during manual collection
        was_enabled = gc.isenabled()
        if was_enabled:
            gc.disable()
        
        try:
            if aggressive:
                # Run collection on all generations
                collected = gc.collect(2)
            else:
                # Standard collection
                collected = gc.collect()
        finally:
            # Restore previous automatic collection state
            if was_enabled:
                gc.enable()
        
        return collected
    
    def get_memory_stats(self) -> Dict[str, Any]:
        """Get detailed memory statistics."""
        memory_info = self.process.memory_info()
        virtual_memory = psutil.virtual_memory()
        
        # Calculate percentage used by this process
        process_percent = (memory_info.rss / virtual_memory.total) * 100
        
        return {
            "rss": memory_info.rss,
            "vms": memory_info.vms,
            "shared": getattr(memory_info, 'shared', 0),
            "peak_rss": self.stats["peak_memory_usage"],
            "collections_triggered": self.stats["collections_triggered"],
            "process_memory_percent": process_percent,
            "system_memory_used_percent": virtual_memory.percent,
            "system_memory_available_mb": virtual_memory.available / (1024 * 1024),
            "gc_stats": {
                "garbage": len(gc.garbage),
                "thresholds": gc.get_threshold(),
                "counts": gc.get_count()
            }
        }

# Singleton instance for global use
memory_manager = None

def initialize_memory_manager(warning_threshold_mb: int = 1000,
                             critical_threshold_mb: int = 1800,
                             check_interval_seconds: int = 300,
                             auto_collect: bool = True,
                             auto_start: bool = True) -> MemoryManager:
    """Initialize the global memory manager."""
    global memory_manager
    
    if memory_manager is None:
        memory_manager = MemoryManager(
            warning_threshold_mb=warning_threshold_mb,
            critical_threshold_mb=critical_threshold_mb,
            check_interval_seconds=check_interval_seconds,
            auto_collect=auto_collect
        )
        
        if auto_start:
            memory_manager.start_monitoring()
    
    return memory_manager

def get_memory_manager() -> MemoryManager:
    """Get the global memory manager instance."""
    if memory_manager is None:
        return initialize_memory_manager()
    return memory_manager