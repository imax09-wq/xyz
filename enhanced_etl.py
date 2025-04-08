#!/usr/bin/env python3
"""
Enhanced ETL pipeline for processing Sierra Chart data files with multi-contract support.

Part of the Sierra Chart ETL Pipeline project.
"""

import asyncio
import logging
import os
import signal
import sys
import time
import re
import json
import gc
from typing import Dict, List, Tuple, Optional, Set, Any
import argparse
from datetime import datetime, timedelta
import traceback
import atexit
import psutil
import multiprocessing

# Import our enhanced modules
from error_handling import ETLError, ErrorCategory, create_error_context, global_exception_handler
from checkpointing import AtomicConfigManager, CheckpointManager
from db_manager import DatabaseManager, DatabaseOperation
from enhanced_parsers import (
    parse_tas_header, parse_tas, transform_tas,
    parse_depth_header, parse_depth, transform_depth,
    filter_and_transform_tas, filter_and_transform_depth,
    ParserContext, validate_tas_data, validate_depth_data,
    validate_file
)
from data_validator import DataValidator, DataQualityIssue, ValidationResult
from memory_manager import initialize_memory_manager, get_memory_manager
from hot_cold_storage import StoragePartitionManager
from query_cache import QueryCache
from prometheus_exporter import PrometheusExporter
from realtime_service import RealTimeService, TradeUpdate, OrderBookUpdate

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("etl_pipeline.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("SierraChartETL")

# Global state
running = True
stats = {
    "tas_files_processed": 0,
    "depth_files_processed": 0,
    "tas_records_processed": 0,
    "depth_records_processed": 0,
    "errors": 0,
    "start_time": time.time(),
    "last_checkpoint_time": 0,
    "last_maintenance_time": 0,
    "records_filtered": 0,
    "contracts": {}  # Per-contract stats
}

# Signal handlers for graceful shutdown
def signal_handler(sig, frame):
    global running
    logger.info(f"Received signal {sig}, shutting down gracefully...")
    running = False

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

class EnhancedETLPipeline:
    """
    Enhanced ETL Pipeline for Sierra Chart data with multi-contract support.
    This class orchestrates the entire ETL process with robust error handling.
    """
    
    def __init__(self, config_path: str):
        self.config_manager = AtomicConfigManager(config_path)
        self.config = self.config_manager.get_config()
        self.checkpoint_manager = CheckpointManager(self.config_manager)
        
        # Initialize memory manager
        memory_config = self.config.get("memory", {})
        self.memory_manager = initialize_memory_manager(
            warning_threshold_mb=memory_config.get("warning_threshold_mb", 1000),
            critical_threshold_mb=memory_config.get("critical_threshold_mb", 1800),
            check_interval_seconds=memory_config.get("check_interval_seconds", 300),
            auto_collect=memory_config.get("auto_collect", True),
            auto_start=True
        )
        
        # Initialize database manager
        db_config = self.config.get("database", {})
        hot_db_path = self.config.get("db_path")
        self.db_manager = DatabaseManager(
            hot_db_path, 
            max_connections=db_config.get("max_connections", 10)
        )
        
        # Initialize storage partition manager if enabled
        storage_config = self.config.get("storage", {})
        if storage_config.get("enable_partitioning", False):
            cold_db_path = storage_config.get("cold_db_path", hot_db_path + '.cold')
            archive_dir = storage_config.get("archive_dir", os.path.join(os.path.dirname(hot_db_path), 'archives'))
            cold_threshold_days = storage_config.get("cold_threshold_days", 30)
            
            self.storage_manager = StoragePartitionManager(
                hot_db_path=hot_db_path,
                cold_db_path=cold_db_path,
                archive_dir=archive_dir,
                cold_threshold_days=cold_threshold_days
            )
            self.storage_manager.initialize_storage()
        else:
            self.storage_manager = None
        
        # Initialize query cache if enabled
        cache_config = self.config.get("cache", {})
        if cache_config.get("enable_redis", False):
            self.query_cache = QueryCache(
                redis_host=cache_config.get("redis_host", "localhost"),
                redis_port=cache_config.get("redis_port", 6379),
                redis_db=cache_config.get("redis_db", 0),
                redis_password=cache_config.get("redis_password"),
                default_ttl=cache_config.get("default_ttl", 60),
                namespace=cache_config.get("namespace", 'sietl:')
            )
        else:
            self.query_cache = None
        
        # Initialize Prometheus exporter if enabled
        monitoring_config = self.config.get("monitoring", {})
        if monitoring_config.get("enable_prometheus", False):
            self.prometheus_exporter = PrometheusExporter(
                port=monitoring_config.get("prometheus_port", 9090)
            )
            self.prometheus_exporter.start_server()
        else:
            self.prometheus_exporter = None
        
        # Initialize real-time service if enabled
        realtime_config = self.config.get("realtime", {})
        if realtime_config.get("enable_websockets", False):
            self.realtime_service = RealTimeService(self.db_manager)
            self.realtime_task = None
        else:
            self.realtime_service = None
        
        # Set up file paths
        self.sc_root = self.config.get("sc_root", "C:/SierraChart")
        self.data_dir = os.path.join(self.sc_root, "Data")
        self.depth_dir = os.path.join(self.data_dir, "MarketDepthData")
        
        # Runtime settings
        self.sleep_interval = self.config.get("sleep_int", 1.0)
        
        # Performance tuning
        self.batch_size = self.config.get("batch_size", 5000)
        
        # Maintenance settings
        self.maintenance_interval = self.config.get("maintenance_interval", 86400)  # Default: once per day
        self.backup_interval = self.config.get("backup_interval", 86400 * 7)  # Default: once per week
        
        # Health check
        self.health_check_interval = self.config.get("health_check_interval", 300)  # Default: every 5 minutes
        
        # Initialize last timestamps
        self.last_maintenance = time.time()
        self.last_backup = time.time()
        self.last_health_check = time.time()
        
        # Validation config
        self.validation_config = self.config.get("validation", {})
        
        # Error handling config
        self.error_handling_config = self.config.get("error_handling", {})
        
        # Initialize known problem records
        self.known_problem_records = {}
        for contract_id, record_list in self.error_handling_config.get("known_problem_records", {}).items():
            self.known_problem_records[contract_id] = set(record_list)
        
        # Multi-contract processing
        self.multiprocessing_enabled = self.config.get("multiprocessing", {}).get("enabled", False)
        self.max_workers = self.config.get("multiprocessing", {}).get("max_workers", multiprocessing.cpu_count())
        
        # Create directories if they don't exist
        os.makedirs(self.data_dir, exist_ok=True)
        os.makedirs(self.depth_dir, exist_ok=True)
        
        # Initialize contract stats
        for contract_id in self.config["contracts"].keys():
            self._init_contract_stats(contract_id)
        
        # Register cleanup handler
        atexit.register(self.cleanup)
    
    def _init_contract_stats(self, contract_id: str) -> None:
        """Initialize statistics for a contract"""
        if contract_id not in stats["contracts"]:
            stats["contracts"][contract_id] = {
                "tas_records_processed": 0,
                "depth_records_processed": 0,
                "tas_records_filtered": 0,
                "depth_records_filtered": 0,
                "tas_files_processed": 0,
                "depth_files_processed": 0,
                "errors": 0,
                "last_tas_timestamp": None,
                "last_depth_timestamp": None
            }
    
    def cleanup(self):
        """Clean up resources when the ETL pipeline is shut down"""
        logger.info("Cleaning up resources...")
        self.db_manager.close()
        self.memory_manager.stop_monitoring()
        logger.info("Cleanup complete")
    
    async def etl_tas(self, contract_id: str, checkpoint: int, price_adj: float, loop_mode: bool) -> Tuple[str, int]:
        """
        Process time and sales data for a contract with robust error handling
        
        Args:
            contract_id: Contract identifier
            checkpoint: Starting record offset
            price_adj: Price adjustment multiplier
            loop_mode: If True, continuously poll for new data
            
        Returns:
            Tuple of contract_id and new checkpoint
        """
        file_path = os.path.join(self.data_dir, f"{contract_id}.scid")
        
        try:
            # Validate file exists
            validate_file(
                file_path=file_path,
                component="ETLPipeline",
                operation="etl_tas",
                contract_id=contract_id
            )
            
            # Process in a loop or once
            while running:
                try:
                    with open(file_path, "rb") as fd:
                        # Parse header
                        parse_tas_header(fd)
                        
                        # Create parsing context with validation config
                        context = ParserContext(
                            contract_id=contract_id,
                            file_path=file_path,
                            checkpoint=checkpoint,
                            validation_config=self.validation_config
                        )
                        
                        # Add known problem records
                        if contract_id in self.known_problem_records:
                            context.known_problem_records = self.known_problem_records[contract_id]
                        
                        # Parse records
                        parsed = parse_tas(fd, checkpoint, context)
                        record_count = len(parsed)
                        
                        if record_count:
                            # Detect if this batch contains a known problematic record
                            contains_problem_record = False
                            if contract_id in self.known_problem_records:
                                for problem_pos in self.known_problem_records[contract_id]:
                                    if checkpoint <= problem_pos < checkpoint + record_count:
                                        contains_problem_record = True
                                        break
                            
                            # For problematic batches, use special handling
                            if contains_problem_record:
                                logger.info(f"Batch contains known problematic record(s). Using special handling.")
                                
                                # Use filter and transform for problem records
                                transformed = filter_and_transform_tas(parsed, context, price_adj)
                                
                                # Load into database (in batches for large datasets)
                                total_loaded = 0
                                for i in range(0, len(transformed), self.batch_size):
                                    batch = transformed[i:i + self.batch_size]
                                    records_loaded = self.db_manager.load_tas_data(contract_id, batch)
                                    total_loaded += records_loaded
                                
                                # Track filtering stats
                                filtered_count = record_count - len(transformed)
                                stats["records_filtered"] += filtered_count
                                stats["contracts"][contract_id]["tas_records_filtered"] += filtered_count
                                
                                # Update checkpoint
                                new_checkpoint = checkpoint + record_count
                                self.checkpoint_manager.update_tas_checkpoint(contract_id, new_checkpoint)
                                checkpoint = new_checkpoint
                                
                                # Update stats
                                stats["tas_files_processed"] += 1
                                stats["tas_records_processed"] += total_loaded
                                stats["last_checkpoint_time"] = time.time()
                                stats["contracts"][contract_id]["tas_files_processed"] += 1
                                stats["contracts"][contract_id]["tas_records_processed"] += total_loaded
                                
                                # Update last timestamp for the contract
                                if len(transformed) > 0:
                                    last_ts = transformed[-1][0]  # Timestamp is the first field
                                    stats["contracts"][contract_id]["last_tas_timestamp"] = last_ts
                                
                                logger.info(
                                    f"[TAS] {contract_id}: Processed {total_loaded} valid records "
                                    f"(filtered {filtered_count} problematic records) "
                                    f"from offset {context.checkpoint}. New checkpoint: {new_checkpoint}"
                                )
                                
                                # Publish to real-time service if enabled
                                if self.realtime_service and transformed:
                                    await self._publish_trades(contract_id, transformed)
                            
                            # Normal validation for regular batches
                            elif validate_tas_data(parsed, context):
                                # Transform records
                                transformed = transform_tas(parsed, price_adj)
                                
                                # Load into database (in batches for large datasets)
                                total_loaded = 0
                                for i in range(0, len(transformed), self.batch_size):
                                    batch = transformed[i:i + self.batch_size]
                                    records_loaded = self.db_manager.load_tas_data(contract_id, batch)
                                    total_loaded += records_loaded
                                
                                # Update checkpoint
                                new_checkpoint = checkpoint + record_count
                                self.checkpoint_manager.update_tas_checkpoint(contract_id, new_checkpoint)
                                checkpoint = new_checkpoint
                                
                                # Update stats
                                stats["tas_files_processed"] += 1
                                stats["tas_records_processed"] += total_loaded
                                stats["last_checkpoint_time"] = time.time()
                                stats["contracts"][contract_id]["tas_files_processed"] += 1
                                stats["contracts"][contract_id]["tas_records_processed"] += total_loaded
                                
                                # Update last timestamp for the contract
                                if transformed:
                                    last_ts = transformed[-1][0]  # Timestamp is the first field
                                    stats["contracts"][contract_id]["last_tas_timestamp"] = last_ts
                                
                                logger.info(
                                    f"[TAS] {contract_id}: Processed {record_count} records from offset {context.checkpoint}. "
                                    f"New checkpoint: {new_checkpoint}"
                                )
                                
                                # Publish to real-time service if enabled
                                if self.realtime_service and transformed:
                                    await self._publish_trades(contract_id, transformed)
                            else:
                                logger.error(f"Data validation failed for {contract_id}, skipping batch")
                                
                                # Even for failed batches, we need a strategy to move forward
                                # This is a last resort for batches that have irreparable issues
                                if self.error_handling_config.get("skip_invalid_batches", False):
                                    logger.warning(f"Skip invalid batches is enabled. Advancing checkpoint.")
                                    new_checkpoint = checkpoint + min(record_count, 1000)  # Skip at most 1000 records
                                    self.checkpoint_manager.update_tas_checkpoint(contract_id, new_checkpoint)
                                    checkpoint = new_checkpoint
                                    logger.info(f"Advanced checkpoint to {new_checkpoint} to skip problematic data")
                                
                                stats["errors"] += 1
                                stats["contracts"][contract_id]["errors"] += 1
                        else:
                            logger.info(f"[TAS] {contract_id}: No new records found.")
                
                except (IOError, OSError) as e:
                    logger.error(f"I/O error processing TAS for {contract_id}: {str(e)}")
                    stats["errors"] += 1
                    stats["contracts"][contract_id]["errors"] += 1
                    # Wait a bit before retry to avoid hammering the file system
                    await asyncio.sleep(self.sleep_interval * 2)
                
                # Break if not in loop mode
                if not loop_mode:
                    break
                
                # Wait before checking for new data
                await asyncio.sleep(self.sleep_interval)
            
            return (contract_id, checkpoint)
        
        except ETLError as e:
            logger.error(f"Error processing TAS for {contract_id}: {str(e)}")
            stats["errors"] += 1
            stats["contracts"][contract_id]["errors"] += 1
            return (contract_id, checkpoint)
        
        except Exception as e:
            logger.error(
                f"Unexpected error processing TAS for {contract_id}: {str(e)}\n{traceback.format_exc()}"
            )
            stats["errors"] += 1
            stats["contracts"][contract_id]["errors"] += 1
            return (contract_id, checkpoint)
    
    async def _publish_trades(self, contract_id: str, trades: List[Tuple]) -> None:
        """
        Publish trades to real-time service
        
        Args:
            contract_id: Contract identifier
            trades: List of trade records
        """
        if not self.realtime_service:
            return
        
        for trade in trades:
            timestamp, price, quantity, side = trade
            await self.realtime_service.publish_trade(
                contract_id=contract_id,
                timestamp=timestamp,
                price=price,
                quantity=quantity,
                side=side
            )
    
    async def etl_depth(self, contract_id: str, loop_mode: bool) -> None:
        """
        Process market depth data for a contract with enhanced filename matching
        
        Args:
            contract_id: Contract identifier
            loop_mode: If True, continuously poll for new data
        """
        try:
            checkpoint_date, checkpoint_rec = self.checkpoint_manager.get_depth_checkpoint(contract_id)
            price_adj = self.config["contracts"][contract_id]["price_adj"]
            
            # Get list of depth files for this contract
            depth_files = self._get_depth_files(contract_id, checkpoint_date)
            
            if not depth_files:
                logger.warning(f"No depth files found for {contract_id} on or after {checkpoint_date}")
                return
            
            for idx, (file_date, file_path) in enumerate(depth_files):
                try:
                    is_latest_file = (idx == len(depth_files) - 1)
                    
                    # For all but the latest file, just process once
                    # For the latest file, process in a loop if loop_mode is True
                    should_loop = loop_mode and is_latest_file
                    
                    # Determine the starting checkpoint
                    start_checkpoint = checkpoint_rec if file_date == checkpoint_date else 0
                    
                    # Process the file
                    new_checkpoint = await self._process_depth_file(
                        contract_id=contract_id,
                        file_path=file_path,
                        file_date=file_date,
                        checkpoint=start_checkpoint,
                        price_adj=price_adj,
                        loop_mode=should_loop
                    )
                    
                    # Update checkpoints
                    self.checkpoint_manager.update_depth_checkpoint(
                        contract_id=contract_id,
                        date=file_date,
                        rec=new_checkpoint
                    )
                    
                    # Update for next iteration
                    checkpoint_date = file_date
                    checkpoint_rec = new_checkpoint
                
                except ETLError as e:
                    logger.error(f"Error processing depth file {file_path}: {str(e)}")
                    stats["errors"] += 1
                    stats["contracts"][contract_id]["errors"] += 1
                    continue
                
                except Exception as e:
                    logger.error(
                        f"Unexpected error processing depth file {file_path}: {str(e)}\n{traceback.format_exc()}"
                    )
                    stats["errors"] += 1
                    stats["contracts"][contract_id]["errors"] += 1
                    continue
        
        except ETLError as e:
            logger.error(f"Error in depth ETL for {contract_id}: {str(e)}")
            stats["errors"] += 1
            stats["contracts"][contract_id]["errors"] += 1
        
        except Exception as e:
            logger.error(
                f"Unexpected error in depth ETL for {contract_id}: {str(e)}\n{traceback.format_exc()}"
            )
            stats["errors"] += 1
            stats["contracts"][contract_id]["errors"] += 1
    
    def _get_depth_files(self, contract_id: str, start_date: str) -> List[Tuple[str, str]]:
        """
        Get a list of depth files for a contract, with enhanced filename matching
        
        Args:
            contract_id: Contract identifier
            start_date: Minimum date to include
            
        Returns:
            List of (date, file_path) tuples
        """
        if not os.path.exists(self.depth_dir):
            logger.error(f"Depth directory not found: {self.depth_dir}")
            return []
        
        depth_files = []
        
        # Extract base symbol and exchange info from contract_id
        base_symbol = None
        exchange = None
        month_year = None
        
        # Pattern: ESM25_FUT_CME
        symbol_pattern = re.compile(r"([A-Z]+)([A-Z0-9]+)_([A-Z]+)_([A-Z]+)")
        match = symbol_pattern.match(contract_id)
        if match:
            base_symbol = match.group(1)  # e.g., "ES"
            month_year = match.group(2)   # e.g., "M25"
            contract_type = match.group(3)  # e.g., "FUT"
            exchange = match.group(4)     # e.g., "CME"
            
            logger.debug(f"Parsed contract '{contract_id}': symbol='{base_symbol}', "
                        f"month_year='{month_year}', type='{contract_type}', exchange='{exchange}'")
        
        try:
            # Define patterns for matching depth files
            patterns = [
                # Format 1: "ESM25_FUT_CME.2025-04-04.depth"
                (r"^(.+)\.(\d{4}-\d{2}-\d{2})\.depth$", 
                 lambda m: (m.group(1), m.group(2))),
                
                # Format 2: "ESH5.CME.2025-01-12.depth"
                (r"^([A-Z]+[A-Z0-9]+)\.([A-Z]+)\.(\d{4}-\d{2}-\d{2})\.depth$", 
                 lambda m: (f"{m.group(1)}_{contract_type if contract_type else 'FUT'}_{m.group(2)}", m.group(3))),
                
                # Format 3: "ES_H5.depth.2025-01-12" (hypothetical alternative format)
                (r"^([A-Z]+)_([A-Za-z0-9]+)\.depth\.(\d{4}-\d{2}-\d{2})$", 
                 lambda m: (f"{m.group(1)}{m.group(2)}_{contract_type if contract_type else 'FUT'}_{exchange if exchange else 'CME'}", m.group(3))),
                
                # Format 4: Handle ".dept" extension (typo in some files)
                (r"^(.+)\.(\d{4}-\d{2}-\d{2})\.dept$", 
                 lambda m: (m.group(1), m.group(2))),
                
                # Format 5: Handle other variations
                (r"^([A-Z]+[A-Z0-9]+)\.([A-Z]+)\.(\d{4}-\d{2}-\d{2})\.dept$",
                 lambda m: (f"{m.group(1)}_{contract_type if contract_type else 'FUT'}_{m.group(2)}", m.group(3))),
            ]
            
            # Iterate through all files in the depth directory
            for filename in os.listdir(self.depth_dir):
                # Skip non-depth files, but allow both .depth and .dept extensions
                if not filename.lower().endswith((".depth", ".dept")):
                    continue
                    
                # Try to match the filename against our patterns
                match_found = False
                file_contract = None
                file_date = None
                
                for pattern, extractor in patterns:
                    match = re.match(pattern, filename)
                    if match:
                        try:
                            file_contract, file_date = extractor(match)
                            match_found = True
                            break
                        except Exception as e:
                            logger.debug(f"Error extracting from pattern match: {str(e)}")
                
                if not match_found:
                    logger.warning(f"Unexpected depth filename format: {filename}")
                    continue
                
                # Check if this file is for our contract
                is_match = False
                
                # Direct match
                if file_contract == contract_id:
                    is_match = True
                # Check if base symbol and exchange match
                elif base_symbol and exchange:
                    if file_contract.startswith(base_symbol) and exchange in file_contract:
                        logger.info(f"Found related contract file: {filename} matches {base_symbol}/{exchange}")
                        is_match = True
                # Check just base symbol as last resort
                elif base_symbol and file_contract.startswith(base_symbol):
                    logger.info(f"Found potential match based on symbol: {filename} contains {base_symbol}")
                    is_match = True
                
                if is_match and (not start_date or file_date >= start_date):
                    file_path = os.path.join(self.depth_dir, filename)
                    depth_files.append((file_date, file_path))
        
        except Exception as e:
            logger.error(f"Error listing depth files: {str(e)}")
        
        # Sort by date
        depth_files.sort()
        
        return depth_files
    
    async def _process_depth_file(
        self,
        contract_id: str,
        file_path: str,
        file_date: str,
        checkpoint: int,
        price_adj: float,
        loop_mode: bool
    ) -> int:
        """
        Process a single depth file with enhanced error handling
        
        Args:
            contract_id: Contract identifier
            file_path: Path to the depth file
            file_date: Date string from the filename
            checkpoint: Starting record offset
            price_adj: Price adjustment multiplier
            loop_mode: If True, continuously poll for new data
            
        Returns:
            New checkpoint value
        """
        try:
            # Validate file exists
            validate_file(
                file_path=file_path,
                component="ETLPipeline",
                operation="process_depth_file",
                contract_id=contract_id
            )
            
            original_checkpoint = checkpoint
            
            # Process in a loop or once
            while running:
                try:
                    with open(file_path, "rb") as fd:
                        # Parse header
                        parse_depth_header(fd)
                        
                        # Create parsing context with validation config
                        context = ParserContext(
                            contract_id=contract_id,
                            file_path=file_path,
                            checkpoint=checkpoint,
                            validation_config=self.validation_config
                        )
                        
                        # Parse records
                        parsed = parse_depth(fd, checkpoint, context)
                        record_count = len(parsed)
                        
                        if record_count:
                            # Validate and process data with enhanced error handling
                            if validate_depth_data(parsed, context):
                                # Transform and filter records if needed
                                if self.validation_config.get("filter_problematic_records", True):
                                    # Use enhanced filtering and transformation
                                    transformed = filter_and_transform_depth(parsed, context, price_adj)
                                else:
                                    # Traditional transform without filtering
                                    transformed = transform_depth(parsed, price_adj)
                                
                                # Load into database (in batches for large datasets)
                                total_loaded = 0
                                for i in range(0, len(transformed), self.batch_size):
                                    batch = transformed[i:i + self.batch_size]
                                    records_loaded = self.db_manager.load_depth_data(contract_id, batch)
                                    total_loaded += records_loaded
                                
                                # Update checkpoint
                                new_checkpoint = checkpoint + record_count
                                checkpoint = new_checkpoint
                                
                                # Update stats
                                stats["depth_files_processed"] += 1
                                stats["depth_records_processed"] += total_loaded
                                stats["last_checkpoint_time"] = time.time()
                                stats["contracts"][contract_id]["depth_files_processed"] += 1
                                stats["contracts"][contract_id]["depth_records_processed"] += total_loaded
                                
                                # Update last timestamp for the contract
                                if transformed:
                                    last_ts = transformed[-1][0]  # Timestamp is the first field
                                    stats["contracts"][contract_id]["last_depth_timestamp"] = last_ts
                                
                                # Track filtered records if applicable
                                if len(transformed) < record_count:
                                    filtered_count = record_count - len(transformed)
                                    stats["records_filtered"] += filtered_count
                                    stats["contracts"][contract_id]["depth_records_filtered"] += filtered_count
                                    logger.info(
                                        f"[Depth] {contract_id} (file {file_date}): Processed {total_loaded} records "
                                        f"(filtered {filtered_count} problematic records) "
                                        f"from offset {context.checkpoint}. New checkpoint: {new_checkpoint}"
                                    )
                                else:
                                    logger.info(
                                        f"[Depth] {contract_id} (file {file_date}): Processed {record_count} records "
                                        f"from offset {context.checkpoint}. New checkpoint: {new_checkpoint}"
                                    )
                                
                                # Process real-time order book updates if enabled
                                if self.realtime_service and transformed:
                                    await self._process_orderbook_updates(contract_id, transformed)
                            else:
                                logger.error(f"Data validation failed for {file_path}, skipping batch")
                                
                                # Even for failed batches, we need a strategy to move forward
                                if self.error_handling_config.get("skip_invalid_batches", False):
                                    logger.warning(f"Skip invalid batches is enabled. Advancing checkpoint.")
                                    new_checkpoint = checkpoint + min(record_count, 1000)  # Skip at most 1000 records
                                    checkpoint = new_checkpoint
                                    logger.info(f"Advanced checkpoint to {new_checkpoint} to skip problematic data")
                                
                                stats["errors"] += 1
                                stats["contracts"][contract_id]["errors"] += 1
                        else:
                            logger.info(f"[Depth] {contract_id} (file {file_date}): No new records found.")
                
                except (IOError, OSError) as e:
                    logger.error(f"I/O error processing depth file {file_path}: {str(e)}")
                    stats["errors"] += 1
                    stats["contracts"][contract_id]["errors"] += 1
                    # Wait a bit before retry
                    await asyncio.sleep(self.sleep_interval * 2)
                    continue
                
                # Break if not in loop mode
                if not loop_mode:
                    break
                
                # Wait before checking for new data
                await asyncio.sleep(self.sleep_interval)
            
            return checkpoint
        
        except ETLError as e:
            logger.error(f"Error processing depth file {file_path}: {str(e)}")
            stats["errors"] += 1
            stats["contracts"][contract_id]["errors"] += 1
            return original_checkpoint
        
        except Exception as e:
            logger.error(
                f"Unexpected error processing depth file {file_path}: {str(e)}\n{traceback.format_exc()}"
            )
            stats["errors"] += 1
            stats["contracts"][contract_id]["errors"] += 1
            return original_checkpoint
    
    async def _process_orderbook_updates(self, contract_id: str, depth_records: List[Tuple]) -> None:
        """
        Process depth records to generate order book updates
        
        Args:
            contract_id: Contract identifier
            depth_records: List of depth records
        """
        if not self.realtime_service:
            return
        
        # Track the current order book state
        order_book = {
            "bids": {},  # price -> (quantity, num_orders)
            "asks": {}   # price -> (quantity, num_orders)
        }
        
        # Process each record
        for record in depth_records:
            timestamp, command, flags, num_orders, price, quantity = record
            
            # Process based on command type
            if command == 1:  # Clear book
                order_book["bids"].clear()
                order_book["asks"].clear()
            
            elif command == 2:  # Add bid
                order_book["bids"][price] = (quantity, num_orders)
            
            elif command == 3:  # Add ask
                order_book["asks"][price] = (quantity, num_orders)
            
            elif command == 4:  # Modify bid
                order_book["bids"][price] = (quantity, num_orders)
            
            elif command == 5:  # Modify ask
                order_book["asks"][price] = (quantity, num_orders)
            
            elif command == 6:  # Delete bid
                if price in order_book["bids"]:
                    del order_book["bids"][price]
            
            elif command == 7:  # Delete ask
                if price in order_book["asks"]:
                    del order_book["asks"][price]
        
        # If we have a valid order book, publish an update
        if order_book["bids"] or order_book["asks"]:
            # Sort bids (descending)
            bid_levels = [
                {"price": price, "quantity": qty, "num_orders": orders}
                for price, (qty, orders) in sorted(order_book["bids"].items(), key=lambda x: x[0], reverse=True)
            ]
            
            # Sort asks (ascending)
            ask_levels = [
                {"price": price, "quantity": qty, "num_orders": orders}
                for price, (qty, orders) in sorted(order_book["asks"].items(), key=lambda x: x[0])
            ]
            
            # Use the last timestamp as the order book timestamp
            if depth_records:
                timestamp = depth_records[-1][0]
                
                # Publish order book update
                await self.realtime_service.publish_orderbook(
                    contract_id=contract_id,
                    timestamp=timestamp,
                    bid_levels=bid_levels[:10],  # Limit to top 10 levels
                    ask_levels=ask_levels[:10]   # Limit to top 10 levels
                )
    
    async def perform_maintenance(self) -> None:
        """Perform database maintenance and storage management tasks"""
        current_time = time.time()
        
        # Check if it's time for database maintenance
        if current_time - self.last_maintenance >= self.maintenance_interval:
            logger.info("Starting scheduled database maintenance")
            try:
                self.db_manager.perform_maintenance()
                self.last_maintenance = current_time
                logger.info("Scheduled maintenance completed successfully")
            except Exception as e:
                logger.error(f"Error during database maintenance: {str(e)}")
                stats["errors"] += 1
        
        # Check if it's time for backup
        if current_time - self.last_backup >= self.backup_interval:
            logger.info("Starting scheduled database backup")
            try:
                backup_path = self.db_manager.backup_database()
                self.last_backup = current_time
                logger.info(f"Scheduled backup completed successfully: {backup_path}")
            except Exception as e:
                logger.error(f"Error during database backup: {str(e)}")
                stats["errors"] += 1
        
        # Perform storage maintenance if enabled
        if self.storage_manager:
            storage_config = self.config.get("storage", {})
            
            # Check if it's time to move data to cold storage
            if storage_config.get("auto_move_to_cold", False):
                last_move_time = self.storage_manager.stats["last_move_time"]
                move_interval = storage_config.get("move_interval_hours", 24) * 3600
                
                if current_time - last_move_time >= move_interval:
                    logger.info("Starting data migration to cold storage")
                    
                    for contract_id in self.config["contracts"].keys():
                        # Move TAS data
                        try:
                            result = self.storage_manager.move_to_cold_storage(
                                contract_id=contract_id,
                                data_type="tas"
                            )
                            if result["status"] == "success":
                                logger.info(f"Moved {result['records_moved']} TAS records to cold storage for {contract_id}")
                        except Exception as e:
                            logger.error(f"Error moving TAS data to cold storage for {contract_id}: {str(e)}")
                        
                        # Move depth data
                        try:
                            result = self.storage_manager.move_to_cold_storage(
                                contract_id=contract_id,
                                data_type="depth"
                            )
                            if result["status"] == "success":
                                logger.info(f"Moved {result['records_moved']} depth records to cold storage for {contract_id}")
                        except Exception as e:
                            logger.error(f"Error moving depth data to cold storage for {contract_id}: {str(e)}")
            
            # Check if it's time to archive cold data
            if storage_config.get("auto_archive", False):
                last_archive_time = self.storage_manager.stats["last_archive_time"]
                archive_interval = storage_config.get("archive_interval_days", 30) * 86400
                
                if current_time - last_archive_time >= archive_interval:
                    logger.info("Starting archiving of old data")
                    
                    archive_age = storage_config.get("archive_age_days", 365)
                    
                    for contract_id in self.config["contracts"].keys():
                        # Archive TAS data
                        try:
                            result = self.storage_manager.archive_cold_data(
                                contract_id=contract_id,
                                data_type="tas",
                                age_days=archive_age
                            )
                            if result["status"] == "success":
                                logger.info(
                                    f"Archived {result['records_archived']} TAS records for {contract_id} "
                                    f"to {result['archive_path']}"
                                )
                        except Exception as e:
                            logger.error(f"Error archiving TAS data for {contract_id}: {str(e)}")
                        
                        # Archive depth data
                        try:
                            result = self.storage_manager.archive_cold_data(
                                contract_id=contract_id,
                                data_type="depth",
                                age_days=archive_age
                            )
                            if result["status"] == "success":
                                logger.info(
                                    f"Archived {result['records_archived']} depth records for {contract_id} "
                                    f"to {result['archive_path']}"
                                )
                        except Exception as e:
                            logger.error(f"Error archiving depth data for {contract_id}: {str(e)}")
    
    async def perform_health_check(self) -> None:
        """Perform system health check"""
        current_time = time.time()
        
        # Check if it's time for health check
        if current_time - self.last_health_check >= self.health_check_interval:
            logger.info("Performing system health check")
            
            try:
                # Check database connection
                with self.db_manager.connection_pool.get_connection() as conn:
                    cursor = conn.execute("SELECT 1")
                    result = cursor.fetchone()
                    if result and result[0] == 1:
                        logger.info("Database connection check: OK")
                    else:
                        logger.error("Database connection check: FAILED")
                        stats["errors"] += 1
                
                # Check disk space
                db_dir = os.path.dirname(self.db_manager.db_path)
                if os.name == 'nt':  # Windows
                    import ctypes
                    free_bytes = ctypes.c_ulonglong(0)
                    ctypes.windll.kernel32.GetDiskFreeSpaceExW(
                        ctypes.c_wchar_p(db_dir), None, None, ctypes.pointer(free_bytes)
                    )
                    free_gb = free_bytes.value / (1024 ** 3)
                else:  # Unix
                    import shutil
                    usage = shutil.disk_usage(db_dir)
                    free_gb = usage.free / (1024 ** 3)
                
                if free_gb < 5:  # Less than 5 GB free
                    logger.warning(f"Low disk space: {free_gb:.2f} GB remaining")
                else:
                    logger.info(f"Disk space check: OK ({free_gb:.2f} GB free)")
                
                # Check memory usage
                memory_stats = self.memory_manager.get_memory_stats()
                memory_mb = memory_stats["rss"] / (1024 * 1024)
                memory_percent = memory_stats["process_memory_percent"]
                
                logger.info(f"Memory usage: {memory_mb:.2f} MB ({memory_percent:.1f}% of system memory)")
                
                if memory_percent > 80:
                    logger.warning("High memory usage detected")
                
                # Check ETL stats
                uptime = current_time - stats["start_time"]
                records_per_minute = (stats["tas_records_processed"] + stats["depth_records_processed"]) / (uptime / 60)
                logger.info(
                    f"ETL stats: Processing rate: {records_per_minute:.2f} records/min, "
                    f"Errors: {stats['errors']}"
                )
                
                # Check Redis connection if enabled
                if self.query_cache:
                    try:
                        cache_stats = self.query_cache.get_stats()
                        hit_ratio = cache_stats.get("hit_ratio", 0) * 100
                        logger.info(f"Redis cache: Hit ratio: {hit_ratio:.1f}%, Keys: {cache_stats.get('redis_total_keys', 0)}")
                    except Exception as e:
                        logger.error(f"Redis connection check: FAILED - {str(e)}")
                
                # Check data quality
                if self.error_handling_config.get("run_data_validator_on_health_check", False):
                    logger.info("Running data quality validation checks")
                    validator = DataValidator(self.db_manager.db_path)
                    try:
                        contracts_to_validate = list(self.config["contracts"].keys())
                        validation_results = validator.run_validation(
                            contracts_to_validate, 
                            check_freshness=True
                        )
                        
                        # Log validation issues
                        for result in validation_results:
                            if not result.is_valid:
                                logger.warning(
                                    f"Data validation issue for {result.contract_id} ({result.data_type}): "
                                    f"{len(result.issues)} issues found."
                                )
                    except Exception as e:
                        logger.error(f"Error during data validation: {str(e)}")
                    finally:
                        validator.close()
                
                # Update Prometheus metrics if enabled
                if self.prometheus_exporter:
                    try:
                        # Collect all stats
                        all_stats = {
                            "etl": stats,
                            "database": self.db_manager.connection_pool.stats,
                            "memory": memory_stats,
                            "cache": self.query_cache.get_stats() if self.query_cache else {},
                            "disk": {
                                db_dir: {
                                    "total": usage.total if 'usage' in locals() else 0,
                                    "used": usage.used if 'usage' in locals() else 0,
                                    "free": usage.free if 'usage' in locals() else 0
                                }
                            }
                        }
                        
                        # Update Prometheus metrics
                        self.prometheus_exporter.update_all_metrics(all_stats)
                        logger.info("Updated Prometheus metrics")
                    except Exception as e:
                        logger.error(f"Error updating Prometheus metrics: {str(e)}")
                
                self.last_health_check = current_time
            
            except Exception as e:
                logger.error(f"Error during health check: {str(e)}")
                stats["errors"] += 1
    
    async def _start_realtime_service(self) -> None:
        """Start the real-time service in the background"""
        if not self.realtime_service:
            return
        
        try:
            import uvicorn
            
            # Get config
            realtime_config = self.config.get("realtime", {})
            host = realtime_config.get("host", "0.0.0.0")
            port = realtime_config.get("port", 8001)
            
            # Start Uvicorn server
            config = uvicorn.Config(
                app=self.realtime_service.app,
                host=host,
                port=port,
                log_level="info"
            )
            server = uvicorn.Server(config)
            
            logger.info(f"Starting real-time WebSocket service on {host}:{port}")
            await server.serve()
        
        except Exception as e:
            logger.error(f"Error starting real-time service: {str(e)}")
    
    async def process_contract(self, contract_id: str, contract_config: Dict[str, Any], loop_mode: bool) -> None:
        """
        Process a single contract's data
        
        Args:
            contract_id: Contract identifier
            contract_config: Contract configuration
            loop_mode: If True, continuously poll for new data
        """
        tasks = []
        
        # Process time & sales
        if contract_config.get("tas", False):
            checkpoint = self.checkpoint_manager.get_tas_checkpoint(contract_id)
            price_adj = contract_config["price_adj"]
            
            tasks.append(
                self.etl_tas(contract_id, checkpoint, price_adj, loop_mode)
            )
        
        # Process market depth
        if contract_config.get("depth", False):
            tasks.append(
                self.etl_depth(contract_id, loop_mode)
            )
        
        # Run tasks concurrently
        await asyncio.gather(*tasks)
    
    async def _run_multi_contract(self, loop_mode: bool) -> None:
        """
        Run the ETL pipeline using multiprocessing for multiple contracts
        
        Args:
            loop_mode: If True, continuously poll for new data
        """
        if not self.multiprocessing_enabled:
            # If multiprocessing not enabled, just run normally
            await self._run_single_process(loop_mode)
            return
        
        try:
            from concurrent.futures import ProcessPoolExecutor
            
            # Create a pool with max_workers
            with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
                futures = []
                
                # Submit jobs for each contract
                for contract_id, contract_config in self.config["contracts"].items():
                    # Need to run in a separate process
                    future = executor.submit(
                        run_contract_process,
                        self.config_manager.config_path,
                        contract_id,
                        loop_mode
                    )
                    futures.append(future)
                
                # Wait for all processes to complete
                for future in futures:
                    try:
                        result = future.result()
                        logger.info(f"Contract process completed: {result}")
                    except Exception as e:
                        logger.error(f"Error in contract process: {str(e)}")
        
        except Exception as e:
            logger.error(f"Error in multiprocessing: {str(e)}")
            # Fall back to single process mode
            logger.info("Falling back to single process mode")
            await self._run_single_process(loop_mode)
    
    async def _run_single_process(self, loop_mode: bool) -> None:
        """
        Run the ETL pipeline in a single process
        
        Args:
            loop_mode: If True, continuously poll for new data
        """
        # Start real-time service if enabled
        if self.realtime_service:
            self.realtime_task = asyncio.create_task(self._start_realtime_service())
        
        try:
            while running:
                tasks = []
                
                # Process each enabled contract
                for contract_id, contract_config in self.config["contracts"].items():
                    task = self.process_contract(contract_id, contract_config, loop_mode)
                    tasks.append(task)
                
                # Wait for all contract tasks to complete
                await asyncio.gather(*tasks)
                
                # Perform maintenance and health checks
                await self.perform_maintenance()
                await self.perform_health_check()
                
                # Force garbage collection
                gc.collect()
                
                # Break if not in loop mode
                if not loop_mode:
                    break
                
                # Wait before next processing cycle
                await asyncio.sleep(self.sleep_interval * 10)  # Longer sleep between full cycles
        
        except Exception as e:
            logger.error(f"Unexpected error in ETL pipeline: {str(e)}\n{traceback.format_exc()}")
            stats["errors"] += 1
        
        finally:
            if self.realtime_task:
                self.realtime_task.cancel()
                try:
                    await self.realtime_task
                except asyncio.CancelledError:
                    pass
    
    async def run(self, loop_mode: bool = False) -> None:
        """
        Run the ETL pipeline
        
        Args:
            loop_mode: If True, continuously poll for new data
        """
        logger.info(f"Starting ETL pipeline (loop_mode={loop_mode})")
        stats["start_time"] = time.time()
        
        # Verify checkpoint integrity
        try:
            self.checkpoint_manager.verify_checkpoint_integrity()
        except ETLError as e:
            logger.error(f"Checkpoint integrity check failed: {str(e)}")
            return
        
        # Determine whether to use multiprocessing
        if self.multiprocessing_enabled and len(self.config["contracts"]) > 1:
            await self._run_multi_contract(loop_mode)
        else:
            await self._run_single_process(loop_mode)
        
        # Log completion
        elapsed = time.time() - stats["start_time"]
        logger.info(
            f"ETL pipeline completed. "
            f"Stats: TAS files={stats['tas_files_processed']}, "
            f"Depth files={stats['depth_files_processed']}, "
            f"TAS records={stats['tas_records_processed']}, "
            f"Depth records={stats['depth_records_processed']}, "
            f"Records filtered={stats['records_filtered']}, "
            f"Errors={stats['errors']}, "
            f"Elapsed={elapsed:.2f}s"
        )

def run_contract_process(config_path: str, contract_id: str, loop_mode: bool) -> str:
    """
    Run ETL pipeline for a single contract in a separate process
    
    Args:
        config_path: Path to configuration file
        contract_id: Contract identifier to process
        loop_mode: If True, continuously poll for new data
        
    Returns:
        Result message
    """
    try:
        # Configure logging for this process
        logging.basicConfig(
            level=logging.INFO,
            format=f'%(asctime)s - {contract_id} - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(f"etl_{contract_id}.log"),
                logging.StreamHandler(sys.stdout)
            ]
        )
        
        logger = logging.getLogger(f"SierraChartETL.{contract_id}")
        
        # Create a new config with just this contract
        config_manager = AtomicConfigManager(config_path)
        full_config = config_manager.get_config()
        
        # Create single-contract config
        single_contract_config = full_config.copy()
        single_contract_config["contracts"] = {
            contract_id: full_config["contracts"][contract_id]
        }
        
        # Create temporary config file
        temp_config_path = f"temp_config_{contract_id}.json"
        with open(temp_config_path, 'w') as f:
            json.dump(single_contract_config, f, indent=2)
        
        # Initialize pipeline
        pipeline = EnhancedETLPipeline(temp_config_path)
        
        # Run the contract
        asyncio.run(pipeline.run(loop_mode))
        
        # Clean up
        pipeline.cleanup()
        try:
            os.remove(temp_config_path)
        except:
            pass
        
        return f"Completed processing for {contract_id}"
    
    except Exception as e:
        logger.error(f"Error processing {contract_id}: {str(e)}\n{traceback.format_exc()}")
        return f"Error processing {contract_id}: {str(e)}"

async def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="Sierra Chart ETL Pipeline")
    parser.add_argument(
        "mode", type=int, choices=[0, 1],
        help="0 for one-shot mode, 1 for continuous mode"
    )
    parser.add_argument(
        "--config", type=str, default="./config.json",
        help="Path to configuration file (default: ./config.json)"
    )
    parser.add_argument(
        "--log-level", type=str, choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO", help="Set the logging level"
    )
    parser.add_argument(
        "--contract", type=str, help="Process only this contract (for testing)"
    )
    
    args = parser.parse_args()
    
    # Set log level
    logging.getLogger().setLevel(getattr(logging, args.log_level))
    
    # Create and run the ETL pipeline
    pipeline = EnhancedETLPipeline(args.config)
    
    # If a specific contract was specified, create a modified config
    if args.contract:
        if args.contract in pipeline.config["contracts"]:
            # Create a new config with just this contract
            original_contracts = pipeline.config["contracts"]
            pipeline.config["contracts"] = {
                args.contract: original_contracts[args.contract]
            }
            logger.info(f"Processing only contract: {args.contract}")
        else:
            logger.error(f"Contract {args.contract} not found in configuration")
            return
    
    await pipeline.run(args.mode == 1)

if __name__ == "__main__":
    # Install the global exception handler
    sys.excepthook = global_exception_handler
    
    # Run the main async function
    asyncio.run(main())