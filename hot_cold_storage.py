#!/usr/bin/env python3
"""
Hot/cold storage management for the ETL pipeline.
"""

import logging
import os
import time
import sqlite3
import shutil
import threading
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta
import json

from error_handling import ETLError, ErrorCategory, create_error_context

logger = logging.getLogger("SierraChartETL.storage")

class StoragePartitionManager:
    """
    Manages partitioning of data between hot (recent) and cold (historical) storage.
    This improves performance by keeping the most active data in optimal storage.
    """
    
    def __init__(self, 
                 hot_db_path: str, 
                 cold_db_path: Optional[str] = None,
                 archive_dir: Optional[str] = None,
                 cold_threshold_days: int = 30):
        """
        Initialize the storage partition manager.
        
        Args:
            hot_db_path: Path to the hot database
            cold_db_path: Path to the cold database (if None, cold_db_path = hot_db_path + '.cold')
            archive_dir: Directory for archiving old data
            cold_threshold_days: Days after which data is moved to cold storage
        """
        self.hot_db_path = hot_db_path
        self.cold_db_path = cold_db_path or hot_db_path + '.cold'
        self.archive_dir = archive_dir or os.path.join(os.path.dirname(hot_db_path), 'archives')
        self.cold_threshold_days = cold_threshold_days
        
        # Create archive directory if it doesn't exist
        os.makedirs(self.archive_dir, exist_ok=True)
        
        # Track operations
        self.stats = {
            "records_moved_to_cold": 0,
            "records_archived": 0,
            "last_move_time": 0,
            "last_archive_time": 0
        }
        
        # Thread lock
        self.lock = threading.RLock()
    
    def initialize_storage(self) -> Dict[str, Any]:
        """
        Initialize hot and cold storage databases.
        
        Returns:
            Dictionary with initialization results
        """
        results = {
            "hot_db": {"status": "unknown", "error": None},
            "cold_db": {"status": "unknown", "error": None}
        }
        
        # Initialize hot database
        try:
            # Ensure hot database directory exists
            os.makedirs(os.path.dirname(self.hot_db_path), exist_ok=True)
            
            # Initialize hot database with metadata table
            hot_conn = sqlite3.connect(self.hot_db_path)
            hot_conn.execute("""
                CREATE TABLE IF NOT EXISTS storage_metadata (
                    key TEXT PRIMARY KEY,
                    value TEXT,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Set metadata
            hot_conn.execute(
                "INSERT OR REPLACE INTO storage_metadata (key, value) VALUES (?, ?)",
                ("storage_type", "hot")
            )
            hot_conn.execute(
                "INSERT OR REPLACE INTO storage_metadata (key, value) VALUES (?, ?)",
                ("cold_threshold_days", str(self.cold_threshold_days))
            )
            hot_conn.execute(
                "INSERT OR REPLACE INTO storage_metadata (key, value) VALUES (?, ?)",
                ("initialized_at", datetime.now().isoformat())
            )
            
            hot_conn.commit()
            hot_conn.close()
            
            results["hot_db"]["status"] = "initialized"
        
        except Exception as e:
            logger.error(f"Error initializing hot database: {str(e)}")
            results["hot_db"]["status"] = "error"
            results["hot_db"]["error"] = str(e)
        
        # Initialize cold database
        try:
            # Ensure cold database directory exists
            os.makedirs(os.path.dirname(self.cold_db_path), exist_ok=True)
            
            # Initialize cold database with metadata table
            cold_conn = sqlite3.connect(self.cold_db_path)
            cold_conn.execute("""
                CREATE TABLE IF NOT EXISTS storage_metadata (
                    key TEXT PRIMARY KEY,
                    value TEXT,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Set metadata
            cold_conn.execute(
                "INSERT OR REPLACE INTO storage_metadata (key, value) VALUES (?, ?)",
                ("storage_type", "cold")
            )
            cold_conn.execute(
                "INSERT OR REPLACE INTO storage_metadata (key, value) VALUES (?, ?)",
                ("linked_hot_db", self.hot_db_path)
            )
            cold_conn.execute(
                "INSERT OR REPLACE INTO storage_metadata (key, value) VALUES (?, ?)",
                ("initialized_at", datetime.now().isoformat())
            )
            
            cold_conn.commit()
            cold_conn.close()
            
            results["cold_db"]["status"] = "initialized"
        
        except Exception as e:
            logger.error(f"Error initializing cold database: {str(e)}")
            results["cold_db"]["status"] = "error"
            results["cold_db"]["error"] = str(e)
        
        return results
    
    def check_table_exists(self, db_path: str, table_name: str) -> bool:
        """
        Check if a table exists in the database.
        
        Args:
            db_path: Path to the database
            table_name: Name of the table to check
            
        Returns:
            True if the table exists, False otherwise
        """
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (table_name,)
            )
            exists = cursor.fetchone() is not None
            conn.close()
            return exists
        except Exception as e:
            logger.error(f"Error checking if table exists: {str(e)}")
            return False
    
    def move_to_cold_storage(self, contract_id: str, data_type: str, age_days: Optional[int] = None) -> Dict[str, Any]:
        """
        Move older data for a contract to cold storage.
        
        Args:
            contract_id: Contract identifier
            data_type: Data type ('tas' or 'depth')
            age_days: Age threshold in days, or None to use the default
            
        Returns:
            Dictionary with operation results
        """
        with self.lock:
            table_name = f"{contract_id}_{data_type}"
            
            # Use specified age or default
            threshold_days = age_days or self.cold_threshold_days
            
            # Calculate cutoff timestamp (microseconds since epoch)
            cutoff_time = int((time.time() - threshold_days * 86400) * 1000000)
            
            results = {
                "contract_id": contract_id,
                "data_type": data_type,
                "cutoff_timestamp": cutoff_time,
                "records_moved": 0,
                "status": "pending",
                "error": None
            }
            
            logger.info(f"Moving data older than {threshold_days} days to cold storage for {table_name}")
            
            try:
                # Check if table exists in hot database
                if not self.check_table_exists(self.hot_db_path, table_name):
                    results["status"] = "no_table"
                    logger.warning(f"Table {table_name} not found in hot database")
                    return results
                
                # Connect to databases
                hot_conn = sqlite3.connect(self.hot_db_path)
                cold_conn = sqlite3.connect(self.cold_db_path)
                
                try:
                    # Get table schema from hot database
                    cursor = hot_conn.execute(f"SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
                    table_sql = cursor.fetchone()[0]
                    
                    # Create table in cold database if it doesn't exist
                    if not self.check_table_exists(self.cold_db_path, table_name):
                        cold_conn.execute(table_sql)
                        logger.info(f"Created table {table_name} in cold database")
                        
                        # Get indexes for the table
                        cursor = hot_conn.execute(
                            "SELECT sql FROM sqlite_master WHERE type='index' AND tbl_name=?",
                            (table_name,)
                        )
                        for index_sql in cursor.fetchall():
                            if index_sql[0]:  # Check if SQL is not None
                                cold_conn.execute(index_sql[0])
                        
                        cold_conn.commit()
                    
                    # Get records to move
                    cursor = hot_conn.execute(
                        f"SELECT COUNT(*) FROM {table_name} WHERE timestamp < ?",
                        (cutoff_time,)
                    )
                    count_to_move = cursor.fetchone()[0]
                    
                    if count_to_move == 0:
                        results["status"] = "no_records"
                        logger.info(f"No records to move for {table_name}")
                        return results
                    
                    # Get column names
                    cursor = hot_conn.execute(f"PRAGMA table_info({table_name})")
                    columns = [row[1] for row in cursor.fetchall()]
                    columns_str = ", ".join(columns)
                    placeholders = ", ".join(["?" for _ in columns])
                    
                    # Begin transaction
                    hot_conn.execute("BEGIN TRANSACTION")
                    cold_conn.execute("BEGIN TRANSACTION")
                    
                    # Move in batches to avoid memory issues
                    batch_size = 10000
                    total_moved = 0
                    
                    while total_moved < count_to_move:
                        # Get a batch of records
                        cursor = hot_conn.execute(
                            f"SELECT {columns_str} FROM {table_name} WHERE timestamp < ? LIMIT {batch_size}",
                            (cutoff_time,)
                        )
                        records = cursor.fetchall()
                        
                        if not records:
                            break
                        
                        # Insert into cold database
                        cold_conn.executemany(
                            f"INSERT OR REPLACE INTO {table_name} ({columns_str}) VALUES ({placeholders})",
                            records
                        )
                        
                        # Delete from hot database
                        placeholders = ", ".join(["?" for _ in records])
                        hot_conn.execute(
                            f"DELETE FROM {table_name} WHERE timestamp IN (SELECT timestamp FROM {table_name} WHERE timestamp < ? LIMIT {batch_size})",
                            (cutoff_time,)
                        )
                        
                        total_moved += len(records)
                        logger.info(f"Moved {total_moved}/{count_to_move} records for {table_name}")
                    
                    # Commit transactions
                    cold_conn.commit()
                    hot_conn.commit()
                    
                    # Update stats
                    self.stats["records_moved_to_cold"] += total_moved
                    self.stats["last_move_time"] = time.time()
                    
                    results["records_moved"] = total_moved
                    results["status"] = "success"
                    
                    logger.info(f"Successfully moved {total_moved} records to cold storage for {table_name}")
                    
                    return results
                
                finally:
                    hot_conn.close()
                    cold_conn.close()
            
            except Exception as e:
                logger.error(f"Error moving data to cold storage: {str(e)}")
                results["status"] = "error"
                results["error"] = str(e)
                return results
    
    def archive_cold_data(self, contract_id: str, data_type: str, age_days: int = 365) -> Dict[str, Any]:
        """
        Archive very old data from cold storage to compressed files.
        
        Args:
            contract_id: Contract identifier
            data_type: Data type ('tas' or 'depth')
            age_days: Age threshold in days
            
        Returns:
            Dictionary with operation results
        """
        with self.lock:
            table_name = f"{contract_id}_{data_type}"
            
            # Calculate cutoff timestamp (microseconds since epoch)
            cutoff_time = int((time.time() - age_days * 86400) * 1000000)
            
            results = {
                "contract_id": contract_id,
                "data_type": data_type,
                "age_days": age_days,
                "cutoff_timestamp": cutoff_time,
                "records_archived": 0,
                "archive_path": None,
                "status": "pending",
                "error": None
            }
            
            logger.info(f"Archiving data older than {age_days} days for {table_name}")
            
            try:
                # Check if table exists in cold database
                if not self.check_table_exists(self.cold_db_path, table_name):
                    results["status"] = "no_table"
                    logger.warning(f"Table {table_name} not found in cold database")
                    return results
                
                # Connect to cold database
                conn = sqlite3.connect(self.cold_db_path)
                
                try:
                    # Get records to archive
                    cursor = conn.execute(
                        f"SELECT COUNT(*) FROM {table_name} WHERE timestamp < ?",
                        (cutoff_time,)
                    )
                    count_to_archive = cursor.fetchone()[0]
                    
                    if count_to_archive == 0:
                        results["status"] = "no_records"
                        logger.info(f"No records to archive for {table_name}")
                        return results
                    
                    # Create archive filename
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    archive_path = os.path.join(
                        self.archive_dir,
                        f"{table_name}_{timestamp}.csv.gz"
                    )
                    
                    # Export to CSV with gzip compression
                    import csv
                    import gzip
                    
                    with gzip.open(archive_path, 'wt', newline='') as f:
                        # Get column names
                        cursor = conn.execute(f"PRAGMA table_info({table_name})")
                        columns = [row[1] for row in cursor.fetchall()]
                        
                        # Create CSV writer
                        writer = csv.writer(f)
                        writer.writerow(columns)
                        
                        # Write data in batches
                        batch_size = 10000
                        total_archived = 0
                        
                        while total_archived < count_to_archive:
                            cursor = conn.execute(
                                f"SELECT * FROM {table_name} WHERE timestamp < ? LIMIT {batch_size}",
                                (cutoff_time,)
                            )
                            records = cursor.fetchall()
                            
                            if not records:
                                break
                            
                            writer.writerows(records)
                            total_archived += len(records)
                            logger.info(f"Archived {total_archived}/{count_to_archive} records for {table_name}")
                    
                    # Delete archived records
                    conn.execute("BEGIN TRANSACTION")
                    conn.execute(
                        f"DELETE FROM {table_name} WHERE timestamp < ?",
                        (cutoff_time,)
                    )
                    conn.commit()
                    
                    # Update stats
                    self.stats["records_archived"] += total_archived
                    self.stats["last_archive_time"] = time.time()
                    
                    # Create metadata file
                    meta_path = archive_path + '.meta'
                    with open(meta_path, 'w') as f:
                        json.dump({
                            "contract_id": contract_id,
                            "data_type": data_type,
                            "archived_at": timestamp,
                            "record_count": total_archived,
                            "age_days": age_days,
                            "cutoff_timestamp": cutoff_time
                        }, f, indent=2)
                    
                    results["records_archived"] = total_archived
                    results["archive_path"] = archive_path
                    results["status"] = "success"
                    
                    logger.info(
                        f"Successfully archived {total_archived} records for {table_name} "
                        f"to {archive_path}"
                    )
                    
                    return results
                
                finally:
                    conn.close()
            
            except Exception as e:
                logger.error(f"Error archiving data: {str(e)}")
                results["status"] = "error"
                results["error"] = str(e)
                return results
    
    def restore_from_archive(self, archive_path: str, target_db_path: Optional[str] = None) -> Dict[str, Any]:
        """
        Restore data from an archive file.
        
        Args:
            archive_path: Path to the archive file
            target_db_path: Path to the target database (default: cold database)
            
        Returns:
            Dictionary with operation results
        """
        with self.lock:
            target_db = target_db_path or self.cold_db_path
            
            results = {
                "archive_path": archive_path,
                "target_db": target_db,
                "records_restored": 0,
                "status": "pending",
                "error": None
            }
            
            logger.info(f"Restoring data from {archive_path} to {target_db}")
            
            try:
                # Check if archive file exists
                if not os.path.exists(archive_path):
                    results["status"] = "file_not_found"
                    logger.warning(f"Archive file not found: {archive_path}")
                    return results
                
                # Check if metadata file exists
                meta_path = archive_path + '.meta'
                if not os.path.exists(meta_path):
                    results["status"] = "metadata_not_found"
                    logger.warning(f"Metadata file not found: {meta_path}")
                    return results
                
                # Load metadata
                with open(meta_path, 'r') as f:
                    metadata = json.load(f)
                
                contract_id = metadata.get("contract_id")
                data_type = metadata.get("data_type")
                
                if not contract_id or not data_type:
                    results["status"] = "invalid_metadata"
                    logger.warning(f"Invalid metadata: {metadata}")
                    return results
                
                table_name = f"{contract_id}_{data_type}"
                
                # Connect to target database
                conn = sqlite3.connect(target_db)
                
                try:
                    # Check if table exists, create it if not
                    if not self.check_table_exists(target_db, table_name):
                        # Create table based on data type
                        if data_type == "tas":
                            conn.execute(f"""
                                CREATE TABLE {table_name} (
                                    timestamp   INTEGER PRIMARY KEY,
                                    price       REAL NOT NULL,
                                    qty         INTEGER NOT NULL,
                                    side        INTEGER NOT NULL
                                )
                            """)
                        elif data_type == "depth":
                            conn.execute(f"""
                                CREATE TABLE {table_name} (
                                    timestamp   INTEGER,
                                    command     INTEGER NOT NULL,
                                    flags       INTEGER NOT NULL,
                                    num_orders  INTEGER NOT NULL,
                                    price       REAL NOT NULL,
                                    qty         INTEGER NOT NULL,
                                    PRIMARY KEY (timestamp, command, price)
                                )
                            """)
                        else:
                            results["status"] = "unknown_data_type"
                            logger.warning(f"Unknown data type: {data_type}")
                            return results
                    
                    # Import from CSV
                    import csv
                    import gzip
                    
                    with gzip.open(archive_path, 'rt', newline='') as f:
                        reader = csv.reader(f)
                        columns = next(reader)  # Read header row
                        
                        # Get column types based on data type
                        if data_type == "tas":
                            column_types = [int, float, int, int]
                        elif data_type == "depth":
                            column_types = [int, int, int, int, float, int]
                        else:
                            column_types = [str] * len(columns)
                        
                        # Insert data in batches
                        conn.execute("BEGIN TRANSACTION")
                        
                        batch_size = 10000
                        batch = []
                        total_restored = 0
                        
                        for row in reader:
                            # Convert values to correct types
                            typed_row = [typ(val) for typ, val in zip(column_types, row)]
                            batch.append(typed_row)
                            
                            if len(batch) >= batch_size:
                                placeholders = ", ".join(["?" for _ in columns])
                                conn.executemany(
                                    f"INSERT OR REPLACE INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})",
                                    batch
                                )
                                total_restored += len(batch)
                                batch = []
                                logger.info(f"Restored {total_restored} records so far")
                        
                        # Insert remaining records
                        if batch:
                            placeholders = ", ".join(["?" for _ in columns])
                            conn.executemany(
                                f"INSERT OR REPLACE INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})",
                                batch
                            )
                            total_restored += len(batch)
                        
                        conn.commit()
                    
                    results["records_restored"] = total_restored
                    results["status"] = "success"
                    
                    logger.info(f"Successfully restored {total_restored} records to {table_name}")
                    
                    return results
                
                finally:
                    conn.close()
            
            except Exception as e:
                logger.error(f"Error restoring from archive: {str(e)}")
                results["status"] = "error"
                results["error"] = str(e)
                return results
    
    def query_across_storage(self, sql: str, params: Optional[Tuple] = None, 
                           prioritize_hot: bool = True) -> List[Tuple]:
        """
        Execute a query across both hot and cold storage.
        
        Args:
            sql: SQL query to execute
            params: Query parameters
            prioritize_hot: Whether to query hot storage first
            
        Returns:
            Combined query results
        """
        hot_results = []
        cold_results = []
        
        # Query hot storage
        try:
            hot_conn = sqlite3.connect(self.hot_db_path)
            hot_cursor = hot_conn.execute(sql, params or ())
            hot_results = hot_cursor.fetchall()
            hot_conn.close()
        except Exception as e:
            logger.error(f"Error querying hot storage: {str(e)}")
        
        # Query cold storage
        try:
            cold_conn = sqlite3.connect(self.cold_db_path)
            cold_cursor = cold_conn.execute(sql, params or ())
            cold_results = cold_cursor.fetchall()
            cold_conn.close()
        except Exception as e:
            logger.error(f"Error querying cold storage: {str(e)}")
        
        # Combine results
        if prioritize_hot:
            # Hot storage data has precedence
            return hot_results + cold_results
        else:
            # Cold storage data has precedence
            return cold_results + hot_results