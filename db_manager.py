#!/usr/bin/env python3
"""
Database management system with connection pooling and optimized queries.

Part of the Sierra Chart ETL Pipeline project.
"""

import sqlite3
import logging
import threading
import time
import os
import re
from typing import List, Dict, Any, Optional, Tuple, Union
from contextlib import contextmanager
from dataclasses import dataclass
from enum import Enum
import traceback
from error_handling import ETLError, ErrorCategory, create_error_context, with_retry, RetryConfig

logger = logging.getLogger("SierraChartETL.database")

# Regular expression for validating table names (prevent SQL injection)
TABLE_NAME_PATTERN = re.compile(r'^[a-zA-Z0-9_]+$')

class DatabaseOperation(Enum):
    READ = "READ"
    WRITE = "WRITE"
    SCHEMA = "SCHEMA"
    MAINTENANCE = "MAINTENANCE"

@dataclass
class DatabaseStats:
    """Statistics for database operations"""
    total_tas_records: int = 0
    total_depth_records: int = 0
    last_write_time: float = 0
    total_queries: int = 0
    query_time_ms: float = 0
    total_errors: int = 0
    last_vacuum_time: float = 0
    last_backup_time: float = 0
    avg_query_time_ms: float = 0
    max_query_time_ms: float = 0
    total_filtered_records: int = 0

class ConnectionPool:
    """
    A robust SQLite connection pool with enhanced error recovery.
    Since SQLite is single-writer, multiple-reader, we need to manage connections carefully.
    """
    def __init__(self, db_path: str, max_connections: int = 5, timeout: float = 30.0, 
                 retry_attempts: int = 3):
        self.db_path = db_path
        self.max_connections = max_connections
        self.timeout = timeout
        self.retry_attempts = retry_attempts
        self.connections = []
        self.connections_lock = threading.RLock()
        self.pool_semaphore = threading.BoundedSemaphore(max_connections)
        self.stats = DatabaseStats()
        self.critical_section_lock = threading.RLock()  # For operations that need exclusive access
        
        # Make sure the database directory exists
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
    
    def _create_connection(self) -> sqlite3.Connection:
        """Create a new SQLite connection with optimal settings"""
        # Create connection with extended options for better reliability
        conn = sqlite3.connect(
            self.db_path,
            timeout=self.timeout,
            isolation_level=None,  # We'll manage transactions explicitly
            check_same_thread=False  # We'll manage thread safety with locks
        )
        
        # Enable foreign keys for data integrity
        conn.execute("PRAGMA foreign_keys = ON")
        
        # Use WAL for better concurrency
        conn.execute("PRAGMA journal_mode = WAL")
        
        # Other performance settings
        conn.execute("PRAGMA synchronous = NORMAL")  # Balance between safety and performance
        conn.execute("PRAGMA cache_size = -65536")   # 64MB cache (larger than default)
        conn.execute("PRAGMA mmap_size = 1073741824")  # 1GB memory mapping for faster reads
        conn.execute("PRAGMA temp_store = MEMORY")  # Store temp tables in memory
        
        # Set busy timeout to wait for locks to be released
        conn.execute(f"PRAGMA busy_timeout = {int(self.timeout * 1000)}")
        
        return conn
    
    @contextmanager
    def get_connection(self, operation: DatabaseOperation = DatabaseOperation.READ):
        """
        Get a connection from the pool with enhanced error handling and retries
        
        Args:
            operation: The type of database operation being performed
        
        Yields:
            A database connection
        """
        # For exclusive operations like schema changes, use a critical section
        exclusive_op = operation in [DatabaseOperation.SCHEMA, DatabaseOperation.MAINTENANCE]
        
        # Acquire locks
        if exclusive_op:
            acquired_critical = self.critical_section_lock.acquire(timeout=self.timeout)
            if not acquired_critical:
                ctx = create_error_context(
                    component="ConnectionPool",
                    operation="get_connection",
                    additional_info={"db_path": self.db_path, "operation": operation.value}
                )
                raise ETLError(
                    message=f"Timed out waiting for critical section after {self.timeout}s",
                    category=ErrorCategory.SYSTEM_ERROR,
                    context=ctx
                )
        
        # Acquire a semaphore slot
        acquired = self.pool_semaphore.acquire(timeout=self.timeout)
        if not acquired:
            if exclusive_op:
                self.critical_section_lock.release()
            
            ctx = create_error_context(
                component="ConnectionPool",
                operation="get_connection",
                additional_info={"db_path": self.db_path, "operation": operation.value}
            )
            raise ETLError(
                message=f"Timed out waiting for a database connection after {self.timeout}s",
                category=ErrorCategory.SYSTEM_ERROR,
                context=ctx
            )
        
        conn = None
        try:
            # Get or create a connection
            for attempt in range(self.retry_attempts):
                try:
                    with self.connections_lock:
                        if self.connections:
                            conn = self.connections.pop()
                        else:
                            conn = self._create_connection()
                    
                    # Begin a transaction based on the operation type
                    if operation == DatabaseOperation.READ:
                        conn.execute("BEGIN")
                    elif operation == DatabaseOperation.WRITE:
                        conn.execute("BEGIN IMMEDIATE")
                    elif operation == DatabaseOperation.SCHEMA:
                        conn.execute("BEGIN EXCLUSIVE")
                    # For MAINTENANCE operations, don't start a transaction
                    
                    # Got a good connection, break the retry loop
                    break
                
                except sqlite3.Error as e:
                    logger.warning(f"Error getting connection on attempt {attempt+1}: {str(e)}")
                    # Close the bad connection
                    if conn:
                        try:
                            conn.close()
                        except:
                            pass
                        conn = None
                    
                    # Last attempt failed
                    if attempt == self.retry_attempts - 1:
                        raise
                    
                    # Wait before retry
                    time.sleep(0.5 * (attempt + 1))
            
            # Start timing for performance tracking
            start_time = time.time()
            
            yield conn
            
            # Track query execution time
            query_time_ms = (time.time() - start_time) * 1000
            self.stats.total_queries += 1
            self.stats.query_time_ms += query_time_ms
            self.stats.avg_query_time_ms = self.stats.query_time_ms / self.stats.total_queries
            self.stats.max_query_time_ms = max(self.stats.max_query_time_ms, query_time_ms)
            
            # Commit the transaction if it's still active
            if operation != DatabaseOperation.MAINTENANCE:
                conn.commit()
        
        except sqlite3.OperationalError as e:
            # Handle database is locked error specifically
            if "database is locked" in str(e).lower():
                logger.warning(f"Database lock error during {operation.value} operation: {str(e)}")
                if operation != DatabaseOperation.MAINTENANCE:
                    try:
                        conn.rollback()
                    except:
                        pass
                
                # Increment error counter
                self.stats.total_errors += 1
                
                ctx = create_error_context(
                    component="ConnectionPool",
                    operation=f"database_{operation.value.lower()}",
                    additional_info={"db_path": self.db_path, "error": str(e)}
                )
                raise ETLError(
                    message=f"Database locked error: {str(e)}. You may need to increase timeout or reduce concurrency.",
                    category=ErrorCategory.SYSTEM_ERROR,
                    context=ctx
                ) from e
            
            # Other operational errors
            logger.error(f"Database operational error during {operation.value} operation: {str(e)}")
            if operation != DatabaseOperation.MAINTENANCE:
                try:
                    conn.rollback()
                except:
                    pass
            
            # Increment error counter
            self.stats.total_errors += 1
            
            ctx = create_error_context(
                component="ConnectionPool",
                operation=f"database_{operation.value.lower()}",
                additional_info={"db_path": self.db_path, "error": str(e)}
            )
            raise ETLError(
                message=f"Database operational error: {str(e)}",
                category=ErrorCategory.SYSTEM_ERROR,
                context=ctx
            ) from e
        
        except sqlite3.Error as e:
            # General SQLite errors
            logger.error(f"Database error during {operation.value} operation: {str(e)}")
            if operation != DatabaseOperation.MAINTENANCE:
                try:
                    conn.rollback()
                except:
                    pass
            
            # Increment error counter
            self.stats.total_errors += 1
            
            ctx = create_error_context(
                component="ConnectionPool",
                operation=f"database_{operation.value.lower()}",
                additional_info={"db_path": self.db_path, "error": str(e)}
            )
            raise ETLError(
                message=f"Database error: {str(e)}",
                category=ErrorCategory.SYSTEM_ERROR,
                context=ctx
            ) from e
        
        finally:
            if conn:
                try:
                    # Return connection to pool if it's in a good state
                    if not conn.in_transaction:
                        with self.connections_lock:
                            self.connections.append(conn)
                    else:
                        # Close bad connections
                        conn.close()
                except:
                    # If anything goes wrong, just close the connection
                    try:
                        conn.close()
                    except:
                        pass
            
            # Release locks
            self.pool_semaphore.release()
            if exclusive_op:
                self.critical_section_lock.release()
    
    def close_all(self):
        """Close all connections in the pool"""
        with self.connections_lock:
            for conn in self.connections:
                try:
                    conn.close()
                except:
                    pass
            self.connections = []

class DatabaseManager:
    """
    Manages database operations, schema, and maintenance with enhanced error handling
    and performance optimizations.
    """
    def __init__(self, db_path: str, max_connections: int = 10):
        self.db_path = db_path
        self.connection_pool = ConnectionPool(db_path, max_connections)
        self._validate_database()
        self._optimized_indexes = set()  # Track which tables have optimized indexes
    
    def _validate_database(self):
        """Validate database structure and create metadata tables if they don't exist"""
        with self.connection_pool.get_connection(DatabaseOperation.SCHEMA) as conn:
            # Create metadata table if it doesn't exist
            conn.execute("""
                CREATE TABLE IF NOT EXISTS etl_metadata (
                    key TEXT PRIMARY KEY,
                    value TEXT,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Check if we need to set the database version
            cursor = conn.execute("SELECT value FROM etl_metadata WHERE key = 'db_version'")
            result = cursor.fetchone()
            
            if not result:
                conn.execute(
                    "INSERT INTO etl_metadata (key, value) VALUES (?, ?)", 
                    ("db_version", "1.1.0")  # Updated version for enhanced ETL
                )
                conn.execute(
                    "INSERT INTO etl_metadata (key, value) VALUES (?, ?)", 
                    ("created_at", time.strftime("%Y-%m-%d %H:%M:%S"))
                )
            
            # Create tables_info table to track schema changes
            conn.execute("""
                CREATE TABLE IF NOT EXISTS tables_info (
                    table_name TEXT PRIMARY KEY,
                    record_count INTEGER DEFAULT 0,
                    last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    has_indexes BOOLEAN DEFAULT 0,
                    min_timestamp INTEGER,
                    max_timestamp INTEGER,
                    avg_price REAL,
                    last_maintenance TIMESTAMP
                )
            """)
            
            # Create data quality metrics table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS data_quality_metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    table_name TEXT NOT NULL,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    metric_name TEXT NOT NULL,
                    metric_value REAL NOT NULL,
                    notes TEXT
                )
            """)
            
            # Create index on data_quality_metrics
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_data_quality_table_metric
                ON data_quality_metrics(table_name, metric_name)
            """)
    
    def _validate_table_name(self, table_name: str) -> bool:
        """Validate table name to prevent SQL injection"""
        if not TABLE_NAME_PATTERN.match(table_name):
            logger.error(f"Invalid table name format: {table_name}")
            return False
        return True
    
    def ensure_table_exists(self, table_name: str, table_type: str) -> None:
        """
        Ensure that a table exists with the correct schema and optimal indexes
        
        Args:
            table_name: The name of the table to create/validate
            table_type: The type of table ('tas' or 'depth')
        """
        if not self._validate_table_name(table_name):
            ctx = create_error_context(
                component="DatabaseManager",
                operation="ensure_table_exists",
                additional_info={"table_name": table_name}
            )
            raise ETLError(
                message=f"Invalid table name format: {table_name}",
                category=ErrorCategory.DATA_ERROR,
                context=ctx
            )
        
        with self.connection_pool.get_connection(DatabaseOperation.SCHEMA) as conn:
            # Check if table exists
            cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?", 
                (table_name,)
            )
            exists = cursor.fetchone() is not None
            
            if not exists:
                # Create the table based on type
                if table_type == "tas":
                    conn.execute(f"""
                        CREATE TABLE {table_name} (
                            timestamp   INTEGER PRIMARY KEY,
                            price       REAL NOT NULL,
                            qty         INTEGER NOT NULL,
                            side        INTEGER NOT NULL
                        )
                    """)
                elif table_type == "depth":
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
                    ctx = create_error_context(
                        component="DatabaseManager",
                        operation="ensure_table_exists",
                        additional_info={"table_name": table_name, "table_type": table_type}
                    )
                    raise ETLError(
                        message=f"Unknown table type: {table_type}",
                        category=ErrorCategory.DATA_ERROR,
                        context=ctx
                    )
                
                # Add to tables_info
                conn.execute(
                    "INSERT INTO tables_info (table_name, last_update) VALUES (?, CURRENT_TIMESTAMP)",
                    (table_name,)
                )
                
                logger.info(f"Created table {table_name} of type {table_type}")
            
            # Check if indexes exist and are optimized
            cursor = conn.execute(
                "SELECT has_indexes FROM tables_info WHERE table_name = ?",
                (table_name,)
            )
            result = cursor.fetchone()
            
            # If not in tables_info or indexes not created
            if not result or not result[0] or table_name not in self._optimized_indexes:
                # Create enhanced indexes for the table
                if table_type == "tas":
                    # Drop old indexes if they exist
                    try:
                        conn.execute(f"DROP INDEX IF EXISTS idx_{table_name}_price")
                        conn.execute(f"DROP INDEX IF EXISTS idx_{table_name}_side")
                    except:
                        pass
                    
                    # Create optimized indexes
                    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_price ON {table_name}(price)")
                    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_side_price ON {table_name}(side, price)")
                    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_price_qty ON {table_name}(price, qty)")
                    
                elif table_type == "depth":
                    # Drop old indexes if they exist
                    try:
                        conn.execute(f"DROP INDEX IF EXISTS idx_{table_name}_command")
                        conn.execute(f"DROP INDEX IF EXISTS idx_{table_name}_price")
                    except:
                        pass
                    
                    # Create optimized indexes
                    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_command ON {table_name}(command)")
                    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_price ON {table_name}(price)")
                    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_cmd_price ON {table_name}(command, price)")
                
                # Update tables_info to show indexes are created
                if result:
                    conn.execute(
                        "UPDATE tables_info SET has_indexes = 1, last_update = CURRENT_TIMESTAMP WHERE table_name = ?",
                        (table_name,)
                    )
                else:
                    conn.execute(
                        "INSERT INTO tables_info (table_name, has_indexes, last_update) VALUES (?, 1, CURRENT_TIMESTAMP)",
                        (table_name,)
                    )
                
                # Track that this table has optimized indexes
                self._optimized_indexes.add(table_name)
                
                logger.info(f"Created optimized indexes for {table_name}")
    
    @with_retry(retry_config=RetryConfig(max_retries=3, base_delay=1.0, max_delay=10.0))
    def load_tas_data(self, contract_id: str, records: List[Tuple]) -> int:
        """
        Load time & sales data into the database with retry capability
        
        Args:
            contract_id: The contract ID
            records: List of tuples with (timestamp, price, qty, side)
            
        Returns:
            Number of records inserted
        """
        if not records:
            return 0
        
        table_name = f"{contract_id}_tas"
        self.ensure_table_exists(table_name, "tas")
        
        with self.connection_pool.get_connection(DatabaseOperation.WRITE) as conn:
            cursor = conn.executemany(
                f"""
                INSERT OR REPLACE INTO {table_name} (
                    timestamp, price, qty, side
                ) VALUES (?, ?, ?, ?)
                """,
                records
            )
            
            # Update record count in tables_info
            conn.execute(
                """
                UPDATE tables_info 
                SET record_count = record_count + ?, 
                    last_update = CURRENT_TIMESTAMP,
                    min_timestamp = CASE
                                     WHEN min_timestamp IS NULL THEN ?
                                     WHEN ? < min_timestamp THEN ?
                                     ELSE min_timestamp
                                   END,
                    max_timestamp = CASE
                                     WHEN max_timestamp IS NULL THEN ?
                                     WHEN ? > max_timestamp THEN ?
                                     ELSE max_timestamp
                                   END
                WHERE table_name = ?
                """,
                (
                    len(records), 
                    min(r[0] for r in records) if records else None,
                    min(r[0] for r in records) if records else None,
                    min(r[0] for r in records) if records else None,
                    max(r[0] for r in records) if records else None,
                    max(r[0] for r in records) if records else None,
                    max(r[0] for r in records) if records else None,
                    table_name
                )
            )
            
            # Update stats
            self.connection_pool.stats.total_tas_records += len(records)
            self.connection_pool.stats.last_write_time = time.time()
            
            return len(records)
    
    @with_retry(retry_config=RetryConfig(max_retries=3, base_delay=1.0, max_delay=10.0))
    def load_depth_data(self, contract_id: str, records: List[Tuple]) -> int:
        """
        Load market depth data into the database with retry capability
        
        Args:
            contract_id: The contract ID
            records: List of tuples with (timestamp, command, flags, num_orders, price, qty)
            
        Returns:
            Number of records inserted
        """
        if not records:
            return 0
        
        table_name = f"{contract_id}_depth"
        self.ensure_table_exists(table_name, "depth")
        
        with self.connection_pool.get_connection(DatabaseOperation.WRITE) as conn:
            cursor = conn.executemany(
                f"""
                INSERT OR REPLACE INTO {table_name} (
                    timestamp, command, flags, num_orders, price, qty
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                records
            )
            
            # Update record count in tables_info with timestamps
            conn.execute(
                """
                UPDATE tables_info 
                SET record_count = record_count + ?, 
                    last_update = CURRENT_TIMESTAMP,
                    min_timestamp = CASE
                                     WHEN min_timestamp IS NULL THEN ?
                                     WHEN ? < min_timestamp THEN ?
                                     ELSE min_timestamp
                                   END,
                    max_timestamp = CASE
                                     WHEN max_timestamp IS NULL THEN ?
                                     WHEN ? > max_timestamp THEN ?
                                     ELSE max_timestamp
                                   END
                WHERE table_name = ?
                """,
                (
                    len(records), 
                    min(r[0] for r in records) if records else None,
                    min(r[0] for r in records) if records else None,
                    min(r[0] for r in records) if records else None,
                    max(r[0] for r in records) if records else None,
                    max(r[0] for r in records) if records else None,
                    max(r[0] for r in records) if records else None,
                    table_name
                )
            )
            
            # Update stats
            self.connection_pool.stats.total_depth_records += len(records)
            self.connection_pool.stats.last_write_time = time.time()
            
            return len(records)
    
    def get_table_stats(self) -> Dict[str, Dict[str, Any]]:
        """Get comprehensive statistics about all tables in the database"""
        with self.connection_pool.get_connection(DatabaseOperation.READ) as conn:
            cursor = conn.execute(
                """
                SELECT table_name, record_count, last_update, min_timestamp, max_timestamp,
                       avg_price, last_maintenance
                FROM tables_info
                """
            )
            
            results = {}
            for row in cursor.fetchall():
                table_name, record_count, last_update, min_ts, max_ts, avg_price, last_maint = row
                
                # Get additional table statistics
                size_cursor = conn.execute(
                    "SELECT SUM(pgsize) FROM dbstat WHERE name = ?",
                    (table_name,)
                )
                size_result = size_cursor.fetchone()
                table_size = size_result[0] if size_result else None
                
                # Get index sizes
                index_cursor = conn.execute(
                    "SELECT name, SUM(pgsize) FROM dbstat WHERE name LIKE ? AND name != ? GROUP BY name",
                    (f"idx_{table_name}%", table_name)
                )
                indexes = {row[0]: row[1] for row in index_cursor.fetchall()}
                
                results[table_name] = {
                    "record_count": record_count,
                    "last_update": last_update,
                    "min_timestamp": min_ts,
                    "max_timestamp": max_ts,
                    "avg_price": avg_price,
                    "last_maintenance": last_maint,
                    "table_size_bytes": table_size,
                    "indexes": indexes,
                    "time_span_hours": None
                }
                
                # Calculate time span if timestamps are available
                if min_ts and max_ts:
                    time_span_seconds = (max_ts - min_ts) / 1000000  # Convert to seconds
                    results[table_name]["time_span_hours"] = time_span_seconds / 3600
            
            return results
    
    def add_data_quality_metric(self, table_name: str, metric_name: str, metric_value: float, 
                              notes: Optional[str] = None) -> None:
        """
        Record a data quality metric for analysis and monitoring
        
        Args:
            table_name: The table the metric applies to
            metric_name: Name of the metric
            metric_value: Value of the metric
            notes: Optional notes about the metric
        """
        if not self._validate_table_name(table_name):
            logger.warning(f"Invalid table name for data quality metric: {table_name}")
            return
        
        with self.connection_pool.get_connection(DatabaseOperation.WRITE) as conn:
            conn.execute(
                """
                INSERT INTO data_quality_metrics 
                (table_name, metric_name, metric_value, notes) 
                VALUES (?, ?, ?, ?)
                """,
                (table_name, metric_name, metric_value, notes)
            )
    
    def get_data_quality_metrics(self, table_name: Optional[str] = None, 
                              metric_name: Optional[str] = None,
                              limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get data quality metrics for analysis
        
        Args:
            table_name: Optional filter by table
            metric_name: Optional filter by metric name
            limit: Maximum number of records to return
            
        Returns:
            List of quality metric records
        """
        query = "SELECT id, table_name, timestamp, metric_name, metric_value, notes FROM data_quality_metrics"
        params = []
        
        # Build WHERE clause
        where_clauses = []
        if table_name:
            if not self._validate_table_name(table_name):
                logger.warning(f"Invalid table name for data quality query: {table_name}")
                return []
            
            where_clauses.append("table_name = ?")
            params.append(table_name)
        
        if metric_name:
            where_clauses.append("metric_name = ?")
            params.append(metric_name)
        
        if where_clauses:
            query += " WHERE " + " AND ".join(where_clauses)
        
        # Add ORDER BY and LIMIT
        query += " ORDER BY timestamp DESC LIMIT ?"
        params.append(limit)
        
        # Execute query
        with self.connection_pool.get_connection(DatabaseOperation.READ) as conn:
            cursor = conn.execute(query, params)
            
            # Convert to list of dictionaries
            columns = [col[0] for col in cursor.description]
            result = [dict(zip(columns, row)) for row in cursor.fetchall()]
            
            return result
    
    @with_retry(retry_config=RetryConfig(max_retries=2, base_delay=10.0))
    def perform_maintenance(self, tables: Optional[List[str]] = None) -> None:
        """
        Perform database maintenance with enhanced routines
        
        Args:
            tables: Optional list of specific tables to maintain
        """
        logger.info("Starting database maintenance")
        with self.connection_pool.get_connection(DatabaseOperation.MAINTENANCE) as conn:
            # Start with overall database analysis
            logger.info("Running ANALYZE")
            conn.execute("ANALYZE")
            
            # Check if we need to optimize specific tables or all tables
            if tables is None:
                # Get all data tables
                cursor = conn.execute(
                    """
                    SELECT name FROM sqlite_master 
                    WHERE type='table' 
                    AND name NOT LIKE 'sqlite%' 
                    AND name NOT LIKE 'etl_%'
                    AND name NOT LIKE 'data_quality_%'
                    """
                )
                tables = [row[0] for row in cursor.fetchall()]
            
            # First pass: calculate statistics for each table
            for table in tables:
                try:
                    if not self._validate_table_name(table):
                        logger.warning(f"Skipping invalid table name during maintenance: {table}")
                        continue
                    
                    logger.info(f"Analyzing table {table}")
                    
                    # Run analyze on this specific table
                    conn.execute(f"ANALYZE {table}")
                    
                    # Update statistics for TAS tables
                    if table.endswith("_tas"):
                        try:
                            # Calculate average price
                            cursor = conn.execute(f"SELECT AVG(price) FROM {table}")
                            avg_price = cursor.fetchone()[0]
                            
                            # Update tables_info
                            conn.execute(
                                """
                                UPDATE tables_info 
                                SET avg_price = ?, last_maintenance = CURRENT_TIMESTAMP
                                WHERE table_name = ?
                                """,
                                (avg_price, table)
                            )
                        except sqlite3.Error as e:
                            logger.warning(f"Error updating statistics for {table}: {str(e)}")
                
                except sqlite3.Error as e:
                    logger.warning(f"Error during maintenance for table {table}: {str(e)}")
            
            # Second pass: optimize tables with recreate if fragmented
            for table in tables:
                try:
                    if not self._validate_table_name(table):
                        continue
                    
                    # Get table size and stats
                    try:
                        cursor = conn.execute(
                            "SELECT SUM(pgsize) FROM dbstat WHERE name = ?",
                            (table,)
                        )
                        table_size = cursor.fetchone()[0]
                        
                        # Check if table needs optimization
                        if table_size > 10 * 1024 * 1024:  # 10MB+
                            logger.info(f"Running OPTIMIZE on large table {table} ({table_size/1024/1024:.2f} MB)")
                            conn.execute(f"PRAGMA optimize('{table}')")
                    except sqlite3.Error as e:
                        logger.warning(f"Error checking table stats for {table}: {str(e)}")
                
                except sqlite3.Error as e:
                    logger.warning(f"Error optimizing table {table}: {str(e)}")
            
            # Final steps: vacuum and integrity check
            try:
                logger.info("Running VACUUM")
                conn.execute("VACUUM")
                
                logger.info("Running integrity check")
                cursor = conn.execute("PRAGMA integrity_check")
                integrity_result = cursor.fetchone()[0]
                
                if integrity_result != "ok":
                    logger.error(f"Integrity check failed: {integrity_result}")
                else:
                    logger.info("Integrity check passed")
                
                # Update metadata
                conn.execute(
                    """
                    INSERT OR REPLACE INTO etl_metadata (key, value, updated_at)
                    VALUES (?, ?, CURRENT_TIMESTAMP)
                    """,
                    ("last_maintenance", time.strftime("%Y-%m-%d %H:%M:%S"))
                )
                
                # Update stats
                self.connection_pool.stats.last_vacuum_time = time.time()
            
            except sqlite3.Error as e:
                logger.error(f"Error during final maintenance steps: {str(e)}")
                raise
        
        logger.info("Database maintenance completed")
    
    def backup_database(self, backup_path: Optional[str] = None) -> str:
        """
        Create a backup of the database with verification
        
        Args:
            backup_path: Optional path for the backup. If None, a timestamped path is created.
            
        Returns:
            Path to the backup file
        """
        if not backup_path:
            timestamp = time.strftime("%Y%m%d_%H%M%S")
            backup_dir = os.path.join(os.path.dirname(self.db_path), "backups")
            os.makedirs(backup_dir, exist_ok=True)
            backup_path = os.path.join(
                backup_dir, 
                f"{os.path.basename(self.db_path)}.{timestamp}.bak"
            )
        
        logger.info(f"Backing up database to {backup_path}")
        
        with self.connection_pool.get_connection(DatabaseOperation.READ) as source_conn:
            # Create the backup
            backup_conn = sqlite3.connect(backup_path)
            try:
                source_conn.backup(backup_conn)
                
                # Verify the backup
                backup_cursor = backup_conn.execute("PRAGMA integrity_check")
                integrity_result = backup_cursor.fetchone()[0]
                
                if integrity_result != "ok":
                    logger.error(f"Backup integrity check failed: {integrity_result}")
                    os.unlink(backup_path)
                    raise ETLError(
                        message=f"Backup verification failed: {integrity_result}",
                        category=ErrorCategory.SYSTEM_ERROR,
                        context=create_error_context(
                            component="DatabaseManager",
                            operation="backup_database",
                            additional_info={"backup_path": backup_path}
                        )
                    )
                
                # Update metadata
                source_conn.execute(
                    """
                    INSERT OR REPLACE INTO etl_metadata (key, value, updated_at)
                    VALUES (?, ?, CURRENT_TIMESTAMP)
                    """,
                    ("last_backup", time.strftime("%Y-%m-%d %H:%M:%S"))
                )
                
                # Update stats
                self.connection_pool.stats.last_backup_time = time.time()
            
            finally:
                backup_conn.close()
        
        logger.info(f"Database backup completed and verified: {backup_path}")
        return backup_path
    
    def optimize_for_queries(self) -> None:
        """Optimize database specifically for query performance"""
        logger.info("Optimizing database for query performance")
        
        with self.connection_pool.get_connection(DatabaseOperation.MAINTENANCE) as conn:
            # Set advanced performance parameters
            conn.execute("PRAGMA cache_size = -131072")  # 128MB cache
            conn.execute("PRAGMA mmap_size = 2147483648")  # 2GB memory mapping
            conn.execute("PRAGMA page_size = 8192")  # 8KB pages (better for larger databases)
            
            # Optimize query planning
            conn.execute("PRAGMA analysis_limit = 1000")
            conn.execute("PRAGMA optimize")
            
            # Update metadata
            conn.execute(
                """
                INSERT OR REPLACE INTO etl_metadata (key, value, updated_at)
                VALUES (?, ?, CURRENT_TIMESTAMP)
                """,
                ("query_optimized", "true")
            )
        
        logger.info("Query optimization completed")
    
    def close(self) -> None:
        """Close all database connections"""
        self.connection_pool.close_all()