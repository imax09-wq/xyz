#!/usr/bin/env python3
"""
Unit tests for core components of the ETL pipeline.

Part of the Sierra Chart ETL Pipeline project.
"""





#!/usr/bin/env python3
"""
Unit tests for Sierra Chart ETL Pipeline core components.
This module provides comprehensive tests for the core functionality of the ETL pipeline.
"""

import os
import sqlite3
import tempfile
import unittest
import json
import time
import threading
from unittest.mock import MagicMock, patch, mock_open

# Import components to test
from error_handling import ETLError, ErrorCategory, create_error_context, with_retry, RetryConfig
from checkpointing import AtomicConfigManager, CheckpointManager
from db_manager import DatabaseManager, DatabaseOperation
from enhanced_parsers import (
    parse_tas_header, parse_tas, transform_tas,
    parse_depth_header, parse_depth, transform_depth,
    ParserContext, validate_tas_data, validate_depth_data
)
from data_validator import DataValidator, DataQualityIssue, ValidationResult

class TestErrorHandling(unittest.TestCase):
    """Test cases for error handling module."""
    
    def test_create_error_context(self):
        """Test creation of error context."""
        context = create_error_context(
            component="TestComponent",
            operation="test_operation",
            file_path="/path/to/file.txt",
            contract_id="ESM25_FUT_CME"
        )
        
        self.assertEqual(context.component, "TestComponent")
        self.assertEqual(context.operation, "test_operation")
        self.assertEqual(context.file_path, "/path/to/file.txt")
        self.assertEqual(context.contract_id, "ESM25_FUT_CME")
    
    def test_etl_error(self):
        """Test ETL error creation and string representation."""
        context = create_error_context(
            component="TestComponent",
            operation="test_operation"
        )
        
        error = ETLError(
            message="Test error message",
            category=ErrorCategory.SYSTEM_ERROR,
            context=context
        )
        
        self.assertEqual(error.message, "Test error message")
        self.assertEqual(error.category, ErrorCategory.SYSTEM_ERROR)
        self.assertEqual(error.context, context)
        
        # Test string representation
        error_str = str(error)
        self.assertIn("SYSTEM_ERROR", error_str)
        self.assertIn("Test error message", error_str)
        self.assertIn("TestComponent", error_str)
    
    def test_retry_decorator(self):
        """Test the retry decorator functionality."""
        mock_func = MagicMock()
        mock_func.side_effect = [ValueError("First failure"), ValueError("Second failure"), "success"]
        
        # Create a decorated function
        @with_retry(retry_config=RetryConfig(max_retries=3, base_delay=0.1), error_types=(ValueError,))
        def test_func():
            return mock_func()
        
        # Call the function
        result = test_func()
        
        # Assert that the function was called 3 times
        self.assertEqual(mock_func.call_count, 3)
        # Assert that the final result is correct
        self.assertEqual(result, "success")

class TestCheckpointing(unittest.TestCase):
    """Test cases for checkpointing module."""
    
    def setUp(self):
        """Set up test environment."""
        # Create a temporary config file
        self.temp_config = tempfile.NamedTemporaryFile(delete=False, suffix=".json", mode="w+")
        
        # Write sample configuration
        sample_config = {
            "contracts": {
                "ESM25_FUT_CME": {
                    "checkpoint_tas": 100,
                    "checkpoint_depth": {
                        "date": "2025-03-25",
                        "rec": 500
                    },
                    "price_adj": 0.01,
                    "tas": True,
                    "depth": True
                }
            },
            "db_path": "/path/to/db.sqlite",
            "sc_root": "/path/to/SierraChart"
        }
        
        json.dump(sample_config, self.temp_config)
        self.temp_config.flush()
        self.temp_config.close()
        
        # Create config manager
        self.config_manager = AtomicConfigManager(self.temp_config.name)
        
        # Create checkpoint manager
        self.checkpoint_manager = CheckpointManager(self.config_manager)
    
    def tearDown(self):
        """Clean up after tests."""
        os.unlink(self.temp_config.name)
    
    def test_get_tas_checkpoint(self):
        """Test retrieving TAS checkpoint."""
        checkpoint = self.checkpoint_manager.get_tas_checkpoint("ESM25_FUT_CME")
        self.assertEqual(checkpoint, 100)
    
    def test_get_depth_checkpoint(self):
        """Test retrieving depth checkpoint."""
        date, rec = self.checkpoint_manager.get_depth_checkpoint("ESM25_FUT_CME")
        self.assertEqual(date, "2025-03-25")
        self.assertEqual(rec, 500)
    
    def test_update_tas_checkpoint(self):
        """Test updating TAS checkpoint."""
        # Update checkpoint
        self.checkpoint_manager.update_tas_checkpoint("ESM25_FUT_CME", 200)
        
        # Verify the update
        checkpoint = self.checkpoint_manager.get_tas_checkpoint("ESM25_FUT_CME")
        self.assertEqual(checkpoint, 200)
    
    def test_update_depth_checkpoint(self):
        """Test updating depth checkpoint."""
        # Update checkpoint
        self.checkpoint_manager.update_depth_checkpoint("ESM25_FUT_CME", "2025-03-26", 600)
        
        # Verify the update
        date, rec = self.checkpoint_manager.get_depth_checkpoint("ESM25_FUT_CME")
        self.assertEqual(date, "2025-03-26")
        self.assertEqual(rec, 600)
    
    def test_checkpoint_integrity(self):
        """Test checkpoint integrity verification."""
        # This should pass with the current configuration
        self.checkpoint_manager.verify_checkpoint_integrity()
        
        # Now corrupt the configuration
        def corruptor(config):
            del config["contracts"]["ESM25_FUT_CME"]["checkpoint_tas"]
        
        self.config_manager.update_config(corruptor)
        
        # Verification should now fail
        with self.assertRaises(ETLError):
            self.checkpoint_manager.verify_checkpoint_integrity()

class TestDatabaseManager(unittest.TestCase):
    """Test cases for database manager module."""
    
    def setUp(self):
        """Set up test environment."""
        # Create a temporary database file
        self.temp_db = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
        self.temp_db.close()
        
        # Create database manager
        self.db_manager = DatabaseManager(self.temp_db.name, max_connections=3)
    
    def tearDown(self):
        """Clean up after tests."""
        self.db_manager.close()
        os.unlink(self.temp_db.name)
    
    def test_table_creation(self):
        """Test table creation functionality."""
        # Create TAS table
        self.db_manager.ensure_table_exists("TEST_tas", "tas")
        
        # Create depth table
        self.db_manager.ensure_table_exists("TEST_depth", "depth")
        
        # Verify tables exist
        with self.db_manager.connection_pool.get_connection(DatabaseOperation.READ) as conn:
            # Check TAS table
            cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='TEST_tas'"
            )
            self.assertIsNotNone(cursor.fetchone())
            
            # Check depth table
            cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='TEST_depth'"
            )
            self.assertIsNotNone(cursor.fetchone())
    
    def test_index_creation(self):
        """Test index creation functionality."""
        # Create TAS table (which should create indexes)
        self.db_manager.ensure_table_exists("TEST_tas", "tas")
        
        # Verify indexes exist
        with self.db_manager.connection_pool.get_connection(DatabaseOperation.READ) as conn:
            # Check price index
            cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='index' AND name='idx_TEST_tas_price'"
            )
            self.assertIsNotNone(cursor.fetchone())
            
            # Check side index
            cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='index' AND name='idx_TEST_tas_side'"
            )
            self.assertIsNotNone(cursor.fetchone())
    
    def test_load_tas_data(self):
        """Test loading time & sales data."""
        # Create TAS table
        self.db_manager.ensure_table_exists("TEST_tas", "tas")
        
        # Sample data
        records = [
            (1000000, 100.0, 5, 0),
            (1000001, 100.5, 10, 1),
            (1000002, 100.25, 3, 0)
        ]
        
        # Load data
        count = self.db_manager.load_tas_data("TEST", records)
        
        # Verify record count
        self.assertEqual(count, 3)
        
        # Verify data was inserted
        with self.db_manager.connection_pool.get_connection(DatabaseOperation.READ) as conn:
            cursor = conn.execute("SELECT COUNT(*) FROM TEST_tas")
            self.assertEqual(cursor.fetchone()[0], 3)
    
    def test_load_depth_data(self):
        """Test loading market depth data."""
        # Create depth table
        self.db_manager.ensure_table_exists("TEST_depth", "depth")
        
        # Sample data
        records = [
            (1000000, 1, 0, 0, 0.0, 0),  # Clear book
            (1000001, 2, 0, 5, 100.0, 10),  # Add bid
            (1000002, 3, 0, 3, 101.0, 5)   # Add ask
        ]
        
        # Load data
        count = self.db_manager.load_depth_data("TEST", records)
        
        # Verify record count
        self.assertEqual(count, 3)
        
        # Verify data was inserted
        with self.db_manager.connection_pool.get_connection(DatabaseOperation.READ) as conn:
            cursor = conn.execute("SELECT COUNT(*) FROM TEST_depth")
            self.assertEqual(cursor.fetchone()[0], 3)
    
    def test_table_stats(self):
        """Test getting table statistics."""
        # Create and populate tables
        self.db_manager.ensure_table_exists("TEST_tas", "tas")
        self.db_manager.load_tas_data("TEST", [(1000000, 100.0, 5, 0)])
        
        # Get stats
        stats = self.db_manager.get_table_stats()
        
        # Verify stats
        self.assertIn("TEST_tas", stats)
        self.assertEqual(stats["TEST_tas"]["record_count"], 1)
    
    def test_concurrent_connections(self):
        """Test concurrent database connections."""
        # Create a table
        self.db_manager.ensure_table_exists("TEST_tas", "tas")
        
        # Function to run in threads
        def worker(thread_id):
            # Insert some data
            records = [(1000000 + thread_id, 100.0 + thread_id, 5, 0)]
            self.db_manager.load_tas_data("TEST", records)
            
            # Read data
            with self.db_manager.connection_pool.get_connection(DatabaseOperation.READ) as conn:
                cursor = conn.execute("SELECT COUNT(*) FROM TEST_tas")
                count = cursor.fetchone()[0]
                self.assertGreaterEqual(count, 1)
        
        # Create and start threads
        threads = []
        for i in range(5):
            thread = threading.Thread(target=worker, args=(i,))
            threads.append(thread)
            thread.start()
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
        
        # Verify final count
        with self.db_manager.connection_pool.get_connection(DatabaseOperation.READ) as conn:
            cursor = conn.execute("SELECT COUNT(*) FROM TEST_tas")
            self.assertEqual(cursor.fetchone()[0], 5)

class TestParsers(unittest.TestCase):
    """Test cases for parser module."""
    
    def test_transform_tas(self):
        """Test transforming time & sales data."""
        # Sample data
        records = [
            (1000000, 100.0, 5, 0),
            (1000001, 100.5, 10, 1)
        ]
        
        # Apply transformation
        transformed = transform_tas(records, 0.01)
        
        # Verify transformation
        self.assertEqual(transformed[0][0], 1000000)  # Timestamp unchanged
        self.assertEqual(transformed[0][1], 1.0)  # Price adjusted
        self.assertEqual(transformed[0][2], 5)  # Quantity unchanged
        self.assertEqual(transformed[0][3], 0)  # Side unchanged
    
    def test_transform_depth(self):
        """Test transforming market depth data."""
        # Sample data
        records = [
            (1000000, 1, 0, 0, 0.0, 0),
            (1000001, 2, 0, 5, 100.0, 10)
        ]
        
        # Apply transformation
        transformed = transform_depth(records, 0.01)
        
        # Verify transformation
        self.assertEqual(transformed[0][0], 1000000)  # Timestamp unchanged
        self.assertEqual(transformed[0][1], 1)  # Command unchanged
        self.assertEqual(transformed[0][4], 0.0)  # Price unchanged (zero price)
        self.assertEqual(transformed[1][4], 1.0)  # Price adjusted
    
    def test_validate_tas_data(self):
        """Test validation of time & sales data."""
        # Valid data
        valid_records = [
            (1000000, 100.0, 5, 0),
            (1000001, 100.5, 10, 1)
        ]
        
        context = ParserContext(
            contract_id="TEST",
            file_path="/path/to/file.scid",
            checkpoint=0
        )
        
        # Validate
        self.assertTrue(validate_tas_data(valid_records, context))
        
        # Invalid data (decreasing timestamp)
        invalid_records = [
            (1000001, 100.0, 5, 0),
            (1000000, 100.5, 10, 1)
        ]
        
        # Validate
        self.assertFalse(validate_tas_data(invalid_records, context))
    
    def test_validate_depth_data(self):
        """Test validation of market depth data."""
        # Valid data
        valid_records = [
            (1000000, 1, 0, 0, 0.0, 0),
            (1000001, 2, 0, 5, 100.0, 10)
        ]
        
        context = ParserContext(
            contract_id="TEST",
            file_path="/path/to/file.depth",
            checkpoint=0
        )
        
        # Validate
        self.assertTrue(validate_depth_data(valid_records, context))

class TestDataValidator(unittest.TestCase):
    """Test cases for data validator module."""
    
    def setUp(self):
        """Set up test environment."""
        # Create a temporary database file
        self.temp_db = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
        self.temp_db.close()
        
        # Create tables and insert test data
        conn = sqlite3.connect(self.temp_db.name)
        
        # Create TAS table
        conn.execute("""
            CREATE TABLE ESM25_FUT_CME_tas (
                timestamp INTEGER PRIMARY KEY,
                price REAL,
                qty INTEGER,
                side INTEGER
            )
        """)
        
        # Insert TAS data
        tas_data = [
            (1000000, 100.0, 5, 0),
            (1000001, 100.5, 10, 1),
            (1000002, 100.25, 3, 0)
        ]
        
        conn.executemany(
            "INSERT INTO ESM25_FUT_CME_tas VALUES (?, ?, ?, ?)",
            tas_data
        )
        
        # Create depth table
        conn.execute("""
            CREATE TABLE ESM25_FUT_CME_depth (
                timestamp INTEGER,
                command INTEGER,
                flags INTEGER,
                num_orders INTEGER,
                price REAL,
                qty INTEGER,
                PRIMARY KEY (timestamp, command, price)
            )
        """)
        
        # Insert depth data
        depth_data = [
            (1000000, 1, 0, 0, 0.0, 0),
            (1000001, 2, 0, 5, 100.0, 10),
            (1000002, 3, 0, 3, 101.0, 5)
        ]
        
        conn.executemany(
            "INSERT INTO ESM25_FUT_CME_depth VALUES (?, ?, ?, ?, ?, ?)",
            depth_data
        )
        
        conn.commit()
        conn.close()
        
        # Create validator
        self.validator = DataValidator(self.temp_db.name)
    
    def tearDown(self):
        """Clean up after tests."""
        self.validator.close()
        os.unlink(self.temp_db.name)
    
    def test_validate_tas_data(self):
        """Test validation of time & sales data."""
        result = self.validator.validate_tas_data("ESM25_FUT_CME")
        
        # Should be valid
        self.assertTrue(result.is_valid)
        self.assertEqual(len(result.issues), 0)
    
    def test_validate_depth_data(self):
        """Test validation of market depth data."""
        result = self.validator.validate_depth_data("ESM25_FUT_CME")
        
        # Should be valid
        self.assertTrue(result.is_valid)
        self.assertEqual(len(result.issues), 0)
    
    def test_validate_nonexistent_data(self):
        """Test validation of nonexistent data."""
        result = self.validator.validate_tas_data("NONEXISTENT")
        
        # Should be invalid
        self.assertFalse(result.is_valid)
        self.assertEqual(len(result.issues), 1)
        self.assertEqual(result.issues[0][0], DataQualityIssue.MISSING_DATA)

if __name__ == '__main__':
    unittest.main()