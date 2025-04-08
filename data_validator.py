#!/usr/bin/env python3
"""
Data validation module to ensure data quality and integrity.

Part of the Sierra Chart ETL Pipeline project.
"""

import logging
import sqlite3
import time
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass
from enum import Enum
import json
import os
from datetime import datetime, timedelta

from error_handling import ETLError, ErrorCategory, create_error_context
from enhanced_parsers import SC_EPOCH, TimeAndSalesField, DepthRecordField

logger = logging.getLogger("SierraChartETL.validator")

class DataQualityIssue(Enum):
    """Types of data quality issues"""
    MISSING_DATA = "MISSING_DATA"  # Gap in data
    DUPLICATE_DATA = "DUPLICATE_DATA"  # Duplicate records
    INCONSISTENT_DATA = "INCONSISTENT_DATA"  # Data inconsistency
    OUTLIER = "OUTLIER"  # Statistical outlier
    INVALID_VALUE = "INVALID_VALUE"  # Value outside reasonable range

@dataclass
class ValidationResult:
    """Result of a validation check"""
    contract_id: str
    data_type: str  # "tas" or "depth"
    is_valid: bool
    issues: List[Tuple[DataQualityIssue, str]]  # List of (issue type, description)
    timestamp: float = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = time.time()
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            "contract_id": self.contract_id,
            "data_type": self.data_type,
            "is_valid": self.is_valid,
            "issues": [(issue.value, desc) for issue, desc in self.issues],
            "timestamp": self.timestamp
        }

class DataValidator:
    """
    Validates data quality for time & sales and market depth records.
    Performs checks for completeness, consistency, and reasonableness.
    """
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.validation_history = []
        self.conn = None
        self._connect()
    
    def _connect(self):
        """Connect to the database"""
        self.conn = sqlite3.connect(self.db_path)
        self.conn.execute("PRAGMA foreign_keys = ON")
    
    def validate_tas_data(self, contract_id: str) -> ValidationResult:
        """
        Validate time & sales data for a contract
        
        Args:
            contract_id: The contract identifier
            
        Returns:
            ValidationResult with details of any issues found
        """
        table_name = f"{contract_id}_tas"
        issues = []
        
        try:
            cursor = self.conn.execute(
                f"""
                SELECT COUNT(*) FROM sqlite_master 
                WHERE type='table' AND name=?
                """,
                (table_name,)
            )
            
            if cursor.fetchone()[0] == 0:
                return ValidationResult(
                    contract_id=contract_id,
                    data_type="tas",
                    is_valid=False,
                    issues=[(DataQualityIssue.MISSING_DATA, f"Table {table_name} does not exist")]
                )
            
            # Check for empty table
            cursor = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}")
            if cursor.fetchone()[0] == 0:
                return ValidationResult(
                    contract_id=contract_id,
                    data_type="tas",
                    is_valid=False,
                    issues=[(DataQualityIssue.MISSING_DATA, f"No data in {table_name}")]
                )
            
            # Check for duplicate timestamps
            cursor = self.conn.execute(
                f"""
                SELECT timestamp, COUNT(*) as cnt
                FROM {table_name}
                GROUP BY timestamp
                HAVING cnt > 1
                LIMIT 5
                """
            )
            
            duplicates = cursor.fetchall()
            if duplicates:
                for ts, count in duplicates:
                    issues.append((
                        DataQualityIssue.DUPLICATE_DATA,
                        f"Duplicate timestamp {ts} appears {count} times"
                    ))
            
            # Check for large gaps in timestamps
            cursor = self.conn.execute(
                f"""
                SELECT timestamp, LEAD(timestamp) OVER (ORDER BY timestamp) as next_ts,
                       LEAD(timestamp) OVER (ORDER BY timestamp) - timestamp as gap
                FROM {table_name}
                ORDER BY timestamp
                """
            )
            
            rows = cursor.fetchall()
            
            # Define what constitutes a significant gap (e.g., 5 minutes during trading hours)
            gap_threshold = 5 * 60 * 1000000  # 5 minutes in microseconds
            
            for row in rows:
                if row[2] is not None and row[2] > gap_threshold:
                    # Convert microseconds to something readable
                    gap_seconds = row[2] / 1000000
                    
                    # Skip if outside trading hours (approximate)
                    # This is a simplified check - real implementation would use calendar
                    ts_datetime = datetime.fromtimestamp(row[0] / 1000000)
                    hour = ts_datetime.hour
                    minute = ts_datetime.minute
                    
                    # Only report gaps during typical trading hours (8:30 AM - 3:15 PM CT for futures)
                    # This is simplified and would need adjustment for different markets
                    if (8 <= hour < 15) or (hour == 15 and minute <= 15):
                        issues.append((
                            DataQualityIssue.MISSING_DATA,
                            f"Gap of {gap_seconds:.2f} seconds between {row[0]} and {row[1]}"
                        ))
            
            # Check for price outliers
            cursor = self.conn.execute(
                f"""
                SELECT 
                    AVG(price) as avg_price,
                    -- STDEV(price) as std_price,
                    MIN(price) as min_price,
                    MAX(price) as max_price
                FROM {table_name}
                """
            )
            
            stats = cursor.fetchone()
            # Comment out the entire block that uses std_price
            # if stats[0] is not None and stats[1] is not None:
            #     avg_price = stats[0]
            #     std_price = stats[1]
            #     min_price = stats[2]
            #     max_price = stats[3]
            #     
            #     # Look for prices more than N standard deviations from the mean
            #     threshold = 5  # Number of standard deviations
            #     lower_bound = avg_price - threshold * std_price
            #     upper_bound = avg_price + threshold * std_price
            #     
            #     if min_price < lower_bound or max_price > upper_bound:
            #         # Find the specific outliers
            #         cursor = self.conn.execute(
            #             f"""
            #             SELECT timestamp, price
            #             FROM {table_name}
            #             WHERE price < ? OR price > ?
            #             LIMIT 10
            #             """,
            #             (lower_bound, upper_bound)
            #         )
            #         
            #         outliers = cursor.fetchall()
            #         for ts, price in outliers:
            #             issues.append((
            #                 DataQualityIssue.OUTLIER,
            #                 f"Price outlier: {price} at timestamp {ts} (avg: {avg_price:.2f}, std: {std_price:.2f})"
            #             ))
            
            # Check for invalid quantities (zero or negative)
            cursor = self.conn.execute(
                f"""
                SELECT timestamp, qty
                FROM {table_name}
                WHERE qty <= 0
                LIMIT 10
                """
            )
            
            invalid_qty = cursor.fetchall()
            for ts, qty in invalid_qty:
                issues.append((
                    DataQualityIssue.INVALID_VALUE,
                    f"Invalid quantity: {qty} at timestamp {ts}"
                ))
            
            return ValidationResult(
                contract_id=contract_id,
                data_type="tas",
                is_valid=len(issues) == 0,
                issues=issues
            )
        
        except sqlite3.Error as e:
            logger.error(f"Database error during TAS validation: {str(e)}")
            return ValidationResult(
                contract_id=contract_id,
                data_type="tas",
                is_valid=False,
                issues=[(
                    DataQualityIssue.INCONSISTENT_DATA,
                    f"Database error: {str(e)}"
                )]
            )
    
    def validate_depth_data(self, contract_id: str) -> ValidationResult:
        """
        Validate market depth data for a contract
        
        Args:
            contract_id: The contract identifier
            
        Returns:
            ValidationResult with details of any issues found
        """
        table_name = f"{contract_id}_depth"
        issues = []
        
        try:
            cursor = self.conn.execute(
                f"""
                SELECT COUNT(*) FROM sqlite_master 
                WHERE type='table' AND name=?
                """,
                (table_name,)
            )
            
            if cursor.fetchone()[0] == 0:
                return ValidationResult(
                    contract_id=contract_id,
                    data_type="depth",
                    is_valid=False,
                    issues=[(DataQualityIssue.MISSING_DATA, f"Table {table_name} does not exist")]
                )
            
            # Check for empty table
            cursor = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}")
            if cursor.fetchone()[0] == 0:
                return ValidationResult(
                    contract_id=contract_id,
                    data_type="depth",
                    is_valid=False,
                    issues=[(DataQualityIssue.MISSING_DATA, f"No data in {table_name}")]
                )
            
            # Check for large gaps in timestamps
            cursor = self.conn.execute(
                f"""
                SELECT timestamp, LEAD(timestamp) OVER (ORDER BY timestamp) as next_ts,
                       LEAD(timestamp) OVER (ORDER BY timestamp) - timestamp as gap
                FROM (
                    SELECT DISTINCT timestamp
                    FROM {table_name}
                    ORDER BY timestamp
                )
                """
            )
            
            rows = cursor.fetchall()
            
            # For depth data, gaps may be more common, so use a higher threshold
            gap_threshold = 15 * 60 * 1000000  # 15 minutes in microseconds
            
            for row in rows:
                if row[2] is not None and row[2] > gap_threshold:
                    # Convert microseconds to something readable
                    gap_seconds = row[2] / 1000000
                    
                    # Same trading hours check as for TAS
                    ts_datetime = datetime.fromtimestamp(row[0] / 1000000)
                    hour = ts_datetime.hour
                    minute = ts_datetime.minute
                    
                    if (8 <= hour < 15) or (hour == 15 and minute <= 15):
                        issues.append((
                            DataQualityIssue.MISSING_DATA,
                            f"Gap of {gap_seconds:.2f} seconds between {row[0]} and {row[1]}"
                        ))
            
            # Check for negative quantities
            cursor = self.conn.execute(
                f"""
                SELECT timestamp, qty
                FROM {table_name}
                WHERE qty < 0
                LIMIT 10
                """
            )
            
            invalid_qty = cursor.fetchall()
            for ts, qty in invalid_qty:
                issues.append((
                    DataQualityIssue.INVALID_VALUE,
                    f"Negative quantity: {qty} at timestamp {ts}"
                ))
            
            # Check for negative prices
            cursor = self.conn.execute(
                f"""
                SELECT timestamp, price
                FROM {table_name}
                WHERE price < 0
                LIMIT 10
                """
            )
            
            invalid_price = cursor.fetchall()
            for ts, price in invalid_price:
                issues.append((
                    DataQualityIssue.INVALID_VALUE,
                    f"Negative price: {price} at timestamp {ts}"
                ))
            
            # Check for price outliers in relation to the mean
            cursor = self.conn.execute(
                f"""
                SELECT 
                    AVG(price) as avg_price,
                    -- STDEV(price) as std_price,
                    MIN(price) as min_price,
                    MAX(price) as max_price
                FROM {table_name}
                """
            )
            
            stats = cursor.fetchone()
            # Comment out the entire block that uses std_price
            # if stats[0] is not None and stats[1] is not None:
            #     avg_price = stats[0]
            #     std_price = stats[1]
            #     min_price = stats[2]
            #     max_price = stats[3]
            #     
            #     # Higher threshold for depth data as price levels can be more dispersed
            #     threshold = 8  # Number of standard deviations
            #     lower_bound = max(0, avg_price - threshold * std_price)  # Ensure non-negative
            #     upper_bound = avg_price + threshold * std_price
            #     
            #     if min_price < lower_bound or max_price > upper_bound:
            #         # Find the specific outliers
            #         cursor = self.conn.execute(
            #             f"""
            #             SELECT timestamp, price
            #             FROM {table_name}
            #             WHERE price < ? OR price > ?
            #             LIMIT 10
            #             """,
            #             (lower_bound, upper_bound)
            #         )
            #         
            #         outliers = cursor.fetchall()
            #         for ts, price in outliers:
            #             issues.append((
            #                 DataQualityIssue.OUTLIER,
            #                 f"Price outlier: {price} at timestamp {ts} (avg: {avg_price:.2f}, std: {std_price:.2f})"
            #             ))
            
            # Check for consistency - every clear book command should be followed by book rebuilding
            cursor = self.conn.execute(
                f"""
                SELECT timestamp
                FROM {table_name}
                WHERE command = 1  -- CLEAR_BOOK
                ORDER BY timestamp
                """
            )
            
            clear_book_timestamps = [row[0] for row in cursor.fetchall()]
            
            for ts in clear_book_timestamps:
                # Check if add commands follow within a reasonable time
                cursor = self.conn.execute(
                    f"""
                    SELECT COUNT(*) 
                    FROM {table_name}
                    WHERE timestamp > ? 
                    AND timestamp < ? + 60000000  -- 60 seconds in microseconds
                    AND (command = 2 OR command = 3)  -- ADD_BID or ADD_ASK
                    """,
                    (ts, ts)
                )
                
                if cursor.fetchone()[0] == 0:
                    issues.append((
                        DataQualityIssue.INCONSISTENT_DATA,
                        f"Clear book command at {ts} not followed by book rebuilding"
                    ))
            
            return ValidationResult(
                contract_id=contract_id,
                data_type="depth",
                is_valid=len(issues) == 0,
                issues=issues
            )
        
        except sqlite3.Error as e:
            logger.error(f"Database error during depth validation: {str(e)}")
            return ValidationResult(
                contract_id=contract_id,
                data_type="depth",
                is_valid=False,
                issues=[(
                    DataQualityIssue.INCONSISTENT_DATA,
                    f"Database error: {str(e)}"
                )]
            )
    
    def check_data_freshness(self, contract_id: str, data_type: str, max_age_minutes: int = 15) -> ValidationResult:
        """
        Check if the most recent data for a contract is fresh (recent enough)
        
        Args:
            contract_id: The contract identifier
            data_type: "tas" or "depth"
            max_age_minutes: Maximum age in minutes for data to be considered fresh
            
        Returns:
            ValidationResult indicating freshness
        """
        table_name = f"{contract_id}_{data_type}"
        issues = []
        
        try:
            cursor = self.conn.execute(
                f"""
                SELECT MAX(timestamp) FROM {table_name}
                """
            )
            
            max_ts = cursor.fetchone()[0]
            if max_ts is None:
                return ValidationResult(
                    contract_id=contract_id,
                    data_type=data_type,
                    is_valid=False,
                    issues=[(DataQualityIssue.MISSING_DATA, f"No data in {table_name}")]
                )
            
            # Convert Sierra Chart timestamp to datetime
            max_ts_seconds = max_ts / 1000000  # Convert microseconds to seconds
            max_ts_datetime = datetime.fromtimestamp(max_ts_seconds)
            
            # Calculate age in minutes
            age_minutes = (datetime.now() - max_ts_datetime).total_seconds() / 60
            
            if age_minutes > max_age_minutes:
                issues.append((
                    DataQualityIssue.MISSING_DATA,
                    f"Data is stale. Last record is {age_minutes:.2f} minutes old (max: {max_age_minutes})"
                ))
            
            return ValidationResult(
                contract_id=contract_id,
                data_type=data_type,
                is_valid=len(issues) == 0,
                issues=issues
            )
        
        except sqlite3.Error as e:
            logger.error(f"Database error during freshness check: {str(e)}")
            return ValidationResult(
                contract_id=contract_id,
                data_type=data_type,
                is_valid=False,
                issues=[(
                    DataQualityIssue.INCONSISTENT_DATA,
                    f"Database error: {str(e)}"
                )]
            )
    
    def validate_database_integrity(self) -> bool:
        """
        Validate database file integrity
        
        Returns:
            True if database passes integrity check
        """
        try:
            cursor = self.conn.execute("PRAGMA integrity_check")
            result = cursor.fetchone()[0]
            
            if result != "ok":
                logger.error(f"Database integrity check failed: {result}")
                return False
            
            return True
        
        except sqlite3.Error as e:
            logger.error(f"Database error during integrity check: {str(e)}")
            return False
    
    def save_validation_results(self, results: List[ValidationResult], output_path: str) -> None:
        """
        Save validation results to a JSON file
        
        Args:
            results: List of validation results
            output_path: Path to save the results
        """
        try:
            with open(output_path, 'w') as f:
                json.dump(
                    [result.to_dict() for result in results],
                    f,
                    indent=2
                )
            
            logger.info(f"Validation results saved to {output_path}")
        
        except Exception as e:
            logger.error(f"Error saving validation results: {str(e)}")
    
    def run_validation(self, contract_ids: List[str], check_freshness: bool = True) -> List[ValidationResult]:
        """
        Run validation for all specified contracts
        
        Args:
            contract_ids: List of contract identifiers to validate
            check_freshness: Whether to check data freshness
            
        Returns:
            List of validation results
        """
        results = []
        
        # First, check database integrity
        if not self.validate_database_integrity():
            ctx = create_error_context(
                component="DataValidator",
                operation="run_validation",
                additional_info={"db_path": self.db_path}
            )
            raise ETLError(
                message="Database integrity check failed",
                category=ErrorCategory.DATA_ERROR,
                context=ctx
            )
        
        # Validate each contract
        for contract_id in contract_ids:
            # Validate time & sales
            tas_result = self.validate_tas_data(contract_id)
            results.append(tas_result)
            
            # Validate market depth
            depth_result = self.validate_depth_data(contract_id)
            results.append(depth_result)
            
            # Check freshness if requested
            if check_freshness:
                tas_freshness = self.check_data_freshness(contract_id, "tas")
                depth_freshness = self.check_data_freshness(contract_id, "depth")
                results.append(tas_freshness)
                results.append(depth_freshness)
            
            # Log validation results
            if not tas_result.is_valid:
                for issue_type, desc in tas_result.issues:
                    logger.warning(f"Validation issue: {contract_id} (TAS): {issue_type.value} - {desc}")
            
            if not depth_result.is_valid:
                for issue_type, desc in depth_result.issues:
                    logger.warning(f"Validation issue: {contract_id} (Depth): {issue_type.value} - {desc}")
        
        # Save validation history
        self.validation_history.extend(results)
        
        # Return results
        return results
    
    def close(self) -> None:
        """Close database connection"""
        if self.conn:
            self.conn.close()

if __name__ == "__main__":
    # Example usage
    import argparse
    
    parser = argparse.ArgumentParser(description="Validate Sierra Chart data quality")
    parser.add_argument("db_path", help="Path to the database")
    parser.add_argument("contracts", nargs="+", help="Contract IDs to validate")
    parser.add_argument("--output", "-o", help="Output path for validation results")
    parser.add_argument("--no-freshness", action="store_true", help="Skip freshness check")
    
    args = parser.parse_args()
    
    validator = DataValidator(args.db_path)
    try:
        results = validator.run_validation(args.contracts, not args.no_freshness)
        
        if args.output:
            validator.save_validation_results(results, args.output)
        
        # Print summary
        valid_count = sum(1 for r in results if r.is_valid)
        print(f"Validation complete: {valid_count}/{len(results)} checks passed")
        
        # Exit with error code if any checks failed
        sys.exit(0 if valid_count == len(results) else 1)
    
    finally:
        validator.close()