#!/usr/bin/env python3
"""
Data repair and maintenance utility for Sierra Chart ETL Pipeline.

This tool helps identify, diagnose, and fix common issues in Sierra Chart data files.
"""

import argparse
import logging
import os
import sys
import json
import time
import re
import sqlite3
from typing import Dict, List, Tuple, Optional, Set, Any
import traceback

# Import our modules
from error_handling import ETLError, ErrorCategory, create_error_context
from enhanced_parsers import (
    TimeAndSalesField, DepthRecordField, DepthCommand,
    INTRADAY_HEADER_LEN, INTRADAY_REC_LEN, INTRADAY_REC_UNPACK,
    DEPTH_HEADER_LEN, DEPTH_REC_LEN, DEPTH_REC_UNPACK
)
from db_manager import DatabaseManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("data_repair.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("SierraChartETL.repair")

class DataRepairTool:
    """Tool for repairing and diagnosing Sierra Chart data issues"""
    
    def __init__(self, config_path: str):
        # Load configuration
        with open(config_path, 'r') as f:
            self.config = json.load(f)
        
        # Set up file paths
        self.sc_root = self.config.get("sc_root", "C:/SierraChart")
        self.data_dir = os.path.join(self.sc_root, "Data")
        self.depth_dir = os.path.join(self.data_dir, "MarketDepthData")
        
        # Initialize database connection
        self.db_manager = DatabaseManager(self.config["db_path"])
        
        # Track known issues
        self.known_issues = {}
        
        # Load known issues from config if present
        if "error_handling" in self.config and "known_problem_records" in self.config["error_handling"]:
            self.known_issues = self.config["error_handling"]["known_problem_records"]
    
    def analyze_tas_file(self, contract_id: str, verbose: bool = False, fix: bool = False) -> Dict[str, Any]:
        """
        Analyze a time and sales file for issues
        
        Args:
            contract_id: Contract identifier
            verbose: Whether to print detailed output
            fix: Whether to attempt to fix issues
            
        Returns:
            Dictionary with analysis results
        """
        file_path = os.path.join(self.data_dir, f"{contract_id}.scid")
        
        if not os.path.exists(file_path):
            logger.error(f"File not found: {file_path}")
            return {"error": "File not found", "file_path": file_path}
        
        logger.info(f"Analyzing TAS file for {contract_id}: {file_path}")
        
        results = {
            "file_path": file_path,
            "file_size": os.path.getsize(file_path),
            "timestamp_irregularities": [],
            "value_irregularities": [],
            "potential_fixes": [],
            "fixed_issues": []
        }
        
        try:
            with open(file_path, "rb") as fd:
                # Skip header
                fd.seek(INTRADAY_HEADER_LEN)
                
                # Read all records
                position = 0
                prev_ts = None
                irregular_positions = []
                records = []
                
                while True:
                    rec_bytes = fd.read(INTRADAY_REC_LEN)
                    if not rec_bytes or len(rec_bytes) < INTRADAY_REC_LEN:
                        break
                    
                    try:
                        ir = INTRADAY_REC_UNPACK(rec_bytes)
                        ts = ir[0]  # Timestamp
                        
                        # Check for decreasing timestamps
                        if prev_ts is not None and ts < prev_ts:
                            diff = prev_ts - ts
                            abs_position = INTRADAY_HEADER_LEN + position * INTRADAY_REC_LEN
                            
                            irregularity = {
                                "position": position,
                                "file_offset": abs_position,
                                "previous_ts": prev_ts,
                                "current_ts": ts,
                                "difference": diff
                            }
                            
                            results["timestamp_irregularities"].append(irregularity)
                            irregular_positions.append(position)
                            
                            if verbose:
                                logger.info(f"Timestamp irregularity at position {position}: {prev_ts} -> {ts} (diff: {diff})")
                        
                        # Check for invalid values
                        price = ir[1]  # Price
                        qty_bid = ir[6]  # Bid volume
                        qty_ask = ir[7]  # Ask volume
                        
                        if price <= 0 or (qty_bid <= 0 and qty_ask <= 0):
                            abs_position = INTRADAY_HEADER_LEN + position * INTRADAY_REC_LEN
                            
                            value_issue = {
                                "position": position,
                                "file_offset": abs_position,
                                "price": price,
                                "bid_vol": qty_bid,
                                "ask_vol": qty_ask
                            }
                            
                            results["value_irregularities"].append(value_issue)
                            
                            if verbose:
                                logger.info(f"Value irregularity at position {position}: price={price}, bid_vol={qty_bid}, ask_vol={qty_ask}")
                        
                        # Add to records for further analysis
                        records.append((position, ts, price, qty_bid, qty_ask))
                        
                        prev_ts = ts
                        position += 1
                    
                    except Exception as e:
                        logger.error(f"Error parsing record at position {position}: {str(e)}")
                        position += 1
                
                # Analyze patterns of irregular records
                if irregular_positions and len(irregular_positions) > 1:
                    # Look for patterns in positions
                    intervals = [irregular_positions[i+1] - irregular_positions[i] for i in range(len(irregular_positions)-1)]
                    
                    if len(set(intervals)) == 1:
                        pattern = {
                            "type": "regular_interval",
                            "interval": intervals[0],
                            "positions": irregular_positions
                        }
                        results["pattern"] = pattern
                        
                        if verbose:
                            logger.info(f"Found regular pattern: irregular records appear every {intervals[0]} positions")
                
                # If requested, try to fix issues
                if fix and results["timestamp_irregularities"]:
                    logger.info(f"Attempting to fix {len(results['timestamp_irregularities'])} timestamp irregularities")
                    
                    # Create a fixed file
                    fixed_file = f"{file_path}.fixed"
                    with open(file_path, "rb") as src, open(fixed_file, "wb") as dst:
                        # Copy header
                        header = src.read(INTRADAY_HEADER_LEN)
                        dst.write(header)
                        
                        # Process records
                        position = 0
                        prev_ts = None
                        
                        while True:
                            rec_bytes = src.read(INTRADAY_REC_LEN)
                            if not rec_bytes or len(rec_bytes) < INTRADAY_REC_LEN:
                                break
                            
                            try:
                                # Check if this position needs fixing
                                needs_fix = position in irregular_positions
                                
                                if needs_fix and prev_ts is not None:
                                    # Fix timestamp by ensuring it's > prev_ts
                                    ir = list(INTRADAY_REC_UNPACK(rec_bytes))
                                    ir[0] = prev_ts + 1  # Make it 1 microsecond after prev_ts
                                    
                                    # Create modified record
                                    import struct
                                    fixed_bytes = struct.pack(
                                        "q4f4I",
                                        ir[0], ir[1], ir[2], ir[3], ir[4], ir[5], ir[6], ir[7], ir[8]
                                    )
                                    dst.write(fixed_bytes)
                                    
                                    fix_info = {
                                        "position": position,
                                        "original_ts": INTRADAY_REC_UNPACK(rec_bytes)[0],
                                        "fixed_ts": ir[0]
                                    }
                                    results["fixed_issues"].append(fix_info)
                                else:
                                    # Copy unchanged
                                    dst.write(rec_bytes)
                                    ir = INTRADAY_REC_UNPACK(rec_bytes)
                                
                                prev_ts = ir[0]
                                position += 1
                            
                            except Exception as e:
                                logger.error(f"Error processing record at position {position}: {str(e)}")
                                dst.write(rec_bytes)  # Write original on error
                                position += 1
                    
                    logger.info(f"Fixed file created at {fixed_file}")
                    
                    # Backup original
                    import shutil
                    backup_file = f"{file_path}.bak"
                    shutil.copy2(file_path, backup_file)
                    logger.info(f"Original file backed up to {backup_file}")
                    
                    # Replace original with fixed file
                    os.replace(fixed_file, file_path)
                    logger.info(f"Original file replaced with fixed version")
                    
                    # Update config with known issues
                    self._update_known_issues(contract_id, irregular_positions)
        
        except Exception as e:
            logger.error(f"Error analyzing file: {str(e)}\n{traceback.format_exc()}")
            results["error"] = str(e)
        
        return results
    
    def _update_known_issues(self, contract_id: str, problem_positions: List[int]) -> None:
        """
        Update the known issues in the configuration
        
        Args:
            contract_id: Contract identifier
            problem_positions: List of problematic record positions
        """
        if "error_handling" not in self.config:
            self.config["error_handling"] = {}
        
        if "known_problem_records" not in self.config["error_handling"]:
            self.config["error_handling"]["known_problem_records"] = {}
        
        # Convert to absolute positions (checkpoint values)
        absolute_positions = [INTRADAY_HEADER_LEN + pos for pos in problem_positions]
        
        # Update the config
        self.config["error_handling"]["known_problem_records"][contract_id] = absolute_positions
        
        # Save the updated config
        with open("config.json", 'w') as f:
            json.dump(self.config, f, indent=2)
        
        logger.info(f"Updated config with {len(problem_positions)} known problem records for {contract_id}")
    
    def analyze_database_integrity(self, contract_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Analyze database integrity and look for potential issues
        
        Args:
            contract_id: Optional contract to focus on, or None for all
            
        Returns:
            Dictionary with analysis results
        """
        results = {
            "db_path": self.db_manager.db_path,
            "integrity_check": None,
            "table_stats": {},
            "issues": []
        }
        
        logger.info(f"Analyzing database integrity for {self.db_manager.db_path}")
        
        try:
            # Check overall database integrity
            with self.db_manager.connection_pool.get_connection() as conn:
                cursor = conn.execute("PRAGMA integrity_check")
                integrity_result = cursor.fetchone()[0]
                results["integrity_check"] = integrity_result
                
                if integrity_result != "ok":
                    logger.error(f"Database integrity check failed: {integrity_result}")
                    results["issues"].append({
                        "type": "integrity_failure",
                        "message": integrity_result
                    })
                else:
                    logger.info("Database integrity check passed")
                
                # Get list of tables to analyze
                if contract_id:
                    tables = [f"{contract_id}_tas", f"{contract_id}_depth"]
                else:
                    cursor = conn.execute(
                        """
                        SELECT name FROM sqlite_master 
                        WHERE type='table' 
                        AND name NOT LIKE 'sqlite%' 
                        AND name NOT LIKE 'etl_%'
                        """
                    )
                    tables = [row[0] for row in cursor.fetchall()]
                
                # Check each table
                for table in tables:
                    try:
                        # Get basic stats
                        cursor = conn.execute(f"SELECT COUNT(*) FROM {table}")
                        count = cursor.fetchone()[0]
                        
                        # Get timestamp range if applicable
                        ts_range = None
                        try:
                            cursor = conn.execute(f"SELECT MIN(timestamp), MAX(timestamp) FROM {table}")
                            min_ts, max_ts = cursor.fetchone()
                            ts_range = {"min": min_ts, "max": max_ts}
                        except:
                            pass
                        
                        results["table_stats"][table] = {
                            "record_count": count,
                            "timestamp_range": ts_range
                        }
                        
                        # Check for timestamp gaps
                        if ts_range and ts_range["min"] and ts_range["max"]:
                            # For TAS tables, check for significant gaps
                            if table.endswith("_tas"):
                                cursor = conn.execute(f"""
                                    SELECT t1.timestamp as ts1, 
                                           (SELECT MIN(timestamp) FROM {table} t2 
                                            WHERE t2.timestamp > t1.timestamp) as ts2,
                                           (SELECT MIN(timestamp) FROM {table} t2 
                                            WHERE t2.timestamp > t1.timestamp) - t1.timestamp as gap
                                    FROM {table} t1
                                    WHERE gap > 3600000000  -- 1 hour in microseconds
                                    ORDER BY gap DESC
                                    LIMIT 10
                                """)
                                
                                gaps = cursor.fetchall()
                                if gaps:
                                    logger.info(f"Found {len(gaps)} significant gaps in {table}")
                                    results["table_stats"][table]["gaps"] = [
                                        {"start": row[0], "end": row[1], "duration_us": row[2]}
                                        for row in gaps
                                    ]
                    
                    except Exception as e:
                        logger.error(f"Error analyzing table {table}: {str(e)}")
                        results["issues"].append({
                            "type": "table_error",
                            "table": table,
                            "message": str(e)
                        })
        
        except Exception as e:
            logger.error(f"Error during database analysis: {str(e)}\n{traceback.format_exc()}")
            results["error"] = str(e)
        
        return results
    
    def repair_database(self, vacuum: bool = True, reindex: bool = True, 
                        optimize: bool = True, backup: bool = True) -> Dict[str, Any]:
        """
        Perform database repairs and optimization
        
        Args:
            vacuum: Whether to vacuum the database
            reindex: Whether to rebuild indexes
            optimize: Whether to optimize for queries
            backup: Whether to create a backup first
            
        Returns:
            Dictionary with repair results
        """
        results = {
            "db_path": self.db_manager.db_path,
            "actions": [],
            "errors": []
        }
        
        logger.info(f"Repairing database {self.db_manager.db_path}")
        
        try:
            # First, create a backup if requested
            if backup:
                try:
                    backup_path = self.db_manager.backup_database()
                    results["actions"].append({
                        "action": "backup",
                        "status": "success",
                        "backup_path": backup_path
                    })
                    logger.info(f"Database backed up to {backup_path}")
                except Exception as e:
                    logger.error(f"Backup failed: {str(e)}")
                    results["errors"].append({
                        "action": "backup",
                        "error": str(e)
                    })
            
            # Perform requested operations
            if vacuum:
                try:
                    logger.info("Running VACUUM on database")
                    with self.db_manager.connection_pool.get_connection() as conn:
                        conn.execute("VACUUM")
                    
                    results["actions"].append({
                        "action": "vacuum",
                        "status": "success"
                    })
                    logger.info("VACUUM completed successfully")
                except Exception as e:
                    logger.error(f"VACUUM failed: {str(e)}")
                    results["errors"].append({
                        "action": "vacuum",
                        "error": str(e)
                    })
            
            if reindex:
                try:
                    logger.info("Rebuilding database indexes")
                    
                    # Get all tables
                    with self.db_manager.connection_pool.get_connection() as conn:
                        cursor = conn.execute(
                            """
                            SELECT name FROM sqlite_master 
                            WHERE type='table' 
                            AND name NOT LIKE 'sqlite%' 
                            AND name NOT LIKE 'etl_%'
                            """
                        )
                        tables = [row[0] for row in cursor.fetchall()]
                    
                    # For each table, determine type and recreate indexes
                    for table in tables:
                        if table.endswith("_tas"):
                            self.db_manager.ensure_table_exists(table, "tas")
                        elif table.endswith("_depth"):
                            self.db_manager.ensure_table_exists(table, "depth")
                    
                    results["actions"].append({
                        "action": "reindex",
                        "status": "success",
                        "tables": tables
                    })
                    logger.info(f"Rebuilt indexes for {len(tables)} tables")
                except Exception as e:
                    logger.error(f"Reindex failed: {str(e)}\n{traceback.format_exc()}")
                    results["errors"].append({
                        "action": "reindex",
                        "error": str(e)
                    })
            
            if optimize:
                try:
                    logger.info("Optimizing database for query performance")
                    self.db_manager.optimize_for_queries()
                    
                    results["actions"].append({
                        "action": "optimize",
                        "status": "success"
                    })
                    logger.info("Database optimization completed")
                except Exception as e:
                    logger.error(f"Optimization failed: {str(e)}")
                    results["errors"].append({
                        "action": "optimize",
                        "error": str(e)
                    })
        
        except Exception as e:
            logger.error(f"Error during database repair: {str(e)}\n{traceback.format_exc()}")
            results["error"] = str(e)
        
        return results
    
    def close(self):
        """Clean up resources"""
        self.db_manager.close()

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="Sierra Chart Data Repair Tool")
    
    parser.add_argument(
        "--config", type=str, default="./config.json",
        help="Path to configuration file (default: ./config.json)"
    )
    
    parser.add_argument(
        "--log-level", type=str, choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO", help="Set the logging level"
    )
    
    # Create subparsers for different commands
    subparsers = parser.add_subparsers(dest="command", help="Command to execute")
    
    # analyze-tas command
    analyze_tas_parser = subparsers.add_parser("analyze-tas", help="Analyze time and sales file")
    analyze_tas_parser.add_argument("contract_id", help="Contract identifier")
    analyze_tas_parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    analyze_tas_parser.add_argument("--fix", "-f", action="store_true", help="Fix issues if found")
    
    # analyze-db command
    analyze_db_parser = subparsers.add_parser("analyze-db", help="Analyze database integrity")
    analyze_db_parser.add_argument("--contract", "-c", help="Optional contract to focus on")
    
    # repair-db command
    repair_db_parser = subparsers.add_parser("repair-db", help="Repair database")
    repair_db_parser.add_argument("--no-vacuum", action="store_true", help="Skip VACUUM")
    repair_db_parser.add_argument("--no-reindex", action="store_true", help="Skip rebuilding indexes")
    repair_db_parser.add_argument("--no-optimize", action="store_true", help="Skip query optimization")
    repair_db_parser.add_argument("--no-backup", action="store_true", help="Skip creating backup")
    
    args = parser.parse_args()
    
    # Set log level
    logging.getLogger().setLevel(getattr(logging, args.log_level))
    
    repair_tool = DataRepairTool(args.config)
    
    try:
        if args.command == "analyze-tas":
            results = repair_tool.analyze_tas_file(args.contract_id, args.verbose, args.fix)
            
            # Print summary
            print("\nAnalysis Summary:")
            print(f"File: {results['file_path']}")
            print(f"Size: {results['file_size']} bytes")
            print(f"Timestamp irregularities: {len(results['timestamp_irregularities'])}")
            print(f"Value irregularities: {len(results['value_irregularities'])}")
            
            if args.fix and "fixed_issues" in results:
                print(f"Fixed issues: {len(results['fixed_issues'])}")
        
        elif args.command == "analyze-db":
            results = repair_tool.analyze_database_integrity(args.contract)
            
            # Print summary
            print("\nDatabase Analysis Summary:")
            print(f"Database: {results['db_path']}")
            print(f"Integrity check: {results['integrity_check']}")
            print(f"Tables analyzed: {len(results['table_stats'])}")
            print(f"Issues found: {len(results['issues'])}")
            
            # Print table stats
            for table, stats in results['table_stats'].items():
                print(f"\nTable: {table}")
                print(f"  Records: {stats['record_count']}")
                if stats.get('timestamp_range'):
                    ts_range = stats['timestamp_range']
                    if ts_range['min'] and ts_range['max']:
                        span_hours = (ts_range['max'] - ts_range['min']) / 3600000000
                        print(f"  Timespan: {span_hours:.2f} hours")
                if 'gaps' in stats:
                    print(f"  Significant gaps: {len(stats['gaps'])}")
        
        elif args.command == "repair-db":
            results = repair_tool.repair_database(
                vacuum=not args.no_vacuum,
                reindex=not args.no_reindex,
                optimize=not args.no_optimize,
                backup=not args.no_backup
            )
            
            # Print summary
            print("\nDatabase Repair Summary:")
            print(f"Database: {results['db_path']}")
            print(f"Actions performed: {len(results['actions'])}")
            print(f"Errors encountered: {len(results['errors'])}")
            
            for action in results['actions']:
                print(f"  {action['action']}: {action['status']}")
        
        else:
            parser.print_help()
    
    finally:
        repair_tool.close()

if __name__ == "__main__":
    main()