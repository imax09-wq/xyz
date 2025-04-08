# Sierra Chart ETL Pipeline - Enhanced Edition

## Executive Summary of Improvements

The enhanced Sierra Chart ETL Pipeline addresses several critical limitations in the original implementation to provide a robust foundation for algorithmic trading. These improvements enable continuous 24/7 operation while maintaining high-quality historical data for both live trading and backtesting.

### Major Enhancements:

1. **Smart Error Recovery**
   - Intelligently handles problematic records without failing entire batches
   - Implements timestamp correction for minor inconsistencies
   - Adds configurable tolerance thresholds for different data types

2. **Flexible File Handling**
   - Supports multiple Sierra Chart naming conventions
   - Automatically detects and processes related contract files
   - Handles file format variations and extensions

3. **Enhanced Database Management**
   - Optimized connection pooling with deadlock prevention
   - Query performance tuning for time-series data
   - Advanced integrity monitoring and self-healing

4. **Data Quality Metrics**
   - Comprehensive validation with configurable strictness
   - Records statistics for data quality tracking
   - Identifies patterns in problematic data

5. **Diagnostic and Repair Utilities**
   - New `data_repair.py` utility for analyzing and fixing issues
   - Automated mapping of known problem records
   - Database optimization and maintenance tools

## Detailed Improvements

### 1. Smart Error Recovery

#### Timestamp Validation Flexibility
The original system rejected entire batches due to timestamp irregularities. The enhanced version:

```json
"validation": {
  "timestamp_tolerance_tas": 10000,   // 10ms tolerance for TAS data
  "timestamp_tolerance_depth": 1000000,  // 1s tolerance for depth data
  "max_anomalies_percent_tas": 2.0,  // Allow up to 2% anomalies
  "max_anomalies_percent_depth": 5.0  // Allow up to 5% anomalies for depth
}
```

#### Selective Record Filtering
Instead of failing entire batches, the system now filters problematic records:

```python
# In enhanced_parsers.py
def filter_and_transform_tas(records, context, price_adj):
    # Filters out problematic records while keeping good ones
    # Adjusts timestamps for minor inconsistencies
```

#### Known Problem Records
The system now tracks specific problematic record positions and handles them specially:

```json
"error_handling": {
  "known_problem_records": {
    "ESM25_FUT_CME": [7142862]  // Known problematic record position
  }
}
```

### 2. Flexible File Handling

#### Enhanced Filename Pattern Matching
Now supports multiple filename formats through pattern-based detection:

```python
# Patterns for matching depth files
patterns = [
    # Format 1: "ESM25_FUT_CME.2025-04-04.depth"
    (r"^(.+)\.(\d{4}-\d{2}-\d{2})\.depth$", lambda m: (m.group(1), m.group(2))),
    
    # Format 2: "ESH5.CME.2025-01-12.depth"
    (r"^([A-Z]+[A-Z0-9]+)\.([A-Z]+)\.(\d{4}-\d{2}-\d{2})\.depth$", 
     lambda m: (f"{m.group(1)}_{contract_type}_{m.group(2)}", m.group(3))),
    
    # Additional formats...
]
```

#### Symbol-Based Matching
Intelligently matches files based on symbol components:

```python
if file_contract.startswith(base_symbol) and exchange in file_contract:
    logger.info(f"Found related contract file: {filename} matches {base_symbol}/{exchange}")
    is_match = True
```

#### Extension Handling
Handles both `.depth` and `.dept` extensions and other common variations.

### 3. Enhanced Database Management

#### Optimized Connection Pool
Improved connection management prevents deadlocks and ensures efficient resource usage:

```python
class ConnectionPool:
    # Enhanced with:
    # - Critical section locks for exclusive operations
    # - Connection health validation
    # - Automatic retry mechanisms
    # - Connection state tracking
```

#### Performance Optimizations
Database is now optimized specifically for time-series data:

```python
# Advanced SQLite performance settings
conn.execute("PRAGMA synchronous = NORMAL")  # Balance between safety and performance
conn.execute("PRAGMA cache_size = -65536")   # 64MB cache (larger than default)
conn.execute("PRAGMA mmap_size = 1073741824")  # 1GB memory mapping for faster reads
conn.execute("PRAGMA temp_store = MEMORY")  # Store temp tables in memory
```

#### Enhanced Indexing Strategies
Indexes are optimized for common query patterns:

```python
# TAS table indexes
conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_price ON {table_name}(price)")
conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_side_price ON {table_name}(side, price)")
conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_price_qty ON {table_name}(price, qty)")
```

### 4. Data Quality Metrics

#### Quality Tracking
The system now records data quality metrics for analysis:

```python
def add_data_quality_metric(self, table_name, metric_name, metric_value, notes=None):
    # Records various quality metrics over time for trend analysis
```

#### Comprehensive Statistics
Detailed statistics provide insights into data quality:

```python
cursor = conn.execute(
    f"""
    SELECT 
        AVG(price) as avg_price,
        STDEV(price) as std_price,
        MIN(price) as min_price,
        MAX(price) as max_price
    FROM {table_name}
    """
)
```

#### Gap Detection
Identifies and tracks significant data gaps:

```python
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
```

### 5. Diagnostic and Repair Utilities

#### Data Repair Tool
New utility specifically for diagnosing and fixing data issues:

```python
class DataRepairTool:
    def analyze_tas_file(self, contract_id, verbose=False, fix=False):
        # Analyzes TAS files for issues and can fix them
    
    def repair_database(self, vacuum=True, reindex=True, optimize=True, backup=True):
        # Comprehensive database repair functions
```

#### Automatic Problem Detection
Detects patterns in problematic data:

```python
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
```

#### Database Integrity Verification
Comprehensive database integrity checks:

```python
def analyze_database_integrity(self, contract_id=None):
    # Performs multiple integrity checks
    # Identifies table-specific issues
    # Tracks data consistency
```

## Configuration Guide

The enhanced configuration file includes several new sections:

### Validation Settings

```json
"validation": {
  "timestamp_tolerance_tas": 10000,
  "timestamp_tolerance_depth": 1000000,
  "max_anomalies_percent_tas": 2.0,
  "max_anomalies_percent_depth": 5.0,
  "filter_problematic_records": true
}
```

| Setting | Description | Default |
|---------|-------------|---------|
| `timestamp_tolerance_tas` | Maximum allowed timestamp decreases in microseconds for TAS data | 10000 (10ms) |
| `timestamp_tolerance_depth` | Maximum allowed timestamp decreases for depth data | 1000000 (1s) |
| `max_anomalies_percent_tas` | Maximum percentage of problematic records allowed | 2.0% |
| `max_anomalies_percent_depth` | Maximum percentage of problematic records allowed for depth | 5.0% |
| `filter_problematic_records` | Whether to filter out problematic records | true |

### Error Handling Settings

```json
"error_handling": {
  "skip_invalid_batches": true,
  "run_data_validator_on_health_check": true,
  "retry_failed_operations": true,
  "known_problem_records": {
    "ESM25_FUT_CME": [7142862]
  }
}
```

| Setting | Description | Default |
|---------|-------------|---------|
| `skip_invalid_batches` | Whether to skip over invalid batches | true |
| `run_data_validator_on_health_check` | Run validation during health checks | true |
| `retry_failed_operations` | Automatically retry failed operations | true |
| `known_problem_records` | Map of contract IDs to problematic record positions | {} |

### Monitoring Settings

```json
"monitoring": {
  "alert_on_errors": true,
  "max_error_rate": 10,
  "stalled_processing_threshold_minutes": 15,
  "low_disk_space_threshold_gb": 5
}
```

| Setting | Description | Default |
|---------|-------------|---------|
| `alert_on_errors` | Whether to generate alerts on errors | true |
| `max_error_rate` | Maximum acceptable error rate per hour | 10 |
| `stalled_processing_threshold_minutes` | Minutes of inactivity before alert | 15 |
| `low_disk_space_threshold_gb` | Low disk space threshold in GB | 5 |

## Usage Instructions

### Running the Enhanced Pipeline

```bash
# Run in continuous mode (24/7 operation)
python enhanced_etl.py 1 --config ./config.json

# Run once for testing
python enhanced_etl.py 0 --config ./config.json
```

### Using the Data Repair Utility

```bash
# Analyze TAS file for issues
python data_repair.py analyze-tas ESM25_FUT_CME --verbose

# Fix issues in TAS file
python data_repair.py analyze-tas ESM25_FUT_CME --fix

# Analyze database integrity
python data_repair.py analyze-db --contract ESM25_FUT_CME

# Repair and optimize the database
python data_repair.py repair-db
```

### Monitoring Data Quality

The system maintains comprehensive data quality metrics that can be queried:

```python
metrics = db_manager.get_data_quality_metrics(
    table_name="ESM25_FUT_CME_tas",
    metric_name="timestamp_consistency",
    limit=10
)
```

## Troubleshooting Guide

### Common Issues and Solutions

#### Issue: Decreasing Timestamps in TAS Data

**Symptoms**: Log messages like "Decreasing timestamp at record 7142862"

**Solution**:
1. Run the data repair utility to analyze the file:
   ```bash
   python data_repair.py analyze-tas ESM25_FUT_CME --verbose
   ```
2. Fix the file if issues are found:
   ```bash
   python data_repair.py analyze-tas ESM25_FUT_CME --fix
   ```
3. Add the problematic record position to your config:
   ```json
   "known_problem_records": {
     "ESM25_FUT_CME": [7142862]
   }
   ```

#### Issue: Unexpected Depth Filename Format

**Symptoms**: Log messages like "Unexpected depth filename format: ESH5.CME.2025-01-12.depth"

**Solution**: The enhanced version should automatically handle various formats. If problems persist:

1. Check if the base symbol is correctly extracted from your contract_id
2. Add a custom pattern in `_get_depth_files()` for your specific format
3. Verify that your depth files are in the correct directory

#### Issue: Database Locked Errors

**Symptoms**: "database is locked" errors during high-volume processing

**Solution**:
1. Increase the database timeout:
   ```json
   "database": {
     "connection_timeout": 60,
     "max_connections": 5
   }
   ```
2. Reduce concurrent operations by adjusting batch size:
   ```json
   "batch_size": 2500
   ```
3. Run database optimization:
   ```bash
   python data_repair.py repair-db --no-backup
   ```

#### Issue: High Memory Usage

**Symptoms**: ETL process consuming excessive memory over time

**Solution**:
1. Reduce cache size in db_manager.py:
   ```python
   conn.execute("PRAGMA cache_size = -32768")  # 32MB instead of 64MB
   ```
2. Process in smaller batches:
   ```json
   "batch_size": 1000
   ```
3. Add periodic garbage collection:
   ```python
   # Add to ETLPipeline run() method, after processing cycle
   import gc
   gc.collect()
   ```

## Performance Optimization Tips

### Database Performance

1. **Adjust cache size** based on your system memory:
   ```python
   # For systems with 16GB+ RAM:
   conn.execute("PRAGMA cache_size = -131072")  # 128MB
   
   # For systems with 8GB RAM:
   conn.execute("PRAGMA cache_size = -65536")   # 64MB
   
   # For systems with 4GB RAM:
   conn.execute("PRAGMA cache_size = -32768")   # 32MB
   ```

2. **Run regular maintenance** to optimize database performance:
   ```bash
   # Set up a daily maintenance task
   python data_repair.py repair-db --no-backup
   ```

3. **Adjust indexes** for your most common query patterns:
   - For TAS queries by price range, prioritize price indexes
   - For order book reconstruction, optimize depth command/price indexes

### Processing Efficiency

1. **Balance batch size** with system capabilities:
   ```json
   # Higher throughput, more memory usage:
   "batch_size": 10000
   
   # Balanced approach:
   "batch_size": 5000
   
   # Lower memory usage, less throughput:
   "batch_size": 1000
   ```

2. **Parallelize contracts** by running multiple instances:
   ```bash
   # Instance 1 for ES contracts
   python enhanced_etl.py 1 --config ./config_es.json
   
   # Instance 2 for GC contracts
   python enhanced_etl.py 1 --config ./config_gc.json
   ```

## Future Enhancements

1. **Real-time analytics pipeline** for live market data processing
2. **Machine learning integration** for anomaly detection in market data
3. **Cloud synchronization** for redundant data storage
4. **Advanced visualization** of market microstructure
5. **High-frequency trading** extensions for ultra-low latency

## Conclusion

The enhanced Sierra Chart ETL Pipeline provides a robust foundation for algorithmic trading with both historical and live data. The improvements in error handling, file processing, and database management ensure reliable 24/7 operation even with imperfect source data. The new data repair utilities and configuration options give you fine-grained control over data quality and system behavior.

---

## Appendix: Implementation Details

### Key Changes to Original Files

1. **enhanced_parsers.py**:
   - Added tolerance thresholds for timestamp validation
   - Implemented record filtering capabilities
   - Enhanced file format detection

2. **db_manager.py**:
   - Optimized connection pooling
   - Added retry mechanisms
   - Enhanced index management
   - Added data quality metrics

3. **enhanced_etl.py**:
   - Added robust error recovery
   - Enhanced filename pattern matching
   - Implemented advanced checkpointing

4. **data_repair.py** (New):
   - Created comprehensive repair utilities
   - Added file analysis capabilities
   - Implemented database optimization tools

### Monitoring and Health Checks

The enhanced system includes improved health checks:

```python
async def perform_health_check(self) -> None:
    # Check database connection
    # Verify disk space
    # Analyze processing rates
    # Run data validation if configured
```

These checks help identify issues before they affect data quality or system stability.