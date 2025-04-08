# Sierra Chart ETL Pipeline

Enhanced ETL pipeline for extracting, transforming, and loading market data from Sierra Chart into a structured database for algorithmic trading.

## Features

- Robust error handling with automatic retries
- Atomic checkpointing to prevent data loss
- Optimized database management with connection pooling
- Comprehensive data validation for quality assurance
- REST API for algorithmic trading access
- Monitoring and alerting system
- Service management for 24/7 operation

## Setup

1. Configure Sierra Chart as described in documentation
2. Edit config.json with your settings
3. Install dependencies: `pip install -r requirements.txt`
4. Run the ETL pipeline: `python enhanced_etl.py 1`
5. Start the API server: `python api_service.py`

See the full documentation for detailed instructions.

# Sierra Chart ETL Pipeline - Setup and Usage Guide

This document provides comprehensive instructions for setting up, configuring, and using the enhanced Sierra Chart ETL Pipeline for algorithmic trading.

## Table of Contents

1. [System Overview](#system-overview)
2. [Prerequisites](#prerequisites)
3. [Installation](#installation)
4. [Configuration](#configuration)
5. [Starting the System](#starting-the-system)
6. [Monitoring and Alerting](#monitoring-and-alerting)
7. [API Usage](#api-usage)
8. [Database Maintenance](#database-maintenance)
9. [Troubleshooting](#troubleshooting)
10. [Extending the System](#extending-the-system)

## System Overview

The Sierra Chart ETL Pipeline is a robust system designed for extracting, transforming, and loading market data from Sierra Chart into a structured database for algorithmic trading. The system includes:

- **ETL Pipeline**: Processes time & sales and market depth data from Sierra Chart files
- **Database Manager**: Optimized SQLite storage with indexes for fast queries
- **Data Validation**: Ensures data quality and integrity
- **REST API**: Provides programmatic access to market data
- **Monitoring System**: Tracks system health and alerts on issues
- **Service Manager**: Ensures 24/7 operation with automatic restarts

## Prerequisites

Before installing the system, ensure you have:

- **Python 3.8+**: The system requires Python 3.8 or higher
- **Sierra Chart**: Installed and configured with data feeds
- **Dependencies**: Required Python packages (automatically installed)
- **Disk Space**: At least 50GB of free space for database and logs
- **Operating System**: Linux, macOS, or Windows

### Required Python Packages

The system depends on the following Python packages:

```
fastapi==0.85.0
uvicorn==0.18.3
sqlite3==2.6.0
psutil==5.9.1
numpy==1.23.3
requests==2.28.1
pydantic==1.10.2
```

## Installation

Follow these steps to install the system:

1. **Clone or download the repository**:

```bash
git clone https://github.com/your-repo/sierrachart-etl.git
cd sierrachart-etl
```

2. **Create a Python virtual environment**:

```bash
python -m venv venv
```

3. **Activate the virtual environment**:

On Windows:
```bash
venv\Scripts\activate
```

On macOS/Linux:
```bash
source venv/bin/activate
```

4. **Install dependencies**:

```bash
pip install -r requirements.txt
```

5. **Create necessary directories**:

```bash
mkdir -p logs data backups
```

## Configuration

### 1. Sierra Chart Configuration

First, configure Sierra Chart to store the required data:

1. **For Time & Sales Data**:
   - In Sierra Chart, go to **Global Settings » Data/Trade Service Settings**
   - Set **Intraday Data Storage Time Unit** to **1 Tick**

2. **For Market Depth Data**:
   - In **Global Settings » Symbol Settings**, find your symbol
   - Set **Record Market Depth Data** to "Yes"

3. **Enable Intraday File Update List**:
   - Configure a list of symbols to track in Sierra Chart's **Intraday File Update List**
   - You can use the `update_file_list.py` utility to generate this list

### 2. ETL Pipeline Configuration

Create or modify the `config.json` file:

```json
{
  "contracts": {
    "ESM25_FUT_CME": {
      "checkpoint_tas": 0,
      "checkpoint_depth": {
        "date": "",
        "rec": 0
      },
      "price_adj": 0.01,
      "tas": true,
      "depth": true
    }
  },
  "db_path": "/path/to/tick.db",
  "sc_root": "/path/to/SierraChart",
  "sleep_int": 1,
  "batch_size": 5000,
  "maintenance_interval": 86400,
  "backup_interval": 604800,
  "health_check_interval": 300
}
```

Configure the following settings:

- `contracts`: List of contracts to process, with their properties
- `db_path`: Path to the SQLite database file
- `sc_root`: Path to your Sierra Chart installation
- `sleep_int`: Polling interval (in seconds) for new data
- `batch_size`: Number of records to process in a batch
- `maintenance_interval`: Interval (in seconds) for database maintenance
- `backup_interval`: Interval (in seconds) for database backups
- `health_check_interval`: Interval (in seconds) for health checks

### 3. Service Manager Configuration

Create a services configuration file:

```bash
python service_manager.py create-etl-config --db-path /path/to/tick.db --sc-root /path/to/SierraChart --config-path ./config.json --api-port 8000
```

This will create a `services.json` file with configurations for both the ETL pipeline and API service.

### 4. API Configuration

The API service is configured automatically by the service manager. You can customize it by editing the `services.json` file.

To create API keys for authentication:

```bash
python api_service.py --db-path /path/to/tick.db --generate-admin-key
```

This will generate an admin API key that you can use to create additional keys through the API.

## Starting the System

### Using the Service Manager (Recommended)

1. **Start all services**:

```bash
python service_manager.py start
```

2. **Check service status**:

```bash
python service_manager.py status
```

3. **Stop all services**:

```bash
python service_manager.py stop
```

### Starting Components Individually (Advanced)

If needed, you can start individual components:

1. **ETL Pipeline**:

```bash
python enhanced_etl.py 1 --config ./config.json
```

2. **API Service**:

```bash
python api_service.py --db-path /path/to/tick.db --host 0.0.0.0 --port 8000
```

3. **Monitoring**:

```bash
python monitoring.py --db-path /path/to/tick.db
```

## Monitoring and Alerting

### Checking System Status

View the current system status:

```bash
python service_manager.py status
```

### Viewing Logs

View logs for a specific service:

```bash
python service_manager.py log sierrachart_etl --lines 100
```

Follow logs in real-time:

```bash
python service_manager.py log sierrachart_etl --follow
```

### Configuring Alerts

Edit the alert configuration in the monitoring module to customize alert thresholds and notification channels.

For email alerts:

```python
monitoring_server.configure_email_alerts(
    smtp_server="smtp.example.com",
    smtp_port=587,
    username="alerts@example.com",
    password="your-password",
    sender="alerts@example.com",
    recipients=["your-email@example.com"]
)
```

For webhook alerts:

```python
monitoring_server.add_webhook("https://example.com/webhook")
```

## API Usage

The API provides access to market data for algorithmic trading applications.

### Authentication

All API endpoints require an API key, passed as a header:

```
X-API-Key: your-api-key
```

### Listing Available Contracts

```
GET /contracts
```

Response:

```json
[
  {
    "contract_id": "ESM25_FUT_CME",
    "has_tas": true,
    "has_depth": true,
    "first_timestamp": 3973137600000000,
    "last_timestamp": 3973140200000000,
    "tas_count": 25000,
    "depth_count": 150000
  }
]
```

### Getting Time & Sales Data

```
GET /contracts/{contract_id}/tas?start_time={start}&end_time={end}&limit=1000
```

Response:

```json
[
  {
    "timestamp": 3973137600000000,
    "price": 4500.50,
    "quantity": 5,
    "side": 0
  },
  ...
]
```

### Getting Order Book Data

```
GET /contracts/{contract_id}/orderbook?timestamp={timestamp}&levels=10
```

Response:

```json
{
  "timestamp": 3973137600000000,
  "bids": [
    {"price": 4500.00, "quantity": 35, "num_orders": 12},
    {"price": 4499.75, "quantity": 60, "num_orders": 18}
  ],
  "asks": [
    {"price": 4500.25, "quantity": 42, "num_orders": 15},
    {"price": 4500.50, "quantity": 28, "num_orders": 9}
  ]
}
```

### Additional API Endpoints

See the complete API documentation by visiting:

```
http://localhost:8000/docs
```

## Database Maintenance

The system performs automatic database maintenance based on the configured intervals. However, you can also perform maintenance manually:

### Running Manual Maintenance

```python
from db_manager import DatabaseManager

db_manager = DatabaseManager("/path/to/tick.db")
db_manager.perform_maintenance()
```

### Creating Database Backups

```python
from db_manager import DatabaseManager

db_manager = DatabaseManager("/path/to/tick.db")
backup_path = db_manager.backup_database("/path/to/backup.db")
print(f"Backup created at: {backup_path}")
```

## Troubleshooting

### Common Issues

1. **No new records being processed**
   - Check if Sierra Chart is connected to data feeds
   - Verify file paths in config.json
   - Check logs for errors

2. **Database performance issues**
   - Run database maintenance
   - Check disk space
   - Verify indexes are created

3. **API connectivity issues**
   - Check if API service is running
   - Verify API key is valid
   - Check firewall settings

### Log Files

The system generates several log files:

- `etl_pipeline.log`: Main ETL pipeline logs
- `service_manager.log`: Service manager logs
- `logs/sierrachart_etl.log`: ETL service logs
- `logs/api_service.log`: API service logs

### Health Checks

Run manual health checks:

```python
from monitoring import MetricsCollector

metrics = MetricsCollector()
system_metrics = metrics.collect_system_metrics()
db_metrics = metrics.collect_database_metrics("/path/to/tick.db")

print("System Metrics:", system_metrics)
print("Database Metrics:", db_metrics)
```

## Extending the System

### Adding New Contracts

To add new contracts:

1. Edit the `config.json` file and add a new entry to the `contracts` object:

```json
"NQM25_FUT_CME": {
  "checkpoint_tas": 0,
  "checkpoint_depth": {
    "date": "",
    "rec": 0
  },
  "price_adj": 0.01,
  "tas": true,
  "depth": true
}
```

2. Ensure Sierra Chart is configured to record data for the new contract

3. Restart the ETL service:

```bash
python service_manager.py restart sierrachart_etl
```

### Customizing Data Validation

The data validation module (`data_validator.py`) can be extended to add custom validation rules for your specific requirements.

### Integrating with Other Systems

The API service makes it easy to integrate with other systems. You can:

- Connect algorithmic trading platforms
- Build custom dashboards
- Develop backtesting frameworks

Custom integration can be done by consuming the REST API endpoints.

---

## Support and Resources

For additional help or to report issues, please contact:

- Email: support@example.com
- GitHub: https://github.com/your-repo/sierrachart-etl/issues

---

This document was last updated: April 4, 2025
