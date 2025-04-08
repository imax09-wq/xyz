#!/usr/bin/env python3
"""
REST API for accessing market data for algorithmic trading.

Part of the Sierra Chart ETL Pipeline project.
"""




import asyncio
import json
import logging
import os
import time
import sqlite3
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any, Union

from fastapi import FastAPI, HTTPException, Query, Depends, BackgroundTasks, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, Field
import uvicorn

# Import our custom modules
from db_manager import DatabaseManager, DatabaseOperation
from enhanced_parsers import TimeAndSalesField, DepthRecordField, DepthCommand
from monitoring import MetricsCollector, AlertManager, MonitoringServer

logger = logging.getLogger("SierraChartETL.api")

# Create FastAPI app
app = FastAPI(
    title="Sierra Chart Market Data API",
    description="API for accessing Sierra Chart time & sales and market depth data",
    version="1.0.0"
)

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, restrict this to your frontend origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# API Models
class HealthStatus(BaseModel):
    status: str = Field(..., description="API health status")
    uptime: float = Field(..., description="API uptime in seconds")
    db_connected: bool = Field(..., description="Database connection status")
    version: str = Field(..., description="API version")
    timestamp: float = Field(..., description="Current timestamp")

class TimeAndSalesRecord(BaseModel):
    timestamp: int = Field(..., description="Timestamp in microseconds since Sierra Chart epoch")
    price: float = Field(..., description="Trade price")
    quantity: int = Field(..., description="Trade quantity")
    side: int = Field(..., description="Trade side (0=bid, 1=ask)")
    
    class Config:
        schema_extra = {
            "example": {
                "timestamp": 3973137600000000,
                "price": 4500.50,
                "quantity": 5,
                "side": 0
            }
        }

class DepthRecord(BaseModel):
    timestamp: int = Field(..., description="Timestamp in microseconds since Sierra Chart epoch")
    command: int = Field(..., description="Command type (1=clear, 2=add bid, 3=add ask, 4=mod bid, 5=mod ask, 6=del bid, 7=del ask)")
    flags: int = Field(..., description="Command flags")
    num_orders: int = Field(..., description="Number of orders at this price level")
    price: float = Field(..., description="Price level")
    quantity: int = Field(..., description="Quantity at this price level")
    
    class Config:
        schema_extra = {
            "example": {
                "timestamp": 3973137600000000,
                "command": 2,
                "flags": 0,
                "num_orders": 12,
                "price": 4500.00,
                "quantity": 35
            }
        }

class OrderBookLevel(BaseModel):
    price: float = Field(..., description="Price level")
    quantity: int = Field(..., description="Quantity at this price level")
    num_orders: int = Field(..., description="Number of orders at this price level")

class OrderBook(BaseModel):
    timestamp: int = Field(..., description="Timestamp in microseconds since Sierra Chart epoch")
    bids: List[OrderBookLevel] = Field(default_factory=list, description="Bid levels sorted by price descending")
    asks: List[OrderBookLevel] = Field(default_factory=list, description="Ask levels sorted by price ascending")
    
    class Config:
        schema_extra = {
            "example": {
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
        }

class ContractInfo(BaseModel):
    contract_id: str = Field(..., description="Contract identifier")
    has_tas: bool = Field(..., description="Whether time & sales data is available")
    has_depth: bool = Field(..., description="Whether market depth data is available")
    first_timestamp: Optional[int] = Field(None, description="First timestamp in data")
    last_timestamp: Optional[int] = Field(None, description="Last timestamp in data")
    tas_count: Optional[int] = Field(None, description="Number of time & sales records")
    depth_count: Optional[int] = Field(None, description="Number of depth records")

class AlertInfo(BaseModel):
    alert_id: str = Field(..., description="Alert identifier")
    severity: str = Field(..., description="Alert severity (info, warning, critical)")
    message: str = Field(..., description="Alert message")
    source: str = Field(..., description="Alert source")
    timestamp: float = Field(..., description="Alert timestamp")
    resolved: bool = Field(..., description="Whether the alert is resolved")

class APIKey(BaseModel):
    key: str = Field(..., description="API key for authentication")
    name: str = Field(..., description="Name of the client")
    created: float = Field(..., description="Creation timestamp")
    expires: Optional[float] = Field(None, description="Expiration timestamp")
    enabled: bool = Field(True, description="Whether the API key is enabled")

# Global state
api_config = {
    "start_time": time.time(),
    "version": "1.0.0",
    "db_path": "",
    "api_keys": {}
}

# Database manager
db_manager = None

# Monitoring components
metrics_collector = None
alert_manager = None
monitoring_server = None

# Order book cache (contract_id -> OrderBook)
order_book_cache = {}
order_book_cache_lock = threading.RLock()

# Event subscriptions (contract_id -> list of websocket connections)
event_subscriptions = {}
event_subscriptions_lock = threading.RLock()

# API key validation
def validate_api_key(api_key: str = Header(..., description="API key for authentication")):
    if api_key not in api_config["api_keys"]:
        raise HTTPException(status_code=401, detail="Invalid API key")
    
    key_info = api_config["api_keys"][api_key]
    
    if not key_info["enabled"]:
        raise HTTPException(status_code=403, detail="API key is disabled")
    
    if key_info["expires"] is not None and time.time() > key_info["expires"]:
        raise HTTPException(status_code=403, detail="API key has expired")
    
    return key_info

# Initialize API
def initialize_api(
    db_path: str,
    config_path: str = None,
    enable_monitoring: bool = True,
    api_keys_file: str = "./api_keys.json"
):
    """Initialize the API with database connection and configuration"""
    global db_manager, metrics_collector, alert_manager, monitoring_server
    
    # Set database path
    api_config["db_path"] = db_path
    
    # Initialize database manager
    db_manager = DatabaseManager(db_path)
    
    # Load API keys if file exists
    if os.path.exists(api_keys_file):
        try:
            with open(api_keys_file, 'r') as f:
                api_config["api_keys"] = json.load(f)
            logger.info(f"Loaded {len(api_config['api_keys'])} API keys from {api_keys_file}")
        except Exception as e:
            logger.error(f"Error loading API keys: {str(e)}")
    
    # Initialize monitoring if enabled
    if enable_monitoring:
        metrics_collector = MetricsCollector()
        alert_manager = AlertManager()
        
        monitoring_server = MonitoringServer(db_path)
        monitoring_server.start()
        
        logger.info("Monitoring server started")
    
    logger.info(f"API initialized with database: {db_path}")

# API Routes

@app.get("/health", response_model=HealthStatus, tags=["System"])
async def health_check():
    """Check API health status"""
    try:
        # Test database connection
        db_connected = False
        if db_manager:
            try:
                with db_manager.connection_pool.get_connection() as conn:
                    cursor = conn.execute("SELECT 1")
                    result = cursor.fetchone()
                    db_connected = (result and result[0] == 1)
            except:
                db_connected = False
        
        return {
            "status": "ok",
            "uptime": time.time() - api_config["start_time"],
            "db_connected": db_connected,
            "version": api_config["version"],
            "timestamp": time.time()
        }
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        return {
            "status": "error",
            "uptime": time.time() - api_config["start_time"],
            "db_connected": False,
            "version": api_config["version"],
            "timestamp": time.time()
        }

@app.get("/contracts", response_model=List[ContractInfo], tags=["Metadata"])
async def list_contracts(api_key: Dict = Depends(validate_api_key)):
    """List all available contracts with metadata"""
    try:
        results = []
        
        # Get all tables from database
        with db_manager.connection_pool.get_connection(DatabaseOperation.READ) as conn:
            cursor = conn.execute(
                """
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name NOT LIKE 'sqlite_%'
                AND name NOT LIKE 'etl_%' AND name NOT LIKE 'tables_%'
                """
            )
            tables = [row[0] for row in cursor.fetchall()]
        
        # Group tables by contract
        contract_tables = {}
        for table in tables:
            parts = table.split('_')
            if len(parts) >= 2:
                contract_id = '_'.join(parts[:-1])
                data_type = parts[-1]
                
                if contract_id not in contract_tables:
                    contract_tables[contract_id] = {"tas": False, "depth": False}
                
                if data_type == "tas":
                    contract_tables[contract_id]["tas"] = True
                elif data_type == "depth":
                    contract_tables[contract_id]["depth"] = True
        
        # Get contract details
        for contract_id, data_types in contract_tables.items():
            contract_info = ContractInfo(
                contract_id=contract_id,
                has_tas=data_types["tas"],
                has_depth=data_types["depth"]
            )
            
            # Get additional metadata if available
            with db_manager.connection_pool.get_connection(DatabaseOperation.READ) as conn:
                # Time and sales stats
                if data_types["tas"]:
                    try:
                        cursor = conn.execute(
                            f"""
                            SELECT COUNT(*), MIN(timestamp), MAX(timestamp)
                            FROM {contract_id}_tas
                            """
                        )
                        count, min_ts, max_ts = cursor.fetchone()
                        contract_info.tas_count = count
                        if min_ts is not None:
                            contract_info.first_timestamp = min_ts
                        if max_ts is not None:
                            contract_info.last_timestamp = max_ts
                    except sqlite3.Error:
                        pass
                
                # Depth stats
                if data_types["depth"]:
                    try:
                        cursor = conn.execute(
                            f"""
                            SELECT COUNT(*), MIN(timestamp), MAX(timestamp)
                            FROM {contract_id}_depth
                            """
                        )
                        count, min_ts, max_ts = cursor.fetchone()
                        contract_info.depth_count = count
                        
                        # Update timestamps if not already set or if depth data has earlier/later timestamps
                        if min_ts is not None and (contract_info.first_timestamp is None or min_ts < contract_info.first_timestamp):
                            contract_info.first_timestamp = min_ts
                        if max_ts is not None and (contract_info.last_timestamp is None or max_ts > contract_info.last_timestamp):
                            contract_info.last_timestamp = max_ts
                    except sqlite3.Error:
                        pass
            
            results.append(contract_info)
        
        return results
    
    except Exception as e:
        logger.error(f"Error listing contracts: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to list contracts: {str(e)}")

@app.get("/contracts/{contract_id}", response_model=ContractInfo, tags=["Metadata"])
async def get_contract_info(contract_id: str, api_key: Dict = Depends(validate_api_key)):
    """Get detailed information about a specific contract"""
    try:
        # Check if contract exists
        with db_manager.connection_pool.get_connection(DatabaseOperation.READ) as conn:
            cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=? OR name=?",
                (f"{contract_id}_tas", f"{contract_id}_depth")
            )
            tables = [row[0] for row in cursor.fetchall()]
        
        if not tables:
            raise HTTPException(status_code=404, detail=f"Contract '{contract_id}' not found")
        
        # Determine data types available
        has_tas = f"{contract_id}_tas" in tables
        has_depth = f"{contract_id}_depth" in tables
        
        contract_info = ContractInfo(
            contract_id=contract_id,
            has_tas=has_tas,
            has_depth=has_depth
        )
        
        # Get additional metadata if available
        with db_manager.connection_pool.get_connection(DatabaseOperation.READ) as conn:
            # Time and sales stats
            if has_tas:
                try:
                    cursor = conn.execute(
                        f"""
                        SELECT COUNT(*), MIN(timestamp), MAX(timestamp)
                        FROM {contract_id}_tas
                        """
                    )
                    count, min_ts, max_ts = cursor.fetchone()
                    contract_info.tas_count = count
                    if min_ts is not None:
                        contract_info.first_timestamp = min_ts
                    if max_ts is not None:
                        contract_info.last_timestamp = max_ts
                except sqlite3.Error:
                    pass
            
            # Depth stats
            if has_depth:
                try:
                    cursor = conn.execute(
                        f"""
                        SELECT COUNT(*), MIN(timestamp), MAX(timestamp)
                        FROM {contract_id}_depth
                        """
                    )
                    count, min_ts, max_ts = cursor.fetchone()
                    contract_info.depth_count = count
                    
                    # Update timestamps if not already set or if depth data has earlier/later timestamps
                    if min_ts is not None and (contract_info.first_timestamp is None or min_ts < contract_info.first_timestamp):
                        contract_info.first_timestamp = min_ts
                    if max_ts is not None and (contract_info.last_timestamp is None or max_ts > contract_info.last_timestamp):
                        contract_info.last_timestamp = max_ts
                except sqlite3.Error:
                    pass
        
        return contract_info
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting contract info: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get contract info: {str(e)}")

@app.get("/contracts/{contract_id}/tas", response_model=List[TimeAndSalesRecord], tags=["Data"])
async def get_time_and_sales(
    contract_id: str,
    start_time: Optional[int] = Query(None, description="Start timestamp (microseconds)"),
    end_time: Optional[int] = Query(None, description="End timestamp (microseconds)"),
    limit: int = Query(1000, description="Maximum number of records to return"),
    api_key: Dict = Depends(validate_api_key)
):
    """Get time and sales data for a contract"""
    try:
        # Validate contract and table existence
        with db_manager.connection_pool.get_connection(DatabaseOperation.READ) as conn:
            cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (f"{contract_id}_tas",)
            )
            if not cursor.fetchone():
                raise HTTPException(status_code=404, detail=f"Time & sales data for contract '{contract_id}' not found")
        
        # Build query
        query = f"SELECT timestamp, price, qty, side FROM {contract_id}_tas"
        params = []
        
        where_clauses = []
        if start_time is not None:
            where_clauses.append("timestamp >= ?")
            params.append(start_time)
        
        if end_time is not None:
            where_clauses.append("timestamp <= ?")
            params.append(end_time)
        
        if where_clauses:
            query += " WHERE " + " AND ".join(where_clauses)
        
        query += " ORDER BY timestamp"
        
        if limit:
            query += " LIMIT ?"
            params.append(limit)
        
        # Execute query
        with db_manager.connection_pool.get_connection(DatabaseOperation.READ) as conn:
            cursor = conn.execute(query, params)
            rows = cursor.fetchall()
        
        # Convert to model
        results = [
            TimeAndSalesRecord(
                timestamp=row[0],
                price=row[1],
                quantity=row[2],
                side=row[3]
            )
            for row in rows
        ]
        
        return results
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting time and sales: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get time and sales data: {str(e)}")

@app.get("/contracts/{contract_id}/depth", response_model=List[DepthRecord], tags=["Data"])
async def get_market_depth(
    contract_id: str,
    start_time: Optional[int] = Query(None, description="Start timestamp (microseconds)"),
    end_time: Optional[int] = Query(None, description="End timestamp (microseconds)"),
    limit: int = Query(1000, description="Maximum number of records to return"),
    api_key: Dict = Depends(validate_api_key)
):
    """Get market depth data for a contract"""
    try:
        # Validate contract and table existence
        with db_manager.connection_pool.get_connection(DatabaseOperation.READ) as conn:
            cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (f"{contract_id}_depth",)
            )
            if not cursor.fetchone():
                raise HTTPException(status_code=404, detail=f"Market depth data for contract '{contract_id}' not found")
        
        # Build query
        query = f"SELECT timestamp, command, flags, num_orders, price, qty FROM {contract_id}_depth"
        params = []
        
        where_clauses = []
        if start_time is not None:
            where_clauses.append("timestamp >= ?")
            params.append(start_time)
        
        if end_time is not None:
            where_clauses.append("timestamp <= ?")
            params.append(end_time)
        
        if where_clauses:
            query += " WHERE " + " AND ".join(where_clauses)
        
        query += " ORDER BY timestamp"
        
        if limit:
            query += " LIMIT ?"
            params.append(limit)
        
        # Execute query
        with db_manager.connection_pool.get_connection(DatabaseOperation.READ) as conn:
            cursor = conn.execute(query, params)
            rows = cursor.fetchall()
        
        # Convert to model
        results = [
            DepthRecord(
                timestamp=row[0],
                command=row[1],
                flags=row[2],
                num_orders=row[3],
                price=row[4],
                quantity=row[5]
            )
            for row in rows
        ]
        
        return results
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting market depth: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get market depth data: {str(e)}")

@app.get("/contracts/{contract_id}/orderbook", response_model=OrderBook, tags=["Data"])
async def get_order_book(
    contract_id: str,
    timestamp: Optional[int] = Query(None, description="Timestamp to get the order book for (microseconds). If not provided, the latest order book is returned."),
    levels: int = Query(10, description="Number of price levels to include"),
    api_key: Dict = Depends(validate_api_key)
):
    """Get the order book (reconstructed from depth data) for a contract at a specific timestamp"""
    try:
        # Validate contract and table existence
        with db_manager.connection_pool.get_connection(DatabaseOperation.READ) as conn:
            cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (f"{contract_id}_depth",)
            )
            if not cursor.fetchone():
                raise HTTPException(status_code=404, detail=f"Market depth data for contract '{contract_id}' not found")
        
        # If timestamp is not provided, get the latest timestamp
        if timestamp is None:
            with db_manager.connection_pool.get_connection(DatabaseOperation.READ) as conn:
                cursor = conn.execute(
                    f"SELECT MAX(timestamp) FROM {contract_id}_depth"
                )
                result = cursor.fetchone()
                if not result or result[0] is None:
                    raise HTTPException(status_code=404, detail=f"No market depth data found for contract '{contract_id}'")
                
                timestamp = result[0]
        
        # Check cache for this timestamp
        cache_key = f"{contract_id}_{timestamp}"
        with order_book_cache_lock:
            if cache_key in order_book_cache:
                return order_book_cache[cache_key]
        
        # Reconstruct the order book at the given timestamp
        with db_manager.connection_pool.get_connection(DatabaseOperation.READ) as conn:
            # Get all depth commands up to the timestamp
            cursor = conn.execute(
                f"""
                SELECT timestamp, command, flags, num_orders, price, qty
                FROM {contract_id}_depth
                WHERE timestamp <= ?
                ORDER BY timestamp
                """,
                (timestamp,)
            )
            rows = cursor.fetchall()
        
        if not rows:
            raise HTTPException(status_code=404, detail=f"No market depth data found for contract '{contract_id}' at or before timestamp {timestamp}")
        
        # Process depth records to build the order book
        bid_levels = {}  # price -> (qty, num_orders)
        ask_levels = {}  # price -> (qty, num_orders)
        
        for row in rows:
            cmd = row[1]  # command
            price = row[4]
            qty = row[5]
            num_orders = row[3]
            
            if cmd == DepthCommand.CLEAR_BOOK.value:
                # Clear the book
                bid_levels.clear()
                ask_levels.clear()
            
            elif cmd == DepthCommand.ADD_BID_LVL.value:
                # Add bid level
                bid_levels[price] = (qty, num_orders)
            
            elif cmd == DepthCommand.ADD_ASK_LVL.value:
                # Add ask level
                ask_levels[price] = (qty, num_orders)
            
            elif cmd == DepthCommand.MOD_BID_LVL.value:
                # Modify bid level
                bid_levels[price] = (qty, num_orders)
            
            elif cmd == DepthCommand.MOD_ASK_LVL.value:
                # Modify ask level
                ask_levels[price] = (qty, num_orders)
            
            elif cmd == DepthCommand.DEL_BID_LVL.value:
                # Delete bid level
                if price in bid_levels:
                    del bid_levels[price]
            
            elif cmd == DepthCommand.DEL_ASK_LVL.value:
                # Delete ask level
                if price in ask_levels:
                    del ask_levels[price]
        
        # Sort and limit the levels
        sorted_bids = sorted(bid_levels.items(), key=lambda x: x[0], reverse=True)
        sorted_asks = sorted(ask_levels.items(), key=lambda x: x[0])
        
        bids = [
            OrderBookLevel(price=price, quantity=qty, num_orders=orders)
            for price, (qty, orders) in sorted_bids[:levels]
        ]
        
        asks = [
            OrderBookLevel(price=price, quantity=qty, num_orders=orders)
            for price, (qty, orders) in sorted_asks[:levels]
        ]
        
        # Create the order book
        order_book = OrderBook(
            timestamp=timestamp,
            bids=bids,
            asks=asks
        )
        
        # Update cache
        with order_book_cache_lock:
            # Limit cache size
            if len(order_book_cache) > 1000:
                # Remove oldest entries
                oldest_keys = sorted(order_book_cache.keys())[:200]
                for key in oldest_keys:
                    del order_book_cache[key]
            
            order_book_cache[cache_key] = order_book
        
        return order_book
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting order book: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get order book: {str(e)}")

@app.get("/system/alerts", response_model=List[AlertInfo], tags=["System"])
async def get_alerts(
    include_resolved: bool = Query(False, description="Whether to include resolved alerts"),
    limit: int = Query(100, description="Maximum number of alerts to return"),
    api_key: Dict = Depends(validate_api_key)
):
    """Get system alerts"""
    try:
        if not monitoring_server or not alert_manager:
            raise HTTPException(status_code=503, description="Monitoring is not enabled")
        
        results = []
        
        # Get active alerts
        active_alerts = alert_manager.get_active_alerts()
        for alert in active_alerts:
            results.append(AlertInfo(
                alert_id=alert.alert_id,
                severity=alert.severity,
                message=alert.message,
                source=alert.source,
                timestamp=alert.timestamp,
                resolved=False
            ))
        
        # Get resolved alerts if requested
        if include_resolved:
            resolved_alerts = alert_manager.get_resolved_alerts(limit)
            for alert in resolved_alerts:
                results.append(AlertInfo(
                    alert_id=alert.alert_id,
                    severity=alert.severity,
                    message=alert.message,
                    source=alert.source,
                    timestamp=alert.timestamp,
                    resolved=True
                ))
        
        # Sort by timestamp (most recent first) and limit
        results.sort(key=lambda x: x.timestamp, reverse=True)
        return results[:limit]
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting alerts: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get alerts: {str(e)}")

@app.get("/admin/api-keys", tags=["Admin"])
async def list_api_keys(api_key: Dict = Depends(validate_api_key)):
    """List all API keys (admin only)"""
    # Check if the caller is an admin
    if not api_key.get("admin", False):
        raise HTTPException(status_code=403, detail="Admin access required")
    
    # Return all API keys (without sensitive info)
    return [
        {
            "key": key,
            "name": info["name"],
            "created": info["created"],
            "expires": info.get("expires"),
            "enabled": info.get("enabled", True),
            "admin": info.get("admin", False)
        }
        for key, info in api_config["api_keys"].items()
    ]

@app.post("/admin/api-keys", tags=["Admin"])
async def create_api_key(
    name: str,
    expires_in_days: Optional[int] = None,
    admin: bool = False,
    api_key: Dict = Depends(validate_api_key)
):
    """Create a new API key (admin only)"""
    # Check if the caller is an admin
    if not api_key.get("admin", False):
        raise HTTPException(status_code=403, detail="Admin access required")
    
    # Generate a new API key
    import secrets
    new_key = secrets.token_hex(16)
    
    # Calculate expiration if specified
    expires = None
    if expires_in_days is not None:
        expires = time.time() + (expires_in_days * 86400)
    
    # Add the new key
    api_config["api_keys"][new_key] = {
        "name": name,
        "created": time.time(),
        "expires": expires,
        "enabled": True,
        "admin": admin
    }
    
    # Save API keys to file
    try:
        with open("./api_keys.json", 'w') as f:
            json.dump(api_config["api_keys"], f, indent=2)
    except Exception as e:
        logger.error(f"Error saving API keys: {str(e)}")
    
    return {
        "key": new_key,
        "name": name,
        "created": api_config["api_keys"][new_key]["created"],
        "expires": expires,
        "admin": admin
    }

@app.delete("/admin/api-keys/{key}", tags=["Admin"])
async def delete_api_key(key: str, api_key: Dict = Depends(validate_api_key)):
    """Delete an API key (admin only)"""
    # Check if the caller is an admin
    if not api_key.get("admin", False):
        raise HTTPException(status_code=403, detail="Admin access required")
    
    # Check if the key exists
    if key not in api_config["api_keys"]:
        raise HTTPException(status_code=404, detail="API key not found")
    
    # Don't allow deleting your own key
    if key == api_key["key"]:
        raise HTTPException(status_code=400, detail="Cannot delete your own API key")
    
    # Delete the key
    del api_config["api_keys"][key]
    
    # Save API keys to file
    try:
        with open("./api_keys.json", 'w') as f:
            json.dump(api_config["api_keys"], f, indent=2)
    except Exception as e:
        logger.error(f"Error saving API keys: {str(e)}")
    
    return {"status": "success", "message": "API key deleted"}

def start_api_server(
    host: str = "0.0.0.0",
    port: int = 8000,
    db_path: str = "./tick.db",
    enable_monitoring: bool = True
):
    """Start the API server"""
    # Initialize API
    initialize_api(db_path, enable_monitoring=enable_monitoring)
    
    # Start Uvicorn server
    uvicorn.run(app, host=host, port=port)

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Sierra Chart Market Data API")
    parser.add_argument("--host", default="0.0.0.0", help="Host to bind to")
    parser.add_argument("--port", type=int, default=8000, help="Port to bind to")
    parser.add_argument("--db-path", required=True, help="Path to the SQLite database")
    parser.add_argument("--no-monitoring", action="store_true", help="Disable monitoring")
    
    args = parser.parse_args()
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Start the API server
    start_api_server(
        host=args.host,
        port=args.port,
        db_path=args.db_path,
        enable_monitoring=not args.no_monitoring
    )