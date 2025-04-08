#!/usr/bin/env python3
"""
Real-time data service with WebSockets for the ETL pipeline.
"""

import asyncio
import json
import logging
import time
from typing import Dict, List, Any, Set, Optional, Callable
import threading

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Depends, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from db_manager import DatabaseManager

logger = logging.getLogger("SierraChartETL.realtime")

# Data models
class MarketUpdate(BaseModel):
    """Base class for market data updates."""
    contract_id: str
    timestamp: int
    update_time: float = Field(default_factory=time.time)

class TradeUpdate(MarketUpdate):
    """Trade update data model."""
    price: float
    quantity: int
    side: int  # 0 = bid, 1 = ask

class OrderBookUpdate(MarketUpdate):
    """Order book update data model."""
    bid_levels: List[Dict[str, Any]] = Field(default_factory=list)
    ask_levels: List[Dict[str, Any]] = Field(default_factory=list)

class Subscription(BaseModel):
    """Subscription details."""
    contract_id: str
    update_type: str  # 'trade', 'book', 'all'
    throttle_ms: int = 0  # 0 = no throttle

class SubscriptionManager:
    """Manages WebSocket subscriptions."""
    
    def __init__(self):
        self.subscriptions: Dict[WebSocket, Set[Subscription]] = {}
        self.connections: Set[WebSocket] = set()
        self.lock = threading.RLock()
    
    async def connect(self, websocket: WebSocket) -> None:
        """
        Handle new WebSocket connection.
        
        Args:
            websocket: The WebSocket connection
        """
        await websocket.accept()
        with self.lock:
            self.connections.add(websocket)
            self.subscriptions[websocket] = set()
        logger.info(f"New WebSocket connection: {id(websocket)}")
    
    async def disconnect(self, websocket: WebSocket) -> None:
        """
        Handle WebSocket disconnection.
        
        Args:
            websocket: The WebSocket connection
        """
        with self.lock:
            if websocket in self.connections:
                self.connections.remove(websocket)
            if websocket in self.subscriptions:
                del self.subscriptions[websocket]
        logger.info(f"WebSocket disconnected: {id(websocket)}")
    
    async def subscribe(self, websocket: WebSocket, subscription: Subscription) -> None:
        """
        Add subscription for a WebSocket connection.
        
        Args:
            websocket: The WebSocket connection
            subscription: The subscription details
        """
        with self.lock:
            if websocket not in self.subscriptions:
                self.subscriptions[websocket] = set()
            
            # Check for existing subscription to update
            for existing in self.subscriptions[websocket]:
                if (existing.contract_id == subscription.contract_id and 
                    existing.update_type == subscription.update_type):
                    self.subscriptions[websocket].remove(existing)
                    break
            
            self.subscriptions[websocket].add(subscription)
        
        logger.info(f"Added subscription for {websocket.client.host}: {subscription.contract_id} - {subscription.update_type}")
    
    async def unsubscribe(self, websocket: WebSocket, contract_id: str, update_type: str) -> bool:
        """
        Remove subscription for a WebSocket connection.
        
        Args:
            websocket: The WebSocket connection
            contract_id: Contract identifier
            update_type: Update type ('trade', 'book', 'all')
            
        Returns:
            True if subscription was removed, False if not found
        """
        with self.lock:
            if websocket not in self.subscriptions:
                return False
            
            for subscription in self.subscriptions[websocket].copy():
                if (subscription.contract_id == contract_id and 
                    (subscription.update_type == update_type or update_type == 'all')):
                    self.subscriptions[websocket].remove(subscription)
                    logger.info(f"Removed subscription: {contract_id} - {update_type}")
                    return True
        
        return False
    
    async def broadcast_trade(self, trade: TradeUpdate) -> int:
        """
        Broadcast trade update to subscribed clients.
        
        Args:
            trade: Trade update to broadcast
            
        Returns:
            Number of clients the update was sent to
        """
        sent_count = 0
        
        def should_receive(sub):
            return (sub.contract_id == trade.contract_id and 
                    (sub.update_type == 'trade' or sub.update_type == 'all'))
        
        with self.lock:
            connections = list(self.connections)
        
        for websocket in connections:
            try:
                send_update = False
                with self.lock:
                    if websocket in self.subscriptions:
                        # Check if client is subscribed to this trade
                        if any(should_receive(sub) for sub in self.subscriptions[websocket]):
                            send_update = True
                
                if send_update:
                    message = {
                        "type": "trade",
                        "data": trade.dict()
                    }
                    await websocket.send_json(message)
                    sent_count += 1
            
            except Exception as e:
                logger.error(f"Error sending trade update: {str(e)}")
                await self.disconnect(websocket)
        
        return sent_count
    
    async def broadcast_orderbook(self, book: OrderBookUpdate) -> int:
        """
        Broadcast order book update to subscribed clients.
        
        Args:
            book: Order book update to broadcast
            
        Returns:
            Number of clients the update was sent to
        """
        sent_count = 0
        
        def should_receive(sub):
            return (sub.contract_id == book.contract_id and 
                    (sub.update_type == 'book' or sub.update_type == 'all'))
        
        with self.lock:
            connections = list(self.connections)
        
        for websocket in connections:
            try:
                send_update = False
                with self.lock:
                    if websocket in self.subscriptions:
                        # Check if client is subscribed to this order book
                        if any(should_receive(sub) for sub in self.subscriptions[websocket]):
                            send_update = True
                
                if send_update:
                    message = {
                        "type": "book",
                        "data": book.dict()
                    }
                    await websocket.send_json(message)
                    sent_count += 1
            
            except Exception as e:
                logger.error(f"Error sending order book update: {str(e)}")
                await self.disconnect(websocket)
        
        return sent_count

class RealTimeService:
    """Real-time market data service with WebSockets."""
    
    def __init__(self, db_manager: DatabaseManager):
        self.app = FastAPI(
            title="Sierra Chart Real-Time Data Service",
            description="Real-time market data delivery with WebSockets",
            version="1.0.0"
        )
        
        # Add CORS middleware
        self.app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )
        
        # Initialize subscription manager
        self.subscription_manager = SubscriptionManager()
        
        # Use the provided DB manager
        self.db_manager = db_manager
        
        # Store last update timestamps per contract/type to implement throttling
        self.last_updates = {}
        
        # Register routes
        self._setup_routes()
    
    def _setup_routes(self) -> None:
        """Set up API routes."""
        
        @self.app.websocket("/ws")
        async def websocket_endpoint(websocket: WebSocket):
            """WebSocket endpoint for real-time data."""
            await self.subscription_manager.connect(websocket)
            
            try:
                while True:
                    # Wait for commands from the client
                    data = await websocket.receive_json()
                    command = data.get("command")
                    
                    if command == "subscribe":
                        await self._handle_subscribe(websocket, data)
                    elif command == "unsubscribe":
                        await self._handle_unsubscribe(websocket, data)
                    else:
                        await websocket.send_json({
                            "status": "error",
                            "message": f"Unknown command: {command}"
                        })
            
            except WebSocketDisconnect:
                await self.subscription_manager.disconnect(websocket)
            except Exception as e:
                logger.error(f"WebSocket error: {str(e)}")
                await self.subscription_manager.disconnect(websocket)
    
    async def _handle_subscribe(self, websocket: WebSocket, data: Dict[str, Any]) -> None:
        """
        Handle subscribe command.
        
        Args:
            websocket: The WebSocket connection
            data: The command data
        """
        contract_id = data.get("contract_id")
        update_type = data.get("update_type", "all")
        throttle_ms = data.get("throttle_ms", 0)
        
        if not contract_id:
            await websocket.send_json({
                "status": "error",
                "message": "Missing contract_id parameter"
            })
            return
        
        if update_type not in ["trade", "book", "all"]:
            await websocket.send_json({
                "status": "error",
                "message": f"Invalid update_type: {update_type}. Must be 'trade', 'book', or 'all'."
            })
            return
        
        subscription = Subscription(
            contract_id=contract_id,
            update_type=update_type,
            throttle_ms=throttle_ms
        )
        
        # Add subscription
        await self.subscription_manager.subscribe(websocket, subscription)
        
        # Confirm subscription
        await websocket.send_json({
            "status": "success",
            "message": f"Subscribed to {contract_id} {update_type} updates"
        })
    
    async def _handle_unsubscribe(self, websocket: WebSocket, data: Dict[str, Any]) -> None:
        """
        Handle unsubscribe command.
        
        Args:
            websocket: The WebSocket connection
            data: The command data
        """
        contract_id = data.get("contract_id")
        update_type = data.get("update_type", "all")
        
        if not contract_id:
            await websocket.send_json({
                "status": "error",
                "message": "Missing contract_id parameter"
            })
            return
        
        # Remove subscription
        result = await self.subscription_manager.unsubscribe(websocket, contract_id, update_type)
        
        # Confirm unsubscription
        if result:
            await websocket.send_json({
                "status": "success",
                "message": f"Unsubscribed from {contract_id} {update_type} updates"
            })
        else:
            await websocket.send_json({
                "status": "error",
                "message": f"Not subscribed to {contract_id} {update_type} updates"
            })
    
    async def publish_trade(self, contract_id: str, timestamp: int, price: float, quantity: int, side: int) -> int:
        """
        Publish a trade update.
        
        Args:
            contract_id: Contract identifier
            timestamp: Trade timestamp
            price: Trade price
            quantity: Trade quantity
            side: Trade side (0=bid, 1=ask)
            
        Returns:
            Number of clients the update was sent to
        """
        # Check throttling
        key = f"{contract_id}:trade"
        current_time = time.time()
        
        # Create trade update
        trade = TradeUpdate(
            contract_id=contract_id,
            timestamp=timestamp,
            price=price,
            quantity=quantity,
            side=side,
            update_time=current_time
        )
        
        # Broadcast to subscribers
        return await self.subscription_manager.broadcast_trade(trade)
    
    async def publish_orderbook(self, contract_id: str, timestamp: int, 
                               bid_levels: List[Dict[str, Any]], 
                               ask_levels: List[Dict[str, Any]]) -> int:
        """
        Publish an order book update.
        
        Args:
            contract_id: Contract identifier
            timestamp: Order book timestamp
            bid_levels: List of bid levels
            ask_levels: List of ask levels
            
        Returns:
            Number of clients the update was sent to
        """
        # Check throttling
        key = f"{contract_id}:book"
        current_time = time.time()
        
        # Create order book update
        book = OrderBookUpdate(
            contract_id=contract_id,
            timestamp=timestamp,
            bid_levels=bid_levels,
            ask_levels=ask_levels,
            update_time=current_time
        )
        
        # Broadcast to subscribers
        return await self.subscription_manager.broadcast_orderbook(book)