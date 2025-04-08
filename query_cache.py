#!/usr/bin/env python3
"""
Query cache implementation using Redis for the ETL pipeline.
"""

import logging
import time
import json
import hashlib
import threading
from typing import Dict, List, Any, Optional, Tuple, Union, Callable
import redis
from redis.exceptions import RedisError

logger = logging.getLogger("SierraChartETL.cache")

class QueryCache:
    """
    Cache for database queries using Redis.
    Improves performance for frequently accessed data.
    """
    
    def __init__(self, 
                 redis_host: str = 'localhost', 
                 redis_port: int = 6379,
                 redis_db: int = 0,
                 redis_password: Optional[str] = None,
                 default_ttl: int = 60,  # seconds
                 namespace: str = 'sietl:',
                 enable_stats: bool = True):
        """
        Initialize the query cache.
        
        Args:
            redis_host: Redis server hostname
            redis_port: Redis server port
            redis_db: Redis database number
            redis_password: Redis password
            default_ttl: Default time-to-live for cache entries
            namespace: Namespace prefix for cache keys
            enable_stats: Whether to track cache statistics
        """
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis_db = redis_db
        self.redis_password = redis_password
        self.default_ttl = default_ttl
        self.namespace = namespace
        self.enable_stats = enable_stats
        
        # Initialize Redis client
        self.redis = redis.Redis(
            host=redis_host,
            port=redis_port,
            db=redis_db,
            password=redis_password,
            socket_timeout=5,
            socket_connect_timeout=5,
            retry_on_timeout=True
        )
        
        # Cache statistics
        self.stats = {
            "hits": 0,
            "misses": 0,
            "errors": 0,
            "sets": 0,
            "invalidations": 0,
            "start_time": time.time()
        }
        
        # Lock for thread safety
        self.lock = threading.RLock()
    
    def generate_key(self, query_type: str, params: Dict[str, Any]) -> str:
        """
        Generate a cache key from query parameters.
        
        Args:
            query_type: Type of query
            params: Query parameters
            
        Returns:
            Cache key
        """
        # Sort parameters for consistent key generation
        sorted_params = {k: params[k] for k in sorted(params.keys())}
        
        # Create a string representation
        param_str = json.dumps(sorted_params, sort_keys=True)
        
        # Generate hash
        key_hash = hashlib.md5(param_str.encode('utf-8')).hexdigest()
        
        # Format key
        return f"{self.namespace}{query_type}:{key_hash}"
    
    def get(self, query_type: str, params: Dict[str, Any]) -> Optional[Any]:
        """
        Get data from the cache.
        
        Args:
            query_type: Type of query
            params: Query parameters
            
        Returns:
            Cached data, or None if not found
        """
        key = self.generate_key(query_type, params)
        
        try:
            data = self.redis.get(key)
            
            if data:
                # Update stats
                if self.enable_stats:
                    with self.lock:
                        self.stats["hits"] += 1
                
                # Deserialize
                return json.loads(data)
            else:
                # Update stats
                if self.enable_stats:
                    with self.lock:
                        self.stats["misses"] += 1
                
                return None
        
        except RedisError as e:
            logger.warning(f"Redis error getting cache key {key}: {str(e)}")
            
            # Update stats
            if self.enable_stats:
                with self.lock:
                    self.stats["errors"] += 1
            
            return None
    
    def set(self, query_type: str, params: Dict[str, Any], data: Any, ttl: Optional[int] = None) -> bool:
        """
        Set data in the cache.
        
        Args:
            query_type: Type of query
            params: Query parameters
            data: Data to cache
            ttl: Time-to-live in seconds, or None to use default
            
        Returns:
            True if successful, False otherwise
        """
        key = self.generate_key(query_type, params)
        ttl = ttl if ttl is not None else self.default_ttl
        
        try:
            # Serialize data
            serialized = json.dumps(data)
            
            # Set in Redis
            self.redis.setex(key, ttl, serialized)
            
            # Update stats
            if self.enable_stats:
                with self.lock:
                    self.stats["sets"] += 1
            
            return True
        
        except RedisError as e:
            logger.warning(f"Redis error setting cache key {key}: {str(e)}")
            
            # Update stats
            if self.enable_stats:
                with self.lock:
                    self.stats["errors"] += 1
            
            return False
    
    def invalidate(self, query_type: str, params: Optional[Dict[str, Any]] = None) -> int:
        """
        Invalidate cache entries.
        
        Args:
            query_type: Type of query
            params: Query parameters, or None to invalidate all entries of this type
            
        Returns:
            Number of entries invalidated
        """
        try:
            if params:
                # Invalidate specific key
                key = self.generate_key(query_type, params)
                self.redis.delete(key)
                count = 1
            else:
                # Invalidate all keys of this type
                pattern = f"{self.namespace}{query_type}:*"
                keys = self.redis.keys(pattern)
                
                if keys:
                    count = self.redis.delete(*keys)
                else:
                    count = 0
            
            # Update stats
            if self.enable_stats:
                with self.lock:
                    self.stats["invalidations"] += count
            
            return count
        
        except RedisError as e:
            logger.warning(f"Redis error invalidating cache: {str(e)}")
            
            # Update stats
            if self.enable_stats:
                with self.lock:
                    self.stats["errors"] += 1
            
            return 0
    
    def invalidate_all(self) -> int:
        """
        Invalidate all cache entries in the namespace.
        
        Returns:
            Number of entries invalidated
        """
        try:
            pattern = f"{self.namespace}*"
            keys = self.redis.keys(pattern)
            
            if keys:
                count = self.redis.delete(*keys)
            else:
                count = 0
            
            # Update stats
            if self.enable_stats:
                with self.lock:
                    self.stats["invalidations"] += count
            
            return count
        
        except RedisError as e:
            logger.warning(f"Redis error invalidating all cache: {str(e)}")
            
            # Update stats
            if self.enable_stats:
                with self.lock:
                    self.stats["errors"] += 1
            
            return 0
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        with self.lock:
            stats_copy = self.stats.copy()
            
            # Calculate derived stats
            total_requests = stats_copy["hits"] + stats_copy["misses"]
            
            if total_requests > 0:
                stats_copy["hit_ratio"] = stats_copy["hits"] / total_requests
            else:
                stats_copy["hit_ratio"] = 0
            
            # Get Redis statistics
            try:
                info = self.redis.info()
                stats_copy["redis_memory_used"] = info.get("used_memory", 0)
                stats_copy["redis_connected_clients"] = info.get("connected_clients", 0)
                stats_copy["redis_total_keys"] = sum(count for dbname, count in [
                    (name, info[name]) for name in info if name.startswith('db')
                ])
            except RedisError as e:
                logger.warning(f"Error getting Redis info: {str(e)}")
            
            return stats_copy
    
    def cached_query(self, ttl: Optional[int] = None) -> Callable:
        """
        Decorator for caching function results.
        
        Args:
            ttl: Time-to-live in seconds, or None to use default
            
        Returns:
            Decorator function
        """
        def decorator(func: Callable) -> Callable:
            def wrapper(*args, **kwargs) -> Any:
                # Extract query type from function name
                query_type = func.__name__
                
                # Combine args and kwargs into params
                params = {
                    "args": args,
                    "kwargs": kwargs
                }
                
                # Try to get from cache
                cached_result = self.get(query_type, params)
                
                if cached_result is not None:
                    return cached_result
                
                # Execute function
                result = func(*args, **kwargs)
                
                # Store in cache
                self.set(query_type, params, result, ttl)
                
                return result
            
            return wrapper
        
        return decorator