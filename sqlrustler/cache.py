"""
Query result caching with LRU eviction and TTL support.

Provides thread-safe caching for query results to improve performance
for repeated queries.
"""
import hashlib
import json
import time
from collections import OrderedDict
from threading import Lock
from typing import Any, Dict, List, Optional, Tuple


class QueryCache:
    """Thread-safe LRU cache for query results with TTL support."""
    
    def __init__(self, max_size: int = 1000, default_ttl: int = 300):
        """Initialize query cache.
        
        Args:
            max_size: Maximum number of cached queries
            default_ttl: Default time-to-live in seconds
        """
        self.max_size = max_size
        self.default_ttl = default_ttl
        self._cache: OrderedDict[str, Dict[str, Any]] = OrderedDict()
        self._lock = Lock()
        self._stats = {
            'hits': 0,
            'misses': 0,
            'evictions': 0,
            'invalidations': 0
        }
    
    def _generate_key(self, sql: str, params: List[Any]) -> str:
        """Generate cache key from SQL and parameters.
        
        Args:
            sql: SQL query string
            params: Query parameters
            
        Returns:
            Cache key hash
        """
        # Create deterministic key from SQL and params
        key_data = f"{sql}:{json.dumps(params, sort_keys=True, default=str)}"
        return hashlib.md5(key_data.encode()).hexdigest()
    
    def get(self, sql: str, params: List[Any]) -> Optional[List[Any]]:
        """Get cached query result.
        
        Args:
            sql: SQL query string
            params: Query parameters
            
        Returns:
            Cached result or None if not found/expired
        """
        key = self._generate_key(sql, params)
        
        with self._lock:
            if key not in self._cache:
                self._stats['misses'] += 1
                return None
            
            entry = self._cache[key]
            
            # Check if expired
            if time.time() > entry['expires_at']:
                del self._cache[key]
                self._stats['misses'] += 1
                return None
            
            # Move to end (most recently used)
            self._cache.move_to_end(key)
            self._stats['hits'] += 1
            
            return entry['result']
    
    def set(self, sql: str, params: List[Any], result: List[Any], ttl: Optional[int] = None) -> None:
        """Cache query result.
        
        Args:
            sql: SQL query string
            params: Query parameters
            result: Query result to cache
            ttl: Time-to-live in seconds (uses default if None)
        """
        key = self._generate_key(sql, params)
        ttl = ttl if ttl is not None else self.default_ttl
        
        with self._lock:
            # Evict oldest if at capacity
            if len(self._cache) >= self.max_size and key not in self._cache:
                self._cache.popitem(last=False)
                self._stats['evictions'] += 1
            
            self._cache[key] = {
                'result': result,
                'expires_at': time.time() + ttl,
                'created_at': time.time(),
                'sql': sql,  # Store SQL for invalidation matching
            }
            
            # Move to end (most recently used)
            self._cache.move_to_end(key)
    
    def invalidate_pattern(self, table_name: str) -> int:
        """Invalidate all cached queries for a table.
        
        Args:
            table_name: Name of table to invalidate
            
        Returns:
            Number of entries invalidated
        """
        count = 0
        
        with self._lock:
            # Find all keys where the SQL contains the table name
            keys_to_remove = []
            for key, entry in self._cache.items():
                # Check if table name is in the stored SQL
                if table_name.lower() in entry['sql'].lower():
                    keys_to_remove.append(key)
            
            for key in keys_to_remove:
                del self._cache[key]
                count += 1
            
            self._stats['invalidations'] += count
        
        return count
    
    def clear(self) -> None:
        """Clear all cached entries."""
        with self._lock:
            self._cache.clear()
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics.
        
        Returns:
            Dictionary with cache statistics
        """
        with self._lock:
            total_requests = self._stats['hits'] + self._stats['misses']
            hit_rate = (
                self._stats['hits'] / total_requests 
                if total_requests > 0 
                else 0.0
            )
            
            return {
                'size': len(self._cache),
                'max_size': self.max_size,
                'hits': self._stats['hits'],
                'misses': self._stats['misses'],
                'hit_rate': hit_rate,
                'evictions': self._stats['evictions'],
                'invalidations': self._stats['invalidations']
            }
    
    def reset_stats(self) -> None:
        """Reset cache statistics."""
        with self._lock:
            self._stats = {
                'hits': 0,
                'misses': 0,
                'evictions': 0,
                'invalidations': 0
            }


# Global cache instance
_global_cache: Optional[QueryCache] = None


def get_cache() -> QueryCache:
    """Get or create global cache instance."""
    global _global_cache
    if _global_cache is None:
        _global_cache = QueryCache()
    return _global_cache


def configure_cache(max_size: int = 1000, default_ttl: int = 300) -> None:
    """Configure global cache settings.
    
    Args:
        max_size: Maximum number of cached queries
        default_ttl: Default time-to-live in seconds
    """
    global _global_cache
    _global_cache = QueryCache(max_size=max_size, default_ttl=default_ttl)


def clear_cache() -> None:
    """Clear global cache."""
    cache = get_cache()
    cache.clear()


def get_cache_stats() -> Dict[str, Any]:
    """Get global cache statistics."""
    cache = get_cache()
    return cache.get_stats()
