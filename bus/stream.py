#!/usr/bin/env python3
"""
MI-3 News Scraper - Message Bus Stream
Unified event streaming bus for all news sources with deduplication and rate limiting.
"""

import json
import time
import logging
import asyncio
from collections import defaultdict, deque
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional, Set, Callable
from dataclasses import dataclass
import threading
from storage.schemas import RawItem, validate_raw_item

logger = logging.getLogger(__name__)

@dataclass
class StreamMessage:
    """Message in the event stream"""
    id: str
    channel: str
    data: Dict[str, Any]
    timestamp: float
    source: str

class RateLimiter:
    """Token bucket rate limiter per source"""
    
    def __init__(self, tokens_per_second: float = 10.0, max_tokens: int = 50):
        self.tokens_per_second = tokens_per_second
        self.max_tokens = max_tokens
        self.tokens = max_tokens
        self.last_update = time.time()
        self._lock = threading.Lock()
    
    def allow(self) -> bool:
        """Check if request is allowed (consumes 1 token if available)"""
        with self._lock:
            now = time.time()
            elapsed = now - self.last_update
            
            # Add tokens based on elapsed time
            self.tokens = min(self.max_tokens, self.tokens + elapsed * self.tokens_per_second)
            self.last_update = now
            
            if self.tokens >= 1.0:
                self.tokens -= 1.0
                return True
            else:
                return False

class EventBus:
    """
    Unified event streaming bus for MI-3 news ingestion.
    
    Provides:
    - Multi-channel message streaming (news.raw, news.processed, etc.)
    - Deduplication based on message ID
    - Rate limiting per source
    - Recent message tracking
    - Async/sync publishing support
    """
    
    def __init__(self, 
                 max_recent_items: int = 10000,
                 recent_ttl_seconds: int = 3600,
                 default_rate_limit: float = 10.0):
        
        self.max_recent_items = max_recent_items
        self.recent_ttl_seconds = recent_ttl_seconds
        self.default_rate_limit = default_rate_limit
        
        # Message storage
        self.channels: Dict[str, deque] = defaultdict(lambda: deque(maxlen=1000))
        self.recent_ids: Set[str] = set()
        self.recent_timestamps: Dict[str, float] = {}
        
        # Rate limiting per source
        self.rate_limiters: Dict[str, RateLimiter] = {}
        
        # Subscribers (callbacks for real-time processing)
        self.subscribers: Dict[str, List[Callable]] = defaultdict(list)
        
        # Thread safety
        self._lock = threading.RLock()
        
        logger.info("EventBus initialized")
    
    def _get_rate_limiter(self, source: str) -> RateLimiter:
        """Get or create rate limiter for source"""
        if source not in self.rate_limiters:
            self.rate_limiters[source] = RateLimiter(
                tokens_per_second=self.default_rate_limit,
                max_tokens=int(self.default_rate_limit * 5)  # 5 second burst
            )
        return self.rate_limiters[source]
    
    def seen_recent(self, message_id: str) -> bool:
        """
        Check if message ID has been seen recently (for deduplication).
        
        Args:
            message_id: Unique identifier for the message
            
        Returns:
            True if seen recently, False otherwise
        """
        with self._lock:
            # Clean up old entries
            self._cleanup_recent()
            
            return message_id in self.recent_ids
    
    def _cleanup_recent(self):
        """Remove old entries from recent tracking"""
        cutoff = time.time() - self.recent_ttl_seconds
        expired_ids = [
            msg_id for msg_id, ts in self.recent_timestamps.items()
            if ts < cutoff
        ]
        
        for msg_id in expired_ids:
            self.recent_ids.discard(msg_id)
            self.recent_timestamps.pop(msg_id, None)
        
        # Enforce max size limit
        while len(self.recent_ids) > self.max_recent_items:
            # Remove oldest entry
            oldest_id = min(self.recent_timestamps.keys(), 
                          key=lambda x: self.recent_timestamps[x])
            self.recent_ids.discard(oldest_id)
            self.recent_timestamps.pop(oldest_id, None)
    
    def _mark_seen(self, message_id: str):
        """Mark message ID as recently seen"""
        with self._lock:
            self.recent_ids.add(message_id)
            self.recent_timestamps[message_id] = time.time()
    
    def xadd_json(self, channel: str, data: Dict[str, Any], source: str = "unknown") -> bool:
        """
        Add JSON message to stream channel with deduplication and rate limiting.
        
        Args:
            channel: Stream channel (e.g., "news.raw")
            data: Message data (should be RawItem dict for news.raw)
            source: Source identifier for rate limiting
            
        Returns:
            True if message was added, False if rejected (duplicate/rate limit)
        """
        
        # Validate data for news.raw channel
        if channel == "news.raw":
            if not validate_raw_item(data):
                logger.error(f"Invalid RawItem data from {source}: {data}")
                return False
            message_id = data.get('id')
        else:
            message_id = data.get('id', f"{channel}:{time.time()}")
        
        if not message_id:
            logger.error(f"Message missing ID from {source}: {data}")
            return False
        
        # Check rate limiting
        rate_limiter = self._get_rate_limiter(source)
        if not rate_limiter.allow():
            logger.warning(f"Rate limit exceeded for source {source}, dropping message {message_id}")
            return False
        
        # Check deduplication
        if self.seen_recent(message_id):
            logger.debug(f"Duplicate message {message_id} from {source}, skipping")
            return False
        
        # Create stream message
        message = StreamMessage(
            id=message_id,
            channel=channel,
            data=data,
            timestamp=time.time(),
            source=source
        )
        
        with self._lock:
            # Add to channel
            self.channels[channel].append(message)
            
            # Mark as seen
            self._mark_seen(message_id)
            
            # Notify subscribers
            for callback in self.subscribers[channel]:
                try:
                    callback(message)
                except Exception as e:
                    logger.error(f"Subscriber callback error for {channel}: {e}")
        
        logger.debug(f"Added message {message_id} to {channel} from {source}")
        return True
    
    async def xadd_json_async(self, channel: str, data: Dict[str, Any], source: str = "unknown") -> bool:
        """Async version of xadd_json (runs in thread pool)"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.xadd_json, channel, data, source)
    
    def get_recent_messages(self, channel: str, limit: int = 100) -> List[Dict[str, Any]]:
        """Get recent messages from channel"""
        with self._lock:
            messages = list(self.channels[channel])
            # Return most recent first
            messages.reverse()
            return [msg.data for msg in messages[:limit]]
    
    def subscribe(self, channel: str, callback: Callable[[StreamMessage], None]):
        """Subscribe to channel with callback function"""
        with self._lock:
            self.subscribers[channel].append(callback)
        logger.info(f"New subscriber added to {channel}")
    
    def unsubscribe(self, channel: str, callback: Callable[[StreamMessage], None]):
        """Unsubscribe callback from channel"""
        with self._lock:
            try:
                self.subscribers[channel].remove(callback)
                logger.info(f"Subscriber removed from {channel}")
            except ValueError:
                pass
    
    def get_stats(self) -> Dict[str, Any]:
        """Get bus statistics"""
        with self._lock:
            return {
                'channels': {
                    channel: len(messages) 
                    for channel, messages in self.channels.items()
                },
                'recent_ids_count': len(self.recent_ids),
                'rate_limiters': {
                    source: {
                        'tokens': limiter.tokens,
                        'tokens_per_second': limiter.tokens_per_second
                    }
                    for source, limiter in self.rate_limiters.items()
                },
                'subscribers': {
                    channel: len(callbacks)
                    for channel, callbacks in self.subscribers.items()
                }
            }

# Global event bus instance
_event_bus: Optional[EventBus] = None

def get_event_bus() -> EventBus:
    """Get global event bus instance (singleton)"""
    global _event_bus
    if _event_bus is None:
        _event_bus = EventBus()
    return _event_bus

# Convenience functions for common operations
stream = get_event_bus()  # Global reference for adapters

def publish_raw_item(item: RawItem, source: str = "unknown") -> bool:
    """Publish RawItem to news.raw channel"""
    return stream.xadd_json("news.raw", item.to_dict(), source)

async def publish_raw_item_async(item: RawItem, source: str = "unknown") -> bool:
    """Async publish RawItem to news.raw channel"""
    return await stream.xadd_json_async("news.raw", item.to_dict(), source)

def subscribe_to_raw_news(callback: Callable[[StreamMessage], None]):
    """Subscribe to news.raw channel"""
    stream.subscribe("news.raw", callback)

def get_recent_raw_news(limit: int = 100) -> List[Dict[str, Any]]:
    """Get recent raw news items"""
    return stream.get_recent_messages("news.raw", limit)