#!/usr/bin/env python3
"""
MI-3 News Scraper - Unified Storage Schemas
Shared schema definitions for all news sources (RSS, WebSocket, webhook, newswire).
"""

import hashlib
import logging
from datetime import datetime, timezone
from typing import Dict, Any, Optional
from dataclasses import dataclass, asdict
from dateutil import parser as date_parser

logger = logging.getLogger(__name__)

@dataclass
class RawItem:
    """
    Unified schema for news items from any source (RSS, WebSocket, webhook, newswire).
    
    This schema is used across all adapters and ensures consistent downstream processing
    for sentiment analysis, storage, and SSE streaming.
    """
    id: str               # SHA256 hash of link|title|published_norm
    topic: str            # news category/topic (business, finance, markets, etc.)
    title: str            # Article headline
    link: str             # Article URL
    published: str        # ISO 8601 UTC datetime string
    source: str           # Source identifier (websocket, webhook, newswire, or RSS feed name)
    publisher: str        # Publisher/vendor name
    summary: Optional[str] = None        # Article summary/description
    tags: Optional[str] = None           # Comma-separated tags
    raw_payload: Optional[Dict[str, Any]] = None  # Original payload for debugging
    
    def __post_init__(self):
        """Generate unique ID and validate required fields after initialization"""
        if not self.id:
            self.id = self.make_id()
        
        # Validate required fields
        if not all([self.title, self.link, self.published, self.source, self.publisher]):
            missing = [f for f in ['title', 'link', 'published', 'source', 'publisher'] 
                      if not getattr(self, f)]
            raise ValueError(f"Missing required fields: {missing}")
    
    def make_id(self) -> str:
        """
        Generate unique ID from link|title|normalized_published.
        
        Uses SHA256 hash of canonical content to ensure deduplication
        across different sources reporting the same story.
        """
        # Normalize published date for consistent hashing
        published_norm = self._normalize_published_for_id()
        
        # Create canonical content string
        content = f"{self.link}|{self.title}|{published_norm}"
        
        # Generate SHA256 hash (first 16 characters for readability)
        return hashlib.sha256(content.encode('utf-8')).hexdigest()[:16]
    
    def _normalize_published_for_id(self) -> str:
        """Normalize published datetime for ID generation (removes subsecond precision)"""
        try:
            if self.published:
                dt = date_parser.parse(self.published)
                # Round to nearest minute for ID stability
                return dt.replace(second=0, microsecond=0).isoformat()
            return ""
        except Exception:
            return ""
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        data = asdict(self)
        
        # Remove None values to keep JSON clean
        return {k: v for k, v in data.items() if v is not None}
    
    def to_legacy_dict(self) -> Dict[str, Any]:
        """
        Convert to legacy format for backward compatibility with existing RSS code.
        Maps new schema fields to expected legacy field names.
        """
        return {
            'id': self.id,
            'title': self.title,
            'link': self.link,
            'published': self.published,
            'source': self.source,
            'publisher': self.publisher,
            'summary': self.summary or '',
            'category': self.topic,  # Map topic -> category for legacy
        }
    
    @classmethod
    def from_legacy_dict(cls, data: Dict[str, Any]) -> 'RawItem':
        """Create RawItem from legacy RSS format"""
        return cls(
            id=data.get('id', ''),
            topic=data.get('category', 'news'),
            title=data['title'],
            link=data['link'],
            published=data['published'],
            source=data['source'],
            publisher=data['publisher'],
            summary=data.get('summary'),
        )
    
    @classmethod
    def from_websocket_payload(cls, payload: Dict[str, Any], source_config: Dict[str, Any]) -> 'RawItem':
        """Create RawItem from WebSocket payload - to be customized per vendor"""
        # This is a generic implementation - specific adapters should override
        return cls(
            id='',  # Will be generated
            topic=source_config.get('topic', 'news'),
            title=payload.get('title', payload.get('headline', 'No Title')),
            link=payload.get('url', payload.get('link', '')),
            published=cls._normalize_datetime(payload.get('timestamp', payload.get('published'))),
            source=f"websocket:{source_config['name']}",
            publisher=source_config.get('publisher', source_config['name']),
            summary=payload.get('summary', payload.get('description')),
            tags=payload.get('tags'),
            raw_payload=payload
        )
    
    @classmethod
    def from_webhook_payload(cls, payload: Dict[str, Any], headers: Dict[str, str]) -> 'RawItem':
        """Create RawItem from webhook payload - to be customized per vendor"""
        # Generic implementation - specific webhook handlers should override
        vendor = headers.get('X-Vendor', 'unknown')
        
        return cls(
            id='',  # Will be generated
            topic=payload.get('category', payload.get('topic', 'news')),
            title=payload.get('title', payload.get('headline', 'No Title')),
            link=payload.get('url', payload.get('link', '')),
            published=cls._normalize_datetime(payload.get('published', payload.get('timestamp'))),
            source=f"webhook:{vendor}",
            publisher=payload.get('publisher', vendor),
            summary=payload.get('summary', payload.get('description')),
            tags=payload.get('tags'),
            raw_payload=payload
        )
    
    @classmethod
    def from_newswire_payload(cls, payload: Dict[str, Any], vendor_config: Dict[str, Any]) -> 'RawItem':
        """Create RawItem from newswire payload - to be customized per vendor"""
        return cls(
            id='',  # Will be generated
            topic=vendor_config.get('topic', payload.get('category', 'news')),
            title=payload.get('title', payload.get('headline', 'No Title')),
            link=payload.get('url', payload.get('link', '')),
            published=cls._normalize_datetime(payload.get('published', payload.get('timestamp'))),
            source=f"newswire:{vendor_config['vendor']}",
            publisher=payload.get('publisher', vendor_config['vendor']),
            summary=payload.get('body', payload.get('summary')),
            tags=payload.get('tags'),
            raw_payload=payload
        )
    
    @staticmethod
    def _normalize_datetime(dt_input: Any) -> str:
        """
        Normalize various datetime formats to ISO 8601 UTC string.
        
        Handles:
        - Unix timestamps (int/float)
        - ISO strings
        - Datetime objects
        - None/empty values (fallback to current time)
        """
        if not dt_input:
            return datetime.now(timezone.utc).isoformat()
        
        try:
            # Handle Unix timestamp
            if isinstance(dt_input, (int, float)):
                dt = datetime.fromtimestamp(dt_input, tz=timezone.utc)
                return dt.isoformat()
            
            # Handle string datetime
            if isinstance(dt_input, str):
                dt = date_parser.parse(dt_input)
                # Ensure UTC timezone
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                else:
                    dt = dt.astimezone(timezone.utc)
                return dt.isoformat()
            
            # Handle datetime object
            if isinstance(dt_input, datetime):
                if dt_input.tzinfo is None:
                    dt_input = dt_input.replace(tzinfo=timezone.utc)
                else:
                    dt_input = dt_input.astimezone(timezone.utc)
                return dt_input.isoformat()
            
            # Fallback for unknown types
            logger.warning(f"Unknown datetime format: {type(dt_input)} = {dt_input}")
            return datetime.now(timezone.utc).isoformat()
            
        except Exception as e:
            logger.error(f"Error normalizing datetime '{dt_input}': {e}")
            return datetime.now(timezone.utc).isoformat()

def validate_raw_item(item_data: Dict[str, Any]) -> bool:
    """
    Validate that item_data contains required fields for RawItem.
    
    Args:
        item_data: Dictionary representation of item
        
    Returns:
        True if valid, False otherwise
    """
    required_fields = ['title', 'link', 'published', 'source', 'publisher']
    
    for field in required_fields:
        if not item_data.get(field):
            logger.error(f"Invalid RawItem: missing '{field}'")
            return False
    
    # Validate published date format
    try:
        date_parser.parse(item_data['published'])
    except Exception as e:
        logger.error(f"Invalid RawItem: bad published date '{item_data.get('published')}': {e}")
        return False
    
    return True