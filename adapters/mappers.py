#!/usr/bin/env python3
"""
MI-3 News Scraper - Payload Mappers
Normalize vendor payloads to unified RawItem format.
"""

import logging
from typing import Dict, Any, Optional, List
from storage.schemas import RawItem

logger = logging.getLogger(__name__)

def map_ws_payload_to_raw(payload: Dict[str, Any], cfg: Dict[str, Any]) -> Optional[RawItem]:
    """
    Map WebSocket payload to RawItem.
    
    Args:
        payload: Raw WebSocket message payload
        cfg: WebSocket source configuration from settings
        
    Returns:
        RawItem instance or None if mapping failed
    """
    try:
        # Extract vendor-specific mapping strategy
        vendor = cfg.get('name', 'unknown').lower()
        
        # Vendor-specific mapping strategies
        if 'reuters' in vendor:
            return _map_reuters_ws(payload, cfg)
        elif 'bloomberg' in vendor:
            return _map_bloomberg_ws(payload, cfg)
        elif 'cnbc' in vendor:
            return _map_cnbc_ws(payload, cfg)
        else:
            return _map_generic_ws(payload, cfg)
            
    except Exception as e:
        logger.error(f"WebSocket mapping error for {cfg.get('name', 'unknown')}: {e}")
        logger.debug(f"Failed payload: {payload}")
        return None

def map_webhook_payload_to_raw(payload: Dict[str, Any], headers: Dict[str, str]) -> Optional[RawItem]:
    """
    Map webhook payload to RawItem.
    
    Args:
        payload: Webhook POST body (JSON)
        headers: HTTP headers from webhook request
        
    Returns:
        RawItem instance or None if mapping failed
    """
    try:
        # Determine vendor from headers or payload
        vendor = (
            headers.get('X-Vendor', '') or
            headers.get('User-Agent', '') or 
            payload.get('vendor', '') or
            payload.get('source', 'unknown')
        ).lower()
        
        # Vendor-specific mapping strategies
        if 'reuters' in vendor:
            return _map_reuters_webhook(payload, headers)
        elif 'bloomberg' in vendor:
            return _map_bloomberg_webhook(payload, headers)
        elif 'cnbc' in vendor or 'nbc' in vendor:
            return _map_cnbc_webhook(payload, headers)
        elif 'yahoo' in vendor:
            return _map_yahoo_webhook(payload, headers)
        else:
            return _map_generic_webhook(payload, headers)
            
    except Exception as e:
        logger.error(f"Webhook mapping error for vendor '{vendor}': {e}")
        logger.debug(f"Failed payload: {payload}")
        return None

def map_newswire_to_raw(payload: Dict[str, Any], cfg: Dict[str, Any]) -> Optional[RawItem]:
    """
    Map newswire payload to RawItem.
    
    Args:
        payload: Newswire message payload
        cfg: Newswire source configuration
        
    Returns:
        RawItem instance or None if mapping failed
    """
    try:
        vendor = cfg.get('vendor', 'unknown').lower()
        
        # Vendor-specific mapping strategies
        if vendor in ['bloomberg', 'bloomberg_api']:
            return _map_bloomberg_newswire(payload, cfg)
        elif vendor in ['reuters', 'reuters_eikon']:
            return _map_reuters_newswire(payload, cfg)
        elif vendor in ['dow_jones', 'factiva']:
            return _map_dow_jones_newswire(payload, cfg)
        else:
            return _map_generic_newswire(payload, cfg)
            
    except Exception as e:
        logger.error(f"Newswire mapping error for {cfg.get('vendor', 'unknown')}: {e}")
        logger.debug(f"Failed payload: {payload}")
        return None

# WebSocket vendor-specific mappers

def _map_reuters_ws(payload: Dict[str, Any], cfg: Dict[str, Any]) -> RawItem:
    """Map Reuters WebSocket message"""
    # Reuters WebSocket typically sends:
    # {"id": "...", "headline": "...", "url": "...", "timestamp": "...", "category": "..."}
    return RawItem(
        id='',  # Will be generated
        topic=payload.get('category', cfg.get('topic', 'business')),
        title=payload.get('headline', payload.get('title', 'No Title')),
        link=payload.get('url', payload.get('link', '')),
        published=RawItem._normalize_datetime(payload.get('timestamp', payload.get('published'))),
        source=f"websocket:{cfg['name']}",
        publisher='Reuters',
        summary=payload.get('summary', payload.get('lead')),
        tags=','.join(payload.get('tags', [])) if payload.get('tags') else None,
        raw_payload=payload
    )

def _map_bloomberg_ws(payload: Dict[str, Any], cfg: Dict[str, Any]) -> RawItem:
    """Map Bloomberg WebSocket message"""
    # Bloomberg WebSocket format:
    # {"story_id": "...", "headline": "...", "url": "...", "datetime": "...", "topic": "..."}
    return RawItem(
        id='',
        topic=payload.get('topic', payload.get('category', cfg.get('topic', 'markets'))),
        title=payload.get('headline', payload.get('title', 'No Title')),
        link=payload.get('url', payload.get('story_url', '')),
        published=RawItem._normalize_datetime(payload.get('datetime', payload.get('timestamp'))),
        source=f"websocket:{cfg['name']}",
        publisher='Bloomberg',
        summary=payload.get('summary', payload.get('abstract')),
        tags=payload.get('keywords'),
        raw_payload=payload
    )

def _map_cnbc_ws(payload: Dict[str, Any], cfg: Dict[str, Any]) -> RawItem:
    """Map CNBC WebSocket message"""
    # CNBC format may vary
    return RawItem(
        id='',
        topic=payload.get('section', cfg.get('topic', 'business')),
        title=payload.get('title', payload.get('headline', 'No Title')),
        link=payload.get('link', payload.get('url', '')),
        published=RawItem._normalize_datetime(payload.get('datePublished', payload.get('timestamp'))),
        source=f"websocket:{cfg['name']}",
        publisher='CNBC',
        summary=payload.get('description', payload.get('summary')),
        raw_payload=payload
    )

def _map_generic_ws(payload: Dict[str, Any], cfg: Dict[str, Any]) -> RawItem:
    """Generic WebSocket mapper for unknown vendors"""
    # Try common field names
    title_fields = ['title', 'headline', 'subject', 'summary']
    link_fields = ['url', 'link', 'href', 'story_url']
    time_fields = ['timestamp', 'published', 'datetime', 'created_at', 'date']
    summary_fields = ['summary', 'description', 'body', 'abstract', 'lead']
    
    title = next((payload.get(f) for f in title_fields if payload.get(f)), 'No Title')
    link = next((payload.get(f) for f in link_fields if payload.get(f)), '')
    timestamp = next((payload.get(f) for f in time_fields if payload.get(f)), None)
    summary = next((payload.get(f) for f in summary_fields if payload.get(f)), None)
    
    return RawItem(
        id='',
        topic=cfg.get('topic', 'news'),
        title=str(title).strip(),
        link=str(link).strip(),
        published=RawItem._normalize_datetime(timestamp),
        source=f"websocket:{cfg['name']}",
        publisher=cfg.get('publisher', cfg['name']),
        summary=summary,
        raw_payload=payload
    )

# Webhook vendor-specific mappers

def _map_reuters_webhook(payload: Dict[str, Any], headers: Dict[str, str]) -> RawItem:
    """Map Reuters webhook payload"""
    return RawItem(
        id='',
        topic=payload.get('category', 'business'),
        title=payload.get('headline', payload.get('title', 'No Title')),
        link=payload.get('canonical_url', payload.get('url', '')),
        published=RawItem._normalize_datetime(payload.get('date_published')),
        source="webhook:reuters",
        publisher="Reuters",
        summary=payload.get('description', payload.get('lead')),
        tags=','.join(payload.get('topics', [])) if payload.get('topics') else None,
        raw_payload=payload
    )

def _map_bloomberg_webhook(payload: Dict[str, Any], headers: Dict[str, str]) -> RawItem:
    """Map Bloomberg webhook payload"""
    return RawItem(
        id='',
        topic=payload.get('primary_category', payload.get('category', 'markets')),
        title=payload.get('headline', 'No Title'),
        link=payload.get('story_url', payload.get('url', '')),
        published=RawItem._normalize_datetime(payload.get('published_at')),
        source="webhook:bloomberg",
        publisher="Bloomberg",
        summary=payload.get('abstract', payload.get('summary')),
        tags=','.join(payload.get('tags', [])) if payload.get('tags') else None,
        raw_payload=payload
    )

def _map_cnbc_webhook(payload: Dict[str, Any], headers: Dict[str, str]) -> RawItem:
    """Map CNBC webhook payload"""
    return RawItem(
        id='',
        topic=payload.get('section', 'business'),
        title=payload.get('headline', payload.get('title', 'No Title')),
        link=payload.get('url', ''),
        published=RawItem._normalize_datetime(payload.get('dateFirstPublished')),
        source="webhook:cnbc",
        publisher="CNBC",
        summary=payload.get('description'),
        raw_payload=payload
    )

def _map_yahoo_webhook(payload: Dict[str, Any], headers: Dict[str, str]) -> RawItem:
    """Map Yahoo Finance webhook payload"""
    return RawItem(
        id='',
        topic=payload.get('category', 'finance'),
        title=payload.get('title', 'No Title'),
        link=payload.get('link', ''),
        published=RawItem._normalize_datetime(payload.get('pubDate')),
        source="webhook:yahoo",
        publisher="Yahoo Finance",
        summary=payload.get('summary'),
        raw_payload=payload
    )

def _map_generic_webhook(payload: Dict[str, Any], headers: Dict[str, str]) -> RawItem:
    """Generic webhook mapper"""
    vendor = headers.get('X-Vendor', 'unknown')
    
    # Try common field names for webhooks
    title_fields = ['title', 'headline', 'subject', 'name']
    link_fields = ['url', 'link', 'href', 'canonical_url', 'story_url']
    time_fields = ['published', 'datePublished', 'created_at', 'timestamp', 'date']
    summary_fields = ['description', 'summary', 'abstract', 'excerpt']
    
    title = next((payload.get(f) for f in title_fields if payload.get(f)), 'No Title')
    link = next((payload.get(f) for f in link_fields if payload.get(f)), '')
    timestamp = next((payload.get(f) for f in time_fields if payload.get(f)), None)
    summary = next((payload.get(f) for f in summary_fields if payload.get(f)), None)
    
    return RawItem(
        id='',
        topic=payload.get('category', payload.get('topic', 'news')),
        title=str(title).strip(),
        link=str(link).strip(),
        published=RawItem._normalize_datetime(timestamp),
        source=f"webhook:{vendor}",
        publisher=payload.get('publisher', vendor),
        summary=summary,
        raw_payload=payload
    )

# Newswire vendor-specific mappers

def _map_bloomberg_newswire(payload: Dict[str, Any], cfg: Dict[str, Any]) -> RawItem:
    """Map Bloomberg Terminal/API newswire message"""
    return RawItem(
        id='',
        topic=cfg.get('topic', payload.get('category', 'markets')),
        title=payload.get('headline', payload.get('title', 'No Title')),
        link=payload.get('url', f"bloomberg://story/{payload.get('story_id', '')}"),
        published=RawItem._normalize_datetime(payload.get('published_date')),
        source=f"newswire:{cfg['vendor']}",
        publisher="Bloomberg Terminal",
        summary=payload.get('story_abstract'),
        tags=','.join(payload.get('topics', [])) if payload.get('topics') else None,
        raw_payload=payload
    )

def _map_reuters_newswire(payload: Dict[str, Any], cfg: Dict[str, Any]) -> RawItem:
    """Map Reuters Eikon/Terminal newswire message"""
    return RawItem(
        id='',
        topic=cfg.get('topic', payload.get('category', 'business')),
        title=payload.get('headline', 'No Title'),
        link=payload.get('url', f"reuters://story/{payload.get('storyId', '')}"),
        published=RawItem._normalize_datetime(payload.get('versionCreated')),
        source=f"newswire:{cfg['vendor']}",
        publisher="Reuters Terminal",
        summary=payload.get('bodyText'),
        tags=payload.get('subject'),
        raw_payload=payload
    )

def _map_dow_jones_newswire(payload: Dict[str, Any], cfg: Dict[str, Any]) -> RawItem:
    """Map Dow Jones/Factiva newswire message"""
    return RawItem(
        id='',
        topic=cfg.get('topic', payload.get('category', 'business')),
        title=payload.get('headline', payload.get('title', 'No Title')),
        link=payload.get('url', f"factiva://article/{payload.get('an', '')}"),
        published=RawItem._normalize_datetime(payload.get('publication_date')),
        source=f"newswire:{cfg['vendor']}",
        publisher=payload.get('source_name', 'Dow Jones'),
        summary=payload.get('snippet', payload.get('lead_paragraph')),
        raw_payload=payload
    )

def _map_generic_newswire(payload: Dict[str, Any], cfg: Dict[str, Any]) -> RawItem:
    """Generic newswire mapper"""
    # Standard newswire field attempts
    title_fields = ['headline', 'title', 'subject']
    link_fields = ['url', 'link', 'uri']
    time_fields = ['published_date', 'date_created', 'timestamp']
    summary_fields = ['body', 'text', 'summary', 'abstract']
    
    title = next((payload.get(f) for f in title_fields if payload.get(f)), 'No Title')
    link = next((payload.get(f) for f in link_fields if payload.get(f)), '')
    timestamp = next((payload.get(f) for f in time_fields if payload.get(f)), None)
    summary = next((payload.get(f) for f in summary_fields if payload.get(f)), None)
    
    return RawItem(
        id='',
        topic=cfg.get('topic', 'news'),
        title=str(title).strip(),
        link=str(link).strip(),
        published=RawItem._normalize_datetime(timestamp),
        source=f"newswire:{cfg['vendor']}",
        publisher=cfg.get('publisher', cfg['vendor']),
        summary=summary,
        raw_payload=payload
    )

def safe_map_payload(payload: Dict[str, Any], 
                    adapter_type: str,
                    config: Optional[Dict[str, Any]] = None,
                    headers: Optional[Dict[str, str]] = None) -> Optional[RawItem]:
    """
    Safely map any payload to RawItem with comprehensive error handling.
    
    Args:
        payload: Raw message payload
        adapter_type: 'websocket', 'webhook', or 'newswire'
        config: Source configuration (required for websocket/newswire)
        headers: HTTP headers (required for webhook)
        
    Returns:
        RawItem instance or None if mapping failed
    """
    try:
        if adapter_type == 'websocket':
            if not config:
                raise ValueError("WebSocket mapping requires config")
            return map_ws_payload_to_raw(payload, config)
        
        elif adapter_type == 'webhook':
            if not headers:
                raise ValueError("Webhook mapping requires headers")
            return map_webhook_payload_to_raw(payload, headers)
        
        elif adapter_type == 'newswire':
            if not config:
                raise ValueError("Newswire mapping requires config")
            return map_newswire_to_raw(payload, config)
        
        else:
            raise ValueError(f"Unknown adapter type: {adapter_type}")
            
    except Exception as e:
        logger.error(f"Safe mapping failed for {adapter_type}: {e}")
        return None