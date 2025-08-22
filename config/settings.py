#!/usr/bin/env python3
"""
MI-3 News Scraper - Settings and Configuration
Centralized settings management with environment variable support.
"""

import os
from pathlib import Path
from typing import Dict, Any, List
import yaml
from dataclasses import dataclass

# Base paths
BASE_DIR = Path(__file__).parent.parent
CONFIG_DIR = BASE_DIR / "config"
DATA_DIR = BASE_DIR / "data"
REALTIME_DATA_DIR = DATA_DIR / "realtime"

@dataclass 
class EventDrivenSettings:
    """Settings for event-driven ingestion (WebSocket, webhook, newswire)"""
    enabled: bool = False
    webhook_secret: str = ""
    webhook_path: str = "/push/inbound"
    default_rate_limit: float = 10.0  # messages per second per source
    max_queue_size: int = 10000
    
    # WebSocket sources configuration
    # [{"name": "vendor", "url": "wss://...", "topic": "business", "headers": {...}, "ping_interval": 30, "reconnect_backoff": [1,2,4,8]}]
    ws_sources: List[Dict[str, Any]] = None
    
    # Newswire sources configuration  
    # [{"name": "bloomberg", "vendor": "bloomberg", "credentials": {...}, "topic": "markets"}]
    newswire_sources: List[Dict[str, Any]] = None
    
    def __post_init__(self):
        if self.ws_sources is None:
            self.ws_sources = []
        if self.newswire_sources is None:
            self.newswire_sources = []

@dataclass
class RealtimeSettings:
    """Settings for realtime feed ingestion (adaptive RSS polling)"""
    enabled: bool = False
    
    # Adaptive polling configuration
    poll_baseline_seconds: int = 60          # baseline polling interval
    poll_min_seconds: int = 30               # never poll faster than this per host
    poll_safe_min_seconds: int = 60          # recommended minimum; warn if <60
    poll_max_seconds: int = 900              # cap interval at 15 min for backoff
    
    # Backoff configuration
    backoff_base: int = 30                   # initial backoff seconds for 429/5xx/403
    backoff_factor: float = 2.0              # backoff multiplier
    backoff_cap: int = 900                   # maximum backoff seconds
    
    # Timing variation
    jitter_range_seconds: List[int] = None   # [min, max] jitter on each poll
    stagger_startup_seconds: List[int] = None # [min, max] startup delay per host
    
    # Legacy settings (preserved for compatibility)
    poll_interval: int = 300                 # deprecated - use poll_baseline_seconds
    max_items_per_feed: int = 100
    api_host: str = "127.0.0.1"
    api_port: int = 8000
    feed_data_file: str = str(REALTIME_DATA_DIR / "latest_feeds.json")
    sources_file: str = str(CONFIG_DIR / "sources.yaml")
    
    def __post_init__(self):
        if self.jitter_range_seconds is None:
            self.jitter_range_seconds = [0, 5]
        if self.stagger_startup_seconds is None:
            self.stagger_startup_seconds = [0, 10]
        
        # Warn if baseline is below safe minimum
        if self.poll_baseline_seconds < self.poll_safe_min_seconds:
            import logging
            logger = logging.getLogger(__name__)
            logger.warning(f"POLL_BASELINE_SECONDS ({self.poll_baseline_seconds}) is below "
                         f"POLL_SAFE_MIN_SECONDS ({self.poll_safe_min_seconds}). "
                         f"Consider using {self.poll_safe_min_seconds}s or higher.")
        
        # Ensure minimum constraints
        if self.poll_min_seconds < 10:
            self.poll_min_seconds = 10  # absolute minimum for server respect

@dataclass
class AppSettings:
    """Main application settings"""
    # Feature flags
    realtime_enabled: bool = False
    event_driven_enabled: bool = False
    
    # Existing scraper settings (preserved)
    selenium_enabled: bool = True
    sentiment_enabled: bool = True
    
    # Settings objects
    realtime: RealtimeSettings = None
    event_driven: EventDrivenSettings = None
    
    def __post_init__(self):
        if self.realtime is None:
            self.realtime = RealtimeSettings(enabled=self.realtime_enabled)
        if self.event_driven is None:
            self.event_driven = EventDrivenSettings(enabled=self.event_driven_enabled)

def load_settings() -> AppSettings:
    """Load settings from environment variables with defaults"""
    
    # Feature flags
    realtime_enabled = os.getenv("REALTIME_ENABLED", "false").lower() == "true"
    event_driven_enabled = os.getenv("EVENT_DRIVEN_ENABLED", "false").lower() == "true"
    selenium_enabled = os.getenv("SELENIUM_ENABLED", "true").lower() == "true"
    sentiment_enabled = os.getenv("SENTIMENT_ENABLED", "true").lower() == "true"
    
    # Realtime settings (adaptive RSS polling)
    realtime = RealtimeSettings(
        enabled=realtime_enabled,
        
        # Adaptive polling parameters
        poll_baseline_seconds=int(os.getenv("POLL_BASELINE_SECONDS", "60")),
        poll_min_seconds=int(os.getenv("POLL_MIN_SECONDS", "30")),
        poll_safe_min_seconds=int(os.getenv("POLL_SAFE_MIN_SECONDS", "60")),
        poll_max_seconds=int(os.getenv("POLL_MAX_SECONDS", "900")),
        
        # Backoff parameters
        backoff_base=int(os.getenv("BACKOFF_BASE", "30")),
        backoff_factor=float(os.getenv("BACKOFF_FACTOR", "2.0")),
        backoff_cap=int(os.getenv("BACKOFF_CAP", "900")),
        
        # Timing variation
        jitter_range_seconds=[int(x) for x in os.getenv("JITTER_RANGE_SECONDS", "0,5").split(",")],
        stagger_startup_seconds=[int(x) for x in os.getenv("STAGGER_STARTUP_SECONDS", "0,10").split(",")],
        
        # Legacy settings (preserved for compatibility)
        poll_interval=int(os.getenv("FEED_POLL_INTERVAL", "300")),
        max_items_per_feed=int(os.getenv("MAX_ITEMS_PER_FEED", "100")),
        api_host=os.getenv("REALTIME_API_HOST", "127.0.0.1"),
        api_port=int(os.getenv("REALTIME_API_PORT", "8000")),
        feed_data_file=os.getenv("FEED_DATA_FILE", str(REALTIME_DATA_DIR / "latest_feeds.json")),
        sources_file=os.getenv("SOURCES_FILE", str(CONFIG_DIR / "sources.yaml"))
    )
    
    # Event-driven settings (WebSocket, webhook, newswire)
    event_driven = EventDrivenSettings(
        enabled=event_driven_enabled,
        webhook_secret=os.getenv("WEBHOOK_SECRET", ""),
        webhook_path=os.getenv("WEBHOOK_PATH", "/push/inbound"),
        default_rate_limit=float(os.getenv("EVENT_RATE_LIMIT", "10.0")),
        max_queue_size=int(os.getenv("EVENT_MAX_QUEUE_SIZE", "10000")),
    )
    
    # Load WebSocket sources from environment (JSON format)
    ws_sources_json = os.getenv("WS_SOURCES", "[]")
    try:
        import json
        event_driven.ws_sources = json.loads(ws_sources_json)
    except json.JSONDecodeError as e:
        print(f"Warning: Invalid WS_SOURCES JSON format: {e}")
        event_driven.ws_sources = []
    
    # Load newswire sources from environment (JSON format)
    newswire_sources_json = os.getenv("NEWSWIRE_SOURCES", "[]")
    try:
        event_driven.newswire_sources = json.loads(newswire_sources_json)
    except json.JSONDecodeError as e:
        print(f"Warning: Invalid NEWSWIRE_SOURCES JSON format: {e}")
        event_driven.newswire_sources = []
    
    # Ensure directories exist
    REALTIME_DATA_DIR.mkdir(parents=True, exist_ok=True)
    
    return AppSettings(
        realtime_enabled=realtime_enabled,
        event_driven_enabled=event_driven_enabled,
        selenium_enabled=selenium_enabled,
        sentiment_enabled=sentiment_enabled,
        realtime=realtime,
        event_driven=event_driven
    )

def load_feed_sources(sources_file: str = None) -> List[Dict[str, Any]]:
    """Load RSS/Atom feed sources from YAML file"""
    if sources_file is None:
        sources_file = CONFIG_DIR / "sources.yaml"
    
    sources_path = Path(sources_file)
    if not sources_path.exists():
        return []
    
    try:
        with open(sources_path, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f)
            return data.get('feeds', [])
    except Exception as e:
        print(f"Error loading feed sources: {e}")
        return []

# Global settings instance
settings = load_settings()